
import java.io.*;
import java.util.*;

/**
 * Tri externe par fusion (cours p.50-70).
 *
 * Passe 0  : Lire B blocs en mémoire, trier, écrire un run sur disque.
 *            Produit ceil(N/B) runs triés.
 *
 * Passes 1+ : Fusionner B-1 runs à la fois via tas-min (PriorityQueue).
 *             Répéter jusqu'à obtenir 1 seul run.
 *
 * Coût (2 passes, N blocs, B blocs mémoire) :
 *   Passe 0 : N lectures  + N écritures
 *   Passe 1 : N lectures  + N écritures
 *   Total   : 4N I/O
 *   Condition 2 passes : ceil(N/B) <= B-1  =>  N <= B*(B-1)
 *
 * Contrainte : memoryBlocs >= 3  (2 tampons entrée + 1 tampon sortie minimum).
 */
public class TriExterne implements Operateur {

    private TableDisque table;
    private int colTri;
    private int memoryBlocs;
    private int blockSize;

    public int reads  = 0;
    public int writes = 0;

    private int        tupleSize;
    private int        sortedTaille;
    private int        tuplesLus = 0;
    private FileReader sortedReader;

    private List<String> tempFiles = new ArrayList<>();
    private int          tempCount = 0;
    private String       tempDir;

    // ── Constructeurs ────────────────────────────────────────────────────────

    public TriExterne(TableDisque table, int colTri) {
        this(table, colTri, 3, 4);
    }

    public TriExterne(TableDisque table, int colTri, int memoryBlocs, int blockSize) {
        this.table       = table;
        this.colTri      = colTri;
        this.blockSize   = blockSize;
        // IMPORTANT : minimum 3 blocs (2 tampons entrée + 1 tampon sortie).
        // Avec B<3, fanIn = B-1 < 2 : la boucle de fusion ne converge pas.
        this.memoryBlocs = Math.max(3, memoryBlocs);
        File f = new File(table.filePath);
        this.tempDir = (f.getParent() != null) ? f.getParent() : ".";
    }

    // ── Operateur ────────────────────────────────────────────────────────────

    @Override
    public void open() {
        reads = 0; writes = 0;
        try {
            List<String> runs = genererRuns();
            int passe = 1;
            while (runs.size() > 1) {
                System.out.println("  Passe " + passe++ + " : "
                    + runs.size() + " runs, fusion " + (memoryBlocs - 1) + "-voies");
                runs = fusionnerPasse(runs);
            }
            sortedReader = new FileReader(runs.get(0));
            sortedTaille = sortedReader.read();
            tupleSize    = sortedReader.read();
            tuplesLus    = 0;
        } catch (IOException e) {
            System.err.println("TriExterne : erreur lors du tri.");
            e.printStackTrace();
        }
    }

    @Override
    public Tuple next() {
        if (tuplesLus >= sortedTaille) return null;
        try {
            Tuple t = new Tuple(tupleSize);
            for (int j = 0; j < tupleSize; j++) t.val[j] = sortedReader.read();
            tuplesLus++;
            return t;
        } catch (IOException e) { return null; }
    }

    @Override
    public void close() {
        try { if (sortedReader != null) sortedReader.close(); } catch (IOException ignored) {}
        for (String path : tempFiles) new File(path).delete();
        tempFiles.clear();
    }

    @Override
    public int estimateSize() { return table.taille; }

    // ── Passe 0 : génération des runs ────────────────────────────────────────

    private List<String> genererRuns() throws IOException {
        List<String> runs = new ArrayList<>();
        FileReader reader = new FileReader(table.filePath);
        int taille    = reader.read();
        tupleSize     = reader.read();
        int capacity  = memoryBlocs * blockSize;
        int restants  = taille;

        System.out.println("  Passe 0 : " + taille + " tuples, buffer="
            + memoryBlocs + " blocs x " + blockSize + " = " + capacity + " tuples/run max");

        while (restants > 0) {
            int nbALire = Math.min(capacity, restants);
            List<Tuple> buf = new ArrayList<>();
            for (int i = 0; i < nbALire; i++) {
                Tuple t = new Tuple(tupleSize);
                for (int j = 0; j < tupleSize; j++) t.val[j] = reader.read();
                buf.add(t);
            }
            reads   += (int) Math.ceil((double) nbALire / blockSize);
            restants -= nbALire;
            final int col = colTri;
            buf.sort((a, b) -> a.val[col] - b.val[col]);
            String runPath = newTempPath("run");
            ecrireRun(runPath, buf);
            writes += (int) Math.ceil((double) buf.size() / blockSize);
            runs.add(runPath);
        }
        reader.close();
        System.out.println("  -> " + runs.size() + " runs generes");
        return runs;
    }

    // ── Passes suivantes : fusion multi-voies ────────────────────────────────

    private List<String> fusionnerPasse(List<String> runs) throws IOException {
        List<String> newRuns = new ArrayList<>();
        int fanIn = memoryBlocs - 1; // >= 2 car memoryBlocs >= 3

        for (int i = 0; i < runs.size(); i += fanIn) {
            List<String> batch = new ArrayList<>(
                runs.subList(i, Math.min(i + fanIn, runs.size())));
            String outPath = newTempPath("merge");
            fusionnerBatch(batch, outPath);
            newRuns.add(outPath);
        }
        return newRuns;
    }

    /**
     * Fusionne un groupe de runs triés en un seul via tas-min.
     * Entrée du tas : int[] { cle, runIndex, val0..valN }
     */
    private void fusionnerBatch(List<String> runPaths, String outPath) throws IOException {
        int n = runPaths.size();
        FileReader[] readers   = new FileReader[n];
        int[]        tailles   = new int[n];
        int[]        positions = new int[n];

        for (int i = 0; i < n; i++) {
            readers[i] = new FileReader(runPaths.get(i));
            tailles[i] = readers[i].read();
            readers[i].read(); // tupleSize deja connu
            reads += (int) Math.ceil((double) tailles[i] / blockSize);
        }

        PriorityQueue<int[]> pq = new PriorityQueue<>(
            (a, b) -> a[0] != b[0] ? a[0] - b[0] : a[1] - b[1]);

        for (int i = 0; i < n; i++)
            if (tailles[i] > 0) { Tuple t = lireTuple(readers[i]); if (t != null) pq.add(mkEntry(t, i)); }

        List<Tuple> out = new ArrayList<>();
        while (!pq.isEmpty()) {
            int[] e = pq.poll();
            int   ri = e[1];
            out.add(entryToTuple(e));
            positions[ri]++;
            if (positions[ri] < tailles[ri]) {
                Tuple t = lireTuple(readers[ri]);
                if (t != null) pq.add(mkEntry(t, ri));
            }
        }
        for (FileReader r : readers) r.close();
        ecrireRun(outPath, out);
        writes += (int) Math.ceil((double) out.size() / blockSize);
    }

    // ── Utilitaires ──────────────────────────────────────────────────────────

    private int[] mkEntry(Tuple t, int ri) {
        int[] e = new int[2 + tupleSize];
        e[0] = t.val[colTri]; e[1] = ri;
        for (int j = 0; j < tupleSize; j++) e[2 + j] = t.val[j];
        return e;
    }

    private Tuple entryToTuple(int[] e) {
        Tuple t = new Tuple(tupleSize);
        for (int j = 0; j < tupleSize; j++) t.val[j] = e[2 + j];
        return t;
    }

    private Tuple lireTuple(FileReader r) throws IOException {
        Tuple t = new Tuple(tupleSize);
        for (int j = 0; j < tupleSize; j++) {
            int v = r.read(); if (v == -1) return null; t.val[j] = v;
        }
        return t;
    }

    private void ecrireRun(String path, List<Tuple> tuples) throws IOException {
        FileWriter w = new FileWriter(path);
        w.write(tuples.size()); w.write(tupleSize);
        for (Tuple t : tuples) for (int j = 0; j < tupleSize; j++) w.write(t.val[j]);
        w.close();
    }

    private String newTempPath(String type) {
        String path = tempDir + File.separator + "_tri_" + type + "_" + (tempCount++);
        tempFiles.add(path);
        return path;
    }
}
