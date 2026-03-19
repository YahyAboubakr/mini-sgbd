
import java.io.*;
import java.util.*;

/**
 * Opérateur IndexScan par hachage statique (cours p.97-98).
 *
 * Au lieu d'un FullScan (N/blockSize lectures), utilise l'IndexHachage
 * pour localiser directement les tuples correspondant à une valeur de clé.
 *
 * Coût :
 *   - open()  → 1 lecture de bucket dans l'index
 *   - next()  → 1 lecture directe par tuple retourné
 *   Total     → 1 + k  (vs ceil(N/b) pour le FullScan)
 */
public class IndexScanHachage implements Operateur {

    private IndexHachage index;
    private TableDisque  table;
    private int valeurRecherchee;

    private List<Integer> tupleIds;
    private int cursor;

    public int reads = 0; // total des lectures disque (bucket + accès directs)

    public IndexScanHachage(IndexHachage index, TableDisque table, int valeur) {
        this.index = index;
        this.table = table;
        this.valeurRecherchee = valeur;
    }

    // ── Operateur ─────────────────────────────────────────────────────────────

    @Override
    public void open() {
        try {
            index.reads = 0;
            tupleIds = index.chercher(valeurRecherchee); // 1 lecture de bucket
            this.reads += index.reads;
            index.reads = 0;
        } catch (IOException e) {
            System.err.println("IndexScanHachage : erreur lors de la recherche dans l'index.");
            tupleIds = new ArrayList<>();
        }
        cursor = 0;
    }

    @Override
    public Tuple next() {
        if (cursor >= tupleIds.size()) return null;
        return lireTupleDirectement(tupleIds.get(cursor++));
    }

    @Override
    public void close() {}

    @Override
    public int estimateSize() {
        return tupleIds == null ? 0 : tupleIds.size();
    }

    // ── Accès direct ──────────────────────────────────────────────────────────

    /**
     * Lit le tuple numéro numTuple depuis TableDisque sans FullScan.
     *
     * Calcul de l'offset :
     *   position = 2 (header) + numTuple * tupleSize octets
     *
     * Comptage : reads++ par appel (1 accès disque = 1 tuple lu directement).
     */
    private Tuple lireTupleDirectement(int numTuple) {
        try {
            FileReader reader = new FileReader(table.filePath);
            reader.read();            // skip : taille de la table
            int ts = reader.read();   // tupleSize
            reader.skip((long) numTuple * ts);
            Tuple t = new Tuple(ts);
            for (int j = 0; j < ts; j++) t.val[j] = reader.read();
            reader.close();
            this.reads++;
            return t;
        } catch (IOException e) {
            System.err.println("IndexScanHachage : erreur d'accès direct au tuple " + numTuple);
            return null;
        }
    }
}
