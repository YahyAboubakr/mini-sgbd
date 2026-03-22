
import java.io.*;
import java.util.*;

/**
 * Tests et comparaisons des stratégies d'accès à TableDisque.
 * Utilise table1 et table2 fournies par le prof.
 *
 * Scénarios testés :
 *   1. FullScan (référence)
 *   2. IndexScan par hachage statique  (cours p.92-99)
 *   3. IndexScan par Arbre B+, égalité (cours p.73-87)
 *   4. IndexScan par Arbre B+, intervalle
 *
 * Mesure : nombre de lectures disque (blocs / accès directs) pour chaque approche.
 */
public class ExempleIndex {

    static final String BASE = "/home/jules/Documents/4A-Apprentis/SGBD/mini-sgbd/Code SGBD 2026-20260317/Table Disque et exemples/";

    // Doit correspondre à FullScanTableDisque.blockSize
    private static final int BLOCK_SIZE = 4;

    public static void main(String[] args) throws IOException {

        // ── Tables du prof ──────────────────────────────────────────────────
        // Lire les métadonnées depuis le fichier (taille, tupleSize)
        TableDisque table1 = new TableDisque(BASE + "table1");
        FileReader metaReader = new FileReader(BASE + "table1");
        table1.taille    = metaReader.read();
        table1.tupleSize = metaReader.read();
        metaReader.close();
        // table1 : 25 tuples, 4 attributs, col0 ∈ {0,1,2,3,4}
        //   → valeur 2 présente en 5 tuples (bon cas de test avec doublons)

        int N             = table1.taille;
        int cleRecherche  = 2;   // col0=2 → 5 tuples attendus
        int rangeMin      = 1;
        int rangeMax      = 3;   // col0 ∈ [1,3] → 15 tuples attendus

        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║      COMPARAISON DES STRATÉGIES D'ACCÈS          ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("Table     : table1  (" + N + " tuples, " + table1.tupleSize + " attributs)");
        System.out.println("Blocksize : " + BLOCK_SIZE);
        System.out.println("Requête   : colonne 0 = " + cleRecherche);
        System.out.println();

        // ── 1. FullScan ────────────────────────────────────────────────────
        System.out.println("── 1. FullScan ──────────────────────────────────────");
        FullScanTableDisque scan = new FullScanTableDisque(table1);
        scan.open();
        List<Tuple> resFull = new ArrayList<>();
        Tuple t;
        while ((t = scan.next()) != null)
            if (t.val[0] == cleRecherche) resFull.add(t);
        scan.close();

        System.out.println("Tuples trouvés : " + resFull.size());
        for (Tuple r : resFull) System.out.println("   " + r);
        System.out.printf("Lectures disque  : %d  (attendu = ⌈%d/%d⌉ = %d)%n",
            scan.reads, N, BLOCK_SIZE, (int) Math.ceil((double) N / BLOCK_SIZE));
        int readsFullScan = scan.reads;

        // ── 2. IndexScan Hachage ───────────────────────────────────────────
        System.out.println("\n── 2. IndexScan Hachage Statique (7 buckets) ────────");
        String indexFile = BASE + "index_hachage_table1";
        IndexHachage index = new IndexHachage(indexFile);
        index.construire(table1, 0, 7); // 7 buckets (nombre premier), colonne 0

        IndexScanHachage scanH = new IndexScanHachage(index, table1, cleRecherche);
        scanH.open();
        List<Tuple> resHash = new ArrayList<>();
        while ((t = scanH.next()) != null) resHash.add(t);
        scanH.close();

        System.out.println("Tuples trouvés : " + resHash.size());
        for (Tuple r : resHash) System.out.println("   " + r);
        System.out.println("Lectures disque  : " + scanH.reads
            + "  (1 lecture bucket + " + resHash.size() + " accès directs)");
        int readsHachage = scanH.reads;

        // ── 3. IndexScan Arbre B+ – égalité ───────────────────────────────
        System.out.println("\n── 3. IndexScan Arbre B+ (ordre 4) – égalité ────────");
        ArbreB arbre = new ArbreB(4);

        // Construction de l'arbre : lire table1 et insérer chaque tuple
        FileReader buildReader = new FileReader(BASE + "table1");
        int taille    = buildReader.read();
        int tupleSize = buildReader.read();
        for (int i = 0; i < taille; i++) {
            int[] vals = new int[tupleSize];
            for (int j = 0; j < tupleSize; j++) vals[j] = buildReader.read();
            arbre.inserer(vals[0], i); // indexer sur col0
        }
        buildReader.close();
        arbre.afficher();

        IndexScanArbre scanA = new IndexScanArbre(arbre, table1, cleRecherche);
        scanA.open();
        List<Tuple> resArbre = new ArrayList<>();
        while ((t = scanA.next()) != null) resArbre.add(t);
        scanA.close();

        System.out.println("Tuples trouvés : " + resArbre.size());
        for (Tuple r : resArbre) System.out.println("   " + r);
        System.out.println("Lectures disque  : " + scanA.reads
            + "  (" + arbre.hauteur() + " niveau(x) d'arbre + "
            + resArbre.size() + " accès directs)");
        int readsArbre = scanA.reads;

        // ── 4. Intervalle [rangeMin, rangeMax] avec Arbre B+ ──────────────
        System.out.println("\n── 4. Intervalle [" + rangeMin + "," + rangeMax + "] via Arbre B+ ────────────");
        arbre.reads = 0;
        IndexScanArbre scanI = new IndexScanArbre(arbre, table1, rangeMin, rangeMax);
        scanI.open();
        List<Tuple> resRange = new ArrayList<>();
        while ((t = scanI.next()) != null) resRange.add(t);
        scanI.close();

        System.out.println("Tuples trouvés : " + resRange.size());
        for (Tuple r : resRange) System.out.println("   " + r);
        System.out.println("Lectures disque  : " + scanI.reads
            + "  (naviguation arbre + feuilles + accès directs)");

        // ── Bilan ─────────────────────────────────────────────────────────
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.printf ("║  BILAN  —  col0 = %d  sur %d tuples (table1)     ║%n",
            cleRecherche, N);
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.printf("FullScan          : %2d lectures%n", readsFullScan);
        System.out.printf("IndexScan Hachage : %2d lectures  (1 bucket + k accès directs)%n",
            readsHachage);
        System.out.printf("IndexScan B+      : %2d lectures  (O(log N) + k accès directs)%n",
            readsArbre);
        System.out.println();
        System.out.println("Note : avec N=" + N + " tuples l'avantage est limité.");
        System.out.println("       Sur une grande table (N=10000, k=3) :");
        System.out.printf ("         FullScan  = ⌈10000/%d⌉ = 2500 lectures%n", BLOCK_SIZE);
        System.out.println("         Hachage   =  1 + 3   =    4 lectures  (×625 plus rapide)");
        System.out.println("         B+ (h=3)  =  3 + 3   =    6 lectures  (×416 plus rapide)");

        // Vérification de cohérence
        System.out.println();
        boolean ok = resFull.size() == resHash.size() && resFull.size() == resArbre.size();
        System.out.println("Cohérence des résultats : " + (ok ? "OK ✓" : "ERREUR ✗"));

        // ── 5. IndexScan Hachage Dynamique ────────────────────────────────
        System.out.println("\n── 5. IndexScan Hachage Dynamique ───────────────────");
        // bucketCapacity=2 : provoque plus de splits → montre mieux le côté dynamique
        IndexHachageDynamique indexDyn = new IndexHachageDynamique(2);
        indexDyn.construire(table1, 0);

        IndexScanHachage scanD = new IndexScanHachage(indexDyn, table1, cleRecherche);
        scanD.open();
        List<Tuple> resDyn = new ArrayList<>();
        while ((t = scanD.next()) != null) resDyn.add(t);
        scanD.close();

        System.out.println("Tuples trouvés : " + resDyn.size());
        for (Tuple r : resDyn) System.out.println("   " + r);
        System.out.println("Lectures disque  : " + scanD.reads
            + "  (1 lecture bucket + " + resDyn.size() + " accès directs)");
        System.out.println("Cohérence avec FullScan : "
            + (resDyn.size() == resFull.size() ? "OK ✓" : "ERREUR ✗"));

        // ── 6. Mise en évidence du côté dynamique ────────────────────────
        System.out.println("\n── 6. Évolution de la structure dynamique ───────────");
        System.out.println("(bucketCapacity=2 pour forcer de nombreux splits)");
        System.out.println();

        // Insérer les clés une par une et afficher la structure après chaque doublement
        IndexHachageDynamique demo = new IndexHachageDynamique(2);
        demo.attrIndex = 0;
        int derniereProfondeur = demo.getGlobalDepth();

        System.out.println("État initial :");
        System.out.println("  profondeur globale=" + demo.getGlobalDepth()
            + "  |  répertoire=" + (1 << demo.getGlobalDepth()) + " entrées"
            + "  |  buckets distincts=" + demo.getNbBucketsDistincts());

        // Relire la table et insérer tuple par tuple
        FileReader demoReader = new FileReader(BASE + "table1");
        int demTaille    = demoReader.read();
        int demTupleSize = demoReader.read();
        for (int i = 0; i < demTaille; i++) {
            int[] vals = new int[demTupleSize];
            for (int j = 0; j < demTupleSize; j++) vals[j] = demoReader.read();
            // Insérer via construire() n'est pas disponible tuple par tuple,
            // on reconstruit un mini-index à chaque doublement pour afficher l'évolution.
            // → On insère dans 'demo' via une table temporaire simulée.
            // Pour garder le code simple, on surveille via getGlobalDepth() après construire()
        }
        demoReader.close();

        // Montrer l'évolution avec des capacités croissantes
        int[] capacites = {8, 4, 2};
        for (int cap : capacites) {
            IndexHachageDynamique etape = new IndexHachageDynamique(cap);
            etape.construire(table1, 0);
            System.out.println("  capacité=" + cap
                + "  →  profondeur globale=" + etape.getGlobalDepth()
                + "  |  répertoire=" + (1 << etape.getGlobalDepth()) + " entrées"
                + "  |  buckets distincts=" + etape.getNbBucketsDistincts());
        }

        System.out.println();
        System.out.println("Structure finale (capacité=2) :");
        indexDyn.afficher();

        System.out.println();
        System.out.println("Explication :");
        System.out.println("  - Statique  : N buckets fixes, risque de collisions");
        System.out.println("  - Dynamique : commence petit, double le répertoire quand");
        System.out.println("                un bucket dépasse sa capacité → jamais de");
        System.out.println("                débordement structurel, coût O(1) en lecture");

        // ── 7. JointureBoucleIndex avec hachage dynamique ─────────────────
        System.out.println("\n── 7. JointureBoucleIndex — statique vs dynamique ───");
        System.out.println("Requête : SELECT * FROM table1, table1 WHERE t1.col0 = t2.col0");

        // Index statique sur table1 col0
        IndexHachage indexStatJoin = new IndexHachage(BASE + "index_hachage_join_static");
        indexStatJoin.construire(table1, 0, 7);
        Operateur fsStat = new FullScanTableDisque(table1);
        JointureBoucleIndex joinStat = new JointureBoucleIndex(fsStat, table1, indexStatJoin, 0);
        joinStat.open();
        int countStat = 0;
        while (joinStat.next() != null) countStat++;
        joinStat.close();

        // Index dynamique sur table1 col0
        IndexHachageDynamique indexDynJoin = new IndexHachageDynamique(4);
        indexDynJoin.construire(table1, 0);
        Operateur fsDyn = new FullScanTableDisque(table1);
        JointureBoucleIndex joinDyn = new JointureBoucleIndex(fsDyn, table1, indexDynJoin, 0);
        joinDyn.open();
        int countDyn = 0;
        while (joinDyn.next() != null) countDyn++;
        joinDyn.close();

        System.out.println("Tuples produits (statique)  : " + countStat);
        System.out.println("Tuples produits (dynamique) : " + countDyn);
        System.out.println("Cohérence : " + (countStat == countDyn ? "OK ✓" : "ERREUR ✗"));
    }
}
