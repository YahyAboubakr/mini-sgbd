import java.io.*;
import java.util.*;

public class Main {

    static final String PATH = "/home/jules/Documents/4A-Apprentis/SGBD/mini-sgbd/Code SGBD 2026-20260317/Table Disque et exemples/";

    public static void main(String[] args) throws IOException {

        // ══════════════════════════════════════════════════════════════
        // 1. FULLSCAN TABLE MÉMOIRE
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 1. FULLSCAN TABLE MÉMOIRE ═══════════\n");
        Tuple t;
        TableMemoire tm = TableMemoire.randomize(3, 10, 5);
        Operateur requete1 = new FullScanTableMemoire(tm);
        requete1.open();
        while ((t = requete1.next()) != null) System.out.println(t);
        requete1.close();

        // ══════════════════════════════════════════════════════════════
        // 2. AGRÉGATION (SUM, AVG, MIN, MAX)
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 2. AGRÉGATION ═══════════\n");
        TableMemoire tmAgg = TableMemoire.randomize(2, 10, 5);
        System.out.println("=== Contenu de la table ===");
        Operateur scanAgg = new FullScanTableMemoire(tmAgg);
        scanAgg.open();
        while ((t = scanAgg.next()) != null) System.out.println(t);
        scanAgg.close();

        
        //Data source de l'aggregation = Full scan de la table mémoire
        Operateur somme = new Agregation(new FullScanTableMemoire(tmAgg), 0, Agregation.SUM);
        somme.open();
        System.out.println("SUM colonne 0 : " + somme.next().val[0]);
        somme.close();

        Agregation moyenne = new Agregation(new FullScanTableMemoire(tmAgg), 0, Agregation.AVG);
        moyenne.open();
        moyenne.next();
        System.out.printf("AVG colonne 0 : %.2f%n", moyenne.getMoyenne());
        moyenne.close();

        Operateur minimum = new Agregation(new FullScanTableMemoire(tmAgg), 0, Agregation.MIN);
        minimum.open();
        System.out.println("MIN colonne 0 : " + minimum.next().val[0]);
        minimum.close();

        Operateur maximum = new Agregation(new FullScanTableMemoire(tmAgg), 0, Agregation.MAX);
        maximum.open();
        System.out.println("MAX colonne 0 : " + maximum.next().val[0]);
        maximum.close();

        // ══════════════════════════════════════════════════════════════
        // 3. FULL SCAN TABLE DISQUE + TESTS DE VALIDATION
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 3. FULL SCAN TABLE DISQUE ═══════════\n");
        TableDisque td1 = new TableDisque(PATH + "table1");
        TableDisque td2 = new TableDisque(PATH + "table2");

        System.out.println("=== Full Scan table1 ===");
        FullScanTableDisque scan1 = new FullScanTableDisque(td1);
        scan1.open();
        while ((t = scan1.next()) != null) System.out.println(t);
        System.out.println("Nb blocs lus : " + scan1.reads);
        scan1.close();

        System.out.println("\n=== Full Scan table2 ===");
        FullScanTableDisque scan2 = new FullScanTableDisque(td2);
        scan2.open();
        while ((t = scan2.next()) != null) System.out.println(t);
        System.out.println("Nb blocs lus : " + scan2.reads);
        scan2.close();

        System.out.println("\n=== Full Scan table3 (valeurs connues 0-9) ===");
        TableDisque td3 = new TableDisque(PATH + "table3");
        td3.ecrire(new int[][]{{0,1},{2,3},{4,5},{6,7},{8,9}});
        FullScanTableDisque scan3 = new FullScanTableDisque(td3);
        scan3.open();
        while ((t = scan3.next()) != null) System.out.println(t);
        System.out.println("Nb blocs lus : " + scan3.reads);
        scan3.close();

        runValidationTests();

        // ══════════════════════════════════════════════════════════════
        // 4. ARBRE D'EXÉCUTION — REQUÊTES SQL
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 4. ARBRE D'EXÉCUTION — REQUÊTES SQL ═══════════\n");
        System.out.println("--- SELECT * FROM table1 ---");
        ExecutionTree.executeQuery("SELECT * FROM table1", PATH);

        System.out.println("\n--- SELECT * FROM table2 ---");
        ExecutionTree.executeQuery("SELECT * FROM table2", PATH);

        System.out.println("\n--- JOIN table1, table2 WHERE table1.0 > table2.1 ---");
        ExecutionTree.executeQuery("SELECT * FROM table1, table2 WHERE table1.0 > table2.1", PATH);

        // ══════════════════════════════════════════════════════════════
        // 5. TRI EXTERNE
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 5. TRI EXTERNE ═══════════\n");
        final int BLOCK_SIZE = 4;

        TableDisque tableTriExt = new TableDisque(PATH + "table1");
        FileReader metaTri = new FileReader(PATH + "table1");
        tableTriExt.taille    = metaTri.read();
        tableTriExt.tupleSize = metaTri.read();
        metaTri.close();

        int N = tableTriExt.taille;
        int blocs = (int) Math.ceil((double) N / BLOCK_SIZE);
        System.out.println("Table : table1  (" + N + " tuples, "
            + tableTriExt.tupleSize + " attributs, blockSize=" + BLOCK_SIZE + ")");
        System.out.println("Cout theorique 2 passes : 4 x " + blocs + " = " + (4 * blocs) + " I/O");

        System.out.println("\n── Test 1 : tri sur col0, buffer=3 blocs ────────────");
        TriExterne tri1 = new TriExterne(tableTriExt, 0, 3, BLOCK_SIZE);
        tri1.open();
        List<Tuple> result1 = new ArrayList<>();
        while ((t = tri1.next()) != null) result1.add(t);
        tri1.close();
        System.out.println("Resultat trie par col0 :");
        for (Tuple r : result1) System.out.println("   " + r);
        System.out.println("Lectures  : " + tri1.reads + "  Ecritures : " + tri1.writes
            + "  Total : " + (tri1.reads + tri1.writes));
        System.out.println("Trie correctement : " + (verifTri(result1, 0) ? "OK" : "ERREUR"));
        System.out.println("Nb tuples correct : " + (result1.size() == N ? "OK" : "ERREUR"));

        System.out.println("\n── Test 2 : tri sur col1, buffer=5 blocs ────────────");
        TriExterne tri2 = new TriExterne(tableTriExt, 1, 5, BLOCK_SIZE);
        tri2.open();
        List<Tuple> result2 = new ArrayList<>();
        while ((t = tri2.next()) != null) result2.add(t);
        tri2.close();
        System.out.println("Resultat trie par col1 :");
        for (Tuple r : result2) System.out.println("   " + r);
        System.out.println("Lectures  : " + tri2.reads + "  Ecritures : " + tri2.writes
            + "  Total : " + (tri2.reads + tri2.writes));
        System.out.println("Trie correctement : " + (verifTri(result2, 1) ? "OK" : "ERREUR"));

        System.out.println("\nBILAN I/O :");
        System.out.printf("FullScan seul    : %2d lectures (pas d'ecriture)%n", blocs);
        System.out.printf("TriExterne B=3   : %2d lect + %2d ecrit = %2d I/O%n",
            tri1.reads, tri1.writes, tri1.reads + tri1.writes);
        System.out.printf("TriExterne B=5   : %2d lect + %2d ecrit = %2d I/O%n",
            tri2.reads, tri2.writes, tri2.reads + tri2.writes);

        // ══════════════════════════════════════════════════════════════
        // 6. INDEX — COMPARAISON DES STRATÉGIES D'ACCÈS
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 6. INDEX — COMPARAISON DES STRATÉGIES ═══════════\n");

        TableDisque tableIdx = new TableDisque(PATH + "table1");
        FileReader metaIdx = new FileReader(PATH + "table1");
        tableIdx.taille    = metaIdx.read();
        tableIdx.tupleSize = metaIdx.read();
        metaIdx.close();

        int NIdx        = tableIdx.taille;
        int cleRecherche = 2;
        int rangeMin    = 1, rangeMax = 3;

        System.out.println("Table : table1  (" + NIdx + " tuples)  —  requête : col0 = " + cleRecherche);

        // FullScan
        System.out.println("\n── FullScan ──────────────────────────────────────────");
        FullScanTableDisque scanFS = new FullScanTableDisque(tableIdx);
        scanFS.open();
        List<Tuple> resFull = new ArrayList<>();
        while ((t = scanFS.next()) != null) if (t.val[0] == cleRecherche) resFull.add(t);
        scanFS.close();
        System.out.println("Tuples trouvés : " + resFull.size());
        for (Tuple r : resFull) System.out.println("   " + r);
        System.out.printf("Lectures disque  : %d  (attendu = ⌈%d/%d⌉ = %d)%n",
            scanFS.reads, NIdx, BLOCK_SIZE, (int) Math.ceil((double) NIdx / BLOCK_SIZE));

        // IndexScan Hachage
        System.out.println("\n── IndexScan Hachage Statique (7 buckets) ────────────");
        String indexFile = PATH + "index_hachage_table1";
        IndexHachage index = new IndexHachage(indexFile);
        index.construire(tableIdx, 0, 7);

        IndexScanHachage scanH = new IndexScanHachage(index, tableIdx, cleRecherche);
        scanH.open();
        List<Tuple> resHash = new ArrayList<>();
        while ((t = scanH.next()) != null) resHash.add(t);
        scanH.close();
        System.out.println("Tuples trouvés : " + resHash.size());
        for (Tuple r : resHash) System.out.println("   " + r);
        System.out.println("Lectures disque  : " + scanH.reads
            + "  (1 bucket + " + resHash.size() + " accès directs)");

        // IndexScan Arbre B+
        System.out.println("\n── IndexScan Arbre B+ (ordre 4) – égalité ────────────");
        ArbreB arbre = new ArbreB(4);
        FileReader buildReader = new FileReader(PATH + "table1");
        int bTaille    = buildReader.read();
        int bTupleSize = buildReader.read();
        for (int i = 0; i < bTaille; i++) {
            int[] vals = new int[bTupleSize];
            for (int j = 0; j < bTupleSize; j++) vals[j] = buildReader.read();
            arbre.inserer(vals[0], i);
        }
        buildReader.close();
        arbre.afficher();

        IndexScanArbre scanA = new IndexScanArbre(arbre, tableIdx, cleRecherche);
        scanA.open();
        List<Tuple> resArbre = new ArrayList<>();
        while ((t = scanA.next()) != null) resArbre.add(t);
        scanA.close();
        System.out.println("Tuples trouvés : " + resArbre.size());
        for (Tuple r : resArbre) System.out.println("   " + r);
        System.out.println("Lectures disque  : " + scanA.reads
            + "  (" + arbre.hauteur() + " niveau(x) + " + resArbre.size() + " accès directs)");

        // Intervalle [rangeMin, rangeMax]
        System.out.println("\n── Intervalle [" + rangeMin + "," + rangeMax + "] via Arbre B+ ────────────");
        arbre.reads = 0;
        IndexScanArbre scanI = new IndexScanArbre(arbre, tableIdx, rangeMin, rangeMax);
        scanI.open();
        List<Tuple> resRange = new ArrayList<>();
        while ((t = scanI.next()) != null) resRange.add(t);
        scanI.close();
        System.out.println("Tuples trouvés : " + resRange.size());
        for (Tuple r : resRange) System.out.println("   " + r);
        System.out.println("Lectures disque  : " + scanI.reads);

        System.out.println("\nBILAN :");
        System.out.printf("FullScan          : %2d lectures%n", scanFS.reads);
        System.out.printf("IndexScan Hachage : %2d lectures%n", scanH.reads);
        System.out.printf("IndexScan B+      : %2d lectures%n", scanA.reads);
        boolean coherence = resFull.size() == resHash.size() && resFull.size() == resArbre.size();
        System.out.println("Cohérence des résultats : " + (coherence ? "OK ✓" : "ERREUR ✗"));

        // ══════════════════════════════════════════════════════════════
        // 7. JOINTURE TRI-FUSION
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 7. JOINTURE TRI-FUSION ═══════════\n");

        JointureTriFusion jtfMain = new JointureTriFusion(
                new FullScanTableDisque(new TableDisque(PATH + "table1")),
                new FullScanTableDisque(new TableDisque(PATH + "table2")),
                0, 0);
        jtfMain.open();
        int countJTF = 0;
        while ((t = jtfMain.next()) != null) { System.out.println(t); countJTF++; }
        jtfMain.close();
        System.out.println("Tuples joints (Tri-Fusion) : " + countJTF);
        System.out.println(jtfMain);

        // ══════════════════════════════════════════════════════════════
        // 8. JOINTURE HACHAGE (+ vérification croisée avec Tri-Fusion)
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 8. JOINTURE HACHAGE ═══════════\n");

        JointureHachage jh = new JointureHachage(
                new FullScanTableDisque(new TableDisque(PATH + "table1")),
                new FullScanTableDisque(new TableDisque(PATH + "table2")),
                0, 0);
        jh.open();
        int countJH = 0;
        while ((t = jh.next()) != null) { System.out.println(t); countJH++; }
        jh.close();
        System.out.println("Tuples joints (Hachage) : " + countJH);
        System.out.println(jh);

        System.out.println("Vérification croisée Hachage vs Tri-Fusion : "
            + (countJH == countJTF ? "[OK] " + countJH + " tuples" : "[ERREUR] " + countJH + " vs " + countJTF));

        // ══════════════════════════════════════════════════════════════
        // 9. JOINTURE BOUCLE INDEX
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 9. JOINTURE BOUCLE INDEX ═══════════\n");
        System.out.println("Requête : SELECT * FROM table1, table2 WHERE table1.col0 = table2.col0");

        TableDisque tdJBI1 = new TableDisque(PATH + "table1");
        TableDisque tdJBI2 = new TableDisque(PATH + "table2");
        FileReader metaJBI1 = new FileReader(PATH + "table1");
        tdJBI1.taille    = metaJBI1.read();
        tdJBI1.tupleSize = metaJBI1.read();
        metaJBI1.close();
        FileReader metaJBI2 = new FileReader(PATH + "table2");
        tdJBI2.taille    = metaJBI2.read();
        tdJBI2.tupleSize = metaJBI2.read();
        metaJBI2.close();

        IndexHachage indexJBI = new IndexHachage(PATH + "index_table2_col0");
        indexJBI.construire(tdJBI2, 0, 7);

        JointureBoucleIndex joinBI = new JointureBoucleIndex(
                new FullScanTableDisque(tdJBI1), tdJBI2, indexJBI, 0);
        joinBI.open();
        int countJBI = 0;
        while ((t = joinBI.next()) != null) { System.out.println(t); countJBI++; }
        joinBI.close();
        System.out.println("Nombre de résultats : " + countJBI);

        // ══════════════════════════════════════════════════════════════
        // 10. SÉLECTEUR DE JOINTURE (grandes tables — tri-fusion forcé)
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 10. SÉLECTEUR DE JOINTURE (grandes tables) ═══════════\n");
        TableDisque tdBig3 = new TableDisque(PATH + "table_big3");
        tdBig3.randomize(2, 1200);
        TableDisque tdBig4 = new TableDisque(PATH + "table_big4");
        tdBig4.randomize(2, 1200);
        ExecutionTree.executeQuery("SELECT * FROM table_big3, table1 WHERE table_big3.0 != table1.0", PATH);

        // ══════════════════════════════════════════════════════════════
        // 11. JOINTURE DOUBLE BOUCLE IMBRIQUÉE (DBI) — petites tables
        // ══════════════════════════════════════════════════════════════
        System.out.println("\n═══════════ 11. JOINTURE DOUBLE BOUCLE IMBRIQUÉE (DBI) ═══════════\n");
        TableDisque td5 = new TableDisque(PATH + "table5");
        td5.randomize(2, 5);
        TableDisque td6 = new TableDisque(PATH + "table6");
        td6.randomize(2, 5);

        System.out.println("--- SELECT * FROM table5 ---");
        ExecutionTree.executeQuery("SELECT * FROM table5", PATH);
        System.out.println("\n--- SELECT * FROM table6 ---");
        ExecutionTree.executeQuery("SELECT * FROM table6", PATH);
        System.out.println("\n--- SELECT * FROM table5, table6 WHERE table5.0 = table6.1 ---");
        ExecutionTree.executeQuery("SELECT * FROM table5, table6 WHERE table5.0 = table6.1", PATH);
    }

    // ──────────────────────────────────────────────────────────────────
    // Helpers
    // ──────────────────────────────────────────────────────────────────

    /** Affiche tous les tuples d'une table disque. */
    private static void afficherTable(String chemin) {
        FullScanTableDisque scan = new FullScanTableDisque(new TableDisque(chemin));
        scan.open();
        Tuple t;
        while ((t = scan.next()) != null) System.out.println(t);
        scan.close();
    }

    /** Vérifie qu'une liste de tuples est triée sur la colonne donnée. */
    static boolean verifTri(List<Tuple> tuples, int col) {
        for (int i = 1; i < tuples.size(); i++)
            if (tuples.get(i).val[col] < tuples.get(i-1).val[col]) return false;
        return true;
    }

    // ──────────────────────────────────────────────────────────────────
    // Tests de validation FullScanTableDisque
    // ──────────────────────────────────────────────────────────────────

    static void runValidationTests() {
        System.out.println("\n═══════ TESTS DE VALIDATION FullScanTableDisque ═══════");
        testCoherence();
        testNombreBlocs();
        testReouverture();
        testTableVide();
        testUnBlocExact();
        System.out.println("═══════════════════════════════════════════════════════\n");
    }

    static void testCoherence() {
        TableDisque td = new TableDisque(PATH + "test_coherence");
        int[][] data = {{1, 2}, {3, 4}, {5, 6}};
        td.ecrire(data);
        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open();
        int i = 0;
        Tuple t;
        while ((t = scan.next()) != null) {
            assert t.val[0] == data[i][0] : "Erreur A1 ligne " + i;
            assert t.val[1] == data[i][1] : "Erreur A2 ligne " + i;
            i++;
        }
        assert i == data.length : "Nombre de tuples incorrect : " + i;
        scan.close();
        System.out.println("[OK] Test cohérence écriture/lecture");
    }

    static void testNombreBlocs() {
        int n = 9, blockSize = 4;
        TableDisque td = new TableDisque(PATH + "test_blocs");
        td.randomize(2, n);
        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open();
        while (scan.next() != null) {}
        scan.close();
        int expected = (int) Math.ceil((double) n / blockSize);
        assert scan.reads == expected : "Blocs lus : " + scan.reads + " (attendu " + expected + ")";
        System.out.println("[OK] Test nombre de blocs lus (" + scan.reads + " blocs pour " + n + " tuples)");
    }

    static void testReouverture() {
        TableDisque td = new TableDisque(PATH + "test_reopen");
        td.ecrire(new int[][]{{10, 20}, {30, 40}});
        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open(); Tuple first1 = scan.next(); scan.close();
        scan.open(); Tuple first2 = scan.next(); scan.close();
        assert first1 != null && first2 != null : "Les tuples ne doivent pas être null";
        assert first1.val[0] == first2.val[0] : "Réouverture incohérente sur val[0]";
        assert first1.val[1] == first2.val[1] : "Réouverture incohérente sur val[1]";
        System.out.println("[OK] Test réouverture (open/close/open)");
    }

    static void testTableVide() {
        TableDisque td = new TableDisque(PATH + "test_vide");
        td.ecrire(new int[0][2]);
        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open();
        Tuple t = scan.next();
        scan.close();
        assert t == null : "Une table vide doit retourner null au premier next()";
        System.out.println("[OK] Test table vide");
    }

    static void testUnBlocExact() {
        TableDisque td = new TableDisque(PATH + "test_1bloc");
        td.ecrire(new int[][]{{1,1},{2,2},{3,3},{4,4}});
        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open();
        int count = 0;
        while (scan.next() != null) count++;
        scan.close();
        assert count == 4 : "Attendu 4 tuples, obtenu " + count;
        assert scan.reads == 2 : "Attendu 2 blocs lus, obtenu " + scan.reads;
        System.out.println("[OK] Test 1 bloc exact (4 tuples, 2 lectures dont 1 EOF)");
    }
}
