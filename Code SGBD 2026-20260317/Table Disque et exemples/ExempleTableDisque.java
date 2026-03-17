public class ExempleTableDisque {

    static final String BASE = "/home/jules/Documents/4A-Apprentis/SGBD/mini-sgbd/Code SGBD 2026-20260317/Table Disque et exemples/";

    public static void main(String[] args) {

        Tuple t = null;

        // --- Test 1 : Full Scan sur table1 et table2 existantes ---
        TableDisque td1 = new TableDisque(BASE + "table1");
        TableDisque td2 = new TableDisque(BASE + "table2");

        System.out.println("=== Full Scan table1 ===");
        FullScanTableDisque scan1 = new FullScanTableDisque(td1);
        scan1.open();
        while ((t = scan1.next()) != null)
            System.out.println(t);
        System.out.println("Nb blocs lus : " + scan1.reads);
        scan1.close();

        System.out.println("=== Full Scan table2 ===");
        FullScanTableDisque scan2 = new FullScanTableDisque(td2);
        scan2.open();
        while ((t = scan2.next()) != null)
            System.out.println(t);
        System.out.println("Nb blocs lus : " + scan2.reads);
        scan2.close();

        // --- Test 2 : Écriture de table3 avec des valeurs connues (0 à 9) ---
        TableDisque td3 = new TableDisque(BASE + "table3");
        td3.ecrire(new int[][]{{0,1},{2,3},{4,5},{6,7},{8,9}});

        System.out.println("=== Full Scan table3 (valeurs connues 0-9) ===");
        FullScanTableDisque scan3 = new FullScanTableDisque(td3);
        scan3.open();
        while ((t = scan3.next()) != null)
            System.out.println(t);
        System.out.println("Nb blocs lus : " + scan3.reads);
        scan3.close();

        // --- Tests de validation de la factorisation ---
        runAllTests();
    }

    static void runAllTests() {
        System.out.println("\n========== TESTS DE VALIDATION ==========");
        testCoherence();
        testNombreBlocs();
        testReouverture();
        testTableVide();
        testUnBlocExact();
        System.out.println("========== FIN DES TESTS ==========\n");
    }

    // Test 1 : vérifier que les valeurs relues sont exactement celles écrites
    static void testCoherence() {
        TableDisque td = new TableDisque(BASE + "test_coherence");
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
        assert i == data.length : "Nombre de tuples incorrect : " + i + " (attendu " + data.length + ")";
        scan.close();
        System.out.println("[OK] Test cohérence écriture/lecture");
    }

    // Test 2 : vérifier que le nombre de blocs lus vaut ceil(n / blockSize)
    // avec blockSize = 4 et n = 9 tuples → 3 blocs attendus
    static void testNombreBlocs() {
        int n = 9;
        int blockSize = 4; // doit correspondre à la valeur dans FullScanTableDisque

        TableDisque td = new TableDisque(BASE + "test_blocs");
        td.randomize(2, n);

        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open();
        while (scan.next() != null) {}
        scan.close();

        int expected = (int) Math.ceil((double) n / blockSize);
        assert scan.reads == expected
            : "Blocs lus : " + scan.reads + " (attendu " + expected + ")";
        System.out.println("[OK] Test nombre de blocs lus (" + scan.reads + " blocs pour " + n + " tuples)");
    }

    // Test 3 : un second open() doit remettre le curseur au début
    static void testReouverture() {
        TableDisque td = new TableDisque(BASE + "test_reopen");
        td.ecrire(new int[][]{{10, 20}, {30, 40}});

        FullScanTableDisque scan = new FullScanTableDisque(td);

        scan.open();
        Tuple first1 = scan.next();
        scan.close();

        scan.open();
        Tuple first2 = scan.next();
        scan.close();

        assert first1 != null && first2 != null : "Les tuples ne doivent pas être null";
        assert first1.val[0] == first2.val[0] : "Réouverture incohérente sur val[0]";
        assert first1.val[1] == first2.val[1] : "Réouverture incohérente sur val[1]";
        System.out.println("[OK] Test réouverture (open/close/open)");
    }

    // Test 4 : une table sans tuples doit retourner null immédiatement
    static void testTableVide() {
        TableDisque td = new TableDisque(BASE + "test_vide");
        td.ecrire(new int[0][2]); // 0 tuples, 2 attributs

        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open();
        Tuple t = scan.next();
        scan.close();

        assert t == null : "Une table vide doit retourner null au premier next()";
        System.out.println("[OK] Test table vide");
    }

    // Test 5 : cas limite — exactement 1 bloc (blockSize = 4 tuples)
    static void testUnBlocExact() {
        TableDisque td = new TableDisque(BASE + "test_1bloc");
        td.ecrire(new int[][]{{1, 1}, {2, 2}, {3, 3}, {4, 4}});

        FullScanTableDisque scan = new FullScanTableDisque(td);
        scan.open();
        int count = 0;
        while (scan.next() != null) count++;
        scan.close();

        assert count == 4 : "Attendu 4 tuples, obtenu " + count;
        // 1 bloc de données + 1 lecture EOF pour détecter la fin
        assert scan.reads == 2 : "Attendu 2 blocs lus, obtenu " + scan.reads;
        System.out.println("[OK] Test 1 bloc exact (4 tuples, 2 lectures dont 1 EOF)");
    }

}