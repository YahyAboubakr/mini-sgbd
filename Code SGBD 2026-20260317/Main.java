public class Main {

    public static void main(String[] args) {

        String path = "/home/amadou/insa/sgbd/src/mini/Code SGBD 2026-20260317/";
        System.out.println("-----------------------T1-----------------------\n");
        String queryTab1 = "SELECT * FROM table1";
        ExecutionTree.executeQuery(queryTab1, path);
        System.out.println("\n-----------------------T2-----------------------\n");
        String queryTab2 = "SELECT * FROM table2";
        ExecutionTree.executeQuery(queryTab2, path);
        System.out.println("\n-----------------------JOIN-----------------------\n");
        String query = "SELECT * FROM table1, table2 WHERE table1.0 > table2.1";
        ExecutionTree.executeQuery(query, path);

        System.out.println("\n-----------------------TEST JOINTURE TRI-FUSION-----------------------\n");
        // Création de deux grandes tables pour dépasser la limite de 100 Ko en mémoire 
        // L'estimation du HashJoin prend ~100 octets par tuple. Avec 1200 tuples on dépasse 100 Ko.
        TableDisque td3 = new TableDisque(path + "table3");
        td3.randomize(2, 1200); 
        TableDisque td4 = new TableDisque(path + "table4");
        td4.randomize(2, 1200);

        String queryBig = "SELECT * FROM table3, table1 WHERE table3.0 != table1.0";
        // ExecutionTree.executeQuery(queryBig, path);

        System.out.println("\n-----------------------TEST JOINTURE DOUBLE BOUCLE (DBI)-----------------------\n");
        // Création de deux très petites tables pour déclencher la Double Boucle Imbriquée (produit cartésien < 100)
        TableDisque td5 = new TableDisque(path + "table5");
        td5.randomize(2, 5); 
        TableDisque td6 = new TableDisque(path + "table6");
        td6.randomize(2, 5);

        String queryMicro = "SELECT * FROM table5";
        ExecutionTree.executeQuery(queryMicro, path);

        String queryMicro2 = "SELECT * FROM table6";
        ExecutionTree.executeQuery(queryMicro2, path);

        System.out.println("\n-----------------------TEST JOINTURE DOUBLE BOUCLE (DBI)-----------------------\n");
        
        String queryMicro3 = "SELECT * FROM table5, table6 WHERE table5.0 = table6.1";
        ExecutionTree.executeQuery(queryMicro3, path);

    }
}
