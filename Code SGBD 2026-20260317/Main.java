public class Main {

    public static void main(String[] args) {
        // Exemple d'utilisation
        String path = "/home/amadou/insa/sgbd/src/mini/Code SGBD 2026-20260317/Table Disque et exemples/";
        System.out.println("-----------------------T1-----------------------\n");
        String queryTab1 = "SELECT * FROM table1_small";
        ExecutionTree.executeQuery(queryTab1, path);
        System.out.println("\n-----------------------T2-----------------------\n");
        String queryTab2 = "SELECT * FROM table2_small";
        ExecutionTree.executeQuery(queryTab2, path);
        System.out.println("\n-----------------------JOIN-----------------------\n");
        String query = "SELECT * FROM table1_small, table2_small WHERE table1_small.0 = table2_small.0";
        ExecutionTree.executeQuery(query, path);

    }
}
