public class TestPushdown {
    public static void main(String[] args) {
        String PATH = "/home/amadou/insa/sgbd/src/mini/Code SGBD 2026-20260317/Table Disque et exemples/";
        
        // 1. Création de deux petites tables avec des valeurs connues
        TableDisque td1 = new TableDisque(PATH + "test_pushdown1");
        td1.ecrire(new int[][]{
            {1, 10}, 
            {2, 20}, 
            {3, 30}, 
            {4, 40}, 
            {5, 50}
        });
        
        TableDisque td2 = new TableDisque(PATH + "test_pushdown2");
        td2.ecrire(new int[][]{
            {20, 100}, 
            {30, 200}, 
            {30, 250}, 
            {50, 300}, 
            {60, 400}
        });
        
        // 2. Affichage étape par étape pour vérification manuelle
        
        System.out.println("==========================================================");
        System.out.println("1. PREMIÈRE TABLE COMPLETE (test_pushdown1)");
        System.out.println("==========================================================");
        ExecutionTree.executeQuery("SELECT * FROM test_pushdown1", PATH);
        
        System.out.println("\n==========================================================");
        System.out.println("2. PREMIÈRE TABLE FILTRÉE SEULE (test_pushdown1.0 > 2)");
        System.out.println("==========================================================");
        ExecutionTree.executeQuery("SELECT * FROM test_pushdown1 WHERE test_pushdown1.0 > 2", PATH);
        
        System.out.println("\n==========================================================");
        System.out.println("3. DEUXIÈME TABLE COMPLETE (test_pushdown2)");
        System.out.println("==========================================================");
        ExecutionTree.executeQuery("SELECT * FROM test_pushdown2", PATH);
        
        System.out.println("\n==========================================================");
        System.out.println("4. DEUXIÈME TABLE FILTRÉE SEULE (test_pushdown2.0 < 40)");
        System.out.println("==========================================================");
        ExecutionTree.executeQuery("SELECT * FROM test_pushdown2 WHERE test_pushdown2.0 < 40", PATH);
        
        System.out.println("\n==========================================================");
        System.out.println("5. JOIN FINAL AVEC PUSHDOWN (t1.1 = t2.0)");
        System.out.println("La requête finale applique tout en même temps !");
        System.out.println("==========================================================");
        ExecutionTree.executeQuery("SELECT * FROM test_pushdown1, test_pushdown2 WHERE test_pushdown1.0 > 2 AND test_pushdown2.0 < 40 AND test_pushdown1.1 = test_pushdown2.0", PATH);
    }
}
