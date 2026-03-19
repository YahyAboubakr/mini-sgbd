import java.io.IOException;

public class TestJointureBoucleIndex {

    public static void main(String[] args) {
        String basePath = "/home/amadou/insa/sgbd/src/mini/Code SGBD 2026-20260317/";
        
        System.out.println("-----------------------TEST JOINTURE BOUCLE INDEX-----------------------\n");
        // Utilisation de table1 et table2
        TableDisque td1 = new TableDisque(basePath + "Table Disque et exemples/table1");
        TableDisque td2 = new TableDisque(basePath + "Table Disque et exemples/table2");
        
        // Charger les méta-données
        try {
            java.io.FileReader metaReader1 = new java.io.FileReader(basePath + "Table Disque et exemples/table1");
            td1.taille    = metaReader1.read();
            td1.tupleSize = metaReader1.read();
            metaReader1.close();
            
            java.io.FileReader metaReader2 = new java.io.FileReader(basePath + "Table Disque et exemples/table2");
            td2.taille    = metaReader2.read();
            td2.tupleSize = metaReader2.read();
            metaReader2.close();
        } catch(IOException e) {
            e.printStackTrace();
            return;
        }

        // Créer l'index sur table2, colonne 0
        String indexPath = basePath + "Table Disque et exemples/index_table2_col0";
        IndexHachage index2 = new IndexHachage(indexPath);
        try {
            index2.construire(td2, 0, 7); // 7 buckets, sur col 0
        } catch(IOException e) {
            e.printStackTrace();
            return;
        }
        
        System.out.println("\n--- Execution JointureBoucleIndex ---");
        System.out.println("Requête : SELECT * FROM table1, table2 WHERE table1.col0 = table2.col0");
        Operateur fst1_join = new FullScanTableDisque(td1);
        JointureBoucleIndex joinOp = new JointureBoucleIndex(fst1_join, td2, index2, 0);
        
        joinOp.open();
        int count = 0;
        Tuple t;
        while ((t = joinOp.next()) != null) {
            System.out.println(t);
            count++;
        }
        joinOp.close();
        System.out.println("Nombre de résultats : " + count);
    }
}
