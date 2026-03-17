
/**
 * Exemple de test pour JointureTriFusion sur table1 et table2 (disque).
 *
 * table1 et table2 : 25 tuples chacune, 4 attributs, valeurs aléatoires 0-4.
 */
public class ExempleJointureTriFusion {

    static final String PATH = "/home/jules/Documents/4A-Apprentis/SGBD/mini-sgbd/Code SGBD 2026-20260317/Table Disque et exemples/";
    static final String FICHIER_T1 = PATH + "table1";
    static final String FICHIER_T2 = PATH + "table2";

    public static void main(String[] args) {

        // Affichage des deux tables
        System.out.println("=== Table 1 ===");
        afficherTable(FICHIER_T1);

        System.out.println("\n=== Table 2 ===");
        afficherTable(FICHIER_T2);

        // Jointure Tri-Fusion sur col 0 de T1 = col 0 de T2
        System.out.println("\n=== Jointure Tri-Fusion (T1.col0 = T2.col0) ===");
        JointureTriFusion jointure = new JointureTriFusion(
                new FullScanTableDisque(new TableDisque(FICHIER_T1)),
                new FullScanTableDisque(new TableDisque(FICHIER_T2)),
                0, 0);

        jointure.open();
        Tuple t;
        int count = 0;
        while ((t = jointure.next()) != null) {
            System.out.println(t);
            count++;
        }
        jointure.close();

        System.out.println("Tuples joints : " + count);
        System.out.println(jointure);
    }

    /** Affiche tous les tuples d'une table disque. */
    private static void afficherTable(String chemin) {
        FullScanTableDisque scan = new FullScanTableDisque(new TableDisque(chemin));
        scan.open();
        Tuple t;
        while ((t = scan.next()) != null) System.out.println(t);
        scan.close();
    }

}
