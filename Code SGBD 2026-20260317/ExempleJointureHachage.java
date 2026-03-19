/**
 * Exemple de test pour JointureHachage sur table1 et table2 (disque).
 *
 * table1 et table2 : 25 tuples chacune, 4 attributs, valeurs aléatoires 0-4.
 *
 * On compare le résultat de JointureHachage avec JointureTriFusion sur la même
 * jointure pour valider la cohérence (même nombre de tuples produits).
 */
public class ExempleJointureHachage {

    static final String PATH = "/home/jules/Documents/4A-Apprentis/SGBD/mini-sgbd/Code SGBD 2026-20260317/Table Disque et exemples/";
    static final String FICHIER_T1 = PATH + "table1";
    static final String FICHIER_T2 = PATH + "table2";

    public static void main(String[] args) {

        // --- Affichage des deux tables ---
        System.out.println("=== Table 1 ===");
        afficherTable(FICHIER_T1);

        System.out.println("\n=== Table 2 ===");
        afficherTable(FICHIER_T2);

        // --- Jointure par Hachage sur T1.col0 = T2.col0 ---
        System.out.println("\n=== Jointure par Hachage (T1.col0 = T2.col0) ===");
        JointureHachage jh = new JointureHachage(
                new FullScanTableDisque(new TableDisque(FICHIER_T1)),
                new FullScanTableDisque(new TableDisque(FICHIER_T2)),
                0, 0);

        jh.open();
        Tuple t;
        int countJH = 0;
        while ((t = jh.next()) != null) {
            System.out.println(t);
            countJH++;
        }
        jh.close();
        System.out.println("Tuples joints (Hachage) : " + countJH);
        System.out.println(jh);

        // --- Vérification croisée avec JointureTriFusion ---
        System.out.println("\n=== Vérification croisée : JointureTriFusion (même jointure) ===");
        JointureTriFusion jtf = new JointureTriFusion(
                new FullScanTableDisque(new TableDisque(FICHIER_T1)),
                new FullScanTableDisque(new TableDisque(FICHIER_T2)),
                0, 0);

        jtf.open();
        int countJTF = 0;
        while ((t = jtf.next()) != null) {
            System.out.println(t);
            countJTF++;
        }
        jtf.close();
        System.out.println("Tuples joints (Tri-Fusion) : " + countJTF);
        System.out.println(jtf);

        // --- Validation ---
        System.out.println("\n=== Résultat ===");
        if (countJH == countJTF) {
            System.out.println("[OK] Les deux algorithmes produisent le même nombre de tuples (" + countJH + ")");
        } else {
            System.out.println("[ERREUR] Hachage=" + countJH + " vs Tri-Fusion=" + countJTF);
        }
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
