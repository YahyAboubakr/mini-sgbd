/**
 * Exemple de test pour JointureHachage.
 *
 * Données connues (pas aléatoires) pour pouvoir vérifier les résultats à la main.
 * On compare ensuite avec JointureTriFusion pour valider la cohérence.
 *
 * Table 1 (3 tuples, 2 attributs) :   Table 2 (4 tuples, 2 attributs) :
 *   col0  col1                           col0  col1
 *    1     10                              1     100
 *    2     20                              2     200
 *    3     30                              2     201
 *                                          4     400
 *
 * Jointure sur T1.col0 = T2.col0 — résultats attendus :
 *   1  10  1  100
 *   2  20  2  200
 *   2  20  2  201
 */
public class ExempleJointureHachage {

    static final String PATH = "/home/jules/Documents/4A-Apprentis/SGBD/mini-sgbd/Code SGBD 2026-20260317/Table Disque et exemples/";
    static final String FICHIER_T1 = PATH + "test_jh_t1";
    static final String FICHIER_T2 = PATH + "test_jh_t2";

    public static void main(String[] args) {

        // --- Création des tables sur disque ---
        int[][] donneesT1 = {
            {1, 10},
            {2, 20},
            {3, 30}
        };
        int[][] donneesT2 = {
            {1, 100},
            {2, 200},
            {2, 201},
            {4, 400}
        };

        TableDisque td1 = new TableDisque(FICHIER_T1);
        td1.ecrire(donneesT1);

        TableDisque td2 = new TableDisque(FICHIER_T2);
        td2.ecrire(donneesT2);

        // --- Affichage des deux tables ---
        System.out.println("=== Table 1 ===");
        afficherTable(FICHIER_T1);

        System.out.println("=== Table 2 ===");
        afficherTable(FICHIER_T2);

        // --- Jointure par Hachage sur T1.col0 = T2.col0 ---
        System.out.println("=== Jointure par Hachage (T1.col0 = T2.col0) ===");
        JointureHachage jh = new JointureHachage(
                new FullScanTableDisque(new TableDisque(FICHIER_T1)),
                new FullScanTableDisque(new TableDisque(FICHIER_T2)),
                0, 0);

        jh.open();
        Tuple t;
        int count = 0;
        while ((t = jh.next()) != null) {
            System.out.println(t);
            count++;
        }
        jh.close();
        System.out.println("Tuples joints : " + count + " (attendu : 3)");
        System.out.println(jh);

        // --- Vérification croisée avec JointureTriFusion ---
        System.out.println("\n=== Vérification : JointureTriFusion (même jointure) ===");
        JointureTriFusion jtf = new JointureTriFusion(
                new FullScanTableDisque(new TableDisque(FICHIER_T1)),
                new FullScanTableDisque(new TableDisque(FICHIER_T2)),
                0, 0);

        jtf.open();
        count = 0;
        while ((t = jtf.next()) != null) {
            System.out.println(t);
            count++;
        }
        jtf.close();
        System.out.println("Tuples joints : " + count + " (attendu : 3)");
        System.out.println(jtf);
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
