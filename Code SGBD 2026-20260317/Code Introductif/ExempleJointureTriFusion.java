
public class ExempleJointureTriFusion {

    public static void main(String[] args) {

        // Table 1 : 2 attributs, valeurs 0-4, 6 lignes
        TableMemoire t1 = TableMemoire.randomize(2, 5, 6);
        // Table 2 : 2 attributs, valeurs 0-4, 6 lignes
        TableMemoire t2 = TableMemoire.randomize(2, 5, 6);

        // Affichage des deux tables
        System.out.println("=== Table 1 ===");
        Operateur scan1 = new FullScanTableMemoire(t1);
        scan1.open();
        Tuple t;
        while ((t = scan1.next()) != null) System.out.println(t);
        scan1.close();

        System.out.println("=== Table 2 ===");
        Operateur scan2 = new FullScanTableMemoire(t2);
        scan2.open();
        while ((t = scan2.next()) != null) System.out.println(t);
        scan2.close();

        // Jointure Tri-Fusion sur col 0 de T1 = col 0 de T2
        System.out.println("=== Jointure Tri-Fusion (T1.col0 = T2.col0) ===");
        JointureTriFusion jointure = new JointureTriFusion(
                new FullScanTableMemoire(t1),
                new FullScanTableMemoire(t2),
                0, 0);

        jointure.open();
        while ((t = jointure.next()) != null) System.out.println(t);
        jointure.close();

        System.out.println(jointure);
    }

}
