
public class ExempleAgregation {

    public static void main(String[] args) {

        // Table aléatoire : 2 attributs, valeurs entre 0 et 9, 5 lignes
        TableMemoire tm = TableMemoire.randomize(2, 10, 5);

        // Affichage de la table
        System.out.println("=== Contenu de la table ===");
        Operateur scan = new FullScanTableMemoire(tm);
        scan.open();
        Tuple t;
        while ((t = scan.next()) != null) {
            System.out.println(t);
        }
        scan.close();

        // SUM sur la colonne 0
        Operateur somme = new Agregation(new FullScanTableMemoire(tm), 0, Agregation.SUM);
        somme.open();
        System.out.println("SUM colonne 0 : " + somme.next().val[0]);
        somme.close();

        // AVG sur la colonne 0
        Agregation moyenne = new Agregation(new FullScanTableMemoire(tm), 0, Agregation.AVG);
        moyenne.open();
        moyenne.next();
        System.out.printf("AVG colonne 0 : %.2f%n", moyenne.getMoyenne());
        moyenne.close();

        // MIN sur la colonne 0
        Operateur minimum = new Agregation(new FullScanTableMemoire(tm), 0, Agregation.MIN);
        minimum.open();
        System.out.println("MIN colonne 0 : " + minimum.next().val[0]);
        minimum.close();

        // MAX sur la colonne 0
        Operateur maximum = new Agregation(new FullScanTableMemoire(tm), 0, Agregation.MAX);
        maximum.open();
        System.out.println("MAX colonne 0 : " + maximum.next().val[0]);
        maximum.close();

    }

}
