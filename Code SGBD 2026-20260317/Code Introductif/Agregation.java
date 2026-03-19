
public class Agregation extends Instrumentation implements Operateur {

    public static final int SUM = 0;
    public static final int AVG = 1;
    public static final int MIN = 2;
    public static final int MAX = 3;

    private Operateur dataSource;
    private int colonne;
    private int typeOperation;

    private Tuple resultat;
    private double moyenne;
    private boolean dejaRetourne;

    public Agregation(Operateur in, int col, int op) {
        this.dataSource = in;
        this.colonne = col;
        this.typeOperation = op;
    }

    public double getMoyenne() {
        return this.moyenne;
    }

    @Override
    public void open() {
        this.start();
        this.dataSource.open();

        // On lit tous les tuples et on calcule le résultat
        int somme = 0;
        int count = 0;
        int min = Integer.MAX_VALUE;
        int max = Integer.MIN_VALUE;

        Tuple t;
        while ((t = this.dataSource.next()) != null) {
            int val = t.val[this.colonne];
            somme += val;
            count++;
            if (val < min) min = val;
            if (val > max) max = val;
        }

        // Calcul de la moyenne en double
        this.moyenne = (count == 0) ? 0.0 : (double) somme / count;

        // On stocke le résultat entier dans le tuple (sauf AVG qui utilise getMoyenne())
        this.resultat = new Tuple(1);
        if (count == 0) {
            this.resultat.val[0] = 0;
        } else if (this.typeOperation == Agregation.SUM) {
            this.resultat.val[0] = somme;
        } else if (this.typeOperation == Agregation.AVG) {
            this.resultat.val[0] = (int) this.moyenne; // partie entière dans le tuple
        } else if (this.typeOperation == Agregation.MIN) {
            this.resultat.val[0] = min;
        } else if (this.typeOperation == Agregation.MAX) {
            this.resultat.val[0] = max;
        }

        this.dejaRetourne = false;
        this.stop();
    }

    @Override
    public Tuple next() {
        this.start();
        if (!this.dejaRetourne) {
            this.dejaRetourne = true;
            this.produit(this.resultat);
            this.stop();
            return this.resultat;
        }
        this.stop();
        return null;
    }

    @Override
    public void close() {
        this.start();
        this.dataSource.close();
        this.stop();
    }

    @Override
    public int estimateSize() {
        return this.dataSource.estimateSize();
    }

}
