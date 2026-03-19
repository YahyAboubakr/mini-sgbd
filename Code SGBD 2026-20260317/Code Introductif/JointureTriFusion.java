
import java.util.ArrayList;
import java.util.Collections;

public class JointureTriFusion extends Instrumentation implements Operateur {

    private Operateur op1;
    private Operateur op2;
    private int col1;
    private int col2;

    private ArrayList<Tuple> resultats;
    private int cursor;

    public JointureTriFusion(Operateur o1, Operateur o2, int c1, int c2) {
        this.op1 = o1;
        this.op2 = o2;
        this.col1 = c1;
        this.col2 = c2;
    }

    @Override
    public void open() {
        this.start();

        // Étape 1 : lire tous les tuples des deux sources
        ArrayList<Tuple> gauche = new ArrayList<Tuple>();
        ArrayList<Tuple> droite = new ArrayList<Tuple>();

        this.op1.open();
        Tuple t;
        while ((t = this.op1.next()) != null) gauche.add(t);
        this.op1.close();

        this.op2.open();
        while ((t = this.op2.next()) != null) droite.add(t);
        this.op2.close();

        // Étape 2 : trier chaque liste sur la colonne de jointure
        Collections.sort(gauche, (a, b) -> a.val[this.col1] - b.val[this.col1]);
        Collections.sort(droite, (a, b) -> a.val[this.col2] - b.val[this.col2]);

        // Étape 3 : fusion — on parcourt les deux listes triées
        this.resultats = new ArrayList<Tuple>();
        int i = 0;
        int j = 0;

        while (i < gauche.size() && j < droite.size()) {
            Tuple tg = gauche.get(i);
            Tuple td = droite.get(j);

            if (tg.val[this.col1] < td.val[this.col2]) {
                i++;
            } else if (tg.val[this.col1] > td.val[this.col2]) {
                j++;
            } else {
                // Valeur identique : on produit toutes les combinaisons
                int valeur = tg.val[this.col1];
                int jDebut = j;

                while (i < gauche.size() && gauche.get(i).val[this.col1] == valeur) {
                    j = jDebut;
                    while (j < droite.size() && droite.get(j).val[this.col2] == valeur) {
                        Tuple ret = new Tuple(tg.val.length + td.val.length);
                        for (int k = 0; k < tg.val.length; k++)
                            ret.val[k] = gauche.get(i).val[k];
                        for (int k = 0; k < td.val.length; k++)
                            ret.val[k + tg.val.length] = droite.get(j).val[k];
                        this.resultats.add(ret);
                        j++;
                    }
                    i++;
                }
            }
        }

        this.cursor = 0;
        this.stop();
    }

    @Override
    public Tuple next() {
        this.start();
        if (this.cursor < this.resultats.size()) {
            Tuple ret = this.resultats.get(this.cursor++);
            this.produit(ret);
            this.stop();
            return ret;
        }
        this.stop();
        return null;
    }

    @Override
    public void close() {
        // les sources sont déjà fermées dans open()
    }

    @Override
    public int estimateSize() {
        return Math.max(this.op1.estimateSize(), this.op2.estimateSize());
    }

}
