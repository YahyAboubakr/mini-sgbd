import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Jointure par hachage (hash join).
 *
 * Opérateur bloquant en deux phases :
 *   1. Build  : lit toute op1 et la stocke dans une table de hachage (clé = col1).
 *   2. Probe  : parcourt op2 tuple par tuple et sonde la table de hachage (clé = col2).
 *
 * Hypothèse : op1 (la plus petite table) tient en mémoire.
 * Les tuples résultats sont [tuple_op1 | tuple_op2].
 */
public class JointureHachage extends Instrumentation implements Operateur {

    private Operateur op1;
    private Operateur op2;
    private int col1; // colonne de jointure dans op1
    private int col2; // colonne de jointure dans op2

    private ArrayList<Tuple> resultats;
    private int cursor;

    public JointureHachage(Operateur o1, Operateur o2, int c1, int c2) {
        super("JointureHachage" + Instrumentation.number++);
        this.op1 = o1;
        this.op2 = o2;
        this.col1 = c1;
        this.col2 = c2;
    }

    @Override
    public void open() {
        this.start();

        // --- Phase 1 : Build ---
        // Lire tous les tuples de op1 et les indexer par la valeur de col1
        HashMap<Integer, List<Tuple>> tableHachage = new HashMap<>();

        this.op1.open();
        Tuple t;
        while ((t = this.op1.next()) != null) {
            int cle = t.val[this.col1];
            tableHachage.computeIfAbsent(cle, k -> new ArrayList<>()).add(t);
        }
        this.op1.close();

        // --- Phase 2 : Probe ---
        // Parcourir op2 et sonder la table de hachage
        this.resultats = new ArrayList<>();

        this.op2.open();
        while ((t = this.op2.next()) != null) {
            int cle = t.val[this.col2];
            List<Tuple> correspondants = tableHachage.get(cle);
            if (correspondants != null) {
                for (Tuple t1 : correspondants) {
                    // Concaténer [t1 | t2]
                    Tuple ret = new Tuple(t1.val.length + t.val.length);
                    for (int i = 0; i < t1.val.length; i++)
                        ret.val[i] = t1.val[i];
                    for (int i = 0; i < t.val.length; i++)
                        ret.val[i + t1.val.length] = t.val[i];
                    this.resultats.add(ret);
                }
            }
        }
        this.op2.close();

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
        // Les sources sont déjà fermées dans open()
    }
}
