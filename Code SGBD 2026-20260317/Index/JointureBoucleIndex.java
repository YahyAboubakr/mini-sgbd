
public class JointureBoucleIndex extends Instrumentation implements Operateur {

    private Operateur op1;
    private TableDisque table2;
    private IndexHachageBase index2;
    private int col1; // colonne de jointure dans op1

    private Tuple t1;
    private IndexScanHachage currentScan2;

    public JointureBoucleIndex(Operateur op1, TableDisque table2, IndexHachageBase index2, int col1) {
        super("JointureBoucleIndex" + Instrumentation.number++);
        this.op1 = op1;
        this.table2 = table2;
        this.index2 = index2;
        this.col1 = col1;
    }

    @Override
    public void open() {
        this.start();
        this.op1.open();
        this.t1 = null;
        this.currentScan2 = null;
        this.stop();
    }

    @Override
    public Tuple next() {
        this.start();
        while (true) {
            // Si on n'a pas de scan en cours, on lit le prochain t1
            if (this.currentScan2 == null) {
                this.t1 = this.op1.next();
                if (this.t1 == null) {
                    this.stop();
                    return null; // Fin de la jointure
                }
                // Initialiser un nouveau scan sur table2 via l'index
                this.currentScan2 = new IndexScanHachage(this.index2, this.table2, this.t1.val[this.col1]);
                this.currentScan2.open();
            }

            // Lire le prochain tuple correspondant dans table2
            Tuple t2 = this.currentScan2.next();
            if (t2 != null) {
                // On a trouvé une correspondance
                Tuple ret = new Tuple(this.t1.val.length + t2.val.length);
                for (int i = 0; i < this.t1.val.length; i++) {
                    ret.val[i] = this.t1.val[i];
                }
                for (int i = 0; i < t2.val.length; i++) {
                    ret.val[i + this.t1.val.length] = t2.val[i];
                }
                this.produit(ret);
                this.stop();
                return ret;
            } else {
                // Fin des correspondances pour ce t1
                this.currentScan2.close();
                this.currentScan2 = null;
                // on continue la boucle pour chercher le prochain t1
            }
        }
    }

    @Override
    public void close() {
        this.op1.close();
        if (this.currentScan2 != null) {
            this.currentScan2.close();
        }
    }

    @Override
    public int estimateSize() {
        return Math.max(this.op1.estimateSize(), this.table2.taille);
    }
}
