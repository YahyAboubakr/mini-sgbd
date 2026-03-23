
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class FullScanTableDisque extends Instrumentation implements Operateur {

    private TableDisque table;
    private int tupleSize;
    private int blockSize = 4;
    private int blockCursor = blockSize; // force la lecture du premier bloc
    private int memorySize = 3;
    private Tuple[][] cache = new Tuple[memorySize][blockSize];
    private Queue<Integer> q = new LinkedList<Integer>();
    private int currentMemoryBlock = 0;
    private FileReader myReader;
    int compteur = 0;
    int taille = 0;
    long total;
    public int reads = 0;

    public FullScanTableDisque(TableDisque table) {
        super("FullScan" + Instrumentation.number++);
        this.table = table;
        this.total = 0;
        try {
            java.io.FileReader reader = new java.io.FileReader(table.filePath);
            this.taille = reader.read();
            reader.close();
        } catch (IOException e) {
            this.taille = 0;
        }
    }

    public TableDisque getTable() {
        return this.table;
    }

    @Override
    public void open() {
        this.start();
        try {
            this.myReader = new FileReader(this.table.filePath);
            this.taille = this.myReader.read();
            this.tupleSize = this.myReader.read();
        } catch (IOException e) {
            System.out.println("Erreur de lecture");
            e.printStackTrace();
        }
        this.compteur = 0;
        this.tuplesProduits = 0;
        this.memoire = 0;
        this.reads = 0;
        this.blockCursor = this.blockSize;
        this.q = new LinkedList<Integer>();
        this.cache = new Tuple[memorySize][blockSize];
        this.stop();
    }

    @Override
    public Tuple next() {
        this.start();
        if (this.compteur < this.taille) {
            Tuple t = this.lireTuple();
            if (t == null) {
                // bloc partiel : fin réelle des données avant taille annoncée
                this.stop();
                return null;
            }
            this.compteur++;
            this.produit(t);
            this.stop();
            return t;
        } else {
            this.stop();
            return null;
        }
    }

    @Override
    public void close() {
        this.total += this.tuplesProduits;
        try {
            this.myReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private Tuple lireTuple() {
        if (this.blockCursor == this.blockSize) {
            this.readNextBlock();
            this.blockCursor = 0;
        }
        return this.cache[this.currentMemoryBlock][this.blockCursor++];
    }

    private void readNextBlock() {
        try {

            //gère la mémoire
            if (q.size() < this.memorySize) {
                this.currentMemoryBlock = q.size();
                q.add(q.size());
            } else {
                int lastBlock = q.remove();
                this.currentMemoryBlock = lastBlock;
                q.add(lastBlock);
            }


            for (int i = 0; i < this.blockSize; i++) {
                Tuple t = new Tuple(this.tupleSize);
                //lit chaque élément du tuple
                for (int j = 0; j < this.tupleSize; j++) {
                    t.val[j] = this.myReader.read();
                }
                //read renvoi -1 à la fin,
                this.cache[this.currentMemoryBlock][i] = (t.val[0] != -1) ? t : null;
            }
            this.reads++;
        } catch (IOException e) {
            System.err.println("Erreur de lecture.");
        }
    }

    @Override
    public int estimateSize() {
        return this.taille;
    }
}
