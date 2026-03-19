
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class FullScanTableDisque implements Operateur {

    private TableDisque table;
    private int tupleSize;
    private int blockSize = 4;
    private int blockCursor = 0;
    private int memorySize = 3; // nombre de blocs
    private Tuple[][] cache = new Tuple[memorySize][blockSize];
    private Queue<Integer> q = new LinkedList<Integer>();
    private int currentMemoryBlock = 0;
    private FileReader myReader;
    private Boolean start = true;
    public int reads = 0;

    public FullScanTableDisque(TableDisque table) {
        this.table = table;
    }

    @Override
    public void open() {
        try {
            this.myReader = new FileReader(this.table.filePath);
            this.myReader.read(); // header : taille de la table
            this.tupleSize = this.myReader.read(); // header : taille d'un tuple
        } catch (IOException e) {
            System.out.println("Erreur de lecture");
            e.printStackTrace();
        }
        this.start = true;
        this.q = new LinkedList<Integer>();
        this.cache = new Tuple[memorySize][blockSize];
    }

    @Override
    public Tuple next() {
        if (this.tupleSize == 0) return null;
        if (this.start || this.blockCursor == this.blockSize) {
            this.readNextBlock();
            this.blockCursor = 0;
            this.start = false;
        }
        return this.cache[this.currentMemoryBlock][this.blockCursor++];
    }

    @Override
    public void close() {
        try {
            this.myReader.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void readNextBlock() {
        try {
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
                for (int j = 0; j < this.tupleSize; j++) {
                    t.val[j] = this.myReader.read();
                }
                if (t.val[0] != -1)
                    this.cache[this.currentMemoryBlock][i] = t;
                else
                    this.cache[this.currentMemoryBlock][i] = null;
            }
            this.reads++;
        } catch (IOException e) {
            System.err.println("Erreur de lecture.");
        }
    }

    @Override
    public int estimateSize() {
        try {
            java.io.FileReader reader = new java.io.FileReader(this.table.filePath);
            int size = reader.read();
            reader.close();
            return size;
        } catch (IOException e) {
            return -1;
        }
    }

}
