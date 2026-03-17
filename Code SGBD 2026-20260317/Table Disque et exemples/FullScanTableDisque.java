import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;

public class FullScanTableDisque implements Operateur {
	
	private String filePath= "";
	private int taille = 0;
	private int tupleSize = 0;
	private int range = 100;
	private int blockSize = 4;
	private int blockCursor = 0;
	private int memorySize = 3; // nombre de blocs
	private Tuple[][] cache = new Tuple[memorySize][blockSize];
	private Queue<Integer> q = new LinkedList<Integer>(); // pour g�rer les blocs en m�moire
	private int currentMemoryBlock=0;
	private FileWriter myWriter;
	private FileReader myReader;
	private Boolean start = true;
	public int reads = 0;

	public void open() {
		this.openFile();
		this.start = true;
		q = new LinkedList<Integer>();
		cache = new Tuple[memorySize][blockSize];
	}
	
	public void close(){
		try {
			this.myReader.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void setFilePath(String fp) {
		this.filePath = fp;
	}

	
	public Tuple next() {
		if(this.start || this.blockCursor == this.blockSize) {
			this.readNextBlock();
			this.blockCursor = 0;
			this.start = false;
		}
		return(this.cache[this.currentMemoryBlock][this.blockCursor++]);			
	}
	
	public void openFile() {
		try {
			this.myReader = new FileReader(filePath);
			this.taille = this.myReader.read(); // header : table size puis tuplesize
			this.tupleSize = this.myReader.read(); // header : table size puis tuplesize			
		}catch (IOException e) {
		      System.out.println("Erreur de lecture");
		      e.printStackTrace();
		}
			
	}
	
	
	public void readNextBlock() {
		int i=0;
		try {
		
		if(q.size()<this.memorySize) {
			this.currentMemoryBlock = q.size();
			q.add(q.size());
		}else {
			int lastBlock = q.remove(); 
			this.currentMemoryBlock = lastBlock;
			q.add(lastBlock);
		}
		for(i=0;i<this.blockSize;i++) {
			Tuple t = new Tuple(this.tupleSize);
			for(int j=0;j<this.tupleSize;j++) {
				t.val[j] = this.myReader.read();
			}
			if(t.val[0] != -1)
				this.cache[this.currentMemoryBlock][i] = t;
			else
				this.cache[this.currentMemoryBlock][i] = null;
		}
		//System.out.println ("Block read : "+this.currentMemoryBlock);
		this.reads++;
		}catch (IOException e) {
				System.err.println("Erreur de lecture.");
		}
	}
	
	public void randomize(int tuplesize, int tablesize) {
		try {
	    this.myWriter = new FileWriter(filePath); 
		this.myWriter.write(tablesize); // header : table size puis tuplesize
		this.myWriter.write(tuplesize); // header : table size puis tuplesize
		for(int i=0;i<tablesize;i++) {
			Tuple t = new Tuple(tuplesize);
			for(int j=0;j<tuplesize;j++) {
				t.val[j]=(int)(Math.random()*this.range);
				this.myWriter.write(t.val[j]);
			}
		}
        myWriter.close();
        System.out.println("Table générée");
		this.taille = tablesize;
		} catch (IOException e) {
		      System.out.println("Erreur de création ou d'écriture de fichier.");
		      e.printStackTrace();
		}
	}
}

