
public class Instrumentation {
	
	int tuplesProduits = 0;
	int memoire = 0;
	String opName;
	static int number = 0;
	int time;
	private long start;
	private long stop;
	
	public Instrumentation() {
		this("operateur"+Instrumentation.number++);
	}
	
	public void reset() {
		this.tuplesProduits = 0;
		this.memoire = 0;
		this.time = 0;
	}
	
	public Instrumentation(String _opName) {
		//this.tuplesProduits = 0;
		//this.memoire = 0;
		this.time = 0;
		this.start = 0;
		this.stop = 0;
		this.opName = _opName;
	}
	
	public void start() {
		this.start = System.currentTimeMillis();
	}
	
	public void stop() {
		this.stop = System.currentTimeMillis();
		this.time+=(this.stop-this.start);
		
	}
	
	public void produit(Tuple t) {
		this.tuplesProduits++;
		this.memoire+=t.size;
	}
	
	public String toString() {
		return (this.opName+" -- tuples produits : "+this.tuplesProduits+" -- mémoire utilisée : "+this.memoire+" -- Time : "+this.time);
	}

}
