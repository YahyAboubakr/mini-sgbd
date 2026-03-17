
public class Project extends Instrumentation implements Operateur{

	
	private Operateur in;
	private int[] cols;
	
	public Project(Operateur _in, int[] _cols) {
		super("Project"+Instrumentation.number++);
		this.in = _in;
		this.cols = _cols;
	}

	@Override
	public void open() {
		this.start();
		this.in.open();	
		this.tuplesProduits = 0;
		this.memoire = 0;
		this.stop();
	}

	@Override
	public Tuple next() {
		this.start();
		Tuple temp = null;
		Tuple ret = new Tuple(this.cols.length);
		if((temp=this.in.next())==null) {
			this.stop();
			return null;
		}
		else{
			for(int i=0;i<this.cols.length;i++)
				ret.val[i] = temp.val[this.cols[i]];
		}
		this.produit(ret);
		this.stop();
		return ret;
	}

	@Override
	public void close() {
		this.in.close();
	}

}
