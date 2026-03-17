
public class DBI extends Instrumentation implements Operateur {

	Operateur op1;
	Operateur op2;
	int col1;
	int col2;
	boolean nouveauTour;
	Tuple t1;
	Tuple t2;
	
	public DBI(Operateur o1, Operateur o2, int c1, int c2) {
		super("DBI"+Instrumentation.number++);
		this.op1 = o1;
		this.op2 = o2;
		this.col1 = c1;
		this.col2 = c2;
	}
	
	public void open() {
		this.start();
		this.op1.open();
		this.nouveauTour = true;
		this.t1=null;
		this.t2=null;
		this.stop();
	}
	
	public Tuple next() {
		this.start();
		if(nouveauTour) {
			while((t1=this.op1.next())!=null) {
				this.op2.open();
				nouveauTour = false;
				while((t2=this.op2.next())!=null) {
					if(t1.val[this.col1]==t2.val[this.col2]) {
						Tuple ret = new Tuple(t1.val.length+t2.val.length);
						for(int i=0;i<t1.val.length;i++)
							ret.val[i]=t1.val[i];
						for(int i=0;i<t2.val.length;i++)
							ret.val[i+t1.val.length]=t2.val[i];
						this.produit(ret);
						this.stop();
						return ret;
					}
					// sinon on continue la boucle	
				}
				nouveauTour = true;
			}
			this.stop();
			return null;
		}
		// pas nouveau tour
		else {
			while((t2=this.op2.next())!=null) {
				if(t1.val[this.col1]==t2.val[this.col2]) {
					Tuple ret = new Tuple(t1.val.length+t2.val.length);
					for(int i=0;i<t1.val.length;i++)
						ret.val[i]=t1.val[i];
					for(int i=0;i<t2.val.length;i++)
						ret.val[i+t1.val.length]=t2.val[i];
					this.produit(ret);
					this.stop();
					return ret;
				}
				// sinon on continue la boucle	
			}
			nouveauTour = true;
			this.stop();
			return this.next();
		}
	}
	
	public void close(){
		this.op1.close();
		this.op2.close();
	}
}
