
public class Restrict extends Instrumentation implements Operateur {
	
	public static final int EGAL     = 0; // col = val
	public static final int INFEGAL  = 1; // col <= val
	public static final int SUPERIEUR = 2; // col > val
	public static final int INFERIEUR = 3; // col < val
	public static final int SUPEGAL  = 4; // col >= val
	
	
	private Operateur dataSource;
	private int colonneATester;
	private int valeurATester;
	private int typeOperationTest;
	
	public Restrict(Operateur in, int col, int val, int op) {
		this.dataSource = in;
		this.colonneATester = col;
		this.valeurATester = val;
		this.typeOperationTest = op;
	}

	@Override
	public void open() {
		this.start(); // instrumentation
		this.dataSource.open();
		this.stop(); // instrumentation
	}

	@Override
	public Tuple next() {
		this.start();
		Tuple retour = null;
		if(this.typeOperationTest == Restrict.EGAL) {
			while((retour = this.dataSource.next())!=null) {
				if(retour.val[this.colonneATester] == this.valeurATester) {
					this.produit(retour); // instrumentation
					this.stop();
					return retour;
				}
			}
		}
		if(this.typeOperationTest == Restrict.INFEGAL) {
			while((retour = this.dataSource.next())!=null) {
				if(retour.val[this.colonneATester] <= this.valeurATester) {
					this.produit(retour);
					this.stop();
					return retour;
				}
			}
		}
		if(this.typeOperationTest == Restrict.SUPERIEUR) {
			while((retour = this.dataSource.next())!=null) {
				if(retour.val[this.colonneATester] > this.valeurATester) {
					this.produit(retour);
					this.stop();
					return retour;
				}
			}
		}
		if(this.typeOperationTest == Restrict.INFERIEUR) {
			while((retour = this.dataSource.next())!=null) {
				if(retour.val[this.colonneATester] < this.valeurATester) {
					this.produit(retour);
					this.stop();
					return retour;
				}
			}
		}
		if(this.typeOperationTest == Restrict.SUPEGAL) {
			while((retour = this.dataSource.next())!=null) {
				if(retour.val[this.colonneATester] >= this.valeurATester) {
					this.produit(retour);
					this.stop();
					return retour;
				}
			}
		}
		return null;
	}

	@Override
	public void close() {
		this.start();
		this.dataSource.close();
		this.stop();
	}

}
