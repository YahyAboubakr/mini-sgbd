import java.util.Vector;

public class FullScanTableMemoire extends Instrumentation implements Operateur{
	
	private TableMemoire contenu;
	int compteur = 0;
	int taille = 0;
	int range = 5;
	long total;
	
	public FullScanTableMemoire(TableMemoire tbl) {
		super("FullScan"+Instrumentation.number++);
		this.contenu = tbl;
		this.taille = this.contenu.valeurs.size();
		this.total = 0;
	}
	

	public void open() {
		this.start();
		this.compteur = 0;
		this.tuplesProduits = 0;
		this.memoire = 0;
		this.stop();
	}
	
	public Tuple next() {
		this.start();
		if(this.compteur<this.taille) {
			Tuple t = this.contenu.valeurs.elementAt(this.compteur++); 
			this.produit(t);
			this.stop();
			return(t);
		}
		else {
			this.stop();
			return null;
		}
	}
	
	public void close(){
		this.total+=this.tuplesProduits;
	}
}
