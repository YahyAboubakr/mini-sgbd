import java.util.Vector;

public class TableMemoire{
	
	public Vector<Tuple> valeurs;
	public int nbAtt;
	
	public TableMemoire(int _nbAtt) {
		this.nbAtt = _nbAtt;
		this.valeurs = new Vector<Tuple>();
	}
	
	public static TableMemoire randomize(int tuplesize, int range, int tablesize) {
		TableMemoire contenu = new TableMemoire(tuplesize);
		for(int i=0;i<tablesize;i++) {
			Tuple t = new Tuple(tuplesize);
			for(int j=0;j<tuplesize;j++) {
				t.val[j]=(int)(Math.random()*range);
			}
			contenu.valeurs.add(t);
		}
		return contenu;
	}
}