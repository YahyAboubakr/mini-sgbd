
public class ExempleFSTM {

	public static void main(String[] args) {
		Tuple t;
		TableMemoire tm = TableMemoire.randomize(3, 10, 5);
		System.out.println("Début");
		Operateur requete1 = new FullScanTableMemoire(tm);
		requete1.open();
		while((t =requete1.next())!=null) {
			System.out.println(t);
		}
		requete1.close();
		System.out.println("Fin");
	}
}
