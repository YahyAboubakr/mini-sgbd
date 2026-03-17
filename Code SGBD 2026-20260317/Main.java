import java.util.List;

public class Main {

    public static void main(String[] args) {
        // String query = "SELECT nom, age FROM utilisateurs WHERE age > 18 AND actif = 1 OR score < 50";
        // Parseur parseur = new Parseur(query);
        // ResultatRequete resultat = parseur.stringAnalyser();

        // String[] tables = parseur.tables(resultat.tables());
        // String[] champs = parseur.champs(resultat.champs());
        // String[] conditions = parseur.conditions(resultat.conditions());

        // System.out.println("Tables: " + String.join(",", tables));
        // System.out.println("Champs: " + String.join(", ", champs));
        // System.out.println("Conditions: " + String.join(", ", conditions));

        // System.out.println("\n--- Analyse détaillée du WHERE ---");
        // List<Condition> conditionsExtraites = parseur.whereParseur(resultat.conditions());
        
        // for (Condition cond : conditionsExtraites) {
        //     System.out.println(cond);
        // }

        String path = "/home/amadou/insa/sgbd/src/mini/Code SGBD 2026-20260317/Table Disque et exemples/";
        String query = "SELECT * FROM table1 WHERE 0 = 1";
        Parseur parseur = new Parseur(query);

		ResultatRequete resultat = parseur.stringAnalyser();

		String[] tables = parseur.tables(resultat.tables());
		String[] champs = parseur.champs(resultat.champs());
		List<Condition> conditions = parseur.whereParseur(resultat.conditions());
		
		// Exécuter la requête en mode pipelinage
		QueryExecutor executor = new QueryExecutor(path);
		QueryExecutor.ResultIterator iterator = executor.execute(tables, champs, conditions);
		
		// Afficher les résultats
		System.out.println("\n--- Résultats de la requête ---");
		System.out.println("Champs sélectionnés: " + String.join(", ", iterator.getSelectedFields()));
		System.out.println();
		
		int count = 0;
		while (iterator.hasNext()) {
			QueryExecutor.TupleResultat t = iterator.next();
			System.out.println(t);
			count++;
		}
		
		// Fermer l'itérateur
		iterator.close();
		
		System.out.println("\nNombre de résultats : " + count);
    }
}
