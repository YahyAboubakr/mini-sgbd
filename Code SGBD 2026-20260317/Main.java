public class Main {

    public static void main(String[] args) {
        String query = "SELECT name, age FROM users WHERE age > 30";
        Parseur parseur = new Parseur(query);
        parseur.stringAnalyser();
    }
}
