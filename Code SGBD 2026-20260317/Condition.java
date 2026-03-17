public record Condition(
    String operandeGauche,    // ex: "cond1" ou "table1.col1"
    Tests operateur,          // ex: Tests.EQUALS
    String operandeDroit,     // ex: "5" ou "table2.col2"
    Tests lienLogiqueSuivant, // ex: Tests.AND (ou null si c'est la dernière condition)
    boolean isJoinCondition   // true si c'est une condition de jointure (table.col = table.col)
) {

    // Constructeur pour conditions simples (rétrocompatibilité)
    public Condition(String operandeGauche, Tests operateur, String operandeDroit, Tests lienLogiqueSuivant) {
        this(operandeGauche, operateur, operandeDroit, lienLogiqueSuivant, false);
    }

    // Méthode pour déterminer si une condition est de jointure
    public static boolean isJoinCondition(String left, String right) {
        return left.contains(".") && right.contains(".");
    }

    // Méthode pour extraire le nom de table depuis une référence table.colonne
    public static String extractTableName(String tableColumnRef) {
        if (tableColumnRef.contains(".")) {
            return tableColumnRef.split("\\.")[0];
        }
        return null; // pas de table spécifiée
    }

    // Méthode pour extraire le nom de colonne depuis une référence table.colonne
    public static String extractColumnName(String tableColumnRef) {
        if (tableColumnRef.contains(".")) {
            return tableColumnRef.split("\\.")[1];
        }
        return tableColumnRef; // c'est déjà juste le nom de colonne
    }

    @Override
    public String toString() {
        String lien = (lienLogiqueSuivant != null) ? lienLogiqueSuivant.name() : "FIN";
        String type = isJoinCondition ? "JOIN" : "FILTER";
        return String.format("[%s %s %s] -> %s (%s)", operandeGauche, operateur, operandeDroit, lien, type);
    }
}
    

