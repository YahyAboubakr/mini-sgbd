public record Condition(
    String operandeGauche,    // ex: "cond1"
    Tests operateur,          // ex: Tests.EQUALS
    String operandeDroit,     // ex: "5"
    Tests lienLogiqueSuivant  // ex: Tests.AND (ou null si c'est la dernière condition)
) {
    @Override
    public String toString() {
        String lien = (lienLogiqueSuivant != null) ? lienLogiqueSuivant.name() : "FIN";
        return String.format("[%s %s %s] -> %s", operandeGauche, operateur, operandeDroit, lien);
    }
}
    

