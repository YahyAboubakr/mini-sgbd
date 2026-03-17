import java.util.List;

/**
 * Opérateur Restrict étendu pour gérer plusieurs conditions avec opérateurs logiques AND/OR
 */
public class MultiRestrict extends Instrumentation implements Operateur {

    private Operateur dataSource;
    private List<Condition> conditions;
    private int tupleSize;

    public MultiRestrict(Operateur in, List<Condition> conditions, int tupleSize) {
        this.dataSource = in;
        this.conditions = conditions;
        this.tupleSize = tupleSize;
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
        Tuple tuple;

        while ((tuple = this.dataSource.next()) != null) {
            if (evaluateConditions(tuple)) {
                this.produit(tuple); // instrumentation
                this.stop();
                return tuple;
            }
        }

        this.stop();
        return null;
    }

    @Override
    public void close() {
        this.dataSource.close();
    }

    /**
     * Évalue toutes les conditions sur un tuple selon les opérateurs logiques
     */
    private boolean evaluateConditions(Tuple tuple) {
        boolean result = true; // Pour la première condition ou après un AND

        for (int i = 0; i < conditions.size(); i++) {
            Condition cond = conditions.get(i);
            boolean currentResult = evaluateSingleCondition(tuple, cond);

            if (i == 0) {
                // Première condition
                result = currentResult;
            } else {
                // Conditions suivantes avec opérateur logique
                Tests lien = conditions.get(i-1).lienLogiqueSuivant();
                if (lien == null) {
                    // Si pas de lien spécifié, on considère AND par défaut
                    result = result && currentResult;
                } else if (lien == Tests.AND) {
                    result = result && currentResult;
                } else if (lien == Tests.OR) {
                    result = result || currentResult;
                }
            }
        }

        return result;
    }

    /**
     * Évalue une seule condition sur un tuple
     */
    private boolean evaluateSingleCondition(Tuple tuple, Condition cond) {
        // Convertir le nom de colonne en index
        int colonneIndex;
        try {
            // Extraire le nom de colonne (peut être table.colonne ou juste colonne)
            String columnRef = cond.operandeGauche();
            String columnName = Condition.extractColumnName(columnRef);
            colonneIndex = Integer.parseInt(columnName.trim());
        } catch (NumberFormatException e) {
            // Si ce n'est pas un numéro, on ne peut pas l'évaluer pour l'instant
            return false;
        }

        // Vérifier que la colonne existe
        if (colonneIndex < 0 || colonneIndex >= tupleSize) {
            return false;
        }

        // Convertir la valeur
        int valeur;
        try {
            valeur = Integer.parseInt(cond.operandeDroit().trim());
        } catch (NumberFormatException e) {
            return false;
        }

        // Évaluer selon l'opérateur
        int tupleValue = tuple.val[colonneIndex];
        return switch (cond.operateur()) {
            case EQUALS -> tupleValue == valeur;
            case GREATER_THAN -> tupleValue > valeur;
            case LESS_THAN -> tupleValue < valeur;
            default -> false;
        };
    }
}