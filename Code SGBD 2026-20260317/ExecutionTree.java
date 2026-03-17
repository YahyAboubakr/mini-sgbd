import java.util.ArrayList;
import java.util.List;

/**
 * Arbre d'exécution qui implémente le mode pipelinage.
 * Construit un arbre d'opérateurs et retourne les résultats ligne par ligne.
 */
public class ExecutionTree implements Operateur {
    
    private Operateur rootOperator;
    private String[] selectedFields;
    private boolean selectAll;
    private int[] fieldIndices;
    
    /**
     * Construit l'arbre d'exécution
     * @param basePath chemin de base des fichiers
     * @param tables noms des tables
     * @param champs champs à sélectionner
     * @param conditions conditions WHERE
     */
    public ExecutionTree(String basePath, String[] tables, String[] champs, List<Condition> conditions) {
        // Vérifications de base
        if (tables == null || tables.length == 0) {
            throw new IllegalArgumentException("Aucune table spécifiée");
        }
        if (champs == null || champs.length == 0) {
            throw new IllegalArgumentException("Aucun champ spécifié");
        }
        
        this.selectedFields = champs;
        this.selectAll = champs[0].trim().equals("*");
        
        // Construire l'arbre d'exécution
        buildExecutionTree(basePath, tables, conditions);
    }
    
    /**
     * Construit l'arbre d'exécution en partant des feuilles (tables) vers la racine
     */
    private void buildExecutionTree(String basePath, String[] tables, List<Condition> conditions) {
        // Séparer les conditions de jointure des conditions de filtrage
        List<Condition> joinConditions = new ArrayList<>();
        List<Condition> filterConditions = new ArrayList<>();

        if (conditions != null) {
            for (Condition cond : conditions) {
                if (cond.isJoinCondition()) {
                    joinConditions.add(cond);
                } else {
                    filterConditions.add(cond);
                }
            }
        }

        // Commencer avec la première table
        Operateur currentOperator = createTableScan(basePath, tables[0]);

        // Appliquer les jointures si plusieurs tables
        if (tables.length > 1) {
            currentOperator = applyJoins(currentOperator, basePath, tables, joinConditions);
        }

        // Appliquer les conditions de filtrage
        if (!filterConditions.isEmpty()) {
            // Déterminer la taille du tuple après jointure
            int tupleSize = determineTupleSize(currentOperator);
            currentOperator = applyConditions(currentOperator, filterConditions, tupleSize);
        }

        this.rootOperator = currentOperator;
    }

    /**
     * Crée un opérateur de scan pour une table
     */
    private Operateur createTableScan(String basePath, String tableName) {
        TableDisque td = new TableDisque(basePath + tableName);
        return new FullScanTableDisque(td);
    }

    /**
     * Applique les jointures entre les tables
     */
    private Operateur applyJoins(Operateur currentOperator, String basePath, String[] tables, List<Condition> joinConditions) {
        Operateur result = currentOperator;

        // Pour chaque table supplémentaire, appliquer les jointures
        for (int i = 1; i < tables.length; i++) {
            Operateur rightOperator = createTableScan(basePath, tables[i]);

            // Trouver la condition de jointure pour cette table
            Condition joinCondition = findJoinCondition(joinConditions, tables[i-1], tables[i]);
            if (joinCondition != null) {
                // Extraire les indices de colonnes pour la jointure
                int leftColIndex = resolveColumnIndex(joinCondition.operandeGauche(), tables, result);
                int rightColIndex = resolveColumnIndex(joinCondition.operandeDroit(), tables, rightOperator);

                result = new JointureTriFusion(result, rightOperator, leftColIndex, rightColIndex);
            } else {
                // Si pas de condition de jointure explicite, on fait un produit cartésien
                // Pour l'instant, on suppose qu'il y a toujours une condition de jointure
                throw new IllegalArgumentException("Jointure requise entre " + tables[i-1] + " et " + tables[i]);
            }
        }

        return result;
    }

    /**
     * Trouve la condition de jointure entre deux tables spécifiques
     */
    private Condition findJoinCondition(List<Condition> joinConditions, String leftTable, String rightTable) {
        for (Condition cond : joinConditions) {
            String leftCondTable = Condition.extractTableName(cond.operandeGauche());
            String rightCondTable = Condition.extractTableName(cond.operandeDroit());

            if ((leftCondTable.equals(leftTable) && rightCondTable.equals(rightTable)) ||
                (leftCondTable.equals(rightTable) && rightCondTable.equals(leftTable))) {
                return cond;
            }
        }
        return null;
    }

    /**
     * Résout l'index d'une colonne dans le contexte d'un opérateur
     */
    private int resolveColumnIndex(String tableColumnRef, String[] allTables, Operateur operator) {
        // String tableName = Condition.extractTableName(tableColumnRef);
        String columnName = Condition.extractColumnName(tableColumnRef);

        // Pour l'instant, on suppose que les colonnes sont référencées par leur index numérique
        try {
            return Integer.parseInt(columnName);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Nom de colonne non numérique non supporté: " + columnName);
        }
    }

    /**
     * Détermine la taille d'un tuple produit par un opérateur
     */
    private int determineTupleSize(Operateur operator) {
        // Ouvrir temporairement l'opérateur pour lire un tuple
        operator.open();
        Tuple sampleTuple = operator.next();
        operator.close();

        if (sampleTuple == null) {
            throw new IllegalArgumentException("Impossible de déterminer la taille du tuple");
        }

        return sampleTuple.val.length;
    }
    
    /**
     * Applique les conditions WHERE (filtrage) en créant un opérateur MultiRestrict
     */
    private Operateur applyConditions(Operateur source, List<Condition> conditions, int tupleSize) {
        if (conditions == null || conditions.isEmpty()) {
            return source;
        }

        // Utiliser MultiRestrict pour gérer toutes les conditions de filtrage avec AND/OR
        return new MultiRestrict(source, conditions, tupleSize);
    }
    
    @Override
    public void open() {
        rootOperator.open();
    }
    
    @Override
    public Tuple next() {
        Tuple tuple = rootOperator.next();
        if (tuple == null) {
            return null;
        }
        
        // Appliquer la projection (sélection des colonnes)
        if (selectAll) {
            return tuple;
        } else {
            // Créer un nouveau tuple avec seulement les colonnes sélectionnées
            int[] selectedValues = new int[selectedFields.length];
            for (int i = 0; i < selectedFields.length; i++) {
                int colIndex = Integer.parseInt(selectedFields[i].trim());
                if (colIndex >= 0 && colIndex < tuple.val.length) {
                    selectedValues[i] = tuple.val[colIndex];
                }
            }
            Tuple resultTuple = new Tuple(selectedValues.length);
            resultTuple.val = selectedValues;
            return resultTuple;
        }
    }
    
    @Override
    public void close() {
        rootOperator.close();
    }
    
    /**
     * Retourne les champs sélectionnés (pour affichage)
     */
    public String[] getSelectedFields() {
        return selectedFields;
    }
    
    /**
     * Indique si on sélectionne toutes les colonnes
     */
    public boolean isSelectAll() {
        return selectAll;
    }

    /**
     * Exécute une requête SQL et affiche les résultats
     * @param query la requête SQL à exécuter
     * @param basePath chemin de base des fichiers de données
     */
    public static void executeQuery(String query, String basePath) {
        Parseur parseur = new Parseur(query);

        try {
            ResultatRequete resultat = parseur.stringAnalyser();

            String[] tables = parseur.tables(resultat.tables());
            String[] champs = parseur.champs(resultat.champs());
            List<Condition> conditions = parseur.whereParseur(resultat.conditions());

            // Exécuter la requête en mode pipelinage
            QueryExecutor executor = new QueryExecutor(basePath);
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

        } catch (Exception e) {
            System.out.println("Erreur lors de l'exécution de la requête : " + e.getMessage());
            e.printStackTrace();
        }
    }
}