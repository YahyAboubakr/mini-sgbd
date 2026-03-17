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
        // Pour l'instant, on ne gère qu'une seule table
        TableDisque td = new TableDisque(basePath + tables[0]);
        FullScanTableDisque tableScan = new FullScanTableDisque(td);
        
        // Ouvrir brièvement pour déterminer la taille du tuple
        tableScan.open();
        Tuple firstTuple = tableScan.next();
        if (firstTuple == null) {
            throw new IllegalArgumentException("La table est vide");
        }
        int tupleSize = firstTuple.val.length;
        tableScan.close();
        
        // Recréer l'instance pour l'arbre d'exécution
        td = new TableDisque(basePath + tables[0]);
        tableScan = new FullScanTableDisque(td);
        
        // Commencer avec le scan de table
        Operateur currentOperator = tableScan;
        
        // Appliquer les conditions WHERE si elles existent
        if (conditions != null && !conditions.isEmpty()) {
            currentOperator = applyConditions(currentOperator, conditions, tupleSize);
        }
        
        // TODO: Ajouter d'autres opérateurs (JOIN, GROUP BY, etc.)
        
        this.rootOperator = currentOperator;
    }
    
    /**
     * Applique les conditions WHERE en créant des opérateurs Restrict
     */
    private Operateur applyConditions(Operateur source, List<Condition> conditions, int tupleSize) {
        Operateur current = source;
        
        // Pour l'instant, on ne gère que la première condition
        // TODO: Gérer les opérateurs logiques AND/OR et les conditions multiples
        if (!conditions.isEmpty()) {
            Condition cond = conditions.get(0);
            
            // Convertir le nom de colonne en index
            int colonneIndex;
            try {
                colonneIndex = Integer.parseInt(cond.operandeGauche().trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Colonne invalide: " + cond.operandeGauche());
            }
            
            // Vérifier que la colonne existe
            if (colonneIndex < 0 || colonneIndex >= tupleSize) {
                throw new IllegalArgumentException("Colonne " + colonneIndex + " n'existe pas");
            }
            
            // Convertir la valeur
            int valeur;
            try {
                valeur = Integer.parseInt(cond.operandeDroit().trim());
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Valeur invalide: " + cond.operandeDroit());
            }
            
            // Convertir l'opérateur
            int typeOperation = convertOperator(cond.operateur());
            
            // Créer l'opérateur Restrict
            current = new Restrict(current, colonneIndex, valeur, typeOperation);
        }
        
        return current;
    }
    
    /**
     * Convertit un opérateur Tests en type d'opération Restrict
     */
    private int convertOperator(Tests operateur) {
        switch (operateur) {
            case EQUALS:
                return Restrict.EGAL;
            case LESS_THAN:
                return Restrict.INFEGAL;
            // Pour GREATER_THAN, on pourrait avoir besoin d'une nouvelle constante dans Restrict
            default:
                System.out.println("Attention : Opérateur " + operateur + " non supporté, utilisation de EGAL.");
                return Restrict.EGAL;
        }
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
}