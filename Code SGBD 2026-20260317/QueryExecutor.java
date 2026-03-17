import java.util.Iterator;
import java.util.List;

public class QueryExecutor {

    private String basePath;

    public QueryExecutor(String basePath) {
        this.basePath = basePath;
    }

    /**
     * Exécute une requête et retourne un itérateur sur les résultats (mode pipelinage)
     * @param tables tableau des noms de tables
     * @param champs tableau des colonnes demandées (ou "*" pour toutes)
     * @param conditions liste des conditions WHERE
     * @return itérateur sur les tuples résultats
     */
    public ResultIterator execute(String[] tables, String[] champs, List<Condition> conditions) {
        try {
            ExecutionTree tree = new ExecutionTree(basePath, tables, champs, conditions);
            return new ResultIterator(tree);
        } catch (Exception e) {
            System.out.println("Erreur lors de la construction de l'arbre d'exécution: " + e.getMessage());
            return new ResultIterator(null); // Itérateur vide
        }
    }

    /**
     * Itérateur qui produit les résultats en mode pipelinage
     */
    public static class ResultIterator implements Iterator<TupleResultat> {

        private ExecutionTree executionTree;
        private boolean opened = false;
        private TupleResultat nextResult = null;
        private boolean nextFetched = false;
        
        public ResultIterator(ExecutionTree tree) {
            this.executionTree = tree;
        }
        
        @Override
        public boolean hasNext() {
            if (executionTree == null) return false;
            
            if (!opened) {
                executionTree.open();
                opened = true;
            }
            
            if (!nextFetched) {
                nextResult = fetchNext();
                nextFetched = true;
            }
            
            return nextResult != null;
        }
        
        @Override
        public TupleResultat next() {
            if (!hasNext()) {
                return null;
            }
            
            TupleResultat result = nextResult;
            nextFetched = false; // Force la lecture du prochain élément
            nextResult = null;
            return result;
        }
        
        private TupleResultat fetchNext() {
            Tuple tuple = executionTree.next();
            if (tuple == null) {
                return null;
            }
            return new TupleResultat(tuple.val);
        }
        
        /**
         * Ferme l'itérateur et libère les ressources
         */
        public void close() {
            if (executionTree != null && opened) {
                executionTree.close();
            }
        }
        
        /**
         * Retourne les champs sélectionnés
         */
        public String[] getSelectedFields() {
            return executionTree != null ? executionTree.getSelectedFields() : new String[0];
        }
        
        /**
         * Indique si on sélectionne toutes les colonnes
         */
        public boolean isSelectAll() {
            return executionTree != null ? executionTree.isSelectAll() : false;
        }
    }

    /**
     * Classe interne pour représenter un tuple résultat
     */
    public static class TupleResultat {
        public int[] val;

        public TupleResultat(int[] values) {
            this.val = values;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < val.length; i++) {
                if (i > 0) sb.append("\t");
                sb.append(val[i]);
            }
            return sb.toString();
        }
    }
}