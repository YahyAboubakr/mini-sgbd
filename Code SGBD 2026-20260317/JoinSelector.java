/**
 * Sélecteur de stratégie de jointure optimale basé sur les métadonnées des tables.
 * Choisit entre JointureHachage (Hash Join) et JointureTriFusion (Sort-Merge Join)
 * en fonction du tri des données, de la taille des tables et de la mémoire disponible.
 */
public class JoinSelector {

    /**
     * Sélectionne la meilleure stratégie de jointure en fonction des métadonnées.
     *
     * @param left L'opérateur gauche (table ou sous-arbre)
     * @param right L'opérateur droit (table ou sous-arbre)
     * @param colLeft Index de la colonne de jointure dans l'opérateur gauche
     * @param colRight Index de la colonne de jointure dans l'opérateur droit
     * @param isSorted true si les deux opérateurs sont déjà triés sur leur colonne de jointure respective
     * @param availableMemory Mémoire disponible en octets pour l'opération de jointure
     * @return L'opérateur de jointure optimal (JointureHachage ou JointureTriFusion)
     */
    public static Operateur getOptimalJoin(Operateur left, Operateur right, int colLeft, int colRight, boolean isSorted, int availableMemory) {
        // Estimation des tailles (en nombre de tuples)
        long leftSize = left.estimateSize();
        long rightSize = right.estimateSize();

        if(leftSize == -1 || rightSize == -1){
            throw new IllegalArgumentException("Impossible de déterminer la taille des tables");
        }

        // Estimation de la mémoire nécessaire pour une HashMap (approximation grossière : 100 octets par tuple)
        long estimatedMemoryForHash = Math.min(leftSize, rightSize) * 100;

        // Logique de décision
        long nlrCount = leftSize * rightSize;

        // Si les données sont déjà triées, TriFusion est optimal
        if (isSorted) {
            System.out.println("JoinSelector: Choix -> JointureTriFusion (Données déjà triées). Gain estimé : O(N+M) sans coût additionnel en mémoire/CPU.");
            return new JointureTriFusion(left, right, colLeft, colRight);
        }

        // 1. Si une table tient en mémoire : jointure par boucle imbriquées, ou hachage.
        if (nlrCount < 100) {
            // Les tables sont minuscules : la Double Boucle Imbriquée (Nested Loop Join) est super rapide
            // Gain : Pas d'allocation mémoire complexe (HashMap) ni de tri coûteux au lancement.
            System.out.println("JoinSelector: Choix -> DBI (Double Boucle Imbriquée). Gain estimé : Tables très petites, évite l'overhead d'initialisation.");
            return new DBI(left, right, colLeft, colRight);
        } else if (estimatedMemoryForHash < availableMemory) {
            System.out.println("JoinSelector: Choix -> JointureHachage (La plus petite table tient en RAM). Gain estimé : Lookup O(1) très rapide.");
            return new JointureHachage(left, right, colLeft, colRight);
        }

        // 2. Si au moins un index est utilisable : jointure par boucle imbriquées indexée.
        IndexHachage indexRight = getIndexIfExists(right, colRight);
        if (indexRight != null && right instanceof FullScanTableDisque) {
            System.out.println("JoinSelector: Choix -> JointureBoucleIndex (Index utilisable sur la table). Gain estimé : Accès direct évitant un Full Scan ou Tri.");
            return new JointureBoucleIndex(left, ((FullScanTableDisque)right).getTable(), indexRight, colLeft);
        }

        // 3. Si une des deux tables beaucoup plus petite que l'autre : jointure par hachage.
        if (leftSize < rightSize / 10 || rightSize < leftSize / 10) {
            System.out.println("JoinSelector: Choix -> JointureHachage (Une table est beaucoup plus petite que l'autre). Gain estimé : Construction de la table de hachage rapide.");
            return new JointureHachage(left, right, colLeft, colRight);
        }

        // 4. Sinon : jointure par tri-fusion
        System.out.println("JoinSelector: Choix -> JointureTriFusion (Par défaut ou mémoire insuffisante). Gain estimé : Évite le débordement mémoire (OutOfMemoryError).");
        return new JointureTriFusion(left, right, colLeft, colRight);
    }

    private static IndexHachage getIndexIfExists(Operateur op, int col) {
        if (op instanceof FullScanTableDisque) {
            TableDisque td = ((FullScanTableDisque) op).getTable();
            if (td != null && td.filePath != null) {
                java.io.File file = new java.io.File(td.filePath);
                String indexName = "index_" + file.getName() + "_col" + col;
                java.io.File indexFile = new java.io.File(file.getParent(), indexName);
                if (indexFile.exists()) {
                    try {
                        IndexHachage index = new IndexHachage(indexFile.getAbsolutePath());
                        index.charger();
                        return index;
                    } catch (Exception e) {
                        return null;
                    }
                }
            }
        }
        return null;
    }
}