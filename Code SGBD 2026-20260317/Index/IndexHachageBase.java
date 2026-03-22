
import java.io.*;
import java.util.*;

/**
 * Classe abstraite commune aux index par hachage statique et dynamique.
 *
 * Factorise les champs et l'interface partagés :
 *   - attrIndex : colonne indexée
 *   - reads     : compteur de lectures disque simulées
 *   - construire() : construction depuis une TableDisque
 *   - chercher()   : recherche par valeur de clé → liste de numTuple
 *
 * Les sous-classes implémentent leur propre stratégie de hachage.
 */
public abstract class IndexHachageBase {

    // Colonne de la table sur laquelle l'index est construit
    public int attrIndex;

    // Compteur de lectures disque simulées (reset entre les requêtes)
    public int reads = 0;

    /**
     * Construit l'index en lisant toute la table (FullScan).
     *
     * @param table     table source à indexer
     * @param attrIndex colonne à indexer
     */
    public abstract void construire(TableDisque table, int attrIndex) throws IOException;

    /**
     * Retourne les numéros de tuples associés à la clé donnée.
     * Incrémente reads d'1 par appel (1 accès disque simulé).
     */
    public abstract List<Integer> chercher(int cle) throws IOException;

    // ── Utilitaire partagé ────────────────────────────────────────────────────

    /**
     * Lit toute la table et retourne la liste des EntreeIndex sur la colonne attrIndex.
     * Factorise le FullScan commun aux deux implémentations (statique et dynamique).
     *
     * @param table     table à scanner
     * @param attrIndex colonne à indexer
     * @return liste de toutes les entrées (clé, numTuple)
     */
    protected List<EntreeIndex> lireTable(TableDisque table, int attrIndex) throws IOException {
        this.attrIndex = attrIndex;
        List<EntreeIndex> entrees = new ArrayList<>();
        FileReader reader = new FileReader(table.filePath);
        int taille    = reader.read(); // header : nb tuples
        int tupleSize = reader.read(); // header : nb attributs
        for (int i = 0; i < taille; i++) {
            int[] vals = new int[tupleSize];
            for (int j = 0; j < tupleSize; j++) vals[j] = reader.read();
            entrees.add(new EntreeIndex(vals[attrIndex], i));
        }
        reader.close();
        return entrees;
    }
}
