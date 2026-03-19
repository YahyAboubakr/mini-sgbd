
import java.io.*;
import java.util.*;

/**
 * Index par hachage statique (cours p.92-99).
 *
 * Principe :
 *   - N buckets (pages) sur disque
 *   - h(clé) = clé % N → détermine le bucket
 *   - Chaque bucket stocke une liste d'EntreeIndex {clé, numTuple}
 *
 * Format du fichier d'index (même convention que TableDisque) :
 *   [nbBuckets][attrIndex][bucketCapacity]
 *   Pour chaque bucket : [nbEntrees][cle0][num0][cle1][num1] ... (padding)
 *
 * Construction : FullScan de la table → 1 écriture de l'index.
 * Recherche    : calcul du bucket + skip jusqu'à lui → 1 lecture disque.
 */
public class IndexHachage {

    public String filePath;
    public int nbBuckets;
    public int attrIndex;      // colonne indexée
    public int bucketCapacity; // nb max d'entrées par bucket (taille fixe)
    public int reads = 0;      // lectures de buckets dans l'index

    public IndexHachage(String filePath) {
        this.filePath = filePath;
    }

    // ── Fonction de hachage ───────────────────────────────────────────────────

    private int hash(int cle) {
        return Math.abs(cle % nbBuckets);
    }

    // ── Construction ──────────────────────────────────────────────────────────

    /**
     * Construit l'index depuis une TableDisque sur la colonne attrIndex.
     * Effectue un FullScan de la table, puis écrit le fichier d'index.
     *
     * @param table     table source
     * @param attrIndex colonne à indexer
     * @param nbBuckets nombre de buckets (doit être > 0)
     */
    public void construire(TableDisque table, int attrIndex, int nbBuckets) throws IOException {
        this.nbBuckets = nbBuckets;
        this.attrIndex = attrIndex;

        // Étape 1 : FullScan → répartition dans les buckets en mémoire
        List<List<EntreeIndex>> buckets = new ArrayList<>();
        for (int i = 0; i < nbBuckets; i++) buckets.add(new ArrayList<>());

        FileReader reader = new FileReader(table.filePath);
        int taille    = reader.read(); // header : nb tuples
        int tupleSize = reader.read(); // header : nb attributs

        for (int i = 0; i < taille; i++) {
            int[] vals = new int[tupleSize];
            for (int j = 0; j < tupleSize; j++) vals[j] = reader.read();
            int cle = vals[attrIndex];
            buckets.get(hash(cle)).add(new EntreeIndex(cle, i));
        }
        reader.close();

        // Étape 2 : capacité fixe = taille du plus grand bucket (sans overflow)
        this.bucketCapacity = 1;
        for (List<EntreeIndex> b : buckets)
            if (b.size() > this.bucketCapacity) this.bucketCapacity = b.size();

        // Étape 3 : écriture du fichier d'index
        FileWriter writer = new FileWriter(this.filePath);
        writer.write(nbBuckets);
        writer.write(attrIndex);
        writer.write(bucketCapacity);
        for (List<EntreeIndex> bucket : buckets) {
            writer.write(bucket.size());
            for (EntreeIndex e : bucket) {
                writer.write(e.cle);
                writer.write(e.numTuple);
            }
            // Padding jusqu'à bucketCapacity (taille fixe pour le skip)
            for (int i = bucket.size(); i < bucketCapacity; i++) {
                writer.write(0);
                writer.write(0);
            }
        }
        writer.close();

        System.out.println("Index hachage construit : " + this.filePath
            + "  (" + nbBuckets + " buckets, capacité " + bucketCapacity + " entrées/bucket)");
    }

    // ── Recherche ─────────────────────────────────────────────────────────────

    /**
     * Retourne les numéros de tuples associés à la clé donnée.
     *
     * Algorithme (cours p.97) :
     *   1. Calculer bucketId = h(clé)
     *   2. Sauter jusqu'au bucket dans le fichier → 1 lecture disque
     *   3. Parcourir les entrées du bucket → collecter les numTuple correspondants
     *
     * Comptage : reads++ par appel (= 1 accès disque au bucket).
     */
    public List<Integer> chercher(int cle) throws IOException {
        int bucketId = hash(cle);

        FileReader reader = new FileReader(this.filePath);
        reader.read(); // nbBuckets  (header)
        reader.read(); // attrIndex  (header)
        int cap = reader.read(); // bucketCapacity (header)

        // Sauter jusqu'au bucket voulu :
        //   chaque bucket = 1 octet (nbEntrees) + cap*2 octets (entrées)
        reader.skip((long) bucketId * (1 + cap * 2));

        int nbEntrees = reader.read();
        List<Integer> result = new ArrayList<>();
        for (int i = 0; i < cap; i++) {
            int k   = reader.read();
            int num = reader.read();
            if (i < nbEntrees && k == cle) result.add(num);
        }
        reader.close();
        this.reads++;
        return result;
    }

    /**
     * Charge uniquement les métadonnées (header) depuis le fichier d'index,
     * sans lire les buckets.
     */
    public void charger() throws IOException {
        FileReader reader = new FileReader(this.filePath);
        this.nbBuckets      = reader.read();
        this.attrIndex      = reader.read();
        this.bucketCapacity = reader.read();
        reader.close();
    }
}
