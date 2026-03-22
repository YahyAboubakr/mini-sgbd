
import java.io.*;
import java.util.*;

/**
 * Opérateur IndexScan par hachage statique (cours p.97-98).
 *
 * Au lieu d'un FullScan (N/blockSize lectures), utilise l'IndexHachage
 * pour localiser directement les tuples correspondant à une valeur de clé.
 *
 * Coût :
 *   - open()  → 1 lecture de bucket dans l'index
 *   - next()  → 1 lecture directe par tuple retourné
 *   Total     → 1 + k  (vs ceil(N/b) pour le FullScan)
 *
 * Principe : on calcule le bucket via hash(valeur), on lit son contenu une seule fois,
 * puis on lit chaque tuple directement par son numéro (sans parcourir toute la table).
 */
public class IndexScanHachage implements Operateur {

    // L'index de hachage (statique IndexHachage ou dynamique IndexHachageDynamique)
    private IndexHachageBase index;
    // La table physique pour aller lire les tuples
    private TableDisque  table;
    // La clé pour laquelle on cherche les tuples correspondants
    private int valeurRecherchee;

    // Liste des numéros de tuple retournés par le bucket lors du open()
    private List<Integer> tupleIds;
    // Position courante dans tupleIds (avance à chaque appel à next())
    private int cursor;

    // Total des lectures disque : 1 bucket + k accès directs
    public int reads = 0;

    public IndexScanHachage(IndexHachageBase index, TableDisque table, int valeur) {
        this.index = index;
        this.table = table;
        this.valeurRecherchee = valeur;
    }

    // ── Operateur ─────────────────────────────────────────────────────────────

    @Override
    public void open() {
        try {
            // Reset du compteur de l'index pour isoler le coût de cette requête
            index.reads = 0;
            // 1 lecture de bucket : hash(valeur) → accès direct au bon bucket
            tupleIds = index.chercher(valeurRecherchee);
            // On récupère le coût de la lecture du bucket
            this.reads += index.reads;
            // Reset pour ne pas compter deux fois si open() est rappelé
            index.reads = 0;
        } catch (IOException e) {
            System.err.println("IndexScanHachage : erreur lors de la recherche dans l'index.");
            // En cas d'erreur, on retourne une liste vide plutôt que de planter
            tupleIds = new ArrayList<>();
        }
        // On repart du début de la liste de résultats
        cursor = 0;
    }

    @Override
    public Tuple next() {
        // Plus aucun résultat → fin de l'itération
        if (cursor >= tupleIds.size()) return null;
        // Lire le tuple dont le numéro est à la position cursor, puis avancer le curseur
        return lireTupleDirectement(tupleIds.get(cursor++));
    }

    @Override
    // Rien à libérer (pas de fichier ouvert en continu)
    public void close() {}

    @Override
    public int estimateSize() {
        // Retourne le nombre de tuples trouvés (0 si open() n'a pas encore été appelé)
        return tupleIds == null ? 0 : tupleIds.size();
    }

    // ── Accès direct ──────────────────────────────────────────────────────────

    /**
     * Lit le tuple numéro numTuple depuis TableDisque sans FullScan.
     *
     * Format du fichier TableDisque :
     *   octet 0   → nombre total de tuples (header)
     *   octet 1   → tupleSize (taille d'un tuple en octets)
     *   octets 2+ → tuples consécutifs de tupleSize octets chacun
     *
     * Offset du tuple voulu = 2 (header) + numTuple * tupleSize octets.
     * On saute directement à cet offset avec reader.skip() au lieu de lire séquentiellement.
     *
     * Comptage : reads++ par appel (1 accès disque simulé par tuple lu).
     */
    private Tuple lireTupleDirectement(int numTuple) {
        try {
            FileReader reader = new FileReader(table.filePath);
            // Octet 0 : taille de la table (non utilisé ici, on le saute)
            reader.read();
            // Octet 1 : tupleSize = nombre de champs (octets) par tuple
            int ts = reader.read();
            // Sauter directement au tuple voulu sans lire les précédents
            reader.skip((long) numTuple * ts);
            // Lire les ts octets qui forment le tuple
            Tuple t = new Tuple(ts);
            for (int j = 0; j < ts; j++) t.val[j] = reader.read();
            reader.close();
            // 1 accès disque simulé (ouvrir + lire + fermer = 1 opération logique)
            this.reads++;
            return t;
        } catch (IOException e) {
            System.err.println("IndexScanHachage : erreur d'accès direct au tuple " + numTuple);
            return null;
        }
    }
}
