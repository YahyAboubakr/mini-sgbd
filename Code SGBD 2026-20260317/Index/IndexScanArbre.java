
import java.io.*;
import java.util.*;

/**
 * Opérateur IndexScan utilisant un Arbre B+ (cours p.82-84).
 *
 * Supporte deux modes :
 *   - Égalité    : col = valeur          → chercher(val)
 *   - Intervalle : valMin ≤ col ≤ valMax → chercherIntervalle(min, max)
 *
 * Coût :
 *   - open()  → O(log N) lectures (traversée de l'arbre jusqu'à la feuille)
 *              + quelques lectures de feuilles pour les requêtes range
 *   - next()  → 1 accès direct par tuple retourné
 *
 * Principe : on récupère d'abord TOUS les numTuple correspondants via l'arbre,
 * puis on lit chaque tuple directement dans la table (sans FullScan).
 */
public class IndexScanArbre implements Operateur {

    // L'index B+ sur lequel on cherche
    private ArbreB      arbre;
    // La table physique pour aller lire les tuples
    private TableDisque table;
    // Bornes de la recherche (égalité si valMin == valMax)
    private int valMin, valMax;

    // Liste des numéros de tuple trouvés par l'index lors du open()
    private List<Integer> tupleIds;
    // Position courante dans tupleIds (avance à chaque appel à next())
    private int cursor;

    // Total des lectures disque : traversée arbre + accès directs tuple
    public int reads = 0;

    // Constructeur pour recherche par égalité : col = valeur
    // Appelle le constructeur intervalle avec valMin = valMax = valeur
    public IndexScanArbre(ArbreB arbre, TableDisque table, int valeur) {
        this(arbre, table, valeur, valeur);
    }

    // Constructeur pour recherche par intervalle : valMin ≤ col ≤ valMax
    public IndexScanArbre(ArbreB arbre, TableDisque table, int valMin, int valMax) {
        this.arbre  = arbre;
        this.table  = table;
        this.valMin = valMin;
        this.valMax = valMax;
    }

    // ── Operateur ─────────────────────────────────────────────────────────────

    @Override
    public void open() {
        // Reset du compteur de l'arbre pour isoler le coût de cette requête
        arbre.reads = 0;

        // Choix du mode selon l'égalité ou l'intervalle
        if (valMin == valMax)
            // Recherche exacte : descend l'arbre → O(hauteur) lectures
            tupleIds = arbre.chercher(valMin);
        else
            // Range scan : descend jusqu'à min puis parcourt les feuilles chaînées
            tupleIds = arbre.chercherIntervalle(valMin, valMax);

        // On additionne les lectures de l'arbre au total
        this.reads += arbre.reads;
        // Reset pour ne pas compter deux fois si open() est rappelé
        arbre.reads = 0;
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
            System.err.println("IndexScanArbre : erreur d'accès direct au tuple " + numTuple);
            return null;
        }
    }
}
