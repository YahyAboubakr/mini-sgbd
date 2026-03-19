
import java.io.*;
import java.util.*;

/**
 * Opérateur IndexScan utilisant un Arbre B+ (cours p.82-84).
 *
 * Supporte deux modes :
 *   - Égalité  : col = valeur          → chercher(val)
 *   - Intervalle : valMin ≤ col ≤ valMax → chercherIntervalle(min, max)
 *
 * Coût :
 *   - open()  → O(log N) lectures (traversée de l'arbre jusqu'à la feuille)
 *              + quelques lectures de feuilles pour les requêtes range
 *   - next()  → 1 accès direct par tuple retourné
 */
public class IndexScanArbre implements Operateur {

    private ArbreB      arbre;
    private TableDisque table;
    private int valMin, valMax; // intervalle [valMin, valMax] (égalité si valMin == valMax)

    private List<Integer> tupleIds;
    private int cursor;

    public int reads = 0; // total : traversée arbre + accès directs

    /** Constructeur pour recherche par égalité : col = valeur */
    public IndexScanArbre(ArbreB arbre, TableDisque table, int valeur) {
        this(arbre, table, valeur, valeur);
    }

    /** Constructeur pour recherche par intervalle : valMin ≤ col ≤ valMax */
    public IndexScanArbre(ArbreB arbre, TableDisque table, int valMin, int valMax) {
        this.arbre  = arbre;
        this.table  = table;
        this.valMin = valMin;
        this.valMax = valMax;
    }

    // ── Operateur ─────────────────────────────────────────────────────────────

    @Override
    public void open() {
        arbre.reads = 0;
        if (valMin == valMax)
            tupleIds = arbre.chercher(valMin);
        else
            tupleIds = arbre.chercherIntervalle(valMin, valMax);
        this.reads += arbre.reads;
        arbre.reads = 0;
        cursor = 0;
    }

    @Override
    public Tuple next() {
        if (cursor >= tupleIds.size()) return null;
        return lireTupleDirectement(tupleIds.get(cursor++));
    }

    @Override
    public void close() {}

    @Override
    public int estimateSize() {
        return tupleIds == null ? 0 : tupleIds.size();
    }

    // ── Accès direct ──────────────────────────────────────────────────────────

    /**
     * Lit le tuple numéro numTuple depuis TableDisque sans FullScan.
     *
     * Offset = 2 (header) + numTuple * tupleSize octets.
     * Comptage : reads++ par appel (1 accès disque par tuple).
     */
    private Tuple lireTupleDirectement(int numTuple) {
        try {
            FileReader reader = new FileReader(table.filePath);
            reader.read();          // skip : taille de la table
            int ts = reader.read(); // tupleSize
            reader.skip((long) numTuple * ts);
            Tuple t = new Tuple(ts);
            for (int j = 0; j < ts; j++) t.val[j] = reader.read();
            reader.close();
            this.reads++;
            return t;
        } catch (IOException e) {
            System.err.println("IndexScanArbre : erreur d'accès direct au tuple " + numTuple);
            return null;
        }
    }
}
