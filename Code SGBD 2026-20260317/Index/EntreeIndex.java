
/**
 * Entrée d'index : associe une clé à la position d'un tuple dans TableDisque.
 *
 * Correspond au concept de RID (Record IDentifier) du cours (p.92) :
 *   RID = numéro de tuple → le bloc = numTuple / blockSize
 *                            le slot  = numTuple % blockSize
 */
public class EntreeIndex {

    public int cle;       // valeur de la colonne indexée
    public int numTuple;  // position du tuple dans la table (0-based)

    public EntreeIndex(int cle, int numTuple) {
        this.cle = cle;
        this.numTuple = numTuple;
    }

    @Override
    public String toString() {
        return "(" + cle + " → tuple#" + numTuple + ")";
    }
}
