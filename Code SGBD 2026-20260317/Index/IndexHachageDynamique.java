
import java.io.*;
import java.util.*;

/**
 * Index par hachage dynamique / extensible (cours p.100-106).
 *
 * Principe :
 *   - Un répertoire de taille 2^globalDepth pointe vers des buckets.
 *   - Chaque bucket a une profondeur locale l ≤ globalDepth.
 *   - Plusieurs entrées du répertoire peuvent pointer vers le même bucket
 *     (tant que l < globalDepth).
 *   - Quand un bucket déborde :
 *       - Si l == globalDepth : on double le répertoire (globalDepth++)
 *       - On crée 2 nouveaux buckets (l+1), on redistribue les entrées,
 *         on met à jour le répertoire.
 *
 * Différence clé avec le hachage statique :
 *   - Statique  : taille fixe → collisions possibles, buckets trop pleins
 *   - Dynamique : grandit à la demande → pas de débordement structurel
 *
 * Structure en mémoire (pas de fichier : la structure est trop variable) :
 *   Les lectures disque sont simulées avec reads++ dans chercher().
 */
public class IndexHachageDynamique extends IndexHachageBase {

    // Profondeur globale du répertoire : le répertoire a 2^globalDepth entrées
    private int globalDepth;

    // Le répertoire : directory.get(i) = le bucket associé au préfixe i
    // Plusieurs entrées peuvent pointer vers le même objet Bucket
    private List<Bucket> directory;

    // Nombre maximum d'entrées par bucket avant split
    private final int bucketCapacity;

    // Journal des événements (splits et doublements) — utile pour les tests pédagogiques
    private final List<String> historiqueSplits = new ArrayList<>();

    // ── Bucket ────────────────────────────────────────────────────────────────

    private static class Bucket {
        // Profondeur locale : ce bucket distingue les l derniers bits de la clé
        int localDepth;
        // Entrées stockées dans ce bucket
        List<EntreeIndex> entrees;

        Bucket(int localDepth) {
            this.localDepth = localDepth;
            this.entrees    = new ArrayList<>();
        }
    }

    // ── Constructeurs ─────────────────────────────────────────────────────────

    // Constructeur avec capacité par défaut (4 entrées par bucket)
    public IndexHachageDynamique() {
        this(4);
    }

    public IndexHachageDynamique(int bucketCapacity) {
        this.bucketCapacity = bucketCapacity;
        this.globalDepth    = 1;
        // Répertoire initial : 2 entrées, chacune pointe vers son propre bucket
        this.directory = new ArrayList<>();
        directory.add(new Bucket(1)); // préfixe binaire "0"
        directory.add(new Bucket(1)); // préfixe binaire "1"
    }

    // ── Fonction de hachage ───────────────────────────────────────────────────

    /**
     * Retourne les derniers globalDepth bits de la clé.
     * C'est l'index dans le répertoire correspondant à cette clé.
     */
    private int hash(int cle) {
        return Math.abs(cle) & ((1 << globalDepth) - 1);
    }

    // ── Construction ──────────────────────────────────────────────────────────

    @Override
    public void construire(TableDisque table, int attrIndex) throws IOException {
        this.attrIndex = attrIndex;

        // FullScan de la table mutualisé dans IndexHachageBase.lireTable()
        for (EntreeIndex e : lireTable(table, attrIndex))
            inserer(e.cle, e.numTuple);

        System.out.println("Index hachage dynamique construit : "
            + "profondeur globale=" + globalDepth
            + ", " + directory.size() + " entrées répertoire"
            + ", capacité bucket=" + bucketCapacity);
    }

    // ── Insertion (logique interne) ───────────────────────────────────────────

    /**
     * Insère une entrée dans le bon bucket d'après la clé,
     * puis splitte si le bucket dépasse sa capacité.
     */
    private void inserer(int cle, int numTuple) {
        int idx    = hash(cle);
        Bucket b   = directory.get(idx);
        b.entrees.add(new EntreeIndex(cle, numTuple));

        // Overflow → split uniquement si les clés sont distinctes (sinon impossible de séparer)
        if (b.entrees.size() > bucketCapacity && aDesClésDistinctes(b)) splitter(idx);
    }

    /**
     * Vérifie qu'un bucket contient au moins deux clés différentes.
     * Si toutes les clés sont identiques, aucun split ne pourra les séparer :
     * elles atterriront toujours du même côté, quelle que soit la profondeur.
     */
    private boolean aDesClésDistinctes(Bucket b) {
        if (b.entrees.isEmpty()) return false;
        int premiere = b.entrees.get(0).cle;
        for (EntreeIndex e : b.entrees)
            if (e.cle != premiere) return true;
        return false;
    }

    /**
     * Splitte le bucket à l'index dirIdx du répertoire.
     *
     * Étapes :
     *   1. Si profondeur locale == profondeur globale : doubler le répertoire
     *   2. Créer deux nouveaux buckets (localDepth + 1)
     *   3. Redistribuer les entrées selon le bit discriminant
     *   4. Mettre à jour toutes les entrées du répertoire qui pointaient vers l'ancien bucket
     */
    private void splitter(int dirIdx) {
        Bucket ancien = directory.get(dirIdx);
        int l         = ancien.localDepth;

        // Étape 1 : doublement du répertoire si nécessaire
        if (l == globalDepth) {
            int tailleActuelle = directory.size(); // = 2^globalDepth
            // On copie les pointeurs (pas les buckets) : les paires (i, i+tailleActuelle)
            // continueront à pointer vers les mêmes buckets
            for (int i = 0; i < tailleActuelle; i++) directory.add(directory.get(i));
            globalDepth++;
            historiqueSplits.add("  [DOUBLEMENT] profondeur " + (globalDepth - 1) + " → " + globalDepth
                + "  (répertoire " + tailleActuelle + " → " + directory.size() + " entrées)");
        }

        // Étape 2 : deux nouveaux buckets avec profondeur l+1
        Bucket b0 = new Bucket(l + 1); // recevra les clés dont le bit l vaut 0
        Bucket b1 = new Bucket(l + 1); // recevra les clés dont le bit l vaut 1
        // Clés de l'ancien bucket (pour le journal)
        List<Integer> anciennesCles = new ArrayList<>();
        for (EntreeIndex e : ancien.entrees) anciennesCles.add(e.cle);

        // Le bit discriminant est le bit à la position l (0-indexé)
        int bitDiscriminant = 1 << l;

        // Étape 3 : redistribution des entrées de l'ancien bucket
        for (EntreeIndex e : ancien.entrees) {
            // On regarde uniquement les l+1 derniers bits de la clé
            int h = Math.abs(e.cle) & ((1 << (l + 1)) - 1);
            if ((h & bitDiscriminant) == 0) b0.entrees.add(e);
            else                            b1.entrees.add(e);
        }

        // Étape 4 : mise à jour du répertoire
        // Toutes les entrées qui pointaient vers l'ancien bucket sont redirigées
        for (int i = 0; i < directory.size(); i++) {
            if (directory.get(i) == ancien) {
                int h = i & ((1 << (l + 1)) - 1);
                directory.set(i, (h & bitDiscriminant) == 0 ? b0 : b1);
            }
        }

        // Journaliser le split
        List<Integer> cles0 = new ArrayList<>(), cles1 = new ArrayList<>();
        for (EntreeIndex e : b0.entrees) cles0.add(e.cle);
        for (EntreeIndex e : b1.entrees) cles1.add(e.cle);
        historiqueSplits.add("  [SPLIT]      bucket" + anciennesCles + " → gauche" + cles0 + " | droite" + cles1
            + "  (profondeur locale " + l + " → " + (l + 1) + ")");

        // Split récursif si un nouveau bucket déborde encore,
        // mais seulement si ses clés sont distinctes (sinon le split est inutile)
        if (b0.entrees.size() > bucketCapacity && aDesClésDistinctes(b0)) splitter(trouverIdx(b0));
        if (b1.entrees.size() > bucketCapacity && aDesClésDistinctes(b1)) splitter(trouverIdx(b1));
    }

    // Retourne le premier index du répertoire qui pointe vers ce bucket
    private int trouverIdx(Bucket b) {
        for (int i = 0; i < directory.size(); i++)
            if (directory.get(i) == b) return i;
        return 0;
    }

    // ── Informations ──────────────────────────────────────────────────────────

    public int getGlobalDepth() { return globalDepth; }

    /** Journal de tous les splits et doublements survenus lors de la construction. */
    public List<String> getHistoriqueSplits() { return historiqueSplits; }

    /** Nombre de buckets physiquement distincts (< taille du répertoire si des entrées partagent un bucket). */
    public int getNbBucketsDistincts() {
        // On utilise l'identité objet pour compter les buckets uniques
        Set<Bucket> distincts = Collections.newSetFromMap(new IdentityHashMap<>());
        distincts.addAll(directory);
        return distincts.size();
    }

    /**
     * Affiche l'état interne du répertoire et des buckets.
     * Utile pour visualiser les splits et le partage de buckets.
     */
    public void afficher() {
        Set<Bucket> distincts = Collections.newSetFromMap(new IdentityHashMap<>());
        distincts.addAll(directory);

        System.out.println("=== Hachage Dynamique (profondeur globale=" + globalDepth
            + ", répertoire=" + directory.size() + " entrées"
            + ", buckets distincts=" + distincts.size() + ") ===");

        // Afficher chaque bucket distinct (pas chaque entrée du répertoire)
        Set<Bucket> vus = Collections.newSetFromMap(new IdentityHashMap<>());
        for (int i = 0; i < directory.size(); i++) {
            Bucket b = directory.get(i);
            // Construire le préfixe binaire sur globalDepth bits pour cet index
            String prefixe = String.format("%" + globalDepth + "s",
                Integer.toBinaryString(i)).replace(' ', '0');
            System.out.print("  [" + prefixe + "] → ");
            if (vus.contains(b)) {
                System.out.println("(partagé, profondeur locale=" + b.localDepth + ")");
            } else {
                vus.add(b);
                System.out.println("Bucket(l=" + b.localDepth + ", "
                    + b.entrees.size() + " entrées) " + b.entrees);
            }
        }
    }

    // ── Recherche ─────────────────────────────────────────────────────────────

    /**
     * Retourne les numéros de tuples associés à la clé donnée.
     *
     * Algorithme :
     *   1. hash(clé) → index du répertoire → bucket cible
     *   2. Parcourir les entrées du bucket
     *
     * Coût : O(1) en moyenne (1 accès bucket simulé = reads++)
     */
    @Override
    public List<Integer> chercher(int cle) throws IOException {
        int idx = hash(cle);
        Bucket b = directory.get(idx);

        List<Integer> result = new ArrayList<>();
        for (EntreeIndex e : b.entrees)
            if (e.cle == cle) result.add(e.numTuple);

        // 1 accès disque simulé (lecture du bucket)
        reads++;
        return result;
    }
}
