
import java.util.*;

/**
 * Arbre B+ en mémoire avec comptage de lectures disque (cours p.73-87).
 *
 * Propriétés (ordre m) :
 *   - Nœuds internes  : entre ⌈m/2⌉ et m fils, entre ⌈m/2⌉-1 et m-1 clés
 *   - Feuilles        : entre ⌈(m-1)/2⌉ et m-1 entrées (clé, List<numTuple>)
 *   - Feuilles chaînées en liste → parcours ordonné efficace (requêtes range)
 *   - Toutes les données sont dans les feuilles
 *   - Les nœuds internes ne contiennent que des clés de routage
 *
 * Simulation disque :
 *   Chaque accès à un nœud (racine, nœud interne, feuille) compte pour
 *   1 lecture (reads++). Une recherche coûte O(hauteur) = O(log_{m} N) lectures.
 */
public class ArbreB {

    private int ordre;                   // nombre max de fils d'un nœud interne
    private NoeudB racine;
    private NoeudFeuille premiereFeuille; // tête de la liste chaînée des feuilles
    public  int reads = 0;               // lectures simulées (reset entre les requêtes)

    // ── Nœuds ────────────────────────────────────────────────────────────────

    private abstract class NoeudB {
        int[] cles;
        int   nbCles;
    }

    private class NoeudInterne extends NoeudB {
        NoeudB[] enfants;

        NoeudInterne() {
            this.cles     = new int[ordre];     // ordre-1 clés + 1 place pour split temporaire
            this.enfants  = new NoeudB[ordre + 1];
            this.nbCles   = 0;
        }
    }

    private class NoeudFeuille extends NoeudB {
        List<Integer>[] valeurs; // valeurs[i] = liste de numTuple pour cles[i]
        NoeudFeuille suivant;    // chaînage ordonné des feuilles

        @SuppressWarnings("unchecked")
        NoeudFeuille() {
            this.cles    = new int[ordre];      // ordre-1 entrées + 1 place pour split temporaire
            this.valeurs = new List[ordre];
            for (int i = 0; i < ordre; i++) this.valeurs[i] = new ArrayList<>();
            this.nbCles  = 0;
            this.suivant = null;
        }
    }

    // Résultat d'un split : clé à promouvoir + nouveau nœud droit
    private static class ResultatSplit {
        int    clePromue;
        Object nouveauNoeud; // NoeudInterne ou NoeudFeuille
        ResultatSplit(int cle, Object noeud) { this.clePromue = cle; this.nouveauNoeud = noeud; }
    }

    // ── Constructeur ─────────────────────────────────────────────────────────

    public ArbreB(int ordre) {
        this.ordre = ordre;
        NoeudFeuille feuille = new NoeudFeuille();
        this.racine           = feuille;
        this.premiereFeuille  = feuille;
    }

    // ── Insertion ────────────────────────────────────────────────────────────

    /** Insère la paire (clé, numTuple) dans l'arbre. */
    public void inserer(int cle, int numTuple) {
        ResultatSplit split = insererDans(racine, cle, numTuple);
        if (split != null) {
            // La racine a été divisée → nouvelle racine avec 1 clé et 2 fils
            NoeudInterne nouvelleRacine = new NoeudInterne();
            nouvelleRacine.cles[0]    = split.clePromue;
            nouvelleRacine.enfants[0] = racine;
            nouvelleRacine.enfants[1] = (NoeudB) split.nouveauNoeud;
            nouvelleRacine.nbCles     = 1;
            racine = nouvelleRacine;
        }
    }

    private ResultatSplit insererDans(NoeudB noeud, int cle, int numTuple) {
        if (noeud instanceof NoeudFeuille)
            return insererDansFeuille((NoeudFeuille) noeud, cle, numTuple);
        else
            return insererDansInterne((NoeudInterne) noeud, cle, numTuple);
    }

    private ResultatSplit insererDansFeuille(NoeudFeuille f, int cle, int numTuple) {
        // Trouver la position d'insertion (tri croissant)
        int pos = 0;
        while (pos < f.nbCles && f.cles[pos] < cle) pos++;

        if (pos < f.nbCles && f.cles[pos] == cle) {
            // Clé dupliquée : ajouter numTuple à la liste existante
            f.valeurs[pos].add(numTuple);
            return null;
        }

        // Décaler vers la droite pour insérer
        for (int i = f.nbCles; i > pos; i--) {
            f.cles[i]    = f.cles[i - 1];
            f.valeurs[i] = f.valeurs[i - 1];
        }
        f.cles[pos]    = cle;
        f.valeurs[pos] = new ArrayList<>();
        f.valeurs[pos].add(numTuple);
        f.nbCles++;

        // Overflow → split si la feuille est pleine (ordre-1 max + 1 temporaire)
        if (f.nbCles >= ordre) return splitFeuille(f);
        return null;
    }

    private ResultatSplit splitFeuille(NoeudFeuille gauche) {
        int milieu = gauche.nbCles / 2;
        NoeudFeuille droite = new NoeudFeuille();

        // Copier la moitié droite dans la nouvelle feuille
        for (int i = milieu; i < gauche.nbCles; i++) {
            droite.cles[i - milieu]    = gauche.cles[i];
            droite.valeurs[i - milieu] = gauche.valeurs[i];
            gauche.valeurs[i]          = new ArrayList<>(); // reset slot gauche
        }
        droite.nbCles  = gauche.nbCles - milieu;
        gauche.nbCles  = milieu;

        // Chaîner les feuilles
        droite.suivant = gauche.suivant;
        gauche.suivant = droite;

        // Clé de promotion = première clé de la feuille droite (copie, pas déplacée)
        return new ResultatSplit(droite.cles[0], droite);
    }

    private ResultatSplit insererDansInterne(NoeudInterne n, int cle, int numTuple) {
        // Trouver le bon fils
        int pos = n.nbCles;
        while (pos > 0 && cle < n.cles[pos - 1]) pos--;

        ResultatSplit split = insererDans(n.enfants[pos], cle, numTuple);
        if (split == null) return null;

        // Insérer la clé promue dans ce nœud interne
        for (int i = n.nbCles; i > pos; i--) {
            n.cles[i]        = n.cles[i - 1];
            n.enfants[i + 1] = n.enfants[i];
        }
        n.cles[pos]        = split.clePromue;
        n.enfants[pos + 1] = (NoeudB) split.nouveauNoeud;
        n.nbCles++;

        if (n.nbCles >= ordre) return splitInterne(n);
        return null;
    }

    private ResultatSplit splitInterne(NoeudInterne gauche) {
        int milieu    = gauche.nbCles / 2;
        int clePromue = gauche.cles[milieu]; // la clé du milieu monte

        NoeudInterne droite = new NoeudInterne();

        // Copier les clés/enfants après milieu dans le nœud droit
        for (int i = milieu + 1; i < gauche.nbCles; i++) {
            droite.cles[i - milieu - 1]    = gauche.cles[i];
            droite.enfants[i - milieu - 1] = gauche.enfants[i];
        }
        droite.enfants[gauche.nbCles - milieu - 1] = gauche.enfants[gauche.nbCles];
        droite.nbCles = gauche.nbCles - milieu - 1;
        gauche.nbCles = milieu;

        return new ResultatSplit(clePromue, droite);
    }

    // ── Recherche ────────────────────────────────────────────────────────────

    /**
     * Recherche exacte : retourne tous les numTuple ayant la clé donnée.
     *
     * Coût : O(hauteur) lectures (un nœud par niveau), puis 0 ou 1 feuille.
     * Chaque nœud visité incrémente reads.
     */
    public List<Integer> chercher(int cle) {
        NoeudB noeud = racine;
        reads++; // lecture de la racine
        while (noeud instanceof NoeudInterne) {
            NoeudInterne n = (NoeudInterne) noeud;
            int pos = n.nbCles;
            while (pos > 0 && cle < n.cles[pos - 1]) pos--;
            noeud = n.enfants[pos];
            reads++; // lecture du nœud enfant
        }
        NoeudFeuille f = (NoeudFeuille) noeud;
        for (int i = 0; i < f.nbCles; i++)
            if (f.cles[i] == cle) return new ArrayList<>(f.valeurs[i]);
        return new ArrayList<>();
    }

    /**
     * Recherche par intervalle [min, max].
     *
     * Algorithme (cours p.82) :
     *   1. Naviguer jusqu'à la feuille contenant min  → O(hauteur) lectures
     *   2. Parcourir la liste chaînée des feuilles    → 1 lecture par feuille visitée
     */
    public List<Integer> chercherIntervalle(int min, int max) {
        // Navigation jusqu'à la première feuille candidate
        NoeudB noeud = racine;
        reads++;
        while (noeud instanceof NoeudInterne) {
            NoeudInterne n = (NoeudInterne) noeud;
            int pos = n.nbCles;
            while (pos > 0 && min < n.cles[pos - 1]) pos--;
            noeud = n.enfants[pos];
            reads++;
        }
        // Parcours de la liste chaînée des feuilles
        List<Integer> result = new ArrayList<>();
        NoeudFeuille f = (NoeudFeuille) noeud;
        while (f != null) {
            reads++; // lecture de la feuille courante
            boolean depasse = false;
            for (int i = 0; i < f.nbCles; i++) {
                if (f.cles[i] > max) { depasse = true; break; }
                if (f.cles[i] >= min) result.addAll(f.valeurs[i]);
            }
            if (depasse) break;
            f = f.suivant;
        }
        return result;
    }

    // ── Informations ─────────────────────────────────────────────────────────

    /** Hauteur de l'arbre (nombre de niveaux, 1 = feuille seule). */
    public int hauteur() {
        int h = 1;
        NoeudB n = racine;
        while (n instanceof NoeudInterne) { h++; n = ((NoeudInterne) n).enfants[0]; }
        return h;
    }

    /** Affiche la structure de l'arbre (utile pour debug et cours). */
    public void afficher() {
        System.out.println("=== Arbre B+ (ordre=" + ordre + ", hauteur=" + hauteur() + ") ===");
        afficherNoeud(racine, 0);
        // Afficher la liste chaînée des feuilles
        System.out.print("Feuilles [");
        NoeudFeuille f = premiereFeuille;
        while (f != null) {
            System.out.print("(");
            for (int i = 0; i < f.nbCles; i++) {
                System.out.print(f.cles[i]);
                if (i < f.nbCles - 1) System.out.print(",");
            }
            System.out.print(")");
            if (f.suivant != null) System.out.print(" → ");
            f = f.suivant;
        }
        System.out.println("]");
    }

    private void afficherNoeud(NoeudB noeud, int niveau) {
        String indent = "  ".repeat(niveau);
        if (noeud instanceof NoeudInterne) {
            NoeudInterne n = (NoeudInterne) noeud;
            System.out.print(indent + "[Interne] clés: ");
            for (int i = 0; i < n.nbCles; i++) System.out.print(n.cles[i] + " ");
            System.out.println();
            for (int i = 0; i <= n.nbCles; i++) afficherNoeud(n.enfants[i], niveau + 1);
        } else {
            NoeudFeuille f = (NoeudFeuille) noeud;
            System.out.print(indent + "[Feuille] ");
            for (int i = 0; i < f.nbCles; i++)
                System.out.print(f.cles[i] + "→" + f.valeurs[i] + "  ");
            System.out.println();
        }
    }
}
