
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

    // Classe abstraite commune aux deux types de nœuds
    private abstract class NoeudB {
        int[] cles;   // tableau des clés stockées dans ce nœud
        int   nbCles; // nombre de clés actuellement utilisées (les cases au-delà sont vides)
    }

    // Nœud interne : ne stocke que des clés de routage + des pointeurs vers ses fils
    private class NoeudInterne extends NoeudB {
        NoeudB[] enfants; // enfants[i] = sous-arbre dont toutes les clés sont < cles[i]
                          // enfants[nbCles] = sous-arbre dont toutes les clés sont >= cles[nbCles-1]

        NoeudInterne() {
            this.cles     = new int[ordre];     // ordre-1 clés max + 1 place pour split temporaire
            this.enfants  = new NoeudB[ordre + 1]; // ordre fils max + 1 pour le split temporaire
            this.nbCles   = 0;
        }
    }

    // Nœud feuille : stocke les vraies données (clé → liste de numéros de tuple)
    private class NoeudFeuille extends NoeudB {
        List<Integer>[] valeurs; // valeurs[i] = liste de numTuple pour cles[i]
                                 // (plusieurs tuples peuvent avoir la même clé)
        NoeudFeuille suivant;    // pointeur vers la feuille suivante → liste chaînée pour le range scan

        @SuppressWarnings("unchecked")
        NoeudFeuille() {
            this.cles    = new int[ordre];      // ordre-1 entrées max + 1 place pour split temporaire
            this.valeurs = new List[ordre];
            for (int i = 0; i < ordre; i++) this.valeurs[i] = new ArrayList<>();
            this.nbCles  = 0;
            this.suivant = null; // pas encore de feuille suivante
        }
    }

    // Résultat d'un split : clé à promouvoir vers le parent + nouveau nœud droit créé
    private static class ResultatSplit {
        int    clePromue;
        Object nouveauNoeud; // NoeudInterne ou NoeudFeuille selon ce qui a été splitté
        ResultatSplit(int cle, Object noeud) { this.clePromue = cle; this.nouveauNoeud = noeud; }
    }

    // ── Constructeur ─────────────────────────────────────────────────────────

    public ArbreB(int ordre) {
        this.ordre = ordre;
        // L'arbre démarre avec une unique feuille vide qui est à la fois racine et première feuille
        NoeudFeuille feuille = new NoeudFeuille();
        this.racine           = feuille;
        this.premiereFeuille  = feuille;
    }

    // ── Insertion ────────────────────────────────────────────────────────────

    /** Insère la paire (clé, numTuple) dans l'arbre. */
    public void inserer(int cle, int numTuple) {
        // On descend récursivement dans l'arbre ; si un split remonte jusqu'ici c'est que la racine a débordé
        ResultatSplit split = insererDans(racine, cle, numTuple);
        if (split != null) {
            // La racine a été divisée → on crée une nouvelle racine avec 1 clé et 2 fils
            NoeudInterne nouvelleRacine = new NoeudInterne();
            nouvelleRacine.cles[0]    = split.clePromue;      // clé séparatrice
            nouvelleRacine.enfants[0] = racine;               // ancien arbre = fils gauche
            nouvelleRacine.enfants[1] = (NoeudB) split.nouveauNoeud; // nouveau nœud = fils droit
            nouvelleRacine.nbCles     = 1;
            racine = nouvelleRacine; // l'arbre gagne un niveau
        }
    }

    // Dispatch : redirige vers le bon type de nœud
    private ResultatSplit insererDans(NoeudB noeud, int cle, int numTuple) {
        if (noeud instanceof NoeudFeuille)
            return insererDansFeuille((NoeudFeuille) noeud, cle, numTuple);
        else
            return insererDansInterne((NoeudInterne) noeud, cle, numTuple);
    }

    private ResultatSplit insererDansFeuille(NoeudFeuille f, int cle, int numTuple) {
        // Trouver la position d'insertion pour maintenir le tri croissant
        int pos = 0;
        while (pos < f.nbCles && f.cles[pos] < cle) pos++;

        if (pos < f.nbCles && f.cles[pos] == cle) {
            // Clé déjà présente : on ajoute juste le numTuple à la liste (pas de doublon de clé)
            f.valeurs[pos].add(numTuple);
            return null; // pas de split nécessaire
        }

        // Décaler toutes les entrées à droite de pos pour libérer la place
        for (int i = f.nbCles; i > pos; i--) {
            f.cles[i]    = f.cles[i - 1];
            f.valeurs[i] = f.valeurs[i - 1];
        }
        // Insérer la nouvelle entrée à la position trouvée
        f.cles[pos]    = cle;
        f.valeurs[pos] = new ArrayList<>();
        f.valeurs[pos].add(numTuple);
        f.nbCles++;

        // Overflow → split si la feuille est pleine (ordre-1 max + 1 temporaire utilisé)
        if (f.nbCles >= ordre) return splitFeuille(f);
        return null; // pas de débordement, pas de split
    }

    private ResultatSplit splitFeuille(NoeudFeuille gauche) {
        int milieu = gauche.nbCles / 2; // index de la première entrée qui part à droite
        NoeudFeuille droite = new NoeudFeuille();

        // Copier la moitié droite de la feuille gauche dans la nouvelle feuille droite
        for (int i = milieu; i < gauche.nbCles; i++) {
            droite.cles[i - milieu]    = gauche.cles[i];
            droite.valeurs[i - milieu] = gauche.valeurs[i];
            gauche.valeurs[i]          = new ArrayList<>(); // reset le slot dans la feuille gauche
        }
        droite.nbCles  = gauche.nbCles - milieu; // la droite contient la moitié haute
        gauche.nbCles  = milieu;                 // la gauche ne garde que la moitié basse

        // Insérer la nouvelle feuille droite dans la liste chaînée
        droite.suivant = gauche.suivant; // droite pointe vers l'ancienne suivante de gauche
        gauche.suivant = droite;         // gauche pointe maintenant vers droite

        // Clé de promotion = première clé de la feuille droite
        // IMPORTANT : en B+, la clé est COPIÉE (pas déplacée) → elle reste aussi dans la feuille droite
        return new ResultatSplit(droite.cles[0], droite);
    }

    private ResultatSplit insererDansInterne(NoeudInterne n, int cle, int numTuple) {
        // Trouver le bon fils à descendre : le premier enfant[pos] tel que cle >= cles[pos-1]
        int pos = n.nbCles;
        while (pos > 0 && cle < n.cles[pos - 1]) pos--;

        // Descente récursive dans le fils concerné
        ResultatSplit split = insererDans(n.enfants[pos], cle, numTuple);
        if (split == null) return null; // pas de split en dessous, rien à faire ici

        // Un fils a splitté → insérer la clé promue dans ce nœud interne
        // Décaler clés et enfants à droite pour faire de la place
        for (int i = n.nbCles; i > pos; i--) {
            n.cles[i]        = n.cles[i - 1];
            n.enfants[i + 1] = n.enfants[i];
        }
        // Placer la clé promue et le nouveau nœud droit issu du split
        n.cles[pos]        = split.clePromue;
        n.enfants[pos + 1] = (NoeudB) split.nouveauNoeud;
        n.nbCles++;

        // Vérifier si ce nœud interne déborde à son tour
        if (n.nbCles >= ordre) return splitInterne(n);
        return null;
    }

    private ResultatSplit splitInterne(NoeudInterne gauche) {
        int milieu    = gauche.nbCles / 2;
        int clePromue = gauche.cles[milieu]; // la clé du milieu monte vers le parent
                                             // IMPORTANT : en B+ nœud interne, elle est DÉPLACÉE (pas copiée)

        NoeudInterne droite = new NoeudInterne();

        // Copier les clés et enfants situés APRÈS le milieu dans le nouveau nœud droit
        for (int i = milieu + 1; i < gauche.nbCles; i++) {
            droite.cles[i - milieu - 1]    = gauche.cles[i];
            droite.enfants[i - milieu - 1] = gauche.enfants[i];
        }
        // Copier le dernier enfant du nœud gauche (celui le plus à droite après le split)
        droite.enfants[gauche.nbCles - milieu - 1] = gauche.enfants[gauche.nbCles];
        droite.nbCles = gauche.nbCles - milieu - 1; // droite = tout ce qui était après le milieu
        gauche.nbCles = milieu;                     // gauche = tout ce qui était avant le milieu

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
        reads++; // lecture de la racine (1er accès disque simulé)
        while (noeud instanceof NoeudInterne) {
            NoeudInterne n = (NoeudInterne) noeud;
            // Trouver le bon fils : on part du dernier enfant et on remonte tant que la clé est plus petite
            int pos = n.nbCles;
            while (pos > 0 && cle < n.cles[pos - 1]) pos--;
            noeud = n.enfants[pos]; // on descend dans le fils sélectionné
            reads++; // lecture du nœud enfant (1 accès disque simulé par niveau)
        }
        // On est arrivé sur une feuille : chercher la clé parmi ses entrées
        NoeudFeuille f = (NoeudFeuille) noeud;
        for (int i = 0; i < f.nbCles; i++)
            if (f.cles[i] == cle) return new ArrayList<>(f.valeurs[i]); // clé trouvée → retourner les tuples
        return new ArrayList<>(); // clé absente → liste vide
    }

    /**
     * Recherche par intervalle [min, max].
     *
     * Algorithme (cours p.82) :
     *   1. Naviguer jusqu'à la feuille contenant min  → O(hauteur) lectures
     *   2. Parcourir la liste chaînée des feuilles    → 1 lecture par feuille visitée
     */
    public List<Integer> chercherIntervalle(int min, int max) {
        // Étape 1 : navigation jusqu'à la première feuille candidate (celle qui contient min)
        NoeudB noeud = racine;
        reads++;
        while (noeud instanceof NoeudInterne) {
            NoeudInterne n = (NoeudInterne) noeud;
            int pos = n.nbCles;
            while (pos > 0 && min < n.cles[pos - 1]) pos--;
            noeud = n.enfants[pos];
            reads++;
        }
        // Étape 2 : parcours de la liste chaînée des feuilles tant qu'on est dans [min, max]
        List<Integer> result = new ArrayList<>();
        NoeudFeuille f = (NoeudFeuille) noeud;
        while (f != null) {
            reads++; // chaque feuille traversée = 1 lecture disque simulée
            boolean depasse = false;
            for (int i = 0; i < f.nbCles; i++) {
                if (f.cles[i] > max) { depasse = true; break; } // dépasse l'intervalle → on arrête
                if (f.cles[i] >= min) result.addAll(f.valeurs[i]); // dans l'intervalle → on collecte
            }
            if (depasse) break;
            f = f.suivant; // passer à la feuille suivante via le chaînage
        }
        return result;
    }

    // ── Informations ─────────────────────────────────────────────────────────

    /** Hauteur de l'arbre (nombre de niveaux, 1 = feuille seule). */
    public int hauteur() {
        int h = 1;
        NoeudB n = racine;
        // On descend toujours par le fils le plus à gauche jusqu'à une feuille
        while (n instanceof NoeudInterne) { h++; n = ((NoeudInterne) n).enfants[0]; }
        return h;
    }

    /** Affiche la structure de l'arbre (utile pour debug et cours). */
    public void afficher() {
        System.out.println("=== Arbre B+ (ordre=" + ordre + ", hauteur=" + hauteur() + ") ===");
        afficherNoeud(racine, 0);
        // Afficher la liste chaînée des feuilles (vue "bas de l'arbre")
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

    // Affichage récursif avec indentation selon le niveau (racine = niveau 0)
    private void afficherNoeud(NoeudB noeud, int niveau) {
        String indent = "  ".repeat(niveau); // 2 espaces par niveau pour visualiser la hiérarchie
        if (noeud instanceof NoeudInterne) {
            NoeudInterne n = (NoeudInterne) noeud;
            System.out.print(indent + "[Interne] clés: ");
            for (int i = 0; i < n.nbCles; i++) System.out.print(n.cles[i] + " ");
            System.out.println();
            // Afficher récursivement tous les fils (nbCles+1 enfants pour nbCles clés)
            for (int i = 0; i <= n.nbCles; i++) afficherNoeud(n.enfants[i], niveau + 1);
        } else {
            NoeudFeuille f = (NoeudFeuille) noeud;
            System.out.print(indent + "[Feuille] ");
            // Afficher chaque entrée : clé → liste de tuples
            for (int i = 0; i < f.nbCles; i++)
                System.out.print(f.cles[i] + "→" + f.valeurs[i] + "  ");
            System.out.println();
        }
    }
}
