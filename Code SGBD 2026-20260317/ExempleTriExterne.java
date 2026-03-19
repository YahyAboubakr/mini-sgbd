
import java.io.*;
import java.util.*;

/**
 * Démonstration et validation du Tri Externe.
 * Utilise table1 du prof (25 tuples, 4 attributs).
 *
 * Tests :
 *   1. Tri sur col0, buffer = 3 blocs  (cas standard)
 *   2. Tri sur col1, buffer = 5 blocs  (plus de mémoire = moins de passes)
 *   3. Comparaison du coût I/O avec différentes tailles de buffer
 */
public class ExempleTriExterne {

    static final String BASE = "/home/jules/Documents/4A-Apprentis/SGBD/mini-sgbd/Code SGBD 2026-20260317/Table Disque et exemples/";
    static final int BLOCK_SIZE = 4;

    public static void main(String[] args) throws IOException {

        TableDisque table1 = new TableDisque(BASE + "table1");
        FileReader meta = new FileReader(BASE + "table1");
        table1.taille    = meta.read();
        table1.tupleSize = meta.read();
        meta.close();

        int N = table1.taille;
        System.out.println("╔══════════════════════════════════════════════════╗");
        System.out.println("║              TRI EXTERNE — TESTS                 ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.println("Table : table1  (" + N + " tuples, "
            + table1.tupleSize + " attributs, blockSize=" + BLOCK_SIZE + ")");
        int blocs = (int) Math.ceil((double) N / BLOCK_SIZE);
        System.out.println("Cout theorique 2 passes : 4 x " + blocs + " = " + (4*blocs) + " I/O");
        System.out.println();

        // ── Test 1 : buffer = 3 blocs ─────────────────────────────────────
        System.out.println("── Test 1 : tri sur col0, buffer=3 blocs ────────────");
        TriExterne tri1 = new TriExterne(table1, 0, 3, BLOCK_SIZE);
        tri1.open();
        List<Tuple> result1 = new ArrayList<>();
        Tuple t;
        while ((t = tri1.next()) != null) result1.add(t);
        tri1.close();

        System.out.println("Resultat trie par col0 :");
        for (Tuple r : result1) System.out.println("   " + r);
        System.out.println("Lectures (reads)  : " + tri1.reads);
        System.out.println("Ecritures (writes): " + tri1.writes);
        System.out.println("Total I/O         : " + (tri1.reads + tri1.writes));
        System.out.println("Trie correctement : " + (verifTri(result1, 0) ? "OK" : "ERREUR"));
        System.out.println("Nb tuples correct : " + (result1.size() == N ? "OK" : "ERREUR"));

        // ── Test 2 : buffer = 5 blocs ─────────────────────────────────────
        System.out.println("\n── Test 2 : tri sur col1, buffer=5 blocs ────────────");
        TriExterne tri2 = new TriExterne(table1, 1, 5, BLOCK_SIZE);
        tri2.open();
        List<Tuple> result2 = new ArrayList<>();
        while ((t = tri2.next()) != null) result2.add(t);
        tri2.close();

        System.out.println("Resultat trie par col1 :");
        for (Tuple r : result2) System.out.println("   " + r);
        System.out.println("Lectures  : " + tri2.reads
            + "  Ecritures : " + tri2.writes
            + "  Total : " + (tri2.reads + tri2.writes));
        System.out.println("Trie correctement : " + (verifTri(result2, 1) ? "OK" : "ERREUR"));

        // ── Bilan ─────────────────────────────────────────────────────────
        System.out.println("\n╔══════════════════════════════════════════════════╗");
        System.out.println("║  BILAN I/O                                       ║");
        System.out.println("╚══════════════════════════════════════════════════╝");
        System.out.printf("FullScan seul    : %2d lectures (pas d'ecriture)%n", blocs);
        System.out.printf("TriExterne B=3   : %2d lect + %2d ecrit = %2d I/O%n",
            tri1.reads, tri1.writes, tri1.reads + tri1.writes);
        System.out.printf("TriExterne B=5   : %2d lect + %2d ecrit = %2d I/O%n",
            tri2.reads, tri2.writes, tri2.reads + tri2.writes);
        System.out.println();
        System.out.println("Le tri externe coute plus qu'un simple scan car il");
        System.out.println("ecrit et relit les donnees. Avantage : fonctionne");
        System.out.println("meme quand la table ne tient pas en RAM.");
    }

    static boolean verifTri(List<Tuple> tuples, int col) {
        for (int i = 1; i < tuples.size(); i++)
            if (tuples.get(i).val[col] < tuples.get(i-1).val[col]) return false;
        return true;
    }
}
