
import java.io.FileWriter;
import java.io.IOException;

public class TableDisque {

    public String filePath;
    public int taille;
    public int tupleSize;
    private int range = 100;

    public TableDisque(String filePath) {
        this.filePath = filePath;
    }

    public void ecrire(int[][] donnees) {
        this.taille = donnees.length;
        this.tupleSize = (donnees.length > 0) ? donnees[0].length : 0;
        try {
            FileWriter myWriter = new FileWriter(this.filePath);
            myWriter.write(this.taille);
            myWriter.write(this.tupleSize);
            for (int[] ligne : donnees) {
                for (int val : ligne) {
                    myWriter.write(val);
                }
            }
            myWriter.close();
            System.out.println("Table écrite : " + this.filePath);
        } catch (IOException e) {
            System.out.println("Erreur d'écriture.");
            e.printStackTrace();
        }
    }

    public void randomize(int tupleSize, int taille) {
        this.tupleSize = tupleSize;
        this.taille = taille;
        try {
            FileWriter myWriter = new FileWriter(this.filePath);
            myWriter.write(taille);   // header : taille de la table
            myWriter.write(tupleSize); // header : taille d'un tuple
            for (int i = 0; i < taille; i++) {
                Tuple t = new Tuple(tupleSize);
                for (int j = 0; j < tupleSize; j++) {
                    t.val[j] = (int)(Math.random() * this.range);
                    myWriter.write(t.val[j]);
                }
            }
            myWriter.close();
            System.out.println("Table générée : " + this.filePath);
        } catch (IOException e) {
            System.out.println("Erreur de création ou d'écriture de fichier.");
            e.printStackTrace();
        }
    }

}
