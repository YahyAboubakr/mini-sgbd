public class FiltreEgalite implements Operateur{
    
    private Operateur operateur;
    private int index1;
    private int index2;

    public FiltreEgalite(Operateur operateur, int index1, int index2){
        this.operateur = operateur;
        this.index1 = index1;
        this.index2 = index2;
    }

    public int getIndex1() {
        return index1;
    }

    public void setIndex1(int index1) {
        this.index1 = index1;
    }

    public int getIndex2() {
        return index2;
    }

    public void setIndex2(int index2) {
        this.index2 = index2;
    }

    public Operateur getOperateur() {
        return operateur;
    }

    public void setOperateur(Operateur operateur) {
        this.operateur = operateur;
    }

    @Override
    public void open() {
    }

    @Override
    public void close() {
    }

    @Override
    public Tuple next() {
        return new Tuple(1);
    }

}
