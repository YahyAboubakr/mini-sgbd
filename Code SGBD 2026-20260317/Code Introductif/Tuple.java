
public class Tuple{// implements Comparable {

	int[] val;
	int size=0;
	private static int compareAtt;

	public Tuple(int s) {
		this.val = new int[s];
		this.size = s;
		Tuple.compareAtt = 0;
	}

	public String toString() {
		String s="";
		for(int i=0;i<this.size;i++) {
			s+=this.val[i]+"\t";
		}
		return s;
	}
/*
	public static void setCompareAtt(int a) {
		Tuple.compareAtt = a;
	}

	@Override
	public int compareTo(Object o) {
		return this.val[Tuple.compareAtt] - ((Tuple)o).val[Tuple.compareAtt];
	}*/
}
