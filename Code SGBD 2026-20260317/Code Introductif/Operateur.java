
public interface Operateur {

	public void open();
	public Tuple next();
	public void close();
	
	/**
	 * Estime la taille (nombre de tuples) de l'opérateur.
	 * Utile pour l'optimisation des jointures (ex: choisir la table la plus petite pour la HashMap).
	 */
	public int estimateSize();
}
