package utils;

import java.io.Serializable;
import java.text.ParseException;
import java.util.Comparator;

import scala.Tuple2;

public class Query1Comparator<K, V> implements Comparator<Tuple2<K, V>>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public static void main(String[] args) throws ParseException {
	}
	
	private Comparator<K> comparator_K;
	private Comparator<V> comparator_V;

    public Query1Comparator(Comparator<K> comparator_K, Comparator<V> comparator_V) {
        this.comparator_K = comparator_K;
        this.comparator_V = comparator_V;
    }

    @Override
    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
    	int c = comparator_K.compare(o1._1, o2._1);
    	if ( c == 0) {
        	return comparator_V.compare(o1._2, o2._2);
        }
    	return c;
    }
    
    
    

}
