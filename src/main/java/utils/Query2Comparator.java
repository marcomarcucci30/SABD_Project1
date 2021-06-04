package utils;

import java.io.Serializable;
import java.util.Comparator;

import scala.Tuple2;

public class Query2Comparator<K,V> implements Comparator<Tuple2<K, V>>, Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private Comparator<V> comparator_V;

    public Query2Comparator(Comparator<V> comparator_V) {
        this.comparator_V = comparator_V;
    }

    @Override
    public int compare(Tuple2<K, V> o1, Tuple2<K, V> o2) {
    	return comparator_V.compare(o1._2, o2._2);
    	
    }

}
