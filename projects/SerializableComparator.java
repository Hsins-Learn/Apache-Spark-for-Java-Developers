package com.virtualpairprogrammers;

import java.io.Serializable;
import java.util.Comparator;

public interface SerializableComparator<T> extends Comparator<T>, Serializable {
	static <T> SerializableComparator<T> serialize(SerializableComparator<T> comparator) {
		return comparator;
	}
}