package org.apache.hadoop.examples;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;


public class CompositeKeyGroupingComparator extends WritableComparator {

	protected CompositeKeyGroupingComparator() {

		super(CompositeKey.class, true);
	}
	// we only check the airline in case to let the same airline go into
	// the same reduce task
	@SuppressWarnings("rawtypes")
	@Override
	public int compare(WritableComparable w1, WritableComparable w2) {

		CompositeKey key1 = (CompositeKey) w1;
		CompositeKey key2 = (CompositeKey) w2;

		// (check on airline)
		int result = key1.getsrc().compareTo(key2.getdst()); 
		if (result == 0)
			result  = key2.getsrc().compareTo(key1.getdst()); 
		return result;
			
	}
}