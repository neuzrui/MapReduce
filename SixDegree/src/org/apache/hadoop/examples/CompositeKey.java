package org.apache.hadoop.examples;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.lang.*; 
import java.util.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableUtils;


/**
 * This key is a composite key. The "actual" key is the src. The secondary sort
 * will be performed against the dst.
 */
// a custom compositekey class implements WritableComparable
public class CompositeKey implements WritableComparable{

	private String src;
	private String dst;

	public CompositeKey() {
	}

	public CompositeKey(String src, String dst) {

		this.src = src;
		this.dst = dst;
	}

	@Override
	public String toString() {

		return (new StringBuilder()).append(src).append(',').append(dst)
				.toString();
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		src = WritableUtils.readString(in);
		dst = WritableUtils.readString(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {

		WritableUtils.writeString(out, src);
		WritableUtils.writeString(out, dst);
	}

	@Override
	public int compareTo(Object o) {

		CompositeKey key = (CompositeKey) o;
		int result = src.compareTo(key.getdst());
		if (0 == result) {
			result = dst.compareTo(key.getsrc());
		}
		return result;
	}
	/**
	 * Gets the src.
	 * 
	 * @return src.
	 */
	public String getsrc() {

		return src;
	}

	public void setsrc(String src) {

		this.src = src;
	}

	/**
	 * Gets the dst.
	 * 
	 * @return dst
	 */
	public String getdst() {

		return dst;
	}

	public void setdst(String dst) {

		this.dst = dst;
	}


}