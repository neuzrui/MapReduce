package org.apache.hadoop.examples;

public final class Pair<A, B> {
	public final A a;
	public final B b;

	private Pair(A a, B b) {
		this.a = a;
		this.b = b;
	}

	public static <A, B> Pair<A, B> make(A a, B b) {
		return new Pair<A, B>(a, b);
	}

	public int hashCode() {
		return (a != null ? a.hashCode() : 0) + 31
				* (b != null ? b.hashCode() : 0);
	}

	public boolean equals(Object o) {
		if (o == null || o.getClass() != this.getClass()) {
			return false;
		}
		Pair that = (Pair) o;
		return (a == null ? that.a == null : a.equals(that.a))
				&& (b == null ? that.b == null : b.equals(that.b));
	}
}