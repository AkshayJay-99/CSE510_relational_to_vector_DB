package lsh;

import java.util.Arrays;

// Dummy class to represent a vector key for indexing
public class Vector100DKey {
    private short[] vector;

    public Vector100DKey(short[] vector) {
        this.vector = vector;
    }

    @Override
    public String toString() {
        return Arrays.toString(vector);
    }
}
