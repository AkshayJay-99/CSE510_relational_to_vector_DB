package global;

import java.io.*;

public class Vector100Dtype {
    public static final int VECTOR_SIZE = 100;  // 100 dimensions
    private short[] values;  // Each value is a 2-byte short (-10000 to 10000)

    public Vector100Dtype() {
        values = new short[VECTOR_SIZE];
    }

    public Vector100Dtype(short[] values) {
        if (values.length != VECTOR_SIZE) {
            throw new IllegalArgumentException("Vector must have exactly 100 dimensions.");
        }
        this.values = values.clone();
    }

    public short[] getValues() {
        return values;
    }

    public void setValues(short[] values) {
        if (values.length != VECTOR_SIZE) {
            throw new IllegalArgumentException("Vector must have exactly 100 dimensions.");
        }
        this.values = values.clone();
    }

    // Convert to byte array for storage in MiniBase
    public byte[] toByteArray() throws IOException {
        ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
        DataOutputStream outStream = new DataOutputStream(byteStream);
        for (short value : values) {
            outStream.writeShort(value);
        }
        return byteStream.toByteArray();
    }

    // Convert from byte array to Vector100Dtype
    public static Vector100Dtype fromByteArray(byte[] data) throws IOException {
        ByteArrayInputStream byteStream = new ByteArrayInputStream(data);
        DataInputStream inStream = new DataInputStream(byteStream);
        short[] values = new short[VECTOR_SIZE];
        for (int i = 0; i < VECTOR_SIZE; i++) {
            values[i] = inStream.readShort();
        }
        return new Vector100Dtype(values);
    }

    // Compute Euclidean Distance between two vectors
    public static double computeDistance(Vector100Dtype v1, Vector100Dtype v2) {
        double sum = 0;
        for (int i = 0; i < VECTOR_SIZE; i++) {
            sum += Math.pow(v1.values[i] - v2.values[i], 2);
        }
        return Math.sqrt(sum);
    }
}
