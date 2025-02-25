package iterator;

import global.Vector100Dtype;
import global.AttrType; // Importing AttrType for type safety

public class Operand {
    public FldSpec symbol;       // Symbolic field (e.g., column reference)
    public String string;        // String value
    public int integer;          // Integer value
    public float real;           // Float value
    public Vector100Dtype vector100D;  // Vector100Dtype value

    // Optional: Add a type indicator for better clarity
    public AttrType type;  // Specifies which field is active (optional)

    public Operand() {}

    /**
     * Constructor for Operand to initialize with a vector.
     * @param vector The Vector100Dtype value
     */
    public Operand(Vector100Dtype vector) {
        this.vector100D = vector;
        this.type = new AttrType(AttrType.attrVector100D);  // Set type indicator
    }

    /**
     * Utility method to get the active value as a string (for debugging).
     */
    @Override
    public String toString() {
        if (vector100D != null) {
            return "Vector100Dtype: " + java.util.Arrays.toString(vector100D.getValues());
        } else if (string != null) {
            return "String: " + string;
        } else if (type != null && type.attrType == AttrType.attrInteger) {
            return "Integer: " + integer;
        } else if (type != null && type.attrType == AttrType.attrReal) {
            return "Float: " + real;
        } else {
            return "Unknown Operand";
        }
    }
}
