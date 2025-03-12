package lshfindex;
import global.*;

/**  IntegerKey: It extends the KeyClass.
 *   It defines the integer Key.
 */ 
public class Vector100DKey extends KeyClass {

    private Vector100Dtype key;

    public String toString(){
        return key.toString();
    }

    /** Class constructor
     *  @param     value   the value of the integer key to be set 
     */
    public Vector100DKey(Vector100Dtype value) 
    { 
    key=value;
    }


    /** get a copy of the integer key
     *  @return the reference of the copy 
     */
    public Vector100Dtype getKey() 
    {
        return this.key;
    }

    public int getKeyLength()
    {
        return key.getValues().length * 2;
    }



    /** set the integer key value
     */  
    public void setKey(Vector100Dtype value) 
    { 
    key= value;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Vector100DKey other = (Vector100DKey) obj;
        return this.key.equals(other.key);
    }

    @Override
    public int hashCode() {
        return key.hashCode();
    }

    public int compareTo(Vector100DKey other) {
        double distance = this.key.computeDistance(this.key, other.key);
        return Double.compare(distance, 0.0);
    }
}
