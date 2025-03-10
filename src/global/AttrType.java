package global;

/** 
 * Enumeration class for AttrType
 */
public class AttrType {

  public static final int attrString  = 0;
  public static final int attrInteger = 1;
  public static final int attrReal    = 2;
  public static final int attrSymbol  = 3;
  public static final int attrNull    = 4;
  public static final int attrVector100D = 5;  // attribute type for 100D vectors

  public int attrType;

  /** 
   * AttrType Constructor
   * 
   * An attribute type of String can be defined as:
   * <ul>
   * <li>   AttrType attrType = new AttrType(AttrType.attrString);
   * </ul>
   * and subsequently used as:
   * <ul>
   * <li>   if (attrType.attrType == AttrType.attrString) ....</li>
   * </ul>
   *
   * @param _attrType The types of attributes available in this class
   */
  public AttrType(int _attrType) {
      if (!isValid(_attrType)) {
          throw new IllegalArgumentException("Invalid attribute type: " + _attrType);
      }
      this.attrType = _attrType;
  }

  /** 
   * Returns a string representation of the attribute type 
   */
  public String toString() {
      switch (attrType) {
          case attrString: return "attrString";
          case attrInteger: return "attrInteger";
          case attrReal: return "attrReal";
          case attrSymbol: return "attrSymbol";
          case attrVector100D: return "attrVector100D";  // New case for the vector type
          case attrNull: return "attrNull";
          default:
              throw new IllegalArgumentException("Unexpected AttrType: " + attrType);
      }
  }

  /**
   * Checks if the provided type is a valid attribute type
   * @param type The integer representing the attribute type
   * @return true if valid, false otherwise
   */
  public static boolean isValid(int type) {
      return type >= attrString && type <= attrVector100D;
  }
}
