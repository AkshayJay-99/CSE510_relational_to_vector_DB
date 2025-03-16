package iterator;
import java.lang.*;
import java.io.*;
import global.*;

/**
 *  This clas will hold single select condition
 *  It is an element of linked list which is logically
 *  connected by OR operators.
 */

public class CondExpr {
  
  /**
   * Operator like "<"
   */
  public AttrOperator op;    
  
  /**
   * Types of operands, Null AttrType means that operand is not a
   * literal but an attribute name
   */    
  public AttrType     type1;
  public AttrType     type2;    
 
  /**
   *the left operand and right operand 
   */ 
  public Operand operand1;
  public Operand operand2;
  
  /**
   * Pointer to the next element in linked list
   */    
  public CondExpr    next;   

  /**
   * Distance metric for Vector100Dtype comparisons.
   * Used only when both operands are of type attrVector100D.
   */
  public int distance; // Non-negative distance value
  
  /**
   *constructor
   */
  public  CondExpr() {
    
    operand1 = new Operand();
    operand2 = new Operand();
    
    operand1.integer = 0;
    operand2.integer = 0;
    
    distance = -1; // Default to -1 (invalid) until set in comparison logic
    next = null;
  }

  /**
   * Computes and updates the `distance` field if the operands are of type `attrVector100D`.
   */
  public void computeDistance() {
    if (type1 != null && type2 != null && 
        type1.attrType == AttrType.attrVector100D && 
        type2.attrType == AttrType.attrVector100D) {
      
      Vector100Dtype vector1 = operand1.vector100D;
      Vector100Dtype vector2 = operand2.vector100D;
      
      if (vector1 != null && vector2 != null) {
        distance = (int) Vector100Dtype.computeDistance(vector1, vector2);
      } else {
        distance = -1; // Invalid state
      }
    }
  }
}

