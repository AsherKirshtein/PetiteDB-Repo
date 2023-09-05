package edu.yu.dbimpl.record;

import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.tx.TxBase;

/** Specifies the public API for the RecordPage implementation by requiring all
 * RecordPage implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A RecordPage manages "record-slots" at block granularity.
 *
 * Implementations MUST use the slotted page design for fixed-length fields
 * discussed in lecture and textbook.
 *
 * Design note: the only methods available for manipulating the crucial
 * "in-use" flags are insertAfter() and delete().  Setting slot-values without
 * first setting the "in-use" flag will not accomplish what you're trying to
 * accomplish.
 * 
 * Design note: can help to consider the RecordPageBase API as moving parts of
 * the TxBase API "up a level" such that clients can get/set values in terms of
 * field names rather than block locations.
 */

public abstract class RecordPageBase {

  /** When searching for a slot in a record page, clients use this constant to
   * specify that the search should all slots in that page.  This value
   * specifies that the cursor is positioned before the first valid slot.
   */
  public static final int BEFORE_FIRST_SLOT = -1;

  /** Constructor.
   *
   * IMPORTANT: If the encapsulated block is being used for the first time, the
   * CLIENT is responsible for invoking "format()" before invoking other
   * methods on the RecordPage.  Failure to do so implies that the semantics of
   * subsequent method invocations are undefined.
   *
   * @param tx Defines the transaction scope in which operations on the block
   * will take place.  The client passing the tx continues to be responsible
   * for transaction lifeycle behavior: commit versus rollback.  The
   * RecordPageBase implementation uses the transaction to "get its work done".
   * @param blk The block in which the record is stored
   * @param layout Holds the physical and logical record schema
   */
  public RecordPageBase(TxBase tx, BlockIdBase blk, LayoutBase layout) {
    // fill me in in in your implementation class!
  }

  /** Return the integer value stored for the specified field of a specified
   * slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname the name of the field, must be defined on the page's layout.
   * @return the integer stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated.
   */
  public abstract int getInt(int slot, String fldname);

  /** Return the string value stored for the specified field of the specified
   * slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname the name of the field, must be defined on the page's layout.
   * @return the string stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated.   
   */
  public abstract String getString(int slot, String fldname);

  /** Return the boolean value stored for the specified field of a specified
   * slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname the name of the field, must be defined on the page's layout.
   * @return the boolean stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated.   
   */
  public abstract boolean getBoolean(int slot, String fldname);

  /** Return the double value stored for the specified field of a specified
   * slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname the name of the field, must be defined on the page's layout.
   * @return the double stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated.   
   */
  public abstract double getDouble(int slot, String fldname);
  
  /** Stores an integer at the specified field of the specified slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname must be defined on the page's layout
   * @param val the integer value stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated.   
   */
  public abstract void setInt(int slot, String fldname, int val);

  /** Stores a string at the specified field of the specified slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname must be defined on the page's layout
   * @param val the string value stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated or if the
   * logical length of the string exceeds the logical length specified in the
   * schema.
   */
  public abstract void setString(int slot, String fldname, String val);

  /** Stores a boolean at the specified field of the specified slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname must be defined on the page's layout
   * @param val the string value stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated.      
   */
  public abstract void setBoolean(int slot, String fldname, boolean val);

  /** Stores a double at the specified field of the specified slot.
   *
   * @param slot specifies location storing the value, must be non-negative.
   * @param fldname must be defined on the page's layout
   * @param val the string value stored in that field
   * @throws IllegalArgumentException if pre-conditions are violated.   
   */
  public abstract void setDouble(int slot, String fldname, double val);
  
  /** Deletes the specified slot by setting its "in-use" flag to "not in use".
   *
   * @param slot uniquely identifies the record slot.
   * @throws IllegalArgumentException if slot is negative.
   */
  public abstract void delete(int slot);
   
  /** Initializes all record slots in the block: i.e., all integers are set to
   * 0, all booleans to false, all doubles to 0.0, all strings to the empty
   * string, and all slots to "empty".
   *
   * These operations should not be logged (from a transactional point of view)
   * because we consider the old values to be meaningless.
   */ 
  public abstract void format();

  /** Search the block, starting from the specified slot, for an "in-use" slot.
   *
   * @param slot uniquely identifies the record slot from which the search will
   * begin.  To search from the beginning of the block, set this parameter to
   * BEFORE_FIRST_SLOT.
   * @return Returns the location of the first "in-use" slot AFTER the
   * specified slot: if all slots are "empty", returns -1 as a sentinel value.
   * @throws IllegalArgumentException if slot is less than -1.
   */
  public abstract int nextAfter(int slot);
 
  /** Search the block, starting from the specified slot, for an "empty" slot.
   *
   * @param slot uniquely identifies the record slot from which the search will
   * begin.  To search from the beginning of the block, set this parameter to
   * BEFORE_FIRST_SLOT.
   * @return Returns the location of the first "empty" slot AFTER the specified
   * slot AND sets the state of the slot to "in-use"; if all slots are
   * "in-use", returns -1 as a sentinel value.
   * @throws IllegalArgumentException if slot is less than -1.   
   */
  public abstract int insertAfter(int slot);
  
  /** Returns the block associated with the RecordPageBase instance.
   *
   * @return the block
   */
  public abstract BlockIdBase block();
}
