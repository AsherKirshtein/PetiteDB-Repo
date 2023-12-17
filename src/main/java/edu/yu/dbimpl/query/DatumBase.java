package edu.yu.dbimpl.query;

/** Represents a "value class" encapsulating a single database value (datum)
 * where that value can be exactly one of a set of possible Java types.
 * Although the getters/setters are necessarily specified in terms of Java
 * type, the API requires that a datum be associated with an SQL type.
 * Specifically: strings MUST BE typed as java.sql.Types.VARCHAR, booleans MUST
 * be typed as java.sql.Types.BOOLEAN, doubles as java.sql.Types.DOUBLE, and
 * integers as java.sql.Types.INTEGER.  This approach follows that specified by
 * SchemaBase.
 *
 * NOTE: a DatumBase is conceptually a "value class", with all implications
 * concomitant thereto.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * @author Avraham Leff 
 */

public abstract class DatumBase implements Comparable<DatumBase> {

  /** Constructor: wrap a Java integer.
   */
  public DatumBase(final Integer ival) {
    // fill me in with your implementation!
  }
   
  /** Constructor: wrap a Java String.
   */
  public DatumBase(final String sval) {
    // fill me in with your implementation!
  }

  /** Constructor: wrap a Java boolean.
   */
  public DatumBase(final Boolean bval) {
    // fill me in with your implementation!
  }

  /** Constructor: wrap a Java double.
   */
  public DatumBase(final Double dval) {
    // fill me in with your implementation!
  }
  
  /** Returns the value encapsulated by the DatumBase
   *
   * @throws IllegalStateException if the encapsulated value is the wrong type.
   * If the encapsulated type is of type DOUBLE, returns the equivalent of
   * Double.intValue().
   */
  public abstract int asInt();

  /** Returns the value encapsulated by the DatumBase
   *
   * @throws IllegalStateException if the encapsulated value is the wrong type.
   */
  public abstract boolean asBoolean();

  /** Returns the value encapsulated by the DatumBase
   *
   * @throws IllegalStateException if the encapsulated value is the wrong type.
   * If the encapsulated type is of type INTEGER, returns the equivalent of
   * Integer.doubleValue().
   */
  public abstract double asDouble();

  /** Returns the value encapsulated by the DatumBase
   *
   * @throws IllegalStateException if the encapsulated value is the wrong type.
   */
  public abstract String asString();

  /** Return the type of the wrapped value as a value from the set of constants
   * defined by java.sql.Types
   *
   * @return the type of the datum.
   */
  public abstract int getSQLType();
}
