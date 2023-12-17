package edu.yu.dbimpl.query;

public class Datum extends DatumBase{

    private int sqltype;
    private Integer ival;
    private String sval;
    private Boolean bval;
    private Double dval;

    public Datum(Integer ival) {
        // fill me in with your implementation!
        super(ival);
        this.sqltype = 1;
        this.ival = ival;
      }
       
      /** Constructor: wrap a Java String.
       */
      public Datum(String sval) {
        // fill me in with your implementation!
        super(sval);
        this.sqltype = 2;
        this.sval = sval;
      }
    
      /** Constructor: wrap a Java boolean.
       */
      public Datum(Boolean bval) {
        // fill me in with your implementation!
        super(bval);
        this.sqltype = 3;
        this.bval = bval;
      }
    
      /** Constructor: wrap a Java double.
       */
      public Datum(Double dval) {
        // fill me in with your implementation!
        super(dval);
        this.sqltype = 4;
        this.dval = dval;
      }

    @Override
    public int compareTo(DatumBase o)
    {
        if(this.sqltype != o.getSQLType())
        {
            return 0;
        }
        if(this.bval == o.asBoolean() || this.dval == o.asDouble() || this.ival == o.asInt() || this.sval == o.asString())
        {
            return 1;
        }
        return 0;
    }

    @Override
    public int asInt() {
        return this.ival;
    }

    @Override
    public boolean asBoolean() {
        return this.bval;
    }

    @Override
    public double asDouble() {
        return this.dval;
    }

    @Override
    public String asString() {
       return this.sval;
    }

    @Override
    public int getSQLType()
    {
        return this.sqltype;
    }

    @Override
    public String toString() {
        String s = "";
        s += "SQL type: " + this.sqltype + "\n";
        s += "Integer value: " + this.ival + "\n";
        s += "String value: " + this.sval + "\n";
        s += "Boolean value: " + this.bval + "\n";
        s += "Double value: " + this.dval + "\n";
        return s;
    }

    @Override
    public boolean equals(Object obj)
    {
        if(!(obj instanceof Datum))
        {
            return false;
        }
        Datum other= (Datum) obj;
        return other.toString().hashCode() == this.toString().hashCode();
    }

    @Override
    public int hashCode()
    {
        return this.toString().hashCode();
    }
    
}
