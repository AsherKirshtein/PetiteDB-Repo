package edu.yu.dbimpl.query;

import java.util.logging.Logger;


public class ProductScan extends ProductScanBase
{
    private Scan scan1;
    private Scan scan2;
    //private static Logger logger = Logger.getLogger(ProductScan.class.getName());

    public ProductScan(Scan s1, Scan s2)
    {
        super(s1, s2);
        this.scan1 = s1;
        this.scan2 = s2;
        //logger.info("Created with\nScanner1: " + s1 + "\nScanner2: " + s2 + "\n");
    }

    @Override
    public void beforeFirst()
    {
        this.scan1.beforeFirst();
        this.scan1.next();
        this.scan2.beforeFirst();
    }

    @Override
    public boolean next()
    {
        if(this.scan2.next())
        {
            return true;
        }
        this.scan2.beforeFirst();
        if(this.scan1.next())
        {
            return true;
        }
        return false;

    }

    @Override
    public int getInt(String fldname) 
    {
        int val = -69;
        
        if (this.scan1.hasField(fldname))
        {
            val = this.scan1.getInt(fldname);
        }
        else if(this.scan2.hasField(fldname))
        {
            val = this.scan2.getInt(fldname);
        }
        //logger.info(" Getting int " + val + " from " + fldname);
        return val;
    }

    @Override
    public String getString(String fldname) 
    {
        String val = "NotThisStringAgain";
        if (this.scan1.hasField(fldname))
        {
            val = this.scan1.getString(fldname);
        }
        else if(this.scan2.hasField(fldname))
        {
            val = this.scan2.getString(fldname);
        }
        //logger.info(" Getting String " + val + " from " + fldname);
        return val.trim();
    }

    @Override
    public DatumBase getVal(String fldname) 
    {
        DatumBase val = new Datum("NotTheDruidsYouAreLookingFor");
        if (this.scan1.hasField(fldname))
        {
            val = this.scan1.getVal(fldname);
        }
        else if(this.scan2.hasField(fldname))
        {
            val = this.scan2.getVal(fldname);
        }
        return val;
    }

    @Override
    public boolean hasField(String fldname) 
    {
        if(this.scan1.hasField(fldname) || this.scan2.hasField(fldname))
        {
            return true;
        }
        return false;
    }

    @Override
    public void close() 
    {
        this.scan1.close();
        this.scan2.close();
        //logger.info("Closing scanner 1 & 2");
    }

    @Override
    public boolean getBoolean(String fldname) {
        boolean val = false;
        if (this.scan1.hasField(fldname))
        {
            val = this.scan1.getBoolean(fldname);
        }
        else if(this.scan2.hasField(fldname))
        {
            val = this.scan2.getBoolean(fldname);
        }
        return val;
    }

    @Override
    public double getDouble(String fldname)
    {
        Double val = 0.0;
        if (this.scan1.hasField(fldname))
        {
            val = this.scan1.getDouble(fldname);
        }
        else if(this.scan2.hasField(fldname))
        {
            val = this.scan2.getDouble(fldname);
        }
        return val;
    }

    @Override
    public int getType(String fldname) {
        int val = -1;
        if (this.scan1.hasField(fldname))
        {
            val = this.scan1.getType(fldname);
        }
        else if(this.scan2.hasField(fldname))
        {
            val = this.scan2.getType(fldname);
        }
        return val;
    }
    
}
