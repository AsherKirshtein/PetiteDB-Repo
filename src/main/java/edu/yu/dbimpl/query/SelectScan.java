package edu.yu.dbimpl.query;

import edu.yu.dbimpl.record.RID;

public class SelectScan extends SelectScanBase
{
    private Scan scan;
	private Predicate predifile;
    private UpdateScan uScan;
    
    public SelectScan(Scan scan, Predicate predicate)
    {
        super(scan, predicate);
        this.scan = scan;
        try
        {
            this.uScan = (UpdateScan) scan;
        } 
        catch(Exception e)
        {
            throw new ClassCastException("Need to put in a type of Update Scan");
        }    
        validatePredicate(predicate);  
        this.predifile = predicate;
    }

    @Override
    public void setVal(String fldname, DatumBase val)
    {
        this.uScan.setVal(fldname, val);
    }

    @Override
    public void setInt(String fldname, int val)
    {
        this.uScan.setInt(fldname, val);
    }

    @Override
    public void setString(String fldname, String val)
    {
        this.uScan.setString(fldname,val);
    }

    @Override
    public void insert()
    {
        this.uScan.insert();
    }

    @Override
    public void delete()
    {
        this.uScan.delete();
    }

    @Override
    public RID getRid()
    {
        return this.uScan.getRid();
    }

    @Override
    public void moveToRid(RID rid)
    {
        this.uScan.moveToRid(rid);
    }

    @Override
    public void beforeFirst()
    {
        this.scan.beforeFirst();
    }

    @Override
    public boolean next()
    {
        while(this.scan.next())
        {
            return true; 
        }
        return false;
    }

    @Override
    public int getInt(String fldname)
    {
        return this.scan.getInt(fldname);
    }

    @Override
    public String getString(String fldname)
    {
        return this.scan.getString(fldname).trim();    
    }

    @Override
    public DatumBase getVal(String fldname)
    {
        DatumBase val = this.scan.getVal(fldname);
        if(val == null)
        {
            return new Datum("");
        }
        return val;
    }

    @Override
    public boolean hasField(String fldname)
    {
        return this.scan.hasField(fldname);
    }

    @Override
    public void close()
    {
        this.scan.close();
        this.uScan.close();
    }

    @Override
    public void setDouble(String fldname, double val)
    {
        this.uScan.setDouble(fldname, val);
    }

    @Override
    public void setBoolean(String fldname, boolean val)
    {
        this.uScan.setBoolean(fldname, val);
    }

    @Override
    public boolean getBoolean(String fldname) {
        return this.scan.getBoolean(fldname);
    }

    @Override
    public double getDouble(String fldname) {
        return this.scan.getDouble(fldname);
    }

    @Override
    public int getType(String fldname) 
    {
       return this.scan.getType(fldname);
    }

    private void validatePredicate(Predicate p)
    {
        return;
    }
    
}
