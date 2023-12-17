package edu.yu.dbimpl.query;

import java.util.List;

public class ProjectScan extends ProjectScanBase{

    private Scan scan;
    private List<String> fields;
   
    public ProjectScan(Scan s1, List<String> fields)
    {
        super(s1, fields);
        this.scan = s1;
        this.fields = fields;
        for(String s: fields)
        {
            if(!s1.hasField(s))
            {
                throw new IllegalArgumentException(s1 + " doesn't contain field: " + s);
            }
        }
    }

    @Override
    public void beforeFirst() 
    {
        this.scan.beforeFirst();
    }

    @Override
    public boolean next()
    {
        return this.scan.next();    
    }

    @Override
    public int getInt(String fldname)
    {
        if (hasField(fldname))
        {
            return this.scan.getInt(fldname);
        }
        throw new IllegalArgumentException(fldname + " is not valid");
    }

    @Override
    public String getString(String fldname)
    {
        if (hasField(fldname))
        {
            return this.scan.getString(fldname).trim();
        }
        throw new IllegalArgumentException(fldname + " is not valid");
    }

    @Override
    public DatumBase getVal(String fldname)
    {
        if (hasField(fldname))
        {
            DatumBase val = this.scan.getVal(fldname);
            if(val == null)
            {
                return new Datum("");
            }
            return val;
        }
        throw new IllegalArgumentException(fldname + " is not valid");
    }

    @Override
    public boolean hasField(String fldname)
    {
        return this.fields.contains(fldname);
    }

    @Override
    public void close()
    {
        this.scan.close();
    }

    @Override
    public boolean getBoolean(String fldname) {
         if (hasField(fldname))
        {
            return this.scan.getBoolean(fldname);
        }
        throw new IllegalArgumentException(fldname + " is not valid");
    }

    @Override
    public double getDouble(String fldname) {
         if (hasField(fldname))
        {
            return this.scan.getDouble(fldname);
        }
        throw new IllegalArgumentException(fldname + " is not valid");
    }

    @Override
    public int getType(String fldname)
    {
         if (hasField(fldname))
        {
            return this.scan.getType(fldname);
        }
        throw new IllegalArgumentException(fldname + " is not valid");
    }
}
