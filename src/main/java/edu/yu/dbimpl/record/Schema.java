package edu.yu.dbimpl.record;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static java.sql.Types.BOOLEAN;
import static java.sql.Types.DOUBLE;

public class Schema extends SchemaBase
{

    private List<String> AllFields;
    private Map<String,Info> FieldInfo;

    public Schema() 
    {
        this.AllFields = new ArrayList<String>();
        this.FieldInfo = new HashMap<String,Info>();
    }

    @Override
    public void addField(String fldname, int type, int length)
    {
        AllFields.add(fldname);
        Info info = new Info(type, length);
        FieldInfo.put(fldname, info);
    }

    @Override
    public void addIntField(String fldname) 
    {
        addField(fldname, INTEGER, 0);
    }

    @Override
    public void addBooleanField(String fldname)
    {
        addField(fldname, BOOLEAN, 0);
    }

    @Override
    public void addDoubleField(String fldname)
    {
        addField(fldname, DOUBLE, 0);
    }

    @Override
    public void addStringField(String fldname, int length) 
    {
        addField(fldname, VARCHAR, length);
    }

    @Override
    public void add(String fldname, SchemaBase sch)
    {
        addField(fldname, sch.type(fldname), sch.length(fldname));
    }

    @Override
    public void addAll(SchemaBase sch)
    {
        for (String fn : sch.fields())
        {
            add(fn, sch);
        }
    }

    @Override
    public List<String> fields()
    {
        return this.AllFields;
    }

    @Override
    public boolean hasField(String fldname)
    {
        if(this.AllFields.contains(fldname))
        {
            return true;
        }
        return false;
    }

    @Override
    public int type(String fldname) 
    {
        Info info = FieldInfo.get(fldname);
        int type = info.getType();
        return type;
    }

    @Override
    public int length(String fldname)
    {
        Info info = FieldInfo.get(fldname);
        int length = info.getLength();
        return length;
    }

    private class Info
    {
        private int type; 
        private int length;

        public Info(int type, int length)
        {
           this.type = type;
           this.length = length;
        }

        public int getLength() {
            return length;
        }

        public int getType() {
            return type;
        }

        @Override
        public String toString()
        {
            return "Info: type = " + this.type + "; length = " + this.length;
        }
    }
    
}
