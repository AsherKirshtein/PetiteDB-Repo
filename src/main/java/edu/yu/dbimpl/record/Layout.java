package edu.yu.dbimpl.record;

import static java.sql.Types.BOOLEAN;

import java.util.HashMap;
import java.util.Map;
//import java.util.logging.Logger;


public class Layout extends LayoutBase{

    private Schema schema;
    private Map<String,Integer> offsetMap;
    private int slotSize;
    //private static Logger logger = Logger.getLogger(Layout.class.getName());
   
   public Layout(SchemaBase schema, Map<String,Integer> offsets, int slotsize)
    {
        super(schema, offsets, slotsize);
        this.schema = (Schema) schema;
        this.offsetMap = offsets;
        this.slotSize = slotsize;
        //logger.info("Layout Class Made");
    }
   
   public Layout(SchemaBase schema)
    {
        super(schema);
        this.schema = (Schema) schema;
        this.offsetMap  = new HashMap<>();
        int position = BOOLEAN; 
        for (String fname : schema.fields())
        {
            offsetMap.put(fname, position);
            position += BOOLEAN;
        }
        this.slotSize = position;
    }

    @Override
    public SchemaBase schema() {
        return this.schema;
    }

    @Override
    public int offset(String fldname)
    {
        return this.offsetMap.get(fldname);
    }

    @Override
    public int slotSize() 
    {
        return this.slotSize;
    }
}
