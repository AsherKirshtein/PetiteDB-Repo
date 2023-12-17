package edu.yu.dbimpl.metadata;

import java.util.HashMap;
import java.util.Map;

import edu.yu.dbimpl.record.Layout;
import edu.yu.dbimpl.record.LayoutBase;
import edu.yu.dbimpl.record.Schema;
import edu.yu.dbimpl.record.SchemaBase;
import edu.yu.dbimpl.record.TableScan;
import edu.yu.dbimpl.tx.TxBase;

public class TableMgr extends TableMgrBase{

    private boolean isNew;
    private Schema tschema;
    private Schema schemaFC;
    private Layout layout_T;
    private Layout layout_F;
    
    public TableMgr(boolean isNew, TxBase tx)
    {
        super(isNew, tx);
        this.isNew = isNew;
        setLayout_T();
        setLayout_F();
        if(this.isNew)
        {
            createTable(TABLE_META_DATA_TABLE, this.tschema, tx);
            createTable(FIELD_META_DATA_TABLE, this.schemaFC, tx);
        }
    }

    @Override
    public LayoutBase getLayout(String tblname, TxBase tx)
    {
        int size = getSlotSize(tblname, tx);

        Schema sch = new Schema();
        Map<String,Integer> offsets = new HashMap<>();
        TableScan fScan = new TableScan(tx, FIELD_META_DATA_TABLE, this.layout_F);

        while(fScan.next())
        {
            if(fScan.getString(TABLE_NAME).contains(tblname))
            {
                String fldname = fScan.getString("fldname");
                int fldtype = fScan.getInt("type");
                int fldlen = fScan.getInt("length");
                int offset = fScan.getInt("offset");
                offsets.put(fldname, offset);
                sch.addField(fldname, fldtype, fldlen);
            }
        }
        fScan.close();
        return new Layout(sch, offsets, size);
   }

    @Override
    public void createTable(String tblname, SchemaBase schema, TxBase tx)
    {
        Layout layout = new Layout(schema);
        TableScan tScan = new TableScan(tx, TABLE_META_DATA_TABLE, this.layout_T);

        tScan.insert();
        tScan.setString(TABLE_NAME, tblname);
        tScan.setInt("slotsize", layout.slotSize());
        tScan.close();
        TableScan fScan = new TableScan(tx, FIELD_META_DATA_TABLE, this.layout_F);
        for (String fn : schema.fields())
        {
            fScan.insert();
            fScan.setString(TABLE_NAME, tblname);
            fScan.setString("fldname", fn);
            fScan.setInt("type",   schema.type(fn));
            fScan.setInt("length", schema.length(fn));
            fScan.setInt("offset", layout.offset(fn));
        }
        fScan.close();
    }

    private int getSlotSize(String tn, TxBase t)
    {
        int size = 0;
        TableScan tScan = new TableScan(t, TABLE_META_DATA_TABLE, this.layout_T);
        while(tScan.next())
        {
            if(tScan.getString(TABLE_NAME).contains(tn)) 
            {
                size = tScan.getInt("slotsize");
                break;
            }
        }
        tScan.close();
        return size;
    }

    private void setLayout_T()
    {
        this.tschema = new Schema();
        this.tschema.addStringField(TABLE_NAME, MAX_LENGTH_PER_NAME);
        this.tschema.addIntField("slotsize");
        this.layout_T = new Layout(tschema);
    }

    private void setLayout_F()
    {
        this.schemaFC = new Schema();
        this.schemaFC.addStringField(TABLE_NAME, MAX_LENGTH_PER_NAME);
        this.schemaFC.addStringField("fldname", MAX_LENGTH_PER_NAME);
        this.schemaFC.addIntField("type");
        this.schemaFC.addIntField("length");
        this.schemaFC.addIntField("offset");
        this.layout_F = new Layout(schemaFC);
    }
    
}
//using .contains might be slower but for some reason the string gets modified when stored and I need to look into it
//other than that looks good