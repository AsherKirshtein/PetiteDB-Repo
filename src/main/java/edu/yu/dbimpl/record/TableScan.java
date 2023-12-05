package edu.yu.dbimpl.record;

import java.util.logging.Logger;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.log.LogIterator;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.tx.Transaction;
import edu.yu.dbimpl.tx.TxBase;

public class TableScan extends TableScanBase
{
    private Transaction transaction;
    private Layout layout;
    private RecordPage recordPage;
    private String tableName;
    private int slot;
    private static Logger logger = Logger.getLogger(LogIterator.class.getName());

    public TableScan(TxBase tx, String tblname, LayoutBase layout)
    {
        super(tx, tblname, layout);
        this.transaction = (Transaction) tx;
        this.slot = -1;
        this.tableName = tblname;
        this.layout = (Layout) layout;
        if(this.transaction.size(this.tableName + ".tbl") < 1)//if we are at the first block
        {
            close();
            BlockId blk = (BlockId) tx.append(this.tableName + ".tbl");
            this.recordPage = new RecordPage(tx, blk, layout);
            this.recordPage.format();
        }
        else
        {
            close();
            BlockId blk = new BlockId(this.tableName + ".tbl", 0);
            this.recordPage = new RecordPage(tx, blk, layout);
        }
    }

    @Override
    public void setVal(String fldname, DatumBase val)
    {
        switch (val.getSQLType())
        {
            case 1://val is an int(look in datum class for value assignment)
                setInt(fldname, val.asInt());
                break;
            case 2: //val is a String
                setString(fldname, val.asString());
                break;
            case 3: //val is a boolean
                setBoolean(fldname, val.asBoolean());
                break;
            case 4: //val is a Double
                setDouble(fldname, val.asDouble());
                break;
            default: //something is wrong
                break;

        }         
    }

    @Override
    public void setInt(String fldname, int val)
    {
        this.recordPage.setInt(this.slot, fldname, val);
    }

    @Override
    public void setString(String fldname, String val) 
    {
        this.recordPage.setString(this.slot, fldname, val);
    }

    @Override
    public void insert() 
    {
        this.slot = this.recordPage.insertAfter(this.slot);
        while (this.slot < 0)
        {
         if (this.recordPage.block().number() == this.transaction.size(this.tableName + ".tbl") - 1)
         { 
            close();
            BlockIdBase block = this.transaction.append(this.tableName + ".tbl");
            this.recordPage = new RecordPage(this.transaction, block, layout);
            this.recordPage.format();
            this.slot = -1;
         }
         else
         { 
            int blockNum = this.recordPage.block().number()+1;
            close();
            BlockId blk = new BlockId(this.tableName + ".tbl", blockNum);
            this.recordPage = new RecordPage(this.transaction, blk, layout);
            this.slot = -1;
         }
         this.slot = this.recordPage.insertAfter(this.slot);
      }
    }

    @Override
    public void delete()
    {
        this.recordPage.delete(this.slot);
    }

    @Override
    public RID getRid()
    {
        int blockNumber = this.recordPage.block().number();
        RID rid = new RID(blockNumber, this.slot);
        return rid;
    }

    @Override
    public void moveToRid(RID rid)
    {
        close();
        BlockId blk = new BlockId(this.tableName + ".tbl", rid.blockNumber());
        this.recordPage = new RecordPage(this.transaction, blk, layout);
        this.slot = rid.slot();
    }

    @Override
    public void beforeFirst()
    {
        close();
        BlockId block = new BlockId(this.tableName + ".tbl", 0);
        this.recordPage = new RecordPage(this.transaction, block, layout);
        this.slot = -1;
    }

    @Override
    public boolean next()
    {
        this.slot = this.recordPage.nextAfter(this.slot);
        
        while (this.slot < 0)
        {
            logger.info("slot: " + this.slot);
            if (this.recordPage.block().number() == this.transaction.size(this.tableName + ".tbl") - 1)//we are at the last block
            {
                return false;
            }
            int blockNum = this.recordPage.block().number()+1;

            close();
            BlockId blk = new BlockId(this.tableName + ".tbl", blockNum);
            this.recordPage = new RecordPage(this.transaction, blk, layout);
            this.slot = -1;
            this.slot = this.recordPage.nextAfter(this.slot);
        }
        return true;
    }

    @Override
    public int getInt(String fldname)
    {
        int val = this.recordPage.getInt(this.slot, fldname);
        return val;
    }

    @Override
    public String getString(String fldname)
    {
        String val = this.recordPage.getString(this.slot, fldname);
        return val;
    }

    @Override
    public DatumBase getVal(String fldname)
    {
        SchemaBase scema = this.layout.schema();
        int sqlType = scema.type(fldname);
        DatumBase db = null;
        switch (sqlType)
        {
            case 1://val is an int(look in datum class for value assignment)
                int ival = getInt(fldname);
                db = new Datum(ival);
                return db;
            case 2: //val is a String
                String sval = getString(fldname);
                db = new Datum(sval);
                return db;
            case 3: //val is a boolean
                Boolean bval = getBoolean(fldname);
                db = new Datum(bval);
                return db;
            case 4: //val is a Double
                Double dval = getDouble(fldname);
                db = new Datum(dval);
                return db;
            default: //something is wrong
                return db;

        }       
    }

    @Override
    public boolean hasField(String fldname)
    {
        SchemaBase sb = layout.schema();
        if(sb.hasField(fldname))
        {
            return true;
        }
        return false;
    }

    @Override
    public void close() 
    {
        if (this.recordPage == null)
        {
            return;
        }
        BlockIdBase recordBlock = this.recordPage.block();
        this.transaction.unpin(recordBlock);
    }

    @Override
    public String getTableFileName()
    {
        return this.tableName + ".tbl";
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return "Name: " + this.tableName + " Slot: " + this.slot + " Blocks " + this.transaction.size(this.tableName + ".tbl");
    }
    
}
