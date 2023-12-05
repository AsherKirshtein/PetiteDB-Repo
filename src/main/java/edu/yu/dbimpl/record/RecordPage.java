package edu.yu.dbimpl.record;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.tx.Transaction;
import edu.yu.dbimpl.tx.TxBase;

import java.util.logging.Logger;

public class RecordPage extends RecordPageBase{

    private Transaction transaction;
    private BlockId block;
    private Layout layout;
    private static Logger logger = Logger.getLogger(RecordPage.class.getName());

    public RecordPage(TxBase tx, BlockIdBase blk, LayoutBase layout) {
        super(tx, blk, layout);
        this.transaction = (Transaction) tx;
        this.block = (BlockId) blk;
        this.layout = (Layout) layout;
        transaction.pin(blk);
        logger.info("RecordPage made");
    }

    @Override
    public int getInt(int slot, String fldname) {
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        int val = transaction.getInt(block, location);
        logger.info("getting Int " + val + " at location " + location);
        return val;
    }

    @Override
    public String getString(int slot, String fldname) {
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        String val = transaction.getString(block, location);
        logger.info("getting String " + val + " at location " + location);
        return val;
    }

    @Override
    public boolean getBoolean(int slot, String fldname) {
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        Boolean val = transaction.getBoolean(block, location);
        logger.info("getting Int " + val + " at location " + location);
        return val;
    }

    @Override
    public double getDouble(int slot, String fldname) {
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        Double val = transaction.getDouble(block, location);
        logger.info("getting Double " + val + " at location " + location);
        return val;
    }

    @Override
    public void setInt(int slot, String fldname, int val) {
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        transaction.setInt(block, location, val, true);
        logger.info("Setting int " + val + " at location " + location);
    }

    @Override
    public void setString(int slot, String fldname, String val) { //need to stop overwriting other saves in transaction
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        transaction.setString(block, location, val, true);
        logger.info("Setting String " + val + " at location " + location);
    }

    @Override
    public void setBoolean(int slot, String fldname, boolean val) {
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        transaction.setBoolean(block, location, val, true);
        logger.info("Setting Boolean " + val + " at location " + location);
    }

    @Override
    public void setDouble(int slot, String fldname, double val) {
        int offset = layout.slotSize() * slot;
        int location = layout.offset(fldname) + offset;
        transaction.setDouble(block, location, val, true);
        logger.info("Setting double " + val + " at location " + location);
    }

    @Override
    public void delete(int slot) {
        int offset = layout.slotSize() * slot;
        this.transaction.setBoolean(this.block, offset, false, true); 
    }

    @Override
    public void format() {
        int slot = 0;
        int offset = layout.slotSize() * slot;
        while (offset <= transaction.blockSize())
        {
            transaction.setBoolean(block, offset, false, false);
            Schema sch = (Schema) layout.schema();
            for (String fn : sch.fields()) 
            {
                int position = offset + layout.offset(fn);
                if (sch.type(fn) == 0)//Check to make sure this matches with schema(if it is an integer)
                {
                    this.transaction.setInt(this.block, position, 0, false);
                }
                if (sch.type(fn) == 1)//Check to make sure this matches with schema(if it is a Double)
                {
                    this.transaction.setDouble(this.block, position, 0.0, false);
                }
                if (sch.type(fn) == 2)//Check to make sure this matches with schema(if it is a Boolean)
                {
                    this.transaction.setBoolean(this.block, position, false, false);
                }
                if (sch.type(fn) == 3)//Check to make sure this matches with schema(if it is a String)
                {
                    this.transaction.setString(this.block, position, "", false);
                }
            }
         slot++;
        }
    }

    @Override
    public int nextAfter(int slot) {
        slot++;
        int offset = layout.slotSize() * (slot+1);
        while (offset <= transaction.blockSize())
        {
            //logger.info("Getting Boolean at offset " + offset);
            if (transaction.getBoolean(block, offset))
            {
                return slot;
            }
            offset = layout.slotSize() * slot;
            slot++;
      }
      return -1;
    }

    @Override
    public int insertAfter(int slot) {
        slot++;
        int offset = layout.slotSize() * (slot);
        int nextSlot = -1;
        while (offset <= transaction.blockSize())
        {
            if (!transaction.getBoolean(block, offset))
            {
                nextSlot = slot;
                break;
            }
            slot++;
            offset = layout.slotSize() * slot ;
        } 
        logger.info("Next Slot: " + nextSlot);
        if (nextSlot >= 0)
        {
            offset = layout.slotSize() * (slot);
            this.transaction.setBoolean(block, offset, true, true);
            nextSlot = slot;
        }
        return nextSlot;
    }

    @Override
    public BlockIdBase block() {
        return this.block;
    }
    
}
