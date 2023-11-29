package edu.yu.dbimpl.tx.recovery;

import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.tx.TxBase;

public class LogRecordImpl implements LogRecord{

    private int txnum;
    public int offset;
    public LogRecordType type;
    String oldString = "-1";
    Boolean oldBoolean = false;
    public Double oldDouble = -1.0;
    Integer oldInteger = -1;
    public Page page;
    private String value;

   public LogRecordImpl(Page p, int txnum, LogRecordType type)
   {
      this.txnum = txnum;
      this.type = type;
      this.offset = 0;
      this.page = p;
      this.value = "";
   }

   public void setBoolean(Boolean bool, int offset)
   {
        this.oldBoolean = bool;
        this.offset = offset;
        this.value = Boolean.toString(bool);
   }

   public void setString(String str, int offset)
   {
        this.oldString = str;
        this.offset = offset;
        this.value = str;
   }

   public void setDouble(Double d, int offset)
   {
        this.oldDouble = d;
        this.offset = offset;
        this.value = Double.toString(d);
   }

   public void setInt(int i, int offset)
   {
        this.oldInteger = i;
        this.offset = offset;
        this.value = "" + i;
   }

    @Override
    public int op()
    {
        switch (type) {
            case COMMIT:
                return 0;
            case SET_INT:
                return 1;
            case SET_DOUBLE:
                return 2;
            case SET_BOOLEAN:
                return 3;
            case SET_STRING:
                return 4;
            case ROLLBACK:
                return 5;
            default:
                return -1;
        }
    }

    @Override
    public int txNumber() {
        return this.txnum;
    }

    @Override
    public void undo(TxBase tx)
    {
        switch (type) {
            case COMMIT: //No undo for commit
                return;
            case SET_INT:
                return;
            case SET_DOUBLE:
                return;
            case SET_BOOLEAN:
                return;
            case SET_STRING:
                return;
            case ROLLBACK:
                return;
            default:
                return;
        }
    }

    @Override
    public String toString() {
        return (this.txnum + ": " + type + " with offset: " + offset + " to " + this.value);
    }
    
}
