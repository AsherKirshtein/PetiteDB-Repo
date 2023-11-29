package edu.yu.dbimpl.tx.recovery;

import java.util.Stack;
import java.util.logging.Logger;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.file.Page;
import edu.yu.dbimpl.log.LogIterator;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.Transaction;
import edu.yu.dbimpl.tx.TxBase;

public class RecoveryMgr extends RecoveryMgrBase {

    private Transaction transaction;
    private LogMgrBase logMgr;
    private BufferMgrBase bufferMgr;
    private int lsn;
    private int txnum;
    private Stack<LogRecord> records;

    private Logger logger = Logger.getLogger(LogIterator.class.getName());
    
    public RecoveryMgr(TxBase tx, LogMgrBase logMgr, BufferMgrBase bufferMgr)
    {
        super(tx, logMgr, bufferMgr);
        this.transaction = (Transaction) tx;
        this.txnum = this.transaction.txnum();
        this.logMgr = logMgr;
        this.bufferMgr = bufferMgr;
        lsn = 0;
        logger.info("Recovery Mgr created");
        this.records = new Stack<>();
    }

    @Override
    public void commit()
    {
        LogRecord record = new LogRecordImpl(null, txnum, LogRecordType.COMMIT);
        this.records.push(record);
        this.bufferMgr.flushAll(this.txnum);
        this.lsn++; //is this the right way to deal with the lsn?
        this.logMgr.flush(this.lsn);
        
    }

    @Override
    public void rollback() 
    {
       
        LogRecordImpl record = (LogRecordImpl) this.records.pop();
        logger.info("Committing RollBack on " + record);
        switch (record.type) {
            case SET_BOOLEAN: 
                record.page.setBoolean(record.offset, record.oldBoolean);
                break;
            case COMMIT:
                //Do nothing on a commit record
                break;
            case ROLLBACK:
                //implement writing a rollback record
                break;
            case SET_DOUBLE:
                record.page.setDouble(record.offset, record.oldDouble);
                break;
            case SET_INT:
                record.page.setInt(record.offset, record.oldInteger);
                break;
            case SET_STRING:
                record.page.setString(record.offset, record.oldString);
                break;
            default:
                break;     
        }
        this.bufferMgr.flushAll(txnum);
        this.logMgr.flush(lsn);
    }

    @Override
    public void recover() 
    {
        while (this.records.peek().op() != 0 && !records.isEmpty() && ((LogRecordImpl)this.records.peek()).offset != 0)
        {
            rollback();
        }
        //logger.info("Just did recover up until " + records.peek());
        this.bufferMgr.flushAll(txnum);
        this.logMgr.flush(lsn);
        //write a quiescent checkpoint record to the log and flush it.
    }

    @Override
    public int setInt(BufferBase buff, int offset, int newval) 
    {
        Page p = (Page) buff.contents();
        int oldVal = p.getInt(offset);
        System.out.println("OldVal " + oldVal);
        p.setInt(offset, newval);
        LogRecordImpl com = new LogRecordImpl(p,this.txnum, LogRecordType.SET_INT);
        com.setInt(oldVal,offset);
        records.push(com);
        return ((Buffer) buff).getLSN();
    }

    @Override
    public int setBoolean(BufferBase buff, int offset, boolean newval) {
        Page p = (Page) buff.contents();
        Boolean oldVal = p.getBoolean(offset);
        p.setBoolean(offset, newval);
        LogRecordImpl com = new LogRecordImpl(p, this.txnum, LogRecordType.SET_BOOLEAN);
        com.setBoolean(oldVal,offset);
        records.push(com);
        return ((Buffer) buff).getLSN();
    }

    @Override
    public int setDouble(BufferBase buff, int offset, double newval) {
        Page p = (Page) buff.contents();
        Double oldVal = p.getDouble(offset);
        p.setDouble(offset, newval);
        LogRecordImpl com = new LogRecordImpl(p, this.txnum, LogRecordType.SET_DOUBLE);
        com.setDouble(oldVal,offset);
        records.push(com);
        return ((Buffer) buff).getLSN();
    }

    @Override
    public int setString(BufferBase buff, int offset, String newval) {
        Page p = (Page) buff.contents();
        String oldVal = p.getString(offset);
        p.setString(offset, newval);
        LogRecordImpl com = new LogRecordImpl(p, this.txnum, LogRecordType.SET_STRING);
        com.setString(oldVal,offset);
        records.push(com);
        return ((Buffer) buff).getLSN();
    }

    
}
