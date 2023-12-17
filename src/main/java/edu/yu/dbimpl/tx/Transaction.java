package edu.yu.dbimpl.tx;

//import java.util.logging.Logger;

import edu.yu.dbimpl.buffer.Buffer;
import edu.yu.dbimpl.buffer.BufferBase;
import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.concurrency.ConcurrencyMgr;
import edu.yu.dbimpl.tx.recovery.RecoveryMgr;

public class Transaction extends TxBase {

    private FileMgrBase fileMgr;
    private BufferMgr bufferMgr;
    private ConcurrencyMgr concurrencyMgr;
    private BufferTable totalBuffers;//need to implement this to hold all buffers

    private RecoveryMgr recoveryMgr;

    private static int txnum = 0;//figure out how to make these values increment monotonically
    //private static Logger logger = Logger.getLogger(Transaction.class.getName());
    int currentBlk = 0;


    public Transaction(FileMgrBase fm, LogMgrBase lm, BufferMgrBase bm) 
    {
        super(fm, lm, bm);
        this.fileMgr = fm;
        this.bufferMgr = (BufferMgr) bm;
        this.concurrencyMgr = new ConcurrencyMgr();
        this.recoveryMgr = new RecoveryMgr(this, lm, bm); 
        txnum = incrementTX();
        this.totalBuffers = new BufferTable((BufferMgr)bm);
        //logger.info("Transaction created");
    }

    @Override
    public int txnum() 
    {
        return txnum;
    }

    @Override
    public void commit()
    {
        this.recoveryMgr.commit();
        //release all locks
        this.concurrencyMgr.release();
         //unpin any pinned buffers
        this.totalBuffers.reset();
        //logger.info("Just did commit on txnum: " + this.txnum());
       
    }

    @Override
    public void rollback() 
    {
        //undo modified values
        //flush those buffers
        //write and flush a rollback record to the log
        this.recoveryMgr.rollback();
        //release all locks
        this.concurrencyMgr.release();
        //unpin any pinned buffers
        this.totalBuffers.reset();
        //logger.info("Just Did rollback");
    }

    @Override
    public void recover() 
    {
        this.bufferMgr.flushAll(txnum);
        //traverse the log
        // roll back all uncommitted transactions
        //write a checkpoint record to the log
        recoveryMgr.recover();
        //logger.info("Just did recover");
    }

    @Override
    public void pin(BlockIdBase blk) 
    {
        this.totalBuffers.pin(blk);
    }

    @Override
    public void unpin(BlockIdBase blk) 
    {
        this.totalBuffers.unpin(blk);    
    }

    @Override
    public int getInt(BlockIdBase blk, int offset) {
        //acquires an "s-lock" on behalf of the client
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        int val = buffer.contents().getInt(offset);
        //logger.info(blk + " getting Int " + val + " at offset " + offset);
        return val;
    }

    @Override
    public boolean getBoolean(BlockIdBase blk, int offset) {
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        return buffer.contents().getBoolean(offset);
    }

    @Override
    public double getDouble(BlockIdBase blk, int offset) {
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        //logger.info(blk + " getting Double " + buffer.page.getDouble(offset) + " at offset " + offset);
        return buffer.contents().getDouble(offset);
    }

    @Override
    public String getString(BlockIdBase blk, int offset) {
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        String val =buffer.contents().getString(offset);
        //logger.info(blk + " added " + val + " at offset " + offset);
        return val;
    }

    @Override
    public void setInt(BlockIdBase blk, int offset, int val, boolean okToLog)
    {
        if(offset + 4 >=  blockSize())
        {
            throw new IndexOutOfBoundsException("Can't insert into page because the offset " + offset + "plus the int would exceed the blocksize");
        }
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        int lsn = -69;
        if (okToLog)
        {
            lsn = recoveryMgr.setInt((BufferBase) buffer, offset, val);
        }
        buffer.page.setInt(offset, val);
        buffer.setModified(txnum, lsn);
        //logger.info(blk + " added " + val + " at offset " + offset);
    }

    @Override
    public void setBoolean(BlockIdBase blk, int offset, boolean val, boolean okToLog) {
        if(offset + 4 >=  blockSize())
        {
            throw new IndexOutOfBoundsException("Can't insert into page because the offset " + offset + " plus the boolean would exceed the blocksize");
        }
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        int lsn = -69;
        if (okToLog)
        {
            lsn = recoveryMgr.setBoolean((BufferBase) buffer, offset, val);
        }
        buffer.contents().setBoolean(offset, val);
        buffer.setModified(txnum, lsn);
        //logger.info(blk + " added " + val + " at offset " + offset);
    }

    @Override
    public void setDouble(BlockIdBase blk, int offset, double val, boolean okToLog) {
        if(offset + 8 >=  blockSize())
        {
            throw new IndexOutOfBoundsException("Can't insert into page because the offset " + offset + " the double would exceed the blocksize");
        }
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        int lsn = -69;
        if (okToLog)
        {
            lsn = recoveryMgr.setDouble((BufferBase) buffer, offset, val);
        }
        buffer.contents().setDouble(offset, val);
        buffer.setModified(txnum, lsn);
        //buffer.page.setDouble(offset, val);
        //logger.info(blk + " added " + val + " at offset " + offset);
    }

    @Override
    public void setString(BlockIdBase blk, int offset, String val, boolean okToLog)
    {
        if(offset + val.length() >=  blockSize())
        {
            throw new IndexOutOfBoundsException("Can't insert into page because the offset " + offset + " the String would exceed the blocksize");
        }
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = totalBuffers.getBuffer(blk);
        int lsn = -69;
        if (okToLog)
        {
            //logger.info("String: " + val + " offset: " + offset + " length: " + val.length());
            lsn = recoveryMgr.setString((BufferBase) buffer, offset, val);
        }
        buffer.contents().setString(offset, val);
        buffer.setModified(txnum, lsn);
        //buffer.page.setString(offset, val);
        //logger.info(blk + " added " + val + " at offset " + offset);
    }

    @Override
    public int size(String filename) 
    {
       return this.fileMgr.length(filename);
    }

    @Override
    public BlockIdBase append(String filename) 
    {
        BlockId blk = new BlockId(filename, 0);
        this.concurrencyMgr.xLock(blk);
        BlockIdBase b = fileMgr.append(filename);
        return b;
    }

    @Override
    public int blockSize() 
    {
        return this.fileMgr.blockSize();
    }

    @Override
    public int availableBuffs() 
    {
        return this.bufferMgr.available();
    }

    private synchronized int incrementTX() {
        txnum++;
        return txnum;
     }
    
}
