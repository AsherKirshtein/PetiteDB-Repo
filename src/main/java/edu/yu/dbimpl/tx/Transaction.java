package edu.yu.dbimpl.tx;

import java.util.ArrayList;
import java.util.logging.Logger;

import edu.yu.dbimpl.buffer.Buffer;
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
    private ArrayList<Buffer> totalBuffers = new ArrayList<>();

    private RecoveryMgr recoveryMgr;

    private static int txnum = 0;//figure out how to make these values increment monotonically
    private static Logger logger = Logger.getLogger(Transaction.class.getName());


    public Transaction(FileMgrBase fm, LogMgrBase lm, BufferMgrBase bm) 
    {
        super(fm, lm, bm);
        this.fileMgr = fm;
        //this.logMgr = lm;
        this.bufferMgr = (BufferMgr) bm;
        this.concurrencyMgr = new ConcurrencyMgr();
        txnum = incrementTX();
        logger.info("Transaction created");
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
        for(Buffer b: this.totalBuffers)
        {
            b.unpin();
        }
       
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
        for(Buffer b: this.totalBuffers)
        {
            b.unpin();
        }
    }

    @Override
    public void recover() 
    {
        this.bufferMgr.flushAll(txnum);
        //traverse the log
        // roll back all uncommitted transactions
        //write a checkpoint record to the log
        recoveryMgr.recover();
    }

    @Override
    public void pin(BlockIdBase blk) 
    {
        this.bufferMgr.pin(blk);
    }

    @Override
    public void unpin(BlockIdBase blk) 
    {
        this.totalBuffers.get(blk.number()).unpin();
    }

    @Override
    public int getInt(BlockIdBase blk, int offset) {
        //acquires an "s-lock" on behalf of the client
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.get(blk.number());
        return buffer.page.getInt(offset);
    }

    @Override
    public boolean getBoolean(BlockIdBase blk, int offset) {
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.get(blk.number());
        return buffer.page.getBoolean(offset);
    }

    @Override
    public double getDouble(BlockIdBase blk, int offset) {
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.get(blk.number());
        return buffer.page.getDouble(offset);
    }

    @Override
    public String getString(BlockIdBase blk, int offset) {
        this.concurrencyMgr.sLock(blk);
        Buffer buffer = totalBuffers.get(blk.number());
        return buffer.page.getString(offset);
    }

    @Override
    public void setInt(BlockIdBase blk, int offset, int val, boolean okToLog)
    {
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = this.totalBuffers.get(blk.number());
        buffer.page.setDouble(offset, val);
    }

    @Override
    public void setBoolean(BlockIdBase blk, int offset, boolean val, boolean okToLog) {
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = this.totalBuffers.get(blk.number());
        buffer.page.setBoolean(offset, val);
    }

    @Override
    public void setDouble(BlockIdBase blk, int offset, double val, boolean okToLog) {
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = this.totalBuffers.get(blk.number());
        buffer.page.setDouble(offset, val);
    }

    @Override
    public void setString(BlockIdBase blk, int offset, String val, boolean okToLog)
    {
        this.concurrencyMgr.xLock(blk);
        Buffer buffer = this.totalBuffers.get(blk.number());
        buffer.page.setString(offset, val);
    }

    @Override
    public int size(String filename) 
    {
       return this.fileMgr.length(filename);
    }

    @Override
    public BlockIdBase append(String filename) 
    {
        BlockId blk = new BlockId(filename, blockSize());
        this.concurrencyMgr.xLock(blk);
        this.bufferMgr.pin(blk);
        unpin(blk);
        return blk;
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
