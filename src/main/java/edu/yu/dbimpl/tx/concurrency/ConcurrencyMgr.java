package edu.yu.dbimpl.tx.concurrency;

import java.util.HashMap;
import java.util.Map;
//import java.util.logging.Logger;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;

public class ConcurrencyMgr extends ConcurrencyMgrBase 
{
    //private static Logger logger = Logger.getLogger(ConcurrencyMgr.class.getName());
    private static GlobalLockTable lockTable = new GlobalLockTable();
    private Map<BlockId,Lock> locks = new HashMap<>();

    @Override
    public void sLock(BlockIdBase blk)
    {
        if (locks.get(blk) == null) {
            Lock lock = new Lock(blk, LockType.S_LOCK);
            lockTable.acquireLock((BlockId)blk, LockType.S_LOCK);
            locks.put((BlockId)blk, lock);
         }
    }

    @Override
    public void xLock(BlockIdBase blk)
    {
        if(locks.containsKey(blk) && locks.get(blk).getLockType() == LockType.X_LOCK)
        {
            sLock(blk);
            Lock lock = new Lock(blk, LockType.X_LOCK);
            lockTable.acquireLock((BlockId)blk, LockType.X_LOCK);
            locks.put((BlockId)blk, lock);
        }
    }

    @Override
    public void release() 
    {
        for (BlockId blk : locks.keySet())
        {
            lockTable.releaseLock(blk);
        } 
        locks.clear();
    }

    @Override
    public void resetAllLockState()
    {
        locks.clear();
        lockTable.resetAllLockState();
    }
}
