package edu.yu.dbimpl.tx.concurrency;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Logger;

import edu.yu.dbimpl.file.BlockId;


class GlobalLockTable{
    private Map<BlockId, Lock> lockTable;
    private Map<BlockId, Boolean> multipleSlock;
    private static Logger logger = Logger.getLogger(GlobalLockTable.class.getName());

    public GlobalLockTable() {
        lockTable = new ConcurrentHashMap<>();
        multipleSlock = new ConcurrentHashMap<>();
    }

    public synchronized void acquireLock(BlockId b, LockType type)
    {
        //logger.info(b + " trying to acquire " + type);
        int waitFor = 5000;
        long startTime = System.currentTimeMillis();
        if(type == LockType.S_LOCK)
        {
            while(lockTable.containsKey(b) && lockTable.get(b).getLockType() == LockType.X_LOCK && !(System.currentTimeMillis() - startTime > waitFor))
            {
                try 
                {
                    //logger.info("Waiting for X_Lock to release");
                    wait(waitFor);
                } 
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
            if(lockTable.containsKey(b) && lockTable.get(b).lockType == LockType.X_LOCK)
            {
                throw new LockAbortException();
            }

            Lock lock = lockTable.get(b);
            if(lock == null)
            {
                lock = new Lock(b, type);
                multipleSlock.put(b, false);
            }
            else if(lock.getLockType() != LockType.X_LOCK)
            {
                multipleSlock.put(b, true);
            }
            lockTable.put(b, lock);
        }

        else if(type == LockType.X_LOCK)
        {
            while(multipleSlock.get(b) && !(System.currentTimeMillis() - startTime > waitFor))
            {
                try 
                {
                    //logger.info("waiting for Lock to release");
                    wait(waitFor);
                } 
                catch (InterruptedException e)
                {
                    e.printStackTrace();
                }
            }
            if(multipleSlock.get(b))
            {
                throw new LockAbortException();
            }
            Lock lock = lockTable.get(b);
            lock = new Lock(b, type);
            lockTable.put(b, lock);
        }
        
    }

    public synchronized void releaseLock(BlockId b)
    {
        Lock released = lockTable.remove(b);
        if(released == null)
        {
            return;
        }
        if(released.getLockType() == LockType.X_LOCK)
        {
            logger.info("Realizing " + b);
            notifyAll();
        }
    }

    public synchronized void resetAllLockState()
    {
        this.lockTable.clear();
        this.multipleSlock.clear();
        notifyAll();
    }

    // Additional methods for deadlock detection, lock upgrading, etc.
}
