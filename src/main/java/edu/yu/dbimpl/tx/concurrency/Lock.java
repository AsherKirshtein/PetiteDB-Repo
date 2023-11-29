package edu.yu.dbimpl.tx.concurrency;

import edu.yu.dbimpl.file.BlockIdBase;

public class Lock 
    {
        BlockIdBase blk;
        LockType lockType;

        Lock(BlockIdBase blk, LockType lockType)
        {
            this.blk = blk;
            this.lockType = lockType;
        }

        public BlockIdBase getBlk()
        {
            return blk;
        }

        public LockType getLockType()
        {
            return lockType;
        }

        @Override
        public String toString() {
        // TODO Auto-generated method stub
        return (lockType + " on block " + blk);
        }
    }

    
