package edu.yu.dbimpl.log;

import java.io.File;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;

public class LogIterator extends LogIteratorBase {

    FileMgrBase fm;
    BlockIdBase blk;
    LogMgr logMgr;
    int offset;
    Page page;

    public LogIterator(FileMgrBase fm, BlockIdBase blk)
    {
        super(fm, blk);
        this.fm = fm;
        this.blk = blk;
        this.logMgr = new LogMgr(fm, blk.fileName());
        this.page = new Page(fm.blockSize());
    }

    @Override
    public boolean hasNext()
    {
        if(this.offset == fm.blockSize())
        {
            if(this.blk.number() <= 0)
            {
                return false;
            }
        }
        return true;

    }

    @Override
    public byte[] next() 
    {
        synchronized(this)
        {
            fm.read(blk, this.page);
            //If there are no more log records in the current block, then move to the previous block and return the last log record from that block.
            if (this.offset == fm.blockSize() && this.blk.number() > 0)
            {
                this.blk = new BlockId(blk.fileName(), blk.number() - 1);
                this.offset = 0;
                fm.read(blk, this.page);
                return this.page.getBytes(offset);
            } 
            else 
            {
                // Move to the next log record within the same block
                String type = this.page.getType(offset);
                System.out.println("type: " + type);
                switch(type)
                {
                    case "int":
                        this.offset += Integer.BYTES;
                        break;
                    case "double":
                        this.offset += Double.BYTES;
                        break;
                    case "boolean":
                        this.offset += Integer.BYTES;
                        break;
                    case "byte[]":
                        this.offset += this.page.getInt(offset);
                        break;
                    case "String":
                        this.offset += this.page.getInt(offset);
                        break;
                    case "null":
                        return new byte[fm.blockSize()];
                }
                fm.read(blk, this.page);
                System.out.println("offset: " + offset);
                System.out.println("block size: " + fm.blockSize());
                return this.page.getBytes(offset);
            }
    }
        }
    
}
