package edu.yu.dbimpl.log;

import java.util.Iterator;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;

public class LogMgr extends LogMgrBase{

    Page page;
    int offset;
    int lsn;
    int lastSavedLSN;
    String logfile;
    FileMgrBase fm;
    BlockId blk;

    public LogMgr(FileMgrBase fm, String logfile)
    {
        super(fm, logfile);
        this.page = new Page(fm.blockSize());
        this.offset = fm.blockSize();
        this.logfile = logfile;
        this.fm = fm;
        this.lsn = 0;
    }

    @Override
    public void flush(int lsn)
    {
        synchronized(this)
        {
            this.page.setInt(0, this.offset);
            this.blk = (BlockId) fm.append(logfile);
            fm.write(blk, page);
            this.offset = fm.blockSize();
            // this.lsn = lsn;
        }
    }

    @Override
    public Iterator<byte[]> iterator() {
        synchronized(this)
        {
            flush(lsn);
            return new LogIterator(fm, new BlockId(logfile, lastSavedLSN));
        }
    }

    @Override
    public int append(byte[] logrec)
    {
        synchronized(this)
        {
            if(logrec.length > fm.blockSize())
            {
                throw new IllegalArgumentException("logrec cannot be larger than the blocksize");
            }
            int lsn = this.lsn;
            page.setBytes(this.offset - logrec.length, logrec);
            this.offset -= logrec.length + Integer.BYTES;
            this.lsn++;
            return lsn;
        }
    }
    
}
