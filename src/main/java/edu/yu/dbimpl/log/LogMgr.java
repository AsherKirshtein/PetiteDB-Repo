package edu.yu.dbimpl.log;

import java.io.File;
import java.util.Iterator;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;

public class LogMgr extends LogMgrBase{

    FileMgrBase fm;
    int lsn;
    File logFile;
    Page page;
    int offset;
    //Logger logger = Logger.getLogger(LogMgr.class.getName());
    LogIterator logIterator;
    LogIterator prev;
    BlockId blockId;
    Boolean justCalledIterator = false;

    public LogMgr(FileMgrBase fm, String logfile)
    {
        super(fm, logfile);
        this.fm = fm;
        this.logFile = new File(logfile);   
        this.lsn = 0;
        this.page = new Page(fm.blockSize());
        this.offset = fm.blockSize();
        this.blockId = new BlockId(logfile, 0);
        this.logIterator = new LogIterator(fm, blockId);
        this.prev = new LogIterator(fm, blockId);
        //logger.info("LogMgr created");
    }

    @Override
    public void flush(int lsn)
    {
        synchronized(this)
        {
            for(int i = lsn; i > 0; i--)
            {
                this.fm.write(this.blockId, this.page);
                this.offset = fm.blockSize();
                this.logIterator.removeOffsetandLSN(i);      
            }
            this.justCalledIterator = false;
        }
    }

    @Override
    public Iterator<byte[]> iterator() {
        synchronized(this)
        {
            this.justCalledIterator = true;
            flush(this.lsn);
            return this.prev;
        }
    }

    @Override
    public int append(byte[] logrec)
    {
        if(logrec.length > fm.blockSize())
        {
            throw new IllegalArgumentException("Log record is too large");
        }
        synchronized(this)
        {
            if(this.offset < logrec.length + Integer.BYTES)
            {
                flush(lsn);
            }
            int prevlsn = this.lsn;
            this.lsn++;
            this.offset -= logrec.length + Integer.BYTES;
            this.page.setBytes(this.offset, logrec);
            this.logIterator.addOffsetandLSN(this.offset, this.lsn);
            this.prev.addOffsetandLSN(this.offset, this.lsn);
            //logger.info("Log record appended " + logrec + " at offset " + this.offset + " with lsn " + this.lsn);
            return prevlsn;
        }
    }

    public int getLSN() {
        return this.lsn;
    }
}
