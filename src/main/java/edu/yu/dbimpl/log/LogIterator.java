package edu.yu.dbimpl.log;

import java.util.HashMap;
import java.util.Map;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.Page;

public class LogIterator extends LogIteratorBase {

    private FileMgrBase fm;
    private BlockIdBase blk;
    private Map<Integer, Integer> lsnToOffset;
    private int lsn;
    private Page page;
    private int offset;
    //private Logger logger = Logger.getLogger(LogIterator.class.getName());

    public LogIterator(FileMgrBase fm, BlockIdBase blk)
    {
        super(fm, blk);
        this.fm = fm;
        this.blk = blk;
        this.page = new Page(fm.blockSize());
        this.lsnToOffset = new HashMap<>(); 
        this.lsn = 0;
        this.offset = fm.blockSize(); 
        //logger.info("LogIterator created");
    }

    @Override
    public boolean hasNext()
    {
        return !this.lsnToOffset.isEmpty();
    }

    @Override
    public byte[] next() 
    {
        if(!hasNext() || this.lsn == 0)
        {
            return null;
        }
        if(this.lsnToOffset.get(this.lsn) == null)
        {
            return null;
        }
        int offset = this.lsnToOffset.get(this.lsn);
        this.lsnToOffset.remove(this.lsn);
        this.lsn--;
        this.fm.read(this.blk, this.page);
        return this.page.getBytes(offset);
    }

    public void addOffsetandLSN(int offset, int lsn)
    {
        this.lsnToOffset.put(lsn, offset);
        this.lsn++;
    }

    public void removeOffsetandLSN(int lsn)
    {
        this.lsnToOffset.remove(lsn);
    }

    public int getOffset(int lsn)
    {
        return this.lsnToOffset.get(lsn);
    }
    @Override
    public String toString() {
        return "LogIterator [blk=" + blk + ", fm=" + fm + ", lsn=" + lsn + ", lsnToOffset=" + lsnToOffset + ", page="
                + page + "]";
    }

    public boolean containsLSN(int i) {
        return this.lsnToOffset.containsKey(i);
    }

    public int getLSN() {
        return this.lsn;
    }

    public void setLSN(int lsn2)
    {
        this.lsn = lsn2;
    }

}
