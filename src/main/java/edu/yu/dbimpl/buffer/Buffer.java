package edu.yu.dbimpl.buffer;

import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.file.*;

public class Buffer extends BufferBase 
{  
  FileMgrBase fileMgr;
  LogMgrBase logMgr;
  int lsn;
  int pins;
  int modifyingTx;
  BlockIdBase blockId;
  Page page;
  boolean dirty;

  public Buffer(FileMgrBase fileMgr, LogMgrBase logMgr) 
  {
    super(fileMgr, logMgr);

    if (fileMgr == null || logMgr == null) 
    {
      throw new IllegalArgumentException("fileMgr and logMgr must not be null");
    }

    this.fileMgr = fileMgr;
    this.logMgr = logMgr;
    this.dirty = false;
    this.modifyingTx = 0;
    this.pins = 0;
    this.lsn = -1;
    this.page = new Page(fileMgr.blockSize());
  }


  @Override
  public int modifyingTx() 
  {
    return this.modifyingTx;
  }

  public PageBase contents() 
  {
    return this.page;
  }

  public BlockIdBase block() 
  {
    return this.blockId;
  }

  public void setModified(int txnum, int lsn) 
  {
    this.dirty = true;
    this.lsn = lsn;
    this.modifyingTx = txnum;
  }

  public boolean isPinned()
  {
    if(pins <= 0) 
    {
      return false;
    }
    return pins > 0;
  }

  public synchronized void flush() 
  {
    if(dirty) 
    {
      logMgr.flush(lsn);
      fileMgr.write(blockId, page);
      dirty = false;
      modifyingTx = -1;
    }
  }

  public synchronized void pin() 
  {
    pins++;
  }

  public boolean isModifiedBy(int txnum) 
  {
    if(!dirty) 
    {
      return false;
    }
    if(modifyingTx == txnum) 
    {
      return true;
    }
    return dirty && modifyingTx == txnum;
  }
  public synchronized void unpin() 
  {
    pins--;
  }

  public void assignToBlock(BlockIdBase b) 
  {
    if(dirty) 
    {
      if(!blockId.equals(b)) 
      {
        flush();
      }
    }
    this.blockId = b;
    this.fileMgr.read(blockId, page);
  }
}
