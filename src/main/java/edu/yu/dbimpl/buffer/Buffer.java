package edu.yu.dbimpl.buffer;

import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;

import java.util.logging.Logger;

import edu.yu.dbimpl.file.*;

public class Buffer extends BufferBase 
{  
  FileMgrBase fileMgr;
  LogMgr logMgr;
  int lsn;
  int pins;
  public int modifyingTx;
  BlockIdBase blockId;
  public Page page;
  boolean dirty;
  Logger logger = Logger.getLogger(Buffer.class.getName());

  public Buffer(FileMgrBase fileMgr, LogMgrBase logMgr) 
  {
    super(fileMgr, logMgr);

    if (fileMgr == null || logMgr == null) 
    {
      throw new IllegalArgumentException("fileMgr and logMgr must not be null");
    }

    this.fileMgr = fileMgr;
    this.logMgr = (LogMgr) logMgr;
    this.dirty = false;
    this.modifyingTx = 0;
    this.pins = 0;
    this.lsn = -1;
    this.blockId = new BlockId("BufferBlock", this.modifyingTx);
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
    //logger.info("Buffer flushed");
  }

  public synchronized void pin() 
  {
    logger.info(this.modifyingTx + " Has been pinned");
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
    logger.info("Buffer unpinned " + this.modifyingTx);
  }

  public void assignToBlock(BlockIdBase b) 
  {
    if(dirty) 
    {
      if(!blockId.equals(b)) 
      {
        logger.info("We needed to flush on " + b.fileName());
        flush();
      }
    }
    this.blockId = b;
    this.fileMgr.read(blockId, page);
    logger.info("Buffer assigned to block: " + b.fileName());
  }

  public int getLSN()
  {
    return this.logMgr.getLSN();
  }
}
