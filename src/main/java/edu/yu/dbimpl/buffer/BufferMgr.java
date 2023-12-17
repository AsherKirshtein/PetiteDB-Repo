package edu.yu.dbimpl.buffer;

import edu.yu.dbimpl.log.LogMgrBase;

import java.util.ArrayList;
import edu.yu.dbimpl.file.*;


public class BufferMgr extends BufferMgrBase 
{
  final int maxWaitTime;
  ArrayList<Buffer> allBuffers;
  int availableBuffersCount = 0;

  public BufferMgr(FileMgrBase fileMgr, LogMgrBase logMgr, int nBuffers, int maxWaitTime) 
  {
    super(fileMgr, logMgr, nBuffers, maxWaitTime);
    if (nBuffers <= 0 || maxWaitTime <= 0 || fileMgr == null || logMgr == null ) 
    {
      throw new IllegalArgumentException("Invalid arguments to BufferMgr");
    }
    this.maxWaitTime = maxWaitTime;
    this.availableBuffersCount = nBuffers;
    this.allBuffers = new ArrayList<>();
      for (int i=0; i < this.availableBuffersCount; i++)
      {
         allBuffers.add(new Buffer(fileMgr, logMgr));
      }
  }

  public void flushAll(int txnum) 
  {
    for (Buffer b : this.allBuffers)
    {
         if(b.modifyingTx() == txnum)
         {
            b.flush();
         }
    }
  }

  public int available() 
  {
      return this.availableBuffersCount;
  }

  public synchronized BufferBase pin(BlockIdBase blk) 
  {
    long start = System.currentTimeMillis();
    Buffer buffer = tryPin(blk);
    while (buffer == null && System.currentTimeMillis() - start > this.maxWaitTime) 
    {
            try 
            {
              wait(this.maxWaitTime);
            } 
            catch (InterruptedException e)
            {
              throw new BufferAbortException();
            }
            buffer = tryPin(blk);
         }
         if (buffer == null)
         {
            throw new BufferAbortException();
         }
         return buffer;
  }
    
  private Buffer tryPin(BlockIdBase blk)
  {
    Buffer b = getAvailableBuffer();
    if (b == null) 
    {
       b = getAvailableBuffer();
    }
    if (!b.isPinned())
    {
       this.availableBuffersCount--;
    }
    b.pin();
    return b;
 }

 private Buffer getAvailableBuffer()
 {
  for (Buffer b : this.allBuffers)
  {
     if (!b.isPinned())
     {
        return b;
     }
  }
  return null;
}

  public void unpin(BufferBase buffer) 
  {
    synchronized(this)
    {  
      if(buffer == null)
      {
          return;
      }
      ((Buffer) buffer).unpin();
      if (!buffer.isPinned()) 
      {
        this.availableBuffersCount++;
        notifyAll();
      }
    }
  }
}
