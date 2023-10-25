package edu.yu.dbimpl.buffer;

import edu.yu.dbimpl.log.LogMgrBase;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.Iterator;
import edu.yu.dbimpl.file.*;


public class BufferMgr extends BufferMgrBase 
{
  final int maxWaitTime;
  LinkedBlockingQueue<Buffer> bPool;
  LinkedBlockingQueue<Buffer> pBuffers;
  
  FileMgrBase fileMgr;
  LogMgrBase logMgr;
  int nBuffers;
  
  HashMap<Integer, Buffer> pBuffersMap;

  public BufferMgr(FileMgrBase fileMgr, LogMgrBase logMgr, int nBuffers, int maxWaitTime) 
  {
    super(fileMgr, logMgr, nBuffers, maxWaitTime);
    if (nBuffers <= 0 || maxWaitTime <= 0 || fileMgr == null || logMgr == null ) 
    {
      throw new IllegalArgumentException("Invalid arguments to BufferMgr");
    }
    this.fileMgr = fileMgr;
    this.logMgr = logMgr;
    this.nBuffers = nBuffers;
    this.maxWaitTime = maxWaitTime;

    bPool = new LinkedBlockingQueue<Buffer>(nBuffers);
    pBuffers = new LinkedBlockingQueue<Buffer>(nBuffers);
    pBuffersMap = new HashMap<Integer, Buffer>(nBuffers);

    for (int i = 0; i < nBuffers; i++) 
    {
      bPool.add(new Buffer(fileMgr, logMgr));
    }
  }

  public void flushAll(int txnum) 
  {
    Iterator<Buffer> iterator = pBuffers.iterator();
    while(iterator.hasNext()) 
    {
      Buffer buffer = iterator.next();
      if(buffer.isModifiedBy(txnum)) 
      {
          buffer.flush();
      }
    }
      Iterator<Buffer> bPoolIterator = bPool.iterator();
      while (bPoolIterator.hasNext()) 
      {
        Buffer buffer = bPoolIterator.next();
        if(buffer.isModifiedBy(txnum)) 
        {
          buffer.flush();
        }
    }
  }

  public int available() 
  {
    return bPool.size();
  }

  public synchronized BufferBase pin(BlockIdBase blk) 
  {
    if (blk == null) 
    {
      throw new IllegalArgumentException("blk cannot be null");
    }
    Buffer buffer = null;
    if (!pBuffersMap.containsKey(blk.number())) 
    {
       return null;
    }
    else
    {
      Iterator<Buffer> iterator = pBuffers.iterator();
      while (iterator.hasNext()) 
      {
        Buffer b = iterator.next();
        if (blk.equals(b.block())) 
        {
          buffer = b;
          buffer.pin();
          return buffer;
        }
      }
    }

    Iterator<Buffer> iterator = bPool.iterator();
    while (iterator.hasNext()) 
    {
      Buffer b = iterator.next();
      if(!b.block().equals(blk)) 
      {
        continue;
      }
      else
      {
        buffer = b;
        buffer.pin();
        pBuffersMap.put(blk.number(), buffer);
        bPool.remove(b);
        pBuffers.add(buffer);
        return buffer;
      }
    }
    
    try 
    {
      buffer = bPool.poll(maxWaitTime, TimeUnit.MILLISECONDS);
      if (buffer == null) 
      {
        throw new BufferAbortException("Timed out waiting for a buffer to become available.");
      }
    } 
    catch (InterruptedException e) 
    {
        throw new BufferAbortException("Interrupted while waiting for a buffer to become available.");
    }

    buffer.assignToBlock(blk);
    buffer.pin();
    pBuffersMap.put(blk.number(), buffer);
    pBuffers.add(buffer);
    return buffer;
  }

  public void unpin(BufferBase buffer) 
  {
    if(buffer == null) 
    {
      throw new IllegalArgumentException("buffer cannot be null");
    }
    Buffer buf = (Buffer) buffer;
    if(!buf.isPinned()) 
    {
       return;
    }
    buf.unpin();
    if (buf.isPinned()) 
    {
      pBuffers.remove(buf);
      pBuffersMap.remove(buf.block().number());
      bPool.add(buf);
    }
  }
}
