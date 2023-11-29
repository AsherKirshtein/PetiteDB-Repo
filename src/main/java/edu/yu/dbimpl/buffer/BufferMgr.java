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
  LinkedBlockingQueue<Buffer> availableBuffers;
  LinkedBlockingQueue<Buffer> TotalBuffers;
  
  FileMgrBase fileMgr;
  LogMgrBase logMgr;
  public int nBuffers;
  
  HashMap<Integer, Buffer> TotalBuffersMap;

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

    availableBuffers = new LinkedBlockingQueue<Buffer>(nBuffers);
    TotalBuffers = new LinkedBlockingQueue<Buffer>(nBuffers);
    TotalBuffersMap = new HashMap<Integer, Buffer>(nBuffers);

    for (int i = 0; i < nBuffers; i++) 
    {
      Buffer buff = new Buffer(fileMgr, logMgr);
      buff.modifyingTx = i;
      availableBuffers.add(buff);
      TotalBuffers.add(buff);
      TotalBuffersMap.put(i, buff);
      //System.out.println(buff.modifyingTx() + " has been added to the pool");

    }
  }

  public void flushAll(int txnum) 
  {
    Iterator<Buffer> iterator = TotalBuffers.iterator();
    while(iterator.hasNext()) 
    {
      Buffer buffer = iterator.next();
      if(buffer.isModifiedBy(txnum)) 
      {
          buffer.flush();
      }
    }
      Iterator<Buffer> availableBuffersIterator = availableBuffers.iterator();
      while (availableBuffersIterator.hasNext()) 
      {
        Buffer buffer = availableBuffersIterator.next();
        if(buffer.isModifiedBy(txnum)) 
        {
          buffer.flush();
        }
    }
  }

  public int available() 
  {
    return availableBuffers.size();
  }

  public synchronized BufferBase pin(BlockIdBase blk) 
  {
    if (blk == null) 
    {
      throw new IllegalArgumentException("blk cannot be null");
    }
    Buffer buffer = null;
    if (!TotalBuffersMap.containsKey(blk.number())) 
    { 
      //System.out.println("We don't see that buffer sorry :( " + blk.number());
      return null;
    }
    else
    {
      Iterator<Buffer> iterator = TotalBuffers.iterator();
      while (iterator.hasNext()) 
      {
        Buffer b = iterator.next();
        //System.out.println(" Trying to pin " + b);
        if (blk.equals(b.block())) 
        {
          buffer = b;
          //System.out.println("Buffer " + b + " has been pinned");
          buffer.pin();
          return buffer;
        }
      }
    }

    Iterator<Buffer> iterator = availableBuffers.iterator();
    while (iterator.hasNext()) 
    {
      Buffer b = iterator.next();
      if(b.block().equals(blk)) 
      {
        buffer = b;
        //System.out.println(blk.number() + " Just pinned ");
        buffer.pin();
        TotalBuffersMap.put(blk.number(), buffer);
        availableBuffers.remove(b);
        TotalBuffers.add(buffer);
        return buffer;
      }
    }
    
    try 
    {
      buffer = availableBuffers.poll(maxWaitTime, TimeUnit.MILLISECONDS);
      if (buffer == null) 
      {
        throw new BufferAbortException("Timed out waiting for a buffer to become available.");
      }
    } 
    catch (InterruptedException e) 
    {
        throw new BufferAbortException("Interrupted while waiting for a buffer to become available.");
    }

    //buffer.assignToBlock(blk);
    buffer.pin();
    TotalBuffersMap.put(blk.number(), buffer);
    TotalBuffers.remove(buffer);
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
      //System.out.println("That buffer ain't pinned"); 
      return;
    }
    
    if (buf.isPinned()) 
    {
      TotalBuffersMap.remove(buf.block().number());
      availableBuffers.add(buf);
      buf.unpin();
    }
  }
}
