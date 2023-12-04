package edu.yu.dbimpl.tx;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import edu.yu.dbimpl.buffer.Buffer;
import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.file.BlockIdBase;

public class BufferTable 
{
    
    private BufferMgr bm;
    private Map<BlockIdBase,Buffer> AllBuffers = new HashMap<>();
    private List<BlockIdBase> pinnedBuffers = new ArrayList<>();

   public BufferTable(BufferMgr bm) 
   {
      this.bm = bm;
   }
   
   public void pin(BlockIdBase blk)
   {
        Buffer b = (Buffer) bm.pin(blk);
        AllBuffers.put(blk, b);
        pinnedBuffers.add(blk);
    }

    public void unpin(BlockIdBase blk)
    {
        Buffer b = AllBuffers.get(blk);
        bm.unpin(b);
        pinnedBuffers.remove(blk);
        if (!pinnedBuffers.contains(blk))
        {
            AllBuffers.remove(blk);
        }
        
    }

    public void reset() 
    {
        for (BlockIdBase blk : pinnedBuffers) 
        {
           Buffer buff = AllBuffers.get(blk);
           if(buff != null)
           {
                bm.unpin(buff);
           }
        }
        AllBuffers.clear();
        pinnedBuffers.clear();
     }

   public Buffer getBuffer(BlockIdBase blk)
   {
      return AllBuffers.get(blk);
   }

   @Override
   public String toString() {
      
       return "Buffer Table \nAllBuffers: " + AllBuffers.size() + "\nPinnedBuffers " + pinnedBuffers.size();
   }
}
