package edu.yu.dbimpl.file;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

public class Page extends PageBase{

    private final byte[] bytes;
    private final int blocksize;
    private ByteBuffer buffer;
    private Map<Integer, String> offsets;
    private Logger logger = Logger.getLogger(Page.class.getName() + ".log");
    
    public Page(byte[] b) {
        super(b);
        this.bytes = b;
        this.blocksize = b.length;
        this.buffer = ByteBuffer.wrap(b);
        this.offsets = new HashMap<>();
        logger.info("Page created with byte[]");
    }

    public Page(int blocksize) {
        super(blocksize);
        this.blocksize = blocksize;
        this.bytes = new byte[blocksize];
        this.buffer = ByteBuffer.wrap(bytes);
        this.offsets = new HashMap<>();
        logger.info("Page created with blocksize " + blocksize);
      }

      public String getType(int offset) //needed to add this method to get information into the FileMgr without changing 
      {
        if(this.offsets.get(offset) == null)
        {
            return "null";
        }
        return this.offsets.get(offset);
      }

    @Override
    public int getInt(int offset)
    {
        if(offset < 0 || offset > this.blocksize)
        {
            throw new IllegalArgumentException("offset must be a non-negative integer and cannot be greater than the blocksize");
        }
        synchronized(this)
        {
            logger.info("getInt called with offset: " + offset);
            return this.buffer.getInt(offset);
        }
    }

    @Override
    public void setInt(int offset, int n)
    {
        if(offset < 0 || offset > this.blocksize)
        {
            throw new IllegalArgumentException("offset must be a non-negative integer and cannot be greater than the blocksize");
        }
        synchronized(this)
        {
            logger.info("setInt called with offset: " + offset + " and int: " + n);
            this.offsets.put(offset, "int");
            this.buffer.putInt(offset, n);
        }
    }

    @Override
    public double getDouble(int offset) {
        if(offset < 0 || offset > this.blocksize)
        {
            throw new IllegalArgumentException("offset must be a non-negative integer and cannot be greater than the blocksize");
        }
        synchronized(this)
        {
            logger.info("getDouble called with offset: " + offset);
            return this.buffer.getDouble(offset);
        }
    }

    @Override
    public void setDouble(int offset, double d)
    {
        if(offset < 0 || offset > this.blocksize)
        {
            throw new IllegalArgumentException("offset must be a non-negative integer and cannot be greater than the blocksize");
        }
        synchronized(this)
        {
            logger.info("setDouble called with offset: " + offset + " and double: " + d);
            this.offsets.put(offset, "double");
            this.buffer.putDouble(offset, d);
        }
    }

    @Override
    public boolean getBoolean(int offset)
    {
        synchronized(this)
        {
            logger.info("getBoolean called with offset: " + offset);
            if(this.buffer.getInt(offset) == 1)
            {
                return true;
            }
            else
            {
                return false;
            }
        }
    }

    @Override
    public void setBoolean(int offset, boolean d) {
        synchronized(this)
        {
            logger.info("setBoolean called with offset: " + offset + " and boolean: " + d);
            if(d)
            {
                this.buffer.putInt(offset, 1);
            }
            else
            {
                this.offsets.put(offset, "boolean");
                this.buffer.putInt(offset, 0);
            }
        }
    }

    @Override
    public byte[] getBytes(int offset) {
        synchronized(this)
        {
            logger.info("getBytes called with offset: " + offset);
            int length = this.buffer.getInt(offset);
            byte[] result = new byte[length];
            for(int i = 0 ; i < length; i++)
            {
                result[i] = this.bytes[offset + 4 + i];
            }
            return result;
        }
    }

    @Override
    public void setBytes(int offset, byte[] b)
    {
        synchronized(this)
        {
            logger.info("setBytes called with offset: " + offset + " and byte[]");
            if(offset + b.length > this.blocksize)
            {
                throw new IllegalArgumentException("offset + b.length must be less than the blocksize current offset: " + offset + " b.length: " + b.length + " blocksize: " + this.blocksize);
            }
            this.buffer.putInt(offset, b.length);
            for(int i = 0 ; i < b.length; i++)
            {
                try
                {
                    this.bytes[offset + 4 + i] = b[i];
                }
                catch(ArrayIndexOutOfBoundsException e)
                {
                   break;
                }
            }
            this.offsets.put(offset, "byte[]");
        }
    }

    @Override
    public String getString(int offset) {
        synchronized(this)
        {
            int length = this.buffer.getInt(offset);
            byte[] result = new byte[length];
            for(int i = 0 ; i < length; i++)
            {
                result[i] = this.bytes[offset + 4 + i];
            }
            int validLength = 0;
            for (; validLength < result.length; validLength++) 
            {
                if (result[validLength] == 0) 
                {
                    break; 
                }
            }
            String retrievedString = null;
            try 
            {
                retrievedString = new String(result, 0, validLength, "UTF-8");
            } 
            catch (UnsupportedEncodingException e) 
            {
                e.printStackTrace();
            }
            return retrievedString;
        }

}



    @Override
    public void setString(int offset, String s) {
        
        synchronized(this)
        {
            this.buffer.putInt(offset, PageBase.maxLength(s.length()));
            byte[] b = s.getBytes(); 
            for(int i = 0 ; i < b.length; i++)
            {
                this.bytes[offset + 4 + i] = b[i];
            }
            logger.info("setString called with offset: " + offset + " and String: " + s);
            this.offsets.put(offset, "String");
        }
    }
    
}
