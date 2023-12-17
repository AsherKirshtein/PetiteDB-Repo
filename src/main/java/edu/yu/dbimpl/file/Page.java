package edu.yu.dbimpl.file;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.logging.Logger;

public class Page extends PageBase
{
    
    ByteBuffer buffer;
    public byte[] b;
    public int offset;
    Logger logger = Logger.getLogger(Page.class.getName());

    public Page(byte[] b)
    {
        super(b);
        this.b = b;
        this.buffer = ByteBuffer.wrap(b);
        this.offset = 0;   
        //logger.info("Page created");
    }
    public Page(int pageSize)
    {
        super(pageSize);
        this.b = new byte[pageSize];
        this.buffer = ByteBuffer.wrap(b);
        this.offset = 0;
        //logger.info("Page created");
    }

    @Override
    public int getInt(int offset)
    {
        return this.buffer.getInt(offset);
    }

    @Override
    public void setInt(int offset, int n) 
    {
        this.buffer.putInt(offset, n);
        //Logger.getLogger(Page.class.getName()).info("int "+ n + " set at offset " + offset);
    }

    @Override
    public double getDouble(int offset) {
        return this.buffer.getDouble(offset);
    }

    @Override
    public void setDouble(int offset, double d)
    {
        this.buffer.putDouble(offset, d);
        //logger.info("double "+ d +" set at offset " + offset);
    }

    @Override
    public boolean getBoolean(int offset)
    {
        return this.buffer.get(offset) == 1;
    }

    @Override
    public void setBoolean(int offset, boolean d)
    {
        this.buffer.put(offset, (byte) (d ? 1 : 0));
        //logger.info("boolean set at offset " + offset);
    }

    @Override
    public byte[] getBytes(int offset)
    {
        buffer.position(offset); // Move to the position to start reading the length
        int length = buffer.getInt(); // Read the length of the byte array
        byte[] bytes = new byte[length];
        buffer.get(bytes); // Read the byte array from the buffer
        //logger.info("getting bytes at offset " + offset + " with length " + length);
        return bytes;
    }

    public void setAllBytes(byte[] b)
    {
        buffer.position(0);
        buffer.put(b);
    }

    @Override
    public void setBytes(int offset, byte[] b) 
    {
        buffer.putInt(offset, b.length); // Store the length of the byte array
        buffer.position(offset + Integer.BYTES); // Move to the position to start writing the bytes
        //logger.info("byte[] set at position " + buffer.position());
        buffer.put(b); // Write the byte array into the buffer
        
  
    }

    @Override
    public String getString(int offset)
    {
        int length = this.buffer.getInt(offset);
        //logger.info("String length: " + length + " at offset " + offset);
        offset += Integer.BYTES; // Move position to read string
        //logger.info("Reading String from offset " + offset + " length " + length  + " str = " + new String(this.b, offset, length, StandardCharsets.UTF_8));
        return new String(this.b, offset, length, StandardCharsets.UTF_8);
    }

    @Override
    public void setString(int offset, String s) 
    {
        buffer.putInt(offset, maxLength(s.length())); // Store string length
        //logger.info("String length: " + s.length() + " at offset " + offset);
        buffer.position(offset + Integer.BYTES); // Move position to write string
        buffer.put(s.getBytes(StandardCharsets.UTF_8));
        //logger.info("String: "+ s +" set at offset " + offset);
    }

    @Override
    public String toString() {
        return "Page{" + "buffer=" + buffer + ", b=" + b + ", offset=" + offset;
    }
}
