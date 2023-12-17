package edu.yu.dbimpl.file;

public class BlockId extends BlockIdBase {

    private final String filename;
    private final int blknum;
    
    public BlockId(String filename, int blknum)
    {
        super(filename, blknum);
        if(filename == null || filename.trim().length() <= 0 || blknum < 0)
        {
            throw new IllegalArgumentException("filename must have a trimmed length that's greater than 0 and have a blknum that is a non-negative integer");
        }
        this.filename = filename;
        this.blknum = blknum;
    }

    @Override
    public String fileName() {
        return this.filename;
    }

    @Override
    public int number() {
        return this.blknum;
    }

    @Override
    public String toString() {
        // TODO Auto-generated method stub
        return this.filename + " " + this.blknum;
    }
    
}
