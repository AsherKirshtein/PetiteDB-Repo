package edu.yu.dbimpl.file;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.logging.Logger;


public class FileMgr extends FileMgrBase{

    private final File dbDirectory;
    private final int blocksize;
    private boolean isNew;
    private int offset;

    //Logger logger = Logger.getLogger(FileMgr.class.getName());
    
    public FileMgr(File dbDirectory, int blocksize) 
    {
        super(dbDirectory, blocksize);
        this.isNew = !dbDirectory.exists();
        this.dbDirectory = dbDirectory;
        this.dbDirectory.mkdirs();
        this.blocksize = blocksize;
        this.offset = 0;
        removeTempFiles();  
    }

    public void removeTempFiles()
    {
        File[] files = dbDirectory.listFiles();
        if (files == null)
        {
            return; 
        }
        int numThreads = Runtime.getRuntime().availableProcessors();
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);
        List<Future<Void>> futures = new ArrayList<>();
        for (File file : files) {
            if (file.getName().startsWith("temp")) {
                Callable<Void> task = () -> {
                    file.delete();
                    return null;
                };
                futures.add(executor.submit(task));
            }
        }
        for (Future<Void> future : futures) 
        {
            try 
            {
                future.get(); 
            } 
            catch (Exception e) 
            {
                e.printStackTrace();
            }
        }
        executor.shutdown();
        //logger.info("temp files removed");
    }

    @Override
    public void read(BlockIdBase blk, PageBase p) {
    synchronized (this) {
        // Calculate the position from which the block should be read in the file
        long position = blk.number() * blockSize();
        // Construct the full path to the file based on the block's filename
        File file = new File(dbDirectory, blk.fileName());

        if (!file.exists()) {
            throw new IllegalArgumentException("Block does not exist: " + blk.fileName());
        }
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            randomAccessFile.seek(position);
            byte[] data = new byte[blockSize()];
            int bytesRead = randomAccessFile.read(data);
            if (bytesRead == -1) {
                throw new IOException("End of file reached while trying to read block: " + blk.fileName());
            }
            ((Page) p).setAllBytes(data);

            } 
            catch (IOException e) 
            {
                e.printStackTrace();
            }
        }
    }

    @Override
    public int length(String filename)
    {
        File file = new File(this.dbDirectory, filename);
        if(!file.exists())
        {
            return 0;
        }
        return (int) (file.length() / this.blocksize);
    }

    @Override
    public boolean isNew() {
        return this.isNew;
    }

    @Override
    public int blockSize() {
        return this.blocksize;
    }

    @Override
    public void write(BlockIdBase blk, PageBase p)
    {
        synchronized(this)
        {
            // Calculate the position where the block should be written in the file
            long position = blk.number() * blockSize();
            // Construct the full path to the file based on the block's filename
            File file = new File(dbDirectory, blk.fileName());
            //logger.info(p.toString());
            //logger.info("Writing block: " + blk.fileName() + " to file: " + file.getName() + " at position: " + position);
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rws")) 
            {
                randomAccessFile.seek(position);
                randomAccessFile.write(((Page)p).b);
                randomAccessFile.getFD().sync();
                randomAccessFile.close();
            } 
            catch (IOException e) 
            {
                throw new RuntimeException("Error writing block to file: " + e.getMessage(), e);
            };
        }
    }

    @Override
    public BlockIdBase append(String filename) 
    {
            //logger.info("Appending block to file: " + filename);
            if (filename == null || filename.isEmpty()) 
            {
                throw new IllegalArgumentException("Invalid filename");
            }
            File file = new File(dbDirectory, filename);
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "rws")) 
            {
                long position = randomAccessFile.length();
                byte[] newBlock = new byte[blockSize()];
                randomAccessFile.seek(position);
                randomAccessFile.write(newBlock);
                return new BlockId(filename, length(filename));
            } 
            catch (IOException e) 
            {
                throw new RuntimeException("Error appending block to file: " + e.getMessage(), e);
            }
    }
}
