package edu.yu.dbimpl.file;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
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
    private Logger logger = Logger.getLogger(FileMgr.class.getName() + ".log");
    
    public FileMgr(File dbDirectory, int blocksize) {
        super(dbDirectory, blocksize);
        this.isNew = !dbDirectory.exists();
        this.dbDirectory = dbDirectory;
        this.dbDirectory.mkdirs();
        this.blocksize = blocksize;
        logger.info("FileMgr created");
        logger.info("dbDirectory: " + dbDirectory);
        logger.info("blocksize: " + blocksize);
        logger.info("isNew: " + isNew);
        logger.info("dbDirectory exists: " + dbDirectory.exists());
        logger.info("dbDirectory is deleting temp files at super sonic speed"); 
        removeTempFiles();  
    }

    public void removeTempFiles() {
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
    }

    @Override
    public void read(BlockIdBase blk, PageBase p)
    {
        synchronized(this)
        {
            logger.info("read called with BlockIdBase: " + blk + " and PageBase: " + p);
            File file = new File(this.dbDirectory, blk.fileName());
            if(file.length() < (blk.number() + 1) * this.blocksize)
            {
                throw new IllegalArgumentException("Block does not exist");
            }
            if(p.getBytes(0).length > this.blocksize - 4)
            {
                throw new IllegalArgumentException("Page is too large to fit into a block");
            }
            try(RandomAccessFile raf = new RandomAccessFile(file, "r"))
            {
                raf.seek(blk.number() * this.blocksize);
                byte[] bytes = new byte[this.blocksize];
                raf.read(bytes);
                int offset = blk.number() * this.blocksize;
                Page p2 = (Page) p;
                String type = p2.getType(offset);
                logger.info("Getting type to read: " + type);
                switch (type)
                {
                    case "int":
                        p.setInt(offset, p2.getInt(offset));
                        break;
                    case "double":
                        p.setDouble(offset, p2.getDouble(offset));
                        break;
                    case "boolean":
                        p.setBoolean(offset, p2.getBoolean(offset));
                        break;
                    case "byte[]":
                        p.setBytes(offset, p2.getBytes(offset));
                        break;
                    case "String":
                        p.setString(offset, p2.getString(offset));
                        break;
                    default:
                        throw new IllegalArgumentException("Invalid type");    
                }
            }
            catch(FileNotFoundException e)
            {
                throw new IllegalArgumentException("File does not exist");
            }
            catch(IOException e)
            {
                throw new IllegalArgumentException("IOException");
            }
        }
    }

    @Override
    public void write(BlockIdBase blk, PageBase p) 
    {
        logger.info("write called with BlockIdBase: " + blk + " and PageBase: " + p);
        File file = new File(this.dbDirectory, blk.fileName());
        if(file.length() < (blk.number() + 1) * this.blocksize)
        {
            throw new IllegalArgumentException("Block does not exist");
        }
        try(FileOutputStream fos = new FileOutputStream(file, true))
        {
            int offset = blk.number() * this.blocksize;
            Page p2 = (Page) p;
            String type = p2.getType(offset);
            logger.info("Getting type to write: " + type);
            if(type == null)
            {
                throw new IllegalArgumentException("Probably have wrong offset");
            }
            DataOutputStream dos = new DataOutputStream(fos);
            logger.info("Writing to file: " + file);
            switch(type)
            {
                case "int":
                    fos.write(p2.getInt(offset));
                    break;
                case "double":
                    dos.writeDouble(p2.getDouble(offset));
                    break;
                case "boolean":
                    dos.writeBoolean(p2.getBoolean(offset));
                    break;
                case "byte[]":
                    fos.write(p2.getBytes(offset));
                    break;
                case "String":
                    fos.write(p2.getString(offset).getBytes());
                    break;
                default:
                    throw new IllegalArgumentException("Invalid type");
            }
        }
        catch(FileNotFoundException e)
        {
            throw new IllegalArgumentException("File does not exist");
        }
        catch(IOException e)
        {
            throw new IllegalArgumentException("IOException");
        }
        logger.info("Finished write");
    }

    @Override
    public BlockIdBase append(String filename) 
    {
        synchronized(this)
        {
            logger.info("append called with filename: " + filename);
            File file = new File(this.dbDirectory, filename);
            if(!file.exists())
            {
                try
                {
                    file.createNewFile();
                }
                catch(IOException e)
                {
                    throw new IllegalArgumentException("IOException");
                }
            }
            try(FileOutputStream fos = new FileOutputStream(file, true))
            {
                fos.write(new byte[this.blocksize]);
                logger.info("Writing to file: " + file);
            }
            catch(FileNotFoundException e)
            {
                throw new IllegalArgumentException("File does not exist");
            }
            catch(IOException e)
            {
                throw new IllegalArgumentException("IOException");
            }
        }
        return new BlockId(filename, this.length(filename) - 1);
    }

    @Override
    public int length(String filename) {
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
    
}
