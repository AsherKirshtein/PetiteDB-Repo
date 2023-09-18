package edu.yu.dbimpl;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Test;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.file.Page;

public class FirstStageTests
{
    
    @After
    public void deleteStuff()
    {
        File file = new File("src/test/java/edu/yu/dbimpl/testingDirectory");
        File[] files = file.listFiles();
        for(File f : files)
        {
            f.delete();
        }
        file.delete();
    }
    
    @Test
    public void shouldAnswerWithTrue()
    {
        assertTrue( true );
    }

    @Test 
    public void BlockIdBaseSimpleTest()
    {
        BlockIdBase base = new BlockId("Asher's File", 69);
        assertTrue(base.fileName().equals("Asher's File"));
        assertTrue(base.number() == 69);
    }

    @Test
    public void catchBadInputBlockIDTest()
    {
        int errorsCaught = 0;
        try
        {
            BlockIdBase base = new BlockId(null, 69);
        }
        catch(IllegalArgumentException e)
        {
            errorsCaught++;
        }
        try
        {
            BlockIdBase base = new BlockId("Asher's File", -69);
        }
        catch(IllegalArgumentException e)
        {
            errorsCaught++;
        }
        
        assertTrue(errorsCaught == 2);

    }
    @Test
    public void PageBaseImplGetAndSetIntTest()
    {
        PageBase base = new Page(4096);
        base.setInt(0, 69);
        assertTrue(base.getInt(0) == 69);
        base.setInt(100, 45);
        assertTrue(base.getInt(100) == 45);
    }
    @Test
    public void PageBaseImplGetAndSetDoubleTest()
    {
        PageBase base = new Page(4096);
        base.setDouble(4, 69.69);
        //System.out.println("We got: " + base.getDouble(4));
        assertTrue(base.getDouble(4) == 69.69);
        base.setDouble(105, 45.45);
        assertTrue(base.getDouble(105) == 45.45);
    }

    @Test
    public void PageBaseImplGetAndSetBooleanTest()
    {
        PageBase base = new Page(4096);
        base.setBoolean(4, true);
        assertTrue(base.getBoolean(4) == true);
        base.setBoolean(105, false);
        assertTrue(base.getBoolean(105) == false);
    }

    @Test
    public void testSetAndGetBytes() {

        PageBase page = new Page(100);
        // Data to be written to the block
        byte[] dataToWrite = "Hello, World!".getBytes();

        // Offset to write the data
        int offset = 10;

        // Write the data to the block
        page.setBytes(offset, dataToWrite);

        // Read the data back from the block
        byte[] retrievedData = page.getBytes(offset);

        // Verify that the retrieved data matches the original data
        for(int i = 0; i < dataToWrite.length; i++) {
            assertEquals(dataToWrite[i], retrievedData[i]);
        }
        assertTrue(dataToWrite.length == retrievedData.length);
    }

    @Test
    public void testSetAndGetBytes2() {

        PageBase page = new Page(100);
        // Data to be written to the block
        byte[] dataToWrite = "Hello, World! 69 is my favorite number".getBytes();

        // Offset to write the data
        int offset = 10;

        // Write the data to the block
        page.setBytes(offset, dataToWrite);

        // Read the data back from the block
        byte[] retrievedData = page.getBytes(offset);

        // Verify that the retrieved data matches the original data
        for(int i = 0; i < dataToWrite.length; i++) {
            assertEquals(dataToWrite[i], retrievedData[i]);
        }
        assertTrue(dataToWrite.length == retrievedData.length);
    }

    @Test
    public void testSetAndGetString() {

        PageBase page = new Page(100);
        // Data to be written to the block
        String dataToWrite = "Hello, World!";

        // Offset to write the data
        int offset = 10;

        // Write the data to the block
        page.setString(offset, dataToWrite);

        // Read the data back from the block
        String result = page.getString(offset);
        // Verify that the retrieved data matches the original data
        assertTrue(dataToWrite.equals(result));
    }

    @Test
    public void testSetAndGetString2() {

         PageBase page = new Page(100);
        // Data to be written to the block
        String dataToWrite = "Hello, World! I love 69";

        // Offset to write the data
        int offset = 10;

        // Write the data to the block
        page.setString(offset, dataToWrite);

        // Read the data back from the block
        String result = page.getString(offset);
        // Verify that the retrieved data matches the original data
        assertTrue(dataToWrite.equals(result));
    }

    @Test
    public void putInAfewThingsTest()
    {
        PageBase page = new Page(100);
        page.setInt(0, 69);
        page.setDouble(4, 69.69);
        page.setBoolean(12, true);
        page.setString( 16, "Hello, World!");
        assertTrue(page.getInt(0) == 69);
        assertTrue(page.getDouble(4) == 69.69);
        assertTrue(page.getBoolean(12) == true);
        assertTrue(page.getString(16).equals("Hello, World!"));
    }

    @Test
    public void putInAfewMoreThings()
    {
        PageBase page = new Page(1000);
        for(int i = 0; i < 1000; i+=4)
        {
            page.setInt(i, 100 + i);    
        }
        for(int i = 0; i < 1000; i+=4)
        {
            assertTrue(page.getInt(i) == 100 + i);
        }
    }

    @Test
    public void fileManagerSimple()
    {
        File file = new File("src/test/java/edu/yu/dbimpl/testingDirectory", "test1");
        FileMgrBase fileMgr = new FileMgr(file, 4096);
        assertTrue(fileMgr.length("test1") == 0);
        BlockIdBase block = fileMgr.append("test1");
        assertTrue(fileMgr.length("test1") == 1);
        PageBase page = new Page(10000);
        page.setInt(0, 69);
        fileMgr.write(block, page);
        fileMgr.read(block, page);
        assertTrue(page.getInt(0) == 69);
        file.delete();
    }

    @Test
    public void deleteTempFilesTest()
    {
        // Specify the directory where you want to create the files
        String directoryPath = "src/test/java/edu/yu/dbimpl/testingDirectory/test1/TempFiles";

        // Create the directory if it doesn't exist
        File directory = new File(directoryPath);
        if (!directory.exists()) {
            if (directory.mkdirs()) {
                //System.out.println("Directory created: " + directoryPath);
            } else {
                System.err.println("Failed to create directory: " + directoryPath);
                return;
            }
        }
        int tempFiles = 1000;
        int regularFiles = 200;
        try {
            // Create regular files
            for (int i = 1; i <= regularFiles; i++) {
                String fileName = "file" + i + ".txt";
                File file = new File(directory, fileName);
                if (file.createNewFile()) {
                    //System.out.println("Created regular file: " + file.getAbsolutePath());
                } else {
                    System.err.println("Failed to create regular file: " + fileName);
                }
            }

            // Create temporary files
            for (int i = 1; i <= tempFiles; i++) 
            {
                String prefix = "tempfile";
                String suffix = ".txt";
                Path tempFilePath = Files.createTempFile(directory.toPath(), prefix, suffix);
                Files.write(tempFilePath, "This is temporary content.".getBytes(), StandardOpenOption.WRITE);
            }
        } 
        catch (IOException e) 
        {
            e.printStackTrace();
        }
        assertEquals(regularFiles + tempFiles, directory.listFiles().length);
        FileMgrBase fileMgr = new FileMgr(directory, 30000);
        assertEquals(regularFiles, directory.listFiles().length);


        List<File> files = Arrays.asList(directory.listFiles());
        for(File file : files)
        {
            file.delete();
        }
    }

    @Test
    public void testFileModulePerformance() {

        int blocksize = 4096;
        File file = new File("src/test/java/edu/yu/dbimpl/testingDirectory");
        FileMgrBase fileMgr = new FileMgr(file, blocksize);
        
        long startTime = System.currentTimeMillis();
        String testString = "Hello, World!";
        for(int i = 0; i < 400; i++)
        {
            File fileAppend = new File("src/test/java/edu/yu/dbimpl/testingDirectory", "testFileModulePerformance" + i);
            BlockIdBase block = fileMgr.append("testFileModulePerformance" + i);
            PageBase page = new Page(50000);
            page.setString(block.number() * blocksize, testString);
            page.getString(i * 20);
        }
        // Use a thorough set of log statements
        
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;

        // Print execution time for performance evaluation
        System.out.println("Execution time: " + executionTime + " milliseconds");

        // Assert that the test meets performance expectations
        assertTrue(1500 > executionTime); // Expectation: Less than or equal to 8000 milliseconds (8 seconds)
    }

    @Test
    public void addingALotOfStringsToAPage()
    {
        PageBase page = new Page(4096);
        String testString = "Hello, World!";
        for(int i = 0; i < 100; i++)
        {
            page.setString(i * 20, testString + i);
        }
        for(int i = 0; i < 100; i++)
        {
            assertEquals(testString + i , page.getString(i * 20));
        }
    }
    @Test
    public void anotherTest()
    {
        File file = new File("src/test/java/edu/yu/dbimpl/testingDirectory");
        FileMgrBase fileMgr = new FileMgr(file, 4096);
        String testString = "Hello, World!";
        for(int i = 0; i < 1000; i++)
        {
            BlockIdBase block = fileMgr.append("testFileModulePerformance" + i);
            PageBase page = new Page(50000);
            page.setString(block.number() * 4096, testString);
            assertEquals(testString, page.getString(block.number() * 4096));
        }
    }

    @Test
    public void leffTest()
    {
        
    }
}
