package edu.yu.dbimpl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Iterator;

import org.junit.Test;

import edu.yu.dbimpl.file.FileMgr;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;

public class SecondStageTests
{
    @Test
    public void testLogMgr()
    {
       
    } 
    @Test
    public void testAppendLogRecordAndGetLSN() {
        
        FileMgr fileManager = new FileMgr(new File("src/test/java/edu/yu/dbimpl/testingDirectory/test2"), 4096);
        
        LogMgrBase logManager = new LogMgr(fileManager, "logfile");

        byte[] logRecord1 = "Log Record 1".getBytes();
        byte[] logRecord2 = "Log Record 2".getBytes();

        // Append two log records and get their LSNs
        int lsn1 = logManager.append(logRecord1);
        int lsn2 = logManager.append(logRecord2);

        // Check that LSNs are incremented correctly
        assertEquals(0, lsn1);
        assertEquals(1, lsn2);
    }

     @Test
    public void testAppendLogRecordAndGetLSN2() {
        
        FileMgr fileManager = new FileMgr(new File("src/test/java/edu/yu/dbimpl/testingDirectory/test2"), 4096);
        
        LogMgrBase logManager = new LogMgr(fileManager, "logfile");

        byte[] logRecord1 = "Log Record 1".getBytes();
        byte[] logRecord2 = "Log Record 2".getBytes();
        byte[] logRecord3 = "Log Record 3".getBytes();
        byte[] logRecord4 = "Log Record 4".getBytes();
        byte[] logRecord5 = "Log Record 5".getBytes();
        byte[] logRecord6 = "Log Record 6".getBytes();
        byte[] logRecord7 = "Log Record 7".getBytes();
        byte[] logRecord8 = "Log Record 8".getBytes();
        byte[] logRecord9 = "Log Record 9".getBytes();
        byte[] logRecord10 = "Log Record 10".getBytes();

        // Append two log records and get their LSNs
        int lsn1 = logManager.append(logRecord1);
        int lsn2 = logManager.append(logRecord2);

        // Check that LSNs are incremented correctly
        assertEquals(0, lsn1);
        assertEquals(1, lsn2);
        assertEquals(2, logManager.append(logRecord3));
        assertEquals(3, logManager.append(logRecord4));
        assertEquals(4, logManager.append(logRecord5));
        assertEquals(5, logManager.append(logRecord6));
        assertEquals(6, logManager.append(logRecord7));
        assertEquals(7, logManager.append(logRecord8));
        assertEquals(8, logManager.append(logRecord9));
        assertEquals(9, logManager.append(logRecord10));
    }

    @Test
    public void testFlushAndIterator() {
        
         
    }
 
}
