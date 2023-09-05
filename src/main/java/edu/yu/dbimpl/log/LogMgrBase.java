package edu.yu.dbimpl.log;

/** Specifies the public API for the LogMgr implementation by requiring all
 * LogMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A LogMgr is (conceptually) a singleton.
 *
 * A LogMgr is responsible for writing log records but has no knowledge of the
 * structure of these records: as far as its concerned it's "just a sequence of
 * bytes".
 *
 * A LogMgr is free to persist additional meta-data to the block.  However, for
 * reasonably sized blocks and log records, it's unreasonable to throw an
 * IllegalArgumentException because the meta-data causes log records to exceed
 * the size of a block.  In other words: efficiency counts, or at least
 * "excessive inefficiency" will cost you.
 *
 * The LogMgr latest-sequence-number (LSN) MUST be initialized (to facilitate
 * my testing) to 0.  A successful call to append() returns the current LSN to
 * the client and then increments the value.
 *
 * @author Avraham Leff
 */

import java.util.Iterator;
import edu.yu.dbimpl.file.FileMgrBase;

/** The log manager, which is responsible for writing log records into a log
 * file. The tail of the log is kept in a bytebuffer, which is flushed to disk
 * when needed.
 */
public abstract class LogMgrBase {

  /** Creates the manager for the specified log file.  If the log file does
   * not yet exist, it is created with an empty first block.
   *
   * @param FileMgr the file manager
   * @param logfile the name of the log file
   */
  public LogMgrBase(FileMgrBase fm, String logfile) {
    // fill me in your implementation class!
  }

  /** Ensures that the log record corresponding to the specified LSN has been
   * written to disk.  All earlier log records in the same in-memory page as
   * the specified log record will also be written to disk.
   *
   * @param lsn the LSN of a log record
   */
  public abstract void flush(int lsn);

  /** First flushes the log to disk, then return an LogIterator to the contents
   * of the log on disk.
   *
   * @return a LogIterator (typed as an Iterator<byte[]>)
   */
  public abstract Iterator<byte[]> iterator();

  /** Appends a log record (as an arbitray byte array), and return the current
   * ("pre-incremented") LSN to the client.  If successful, the LSN is
   * incremented internally.
   *
   * Note: appending a record to the log does NOT guarantee that the record
   * will be immediately written to disk.  In general, the log manager
   * implementation chooses when to write log records to disk (see discussion
   * in lecture).  To guarantee that the log record is immediately written to
   * disk, the client must invoke flush().
   *
   * Suggested implementation (non-despositive, as long as the other API
   * semantics (including the Iterator) are provided) follows. Log records are
   * written from "right to left" order in a given Page (i.e., at decreasing
   * byte offsets the in the main-memory page).  Storing the records backwards
   * makes it easy to read them in reverse order.
   *
   * @param logrec a byte buffer containing the bytes.  The only constraint is
   * that the array must fits inside a single Page.
   * @return the LSN at the time before it was incremented as a side-effect of
   * this method.
   * @throws IllegalArgumentException if the log record is too large to fit
   * into a single page.  The implementation can include "reasonable" amounts
   * of meta-data when determining that a log record is too big.
   * @see #flush
   */
  public abstract int append(byte[] logrec);
}
