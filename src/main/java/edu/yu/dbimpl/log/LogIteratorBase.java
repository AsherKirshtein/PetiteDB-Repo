package edu.yu.dbimpl.log;

/** Specifies the public API for the LogIterator implementation by requiring
 * all LogIterator implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A class that provides the ability to move through the records of the log
 * file in REVERSE order (most recently created to earliest created).
 *
 * Design note: given that log iteration is done on startup by the recovery
 * manager as a single-threaded process, or on a per-tx basis for rollback, the
 * iterator need not be thread-safe.  If these assumptions are incorrect, then
 * the iterator must be thread-safe.
 * 
 * Design note: "package private" by design since non-dbms-clients should not
 * be directly creating an instance.
 * 
 * @author Avraham Leff
 */

import java.util.Iterator;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;

abstract class LogIteratorBase implements Iterator<byte[]> {

  /** Constructor: creates an iterator for the records in the log file,
   * positioned after the LAST log record.  Thus if there is a single log
   * record in the file: hasNext() will return true, and next() will return
   * that record.
   */
  public LogIteratorBase(FileMgrBase fm, BlockIdBase blk) {
    // fill me in in your implementation class!
  }

  /** Returns false iff the current log record is the first record in the log
   * file (log iteration proceeds from latest record to earliest record).
   *
   * @return true if there is an earlier record than the current iterator
   * record
   */
  public abstract boolean hasNext();

  /** Moves to the next log record in the block.  If there are no more log
   * records in the current block, then move to the previous block and return
   * the last log record from that block.
   *
   * @return the next earliest log record
   */
  public abstract byte[] next();
}
