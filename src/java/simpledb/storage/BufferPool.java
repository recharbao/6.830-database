package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.LockManagerA;
import simpledb.transaction.LockManger;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.sql.Time;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */

    // List<Page> _pgs;

    // Map<PageId, Page> _map;

    private ConcurrentMap<Integer, Page> _map;
    private int _numPage;
    private TransactionId _tid;

    public BufferPool(int numPages) {
        // System.out.println("numPage = " + numPages);
        _numPage = numPages;
        // some code goes here
        // _pgs = new ArrayList<>(numPages);
        _map = new ConcurrentHashMap<>();
    }

    private int hashCode(Integer tableId, Integer pn) {
        String s = tableId + " " + pn;
        return s.hashCode();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here

        if (perm.equals(Permissions.READ_ONLY)) {
            LockManagerA.getLockManagerA().acquireReadLock(hashCode(pid.getTableId(), pid.getPageNumber()), tid);
        }else if (perm.equals(Permissions.READ_WRITE)) {
            LockManagerA.getLockManagerA().acquireWriteLock(hashCode(pid.getTableId(), pid.getPageNumber()), tid);
        }

        _tid = tid;
        if (_map.get(hashCode(pid.getTableId(), pid.getPageNumber())) == null) {
            DbFile hf = Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page pg = hf.readPage(pid);
            if (_map.size() >= _numPage) {
                evictPage();
            }

            _map.put(hashCode(pid.getTableId(), pid.getPageNumber()), pg);
            return pg;
        }else {
            return _map.get(hashCode(pid.getTableId(), pid.getPageNumber()));
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void unsafeReleasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        //LockManger.getLockManger().releasePageLock(hashCode(pid.getTableId(), pid.getPageNumber()), tid);
        LockManagerA.getLockManagerA().releaseLock(hashCode(pid.getTableId(), pid.getPageNumber()), tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        // return LockManger.getLockManger().isHoldLock(hashCode(p.getTableId(), p.getPageNumber()), tid);
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        //System.out.println("transactionComplete !");

        // System.out.println(Thread.currentThread() + " complete before !");
        Set<Integer> alreadyRelease = new HashSet<>();

        if (commit) {
            _map.entrySet().stream()
                    .map(entry-> entry.getValue())
                    .filter(page -> (page.isDirty() != null && page.isDirty().equals(tid)))
                    .forEach(a-> {
                try {
                    flushPage(a.getId());
                    alreadyRelease.add(hashCode(a.getId().getTableId(), a.getId().getPageNumber()));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
        }else {
            for (ConcurrentMap.Entry<Integer, Page> entry : _map.entrySet()) {
                Integer key = entry.getKey();
                Page page = entry.getValue();
                if (page.isDirty() != null && page.isDirty().equals(tid)) {
                    DbFile hf = Database.getCatalog().getDatabaseFile(page.getId().getTableId());
                    Page pg = hf.readPage(page.getId());
                    _map.put(key, pg);
                }
            }
        }


        for (ConcurrentMap.Entry<Integer, Page> entry : _map.entrySet()) {
            Integer key = entry.getKey();
            if (alreadyRelease.contains(key)) {
                continue;
            }
            Page page = entry.getValue();
            unsafeReleasePage(tid, page.getId());
        }

        // System.out.println(Thread.currentThread() + " complete after !");
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        //LockManagerA.getLockManagerA().acquireWriteLock(hashCode(pid.getTableId(), pid.getPageNumber()), tid);
        DbFile hf = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> pages = hf.insertTuple(tid, t);
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            page.markDirty(true, tid);
//            hf.writePage(page);
            _map.put(hashCode(page.getId().getTableId(), page.getId().getPageNumber()), page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile hf = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        List<Page> pages = hf.deleteTuple(tid, t);
        for (int i = 0; i < pages.size(); i++) {
            Page page = pages.get(i);
            page.markDirty(true, tid);
//            hf.writePage(page);
            _map.put(hashCode(page.getId().getTableId(), page.getId().getPageNumber()), page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (Map.Entry<Integer, Page> entry : _map.entrySet()) {
            flushPage(entry.getValue().getId());
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        _map.remove(hashCode(pid.getTableId(), pid.getPageNumber()));
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        Page page = _map.get(hashCode(pid.getTableId(), pid.getPageNumber()));
        page.markDirty(false, _tid);
        DbFile hf = Database.getCatalog().getDatabaseFile(pid.getTableId());
        hf.writePage(page);
        Database.getBufferPool().unsafeReleasePage(_tid, pid);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        // System.out.println("mapsize : " + _map.size());
        boolean flag = true;
        for (Map.Entry<Integer, Page> entry : _map.entrySet()) {
            Integer key = entry.getKey();
            Page page = entry.getValue();
            if (page != null && page.isDirty() != null) {
                continue;
            }

            flag = false;
            //System.out.println("============");
            try {
                flushPage(page.getId());
            } catch (IOException e) {
                e.printStackTrace();
            }

            discardPage(page.getId());
            break;
        }

        if (flag) {
            //System.out.println(_map.size() + " " + _numPage);
            throw new DbException("bad evict !");
        }
    }
}
