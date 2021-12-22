package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    private File _f;

    private TupleDesc _td;

    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        _f = f;
        _td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return _f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return _f.getAbsoluteFile().hashCode();
        // throw new UnsupportedOperationException("implement this");
    }


    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return _td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId hpi = new HeapPageId(pid.getTableId(), pid.getPageNumber());
        byte[] data = new byte[BufferPool.getPageSize()];
        try {
            // InputStream fr = new FileInputStream(_f);
            RandomAccessFile raf = new RandomAccessFile(_f, "r");
            // raf.read(data, pid.getPageNumber() * BufferPool.getPageSize(), BufferPool.getPageSize());
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            raf.read(data);
            raf.close();
            HeapPage hp = new HeapPage(hpi, data);
            return hp;
        }catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(_f, "rw");
        raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        raf.write(page.getPageData());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here

        return (int) (_f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        int tableId = getId();
        List<Page> pages = new ArrayList<>();
        HeapPage page;
        for (int i = 0; i < numPages(); i++) {
            PageId pageId = new HeapPageId(tableId, i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            //Database.getBufferPool().unsafeReleasePage(tid, pageId);
            if (page.getNumEmptySlots() != 0) {
                page.insertTuple(t);
                pages.add(page);
                return pages;
            }
        }

        HeapPageId heapPageId = new HeapPageId(tableId, numPages());
        HeapPage heapPage = new HeapPage(heapPageId, HeapPage.createEmptyPageData());
        heapPage.insertTuple(t);
        RandomAccessFile raf = new RandomAccessFile(_f, "rw");
        raf.seek(numPages() * BufferPool.getPageSize());
        raf.write(heapPage.getPageData());
        raf.close();

        pages.add(heapPage);

        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pageId = t.getRecordId().getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
        // Database.getBufferPool().unsafeReleasePage(tid, pageId);

        page.deleteTuple(t);

        ArrayList<Page> pages = new ArrayList<>();
        pages.add(page);
        return pages;
    }


    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        DbFileIterator hfi = new HeapFileIterator(tid, getId(), numPages());
        return hfi;
    }
}

class HeapFileIterator implements DbFileIterator {

    int _pid = 0;
    TransactionId _tid;
    int _tableId;
    Iterator<Tuple> _tupleList;
    boolean _isOpen = false;
    int _numPages;

    BufferPool bp = Database.getBufferPool();


    public HeapFileIterator (TransactionId tid, int tableId, int numPages) {
        _tid = tid;
        _tableId = tableId;
        _numPages = numPages;
    }


    private Page IteratorGetPage(int pid) throws TransactionAbortedException, DbException {
        HeapPageId hpi = new HeapPageId(_tableId, pid);
        Page page = bp.getPage(_tid, hpi, Permissions.READ_ONLY);
        // Database.getBufferPool().unsafeReleasePage(_tid, page.getId());
        return page;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        _pid = 0;
        _isOpen = true;
        HeapPage hp = (HeapPage)IteratorGetPage(_pid++);
        _tupleList =  hp.iterator();
    }

    @Override
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if ((_isOpen == true) && (_pid <= _numPages)) {
            if (_tupleList.hasNext()) {
                return true;
            }else {
                while (_pid <= _numPages) {
                    HeapPage hp = (HeapPage) IteratorGetPage(_pid++);
                    _tupleList = hp.iterator();
                    if (_tupleList.hasNext()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    @Override
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (!_isOpen) {
            throw new NoSuchElementException();
        }

        if (hasNext()) {
            return _tupleList.next();
        }

        throw new NoSuchElementException();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    public void close() {
        _isOpen = false;
    }
}

