package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */

    private TransactionId _tid;

    private int _tableid;

    private String _tableAlias;

    // private boolean _isOpen = false;

    private DbFile _dbf;

    DbFileIterator _tuples;

    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        _tid = tid;
        _tableid = tableid;
        _tableAlias = tableAlias;
        _dbf = Database.getCatalog().getDatabaseFile(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(_tableid);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return _tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        _tableid = tableid;
        _tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        _tuples = _dbf.iterator(_tid);
        _tuples.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        Type[] ty = new Type[_dbf.getTupleDesc().numFields()];
        String[] fd = new String[_dbf.getTupleDesc().numFields()];
        for (int i = 0; i < _dbf.getTupleDesc().numFields(); i++) {
            fd[i] = _tableAlias + "." + _dbf.getTupleDesc().getFieldName(i);
            ty[i] = _dbf.getTupleDesc().getFieldType(i);
        }
        TupleDesc td = new TupleDesc(ty, fd);

        return td;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return _tuples.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return _tuples.next();
    }

    public void close() {
        // some code goes here
        // _isOpen = false;
        _tuples.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
       _tuples.rewind();
    }
}
