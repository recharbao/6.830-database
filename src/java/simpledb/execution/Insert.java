package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator _child;
    private TransactionId _t;
    private int _tableId;
    private boolean _called;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        _child = child;
        _t = t;
        _tableId = tableId;
        _called = false;

        if (!Database.getCatalog().getTupleDesc(tableId).equals(child.getTupleDesc())) {
            throw new DbException("TupleDesc of child differs from table into which we are to" +
                    " insert !");
        }

    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return _child.getTupleDesc();
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        _child.open();
    }

    public void close() {
        // some code goes here
        super.close();
        _child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count = 0;
        while (_child.hasNext()) {
            count++;
            Tuple tuple = _child.next();
            try {
                System.out.println("Thread : " + Thread.currentThread() + "begin insert 2222222222222222222222222222222222222222222222222222222");
                Database.getBufferPool().insertTuple(_t, _tableId, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (!_called) {
            Type[] types = {Type.INT_TYPE};
            Tuple tuple = new Tuple(new TupleDesc(types));
            tuple.setField(0, new IntField(count));
            _called = true;
            return tuple;
        }

        return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] childs = {_child};
        return childs;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        _child = children[0];
    }
}
