package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    private TransactionId _t;
    private OpIterator _child;
    private boolean _called;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        _t = t;
        _child = child;
        _called = false;
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int count = 0;
        while (_child.hasNext()) {
            count++;
            Tuple tuple = _child.next();
            try {
                Database.getBufferPool().deleteTuple(_t, tuple);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }


        if (_called == false) {
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
