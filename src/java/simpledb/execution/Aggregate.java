package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    private OpIterator _child;
    private int _afield;
    private int _gfield;
    private Aggregator.Op _aop;

    private OpIterator _a = null;
    Aggregator _aggregator = null;

    TupleDesc _td = null;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        _child = child;
        _afield = afield;
        _gfield = gfield;
        _aop = aop;

        if (gfield != -1) {
            Type[] types = {_child.getTupleDesc().getFieldType(gfield), _child.getTupleDesc().getFieldType(afield)};
            _td = new TupleDesc(types);
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        return _gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        return _child.getTupleDesc().getFieldName(_gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return _afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return _child.getTupleDesc().getFieldName(_afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return _aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        _child.open();

        if (_gfield == -1) {
            List<Tuple> list = new ArrayList<>();
            Type[] type = {Type.INT_TYPE};
            while (_child.hasNext()) {
                Tuple tuple = _child.next();
                Tuple tuple1 = new Tuple(new TupleDesc(type));
                tuple1.setField(0, tuple.getField(_afield));
                list.add(tuple1);
            }

            if (_aop == Aggregator.Op.AVG) {
                _a = new TupleIterator(new TupleDesc(type), avg(list));
            }else if (_aop == Aggregator.Op.MAX) {
                _a = new TupleIterator(new TupleDesc(type), max(list));
            }else if (_aop == Aggregator.Op.MIN) {
                _a = new TupleIterator(new TupleDesc(type), min(list));
            }else if (_aop == Aggregator.Op.COUNT) {
                _a = new TupleIterator(new TupleDesc(type), count(list));
            }else if (_aop == Aggregator.Op.SUM) {
                _a = new TupleIterator(new TupleDesc(type), sum(list));
            }

            _a.open();
            return;
        }

        if (_child.hasNext()) {
            Tuple tuple = _child.next();
            if (tuple.getField(_gfield).getType() == Type.INT_TYPE) {
                _aggregator = new IntegerAggregator(_gfield, Type.INT_TYPE, _afield, _aop);
            }else if (tuple.getField(_gfield).getType() == Type.STRING_TYPE) {
                _aggregator = new StringAggregator(_gfield, Type.STRING_TYPE, _afield, _aop);
            }
        }

        _child.rewind();

        while (_child.hasNext()) {
            _aggregator.mergeTupleIntoGroup(_child.next());
        }

        _a = _aggregator.iterator();

        _a.open();
    }

    private List<Tuple> avg(List<Tuple> list) {
        List<Tuple> res = new ArrayList<>();
        int sum = 0;
        for (int i = 0; i < list.size(); i++) {
            IntField v = (IntField) list.get(i).getField(0);
            sum += v.getValue();
        }
        Type[] type = {Type.INT_TYPE};
        Tuple tuple = new Tuple(new TupleDesc(type));
        tuple.setField(0, new IntField(sum / list.size()));
        res.add(tuple);
        return res;
    }

    private List<Tuple> max(List<Tuple> list) {
        List<Tuple> res = new ArrayList<>();
        int max = -100000;
        for (int i = 0; i < list.size(); i++) {
            IntField v = (IntField) list.get(i).getField(0);
            if (max < v.getValue()) {
                max = v.getValue();
            }
        }
        Type[] type = {Type.INT_TYPE};
        Tuple tuple = new Tuple(new TupleDesc(type));
        tuple.setField(0, new IntField(max));
        res.add(tuple);
        return res;
    }

    private List<Tuple> min(List<Tuple> list) {
        List<Tuple> res = new ArrayList<>();
        int min = 10000;
        for (int i = 0; i < list.size(); i++) {
            IntField v = (IntField) list.get(i).getField(0);
            if (min > v.getValue()) {
                min = v.getValue();
            }
        }
        Type[] type = {Type.INT_TYPE};
        Tuple tuple = new Tuple(new TupleDesc(type));
        tuple.setField(0, new IntField(min));
        res.add(tuple);
        return res;
    }

    private List<Tuple> count(List<Tuple> list) {
        List<Tuple> res = new ArrayList<>();
        int sum = 0;
        for (int i = 0; i < list.size(); i++) {
            sum += 1;
        }
        Type[] type = {Type.INT_TYPE};
        Tuple tuple = new Tuple(new TupleDesc(type));
        tuple.setField(0, new IntField(sum));
        res.add(tuple);
        return res;
    }

    private List<Tuple> sum(List<Tuple> list) {
        List<Tuple> res = new ArrayList<>();
        int sum = 0;
        for (int i = 0; i < list.size(); i++) {
            IntField v = (IntField) list.get(i).getField(0);
            sum += v.getValue();
        }
        Type[] type = {Type.INT_TYPE};
        Tuple tuple = new Tuple(new TupleDesc(type));
        tuple.setField(0, new IntField(sum));
        res.add(tuple);
        return res;
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (_a.hasNext()) {
            return _a.next();
        }
        return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        close();
        open();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return _td;
    }

    public void close() {
        // some code goes here
        super.close();
        _child.close();
        _a.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] child = {_child};
        return child;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        _child = children[0];
    }
}
