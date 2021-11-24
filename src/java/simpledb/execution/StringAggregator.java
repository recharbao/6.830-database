package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.storage.TupleIterator;

import java.util.ArrayList;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int _gbfield;
    private Type _gbfieldtype;
    private int _afield;
    private Op _what;

    private List<List<Tuple>> _groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here

        _gbfield = gbfield;
        _gbfieldtype = gbfieldtype;
        _afield = afield;
        _what = what;
        _groups = new ArrayList<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        boolean flag = false;
        for(int i = 0; i < _groups.size(); i++) {
            if(_groups.get(i).get(0).getField(_gbfield).equals(tup.getField(_gbfield))) {
                flag = true;
                _groups.get(i).add(tup);
            }
        }

        if (!flag) {
            List<Tuple> list = new ArrayList<>();
            list.add(tup);
            _groups.add(list);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        Type[] types = {_gbfieldtype, Type.INT_TYPE};
        Tuple tuple = new Tuple(new TupleDesc(types));

        if (_what == Op.AVG) {
            return new TupleIterator(tuple.getTupleDesc(), avg());
        }else if (_what == Op.MAX) {
            return new TupleIterator(tuple.getTupleDesc(), max());
        }else if (_what == Op.MIN) {
            return new TupleIterator(tuple.getTupleDesc(), min());
        }else if (_what == Op.COUNT) {
            return new TupleIterator(tuple.getTupleDesc(), count());
        }else if (_what == Op.SUM) {
            return new TupleIterator(tuple.getTupleDesc(), sum());
        }

        throw new
                UnsupportedOperationException("please implement me for lab2");
    }


    private List<Tuple> avg() {
        List<Tuple> res = new ArrayList<>();
        for (int i = 0; i < _groups.size(); i++) {
            int sum = 0;
            for (int j = 0; j < _groups.get(i).size(); j++) {
                IntField tmp = (IntField) _groups.get(i).get(j).getField(_afield);
                sum += tmp.getValue();
            }

            Type[] types = {_gbfieldtype, Type.INT_TYPE};
            Tuple tuple = new Tuple(new TupleDesc(types));
            tuple.setField(0, _groups.get(i).get(0).getField(_gbfield));
            tuple.setField(1, new IntField(sum / _groups.get(i).size()));
            res.add(tuple);
        }

        return res;
    }
    private List<Tuple> max() {
        List<Tuple> res = new ArrayList<>();
        for (int i = 0; i < _groups.size(); i++) {
            int maxIndex = 0;
            for (int j = 0; j < _groups.get(i).size(); j++) {
                if (_groups.get(i).get(maxIndex).getField(_afield)
                        .compare(Predicate.Op.LESS_THAN,
                                _groups.get(i).get(j).getField(_afield))) {
                    maxIndex = j;
                }
            }
            Type[] types = {_gbfieldtype, Type.INT_TYPE};
            Tuple tuple = new Tuple(new TupleDesc(types));
            tuple.setField(0, _groups.get(i).get(maxIndex).getField(_gbfield));
            tuple.setField(1, _groups.get(i).get(maxIndex).getField(_afield));
            res.add(tuple);
        }

        return res;
    }
    private List<Tuple> min() {
        List<Tuple> res = new ArrayList<>();
        for (int i = 0; i < _groups.size(); i++) {
            int minIndex = 0;
            for (int j = 0; j < _groups.get(i).size(); j++) {
                if (_groups.get(i).get(minIndex).getField(_afield)
                        .compare(Predicate.Op.GREATER_THAN,
                                _groups.get(i).get(j).getField(_afield))) {
                    minIndex = j;
                }
            }
            Type[] types = {_gbfieldtype, Type.INT_TYPE};
            Tuple tuple = new Tuple(new TupleDesc(types));
            tuple.setField(0, _groups.get(i).get(minIndex).getField(_gbfield));
            tuple.setField(1, _groups.get(i).get(minIndex).getField(_afield));
            res.add(tuple);
        }

        return res;
    }
    private List<Tuple> count() {
        List<Tuple> res = new ArrayList<>();
        for (int i = 0; i < _groups.size(); i++) {
            Type[] types = {_gbfieldtype, Type.INT_TYPE};
            Tuple tuple = new Tuple(new TupleDesc(types));
            tuple.setField(0, _groups.get(i).get(0).getField(_gbfield));
            tuple.setField(1, new IntField(_groups.get(i).size()));
            res.add(tuple);
        }

        return res;
    }


    private List<Tuple> sum() {
        List<Tuple> res = new ArrayList<>();
        for (int i = 0; i < _groups.size(); i++) {
            int sum = 0;
            for (int j = 0; j < _groups.get(i).size(); j++) {
                IntField tmp = (IntField) _groups.get(i).get(j).getField(_afield);
                sum += tmp.getValue();
            }

            Type[] types = {_gbfieldtype, Type.INT_TYPE};
            Tuple tuple = new Tuple(new TupleDesc(types));
            tuple.setField(0, _groups.get(i).get(0).getField(_gbfield));
            tuple.setField(1, new IntField(sum));
            res.add(tuple);
        }

        return res;
    }


}
