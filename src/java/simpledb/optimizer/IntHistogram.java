package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.ArrayList;
import java.util.List;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    private int _buckets;
    private int _min;
    private int _max;
    private int[] _bucketsList;
    private double _totalNum;

    /**
     * Create a new IntHistogram.
     *
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     *
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     *
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't
     * simply store every value that you see in a sorted list.
     *
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        _buckets = buckets;
        _min = min;
        _max = max;
        _bucketsList = new int[buckets];
        for (int i = 0; i < buckets; i++) {
            _bucketsList[i] = 0;
        }
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
        double range = (_max - _min) / (1.0 * _buckets);
//        System.out.println("max = " + _max + " min = " + _min + " buckets = "
//                + _buckets + " v = " + v + " range = " + range);
        int index = (int) ((v - _min) / range);
        if (index >= _buckets) {
            index = _buckets - 1;
        }else if (index < 0) {
            index = 0;
        }
        _bucketsList[index] += 1;
        _totalNum++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     *
     * For example, if "op" is "GREATER_THAN" and "v" is 5,
     * return your estimate of the fraction of elements that are greater than 5.
     *
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double range = (_max - _min) / (1.0 * _buckets);
        int index = (int) ((v - _min) / range);
        if (index >= _buckets && v > _max) {
            // index = _buckets - 1;
            if (op.equals(Predicate.Op.EQUALS) || op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                return 0.0;
            }else {
                return 1.0;
            }
        }else if (index <= 0 && v < _min) {
            if (op.equals(Predicate.Op.EQUALS) || op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                return 0.0;
            }else {
                return 1.0;
            }
        }else if (v == _max) {
            index = _buckets - 1;
        }else if (v == _min) {
            index = 0;
        }

        // System.out.println("_bucketsList = " + _bucketsList[index] + " index = " + index + " range = " + range + " total = " + _totalNum);

        if (op.equals(Predicate.Op.EQUALS)) {
            return _bucketsList[index] / _totalNum;
        }else if (op.equals(Predicate.Op.LESS_THAN) || op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
            double b_f;
            double left = v - (range * index);
            b_f = _bucketsList[index] / _totalNum;
            b_f *= (left / range);
            if (op.equals(Predicate.Op.LESS_THAN_OR_EQ)) {
                b_f += _bucketsList[index] * (1.0 - (left / range)) / _totalNum;
            }
            for (int i = index - 1; i >= 0; i--) {
                b_f += _bucketsList[i] / _totalNum;
            }
            return b_f;
        }else if (op.equals(Predicate.Op.GREATER_THAN) || op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
            double b_f;
            double right = (range * (index + 1)) - v;
//            if (op.equals(Predicate.Op.GREATER_THAN)) {
//                b_f = _bucketsList[index] / _totalNum;
//                b_f *= (right / range);
//            }else if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
//                b_f = _bucketsList[index] / (range * _totalNum);
//            }

            b_f = _bucketsList[index] / _totalNum;
            b_f *= (right / range);
            if (op.equals(Predicate.Op.GREATER_THAN_OR_EQ)) {
                b_f += _bucketsList[index] * (1.0 - (right / range)) / _totalNum;
            }

            for (int i = index + 1; i < _buckets; i++) {
                b_f += _bucketsList[i] / _totalNum;
            }
            return b_f;
        }else if (op.equals(Predicate.Op.NOT_EQUALS)) {
            double b_f = 0.0;
            for (int i = 0; i < _buckets; i++) {
                b_f += _bucketsList[i] / _totalNum;
            }

            b_f -= (_bucketsList[index] / _totalNum);
            return b_f;
        }

        return -1.0;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
