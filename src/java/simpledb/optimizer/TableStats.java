package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the
        // DbFile for the table in question,
        // then scan through its tuples and calculate
        // the values that you need.
        // You should try to do this reasonably efficiently, but you don't
        // necessarily have to (for example) do everything
        // in a single scan of the table.
        // some code goes here
        _hf = (HeapFile) Database.getCatalog().getDatabaseFile(tableid);
        _ioCostPerPage = ioCostPerPage;
        mapIntHist = new HashMap<>();
        mapStringHist = new HashMap<>();
        TupleDesc td = _hf.getTupleDesc();

        for (int i = 0; i < td.numFields(); i++) {
            Type fieldType = td.getFieldType(i);
            int max = -999999999;
            int min = 999999999;
            if (fieldType.equals(Type.INT_TYPE)) {
                tuplesCount = 0;
                for (int j = 0; j < _hf.numPages(); j++) {
                    PageId pageId = new HeapPageId(tableid, j);
                    HeapPage heapPage = (HeapPage) _hf.readPage(pageId);
                    Iterator<Tuple> pageTuples = heapPage.iterator();
                    while (pageTuples.hasNext()) {
                        tuplesCount++;
                        Tuple tuple = pageTuples.next();
                        Field maxField = new IntField(max);
                        Field minField = new IntField(min);
                        if (tuple.getField(i).compare(Predicate.Op.GREATER_THAN, maxField)) {
                            IntField intField = (IntField) tuple.getField(i);
                            max = intField.getValue();
                        }
                        if (tuple.getField(i).compare(Predicate.Op.LESS_THAN, minField)) {
                            IntField intField = (IntField) tuple.getField(i);
                            min = intField.getValue();
                        }
                    }
                }
                mapIntHist.put(i, new IntHistogram(NUM_HIST_BINS, min, max));
                for (int k = 0; k < _hf.numPages(); k++) {
                    PageId pageId = new HeapPageId(tableid, k);
                    HeapPage heapPage = (HeapPage) _hf.readPage(pageId);
                    Iterator<Tuple> pageTuples = heapPage.iterator();
                    while (pageTuples.hasNext()) {
                        Tuple tuple = pageTuples.next();
                        IntHistogram intHistogram = mapIntHist.get(i);
                        IntField intField = (IntField) tuple.getField(i);
                        intHistogram.addValue(intField.getValue());
                    }
                }
            } else if (fieldType.equals(Type.STRING_TYPE)) {
                mapStringHist.put(i, new StringHistogram(NUM_HIST_BINS));
                for (int k = 0; k < _hf.numPages(); k++) {
                    PageId pageId = new HeapPageId(tableid, k);
                    HeapPage heapPage = (HeapPage) _hf.readPage(pageId);
                    Iterator<Tuple> pageTuples = heapPage.iterator();
                    while (pageTuples.hasNext()) {
                        Tuple tuple = pageTuples.next();
                        StringHistogram stringHistogram = mapStringHist.get(i);
                        StringField stringField = (StringField) tuple.getField(i);
                        stringHistogram.addValue(stringField.getValue());
                    }
                }
            }
        }
    }

    private int _ioCostPerPage;
    private HeapFile _hf;
    private Map<Integer, IntHistogram> mapIntHist;
    private Map<Integer, StringHistogram> mapStringHist;
    private int tuplesCount;

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        // some code goes here
        return _ioCostPerPage * _hf.numPages();
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        // some code goes here
        return (int) (totalTuples() * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        TupleDesc td = _hf.getTupleDesc();
        Type fieldType = td.getFieldType(field);
        if (fieldType.equals(Type.INT_TYPE)) {
            try {
                return mapIntHist.get(field).avgSelectivity(op);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }else if (fieldType.equals(Type.STRING_TYPE)) {
            try {
                return mapStringHist.get(field).avgSelectivity(op);
            }catch (Exception e) {
                e.printStackTrace();
            }
        }
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        // some code goes here
        TupleDesc td = _hf.getTupleDesc();
        Type fieldType = td.getFieldType(field);
        if (fieldType.equals(Type.INT_TYPE)) {
            IntField intField = (IntField) constant;
            return mapIntHist.get(field).estimateSelectivity(op, intField.getValue());
        }else if (fieldType.equals(Type.STRING_TYPE)) {
            StringField stringField = (StringField) constant;
            return mapStringHist.get(field).estimateSelectivity(op, stringField.getValue());
        }
        return -1.0;
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return tuplesCount;
    }

}
