package simpledb.storage;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */

    private PageId _pid;

    private int _tupleno;


    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        _pid = pid;
        _tupleno = tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return _tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return _pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here

        if (!(o instanceof RecordId)) {
            return false;
        }

        RecordId O = (RecordId)o;

        return O.getPageId().equals(getPageId()) && O.getTupleNumber() == getTupleNumber();

        // throw new UnsupportedOperationException("implement this");
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        String s = _pid + " " + _tupleno;

        return s.hashCode();
        // throw new UnsupportedOperationException("implement this");
    }

}
