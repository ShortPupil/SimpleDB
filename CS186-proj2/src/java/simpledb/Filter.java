package simpledb;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;

    private Predicate p;
    private DbIterator child;

    //缓存过滤结果
    private TupleIterator filterResult;
    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     *
     * @param p     The predicate to filter tuples with
     * @param child The child operator
     */
    public Filter(Predicate p, DbIterator child) {
        // some code goes here
        this.p = p;
        this.child = child;
    }

    public Predicate getPredicate() {
        // some code goes here
        return p;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return child.getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
        filterResult = filter(child, p);
        filterResult.open();
    }

    private TupleIterator filter(DbIterator child, Predicate predicate) throws DbException, TransactionAbortedException {
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        while (child.hasNext()) {
            Tuple t = child.next();
            if (predicate.filter(t)) {
                tuples.add(t);
            }
        }
        return new TupleIterator(getTupleDesc(), tuples);
    }

    public void close() {
        // some code goes here
        super.close();
        child.close();
        filterResult = null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        filterResult.rewind(); //倒带
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     *      * child operator, applying the predicate to them and returning those that
     *      * pass the predicate (i.e. for which the Predicate.filter() returns true.)

     * AbstractDbIterator.readNext实现。从迭代中迭代元组
     *子操作符，将谓词应用于它们并返回那些谓词
     *传递谓词（即Predicate.filter（）返回true。）
     * @return The next tuple that passes the filter, or null if there are no
     * more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(filterResult.hasNext())
            return filterResult.next();
        else return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        return new DbIterator[]{this.child};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if(this.child != children[0]) {
            this.child = children[0];
        }
    }

}
