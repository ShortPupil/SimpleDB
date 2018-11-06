package simpledb;

import java.io.Serializable;

/**
 * Predicate compares tuples to a specified Field value.
 * 将元组与指定的字段值进行比较
 */
public class Predicate implements Serializable {

    private static final long serialVersionUID = 1L;

    private Field operand;

    private Op op;

    private int field;

    /** Constants used for return codes in Field.compare
     *  用于Field.compare中的返回码的常量*/
    public enum Op implements Serializable {
        EQUALS, GREATER_THAN, LESS_THAN, LESS_THAN_OR_EQ, GREATER_THAN_OR_EQ, LIKE, NOT_EQUALS;

        /**
         * Interface to access operations by a string containing an integer
         * index for command-line convenience.
         * 通过包含整数的字符串访问操作的接口
         * 命令行方便的索引。
         * 
         * @param s
         *            a string containing a valid integer Op index
         */
        public static Op getOp(String s) {
            return getOp(Integer.parseInt(s));
        }

        /**
         * Interface to access operations by integer value for command-line
         * convenience.
         * 用于命令行的整数值方便访问操作的接口
         * 
         * @param i
         *            a valid integer Op index
         */
        public static Op getOp(int i) {
            return values()[i];
        }

        public String toString() {
            if (this == EQUALS)
                return "=";
            if (this == GREATER_THAN)
                return ">";
            if (this == LESS_THAN)
                return "<";
            if (this == LESS_THAN_OR_EQ)
                return "<=";
            if (this == GREATER_THAN_OR_EQ)
                return ">=";
            if (this == LIKE)
                return "like";
            if (this == NOT_EQUALS)
                return "<>";
            throw new IllegalStateException("impossible to reach here");
        }
    }

    /**
     * Constructor.
     * 
     * @param field
     *            field number of passed in tuples to compare against.
     *            要传递给元组的字段数，来进行比较
     * @param op
     *            operation to use for comparison
     *            操作符
     * @param operand
     *            field value to compare passed in tuples to
     */
    public Predicate(int field, Op op, Field operand) {
        // some code goes here
        this.field = field;
        this.op = op;
        this.operand = operand;
    }

    /**
     * @return the field number
     */
    public int getField()
    {
        // some code goes here
        return field;
    }

    /**
     * @return the operator
     */
    public Op getOp()
    {
        // some code goes here
        return op;
    }
    
    /**
     * @return the operand
     */
    public Field getOperand()
    {
        // some code goes here
        return operand;
    }
    
    /**
     * Compares the field number of t specified in the constructor to the
     * operand field specified in the constructor using the operator specific in
     * the constructor. The comparison can be made through Field's compare
     * method.
     * 比较构造函数中指定的t的字段数
     * 使用特定运算符在构造函数中指定的操作数字段
     * 构造函数。可以通过Field的比较进行比较
     * 方法。
     *
     * @param t
     *            The tuple to compare against
     * @return true if the comparison is true, false otherwise.
     */
    public boolean filter(Tuple t) {
        // some code goes here
        return t.getField(field).compare(op, operand);
    }

    /**
     * Returns something useful, like "f = field_id op = op_string operand =
     * operand_string
     */
    public String toString() {
        // some code goes here
        StringBuilder builder = new StringBuilder();
        builder.append("(tuple x).fields[").append(field).append("]")
                .append(op.toString()).append(" ").append(operand).append(" ?");
        return builder.toString();
    }
}
