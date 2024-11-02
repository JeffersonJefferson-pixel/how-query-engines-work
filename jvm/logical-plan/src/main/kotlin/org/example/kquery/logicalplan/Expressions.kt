package org.example.kquery.logicalplan

import org.apache.arrow.vector.types.pojo.ArrowType
import org.example.kquery.datatypes.ArrowTypes
import org.example.kquery.datatypes.KQueryField
import java.sql.SQLException

/** Column expression represents a reference to a named column. */
class Column(val name: String): LogicalExpr {
    override fun toField(input: LogicalPlan): KQueryField {
        return input.schema().fields.find { it.name == name } ?:
            throw SQLException("No column named '$name'")
    }

    override fun toString(): String {
        return "#$name"
    }
}

fun col(name: String) = Column(name)

// Literal Expressions.

class LiteralString(val str: String): LogicalExpr {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(str, ArrowTypes.StringType)
    }

    override fun toString(): String {
        return "'$str'"
    }
}

fun lit(value: String) = LiteralString(value)

class LiteralLong(val n: Long): LogicalExpr {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(n.toString(), ArrowTypes.Int64Type);
    }

    override fun toString(): String {
        return n.toString()
    }
}

class LiteralDouble(val n: Double): LogicalExpr {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(n.toString(), ArrowTypes.DoubleType);
    }

    override fun toString(): String {
        return n.toString()
    }
}

class CastExpression(val expr: LogicalExpr, val dataType: ArrowType): LogicalExpr {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(expr.toField(input).name, dataType)
    }

    override fun toString(): String {
        return "CAST($expr AS $dataType)"
    }
}

/** Binary expressions are expressions that take two inputs.
 * There are Comparison expression, Boolean expression, and Math expression.
 */
abstract class BinaryExpr(
    val name: String,
    val op: String,
    val l: LogicalExpr,
    val r: LogicalExpr
): LogicalExpr {
    override fun toString(): String {
        return "$l $op $r"
    }
}

abstract class UnaryExpr(val name: String, val op: String, val expr: LogicalExpr): LogicalExpr {
   override fun toString(): String {
       return "$op $expr"
   }
}

class Not(expr: LogicalExpr): UnaryExpr("not", "NOT", expr) {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(name, ArrowTypes.BooleanType)
    }
}


/** Boolean expression are Binary expression that produce a Boolean result.
 * There are Comparison expressions and Boolean expressions.
 */
abstract class BooleanBinaryExpr(
    name: String,
    op: String,
    l: LogicalExpr,
    r: LogicalExpr
) : BinaryExpr(name, op, l, r) {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(name, ArrowTypes.BooleanType)
    }
}

// Comparison Expressions

class Eq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("eq", "=", l, r)

class Neq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("neq", "!=", l, r)

class Gt(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("gt", ">", l, r)

class GtEq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("gteq", ">=", l, r)

class Lt(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("lt", "<", l, r)

class LtEq(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("lteq", "<=", l, r)

infix fun LogicalExpr.eq(rhs: LogicalExpr): LogicalExpr {
    return Eq(this, rhs)
}

// Boolean Expressions

class And(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("and", "AND", l, r)

class Or(l: LogicalExpr, r: LogicalExpr) : BooleanBinaryExpr("or", "OR", l, r)

// Math Expressions

abstract class MathExpr(
    name: String,
    op: String,
    l: LogicalExpr,
    r: LogicalExpr
) : BinaryExpr(name, op, l, r) {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(name, l.toField(input).dataType)
    }
}

class Add(l: LogicalExpr, r: LogicalExpr) : MathExpr("add", "+", l, r)

class Subtract(l: LogicalExpr, r: LogicalExpr) : MathExpr("subtract", "-", l, r)

class Multiply(l: LogicalExpr, r: LogicalExpr) : MathExpr("mult", "*", l, r)

class Divide(l: LogicalExpr, r: LogicalExpr) : MathExpr("div", "/", l, r)

class Modulus(l: LogicalExpr, r: LogicalExpr) : MathExpr("mod","%", l, r)

// Aggregate Expressions

abstract class AggregateExpr(
    val name: String,
    val expr: LogicalExpr
) : LogicalExpr {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(name, expr.toField(input).dataType)
    }

    override fun toString(): String {
        return "$name($expr)"
    }
}

class Sum(input: LogicalExpr) : AggregateExpr("SUM", input)
class Min(input: LogicalExpr) : AggregateExpr("MIN", input)
class Max(input: LogicalExpr) : AggregateExpr("MAX", input)
class Avg(input: LogicalExpr) : AggregateExpr("AVG", input)


fun max(expr: LogicalExpr) = Max(expr)

class Count(input: LogicalExpr) : AggregateExpr("COUNT", input) {
    override fun toField(input: LogicalPlan): KQueryField {
        return KQueryField(name, ArrowTypes.Int32Type)
    }
}