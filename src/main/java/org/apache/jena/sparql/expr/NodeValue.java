//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by Fernflower decompiler)
//

package org.apache.jena.sparql.expr;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Calendar;
import javax.xml.datatype.DatatypeConstants;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;
import javax.xml.datatype.XMLGregorianCalendar;
import org.apache.jena.JenaRuntime;
import org.apache.jena.atlas.lib.DateTimeUtils;
import org.apache.jena.atlas.lib.StrUtils;
import org.apache.jena.atlas.logging.Log;
import org.apache.jena.datatypes.DatatypeFormatException;
import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.datatypes.xsd.XSDDateTime;
import org.apache.jena.ext.xerces.DatatypeFactoryInst;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.impl.LiteralLabel;
import org.apache.jena.sparql.ARQInternalErrorException;
import org.apache.jena.sparql.SystemARQ;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.expr.nodevalue.NodeFunctions;
import org.apache.jena.sparql.expr.nodevalue.NodeValueBoolean;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDT;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDecimal;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDouble;
import org.apache.jena.sparql.expr.nodevalue.NodeValueDuration;
import org.apache.jena.sparql.expr.nodevalue.NodeValueFloat;
import org.apache.jena.sparql.expr.nodevalue.NodeValueInteger;
import org.apache.jena.sparql.expr.nodevalue.NodeValueLang;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.jena.sparql.expr.nodevalue.NodeValueSortKey;
import org.apache.jena.sparql.expr.nodevalue.NodeValueString;
import org.apache.jena.sparql.expr.nodevalue.NodeValueVisitor;
import org.apache.jena.sparql.expr.nodevalue.XSDFuncOp;
import org.apache.jena.sparql.function.FunctionEnv;
import org.apache.jena.sparql.graph.NodeConst;
import org.apache.jena.sparql.graph.NodeTransform;
import org.apache.jena.sparql.serializer.SerializationContext;
import org.apache.jena.sparql.util.FmtUtils;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.sparql.util.NodeUtils;
import org.apache.jena.sparql.util.RomanNumeral;
import org.apache.jena.sparql.util.RomanNumeralDatatype;
import org.apache.jena.sparql.util.Utils;
import org.apache.jena.sys.JenaSystem;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import se.liu.ida.rspqlstar.function.LazyNodeValue;

public abstract class NodeValue extends ExprNode {
    private static Logger log;
    public static boolean VerboseWarnings;
    public static boolean VerboseExceptions;
    public static final BigInteger IntegerZERO;
    public static final BigDecimal DecimalZERO;
    public static final NodeValue TRUE;
    public static final NodeValue FALSE;
    public static final NodeValue nvZERO;
    public static final NodeValue nvONE;
    public static final NodeValue nvTEN;
    public static final NodeValue nvNaN;
    public static final NodeValue nvINF;
    public static final NodeValue nvNegINF;
    public static final NodeValue nvEmptyString;
    private static final String strForUnNode = "node value nothing";
    /** @deprecated */
    @Deprecated
    public static final NodeValue nvNothing;
    public static final String xsdNamespace = "http://www.w3.org/2001/XMLSchema#";
    public static DatatypeFactory xmlDatatypeFactory;
    private Node node = null;
    private static final String dtXSDprecisionDecimal = "http://www.w3.org/2001/XMLSchema#precisionDecimal";

    protected NodeValue() {
    }

    protected NodeValue(Node n) {
        this.node = n;
    }

    public static NodeValue parse(String string) {
        return makeNode(NodeFactoryExtra.parseNode(string));
    }

    public static NodeValue makeInteger(long i) {
        return new NodeValueInteger(BigInteger.valueOf(i));
    }

    public static NodeValue makeInteger(BigInteger i) {
        return new NodeValueInteger(i);
    }

    public static NodeValue makeInteger(String lexicalForm) {
        return new NodeValueInteger(new BigInteger(lexicalForm));
    }

    public static NodeValue makeFloat(float f) {
        return new NodeValueFloat(f);
    }

    public static NodeValue makeDouble(double d) {
        return new NodeValueDouble(d);
    }

    public static NodeValue makeString(String s) {
        return new NodeValueString(s);
    }

    public static NodeValue makeSortKey(String s, String collation) {
        return new NodeValueSortKey(s, collation);
    }

    public static NodeValue makeLangString(String s, String lang) {
        return new NodeValueLang(s, lang);
    }

    public static NodeValue makeDecimal(BigDecimal d) {
        return new NodeValueDecimal(d);
    }

    public static NodeValue makeDecimal(long i) {
        return new NodeValueDecimal(BigDecimal.valueOf(i));
    }

    public static NodeValue makeDecimal(double d) {
        return new NodeValueDecimal(BigDecimal.valueOf(d));
    }

    public static NodeValue makeDecimal(String lexicalForm) {
        return makeNode(lexicalForm, XSDDatatype.XSDdecimal);
    }

    public static NodeValue makeDateTime(String lexicalForm) {
        return makeNode(lexicalForm, XSDDatatype.XSDdateTime);
    }

    public static NodeValue makeDate(String lexicalForm) {
        return makeNode(lexicalForm, XSDDatatype.XSDdate);
    }

    public static NodeValue makeDateTime(Calendar cal) {
        String lex = DateTimeUtils.calendarToXSDDateTimeString(cal);
        return makeNode(lex, XSDDatatype.XSDdateTime);
    }

    public static NodeValue makeDateTime(XMLGregorianCalendar cal) {
        String lex = cal.toXMLFormat();
        Node node = NodeFactory.createLiteral(lex, XSDDatatype.XSDdateTime);
        return new NodeValueDT(lex, node);
    }

    public static NodeValue makeDate(Calendar cal) {
        String lex = DateTimeUtils.calendarToXSDDateString(cal);
        return makeNode(lex, XSDDatatype.XSDdate);
    }

    public static NodeValue makeDate(XMLGregorianCalendar cal) {
        String lex = cal.toXMLFormat();
        Node node = NodeFactory.createLiteral(lex, XSDDatatype.XSDdate);
        return new NodeValueDT(lex, node);
    }

    public static NodeValue makeDuration(String lexicalForm) {
        return makeNode(lexicalForm, XSDDatatype.XSDduration);
    }

    public static NodeValue makeDuration(Duration duration) {
        return new NodeValueDuration(duration);
    }

    public static NodeValue makeNodeDuration(Duration duration, Node node) {
        return new NodeValueDuration(duration, node);
    }

    public static NodeValue makeBoolean(boolean b) {
        return b ? TRUE : FALSE;
    }

    public static NodeValue booleanReturn(boolean b) {
        return b ? TRUE : FALSE;
    }

    public static NodeValue makeNode(Node n) {
        NodeValue nv = nodeToNodeValue(n);
        return nv;
    }

    public static NodeValue makeNode(String lexicalForm, RDFDatatype dtype) {
        Node n = NodeFactory.createLiteral(lexicalForm, dtype);
        NodeValue nv = makeNode(n);
        return nv;
    }

    public static NodeValue makeNode(String lexicalForm, String langTag, Node datatype) {
        String uri = datatype == null ? null : datatype.getURI();
        return makeNode(lexicalForm, langTag, uri);
    }

    public static NodeValue makeNode(String lexicalForm, String langTag, String datatype) {
        if (datatype != null && datatype.equals("")) {
            datatype = null;
        }

        if (langTag != null && datatype != null) {
            Log.warn(NodeValue.class, "Both lang tag and datatype defined (lexcial form '" + lexicalForm + "')");
        }

        Node n = null;
        if (langTag != null) {
            n = NodeFactory.createLiteral(lexicalForm, langTag);
        } else if (datatype != null) {
            RDFDatatype dType = TypeMapper.getInstance().getSafeTypeByName(datatype);
            n = NodeFactory.createLiteral(lexicalForm, dType);
        } else {
            n = NodeFactory.createLiteral(lexicalForm);
        }

        return makeNode(n);
    }

    public static NodeValue makeNodeBoolean(boolean b) {
        return b ? TRUE : FALSE;
    }

    public static NodeValue makeNodeBoolean(String lexicalForm) {
        NodeValue nv = makeNode(lexicalForm, (String)null, (String)XSDDatatype.XSDboolean.getURI());
        return nv;
    }

    public static NodeValue makeNodeInteger(long v) {
        NodeValue nv = makeNode(Long.toString(v), (String)null, (String)XSDDatatype.XSDinteger.getURI());
        return nv;
    }

    public static NodeValue makeNodeInteger(String lexicalForm) {
        NodeValue nv = makeNode(lexicalForm, (String)null, (String)XSDDatatype.XSDinteger.getURI());
        return nv;
    }

    public static NodeValue makeNodeFloat(float f) {
        NodeValue nv = makeNode(Utils.stringForm(f), (String)null, (String)XSDDatatype.XSDfloat.getURI());
        return nv;
    }

    public static NodeValue makeNodeFloat(String lexicalForm) {
        NodeValue nv = makeNode(lexicalForm, (String)null, (String)XSDDatatype.XSDdouble.getURI());
        return nv;
    }

    public static NodeValue makeNodeDouble(double v) {
        NodeValue nv = makeNode(Utils.stringForm(v), (String)null, (String)XSDDatatype.XSDdouble.getURI());
        return nv;
    }

    public static NodeValue makeNodeDouble(String lexicalForm) {
        NodeValue nv = makeNode(lexicalForm, (String)null, (String)XSDDatatype.XSDdouble.getURI());
        return nv;
    }

    public static NodeValue makeNodeDecimal(BigDecimal decimal) {
        NodeValue nv = makeNode(Utils.stringForm(decimal), (String)null, (String)XSDDatatype.XSDdecimal.getURI());
        return nv;
    }

    public static NodeValue makeNodeDecimal(String lexicalForm) {
        NodeValue nv = makeNode(lexicalForm, (String)null, (String)XSDDatatype.XSDdecimal.getURI());
        return nv;
    }

    public static NodeValue makeNodeString(String string) {
        NodeValue nv = makeNode(string, (String)null, (String)((String)null));
        return nv;
    }

    public static NodeValue makeNodeDateTime(Calendar date) {
        String lex = DateTimeUtils.calendarToXSDDateTimeString(date);
        NodeValue nv = makeNode(lex, XSDDatatype.XSDdateTime);
        return nv;
    }

    public static NodeValue makeNodeDateTime(String lexicalForm) {
        NodeValue nv = makeNode(lexicalForm, XSDDatatype.XSDdateTime);
        return nv;
    }

    public static NodeValue makeNodeDate(Calendar date) {
        String lex = DateTimeUtils.calendarToXSDDateString(date);
        NodeValue nv = makeNode(lex, XSDDatatype.XSDdate);
        return nv;
    }

    public static NodeValue makeNodeDate(String lexicalForm) {
        NodeValue nv = makeNode(lexicalForm, XSDDatatype.XSDdate);
        return nv;
    }

    public NodeValue eval(Binding binding, FunctionEnv env) {
        return this;
    }

    public Expr copySubstitute(Binding binding) {
        return this;
    }

    public Expr applyNodeTransform(NodeTransform transform) {
        Node n = this.asNode();
        n = (Node)transform.apply(n);
        return makeNode(n);
    }

    public Node evalNode(Binding binding, ExecutionContext execCxt) {
        return this.asNode();
    }

    public boolean isConstant() {
        return true;
    }

    public NodeValue getConstant() {
        return this;
    }

    public boolean isIRI() {
        if (this.node == null) {
            return false;
        } else {
            this.forceToNode();
            return this.node.isURI();
        }
    }

    public boolean isBlank() {
        if (this.node == null) {
            return false;
        } else {
            this.forceToNode();
            return this.node.isBlank();
        }
    }

    public static boolean sameAs(NodeValue nv1, NodeValue nv2) {
        if (nv1 != null && nv2 != null) {
            ValueSpaceClassification compType = classifyValueOp(nv1, nv2);
            Node node1;
            Node node2;
            int x;
            switch(compType) {
                case VSPACE_NUM:
                    return XSDFuncOp.compareNumeric(nv1, nv2) == 0;
                case VSPACE_DATETIME:
                case VSPACE_DATE:
                case VSPACE_TIME:
                case VSPACE_G_YEAR:
                case VSPACE_G_YEARMONTH:
                case VSPACE_G_MONTH:
                case VSPACE_G_MONTHDAY:
                case VSPACE_G_DAY:
                    x = XSDFuncOp.compareDateTime(nv1, nv2);
                    if (x == 2) {
                        throw new ExprNotComparableException("Indeterminate dateTime comparison");
                    }

                    return x == 0;
                case VSPACE_DURATION:
                    x = XSDFuncOp.compareDuration(nv1, nv2);
                    if (x == 2) {
                        throw new ExprNotComparableException("Indeterminate duration comparison");
                    }

                    return x == 0;
                case VSPACE_STRING:
                    return XSDFuncOp.compareString(nv1, nv2) == 0;
                case VSPACE_BOOLEAN:
                    return XSDFuncOp.compareBoolean(nv1, nv2) == 0;
                case VSPACE_LANG:
                    node1 = nv1.asNode();
                    node2 = nv2.asNode();
                    return node1.getLiteralLexicalForm().equals(node2.getLiteralLexicalForm()) && node1.getLiteralLanguage().equalsIgnoreCase(node2.getLiteralLanguage());
                case VSPACE_NODE:
                    return NodeFunctions.sameTerm(nv1.getNode(), nv2.getNode());
                case VSPACE_UNKNOWN:
                    node1 = nv1.asNode();
                    node2 = nv2.asNode();
                    if (!SystemARQ.ValueExtensions) {
                        return NodeFunctions.rdfTermEquals(node1, node2);
                    } else {
                        if (node1.isLiteral() && node2.isLiteral()) {
                            if (NodeFunctions.sameTerm(node1, node2)) {
                                return true;
                            }

                            if (node1.getLiteralLanguage().equals("") && node2.getLiteralLanguage().equals("")) {
                                raise(new ExprEvalException("Unknown equality test: " + nv1 + " and " + nv2));
                                throw new ARQInternalErrorException("raise returned (sameValueAs)");
                            }

                            return false;
                        }

                        return false;
                    }
                case VSPACE_SORTKEY:
                    return nv1.getSortKey().compareTo(nv2.getSortKey()) == 0;
                case VSPACE_DIFFERENT:
                    if (!SystemARQ.ValueExtensions && nv1.isLiteral() && nv2.isLiteral()) {
                        raise(new ExprEvalException("Incompatible: " + nv1 + " and " + nv2));
                    }

                    return false;
                default:
                    throw new ARQInternalErrorException("sameValueAs failure " + nv1 + " and " + nv2);
            }
        } else {
            throw new ARQInternalErrorException("Attempt to sameValueAs on a null");
        }
    }

    public static boolean notSameAs(Node n1, Node n2) {
        return notSameAs(makeNode(n1), makeNode(n2));
    }

    public static boolean notSameAs(NodeValue nv1, NodeValue nv2) {
        return !sameAs(nv1, nv2);
    }

    public static int compareAlways(NodeValue nv1, NodeValue nv2) {
        try {
            int x = compare(nv1, nv2, true);
            if (x != 0) {
                return x;
            }
        } catch (ExprNotComparableException var3) {
        }

        return NodeUtils.compareRDFTerms(nv1.asNode(), nv2.asNode());
    }

    public static int compare(NodeValue nv1, NodeValue nv2) {
        if (nv1 != null && nv2 != null) {
            int x = compare(nv1, nv2, false);
            return x;
        } else {
            throw new ARQInternalErrorException("Attempt to compare on null");
        }
    }

    /**
     * This method has been modified from the original to concert LazyNodeValue to NodeValue.
     *
     * Robin Keskisärkkä
     *
     * @param nv1
     * @param nv2
     * @param sortOrderingCompare
     * @return
     */
    private static int compare(NodeValue nv1, NodeValue nv2, boolean sortOrderingCompare) {
        if(nv1 instanceof LazyNodeValue){
            nv1 = ((LazyNodeValue) nv1).getNodeValue();
        }
        if(nv2 instanceof LazyNodeValue){
            nv2 = ((LazyNodeValue) nv2).getNodeValue();
        }

        if (nv1 == null && nv2 == null) {
            return 0;
        } else if (nv1 == null) {
            return -1;
        } else if (nv2 == null) {
            return 1;
        } else {
            ValueSpaceClassification compType = classifyValueOp(nv1, nv2);
            int cmp;
            switch(compType) {
                case VSPACE_NUM:
                case VSPACE_STRING:
                case VSPACE_BOOLEAN:
                case VSPACE_LANG:
                case VSPACE_NODE:
                case VSPACE_UNKNOWN:
                case VSPACE_SORTKEY:
                case VSPACE_DIFFERENT:
                default:
                    break;
                case VSPACE_DATETIME:
                case VSPACE_DATE:
                case VSPACE_TIME:
                case VSPACE_G_YEAR:
                case VSPACE_G_YEARMONTH:
                case VSPACE_G_MONTH:
                case VSPACE_G_MONTHDAY:
                case VSPACE_G_DAY:
                    cmp = XSDFuncOp.compareDateTime(nv1, nv2);
                    if (cmp != 2) {
                        return cmp;
                    }

                    compType = ValueSpaceClassification.VSPACE_DIFFERENT;
                    break;
                case VSPACE_DURATION:
                    cmp = XSDFuncOp.compareDuration(nv1, nv2);
                    if (cmp == 0) {
                        Duration d1 = nv1.getDuration();
                        Duration d2 = nv2.getDuration();
                        if (XSDFuncOp.isDayTime(d1) && XSDFuncOp.isYearMonth(d2) || XSDFuncOp.isDayTime(d2) && XSDFuncOp.isYearMonth(d1)) {
                            cmp = 2;
                        }
                    }

                    if (cmp != 2) {
                        return cmp;
                    }

                    compType = ValueSpaceClassification.VSPACE_DIFFERENT;
            }

            Node node1;
            Node node2;
            switch(compType) {
                case VSPACE_NUM:
                    return XSDFuncOp.compareNumeric(nv1, nv2);
                case VSPACE_DATETIME:
                case VSPACE_DATE:
                case VSPACE_TIME:
                case VSPACE_G_YEAR:
                case VSPACE_G_YEARMONTH:
                case VSPACE_G_MONTH:
                case VSPACE_G_MONTHDAY:
                case VSPACE_G_DAY:
                case VSPACE_DURATION:
                    throw new ARQInternalErrorException("Still seeing date/dateTime/time/duration compare type");
                case VSPACE_STRING:
                    cmp = XSDFuncOp.compareString(nv1, nv2);
                    if (!sortOrderingCompare) {
                        return cmp;
                    } else if (cmp != 0) {
                        return cmp;
                    } else if (JenaRuntime.isRDF11) {
                        return cmp;
                    } else {
                        String dt1 = nv1.asNode().getLiteralDatatypeURI();
                        String dt2 = nv2.asNode().getLiteralDatatypeURI();
                        if (dt1 == null && dt2 != null) {
                            return -1;
                        } else {
                            if (dt2 == null && dt1 != null) {
                                return 1;
                            }

                            return 0;
                        }
                    }
                case VSPACE_BOOLEAN:
                    return XSDFuncOp.compareBoolean(nv1, nv2);
                case VSPACE_LANG:
                    node1 = nv1.asNode();
                    node2 = nv2.asNode();
                    int x = StrUtils.strCompareIgnoreCase(node1.getLiteralLanguage(), node2.getLiteralLanguage());
                    if (x != 0) {
                        if (!sortOrderingCompare) {
                            raise(new ExprNotComparableException("Can't compare (different languages) " + nv1 + " and " + nv2));
                        }

                        return x;
                    } else {
                        x = StrUtils.strCompare(node1.getLiteralLexicalForm(), node2.getLiteralLexicalForm());
                        if (x != 0) {
                            return x;
                        } else {
                            x = StrUtils.strCompare(node1.getLiteralLanguage(), node2.getLiteralLanguage());
                            if (x == 0 && !NodeFunctions.sameTerm(node1, node2)) {
                                throw new ARQInternalErrorException("Looks like the same (lang tags) but not node equals");
                            }

                            return x;
                        }
                    }
                case VSPACE_NODE:
                    if (sortOrderingCompare) {
                        return NodeUtils.compareRDFTerms(nv1.asNode(), nv2.asNode());
                    }

                    raise(new ExprNotComparableException("Can't compare (nodes) " + nv1 + " and " + nv2));
                    throw new ARQInternalErrorException("NodeValue.raise returned");
                case VSPACE_UNKNOWN:
                    System.err.println("NodeValue VSPACE_UNKNOWN");
                    node1 = nv1.asNode();
                    node2 = nv2.asNode();
                    System.err.println(node1);
                    System.err.println(node2);
                    if (NodeFunctions.sameTerm(node1, node2)) {
                        return 0;
                    } else {
                        if (sortOrderingCompare) {
                            System.err.println("---->" + NodeUtils.compareRDFTerms(node1, node2));
                            return NodeUtils.compareRDFTerms(node1, node2);
                        }
                        System.err.println("Cant ompare :(");
                        raise(new ExprNotComparableException("Can't compare " + nv1 + " and " + nv2));
                        throw new ARQInternalErrorException("NodeValue.raise returned");
                    }
                case VSPACE_SORTKEY:
                    return nv1.getSortKey().compareTo(nv2.getSortKey());
                case VSPACE_DIFFERENT:
                    if (sortOrderingCompare) {
                        return NodeUtils.compareRDFTerms(nv1.asNode(), nv2.asNode());
                    }

                    raise(new ExprNotComparableException("Can't compare (incompatible value spaces)" + nv1 + " and " + nv2));
                    throw new ARQInternalErrorException("NodeValue.raise returned");
                default:
                    throw new ARQInternalErrorException("Compare failure " + nv1 + " and " + nv2);
            }
        }
    }

    public static ValueSpaceClassification classifyValueOp(NodeValue nv1, NodeValue nv2) {
        ValueSpaceClassification c1 = nv1.getValueSpace();
        ValueSpaceClassification c2 = nv2.getValueSpace();
        if (c1 == c2) {
            return c1;
        } else {
            return c1 != ValueSpaceClassification.VSPACE_UNKNOWN && c2 != ValueSpaceClassification.VSPACE_UNKNOWN ? ValueSpaceClassification.VSPACE_DIFFERENT : ValueSpaceClassification.VSPACE_UNKNOWN;
        }
    }

    public ValueSpaceClassification getValueSpace() {
        return classifyValueSpace(this);
    }

    private static ValueSpaceClassification classifyValueSpace(NodeValue nv) {
        if (nv.isNumber()) {
            return ValueSpaceClassification.VSPACE_NUM;
        } else if (nv.isDateTime()) {
            return ValueSpaceClassification.VSPACE_DATETIME;
        } else if (nv.isString()) {
            return ValueSpaceClassification.VSPACE_STRING;
        } else if (nv.isBoolean()) {
            return ValueSpaceClassification.VSPACE_BOOLEAN;
        } else if (!nv.isLiteral()) {
            return ValueSpaceClassification.VSPACE_NODE;
        } else if (!SystemARQ.ValueExtensions) {
            return ValueSpaceClassification.VSPACE_UNKNOWN;
        } else if (nv.isDate()) {
            return ValueSpaceClassification.VSPACE_DATE;
        } else if (nv.isTime()) {
            return ValueSpaceClassification.VSPACE_TIME;
        } else if (nv.isDuration()) {
            return ValueSpaceClassification.VSPACE_DURATION;
        } else if (nv.isGYear()) {
            return ValueSpaceClassification.VSPACE_G_YEAR;
        } else if (nv.isGYearMonth()) {
            return ValueSpaceClassification.VSPACE_G_YEARMONTH;
        } else if (nv.isGMonth()) {
            return ValueSpaceClassification.VSPACE_G_MONTH;
        } else if (nv.isGMonthDay()) {
            return ValueSpaceClassification.VSPACE_G_MONTHDAY;
        } else if (nv.isGDay()) {
            return ValueSpaceClassification.VSPACE_G_DAY;
        } else if (nv.isSortKey()) {
            return ValueSpaceClassification.VSPACE_SORTKEY;
        } else {
            return NodeUtils.hasLang(nv.asNode()) ? ValueSpaceClassification.VSPACE_LANG : ValueSpaceClassification.VSPACE_UNKNOWN;
        }
    }

    public static Node toNode(NodeValue nv) {
        return nv == null ? null : nv.asNode();
    }

    /**
     * This method has been modified from 'final' to allow overriding.
     *
     * Robin Keskisärkkä
     * @return
     */
    public Node asNode() {
        if (this.node == null) {
            this.node = this.makeNode();
        }

        return this.node;
    }

    protected abstract Node makeNode();

    public Node getNode() {
        return this.node;
    }

    public String getDatatypeURI() {
        return this.asNode().getLiteralDatatypeURI();
    }

    public boolean hasNode() {
        return this.node != null;
    }

    public boolean isBoolean() {
        return false;
    }

    public boolean isString() {
        return false;
    }

    public boolean isLangString() {
        return false;
    }

    public boolean isSortKey() {
        return false;
    }

    public boolean isNumber() {
        return false;
    }

    public boolean isInteger() {
        return false;
    }

    public boolean isDecimal() {
        return false;
    }

    public boolean isFloat() {
        return false;
    }

    public boolean isDouble() {
        return false;
    }

    public boolean hasDateTime() {
        return this.isDateTime() || this.isDate() || this.isTime() || this.isGYear() || this.isGYearMonth() || this.isGMonth() || this.isGMonthDay() || this.isGDay();
    }

    public boolean isDateTime() {
        return false;
    }

    public boolean isDate() {
        return false;
    }

    public boolean isLiteral() {
        return this.getNode() == null || this.getNode().isLiteral();
    }

    public boolean isTime() {
        return false;
    }

    public boolean isDuration() {
        return false;
    }

    /** @deprecated */
    @Deprecated
    public boolean isYearMonth() {
        return this.isYearMonthDuration();
    }

    public boolean isYearMonthDuration() {
        if (!this.isDuration()) {
            return false;
        } else {
            Duration dur = this.getDuration();
            return (dur.isSet(DatatypeConstants.YEARS) || dur.isSet(DatatypeConstants.MONTHS)) && !dur.isSet(DatatypeConstants.DAYS) && !dur.isSet(DatatypeConstants.HOURS) && !dur.isSet(DatatypeConstants.MINUTES) && !dur.isSet(DatatypeConstants.SECONDS);
        }
    }

    public boolean isDayTimeDuration() {
        if (!this.isDuration()) {
            return false;
        } else {
            Duration dur = this.getDuration();
            return !dur.isSet(DatatypeConstants.YEARS) && !dur.isSet(DatatypeConstants.MONTHS) && (dur.isSet(DatatypeConstants.DAYS) || dur.isSet(DatatypeConstants.HOURS) || dur.isSet(DatatypeConstants.MINUTES) || dur.isSet(DatatypeConstants.SECONDS));
        }
    }

    public boolean isGYear() {
        return false;
    }

    public boolean isGYearMonth() {
        return false;
    }

    public boolean isGMonth() {
        return false;
    }

    public boolean isGMonthDay() {
        return false;
    }

    public boolean isGDay() {
        return false;
    }

    public boolean getBoolean() {
        raise(new ExprEvalTypeException("Not a boolean: " + this));
        return false;
    }

    public String getString() {
        raise(new ExprEvalTypeException("Not a string: " + this));
        return null;
    }

    public String getLang() {
        raise(new ExprEvalTypeException("Not a string: " + this));
        return null;
    }

    public NodeValueSortKey getSortKey() {
        raise(new ExprEvalTypeException("Not a sort key: " + this));
        return null;
    }

    public BigInteger getInteger() {
        raise(new ExprEvalTypeException("Not an integer: " + this));
        return null;
    }

    public BigDecimal getDecimal() {
        raise(new ExprEvalTypeException("Not a decimal: " + this));
        return null;
    }

    public float getFloat() {
        raise(new ExprEvalTypeException("Not a float: " + this));
        return (float) (0.0F / 0.0);
    }

    public double getDouble() {
        raise(new ExprEvalTypeException("Not a double: " + this));
        return 0.0D / 0.0;
    }

    public XMLGregorianCalendar getDateTime() {
        raise(new ExprEvalTypeException("No DateTime value: " + this));
        return null;
    }

    public Duration getDuration() {
        raise(new ExprEvalTypeException("Not a duration: " + this));
        return null;
    }

    private static NodeValue nodeToNodeValue(Node node) {
        if (node.isVariable()) {
            Log.warn(NodeValue.class, "Variable passed to NodeValue.nodeToNodeValue");
        }

        if (!node.isLiteral()) {
            return new NodeValueNode(node);
        } else {
            boolean hasLangTag = NodeUtils.isLangString(node);
            boolean isPlainLiteral = node.getLiteralDatatypeURI() == null && !hasLangTag;
            if (isPlainLiteral) {
                return new NodeValueString(node.getLiteralLexicalForm(), node);
            } else if (hasLangTag) {
                if (node.getLiteralDatatype() != null && !RDF.dtLangString.equals(node.getLiteralDatatype()) && VerboseWarnings) {
                    Log.warn(NodeValue.class, "Lang tag and datatype (datatype ignored)");
                }

                return new NodeValueLang(node);
            } else {
                LiteralLabel lit = node.getLiteral();
                if (!node.getLiteral().isWellFormed()) {
                    if (VerboseWarnings) {
                        String tmp = FmtUtils.stringForNode(node);
                        Log.warn(NodeValue.class, "Datatype format exception: " + tmp);
                    }

                    return new NodeValueNode(node);
                } else {
                    NodeValue nv = _setByValue(node);
                    return (NodeValue)(nv != null ? nv : new NodeValueNode(node));
                }
            }
        }
    }

    private static NodeValue _setByValue(Node node) {
        if (NodeUtils.hasLang(node)) {
            return new NodeValueLang(node);
        } else {
            LiteralLabel lit = node.getLiteral();
            String lex = lit.getLexicalForm();
            RDFDatatype datatype = lit.getDatatype();
            String datatypeURI = datatype.getURI();
            if (!datatypeURI.startsWith("http://www.w3.org/2001/XMLSchema#") && !SystemARQ.EnableRomanNumerals) {
                return null;
            } else {
                try {
                    if (XSDDatatype.XSDstring.isValidLiteral(lit)) {
                        return new NodeValueString(lit.getLexicalForm(), node);
                    } else if (!datatype.equals(XSDDatatype.XSDdecimal) && XSDDatatype.XSDinteger.isValidLiteral(lit)) {
                        String s = node.getLiteralLexicalForm().trim();
                        if (s.startsWith("+")) {
                            s = s.substring(1);
                        }

                        BigInteger integer = new BigInteger(s);
                        return new NodeValueInteger(integer, node);
                    } else if (datatype.equals(XSDDatatype.XSDdecimal) && XSDDatatype.XSDdecimal.isValidLiteral(lit)) {
                        BigDecimal decimal = new BigDecimal(lit.getLexicalForm());
                        return new NodeValueDecimal(decimal, node);
                    } else if (datatype.equals(XSDDatatype.XSDfloat) && XSDDatatype.XSDfloat.isValidLiteral(lit)) {
                        float f = ((Number)lit.getValue()).floatValue();
                        return new NodeValueFloat(f, node);
                    } else if (datatype.equals(XSDDatatype.XSDdouble) && XSDDatatype.XSDdouble.isValidLiteral(lit)) {
                        double d = ((Number)lit.getValue()).doubleValue();
                        return new NodeValueDouble(d, node);
                    } else {
                        XSDDateTime time;
                        if ((datatype.equals(XSDDatatype.XSDdateTime) || datatype.equals(XSDDatatype.XSDdateTimeStamp)) && XSDDatatype.XSDdateTime.isValid(lex)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else if (datatype.equals(XSDDatatype.XSDdate) && XSDDatatype.XSDdate.isValidLiteral(lit)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else if (datatype.equals(XSDDatatype.XSDtime) && XSDDatatype.XSDtime.isValidLiteral(lit)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else if (datatype.equals(XSDDatatype.XSDgYear) && XSDDatatype.XSDgYear.isValidLiteral(lit)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else if (datatype.equals(XSDDatatype.XSDgYearMonth) && XSDDatatype.XSDgYearMonth.isValidLiteral(lit)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else if (datatype.equals(XSDDatatype.XSDgMonth) && XSDDatatype.XSDgMonth.isValidLiteral(lit)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else if (datatype.equals(XSDDatatype.XSDgMonthDay) && XSDDatatype.XSDgMonthDay.isValidLiteral(lit)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else if (datatype.equals(XSDDatatype.XSDgDay) && XSDDatatype.XSDgDay.isValidLiteral(lit)) {
                            time = (XSDDateTime)lit.getValue();
                            return new NodeValueDT(lex, node);
                        } else {
                            Duration duration;
                            if (datatype.equals(XSDDatatype.XSDduration) && XSDDatatype.XSDduration.isValid(lex)) {
                                duration = xmlDatatypeFactory.newDuration(lex);
                                return new NodeValueDuration(duration, node);
                            } else if (datatype.equals(XSDDatatype.XSDyearMonthDuration) && XSDDatatype.XSDyearMonthDuration.isValid(lex)) {
                                duration = xmlDatatypeFactory.newDuration(lex);
                                return new NodeValueDuration(duration, node);
                            } else if (datatype.equals(XSDDatatype.XSDdayTimeDuration) && XSDDatatype.XSDdayTimeDuration.isValid(lex)) {
                                duration = xmlDatatypeFactory.newDuration(lex);
                                return new NodeValueDuration(duration, node);
                            } else if (datatype.equals(XSDDatatype.XSDboolean) && XSDDatatype.XSDboolean.isValidLiteral(lit)) {
                                boolean b = (Boolean)lit.getValue();
                                return new NodeValueBoolean(b, node);
                            } else if (SystemARQ.EnableRomanNumerals && lit.getDatatypeURI().equals(RomanNumeralDatatype.get().getURI())) {
                                Object obj = RomanNumeralDatatype.get().parse(lit.getLexicalForm());
                                if (obj instanceof Integer) {
                                    return new NodeValueInteger(((Integer)obj).longValue());
                                } else if (obj instanceof RomanNumeral) {
                                    return new NodeValueInteger((long)((RomanNumeral)obj).intValue());
                                } else {
                                    throw new ARQInternalErrorException("DatatypeFormatException: Roman numeral is unknown class");
                                }
                            } else {
                                return null;
                            }
                        }
                    }
                } catch (DatatypeFormatException var7) {
                    throw new ARQInternalErrorException("DatatypeFormatException: " + lit, var7);
                }
            }
        }
    }

    public static void raise(ExprException ex) {
        throw ex;
    }

    public void visit(ExprVisitor visitor) {
        visitor.visit(this);
    }

    private void forceToNode() {
        if (this.node == null) {
            this.node = this.asNode();
        }

        if (this.node == null) {
            raise(new ExprEvalException("Not a node: " + this));
        }

    }

    public final String asUnquotedString() {
        return this.asString();
    }

    public final String asQuotedString() {
        return this.asQuotedString(new SerializationContext());
    }

    public final String asQuotedString(SerializationContext context) {
        if (this.node == null) {
            this.node = this.asNode();
        }

        return this.node != null ? FmtUtils.stringForNode(this.node, context) : this.toString();
    }

    public String asString() {
        this.forceToNode();
        return NodeFunctions.str(this.node);
    }

    public int hashCode() {
        return this.asNode().hashCode();
    }

    public boolean equals(Expr other, boolean bySyntax) {
        if (other == null) {
            return false;
        } else if (this == other) {
            return true;
        } else if (!(other instanceof NodeValue)) {
            return false;
        } else {
            NodeValue nv = (NodeValue)other;
            return this.asNode().equals(nv.asNode());
        }
    }

    public abstract void visit(NodeValueVisitor var1);

    public Expr apply(ExprTransform transform) {
        return transform.transform(this);
    }

    public String toString() {
        return this.asQuotedString();
    }

    static {
        JenaSystem.init();
        log = LoggerFactory.getLogger(NodeValue.class);
        VerboseWarnings = true;
        VerboseExceptions = false;
        IntegerZERO = BigInteger.ZERO;
        DecimalZERO = BigDecimal.ZERO;
        TRUE = makeNode("true", XSDDatatype.XSDboolean);
        FALSE = makeNode("false", XSDDatatype.XSDboolean);
        nvZERO = makeNode(NodeConst.nodeZero);
        nvONE = makeNode(NodeConst.nodeOne);
        nvTEN = makeNode(NodeConst.nodeTen);
        nvNaN = makeNode("NaN", XSDDatatype.XSDdouble);
        nvINF = makeNode("INF", XSDDatatype.XSDdouble);
        nvNegINF = makeNode("-INF", XSDDatatype.XSDdouble);
        nvEmptyString = makeString("");
        nvNothing = makeNode(NodeFactory.createBlankNode("node value nothing"));
        xmlDatatypeFactory = DatatypeFactoryInst.newDatatypeFactory();
    }
}
