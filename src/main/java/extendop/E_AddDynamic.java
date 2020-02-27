package extendop;

import org.apache.jena.sparql.expr.E_Add;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.expr.nodevalue.NodeValueOps ;
import org.apache.jena.sparql.sse.Tags ;
import se.liu.ida.rspqlstar.function.Probability;

public class E_AddDynamic extends E_Add
{
    private static final String functionName = Tags.tagAdd ;
    private static final String symbol = Tags.symPlus ;

    public E_AddDynamic(Expr left, Expr right) {
        super(left, right);
    }

    @Override
    public NodeValue eval(NodeValue x, NodeValue y){
        if(x.getDatatypeURI().equals("http://w3id.org/rsp/rspu#distribution")){
            return addPDF(x, y);
        }
        return NodeValueOps.additionNV(x, y) ;

    }

    public NodeValue addPDF(NodeValue nodeValue1, NodeValue nodeValue2){
        NodeValue value;
        if(nodeValue1.isDouble()){
            value = Probability.add(Probability.getDistribution(nodeValue2), nodeValue1.getDouble());
        } else if(nodeValue2.isDouble()){
            value = Probability.add(Probability.getDistribution(nodeValue1), nodeValue2.getDouble());
        } else {
            value = Probability.add(Probability.getDistribution(nodeValue1), Probability.getDistribution(nodeValue2));
        }
        return value;
    }

    @Override
    public Expr copy(Expr e1, Expr e2) {  return new org.apache.jena.sparql.expr.E_Add(e1 , e2) ; }


}