package expopt;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.algebra.op.OpJoin;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebra;
import se.liu.ida.rspqlstar.algebra.RSPQLStarAlgebraGenerator;
import se.liu.ida.rspqlstar.lang.RSPQLStar;

import java.util.List;

public class Main {
    public static void main(String[] args){
        Query q = QueryFactory.create("" +
                "BASE <file://base/> " +
                "REGISTER STREAM <out> COMPUTED EVERY PT10S AS " +
                "SELECT ?s " +
                "WHERE { " +
                "   WINDOW <w> {" +
                "       GRAPH ?g1 { " +
                "           ?s1 ?p <o> ." +
                "           ?s2 ?p <o> ." +
                //"           FILTER(?s1 < 10) " +
                "       } " +
                "       ?g1 <time> ?t1 ." +
                "       GRAPH ?g2 { " +
                "           ?s2 <p> <o> ." +
                "           FILTER(?s2 < 10) " +
                "       } " +
                "       ?g2 <time> ?t2 ." +
                "   } " +
                "   WINDOW <w> {" +
                "       GRAPH ?g1 { " +
                "           ?s1 ?p <o> ." +
                "           ?s2 ?p <o> ." +
                //"           FILTER(?s1 < 10) " +
                "       } " +
                "       ?g1 <time> ?t1 ." +
                "       GRAPH ?g2 { " +
                "           ?s2 <p> <o> ." +
                "           FILTER(?s2 < 10) " +
                "       } " +
                "       ?g2 <time> ?t2 ." +
                "   } " +
                "   GRAPH <gx> {" +
                "      <a> <b> <c> ." +
                "   }" +
                "   FILTER(?t1 < ?t2)" +
                ""+
                "}", RSPQLStar.syntax);


        RSPQLStarAlgebraGenerator.timePredicate = NodeFactory.createURI("file://base/time");
        Op op = RSPQLStarAlgebra.compile(q);



        System.out.println(op);
        //final Op opTransform = Transformer.transform(new MyTransform(NodeFactory.createURI("f:/time")), op);






        if(false) {
            if (op instanceof OpJoin) {
                System.out.println("OpJoin");
                Op left = ((OpJoin) op).getLeft();
                Op right = ((OpJoin) op).getRight();
                System.out.println("L: " + left);
                System.out.println("R: " + right);
            } else if (op instanceof OpBGP) {
                System.out.println("OpBGP");
                List<Triple> list = ((OpBGP) op).getPattern().getList();
                for (Triple t : list) {
                    System.out.println(t);
                }
            }
        }

    }
}
