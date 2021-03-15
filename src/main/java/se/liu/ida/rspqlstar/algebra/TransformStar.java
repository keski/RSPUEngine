package se.liu.ida.rspqlstar.algebra;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.TransformCopy;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.expr.nodevalue.NodeValueNode;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.op.OpExtendQuad;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

public class TransformStar extends TransformCopy {
    final private Logger logger = Logger.getLogger(RSPQLStarTransform.class);
    final private HashMap<Node_Triple, Var> varMap = new HashMap();
    final private VarDictionary varDict = VarDictionary.get();
    static public boolean opExtendFirst = false;

    /**
     * Quads are wrapped in OpQuadPatterns. For embedded triple pattern, << ?a ?b ?c >> ?e ?f, do:
     * ?a ?b ?c .
     * BIND(<< ?a ?b ?c >> AS ?_t)
     * ?_t ?e ?f
     *
     * @param opQuadPattern
     * @return
     */
    @Override
    public Op transform(final OpQuadPattern opQuadPattern) {
        Op op = OpSequence.create();
        for(Quad quad : opQuadPattern.getPattern()){
            final Op op2 = splitQuadWithEmbeddedTriple(quad);
            op = OpSequence.create(op, op2);
        }
        return op;
    }

    private Op splitQuadWithEmbeddedTriple(final Quad quad) {
        final List<Op> ops = new ArrayList<>();
        final Node subject;
        final Node predicate = quad.getPredicate();
        final Node object;

        // subject
        if (quad.getSubject() instanceof Node_Triple) {
            final Op op = makeExtend((Node_Triple) quad.getSubject(), quad.getGraph());
            if(op != null) ops.add(op);
            subject = varMap.get(quad.getSubject());
        } else {
            subject = quad.getSubject();
        }

        // object
        if (quad.getObject() instanceof Node_Triple) {
            final Op op = makeExtend((Node_Triple) quad.getObject(), quad.getGraph());
            if(op != null) ops.add(op);
            object = varMap.get(quad.getObject());
        } else {
            object = quad.getObject();
        }

        ops.add(new OpQuad(new Quad(quad.getGraph(), subject, predicate, object)));

        // join
        Op op = null;
        for(Op op2 : ops){
            if(op2 == null){
                op = op2;
            } else {
                if(opExtendFirst){
                    op = OpSequence.create(op2, op);
                } else {
                    op = OpSequence.create(op, op2);
                }
            }
        }

        return op;
    }

    public Op makeExtend(Node_Triple node_triple, Node graph){
        if(varMap.containsKey(node_triple)) return null;

        final NodeValueNode exp = new NodeValueNode(node_triple);
        final Var var = varDict.getFreshVariable();
        varMap.put(node_triple, var);
        OpExtend opExtend = (OpExtend) OpExtend.create(OpTable.unit(), var, exp);
        return new OpExtendQuad(opExtend, graph);
    }
}
