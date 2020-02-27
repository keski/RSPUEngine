package se.liu.ida.rspqlstar.store.engine.main;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.*;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.ExecutionContext;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.iterator.QueryIterAssign;
import org.apache.jena.sparql.engine.iterator.QueryIterProject;
import org.apache.jena.sparql.engine.iterator.QueryIterProjectMerge;
import org.apache.jena.sparql.engine.iterator.QueryIterRoot;
import org.apache.jena.sparql.engine.main.OpExecutor;
import org.apache.jena.sparql.engine.main.OpExecutorFactory;
import org.apache.jena.sparql.expr.Expr;
import org.apache.jena.sparql.expr.ExprFunction;
import org.apache.jena.sparql.expr.ExprVar;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.algebra.op.OpExtendQuad;
import se.liu.ida.rspqlstar.algebra.op.OpWindow;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.iterator.*;
import se.liu.ida.rspqlstar.store.engine.main.pattern.*;

import java.util.Iterator;

public class OpRSPQLStarExecutor extends OpExecutor {
    final private Logger logger = Logger.getLogger(OpRSPQLStarExecutor.class);
    static final public OpExecutorFactory factory = execCxt -> new OpRSPQLStarExecutor(execCxt);
    final private VarDictionary varDict = VarDictionary.get();
    final private NodeDictionary nd = NodeDictionaryFactory.get();

    /**
     * Make sure QueryIterRoot closes if query result is empty.
     * @param op
     * @param input
     * @return
     */
    protected QueryIterator exec(final Op op, final QueryIterator input) {
        final QueryIterator iter = super.exec(op, input);
        return iter;
    }

    /**
     * Creates an operator compiler.
     */
    public OpRSPQLStarExecutor(final ExecutionContext execCxt) {
        super(execCxt);
    }

    /**
     * Map to ID based OpJoin.
     * @param opJoin
     * @param input
     * @return
     */
    @Override
    protected QueryIterator execute(final OpJoin opJoin, final QueryIterator input) {
        final Iterator<SolutionMapping> iter = new EncodeBindingsIterator(input, execCxt);
        return new DecodeBindingsIterator(execute(opJoin, iter), execCxt);
    }

    /**
     * Map to ID based OpSequence.
     * @param opSequence
     * @param input
     * @return
     */
    @Override
    protected QueryIterator execute(final OpSequence opSequence, final QueryIterator input) {
        final Iterator<SolutionMapping> iter = new EncodeBindingsIterator(input, execCxt);
        return new DecodeBindingsIterator(execute(opSequence, iter), execCxt);
    }

    /**
     * Map to ID based OpQuad.
     * @param opQuad
     * @param input
     * @return
     */
    @Override
    protected QueryIterator execute(final OpQuad opQuad, final QueryIterator input) {
        final Iterator<SolutionMapping> iter = new EncodeBindingsIterator(input, execCxt);
        return new DecodeBindingsIterator(execute(opQuad, iter), execCxt);
    }

    /**
     * Map OpQuadPattern to ID based OpQuadPattern.
     * @param opQuadPattern
     * @param input
     * @return
     */
    @Override
    protected QueryIterator execute(final OpQuadPattern opQuadPattern, final QueryIterator input) {
        final Iterator<SolutionMapping> iter = new EncodeBindingsIterator(input, execCxt);
        return new DecodeBindingsIterator(execute(opQuadPattern, iter), execCxt);
    }

    /**
     * ID based OpJoin.
     *
     * @param opJoin
     * @param input
     * @return
     */
    protected Iterator<SolutionMapping> execute(final OpJoin opJoin, final Iterator<SolutionMapping> input){
        final Op opLeft = opJoin.getLeft();
        final Op opRight = opJoin.getRight();
        final Iterator<SolutionMapping> iterator = executeIdBasedOp(opRight, input);
        return executeIdBasedOp(opLeft, iterator);
    }

    /**
     * ID based OpQuad.
     *
     * @param opQuad
     * @param input
     * @return
     */
    protected Iterator<SolutionMapping> execute(final OpQuad opQuad, final Iterator<SolutionMapping> input){
        return new IdBasedQuadPatternIterator(encode(opQuad), input, execCxt);
    }

    /**
     * ID based OpSequence.
     *
     * @param opSequence
     * @param input
     * @return
     */
    private Iterator<SolutionMapping> execute(final OpSequence opSequence, final Iterator<SolutionMapping> input) {
        Iterator<SolutionMapping> iterator = input;
        for (Op op : opSequence.getElements()) {
            iterator = executeIdBasedOp(op, iterator);
        }
        return iterator;
    }

    /**
     * ID based OpQuadPattern.
     *
     * @param opQuadPattern
     * @param input
     * @return
     */
    private Iterator<SolutionMapping> execute(final OpQuadPattern opQuadPattern, final Iterator<SolutionMapping> input) {
        Iterator<SolutionMapping> iterator = input;
        for (Quad quad : opQuadPattern.getPattern().getList()) {
            iterator = new IdBasedQuadPatternIterator(encode(quad), input, execCxt);
        }
        return iterator;
    }

    /**
     * OpTable is an empty BGP.
     * @param opTable
     * @param input
     * @return
     */
    protected Iterator<SolutionMapping> execute(final OpTable opTable, Iterator<SolutionMapping> input) {
        if (opTable.isJoinIdentity()) {
            return input;
        }
        throw new IllegalStateException();
    }

    /**
     * ID based OpFilter.
     *
     * @param opFilter
     * @param input
     * @return
     */
    protected Iterator<SolutionMapping> execute(OpFilter opFilter, Iterator<SolutionMapping> input){
        final QueryIterator iter = new DecodeBindingsIterator(input, execCxt);
        return new EncodeBindingsIterator(super.execute(opFilter, iter), execCxt);
    }

    private Iterator<SolutionMapping> execute(OpExtend opExtend, Iterator<SolutionMapping> input) {
        Iterator<SolutionMapping> iter1 = executeIdBasedOp(opExtend.getSubOp(), input);

        Iterator<SolutionMapping> iter;

        // regular extend
        final Expr expr = opExtend.getVarExprList().getExprs().values().iterator().next();
        final Var var = opExtend.getVarExprList().getVars().get(0);
        final Key key;

        if(expr instanceof ExprVar){
            int var2 = encode(expr.getExprVar().asVar()); // should now already have been assigned a value
            iter = new IdBasedVarToVarExtendIterator(encode(var), var2, iter1, execCxt);
        } else if(expr instanceof ExprFunction) {
            encode(var); // add the var to varDict
            QueryIterator qIter = new DecodeBindingsIterator(iter1, execCxt);
            QueryIterator qIter2 = new QueryIterAssign(qIter, opExtend.getVarExprList(), this.execCxt, true);
            iter = new EncodeBindingsIterator(qIter2, execCxt);
        } else {
            final Node node = ((NodeValue) expr).asNode();
            final Long id = encode(node);
            // use NodeWrapperKey if the value cannot be encoded
            key = id != null ? new Key(id) : new NodeWrapperKey(node);
            iter = new IdBasedExtendIterator(encode(var), key, iter1, execCxt);
        }
        return iter;
    }

    private Iterator<SolutionMapping> execute(final OpExtendQuad opExtendQuad, final Iterator<SolutionMapping> input) {
        final OpExtend opExtend = opExtendQuad.getSubOp();
        final Expr expr = opExtend.getVarExprList().getExprs().values().iterator().next();

        final Node node = ((NodeValue) expr).asNode();
        final Var var = opExtend.getVarExprList().getVars().get(0);
        final Iterator<SolutionMapping> iter;
        if (node instanceof Node_Triple) {
            // Get the graph context from the original opExtendQuad
            final Element g = encodeAsElement(opExtendQuad.getGraph());
            // Embedded triple extend
            final Triple t = ((Node_Triple) node).get();
            final Element s = encodeAsElement(t.getSubject());
            final Element p = encodeAsElement(t.getPredicate());
            final Element o = encodeAsElement(t.getObject());
            final QuadStarPattern pattern = new QuadStarPattern(g, s, p, o);
            iter = new IdBasedEmbeddedTriplePatternExtend(encode(var), pattern, input, execCxt);
        } else {
            throw new IllegalStateException("OpExtendQuad does not bind to a Node_Triple");
        }
        return iter;
    }

    private Iterator<SolutionMapping> execute(final OpWindow opWindow, final Iterator<SolutionMapping> input) {
        final StreamingDatasetGraph sdg = (StreamingDatasetGraph) execCxt.getDataset();
        sdg.useWindowDataset(opWindow.getNode().toString());
        final Iterator<SolutionMapping> iter = executeIdBasedOp(opWindow.getSubOp(), input);
        sdg.useBaseDataset();
        return iter;
    }

    public Iterator<SolutionMapping> executeIdBasedOp(final Op op, final Iterator<SolutionMapping> input){
        final Iterator<SolutionMapping> iterator;
        if (op instanceof OpQuad) {
            iterator = execute((OpQuad) op, input);
        } else if (op instanceof OpQuadPattern) {
            iterator = execute((OpQuadPattern) op, input);
        } else if (op instanceof OpJoin) {
            iterator = execute((OpJoin) op, input);
        } else if (op instanceof OpSequence) {
            iterator = execute((OpSequence) op, input);
        } else if (op instanceof OpExtend) {
            // opExtend should be treated similarly to a join
            iterator = execute((OpExtend) op, input);
        } else if (op instanceof OpExtendQuad) {
            iterator = execute((OpExtendQuad) op, input);
        } else if (op instanceof OpFilter) {
            iterator = execute((OpFilter) op, input);
        } else if(op instanceof OpTable) {
            iterator = execute((OpTable) op, input);
        } else if(op instanceof OpWindow) {
            iterator = execute((OpWindow) op, input);
        }
        //else if(op instanceof OpUnion) {
        //   // TODO: Fix union. Currently does not work as expected.
        //    OpUnion opUnion = (OpUnion) op;
        //    iterator = new IteratorChain<>(executeIdBasedOp(opUnion.getLeft(), input), executeIdBasedOp(opUnion.getRight(), input));
        //
        else {
            System.err.println("There is no id-based iterator implemented for " + op + ", defaulting to decode/encode");
            logger.info("There is no id-based iterator implemented for " + op + ", defaulting to decode/encode");
            QueryIterator iter = exec(op, new DecodeBindingsIterator(input, execCxt));
            return new EncodeBindingsIterator(iter, execCxt);
        }
        return iterator;
    }

    /**
     * Encode OpQuad as QuadStarPattern
     * @param opQuad
     * @return
     */
    private QuadStarPattern encode(final OpQuad opQuad) {
        final Quad quad = opQuad.getQuad();
        return encode(quad);
    }

    /**
     * Encode quad as QuadStarPattern
     * @param quad
     * @return
     */
    private QuadStarPattern encode(final Quad quad) {
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        builder.setGraph(quad.getGraph());
        builder.setSubject(quad.getSubject());
        builder.setPredicate(quad.getPredicate());
        builder.setObject(quad.getObject());
        return builder.createQuadPattern();
    }

    /**
     * Encode Node_Triple as QuadStarPattern.
     *
     * @param node_triple
     * @return
     */
    private QuadStarPattern encode(final Node graph, final Node_Triple node_triple) {
        final QuadPatternBuilder builder = new QuadPatternBuilder();
        final Triple t = node_triple.get();
        builder.setGraph(graph);
        builder.setSubject(t.getSubject());
        builder.setPredicate(t.getPredicate());
        builder.setObject(t.getObject());

        return builder.createQuadPattern();
    }

    /**
     * Encode Node or Variable as key.
     *
     * @param node
     * @return
     */
    private Long encode(final Node node) {
        return nd.getId(node);
    }

    /**
     * Encode Var as integer.
     *
     * @param var
     * @return
     */
    private int encode(final Var var) {
        return varDict.createId(var);
    }

    private Element encodeAsElement(Node node){
        final Element el;
        if(node.isVariable()){
            final int varId = encode((Var) node);
            el = new Variable(varId);
        } else {
            final Long id = encode(node);
            if(id == null) {
                el = new NodeWrapperKey(node);
            } else {
                el = new Key(id);
            }
        }
        return el;
    }

    protected QueryIterator execute(OpProject opProject, QueryIterator input) {
        for(Var var : opProject.getVars()){
            encode(var);
        }
        return super.execute(opProject, input);
    }
}
