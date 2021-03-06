package se.liu.ida.rspqlstar.store.index;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.Node_Placeholder;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.iterator.FilteredQuadIterator;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;

import java.util.Collections;
import java.util.Iterator;

public class QuadIndex implements StreamRDF {
    final public Index GSPO;
    final public Index GPOS;
    final public Index GOSP;
    final public Index SPOG;
    final public Index POSG;
    final public Index OSPG;

    public static long DUMMY_KEY_ID;
    public static Node DUMMY_KEY_NODE;
    public static long DEFAULT_GRAPH_ID;
    public static Node DEFAULT_GRAPH_NODE;

    final private NodeDictionary nd = NodeDictionaryFactory.get();
    final private ReferenceDictionary refT = ReferenceDictionaryFactory.get();

    public QuadIndex() {
        // Reserved IDs with special meaning
        DUMMY_KEY_ID = IdFactory.nextNodeId();
        DUMMY_KEY_NODE = new Node_Placeholder("DUMMY_KEY_NODE", DUMMY_KEY_ID);
        DEFAULT_GRAPH_ID = IdFactory.nextNodeId();
        DEFAULT_GRAPH_NODE = new Node_Placeholder("DEFAULT_GRAPH_NODE", DEFAULT_GRAPH_ID);
        nd.addNode(DUMMY_KEY_NODE, DUMMY_KEY_ID);
        nd.addNode(DEFAULT_GRAPH_NODE, DEFAULT_GRAPH_ID);

        GSPO = new TreeIndex(Field.G, Field.S, Field.P, Field.O);
        GPOS = new TreeIndex(Field.G, Field.P, Field.O, Field.S);
        GOSP = new TreeIndex(Field.G, Field.O, Field.S, Field.P);
        SPOG = new TreeIndex(Field.G, Field.S, Field.P, Field.O);
        POSG = new TreeIndex(Field.G, Field.P, Field.O, Field.S);
        OSPG = new TreeIndex(Field.G, Field.O, Field.S, Field.P);
    }

    /**
     * Add a quad to the indexes.
     */
    protected void add(IdBasedQuad quad) {
        GSPO.add(quad);
        GPOS.add(quad);
        GOSP.add(quad);
    }

    @Override
    public void quad(Quad quad) {
        add(quad);
    }

    public IdBasedQuad add(Quad quad) {
        final Node G = quad.getGraph();
        final Node S = quad.getSubject();
        final Node P = quad.getPredicate();
        final Node O = quad.getObject();
        final long g, s, p, o;
        g = G == null ? DEFAULT_GRAPH_ID: nd.addNodeIfNecessary(G);

        if (S instanceof Node_Triple) {
            IdBasedQuad q = add(new Quad(G, ((Node_Triple) S).get()));
            s = getIdForIDBasedTriple(q.getIdBasedTriple());
        } else {
            s = nd.addNodeIfNecessary(S);
        }
        p = nd.addNodeIfNecessary(P);
        if (O instanceof Node_Triple) {
            IdBasedQuad q = add(new Quad(G, ((Node_Triple) O).get()));
            o = getIdForIDBasedTriple(q.getIdBasedTriple());
        } else {
            o = nd.addNodeIfNecessary(O);
        }

        IdBasedQuad idBasedTriple = new IdBasedQuad(g, s, p, o);
        add(idBasedTriple);
        return idBasedTriple;
    }

    @Override
    public void triple(Triple triple) {
        quad(new Quad(null, triple));
    }

    protected Long getIdForIDBasedTriple(IdBasedTriple q) {
        return refT.addIfNecessary(q);
    }

    public long size() {
        return GSPO.size();
    }

    public boolean contains(QuadStarPattern t) {
        return GSPO.contains(t);
    }

    /**
     * Answer an ExtendedIterator returning all the quads from this store that
     * matches the pattern. Note that the default graph is coded as 0.
     *
     * GSPO answers quad patterns: GSP*, GS**, G***, ****
     * GOSP answers quad patterns: GOS*, GO**, G***, ****
     * GPOS answers quad patterns: GPO*, GP**, G***, ****
     */
    public Iterator<IdBasedQuad> find(QuadStarPattern quadPattern) {
        if (!quadPattern.isMatchable()) {
            return Collections.<IdBasedQuad>emptyList().iterator();
        }

        final Iterator<IdBasedQuad> iter;
        if (quadPattern.subject.isConcrete() && quadPattern.predicate.isConcrete()) {
            iter = GSPO.iterator(quadPattern);
        } else if (quadPattern.object.isConcrete() && quadPattern.subject.isConcrete()) {
            iter = GOSP.iterator(quadPattern);
        } else if (quadPattern.predicate.isConcrete() && quadPattern.object.isConcrete()) {
            iter = GPOS.iterator(quadPattern);
        } else if (quadPattern.subject.isConcrete()) {
            iter = GSPO.iterator(quadPattern);
        } else if (quadPattern.object.isConcrete()) {
            iter = GOSP.iterator(quadPattern);
        }else if (quadPattern.predicate.isConcrete()) {
            iter = GPOS.iterator(quadPattern);
        } else {
            iter = GSPO.iterateAll();
        }

        return new FilteredQuadIterator(iter, quadPattern);
    }

    @Override
    public void start() { }

    @Override
    public void finish() { }

    @Override
    public void base(String s) { }

    @Override
    public void prefix(String s, String s1) { }


}
