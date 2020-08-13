package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Triple;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.core.Var;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadPatternBuilder;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;

import static org.junit.Assert.*;

public class DatasetGraphStarTest {
    private DatasetGraphStar ds;
    private NodeDictionary nd;
    private VarDictionary vd;

    @Before
    public void setUp() {
        ds = new DatasetGraphStar();
        nd = NodeDictionaryFactory.get();
        vd = VarDictionary.get();
    }

    @After
    public void tearDown() {
        ds.close();
        nd.clear();
    }

    @Test
    public void contains() {
        final Node x = NodeFactory.createURI("http://x");
        final Quad quad = new Quad(x, x, x, x);
        final IdBasedQuad idBasedQuad1 = new IdBasedQuad(quad);
        final QuadStarPattern quadStarPattern1 = QuadPatternBuilder.createQuadPattern(quad);
        nd.addNodeIfNecessary(x);
        final IdBasedQuad idBasedQuad2 = new IdBasedQuad(quad);
        final QuadStarPattern quadStarPattern2 = QuadPatternBuilder.createQuadPattern(quad);
        assertFalse(ds.contains(quad));
        assertFalse(ds.contains(idBasedQuad1));
        assertFalse(ds.contains(idBasedQuad2));
        assertFalse(ds.contains(quadStarPattern1));
        assertFalse(ds.contains(quadStarPattern2));
        ds.add(quad);
        assertTrue(ds.contains(quad));
        assertFalse(ds.contains(idBasedQuad1));
        assertTrue(ds.contains(idBasedQuad2));
        assertFalse(ds.contains(quadStarPattern1));
        assertTrue(ds.contains(quadStarPattern2));
    }

    @Test
    public void find1() {
        // add
        final Node a = NodeFactory.createURI("http://a");
        final Node b = NodeFactory.createURI("http://b");
        final Node c = NodeFactory.createURI("http://c");
        final Node d = NodeFactory.createURI("http://d");
        // add multiple
        ds.add(new Quad(a, b, c, d));
        ds.add(new Quad(b, c, d, a));
        ds.add(new Quad(c, d, a, b));
        ds.add(new Quad(d, a, b, c));

        // find all, should contain 4 quads
        final Iterator<Quad> iter = ds.find(
                vd.getFreshVariable(),
                vd.getFreshVariable(),
                vd.getFreshVariable(),
                vd.getFreshVariable());

        iter.next();
        iter.next();
        iter.next();
        iter.next();
        assertTrue(!iter.hasNext());
    }

    @Test
    public void find2() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        // add
        final Quad q1 = new Quad(x, y, y, y);
        final Quad q2 = new Quad(y, x, y, y);
        final Quad q3 = new Quad(y, y, x, y);
        final Quad q4 = new Quad(y, y, y, x);
        ds.add(q1);
        ds.add(q2);
        ds.add(q3);
        ds.add(q4);
        // vars
        final Var v1 = vd.getFreshVariable();
        final Var v2 = vd.getFreshVariable();
        final Var v3 = vd.getFreshVariable();
        // find by graph
        final Iterator<Quad> iter1 = ds.find(x, v1, v2, v3);
        assertEquals(iter1.next(), q1);
        assertTrue(!iter1.hasNext());
        // find by subject
        final Iterator<Quad> iter2 = ds.find(v1, x, v2, v3);
        assertEquals(iter2.next(), q2);
        assertTrue(!iter2.hasNext());
        // find by predicate
        final Iterator<Quad> iter3 = ds.find(v1, v2, x, v3);
        assertEquals(iter3.next(), q3);
        assertTrue(!iter3.hasNext());
        // find by object
        final Iterator<Quad> iter4 = ds.find(v1, v2, v3, x);
        assertEquals(iter4.next(), q4);
        assertTrue(!iter4.hasNext());
    }

    @Test
    public void find3() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        // add
        final Quad q1 = new Quad(x, x, y, y);
        final Quad q2 = new Quad(x, y, x, y);
        final Quad q3 = new Quad(x, y, y, x);
        final Quad q4 = new Quad(y, x, x, y);
        final Quad q5 = new Quad(y, x, y, x);
        final Quad q6 = new Quad(y, y, x, x);
        ds.add(q1);
        ds.add(q2);
        ds.add(q3);
        ds.add(q4);
        ds.add(q5);
        ds.add(q6);
        // vars
        final Var v1 = vd.getFreshVariable();
        final Var v2 = vd.getFreshVariable();
        // find by graph: g s ? ?
        final Iterator<Quad> iter1 = ds.find(x, x, v1, v2);
        assertEquals(iter1.next(), q1);
        assertTrue(!iter1.hasNext());
        // find by graph: g ? p ?
        final Iterator<Quad> iter2 = ds.find(x, v1, x, v2);
        assertEquals(iter2.next(), q2);
        assertTrue(!iter2.hasNext());
        // find by graph: g ? ? o
        final Iterator<Quad> iter3 = ds.find(x, v1, v2, x);
        assertEquals(iter3.next(), q3);
        assertTrue(!iter3.hasNext());
        // find by graph: ? s p ?
        final Iterator<Quad> iter4 = ds.find(v1, x, x, v2);
        assertEquals(iter4.next(), q4);
        assertTrue(!iter4.hasNext());
        // find by graph: ? s ? o
        final Iterator<Quad> iter5 = ds.find(v1, x, v2, x);
        assertEquals(iter5.next(), q5);
        assertTrue(!iter5.hasNext());
        // find by graph: ? ? p o
        final Iterator<Quad> iter6 = ds.find(v1, v2, x, x);
        assertEquals(iter6.next(), q6);
        assertTrue(!iter6.hasNext());
    }

    @Test
    public void find4() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        // add
        final Quad q1 = new Quad(x, x, x, y);
        final Quad q2 = new Quad(x, x, y, x);
        final Quad q3 = new Quad(x, y, x, x);
        final Quad q4 = new Quad(y, x, x, x);
        ds.add(q1);
        ds.add(q2);
        ds.add(q3);
        ds.add(q4);
        // vars
        final Var v = vd.getFreshVariable();
        // find by graph: g s p ?
        final Iterator<Quad> iter1 = ds.find(x, x, x, v);
        assertEquals(iter1.next(), q1);
        assertTrue(!iter1.hasNext());
        // find by graph: g s ? o
        final Iterator<Quad> iter2 = ds.find(x, x, v, x);
        assertEquals(iter2.next(), q2);
        assertTrue(!iter2.hasNext());
        // find by graph: g ? p o
        final Iterator<Quad> iter3 = ds.find(x, v, x, x);
        assertEquals(iter3.next(), q3);
        assertTrue(!iter3.hasNext());
        // find by graph: ? s p o
        final Iterator<Quad> iter4 = ds.find(v, x, x, x);
        assertEquals(iter4.next(), q4);
        assertTrue(!iter4.hasNext());
    }

    @Test
    public void add() {
        final Node x = NodeFactory.createURI("http://x");
        final Quad quad = new Quad(x, x, x, x);
        assertFalse(ds.contains(quad));
        ds.add(quad);
        assertTrue(ds.contains(quad));
    }

    @Test
    public void addNestedQuad() {
        final Node g = NodeFactory.createURI("http://g");
        final Node x = NodeFactory.createURI("http://x");
        // create and add nested quad
        final Triple t1 = new Triple(x, x, x);
        final Triple t2 = new Triple(new Node_Triple(t1), x, x);
        final Triple t3 = new Triple(x, x, new Node_Triple(t2));
        final Quad q = new Quad(g, t3);
        assertFalse(ds.contains(q));
        ds.add(q);
        assertEquals(ds.size(), 3);
        assertTrue(ds.contains(new Quad(g, t1)));
        assertTrue(ds.contains(new Quad(g, t2)));
        assertTrue(ds.contains(new Quad(g, t3)));
        assertTrue(ds.contains(q));
    }

    @Test
    public void addToIndex() {
        final Node x = NodeFactory.createURI("http://x");
        nd.addNodeIfNecessary(x);
        final IdBasedQuad idBasedQuad = new IdBasedQuad(new Quad(x, x, x, x));
        assertFalse(ds.contains(idBasedQuad));
        ds.addToIndex(idBasedQuad);
        assertTrue(ds.contains(idBasedQuad));
    }

    @Test
    public void iterateAll() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        final Node z = NodeFactory.createURI("http://z");
        nd.addNodeIfNecessary(x);
        nd.addNodeIfNecessary(y);
        nd.addNodeIfNecessary(z);
        // create and add
        final IdBasedQuad q1 = new IdBasedQuad(new Quad(x, x, x, x));
        final IdBasedQuad q2 = new IdBasedQuad(new Quad(y, y, y, y));
        final IdBasedQuad q3 = new IdBasedQuad(new Quad(z, z, z, z));
        ds.addToIndex(q1);
        ds.addToIndex(q2);
        ds.addToIndex(q3);
        // collect
        final ArrayList<IdBasedQuad> l2 = new ArrayList<>();
        final Iterator<IdBasedQuad> iter = ds.iterateAll();
        l2.add(iter.next());
        l2.add(iter.next());
        l2.add(iter.next());
        assertFalse(iter.hasNext());
        // compare
        final IdBasedQuad[] arr1 = new IdBasedQuad[]{q1, q2, q3};
        Arrays.sort(arr1);
        final Object[] arr2 = l2.toArray();
        Arrays.sort(arr2);
        assertArrayEquals(arr1, arr2);
    }

    @Test
    public void idBasedFind1() {
        // add
        final Node a = NodeFactory.createURI("http://a");
        final Node b = NodeFactory.createURI("http://b");
        final Node c = NodeFactory.createURI("http://c");
        final Node d = NodeFactory.createURI("http://d");
        // add multiple
        ds.add(new Quad(a, b, c, d));
        ds.add(new Quad(b, c, d, a));
        ds.add(new Quad(c, d, a, b));
        ds.add(new Quad(d, a, b, c));

        // find all, should contain 4 quads
        final Iterator<IdBasedQuad> iter = ds.idBasedFind(
                vd.getFreshVariable(),
                vd.getFreshVariable(),
                vd.getFreshVariable(),
                vd.getFreshVariable());
        iter.next();
        iter.next();
        iter.next();
        iter.next();
        assertTrue(!iter.hasNext());
    }

    @Test
    public void idBasedFind2() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        // add
        final Quad q1 = new Quad(x, y, y, y);
        final Quad q2 = new Quad(y, x, y, y);
        final Quad q3 = new Quad(y, y, x, y);
        final Quad q4 = new Quad(y, y, y, x);
        ds.add(q1);
        ds.add(q2);
        ds.add(q3);
        ds.add(q4);
        // vars
        final Var v1 = vd.getFreshVariable();
        final Var v2 = vd.getFreshVariable();
        final Var v3 = vd.getFreshVariable();
        // find by graph
        final Iterator<IdBasedQuad> iter1 = ds.idBasedFind(x, v1, v2, v3);
        assertEquals(new IdBasedQuad(q1), iter1.next());
        assertTrue(!iter1.hasNext());
        // find by subject
        final Iterator<IdBasedQuad> iter2 = ds.idBasedFind(v1, x, v2, v3);
        assertEquals(new IdBasedQuad(q2), iter2.next());
        assertTrue(!iter2.hasNext());
        // find by predicate
        final Iterator<IdBasedQuad> iter3 = ds.idBasedFind(v1, v2, x, v3);
        assertEquals(new IdBasedQuad(q3), iter3.next());
        assertTrue(!iter3.hasNext());
        // find by object
        final Iterator<IdBasedQuad> iter4 = ds.idBasedFind(v1, v2, v3, x);
        assertEquals(new IdBasedQuad(q4), iter4.next());
        assertTrue(!iter4.hasNext());
    }

    @Test
    public void idBasedFind3() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        // add
        final Quad q1 = new Quad(x, x, y, y);
        final Quad q2 = new Quad(x, y, x, y);
        final Quad q3 = new Quad(x, y, y, x);
        final Quad q4 = new Quad(y, x, x, y);
        final Quad q5 = new Quad(y, x, y, x);
        final Quad q6 = new Quad(y, y, x, x);
        ds.add(q1);
        ds.add(q2);
        ds.add(q3);
        ds.add(q4);
        ds.add(q5);
        ds.add(q6);
        // vars
        final Var v1 = vd.getFreshVariable();
        final Var v2 = vd.getFreshVariable();
        // find by graph: g s ? ?
        final Iterator<IdBasedQuad> iter1 = ds.idBasedFind(x, x, v1, v2);
        assertEquals(new IdBasedQuad(q1), iter1.next());
        assertTrue(!iter1.hasNext());
        // find by graph: g ? p ?
        final Iterator<IdBasedQuad> iter2 = ds.idBasedFind(x, v1, x, v2);
        assertEquals(new IdBasedQuad(q2), iter2.next());
        assertTrue(!iter2.hasNext());
        // find by graph: g ? ? o
        final Iterator<IdBasedQuad> iter3 = ds.idBasedFind(x, v1, v2, x);
        assertEquals(new IdBasedQuad(q3), iter3.next());
        assertTrue(!iter3.hasNext());
        // find by graph: ? s p ?
        final Iterator<IdBasedQuad> iter4 = ds.idBasedFind(v1, x, x, v2);
        assertEquals(new IdBasedQuad(q4), iter4.next());
        assertTrue(!iter4.hasNext());
        // find by graph: ? s ? o
        final Iterator<IdBasedQuad> iter5 = ds.idBasedFind(v1, x, v2, x);
        assertEquals(new IdBasedQuad(q5), iter5.next());
        assertTrue(!iter5.hasNext());
        // find by graph: ? ? p o
        final Iterator<IdBasedQuad> iter6 = ds.idBasedFind(v1, v2, x, x);
        assertEquals(new IdBasedQuad(q6), iter6.next());
        assertTrue(!iter6.hasNext());
    }

    @Test
    public void idBasedFind4() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        // add
        final Quad q1 = new Quad(x, x, x, y);
        final Quad q2 = new Quad(x, x, y, x);
        final Quad q3 = new Quad(x, y, x, x);
        final Quad q4 = new Quad(y, x, x, x);
        ds.add(q1);
        ds.add(q2);
        ds.add(q3);
        ds.add(q4);
        // vars
        final Var v = vd.getFreshVariable();
        // find by graph: g s p ?
        final Iterator<IdBasedQuad> iter1 = ds.idBasedFind(x, x, x, v);
        assertEquals(new IdBasedQuad(q1), iter1.next());
        assertTrue(!iter1.hasNext());
        // find by graph: g s ? o
        final Iterator<IdBasedQuad> iter2 = ds.idBasedFind(x, x, v, x);
        assertEquals(new IdBasedQuad(q2), iter2.next());
        assertTrue(!iter2.hasNext());
        // find by graph: g ? p o
        final Iterator<IdBasedQuad> iter3 = ds.idBasedFind(x, v, x, x);
        assertEquals(new IdBasedQuad(q3), iter3.next());
        assertTrue(!iter3.hasNext());
        // find by graph: ? s p o
        final Iterator<IdBasedQuad> iter4 = ds.idBasedFind(v, x, x, x);
        assertEquals(new IdBasedQuad(q4), iter4.next());
        assertTrue(!iter4.hasNext());
    }

    @Test
    public void size() {
        final Node x = NodeFactory.createURI("http://x");
        final Node y = NodeFactory.createURI("http://y");
        final Node z = NodeFactory.createURI("http://z");
        final Quad q1 = new Quad(x, x, x, x);
        final Quad q2 = new Quad(y, y, y, y);
        final Quad q3 = new Quad(z, z, z, z);
        ds.add(q1);
        ds.add(q2);
        ds.add(q3);
        ds.add(q3);
        assertEquals(ds.size(), 3);
    }
}