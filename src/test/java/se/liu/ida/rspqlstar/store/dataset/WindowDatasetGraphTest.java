package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.sparql.core.Quad;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;

import java.time.Duration;
import java.util.Iterator;

import static org.junit.Assert.*;

public class WindowDatasetGraphTest {
    private NodeDictionary nd;
    private WindowDatasetGraph window;
    private RDFStarStream stream;

    @Before
    public void setUp() {
        nd = NodeDictionaryFactory.get();
    }

    @After
    public void tearDown() {
        nd.clear();
    }

    @Test
    public void getBounds() {
        stream = new RDFStarStream("http://s");
        window = new WindowDatasetGraph("http://w", Duration.parse("PT1S"), Duration.parse("PT1S"), 1000, stream);
        assertEquals(window.getLowerBound(12345), 11000);
        assertEquals(window.getUpperBound(12345), 12000);
    }

    @Test
    public void iterate(){
        stream = new RDFStarStream("http://s");
        window = new WindowDatasetGraph("http://w", Duration.parse("PT1S"), Duration.parse("PT1S"), 0, stream);
        final Node s = NodeFactory.createURI("http://s");
        final Node p = NodeFactory.createURI("http://p");
        final Node o = NodeFactory.createURI("http://o");
        for(int i=0; i < 15; i++){
            final Node g = NodeFactory.createURI("http://g" + i);
            final RDFStarStreamElement tg = new RDFStarStreamElement();
            tg.setTime(i*93); // odd number, expect 11 in window
            tg.quad(new Quad(g, s ,p ,o));
            stream.push(tg);
        }

        final DatasetGraphStar ds = window.getDataset(1000);
        assertEquals(11, ds.size());
        for(int i=0; i < 11; i++){
            final Node g = NodeFactory.createURI("http://g" + i);
            assertTrue(ds.contains(g, s, p, o));
        }
    }
}