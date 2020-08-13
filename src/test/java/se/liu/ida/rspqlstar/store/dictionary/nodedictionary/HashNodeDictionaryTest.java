package se.liu.ida.rspqlstar.store.dictionary.nodedictionary;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.dictionary.IdFactory;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.idnodes.NodeWithIDFactory;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

public class HashNodeDictionaryTest {
    private HashNodeDictionary nd;

    @Before
    public void setUp() {
        nd = new HashNodeDictionary();
    }

    @After
    public void tearDown() {
        nd.clear();
        IdFactory.reset();
    }

    @Test
    public void getNode1() {
        assertEquals(null, nd.getNode(1));
    }

    @Test
    public void getNode2() {
        final Node n = NodeFactory.createBlankNode();
        final long id = nd.addNodeIfNecessary(n);
        assertEquals(nd.getNode(id), n); // Note: direction of equality check matters
    }

    @Test
    public void getNode3() {
        final Node n = NodeFactory.createBlankNode();
        final long id = nd.addNodeIfNecessary(n);
        assertEquals(nd.getNode(id), n); // Note: direction of equality check matters
    }

    @Test
    public void getId1() {
        final Node n = NodeFactory.createBlankNode();
        assertEquals(-1, nd.getId(n));
    }

    @Test
    public void getId2() {
        final Node n = NodeWithIDFactory.createNode(NodeFactory.createBlankNode(), 1111);
        assertEquals(1111, nd.getId(n));
    }

    @Test
    public void getId3() {
        final Node n = NodeFactory.createBlankNode();
        final long id = nd.addNodeIfNecessary(n);
        assertEquals(id, nd.getId(n));
    }

    @Test
    public void getId4() {
        final Node n = NodeFactory.createBlankNode();
        final long id = nd.addNodeIfNecessary(n);
        assertEquals(id, nd.getId(n));
    }

    @Test
    public void clear() {
        for(int i=0; i < 1000; i++) {
            nd.addNodeIfNecessary(NodeFactory.createBlankNode());
        }
        assertEquals(1000, nd.size());
        assertEquals(1000, IdFactory.getNodeId());
        nd.clear();
        IdFactory.reset();
        assertEquals(0, nd.size());
        assertEquals(0, IdFactory.getNodeId());
    }

    @Test
    public void stressTest1() throws InterruptedException {
        final int numThreads = 10;
        final int nodesPerThread = 100000;
        final CountDownLatch latch = new CountDownLatch(numThreads);

        final ArrayList<Thread> threads = new ArrayList<>();
        for(int i=0; i < numThreads; i++) {
                final Thread t = new Thread(() -> {
                    final Random r = new Random();
                    TimeUtil.silentSleep(r.nextInt() % 1000); // sleep between 0-1000 ms
                    for (int j = 0; j < nodesPerThread; j++) {
                        nd.addNodeIfNecessary(NodeFactory.createBlankNode());
                    }
                    latch.countDown();
                });
                t.start();
                threads.add(t);
        }
        latch.await();
        assertEquals(numThreads * nodesPerThread, nd.size());
    }

    @Test
    public void stressTest2() throws InterruptedException {
        final int numThreads = 10;
        final int nodesPerThread = 5000;
        final CountDownLatch latch = new CountDownLatch(numThreads);

        final ArrayList<Thread> threads = new ArrayList<>();
        for(int i=0; i < numThreads; i++) {
            final Thread t = new Thread(() -> {
                final Random r = new Random();
                TimeUtil.silentSleep(r.nextInt() % 1000); // sleep between 0-1000 ms
                for (int j = 0; j < nodesPerThread; j++) {
                    nd.addNodeIfNecessary(NodeFactory.createBlankNode(Integer.toString(j)));
                    TimeUtil.silentSleep(r.nextInt() % 10); // sleep between 0-10 ms
                }
                latch.countDown();
            });
            t.start();
            threads.add(t);
        }
        latch.await();
        assertEquals(nodesPerThread, nd.size());
    }
}