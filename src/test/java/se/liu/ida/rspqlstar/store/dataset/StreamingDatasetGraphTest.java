package se.liu.ida.rspqlstar.store.dataset;

import org.apache.jena.query.QueryFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dictionary.VarDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.RSPQLStarEngine;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class StreamingDatasetGraphTest {
    private StreamingDatasetGraph sdg;
    private NodeDictionary nd;
    private VarDictionary vd;

    @Before
    public void setUp() {
        RSPQLStarEngine.register();
        sdg = new StreamingDatasetGraph(0);
        nd = NodeDictionaryFactory.get();
        vd = VarDictionary.get();
    }

    @After
    public void tearDown() {
        sdg.close();
        nd.clear();
    }

    @Test
    public void registerStream1() {
        final String validUri = "http://valid/stream/uri";
        final RDFStarStream s1 = new RDFStarStream(validUri);
        sdg.registerStream(s1);
        final RDFStarStream s2 = sdg.getStream(validUri);
        assertEquals(s1, s2);
    }

    @Test(expected = IllegalStateException.class)
    public void registerStream2() {
        final String validUri = "http:// invalid/stream/uri";
        new RDFStarStream(validUri);
    }

    @Test(expected = IllegalStateException.class)
    public void registerStream3() {
        final String validUri = "http://valid/stream/uri";
        final RDFStarStream s1 = new RDFStarStream(validUri);
        final RDFStarStream s2 = new RDFStarStream(validUri);
        sdg.registerStream(s1);
        sdg.registerStream(s2);
    }

    @Test
    public void initForQuery1() {
        final String qString = "" +
                "REGISTER STREAM <s> COMPUTED EVERY PT1S AS " +
                "SELECT * " +
                "FROM NAMED WINDOW <http://w> ON <http://s> [RANGE PT1S STEP PT1S] " +
                "WHERE {}";
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);
        sdg.initForQuery(query);
        assertEquals(sdg.getWindow("http://x"), null);
        assertNotEquals(sdg.getWindow("http://w"), null);
        assertEquals(sdg.getStream("http://x"), null);
        assertNotEquals(sdg.getStream("http://s"), null);
    }

    @Test(expected = IllegalStateException.class)
    public void initForQuery2() {
        final String qString = "" +
                "REGISTER STREAM <s> COMPUTED EVERY PT1S AS " +
                "SELECT * " +
                "FROM NAMED WINDOW <http://w> ON <http://s> [RANGE PT1S STEP PT1S] " +
                "WHERE {}";
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);
        sdg.initForQuery(query);
        sdg.initForQuery(query);
    }

    @Test
    public void initForQuery3() {
        final String qString = "" +
                "REGISTER STREAM <s> COMPUTED EVERY PT1S AS " +
                "SELECT * " +
                "FROM NAMED WINDOW <http://w> ON <http://s> [RANGE PT1S STEP PT1S] " +
                "WHERE {}";
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(qString, RSPQLStar.syntax);
        sdg.initForQuery(query);
        final WindowDatasetGraph w = sdg.getWindow("http://w");
        assertEquals(w.getWidth(), 1000);
        assertEquals(w.getStep(), 1000);
    }

    @Test
    public void registerWindow1() {
        final String validWindowUri = "http://window";
        final String streamUri = "http://stream";
        final Duration duration = Duration.parse("PT1S");
        final long time = 0;
        // register
        final RDFStarStream rdfStarStream = new RDFStarStream(streamUri);
        final WindowDatasetGraph window = new WindowDatasetGraph(validWindowUri, duration, duration, time, rdfStarStream);
        assertNotEquals(window, null);
        sdg.registerWindow(window);
    }

    @Test(expected = IllegalStateException.class)
    public void registerWindow2() {
        final String invalidWindowUri = "http:// /invalid";
        final String streamUri = "http://stream";
        final Duration duration = Duration.parse("PT1S");
        final long time = 0;
        // register
        final RDFStarStream rdfStarStream = new RDFStarStream(streamUri);
        new WindowDatasetGraph(invalidWindowUri, duration, duration, time, rdfStarStream);
    }

    @Test(expected = IllegalStateException.class)
    public void registerWindow3() {
        final String validWindowUri = "http://window";
        final String streamUri = "http://stream";
        final Duration duration = Duration.parse("PT1S");
        final long time = 0;
        // register
        final RDFStarStream rdfStarStream = new RDFStarStream(streamUri);
        final WindowDatasetGraph w1 = new WindowDatasetGraph(validWindowUri, duration, duration, time, rdfStarStream);
        sdg.registerWindow(w1);
        sdg.registerWindow(w1);
    }

    @Test(expected = IllegalStateException.class)
    public void registerWindow4() {
        final String validWindowUri = "http://window";
        final String streamUri1 = "http://stream1";
        final String streamUri2 = "http://stream2";
        final Duration duration = Duration.parse("PT1S");
        final long time = 0;
        // register
        final RDFStarStream rdfStarStream1 = new RDFStarStream(streamUri1);
        final RDFStarStream rdfStarStream2 = new RDFStarStream(streamUri2);
        final WindowDatasetGraph w1 = new WindowDatasetGraph(validWindowUri, duration, duration, time, rdfStarStream1);
        final WindowDatasetGraph w2 = new WindowDatasetGraph(validWindowUri, duration, duration, time, rdfStarStream2);
        sdg.registerWindow(w1);
        sdg.registerWindow(w2);
    }

    @Test
    public void baseDataset1() {
        final DatasetGraphStar ds1 = sdg.getBaseDataset();
        final DatasetGraphStar ds2 = sdg.getActiveDataset();
        assertEquals(ds1, ds2);
    }

    @Test
    public void baseDataset2() {
        final DatasetGraphStar ds1 = new DatasetGraphStar();
        sdg.setBaseDataset(ds1);
        final DatasetGraphStar ds2 = sdg.getBaseDataset();
        assertEquals(ds1, ds2);
    }

    @Test
    public void windowDataset1() {
        final String wUri = "http://window";
        final String sUri = "http://stream";
        final Duration duration = Duration.parse("PT1S");
        final long time = 0;
        // register
        final RDFStarStream rdfStarStream = new RDFStarStream(sUri);
        final WindowDatasetGraph window = new WindowDatasetGraph(wUri, duration, duration, time, rdfStarStream);
        sdg.registerWindow(window);

        final DatasetGraphStar ds1 = sdg.getActiveDataset();
        sdg.useWindowDataset(wUri);
        final DatasetGraphStar ds2 = sdg.getActiveDataset();
        assertNotEquals(ds1, ds2);
    }

    @Test
    public void windowDataset2() {
        final String wUri = "http://window";
        final String sUri = "http://stream";
        final Duration duration = Duration.parse("PT1S");
        final long time = 0;
        // register
        final RDFStarStream rdfStarStream = new RDFStarStream(sUri);
        final WindowDatasetGraph window = new WindowDatasetGraph(wUri, duration, duration, time, rdfStarStream);
        sdg.registerWindow(window);

        sdg.useWindowDataset(wUri);
        final DatasetGraphStar ds1 = sdg.getActiveDataset();
        sdg.useWindowDataset(wUri);
        final DatasetGraphStar ds2 = sdg.getActiveDataset();
        assertEquals(ds1, ds2);
    }

}