package se.liu.ida.rspqlstar.store.dataset;

import org.apache.commons.collections4.iterators.IteratorChain;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.lang.NamedWindow;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadPatternBuilder;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.time.Duration;
import java.util.*;

/**
 * StreamingDatasetGraph it a wrapper for all datasets to be queried. The execution time is set
 * and remains fixed static throughout each execution.
 */
public class StreamingDatasetGraph extends AbstractDatasetGraph {
    private final Logger logger = Logger.getLogger(StreamingDatasetGraph.class);
    private DatasetGraphStar baseDataset;
    private Map<String, WindowDatasetGraph> windows;
    public Map<String, RDFStarStream> rdfStreams;
    private DatasetGraphStar activeDataset;
    private long time;
    private HashSet<String> preparedForQueries;

    /**
     * Create a StreamingDatasetGraph.
     */
    public StreamingDatasetGraph(long time){
        baseDataset = new DatasetGraphStar();
        windows = new HashMap<>();
        rdfStreams = new HashMap<>();
        activeDataset = baseDataset;
        preparedForQueries = new HashSet<>();
        this.time = time;
    }

    /**
     * Register an RDFStarStream in the SDG.
     * @param rdfStarStream
     * @return
     */
    public RDFStarStream registerStream(RDFStarStream rdfStarStream){
        final RDFStarStream s = rdfStreams.get(rdfStarStream.uri);
        if(s == null){
            rdfStreams.put(rdfStarStream.uri, rdfStarStream);
        } else if(!s.equals(rdfStarStream)){
            throw new IllegalStateException("The stream " + rdfStarStream.uri + " is already registered");
        }
        return rdfStarStream;
    }

    /**
     * Register a new RDFStarStream in the SDG.
     * @param streamUri
     * @return
     */
    public RDFStarStream registerStream(String streamUri){
        if(rdfStreams.containsKey(streamUri)){
            return rdfStreams.get(streamUri);
        } else {
            return registerStream(new RDFStarStream(streamUri));
        }
    }


    /**
     * Initialize the dataset for use with a given query. All streams on which the
     * the query depends must be registered.
     *
     * Note: A single dataset can be used for multiple parallel queries; however, each named
     * window must be unique, since a WindowDatasetGraph is created for each named window
     * mentioned in the query.
     */

    public void initForQuery(RSPQLStarQuery query){
        for(NamedWindow w : query.getNamedWindows().values()){
            final String name = w.getWindowName();
            final Duration range = w.getRange();
            final Duration step = w.getStep();
            final RDFStarStream stream = registerStream(w.getStreamName());
            registerWindow(new WindowDatasetGraph(name, range, step, time, stream));
        }
        preparedForQueries.add(query.getOutputStream());
    }

    public void setBaseDataset(DatasetGraphStar dataset){
        baseDataset = dataset;
    }

    public DatasetGraphStar getBaseDataset(){
        return baseDataset;
    }

    public WindowDatasetGraph registerWindow(WindowDatasetGraph window){
        final String wUri = window.getName();
        final String sUri = window.getRdfStream().uri;
        // check window URI
        if(windows.containsKey(wUri)){
           throw new IllegalStateException("Window " + wUri + " is already registered");
        }
        // check stream
        if(rdfStreams.containsKey(sUri)){
            if(!rdfStreams.get(sUri).equals(window.getRdfStream())){
                logger.error("Stream " + sUri + " is already registered for a different stream");
                return null;
            }
        } else {
            rdfStreams.put(sUri, window.getRdfStream());
        }
        windows.put(window.getName(), window);
        return window;
    }

    public DatasetGraphStar getActiveDataset(){
        return activeDataset;
    }

    public void setTime(long time){
        this.time = time;
    }

    public long getTime(){
        return time;
    }

    @Override
    public Graph getDefaultGraph() {
        return activeDataset.getDefaultGraph();
    }

    @Override
    public void add(Quad quad){
        activeDataset.add(quad);
    }

    @Override
    public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
        return getActiveDataset().find(g, s, p , o);
    }

    public Iterator<IdBasedQuad> idBasedFind(Node g, Node s, Node p, Node o) {
        final QuadStarPattern pattern = QuadPatternBuilder.createQuadPattern(g, s, p, o);
        return getActiveDataset().idBasedFind(pattern);
    }

    @Override
    public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
        if(g.equals(Quad.defaultGraphIRI)){
            return null;
        }
        return getActiveDataset().find(g, s, p , o);
    }

    /**
     * Iterate all quads in all datasets associated with this StreamingDatasetGraph.
     *
     * @return
     */
    public Iterator<IdBasedQuad> iterator(){
        final IteratorChain<IdBasedQuad> iteratorChain = new IteratorChain<>();
        iteratorChain.addIterator(baseDataset.iterateAll());
        windows.forEach((iri, w) -> iteratorChain.addIterator(w.iterate(time)));
        return iteratorChain;
    }

    public DatasetGraphStar useWindowDataset(String name) {
        final WindowDatasetGraph wds = windows.get(name);
        if(wds == null){
            throw new IllegalStateException("The named window " + name + " does not exist.");
        }
        activeDataset = wds.getDataset(time);
        return activeDataset;
    }

    public DatasetGraphStar useBaseDataset() {
        activeDataset = baseDataset;
        return activeDataset;
    }

    public boolean isPreparedForQuery(RSPQLStarQuery query) {
        return preparedForQueries.contains(query.getOutputStream());
    }

    /**
     * Get an RDFStarStream registered in the current SDG. Returns null if no matching stream is found.
     * @param uri
     * @return
     */
    public RDFStarStream getStream(String uri){
        return rdfStreams.get(uri);
    }

    /**
     * Get an RDFStarStream registered in the current SDG. Returns null if no matching stream is found.
     * @param uri
     * @return
     */
    public WindowDatasetGraph getWindow(String uri){
        return windows.get(uri);
    }

    /**
     * Check if quad is in current active dataset.
     * @param quad
     * @return
     */
    public boolean contains(Quad quad) {
        return activeDataset.contains(quad);
    }

    /**
     * Check if IdBasedQuad is in current active dataset.
     * @param idBasedQuad
     * @return
     */
    public boolean contains(IdBasedQuad idBasedQuad) {
        return activeDataset.contains(idBasedQuad);
    }
}
