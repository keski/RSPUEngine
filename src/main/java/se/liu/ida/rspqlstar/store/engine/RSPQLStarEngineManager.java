package se.liu.ida.rspqlstar.store.engine;

import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.stream.StreamFromFile;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RSPQLStarEngineManager {
    private static final Logger logger = Logger.getLogger(RSPQLStarEngineManager.class);
    private Map<String, RSPQLStarQueryExecution> queries = new HashMap<>();
    private Map<String, RDFStarStream> streams = new HashMap<>();
    private StreamingDatasetGraph sdg;
    final ExecutorService executor;
    final List<StreamFromFile> streamsFromFiles = new ArrayList<>();

    /**
     * Init RSPUStarEngine
     */
    public static void init(){
        RSPQLStarEngine.register();
        ARQ.init();
        Probability.init();
        BayesianNetwork.init();
    }

    /**
     * Create a new RSPUEngine manager
     * @param applicationTime
     */
    public RSPQLStarEngineManager(long applicationTime){
        RSPQLStarEngine.register();
        ARQ.init();
        Probability.init();
        Probability.init();
        executor = Executors.newFixedThreadPool(8);
        setApplicationTime(applicationTime);
        sdg = new StreamingDatasetGraph();
    }

    /**
     * Set application time.
     * @param applicationTime
     */
    public void setApplicationTime(long applicationTime){
        TimeUtil.setOffset(new Date().getTime() - applicationTime);
    }

    /**
     * Register a new RSPQLStar query
     * @param queryString
     * @return
     */
    public RSPQLStarQueryExecution registerQuery(String queryString){
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(queryString, RSPQLStar.syntax);
        return registerQuery(query);
    }

    /**
     * Register a new RSPQLStar query
     * @param query
     * @return
     */
    public RSPQLStarQueryExecution registerQuery(RSPQLStarQuery query){
        final String outputStream = query.getOutputStream();
        if(streams.containsKey(outputStream)){
            logger.warn("A stream with the URI " + outputStream + " is already registered");
            return null;
        }
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);
        queries.put(outputStream, qexec);

        // execute query
        if(query.isSelectType()){
            executor.submit(qexec.execContinuousSelect());
        } else if(query.isConstructType()) {
            // create output stream
            //final RDFStarStream stream = registerStream(outputStream);
            //qexec.addContinuousListener(new ConstructStream(stream));
            executor.submit(qexec.execContinuousConstruct());
        }
        return qexec;
    }

    public void stop(){
        streamsFromFiles.iterator().forEachRemaining(StreamFromFile::stop);
        queries.values().iterator().forEachRemaining(RSPQLStarQueryExecution::stop);
        TimeUtil.silentSleep(2000);
        streams.values().iterator().forEachRemaining(RDFStarStream::clearListeners);
        executor.shutdown();
    }

    /**
     * Register a new RDFStarStream.
     * @param streamUri
     * @return
     */
    public RDFStarStream registerStream(String streamUri){
        if(streams.containsKey(streamUri)){
            logger.warn("A stream with the URI " + streamUri + " is already registered");
            return null;
        }
        final RDFStarStream stream = new RDFStarStream(streamUri);
        streams.put(streamUri, stream);
        sdg.registerStream(stream);
        return stream;
    }

    /**
     * Return a map of registered RDF streams.
     * @return
     */
    public Map<String, RDFStarStream> getStreams(){
        return streams;
    }

    /**
     * Register a stream from file.
     * @param filePath
     * @param streamUri
     */
    public void registerStreamFromFile(String filePath, String streamUri){
        final RDFStarStream stream = registerStream(streamUri);
        final StreamFromFile fileStream = new StreamFromFile(stream, filePath, 0);
        executor.submit(fileStream);
        streamsFromFiles.add(fileStream);
    }
}
