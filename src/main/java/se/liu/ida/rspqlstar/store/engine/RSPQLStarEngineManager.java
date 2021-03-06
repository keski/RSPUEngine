package se.liu.ida.rspqlstar.store.engine;

import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ARQ;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.riot.RDFParser;
import org.apache.log4j.Logger;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
import se.liu.ida.rspqlstar.function.Probability;
import se.liu.ida.rspqlstar.lang.RSPQLStar;
import se.liu.ida.rspqlstar.query.RSPQLStarQuery;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.StreamingDatasetGraph;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionary;
import se.liu.ida.rspqlstar.store.dictionary.referencedictionary.ReferenceDictionaryFactory;
import se.liu.ida.rspqlstar.stream.StreamFromFile;
import se.liu.ida.rspqlstar.util.TimeUtil;
import se.liu.ida.rspqlstar.util.Utils;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

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
    }

    public void loadData(String path) {
        try {
            final ByteArrayInputStream bais = new ByteArrayInputStream(FileUtils.readFileToByteArray(new File(path)));
            RDFParser.create()
                    .base("http://base/")
                    .source(bais)
                    .checking(false)
                    .lang(LangTrigStar.TRIGSTAR)
                    .parse(sdg.getBaseDataset());
        } catch (IOException e){
            logger.error("Failed to read file: " + path + e);
        }
        System.out.println("Loaded: " + sdg.getBaseDataset().size());
    }

    public static void loadData(InputStream in){

    }

    /**
     * Create a new RSPUEngine manager
     * @param applicationTime Application time. Offsets internal clock accordingly.
     */
    public RSPQLStarEngineManager(long applicationTime){
        this(applicationTime, 8);
    }

    /**
     * Create a new RSPUEngine manager
     * @param applicationTime Application time. Offsets internal clock accordingly.
     * @param executorThreadPool Size of executor thread pool
     */
    public RSPQLStarEngineManager(long applicationTime, int executorThreadPool){
        executor = Executors.newFixedThreadPool(executorThreadPool);
        TimeUtil.setOffset(new Date().getTime() - applicationTime);
        sdg = new StreamingDatasetGraph(applicationTime);
    }

    public StreamingDatasetGraph getSdg(){
        return sdg;
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
            logger.error("A stream with the URI " + outputStream + " is already registered");
            return null;
        }
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);
        queries.put(outputStream, qexec);

        // execute query
        if(query.isSelectType()){
            executor.submit(qexec.execContinuousSelect());
        } else if(query.isConstructType()) {
            throw new IllegalStateException("Not yet implemented in manager");
            // create output stream
            //final RDFStarStream stream = registerStream(outputStream);
            //qexec.addContinuousListener(new ConstructStream(stream));
            //executor.submit(qexec.execContinuousConstruct(applicationTime + 1));
        }
        return qexec;
    }

    /**
     * Run a query once.
     * @param queryString
     * @return
     */
    public ResultSet runOnce(String queryString){
        final RSPQLStarQuery query = (RSPQLStarQuery) QueryFactory.create(queryString, RSPQLStar.syntax);
        final RSPQLStarQueryExecution qexec = new RSPQLStarQueryExecution(query, sdg);
        return qexec.execSelect();
    }

    public void stop(){
        stop(10000);
    }

    public void stop(long timeout){
        // Stop all queries
        try {
            for (RSPQLStarQueryExecution exec : queries.values()) {
                exec.stop();
            }
            TimeUtil.silentSleep(timeout);
            // Stop all streams
            for (StreamFromFile stream : streamsFromFiles) {
                stream.stop();
                stream.getStream().clearListeners();
            }
            // Close down executor
            executor.shutdown();
            NodeDictionaryFactory.get().clear();
            ReferenceDictionaryFactory.get().clear();
        } catch (Exception e){
            logger.error(e.getMessage());
        }
    }

    /**
     * Register a new RDFStarStream.
     * @param streamUri
     * @return
     */
    public RDFStarStream registerStream(String streamUri){
        // check valid URI
        if(!Utils.isValidUri(streamUri)){
            logger.warn(streamUri + " is not a valid URI");
            return null;
        }

        // check if already registered
        if(streams.containsKey(streamUri)){
            logger.warn(streamUri + " has already been registered");
            return null;
        }

        // register new stream
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
        final StreamFromFile fileStream = new StreamFromFile(stream, filePath);
        executor.submit(fileStream);
        streamsFromFiles.add(fileStream);
    }
}
