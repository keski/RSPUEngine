package se.liu.ida.rspqlstar.stream;

import evaluation.ExperimentConfiguration;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dataset.DatasetGraphStar;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;

public class ResultWriterStream implements ContinuousListener {
    private final ExperimentConfiguration config;
    private final int i;

    private int executionCounter = 0;
    private Logger logger = Logger.getLogger(ResultWriterStream.class);
    private final PrintStream results;
    private final PrintStream executions;
    private String sep = "\t";
    public boolean first = true;

    // other
    private List<Double> durationsList = new ArrayList<>();
    private List<Double> resultsList = new ArrayList<>();

    /**
     * Log output from a continuous SELECT query.
     * @param results Print stream to which all resultsets are written
     * @param executions Print stream to which all execution times are written, along with the number of results in the result set
     * @param config
     * @param i
     */
    public ResultWriterStream(PrintStream results, PrintStream executions, ExperimentConfiguration config, int i){
        this.results = results;
        this.executions = executions;
        this.config = config;
        this.i = i;
    }

    @Override
    public void push(Dataset ds, long executionTime) {
        throw new IllegalStateException("Not supported");
    }

    @Override
    public void push(ResultSet rs, long executionTime) {
        executionCounter++;
        double counter = 0;
        while(rs.hasNext()){
            counter++;
            if(first){
                final Iterator<String> iter = rs.getResultVars().iterator();
                while(iter.hasNext()){
                    results.print(iter.next());
                    if(iter.hasNext()) {
                        results.print(sep);
                    }
                }
                results.print("\n");
                first = false;
            }
            final QuerySolution qs = rs.next();
            final Iterator<String> iter = rs.getResultVars().iterator();
            while(iter.hasNext()){
                final String s = getString(qs.get(iter.next()));
                results.print(s);
                if(iter.hasNext()) {
                    results.print(sep);
                }
            }
            logger.debug(qs);
            results.println();
        }
        final double duration = TimeUtil.getTime() - executionTime;
        if(executionCounter > config.warmUp && executionCounter <= config.warmUp + config.numberOfResults) {
            durationsList.add(duration);
            resultsList.add(counter);
            if(duration < 0){
                System.err.println("Warning! Negative time result, the engine is not keeping up!");
            }

            if(executionCounter == config.warmUp + config.numberOfResults){
                DoubleSummaryStatistics durationStats = durationsList
                        .stream()
                        .mapToDouble(Double::doubleValue)
                        .summaryStatistics();
                double durationAvg = durationStats.getAverage();
                double durationMin = durationStats.getMin();
                double durationMax = durationStats.getMax();

                DoubleSummaryStatistics resultsStats = resultsList
                        .stream()
                        .mapToDouble(Double::doubleValue)
                        .summaryStatistics();
                double resultsAvg = resultsStats.getAverage();
                executions.print(durationAvg);
                executions.print(sep);
                executions.print(durationMin);
                executions.print(sep);
                executions.print(durationMax);
                executions.print(sep);
                executions.print(resultsAvg);
                executions.print(sep);
                executions.print(config.rspuFilterPull);
                executions.print(sep);
                executions.print(config.useLazyVars);
                executions.print(sep);
                executions.print(config.useCache);
                executions.print(sep);
                executions.print(config.thresholds[i]);
                executions.print(sep);
                executions.print(config.selectivities[i]);
                if(config.join != null){
                    executions.print(sep);
                    executions.print(config.join);
                }
                executions.print(sep);
                executions.print(durationsList.stream().map(Object::toString).collect(Collectors.joining(", ")));
                executions.print("\n");
            }
            logger.info("Wrote " + (executionCounter - config.warmUp) + " of " + config.numberOfResults);
        }

        logger.info(String.format("Wrote %s results, executed in %s ms", counter, duration));
    }

    public String getString(RDFNode node){
        if(node == null){
            return "null";
        }
        return node.toString();
    }

    @Override
    public void push(RDFStarStreamElement tg) {
        throw new IllegalStateException("Not implemented");
    }

    public void flush(){
        results.flush();
        executions.flush();
    }

    public void close(){
        flush();
    }
}