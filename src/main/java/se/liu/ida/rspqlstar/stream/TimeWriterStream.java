package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class TimeWriterStream implements ContinuousListener {
    private Logger logger = Logger.getLogger(TimeWriterStream.class);
    private boolean printHeader = true;
    private final PrintStream ps;
    private int skip = 0;
    private String sep = ",";

    /**
     * Log timing information from a continuous SELECT query.
     * @param ps Print stream
     */
    public TimeWriterStream(PrintStream ps){
        this.ps = ps;
    }

    /**
     * Log timing information from a continuous SELECT query.
     * @param fileName
     */
    public TimeWriterStream(String fileName) throws FileNotFoundException {
        this(new PrintStream(new File(fileName)));
    }

    /**
     * Use this function to skip the next x results.
     * @param x
     */
    public void setSkip(int x){
        this.skip = x;
    }


    @Override
    public void push(ResultSet rs, long startedAt) {
        int counter = 0;
        if(printHeader){
            printHeader = false;
            ps.print("rate");
            ps.print(sep);
            ps.print("ratio");
            ps.print(sep);
            ps.print("type");
            ps.print(sep);
            ps.print("threshold");
            ps.print(sep);
            ps.print("exec_time");
            ps.println();
        }

        // skip results?
        if(skip > 0) {
            while(rs.hasNext()) {
                rs.next();
                counter++;
            }
            skip--;
            logger.info("Skipped " + counter + " results ");
            return;
        }

        // collect results
        String rate = "-1";
        String ratio = "-1";
        String threshold = "-1";
        String type = "-1";
        while(rs.hasNext()) {
            counter++;
            final QuerySolution qs = rs.next();
            rate = qs.get("rate").asLiteral().getLexicalForm();
            ratio = qs.get("ratio").asLiteral().getLexicalForm();
            threshold = qs.get("threshold").asLiteral().getLexicalForm();
            type = qs.get("type").asLiteral().getLexicalForm();
            logger.debug(qs.get("max_confidence"));
        }
        final long executionTime = System.currentTimeMillis() - startedAt;

        ps.print(rate);
        ps.print(sep);
        ps.print(ratio);
        ps.print(sep);
        ps.print(type);
        ps.print(sep);
        ps.print(threshold);
        ps.print(sep);
        ps.print(executionTime);
        ps.println();
        logger.debug("Executed in " + executionTime + " ms");
        logger.debug("Found " + counter + " results");
    }

    @Override
    public void push(Dataset ds, long startedAt) {
        throw new IllegalStateException("Not implemented");
    }

    @Override
    public void push(RDFStarStreamElement tg) {
        throw new IllegalStateException("Not implemented");
    }

    public void flush(){
        ps.flush();
    }

    public void close(){
        flush();
        ps.close();
    }
}