package se.liu.ida.rspqlstar.stream;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.riot.ResultSetMgr;
import org.apache.jena.riot.resultset.ResultSetLang;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Iterator;

public class ResultWriterStream implements ContinuousListener {
    private Logger logger = Logger.getLogger(ResultWriterStream.class);
    private boolean printHeader = true;
    private final PrintStream ps;
    private int skip = 0;
    private String sep = "\t";
    public boolean dropLiteralDatatype = true;

    /**
     * Log output from a continuous SELECT query.
     * @param ps Print stream
     */
    public ResultWriterStream(PrintStream ps){
        this.ps = ps;
    }
    /**
     * Log output from a continuous SELECT query.
     * @param fileName
     */
    public ResultWriterStream(String fileName) throws FileNotFoundException {
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
            final Iterator<String> iter = rs.getResultVars().iterator();
            while(iter.hasNext()){
                ps.print(iter.next());
                if(iter.hasNext()) {
                    ps.print(sep);
                }
            }
            ps.println();
        }

        // skip results?
        if(skip > 0) {
            while(rs.hasNext()) {
                rs.next();
                counter++;
            }
            skip--;
            logger.info("Skip " + counter + " results ");
            return;
        }

        while(rs.hasNext()){
            counter++;
            final QuerySolution qs = rs.next();
            final Iterator<String> iter = rs.getResultVars().iterator();
            while(iter.hasNext()){
                final String s = getString(qs.get(iter.next()));
                ps.print(s);
                if(iter.hasNext()) {
                    ps.print(sep);
                }
            }
            //logger.debug(qs);
            ps.println();
        }
        final long executionTime = System.currentTimeMillis() - startedAt;
        logger.debug("Wrote " + counter + " results");
        logger.debug("Executed in " + executionTime + " ms,  found " + counter + " results");
    }

    public String getString(RDFNode node){
        if(dropLiteralDatatype){
            if(node.isLiteral()){
                return  node.asLiteral().getLexicalForm();
            }
        }
        return node.toString();
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