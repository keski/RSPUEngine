package se.liu.ida.rspqlstar.stream;

import org.apache.jena.riot.RDFParser;
import org.apache.log4j.Logger;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Iterator;
import java.util.stream.Stream;

public class StreamFromFile implements Runnable {
    private static Logger logger = Logger.getLogger(StreamFromFile.class);
    private RDFStarStream stream;
    private final String base = "http://base/";
    private boolean stop = false;
    public boolean isRunning = true;
    private String fileName;
    private String prefixes;

    /**
     * Produce a new stream from file. Each line is considered an element and a total delay
     * is produced between streamed elements.
     * @param stream
     * @param fileName
     */
    public StreamFromFile(RDFStarStream stream, String fileName) {
        this.stream = stream;
        this.fileName = fileName;
    }

    @Override
    public void run() {
        final File file = new File(fileName);
        try (Stream linesStream = Files.lines(file.toPath())) {
            final Iterator<String> linesIter = linesStream.iterator();
            while(linesIter.hasNext() && !stop){
                final String line = linesIter.next();

                if(prefixes == null) {
                    prefixes = line;
                } else {
                    final RDFStarStreamElement tg = new RDFStarStreamElement();
                    RDFParser.create()
                            .base(base)
                            .source(new ByteArrayInputStream((prefixes + line).getBytes()))
                            .checking(false)
                            .lang(LangTrigStar.TRIGSTAR)
                            .parse(tg);

                    // sleep until tg.time
                    long sleep = tg.getTime() - TimeUtil.getTime();
                    TimeUtil.silentSleep(sleep);
                    push(tg);
                    if(!linesIter.hasNext()){
                        logger.warn("No more data to stream into " + stream.uri);
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        isRunning = false;
    }

    /**
     * Start streaming from file.
     */
    public void start(){
        new Thread(this).start();
    }

    /**
     * Stop the stream.
     */
    public void stop(){
        this.stop = true;
    }

    public void push(RDFStarStreamElement tg) {
        stream.push(tg);
    }

    public RDFStarStream getStream(){
        return stream;
    }
}
