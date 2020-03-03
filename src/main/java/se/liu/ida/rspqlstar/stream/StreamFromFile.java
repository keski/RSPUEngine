package se.liu.ida.rspqlstar.stream;

import org.apache.jena.riot.RDFParser;
import org.apache.log4j.Logger;
import se.liu.ida.rdfstar.tools.parser.lang.LangTrigStar;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStream;
import se.liu.ida.rspqlstar.store.dataset.RDFStarStreamElement;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;
import se.liu.ida.rspqlstar.util.TimeUtil;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.util.Iterator;
import java.util.stream.Stream;

public class StreamFromFile implements Runnable {
    private static Logger logger = Logger.getLogger(StreamFromFile.class);
    private RDFStarStream stream;
    private final String BASE = "http://base/";
    private boolean stop = false;
    private String fileName;
    private String prefixes;
    private long initialDelay;
    private final String root = new File("").getAbsolutePath() + "/";

    /**
     * Produce a new stream from file. Each line is considered an element and a total delay
     * is produced between streamed elements.
     * @param stream
     * @param fileName
     * @param initialDelay
     */
    public StreamFromFile(RDFStarStream stream, String fileName, long initialDelay) {
        this.stream = stream;
        this.fileName = fileName;
        this.initialDelay = initialDelay;
    }

    @Override
    public void run() {
        final File file = new File(fileName);
        TimeUtil.silentSleep(initialDelay);
        try (Stream linesStream = Files.lines(file.toPath())) {
            final Iterator<String> linesIter = linesStream.iterator();
            while(linesIter.hasNext() && !stop){
                final String line = linesIter.next();

                if(prefixes == null) {
                    prefixes = line;
                } else {
                    final RDFStarStreamElement tg = new RDFStarStreamElement();
                    RDFParser.create()
                            .base("http://base/")
                            .source(new ByteArrayInputStream((prefixes + line).getBytes()))
                            .checking(false)
                            .lang(LangTrigStar.TRIGSTAR)
                            .parse(tg);

                    // sleep until tg.time
                    long sleep = tg.time - TimeUtil.getTime().getTime();
                    if(sleep > 0){
                        TimeUtil.silentSleep(sleep);
                    }
                    push(tg);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
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
