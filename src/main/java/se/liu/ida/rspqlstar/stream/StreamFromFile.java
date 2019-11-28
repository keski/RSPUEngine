package se.liu.ida.rspqlstar.stream;

import org.apache.jena.riot.RDFParser;
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

public class StreamFromFile extends RSPQLStarStream {
    private String fileName;
    private String prefixes;
    private long initialDelay;

    /**
     * Produce a new stream from file. Each line is considered an element and a total delay
     * is produced between streamed elements.
     * @param rdfStream
     * @param fileName
     * @param initialDelay
     * @param totalDelay
     */
    public StreamFromFile(RDFStarStream rdfStream, String fileName, long initialDelay, long totalDelay) {
        super(rdfStream, totalDelay);
        this.fileName = fileName;
        this.initialDelay = initialDelay;
    }

    @Override
    public void run() {
        final URL url = getClass().getClassLoader().getResource(fileName);
        if(url == null) {
            throw new IllegalStateException("File not found: " + fileName) ;
        }
        final File file = new File(url.getFile());

        TimeUtil.silentSleep(initialDelay);
        try (Stream linesStream = Files.lines(file.toPath())) {
            final Iterator<String> linesIter = linesStream.iterator();
            while(linesIter.hasNext() && !stop){
                final String line = linesIter.next();
                final long t0 = TimeUtil.getTime().getTime();
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
                    long delay = tg.time - t0;
                    delayedPush(tg, delay < 0 ? 0 : delay);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void start(){
        new Thread(this).start();
    }
}
