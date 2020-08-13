package se.liu.ida.rspqlstar.util;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionary;
import se.liu.ida.rspqlstar.store.dictionary.nodedictionary.NodeDictionaryFactory;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadPatternBuilder;
import se.liu.ida.rspqlstar.store.engine.main.pattern.QuadStarPattern;
import se.liu.ida.rspqlstar.store.index.IdBasedQuad;

import java.applet.Applet;
import java.applet.AudioClip;
import java.awt.*;
import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Utils {
    private static final Logger logger = Logger.getLogger(Utils.class);
    private static final NodeDictionary nd = NodeDictionaryFactory.get();

    /**
     * Utility function for reading a file to a string. Not recommended for large files.
     * @param fileName
     * @return
     */
    public static String readFile(String fileName) {
        try {
            if (fileName.startsWith("/")) {
                return new String(Files.readAllBytes(Paths.get(fileName)));
            }
            final URL url = Utils.class.getClassLoader().getResource(fileName);
            if (url == null) {
                throw new IllegalStateException("File not found: " + fileName);
            }
            final File file = new File(url.getFile());
            return new String(Files.readAllBytes(file.toPath()));
        } catch (IOException e){
            logger.error(e.getMessage());
            return null;
        }
    }

    /**
     * Return the current gc count.
     * @return
     */
    public static long getGcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = b.getCollectionCount();
            if (count != -1) { sum += count; }
        }
        return sum;
    }

    /**
     * Get the amount of currently allocated memory directly after garbage collection.
     * @return
     */
    public static float getReallyUsedMemory() {
        long before = getGcCount();
        System.gc();
        while (getGcCount() == before);
        return getCurrentlyAllocatedMemory();
    }

    /**
     * Fint the currently allocated memory.
     * @return
     */
    public static float getCurrentlyAllocatedMemory() {
        final Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }

    /**
     * Utility function to convert a string of comma separated integers to an int array.
     * @param s
     * @return
     */
    public static byte[] stringToIntegerArray(String s) {
        String [] str = s.split(",");
        int size = str.length;
        byte[] arr = new byte[size];
        for(int i=0; i<size; i++) {
            arr[i] = Byte.parseByte(str[i]);
        }
        return arr;
    }

    /**
     * Returns true if URL is valid
     * @param uri
     * @return
     */
    public static boolean isValidUri(String uri){
        try {
            new URL(uri).toURI();
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    public static void beep(){
        try {
            File file = new File("src/main/resources/notification.wav");
            URL url = null;
            if (file.canRead()) {
                url = file.toURI().toURL();
            }
            AudioClip clip = Applet.newAudioClip(url);
            clip.play();
        } catch (Exception e){
            logger.error(e.getMessage());
        }
    }
}
