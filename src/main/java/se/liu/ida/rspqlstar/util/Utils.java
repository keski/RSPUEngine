package se.liu.ida.rspqlstar.util;

import java.io.File;
import java.io.IOException;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Utils {

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
            System.err.println(e.getMessage());
            return null;
        }
    }

    public static double calculateStandardDeviation(long[] numArray){
        double sum = 0.0, standardDeviation = 0.0;
        int length = numArray.length;

        for(double num : numArray) {
            sum += num;
        }

        double mean = sum/length;

        for(double num: numArray) {
            standardDeviation += Math.pow(num - mean, 2);
        }

        return Math.sqrt(standardDeviation/length);
    }

    public static double calculateMean(long[] numArray){
        double sum = 0.0;
        int length = numArray.length;

        for(double num : numArray) {
            sum += num;
        }

        double mean = sum/length;
        return mean;
    }

    public static long getGcCount() {
        long sum = 0;
        for (GarbageCollectorMXBean b : ManagementFactory.getGarbageCollectorMXBeans()) {
            long count = b.getCollectionCount();
            if (count != -1) { sum += count; }
        }
        return sum;
    }
    public static float getReallyUsedMemory() {
        long before = getGcCount();
        System.gc();
        while (getGcCount() == before);
        return getCurrentlyAllocatedMemory();
    }

    public static float getCurrentlyAllocatedMemory() {
        final Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }
}
