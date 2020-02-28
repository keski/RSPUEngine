package generator;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import smile.Network;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.rmi.ServerError;
import java.util.concurrent.ThreadPoolExecutor;

public class Generator {
    // Use reference time for streams: 1582757970590 -> 2020-02-26T23:59
    public static final long referenceTime = 1582757970590l;
    public static final int hrThreshold = 100;
    public static final int brThreshold = 30;
    public static final int oxThreshold = 90;

    public static void main(String[] args) throws IOException {
        BayesianNetwork.init();
        final String root = new File("").getAbsolutePath() + "/";
        final Network net = BayesianNetwork.loadNetwork("", root + "data/experiments/heart-attack.bn");
        net.updateBeliefs();

        // Generate 3 minutes for each config
        final int duration = 50;
        int[] streamRateList = new int[]{1};
        double[] occUncList = new double[]{1,0.99,0.95, 0.9, 0.8, 0.7, 0.6, 0.5};
        double[] attrUncList = new double[]{0, 1, 2, 3};

        // occurrence uncertainty
        for(int streamRate : streamRateList){
            for(double occUnc : occUncList) {
                for (double attrUnc : attrUncList) {
                    generate(occUnc, attrUnc, streamRate, duration, net);
                }
            }
        }
    }

    /**
     *
     * @param occUnc The likelihood ratio of a generated event reflecting the true observation in [0,0.5].
     * @param attrUnc Standard deviation above/below thresholds
     * @param streamRate The number of HeartAttackEvents generated per second
     * @param duration The duration for which the stream should be generated in seconds
     * @param net
     * @throws IOException
     */
    public static void generate(double occUnc, double attrUnc, int streamRate, int duration, Network net) throws IOException {
        net.clearAllEvidence();
        net.updateBeliefs();

        // Uniform distributions for HR, BR and OX
        final double haProb = net.getNodeValue("HeartAttackEvent")[0];
        final BinomialDistribution p = new BinomialDistribution(1, haProb);

        final double stddev_ratio = 0.01;
        final double hrShift = attrUnc * hrThreshold * stddev_ratio; // stddev is 1 %
        final double brShift = attrUnc * brThreshold * stddev_ratio;
        final double oxShift = attrUnc * oxThreshold * stddev_ratio;

        // Create file writers. File format is: "<occUnc>-<attrUnc>-<streamRate>-<eventType>.trigs"
        String base = String.format("data/experiments/streams/%s-%s-%s-", occUnc, attrUnc, streamRate);
        final FileWriter haFileWriter = new FileWriter(base + "HA.trigs");
        final FileWriter hrFileWriter = new FileWriter(base + "HR.trigs");
        final FileWriter brFileWriter = new FileWriter(base + "BR.trigs");
        final FileWriter oxFileWriter = new FileWriter(base + "OX.trigs");

        // Add prefixes
        final String prefixes = "" +
                "@prefix : <http://ecare#> . " +
                "@prefix rspu: <http://w3id.org/rsp/rspu#> . " +
                "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . " +
                "@prefix sosa: <http://www.w3.org/ns/sosa/> . " +
                "@prefix prov: <http://www.w3.org/ns/prov#> ." +
                "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
        haFileWriter.write(prefixes);
        hrFileWriter.write(prefixes);
        brFileWriter.write(prefixes);
        oxFileWriter.write(prefixes);

        long time = referenceTime;
        int idCounter = 0;
        for(int i=0; i < duration; i++){
            for(int j=0; j < streamRate; j++) {
                final int haOcc = p.sample();
                if(haOcc == 1){
                    net.setEvidence("HeartAttackEvent", "T");
                } else {
                    net.setEvidence("HeartAttackEvent", "F");
                }
                net.updateBeliefs();

                final double hrProb = net.getNodeValue("HighHeartRateEvent")[0];
                final double brProb = net.getNodeValue("HighBreathingRateEvent")[0];
                final double oxProb = net.getNodeValue("LowOxygenSaturationEvent")[0];
                final int hrOcc = new BinomialDistribution(1, hrProb).sample();
                final int brOcc = new BinomialDistribution(1, brProb).sample();
                final int oxOcc = new BinomialDistribution(1, oxProb).sample();

                // Pr(v > threshold) = 99%

                // Create heart attack event (if sample = 1 then the event occurred)
                final long t = time + j;
                final String foi = "person" + idCounter;
                final HeartAttackEvent ha = new HeartAttackEvent(
                        haOcc, // crisp instead of prior (i.e., not haProb)
                        t,
                        idCounter,
                        foi,
                        hrThreshold + (hrOcc == 0 ? 1 : -1) * hrShift,
                        brThreshold + (brOcc == 0 ? 1 : -1) * brShift,
                        oxThreshold - (oxOcc == 0 ? 1 : -1) * oxShift);

                // Create SDE events
                // The derived event type of each event is associated with a probability
                final Event hr = new HighHeartRateEvent(ha,
                        hrOcc == 1 ? occUnc : 1 - occUnc,
                        hrThreshold * stddev_ratio);
                final Event br = new HighBreathingRateEvent(ha,
                        brOcc == 1 ? occUnc : 1 - occUnc,
                        brThreshold * stddev_ratio);
                final Event ox = new LowOxygenSaturationEvent(ha,
                        oxOcc == 1 ? occUnc : 1 - occUnc,
                        oxThreshold * stddev_ratio);
                haFileWriter.write(ha.toString().replaceAll("\\s+", " ") + "\n");
                hrFileWriter.write(hr.toString().replaceAll("\\s+", " ") + "\n");
                brFileWriter.write(br.toString().replaceAll("\\s+", " ") + "\n");
                oxFileWriter.write(ox.toString().replaceAll("\\s+", " ") + "\n");
                idCounter++;
            }
            time += 1000;
        }

        // close all writers
        haFileWriter.close();
        hrFileWriter.close();
        brFileWriter.close();
        oxFileWriter.close();
    }
}
