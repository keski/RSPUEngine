package generator;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.special.Erf;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import smile.Network;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

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

        // Generate 2 minutes for each config
        final int duration = 60;
        final int[] rates = new int[]{1000};
        // Likelihood (i.e., probability) that uncertain evidence is NOT correct
        final double[] occUncList = new double[]{.001, .01, .05, .10, .15, .20, .25, .30, .35, .40, .45, .50};
        // The probability (p-value) that the value is NOT within the query threshold
        final double[] attrUncList = new double[]{.001, .01, .05, .10, .15, .20, .25, .30, .35, .40, .45, .50};

        for(int rate : rates){
            for(double occUnc : occUncList) {
                generate(occUnc, 0, rate, duration, net);
            }
            for (double attrUnc : attrUncList) {
                generate(0, attrUnc, rate, duration, net);
            }
        }
    }

    /**
     *
     * @param occUnc The likelihood ratio of a virtual node not reflecting its parent.
     * @param attrUnc The percentile probability that the reported value is not within the threshold value
     * @param rate The number of HeartAttackEvents generated per second
     * @param duration The duration for which the stream should be generated in seconds
     * @param net
     * @throws IOException
     */
    public static void generate(double occUnc, double attrUnc, int rate, int duration, Network net) throws IOException {
        net.clearAllEvidence();
        net.updateBeliefs();

        // Uniform distributions for HR, BR and OX
        final double haProb = net.getNodeValue("HeartAttackEvent")[0];

        final BinomialDistribution p = new BinomialDistribution(1, haProb);

        final double percentage = 0.01; // standard deviation is defined as 1% of threshold value
        final double z = attrUnc == 0 ? 0 : pToZ(attrUnc);
        final double hrShift = z * hrThreshold * percentage;
        final double brShift = z * brThreshold * percentage;
        final double oxShift = z * oxThreshold * percentage;

        // Create file writers. File format is: "occUnc-attrUnc-streamRate-eventType.trigs"
        final String base = String.format("data/experiments/streams/%.3f-%.3f-%s-", occUnc, attrUnc, rate);
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
            for(int j=0; j < rate; j++) {
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

                // Create heart attack event (if sample = 1 then the event occurred)
                final long t = time + j;
                final String foi = "person" + idCounter;
                final HeartAttackEvent ha = new HeartAttackEvent(
                        haOcc, // here crisp, not the prior haProb
                        t,
                        idCounter,
                        foi,
                        hrThreshold + (hrOcc == 0 ? 1 : -1) * hrShift,
                        brThreshold + (brOcc == 0 ? 1 : -1) * brShift,
                        oxThreshold - (oxOcc == 0 ? 1 : -1) * oxShift);

                // Create SDE events

                // Derived event types are associated with a probability
                // Note: occUnc is simply the degree to which the virtual node reflects the state of its parent.
                final Event hr = new HighHeartRateEvent(ha,
                        hrOcc == 1 ? occUnc : 1 - occUnc,
                        hrThreshold * percentage);
                final Event br = new HighBreathingRateEvent(ha,
                        brOcc == 1 ? occUnc : 1 - occUnc,
                        brThreshold * percentage);
                final Event ox = new LowOxygenSaturationEvent(ha,
                        oxOcc == 1 ? occUnc : 1 - occUnc,
                        oxThreshold * percentage);
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


    /**
     * Find the z-value equivalent to a given percentile (one-tailed).
     * @param p
     * @return
     */
    public static double pToZ(double p) {
        return Math.sqrt(2) * Erf.erfcInv(2*p);
    }
}
