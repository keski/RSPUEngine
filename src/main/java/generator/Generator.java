package generator;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.special.Erf;
import org.apache.log4j.Logger;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import smile.Network;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Generator {
    // Use reference time for streams: 1582757970590 -> 2020-02-26T23:59
    public static final long referenceTime = 1582757970590l;
    public static final float hrLimit = 100f;
    public static final int brLimit = 30;
    public static final int oxLimit = 90;
    private static final Logger logger = Logger.getLogger(Generator.class);

    public static void main(String[] args) throws IOException {
        BayesianNetwork.init();
        final String root = new File("").getAbsolutePath() + "/";
        final Network net = BayesianNetwork.loadNetwork("", root + "data/experiments/heart-attack.bn");
        net.updateBeliefs();

        // Generate 2 minutes for each config
        final int duration = 30;      // 100, 200, 300, 400, 500, 600, 700, 800, 900, 1000
        final int[] rates = new int[]{100,1000}; //200,300,400,500,600,700,800,900,1000};
        // Likelihood (i.e., probability) that uncertain evidence is NOT correct
        final double[] occUncList = new double[]{.001, .01, .05, .10, .25};
        // The probability (p-value) that the value is NOT within the query threshold
        final double[] attrUncList = new double[]{.001, .01, .05, .10, .25};

        final int total = rates.length * (occUncList.length + attrUncList.length);
        int progress = 0;
        for(int rate : rates){
            for(double occUnc : occUncList) {
                logger.info("Generating: " + progress++ + " of " + total);
                generate(occUnc, 0, rate, duration, net);
            }
            for (double attrUnc : attrUncList) {
                logger.info("Generating: " + progress++ + " of " + total);
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

        // Distributions for sampling attribute values
        final double z = pToZ(attrUnc);
        final double hrSd = hrLimit * 0.01;
        final NormalDistribution hrAbove = new NormalDistribution(hrLimit + z * hrSd, hrSd);
        final NormalDistribution hrBelow = new NormalDistribution(hrLimit - z * hrSd, hrSd);
        final double brSd = brLimit * 0.01;
        final NormalDistribution brAbove = new NormalDistribution(brLimit + z * brSd, brSd);
        final NormalDistribution brBelow = new NormalDistribution(brLimit - z * brSd, brSd);
        final double oxSd = oxLimit * 0.01;
        final NormalDistribution oxAbove = new NormalDistribution(oxLimit + z * oxSd, oxSd);
        final NormalDistribution oxBelow = new NormalDistribution(oxLimit - z * oxSd, oxSd);

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
                        t, idCounter, foi,
                        haOcc == 1 ? hrAbove.sample() : hrBelow.sample(),
                        brOcc == 1 ? brAbove.sample() : brBelow.sample(),
                        haOcc == 1 ? oxBelow.sample() : oxAbove.sample());
                // Derived event types are associated with a probability (likelihood)
                final Event hr = new HighHeartRateEvent(ha, hrOcc == 1 ? 1 - occUnc : occUnc, hrSd);
                final Event br = new HighBreathingRateEvent(ha, brOcc == 1 ? 1 - occUnc : occUnc, brSd);
                final Event ox = new LowOxygenSaturationEvent(ha, oxOcc == 1 ? 1 - occUnc : occUnc, oxSd);
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
        if (p == 0) {
            return 0;
        } else {
            return Math.sqrt(2) * Erf.erfcInv(2 * p);
        }
    }
}
