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
import java.util.Arrays;

public class Generator {
    // Use reference time for streams: 1582757970590 -> 2020-02-26T23:59
    public static final long referenceTime = 1582757970590l;
    public static final float hrLimit = 100f;
    public static final int brLimit = 30;
    public static final int oxLimit = 90;
    private static final Logger logger = Logger.getLogger(Generator.class);

    public static void run(int duration, int[] rates, double[] occUncList, double[] attrUncList) throws IOException {
        BayesianNetwork.init();
        final String root = new File("").getAbsolutePath() + "/";
        final Network net = BayesianNetwork.loadNetwork("", root + "experiments/medical-generator.bn");
        net.updateBeliefs();

        final int total = rates.length * (occUncList.length + attrUncList.length);
        int progress = 0;
        for(int rate : rates) {
            for (double occUnc : occUncList) {
                progress++;
                logger.info("Generating: " + progress + " of " + total);
                generate(occUnc, 0, rate, 1, duration, net);
            }
            for (double attrUnc : attrUncList) {
                progress++;
                logger.info("Generating: " + progress + " of " + total);
                generate(0, attrUnc, rate, 1, duration, net);
            }
        }
    }

    /**
     *
     * @param occUnc The likelihood ratio of a virtual node not reflecting its parent.
     * @param attrUnc The percentile probability that the reported value is not within the threshold value
     * @param rate The number of HeartAttackEvents generated per second
     * @param ratio The ratio between SDE instances and CE. Note: Exponential in terms of query complexity.
     * @param duration The duration for which the stream should be generated in seconds
     * @param net Bayesian network used in generation
     * @throws IOException
     */
    public static void generate(double occUnc, double attrUnc, int rate, int ratio, int duration, Network net) throws IOException {
        net.clearAllEvidence();
        net.setNodeDefinition("VHighHeartRate", new double[]{1-occUnc, occUnc, occUnc, 1-occUnc});
        net.setNodeDefinition("VHighBreathingRate", new double[]{1-occUnc, occUnc, occUnc, 1-occUnc});
        net.setNodeDefinition("VLowOxygenSaturation", new double[]{1-occUnc, occUnc, occUnc, 1-occUnc});
        net.updateBeliefs();

        // Prior probability
        final double ceProb = net.getNodeValue("COPDExacerbation")[0];
        final BinomialDistribution ceDist = new BinomialDistribution(1, ceProb);

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
        final String base = String.format("experiments/streams/%.2f-%.2f-%s_%s", occUnc, attrUnc, rate, ratio);
        final FileWriter ceFileWriter = new FileWriter(base + "_ce.trigs");
        final FileWriter hrFileWriter = new FileWriter(base + "_hr.trigs");
        final FileWriter brFileWriter = new FileWriter(base + "_br.trigs");
        final FileWriter oxFileWriter = new FileWriter(base + "_ox.trigs");

        // Add prefixes
        final String prefixes = "" +
                "@prefix : <http://ecare#> . " +
                "@prefix rspu: <http://w3id.org/rsp/rspu#> . " +
                "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> . " +
                "@prefix sosa: <http://www.w3.org/ns/sosa/> . " +
                "@prefix prov: <http://www.w3.org/ns/prov#> ." +
                "@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n";
        ceFileWriter.write(prefixes);
        hrFileWriter.write(prefixes);
        brFileWriter.write(prefixes);
        oxFileWriter.write(prefixes);

        long time = referenceTime;
        int counter = 0;
        for(int i=0; i < duration/1000; i++){
            for(int j=0; j < rate; j++) {
                // Sample CE (occurred or not)
                final int ceOcc = ceDist.sample();
                net.setEvidence("COPDExacerbation", new String[]{"F", "T"}[ceOcc]);
                net.updateBeliefs();

                // Time and feature of interest
                final long t = time + j;
                final String foi = "person" + counter;

                // Ground data
                final COPDExacerbation ce = new COPDExacerbation(ceOcc, t, "ce" + counter, foi);
                ceFileWriter.write(ce.toString().replaceAll("\\s+", " ") + "\n");

                // Find probabilities for dependent events
                final double vhrProb = net.getNodeValue("VHighHeartRate")[0];
                final double vbrProb = net.getNodeValue("VHighBreathingRate")[0];
                final double voxProb = net.getNodeValue("VLowOxygenSaturation")[0];

                for(int x=0; x < ratio; x++) {
                    // Sample truth values
                    final int hrOcc = new BinomialDistribution(1, vhrProb).sample();
                    final int brOcc = new BinomialDistribution(1, vbrProb).sample();
                    final int oxOcc = new BinomialDistribution(1, voxProb).sample();

                    // Likelihood of the "derived" event types (likelihood)
                    final double pHr = hrOcc == 1 ? 1 - occUnc : occUnc;
                    final double pBr = brOcc == 1 ? 1 - occUnc : occUnc;
                    final double pOx = oxOcc == 1 ? 1 - occUnc : occUnc;

                    // Sampled values
                    final double hrValue = hrOcc == 1 ? hrAbove.sample() : hrBelow.sample();
                    final double brValue = brOcc == 1 ? brAbove.sample() : brBelow.sample();
                    final double oxValue = oxOcc == 1 ? oxBelow.sample() : oxAbove.sample();

                    // Create events
                    final String eventSuffix = "_" + x;
                    final Event hr = new HighHeartRate(pHr, t + x, "hr" + counter + eventSuffix, foi, hrValue, hrSd);
                    final Event br = new HighBreathingRate(pBr, t + x, "br" + counter + eventSuffix, foi, brValue, brSd);
                    final Event ox = new LowOxygenSaturation(pOx, t + x, "ox" + counter + eventSuffix, foi, oxValue, oxSd);

                    // Write to files. Each event on a single line
                    hrFileWriter.write(hr.toString().replaceAll("\\s+", " ") + "\n");
                    brFileWriter.write(br.toString().replaceAll("\\s+", " ") + "\n");
                    oxFileWriter.write(ox.toString().replaceAll("\\s+", " ") + "\n");
                }
                counter++;
            }
            time += 1000;
        }

        // close all writers
        ceFileWriter.close();
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
