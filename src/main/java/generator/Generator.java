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
    public static final long referenceTime = 1582757970000l;
    public static final float hrLimit = 100f;
    public static final int brLimit = 30;
    public static final int oxLimit = 90;
    private static final Logger logger = Logger.getLogger(Generator.class);

    public static void main(String[] args) throws IOException {
        run(10000, new int[]{1}, new double[]{.2}, new double[]{.2});
    }

    public static void run(int duration, int[] rates, double[] occUncList, double[] attrUncList) throws IOException {
        BayesianNetwork.init();
        final String root = new File("").getAbsolutePath() + "/";
        final Network net = BayesianNetwork.loadNetwork("http://bn", root + "experiments/medical.bn");
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
        net.updateBeliefs();

        // Prior probability
        final double ceProb = net.getNodeValue("COPDExacerbation")[0];
        final BinomialDistribution ceDist = new BinomialDistribution(1, ceProb);

        // Distributions for sampling attribute values
        final double z = pToZ(attrUnc);
        final double hrSd = hrLimit * 0.01; // standard deviation is set to 10% of threshold
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
        float increment = 1000f/rate;
        for(int i=0; i < duration/1000; i++){
            for(int j=0; j < rate; j++) {
                // Sample CE (occurred or not)
                final int ceOcc = ceDist.sample();
                net.setEvidence("COPDExacerbation", new String[]{"F", "T"}[ceOcc]);
                net.updateBeliefs();

                // Time and feature of interest
                final long t = time + (int)(j * increment);
                final String foi = "person" + counter;

                // Ground truth (COPDExacerbation occurred or not)
                final COPDExacerbation ce = new COPDExacerbation(ceOcc, t, "ce" + counter, foi);
                ceFileWriter.write(ce.toString().replaceAll("\\s+", " ") + "\n");

                // Find probabilities for dependent events
                final double hrProb = net.getNodeValue("HighHeartRate")[0];
                final double brProb = net.getNodeValue("HighBreathingRate")[0];
                final double oxProb = net.getNodeValue("LowOxygenSaturation")[0];

                // Occurrence uncertainty sampler
                final BinomialDistribution occUncSampler = new BinomialDistribution(1, 1-occUnc);

                for(int x=0; x < ratio; x++) {
                    // Reference truth values (no added uncertainty yet!)
                    final int hrOcc = new BinomialDistribution(1, hrProb).sample();
                    final int brOcc = new BinomialDistribution(1, brProb).sample();
                    final int oxOcc = new BinomialDistribution(1, oxProb).sample();

                    // Add occurrence uncertainty
                    final double pHr = addOccurrenceUncertainty(hrOcc, occUnc, occUncSampler);
                    final double pBr = addOccurrenceUncertainty(brOcc, occUnc, occUncSampler);
                    final double pOx = addOccurrenceUncertainty(oxOcc, occUnc, occUncSampler);

                    // Physical parameter values
                    final double hrValue;
                    final double brValue;
                    final double oxValue;
                    if(attrUnc == 0){
                        hrValue = hrOcc == 1 ? hrLimit + 1 : hrLimit - 1;
                        brValue = brOcc == 1 ? brLimit + 1 : brLimit - 1;
                        oxValue = oxOcc == 1 ? oxLimit - 1 : oxLimit + 1;
                    } else {
                        hrValue = hrOcc == 1 ? hrAbove.sample() : hrBelow.sample();
                        brValue = brOcc == 1 ? brAbove.sample() : brBelow.sample();
                        oxValue = oxOcc == 1 ? oxBelow.sample() : oxAbove.sample();
                    }

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

    private static double addOccurrenceUncertainty(int occ, double occUnc, BinomialDistribution sampler) {
        // add uncertainty
        double p = occ == 1 ? 1 - occUnc : occUnc;
        // if 1, keep original, otherwise flip observation
        if(sampler.sample() == 0) {
            return 1 - p;
        }
        return p;
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
