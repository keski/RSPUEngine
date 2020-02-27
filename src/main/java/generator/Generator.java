package generator;

import org.apache.commons.math3.distribution.BinomialDistribution;
import org.apache.commons.math3.distribution.UniformRealDistribution;
import se.liu.ida.rspqlstar.function.BayesianNetwork;
import smile.Network;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class Generator {


    public static void main(String[] args) throws IOException {
        BayesianNetwork.init();
        final String root = new File("").getAbsolutePath() + "/";
        final Network net = BayesianNetwork.loadNetwork("", root + "data/experiments/heart-attack.bn");
        net.updateBeliefs();

        // Generate 3 minutes for each config
        final int duration = 3600;
        int[] streamRateList = new int[]{100, 200, 300};
        double[] occUncList = new double[]{0.01, 0.05, 0.10, 0.20};
        double[] attrUncList = new double[]{0.01, 0.05, 0.10, 0.20};

        // occurrence uncertainty
        for(int streamRate : streamRateList){
            for(double occUnc : occUncList) {
                generate(occUnc, 0, streamRate, duration, net);
            }
        }

        // attribute uncertainty
        for(int streamRate : streamRateList) {
            for (double attrUnc : attrUncList) {
                generate(0, attrUnc, streamRate, duration, net);
            }
        }
    }

    public static void generate(double occUnc, double attrUnc, int streamRate, int duration, Network net) throws IOException {
        final int hrThreshold = 100;
        final int brThreshold = 30;
        final int oxThreshold = 90;
        final double attrUni = 0.10; // within 10% of threshold value

        // Uniform distributions for HR, BR and OX
        final BinomialDistribution p = new BinomialDistribution(1, net.getNodeValue("HeartAttackEvent")[0]);
        final UniformRealDistribution hrUni = new UniformRealDistribution(0, attrUni * hrThreshold);
        final UniformRealDistribution brUni = new UniformRealDistribution(0, attrUni * brThreshold);
        final UniformRealDistribution oxUni = new UniformRealDistribution(0, attrUni * oxThreshold);

        // Create file writers. File format is: "<occUnc>-<attrUnc>-<streamRate>-<eventType>.trigs"
        String base = String.format("data/experiments/streams/%s-%s-%s-", occUnc, attrUnc, streamRate);
        final FileWriter haFileWriter = new FileWriter(base + "HAE.trigs");
        final FileWriter hrFileWriter = new FileWriter(base + "HR.trigs");
        final FileWriter brFileWriter = new FileWriter(base + "BR.trigs");
        final FileWriter oxFileWriter = new FileWriter(base + "OX.trigs");

        // Use reference time for streams: 1582757970590 -> 2020-02-26T23:59
        long time = 1582757970590l;
        for(int i=0; i < duration; i++){
            final int sample = p.sample();
            final int x = sample == 1 ? 1 : -1;

            // Create heart attack event (if sample = 1 then the event occurred)
            final HeartAttackEvent ha = new HeartAttackEvent(sample, time + i, i, "person" + i,
                    hrThreshold + x * hrUni.sample(),
                    brThreshold + x * brUni.sample(),
                    oxThreshold - x * oxUni.sample());

            // create SDE events
            double prob = Math.abs(sample - occUnc);
            final Event hr = new HighHeartRateEvent(ha, prob, attrUnc);
            final Event br = new HighBreathingRateEvent(ha, prob, attrUnc);
            final Event ox = new LowOxygenSaturationEvent(ha, prob, attrUnc);
            haFileWriter.write(ha.toString().replaceAll("\\s+", " ") + "\n");
            hrFileWriter.write(hr.toString().replaceAll("\\s+", " ") + "\n");
            brFileWriter.write(br.toString().replaceAll("\\s+", " ") + "\n");
            oxFileWriter.write(ox.toString().replaceAll("\\s+", " ") + "\n");
        }

        // close all writers
        haFileWriter.close();
        hrFileWriter.close();
        brFileWriter.close();
        oxFileWriter.close();

    }

}
