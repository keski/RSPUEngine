package generator;

public class HighHeartRate extends Event {
    public HighHeartRate(double prob, long time, String id, String foi, double value, double sd){
        super(prob, time, id, foi, value, sd);
        derived = "HighHeartRate";
        sde = "HeartRate";
    }
}
