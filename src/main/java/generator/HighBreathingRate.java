package generator;

public class HighBreathingRate extends Event {

    public HighBreathingRate(double prob, long time, String id, String foi, double value, double sd){
        super(prob, time, id, foi, value, sd);
        derived = "HighBreathingRate";
        sde = "BreathingRate";
    }
}
