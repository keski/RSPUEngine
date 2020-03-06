package generator;

public class LowOxygenSaturation extends Event {

    public LowOxygenSaturation(double prob, long time, String id, String foi, double value, double sd){
        super(prob, time, id, foi, value, sd);
        derived = "LowOxygenSaturation";
        sde = "OxygenSaturation";
    }
}
