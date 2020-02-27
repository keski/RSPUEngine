package generator;

public class LowOxygenSaturationEvent extends Event {
    public LowOxygenSaturationEvent(HeartAttackEvent e, double prob, double stddev){
        this(prob, e.time, e.id, e.foi, e.hr, stddev);
    }

    public LowOxygenSaturationEvent(double probability, long time, int id, String foi, double value, double stddev){
        super(probability, time, id, foi, value, stddev);
        eid = "ox" + id;
        derived = "LowOxygenSaturationEvent";
        sde = "OxygenSaturationEvent";
    }
}
