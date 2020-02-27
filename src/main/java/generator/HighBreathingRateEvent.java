package generator;

public class HighBreathingRateEvent extends Event {
    public HighBreathingRateEvent(HeartAttackEvent e, double prob, double stddev){
        this(prob, e.time, e.id, e.foi, e.hr, stddev);
    }

    public HighBreathingRateEvent(double probability, long time, int id, String foi, double value, double stddev){
        super(probability, time, id, foi, value, stddev);
        this.eid = "br" + id;
        derived = "HighBreathingRateEvent";
        sde = "BreathingRateEvent";
    }
}
