package generator;

public class HighHeartRateEvent extends Event {
    public HighHeartRateEvent(HeartAttackEvent e, double prob, double stddev){
        this(prob, e.time, e.id, e.foi, e.hr, stddev);
    }

    public HighHeartRateEvent(double prob, long time, int id, String foi, double value, double stddev){
        super(prob, time, id, foi, value, stddev);
        this.eid = "hr" + id;
        derived = "HighHeartRateEvent";
        sde = "HeartRateEvent";
    }
}
