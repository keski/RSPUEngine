package generator;

public class HighHeartRateEvent extends Event {
    public HighHeartRateEvent(COPDExacerbation e, double prob, double sd){
        this(prob, e.time, e.id, e.foi, e.hr, sd);
    }

    public HighHeartRateEvent(double prob, long time, int id, String foi, double value, double sd){
        super(prob, time, id, foi, value, sd);
        this.eid = "hr" + id;
        derived = "HighHeartRate";
        sde = "HeartRate";
    }
}
