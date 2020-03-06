package generator;

import java.util.Date;

public class HeartAttackEvent extends Event {
    public final String eid;
    public final double hr;
    public final double br;
    public final double ox;
    public boolean occurred;

    public HeartAttackEvent(double prob, long time, int id, String foi, double hr, double br, double ox){
        super(prob, time, id, foi, -1, 0);
        this.eid = "ha" + id;
        this.hr = hr;
        this.br = br;
        this.ox = ox;
        derived = "HeartAttackEvent";
    }

    public String toString(){
        // For heart attack events, if prob is 0 the event did not occur, if prob is 1 it did occur.
        String event = "";
        event += String.format("<g_%s> {\n\t", eid);
        event += String.format( "<< <%s> rdf:type :%s >> rspu:probability %s .\n\t", eid, derived, prob);
        event += String.format("<%s> sosa:featureOfInterest <%s> ;\n\t\t", eid, foi);
        event += String.format(":hasOx %s ;\n\t\t", ox);
        event += String.format(":hasBr %s ;\n\t\t", br);
        event += String.format(":hasHr %s .\n}\n", hr);
        event += String.format("<g_%s> prov:generatedAtTime \"%s\"^^xsd:dateTime .", eid, df.format(dateTime));
        return event;
    }


}
