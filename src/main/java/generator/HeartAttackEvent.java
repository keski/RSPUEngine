package generator;

import java.util.Date;

public class HeartAttackEvent extends Event {
    public final String eid;
    public final double hr;
    public final double br;
    public final double ox;

    public HeartAttackEvent(double probability, long time, int id, String foi, double hr, double br, double ox){
        super(probability, time, id, foi, -1, 0);
        this.eid = "ha" + id;
        this.hr = hr;
        this.br = br;
        this.ox = ox;
        derived = "HeartAttackEvent";
    }

    public String toString(){
        String event = "";
        event += String.format("<g_%s> {\n\t", eid);
        event += String.format( "<< <%s> a :%s >> rspu:probability %s .\n\t", eid, derived, prob);
        event += String.format("sosa:hasFeatureOfInterest <%s> ;\n\t\t", foi);
        event += String.format(":hasOx %s ;\n\t\t", ox);
        event += String.format(":hasBr %s ;\n\t\t", br);
        event += String.format(":hasHr %s .\n}\n", hr);
        event += String.format("<g_%s> prov:generatedAtTime \"%s\"^^xsd:dateTime .", eid, df.format(dateTime));
        return event;
    }


}
