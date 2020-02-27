package generator;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Event {
    public double prob;
    public long time;
    public Date dateTime;
    public int id;
    public String foi;
    public String eid;
    public String derived = null;
    public String sde = "Event";
    public double value = -1;
    public double stddev = -1;

    public final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    public Event(double prob, long time, int id, String foi, double value, double sttdev){
        this.prob = prob;
        this.time = time;
        this.id = id;
        this.foi = foi;
        this.value = value;
        this.stddev = sttdev;
        dateTime = new Date(time);
    }

    public String toString(){
        String event = "";
        event += String.format("<g_%s> {\n\t", eid);
        if(derived != null){
            event += String.format( "<< <%s> a :%s >> rspu:probability %s .\n\t", eid, derived, prob);
        }
        if(stddev > 0){
            event += String.format( "<< <%s> sosa:hasSimpleResult %s >> rspu:error " +
                    "\"Normal(0,%s)\"^^rspu:distribution .\n\t", eid, value, stddev * value);
        }

        event += String.format("<%s> a :%s ;\n\t\t", eid, sde);
        if(stddev <= 0){
            event += String.format("sosa:hasSimpleResult %s ;\n\t\t", value);
        }
        event += String.format("sosa:hasFeatureOfInterest <%s> .\n}\n", foi);

        event += String.format("<g_%s> prov:generatedAtTime \"%s\"^^xsd:dateTime .", eid, df.format(dateTime));
        return event;
    }
}
