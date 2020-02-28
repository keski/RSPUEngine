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
    public double value;
    public double stddev;

    public final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    /**
     * @param prob The probability that the derived event occurred
     * @param time The observation time of the event
     * @param id The ide counter of the event.
     * @param foi The feature of interest.
     * @param value The value (if variance > 0 then this is the mean of the distribution)
     * @param stddev The standard deviation associated with the value
     */
    public Event(double prob, long time, int id, String foi, double value, double stddev){
        this.prob = prob;
        this.time = time;
        this.id = id;
        this.foi = foi;
        this.value = value;
        this.stddev = stddev;
        dateTime = new Date(time);
    }

    public String toString(){
        String event = "";
        event += String.format("<g_%s> {\n\t", eid);
        if(derived != null){
            event += String.format( "<< <%s> rdf:type :%s >> rspu:probability %s .\n\t", eid, derived, prob);
        }
        if(sde != null) {
            event += String.format("<%s> rdf:type :%s .\n\t", eid, sde);
        }
        if(stddev > 0){
            event += String.format( "<< <%s> sosa:hasSimpleResult %s >> rspu:error " +
                    "\"Normal(0,%s)\"^^rspu:distribution .\n\t", eid, value, stddev);
        } else {
            event += String.format("<%s> sosa:hasSimpleResult %s .\n\t", eid, value);
        }
        event += String.format("<%s> sosa:featureOfInterest <%s> .\n}\n", eid, foi);
        event += String.format("<g_%s> prov:generatedAtTime \"%s\"^^xsd:dateTime .", eid, df.format(dateTime));
        return event;
    }
}
