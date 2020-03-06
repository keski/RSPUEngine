package generator;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Event {
    public double prob;
    public long time;
    public Date dateTime;
    public String id;
    public String foi;
    public String derived = null;
    public String sde = "Event";
    public double value;
    public double sd;

    public final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS");

    /**
     * @param prob The probability that the derived event occurred
     * @param time The observation time of the event
     * @param id The ID of the event.
     * @param foi The feature of interest.
     * @param value The value (if variance > 0 then this is the mean of the distribution)
     * @param sd The standard deviation associated with the value
     */
    public Event(double prob, long time, String id, String foi, double value, double sd){
        this.prob = prob;
        this.time = time;
        this.id = id;
        this.foi = foi;
        this.value = value;
        this.sd = sd;
        dateTime = new Date(time);
    }

    public String toString(){
        String event = "";
        event += String.format("<g_%s> {\n\t", id);
        if(derived != null){
            event += String.format( "<< <%s> rdf:type :%s >> rspu:probability %s .\n\t", id, derived, prob);
        }
        if(sde != null) {
            event += String.format("<%s> rdf:type :%s .\n\t", id, sde);
        }
        if(sd > 0){
            event += String.format( "<< <%s> sosa:hasSimpleResult %.1f >> rspu:error " +
                    "\"Normal(0,%.1f)\"^^rspu:distribution .\n\t", id, value, Math.pow(sd, 2));
        } else {
            event += String.format("<%s> sosa:hasSimpleResult %.1f .\n\t", id, value);
        }
        event += String.format("<%s> sosa:featureOfInterest <%s> .\n}\n", id, foi);
        event += String.format("<g_%s> prov:generatedAtTime \"%s\"^^xsd:dateTime .", id, df.format(dateTime));
        return event;
    }
}
