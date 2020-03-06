package generator;

public class COPDExacerbation extends Event {
    public COPDExacerbation(double prob, long time, String id, String foi){
        super(prob, time, id, foi, -1, 0);
        derived = "COPDExacerbation";
    }

    public String toString(){
        // For heart attack events, if prob is 0 the event did not occur, if prob is 1 it did occur.
        String event = "";
        event += String.format("<g_%s> {\n\t", id);
        event += String.format( "<< <%s> rdf:type :%s >> rspu:probability %s .\n\t", id, derived, prob);
        event += String.format("<%s> sosa:featureOfInterest <%s> . }\n\t\t", id, foi);
        event += String.format("<g_%s> prov:generatedAtTime \"%s\"^^xsd:dateTime .", id, df.format(dateTime));
        return event;
    }


}
