## FILTER queries
* Q1: `For all tunnel segments, generate a notification if an oxygen sensor report an oxygen
concentration below 0.18 with a probability greater than $P and the temperature is reported to be above 30.`

* Q2: `Q1 with inversed window order.`

* Q3: `For all tunnel segments, generate a notification if both oxygen sensors report an oxygen
concentration below 0.18 with a probability greater than $P, and both temperature sensors report
a temperature above 30 with a probability greater than $P.`

* Q4: `Q3 with inversed window order.`

* Q5: `For all tunnel segments, generate a notification if the two values reported by the two oxygen sensors at a
a location report values that differ with a probability greater than $P, while the temperature at the location is reported
to be above 30.`

* Q6: `Q5 with inversed window order.`

# BIND only queries
* Q7: `Q1 but BIND probability to var instead of filtering on selectivity and apply a regular
filter on the reported value.`
* Q8: `Q7 with inversed window order.`
* Q9: `Q2 but BIND probability to var instead of filtering on selectivity.`
* Q10: `Q8 with inversed window order.`
* Q11: `Q3 but BIND the probabilities to variables instead of filtering on selectivity.`
* Q12: `Q11 with inversed window order  .`
