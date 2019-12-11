# RSPUEngine

This document describes the current state of the RSPUEngine as of December, 2019, and
clarifies some important terms and design decisions. The system is based on [RSPQLStarEngine](https://github.com/keski/RSPQLStarEngine),
which implements the RDF\* and SPARQL\* extension of the RSP-QL semantics to represent and query
statement-level metadata. The underlying engine is primarily based on Apache Jena 3.7.0, but implements a different query
execution engine and dataset models.

The RSPUEngine adds support for processing uncertainty. The engine currently supports:
- Modeling probability using the `rspu:distribution` datatype (e.g `"Normal(10,2)"^^rspu:distribution`)
- Operations on probability distributions (leveraging Apache Commons Math)
- Operations on Bayesian Networks (leveraging the SMILE library from [BayesFusion](https://www.bayesfusion.com/])
- Fuzzy logic operations (including the Zadeh operations for conjunction, disjunction, implication, and negation)

The operations are implemented as SPARQL value functions (to be used in SPARQL expressions).

## RSP-QL
RSP-QL is an extension of standard SPARQL, developed by the RSP W3C Community Group (www.w3id.org/rsp)
to support processing and querying of streaming data. RSP-QL provides language constructs for defining
temporal windows that define discrete selections of over potentially infinite RDF streams. We include
in here the construct for executing queries at a fixed interval, as described in
[C-SPARQL](http://larkc.org/wp-content/uploads/2008/01/2010-Querying-RDF-Streams-with-C-SPARQL.pdf).

The query below illustrates some of the core functionality of the language. Here we define a window over
a stream with a width of 10 seconds that slides over the stream every second. The query is executed once
per second.

```
PREFIX : <http://example.org#>
REGISTER STREAM :mystream COMPUTED EVERY PT1S AS 

SELECT ?obs ?value
FROM NAMED WINDOW :w ON :stream [RANGE PT10S STEP PT1S]
WHERE {
   WINDOW :w {
      GRAPH ?g {
         ?obs a :Observation ;
              :hasValue ?value .
      }
   }
}
```

## Extensions to RDF and SPARQL

The basic idea of RDF\* is that a triple can contain an embedded triple in the subject or
object position, e.g.:

```
<<:obs1 :hasValue 1.5>> :confidence "0.99"^^xsd:float
```

Similarly, embedded triples can also be referenced as triple patterns in SPARQL\*, e.g.:

```
PREFIX : <http://example.org#>
SELECT ?value ?confidence
WHERE {
   <<?obs :hasValue ?value>> :confidence ?confidence
}
```

## RSP-QL*

RSP-QL* [1] is an extension of RSP-QL that leverages RDF* and SPARQL* to provide statement-level
annotations. Let's return to the previous RSP-QL query above, but this time we assume that each
value in the stream is associated with a source. The query below shows how this would
be accomplished by using RDF reification, and the second query shows the same query expressed using
RSP-QL*.

[1] Keskisärkkä, R., Blomqvist, E., Lind, L., Hartig, O.: RSP-QL*: Enabling Statement-Level Annotations in RDF Streams. In: Semantic Systems. The Power of AI and Knowledge Graphs (2019)


```
PREFIX : <http://example.org#>
REGISTER STREAM :mystream COMPUTED EVERY PT1S AS 

SELECT ?value ?source
FROM NAMED WINDOW :w ON :stream [RANGE PT10S STEP PT1S]
WHERE {
   WINDOW :w {
      GRAPH ?g {
         ?obs a :Observation ;
              :hasValue ?value .
         [] a rdf:Statement ;
            rdf:subhect ?obs ;
            rdf:predicate a ;
            rdf:object ?value ;
            :source ?source .
       }
   }
}
```

```
PREFIX : <http://example.org#>
REGISTER STREAM :mystream COMPUTED EVERY PT1S AS 

SELECT ?value ?source
FROM NAMED WINDOW :w ON :stream [RANGE PT10S STEP PT1S]
WHERE {
   WINDOW :w {
      GRAPH ?g {
         ?obs a :Observation .
         <<?obs :hasValue ?value>> :source ?source . }
   }
}
```

## Node dictionary
All incoming nodes are mapped to IDs (longs) in a `NodeDictionary`. All mappings
in the node dictionary use IDs where the most significant bit (MSB) of the ID is
not set. For example, an ID with value 5 would be encoded as:

```
0-000000000000000000000000000000000000000000000000000000000000101
```

## Reference dictionary
The `ReferenceDictionary` instead encodes a special type of ID, which maps a a triple to an ID.
The reference dictionary uses IDs where the most significant (MSB) of the ID is set. For
example, a reference ID with value 5 would be encoded as;
```
1-000000000000000000000000000000000000000000000000000000000000101
```

## Variable dictionary
The `VariableDictionary` encodes the variables of the triple patterns in queries as integers. Having
this numeric representation of variables makes lowers the comparison cost compared with the default
Jena implementation.

## Indexes
The main index of the system contains six types of indexes: `GSPO`, `GPOS`, `GOSP`, `SPOG`,
`POSG`, and `OSPG`. Currently, a `TreeMap` is used for storing the encoded quads in the index, but the
implementation can easily be extended to use other types of indexes.

## Decoding
The bulk of the processing of the system is handled by an extension of the Jena execution
environment. For some of the processing, the node IDs have to be decoded into regular
Jena nodes. This is done by either: 1) looking up the corresponding values in the
dictionaries, 2) wrapping the ID-based iterator representation in a `DecodingQuadsIterator`, or 3) 
and iterator with bindings in `DecodeBindingsIterator`. 
