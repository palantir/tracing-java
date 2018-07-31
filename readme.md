# Usage

Provides [Zipkin](https://github.com/openzipkin/zipkin)-style call tracing libraries. All `JaxRsClient` and
`Retrofit2Client` instances are instrumented by default. Jersey server instrumentation is enabled via the
`HttpRemotingJerseyFeature` (see above).

By default, the instrumentation forwards trace and span information through HTTP headers, but does not emit completed
spans to a log file or to Zipkin.  Span observers are static (similar to SLF4J appenders) and can be configured as
follows:

```java
// Emit all completed spans to a default SLF4J logger:
Tracer.subscribe("SLF4J" /* user-defined name */, AsyncSlf4jSpanObserver.of(executor));

// No longer emit span events to SLF4J:
Tracer.unsubscribe("SLF4J");
```
Note that span observers are static; a server typically subscribes span observers in its initialization phase.
Libraries should never register span observers (since they can trample observers registered by consumers of the library
whose themselves register observers).

In addition to cross-service call tracing, the `Tracer` library supports intra-thread tracing, for example:
```java
// Record tracing information for expensive doSomeComputation() call:
try {
    Tracer.startSpan("doSomeComputation");
    doSomeComputation();  // may itself invoke cross-service or local traced calls
} finally {
    Tracer.completeSpan(); // triggers all span observers
}
```

Example of how to use the `tracing` library:

```groovy
// build.gradle
dependencies {
  compile "com.palantir.tracing:tracing:$version"
}
```
```java
Tracer.subscribe("SLF4J", AsyncSlf4jSpanObserver.of(executor));
try {
    Tracer.startSpan("doSomeComputation");
    doSomeComputation();
} finally {
    Tracer.completeSpan();
}

```

## Call tracing

Clients and servers propagate call trace ids across JVM boundaries according to the
[Zipkin](https://github.com/openzipkin/zipkin) specification. In particular, clients insert `X-B3-TraceId: <Trace ID>`
HTTP headers into all requests which get propagated by Jetty servers into subsequent client invocations.

# License
This repository is made available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).
