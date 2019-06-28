<p align="right">
<a href="https://autorelease.general.dmz.palantir.tech/palantir/tracing-java"><img src="https://img.shields.io/badge/Perform%20an-Autorelease-success.svg" alt="Autorelease"></a>
</p>

# tracing-java [![Download](https://api.bintray.com/packages/palantir/releases/tracing-java/images/download.svg) ](https://bintray.com/palantir/releases/tracing-java/_latestVersion)

[Zipkin](https://github.com/openzipkin/zipkin)-style call tracing libraries.

- **com.palantir.tracing:tracing** - The key `Tracer` class, which stores trace information in a ThreadLocal.  Also includes classes for convenient integration with SLF4J and executor services.
- **com.palantir.tracing:tracing-api** - constants and pure data objects
- **com.palantir.tracing:tracing-jaxrs** - utilities to wrap `StreamingOutput` responses with a new trace.
- **com.palantir.tracing:tracing-okhttp3** - `OkhttpTraceInterceptor`, which adds the appropriate headers to outgoing requests.
- **com.palantir.tracing:tracing-jersey** - `TraceEnrichingFilter`, a jaxrs filter which reads headers from incoming requests and writes headers to outgoing responses.  A traceId is stored in the jaxrs request context under the key `com.palantir.tracing.traceId`.
- **com.palantir.tracing:tracing-undertow** - `TracedOperationHandler`, an Undertow handler reads headers from incoming requests and writes headers to outgoing responses.

Clients and servers propagate call trace ids across JVM boundaries according to the
[Zipkin](https://github.com/openzipkin/zipkin) specification. In particular, clients insert `X-B3-TraceId: <Trace ID>`
HTTP headers into all requests which get propagated by Jetty servers into subsequent client invocations.

## Usage

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
    doSomeComputation();  // may itself invoke cross-service or local traced calls
} finally {
    Tracer.completeSpan(); // triggers all span observers
}
```

The example above demonstrates how the `Tracer` library supports intra-thread tracing.

## Logging with SLF4J

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

## License

This repository is made available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).
