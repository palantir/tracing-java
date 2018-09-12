# tracing-java [![Download](https://api.bintray.com/packages/palantir/releases/tracing-java/images/download.svg) ](https://bintray.com/palantir/releases/tracing-java/_latestVersion)

[Zipkin](https://github.com/openzipkin/zipkin)-style call tracing libraries.

- **com.palantir.tracing:tracing** - The key `Tracer` class, which stores trace information in a ThreadLocal.  Also includes classes for convenient integration with SLF4J and executor services.
- **com.palantir.tracing:tracing-api** - constants and pure data objects
- **com.palantir.tracing:tracing-jaxrs** - utilities to wrap `StreamingOutput` responses with a new trace.
- **com.palantir.tracing:tracing-okhttp3** - `OkhttpTraceInterceptor`, which adds the appropriate headers to outgoing requests.
- **com.palantir.tracing:tracing-jersey** - `TraceEnrichingFilter`, a jaxrs filter which reads headers from incoming requests and writes headers to outgoing responses.  A traceId is stored in the jaxrs request context under the key `com.palantir.tracing.traceId`.

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

## Compatibility Check Against Remoting Tracer
Prior to [http-remoting 3.43.0](https://github.com/palantir/http-remoting/releases/tag/3.43.0), remoting tracer has its
own ThreadLocal to keep track of the current trace. This would result two ThreadLocals independently tracking their own
 traces if any older version of `com.palantir.remoting3.tracing.Trace` were used in conjunction with 
 `com.palantir.tracing.Tracer`. To prevent consumers from getting into this bad state, `com.palantir.tracing.Tracer` will
 do compatibility check against remoting Tracer on class load and throws an exception if any older version of 
 `com.palantir.remoting3.tracing.Trace` is found in the classpath.
 
If you find it necessary to have both the older version of `com.palantir.remoting3.tracing.Trace` and 
the `com.palantir.tracing.Tracer` in the classpath, you can disable this compatibility check by setting the 
`tracing.remoting.compat.check.enabled` system property to `false`. 

## License

This repository is made available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).
