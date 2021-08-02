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
- **com.palantir.tracing:tracing-test-utils** - JUnit classes to render traces and also allow snapshot testing them.

Clients and servers propagate call trace ids across JVM boundaries according to the
[Zipkin](https://github.com/openzipkin/zipkin) specification. In particular, clients insert `X-B3-TraceId: <Trace ID>`
HTTP headers into all requests which get propagated by Jetty servers into subsequent client invocations. We enhance
the Zipkin spec in one regard; with outgoing traces we additionally send an `X-OrigSpanId: <Originating Span ID>`
header which enables request logs to be considered a useful subset of the trace events, even on unsampled requests.

## Usage

Example of how to use the `tracing` library:

```groovy
// build.gradle
dependencies {
  compile "com.palantir.tracing:tracing:$version"
}
```

```java
try (CloseableTracer span = CloseableTracer.startSpan("do work")) {
    Thread.sleep(100);
    doWork();
}
```

At the end of this try-with-resources block, any registered SpanObservers will be notified with a single immutable `Span` object. The above example demonstrates how to instrument chunks of code that start and finish on one thread. For cross-thread tracing, see `DetachedSpan`.

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


## tracing-test-utils

You can set up 'snapshot testing' by adding the `@TestTracing` annotation to a test method (this requires JUnit 5).

```diff
 import org.junit.jupiter.api.Test;
 import com.palantir.tracing.TestTracing;

 public class MyTest {
     @Test
+    @TestTracing(snapshot = true)
     public void foo() {
     }
 }
```

When you run this test for the first time, it will capture all spans and write them to a file `src/test/resources/tracing/MyTest/foo`, which should be checked-in to Git.  This file will be used as a 'golden master', and all future runs will be compared against it.

```
{"traceId":"7e1014caf8a7278e","parentSpanId":"972f9b3a09431b67","spanId":"f701b7f815176ec2","operation":"healthcheck: SERVICE_DEPENDENCY","startTimeMicroSeconds":1566902887342052,"durationNanoSeconds":20377272,"metadata":{}}
...
```

If your production code changes and starts producing different spans, the test will fail and render two HTML visualizations: `expected.html` and `actual.html`.

Snapshot-testing is not available in JUnit4, but you can still see a HTML visualization of your traces using the `RenderTracingRule`:

```diff
 import org.junit.Test;
 import com.palantir.tracing.RenderTracingRule;

 public class MyTest {

+    @Rule
+    public final RenderTracingRule rule = new RenderTracingRule();

     @Test
     public void foo() {
     }
 }
```

## License

This repository is made available under the [Apache 2.0 License](http://www.apache.org/licenses/LICENSE-2.0).
