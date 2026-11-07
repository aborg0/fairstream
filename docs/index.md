## fairstream

[Simple fair and terminating backtracking Monad Transformer](https://okmij.org/ftp/Computation/monads.html#fair-bt-stream)

### FS2 Cancellation and Output Metrics

`FairT` does not define its own cancellation token. When converted with `toFs2`,
cancellation follows FS2 and the capabilities of the supplied effect. You can
race computations using effect-runtime or FS2 concurrency; cancellation stops
future stream evaluation at cancelable effect boundaries, but does not preempt
synchronous or non-cancelable work already running.

`OutputMetrics` counts values pulled through a tagged stream region. Tags with
the same name accumulate, and nested tracked regions each count the values that
pass through them. Snapshots are immutable maps returned in the effect, so they
can be queried concurrently from another fiber.

```scala
import com.codiff.fairstream.fs2.OutputMetrics
import com.codiff.fairstream.fs2.syntax._

for {
	metrics <- OutputMetrics.create[IO]
	results <- metrics.track("solutions")(compatible.toFs2).take(5).compile.toList
	snapshot <- metrics.snapshot
} yield (results, snapshot.getOrElse("solutions", 0L))
```

Metrics count outputs passing through `track`; they do not count internal fair
choice or suspension steps.
