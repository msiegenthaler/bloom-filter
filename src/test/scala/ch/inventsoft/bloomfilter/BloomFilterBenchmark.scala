package ch.inventsoft.bloomfilter

import org.scalameter.api._
import java.nio.ByteBuffer

object BloomFilterBenchmark extends PerformanceTest {
  lazy val executor = LocalExecutor(
    new Executor.Warmer.Default,
    Aggregator.median,
    new Measurer.IgnoringGC with Measurer.OutlierElimination)
  lazy val reporter = new PerSecondReporter
  lazy val persistor = Persistor.None

  def valueForInt(v: Int) = ByteBuffer.allocate(4).putInt(v).array
  val v_non = (1 to 1000).map(_ * 2 + 1).map(valueForInt).toVector
  val v_match = (1 to 1000).map(_ * 2).map(valueForInt).toVector

  val filterSize = Gen.exponential("filter size")(10, 10000, 10)
  def filterFor(size: Int) = BloomFilter(size, 0.001d)
  val filterConfigs = filterSize.map(filterFor)
  val filters = filterSize.map(s => filterFor(s) ++ (1 to s).map(_ * 2).map(valueForInt))
  val configAndSet = filterSize.map(s => (BloomFilterConfig.forFalsePositives(s, 0.001d), (1 to s).map(valueForInt)))

  val thousandFilledFilters = filterSize.map { size =>
    (1 to 1000).map(v => filterFor(size) + valueForInt(v))
  }

  performance of "BloomFilter" in {
    measure method "add (one-per-size)" in {
      using(configAndSet) in { v =>
        val (config, set) = v
        set.foldLeft(BloomFilter(config))(_ + _)
      }
    }
    measure method "add (batch) (one-per-size)" in {
      using(configAndSet) in { v =>
        val (config, set) = v
        BloomFilter(config) ++ set
      }
    }
    measure method "maybeContains 1000 nonmatching with Byte-Array" in {
      using(filters) in { filter =>
        v_non.foreach(filter.maybeContains)
      }
    }
    measure method "maybeContains 1000 matching with Byte-Array" in {
      using(filters) in { filter =>
        v_match.foreach(filter.maybeContains)
      }
    }
    measure method "maybeContains 1000 nonmatching with BloomFilterCheck" in {
      using(filters.map(f => (f, v_non.map(BloomFilterCheck(_, f.config))))) in { v =>
        val (filter, values) = v
        values.foreach(filter.maybeContains)
      }
    }
    measure method "maybeContains 1000 matching with BloomFilterCheck" in {
      using(filters.map(f => (f, v_match.map(BloomFilterCheck(_, f.config))))) in { v =>
        val (filter, values) = v
        values.foreach(filter.maybeContains)
      }
    }
    measure method "maybeContains 1000 nonmatching with BloomFilter" in {
      using(filters.map(f => (f, v_non.map(BloomFilter(f.config).+)))) in { v =>
        val (filter, values) = v
        values.foreach(filter.maybeContains)
      }
    }
    measure method "maybeContains 1000 matching with BloomFilter" in {
      using(filters.map(f => (f, v_match.map(BloomFilter(f.config).+)))) in { v =>
        val (filter, values) = v
        values.foreach(filter.maybeContains)
      }
    }
    measure method "approxNumberOfItems (1000)" in {
      using(filters) in { filter =>
        (1 to 1000).foreach(_ => filter.approxNumberOfItems)
      }
    }
    measure method "union (1000)" in {
      using(thousandFilledFilters) in { filters =>
        filters.reduce(_ union _)
      }
    }
    measure method "intersect (1000)" in {
      using(filters) in { filter =>
        (1 to 1000).foreach(_ => filter intersect filter)
      }
    }
  }
}

class PerSecondReporter extends Reporter {
  import org.scalameter._
  import org.scalameter.utils.Tree
  import org.scalameter.reporting._

  var first = true

  def report(result: CurveData, persistor: Persistor) {
    if (first) {
      for ((key, value) <- result.context.properties.filterKeys(Context.machine.properties.keySet.contains).toSeq.sortBy(_._1)) {
        log(s"$key: $value")
      }
      log("")
      first = false
    }

    log(s"::Benchmark ${result.context.scope}::")
    // output measurements
    for (measurement <- result.measurements) {
      val factor =
        if (result.context.scope.contains("1000")) 1000
        else if (result.context.scope.contains("one-per-size")) measurement.params[Int]("filter size").toInt
        else 1
      val perSecond = "%,.0f".format(1000d * factor / measurement.value)
      log(s"${measurement.params}: ${measurement.value}  => $perSecond per second")
    }

    // add a new line
    log("")
  }
  def report(result: Tree[CurveData], persistor: Persistor) = true
}
