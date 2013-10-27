package support

import java.nio.ByteBuffer
import org.specs2.mutable.Specification
import org.specs2.specification.Scope

class BloomFilterSpec extends Specification {
  "BloomFilterConfig with capacity 1438 and 10 hashes" should {
    "have p of 0.001 on 100 items" in {
      BloomFilterConfig(1438, 10).falsePositives(100) must beCloseTo(0.001d, 1E-4)
    }
  }

  "BloomFilterConfig with capacity 9586 and 7 hashes" should {
    "have p of 0.01 with 1000 items" in {
      BloomFilterConfig(9586, 7).falsePositives(1000) must beCloseTo(0.01d, 1E-4)
    }
  }

  "BloomFilterConfig for 1000 items with p=0.01" should {
    "have capacity 9586 and 7 hashes" in {
      val c = BloomFilterConfig.forFalsePositives(1000, 0.01d)
      c.capacity must_== 9586
      c.hashCount must_== 7
    }
  }

  "BloomFilterConfig" should {
    "be consistent" in {
      (100 to 10000 by 10).map { n =>
        BloomFilterConfig.forFalsePositives(n, 0.1d).falsePositives(n) must beCloseTo(0.1d, 1E-2)
        BloomFilterConfig.forFalsePositives(n, 0.01d).falsePositives(n) must beCloseTo(0.01d, 1E-3)
        BloomFilterConfig.forFalsePositives(n, 0.001d).falsePositives(n) must beCloseTo(0.001d, 1E-4)
      }
    }
  }

  trait xvalues extends Scope {
    def value(d: Int) = ByteBuffer.allocate(4).putInt(d).array
  }
  trait hundred extends xvalues {
    val filter = BloomFilter(100, 0.001d) ++ (1 to 100).map(value)
  }
  trait ten extends xvalues {
    val filter = BloomFilter(100, 0.001d) ++ (1 to 10).map(value)
  }
  trait union extends ten {
    val filter2 = BloomFilter(100, 0.001d) ++ (8 to 15).map(value)
    val union = filter union filter2
  }
  trait intersect extends ten {
    val filter2 = BloomFilter(100, 0.001d) ++ (8 to 15).map(value)
    val intersect = filter intersect filter2
  }

  "BloomFilter that contains the items 1..100" should {
    "return true maybeContains 1..100" in new hundred {
      (1 to 100).map(value).map { v =>
        filter.maybeContains(v) must beTrue
      }
    }
    "have not have more than 120 false positive in 101 to 10000" in new hundred {
      val count = (101 to 100000).map(value).filter(filter.maybeContains).size
      count must beLessThanOrEqualTo(120)
    }
    "have approx item count of 100+-2" in new hundred {
      filter.approxNumberOfItems must beCloseTo(100d, 2)
    }

    "return true maybeContains 1..100 when using a BloomFilter" in new hundred {
      (1 to 100).map(value).map(BloomFilter(filter.config) + _).map { v =>
        filter.maybeContains(v) must beTrue
      }
    }
    "have not have more than 120 false positive in 101 to 10000 using BloomFilter as check" in new hundred {
      val count = (101 to 100000).map(value).map(BloomFilter(filter.config) + _).filter(filter.maybeContains).size
      count must beLessThanOrEqualTo(120)
    }

    "return true maybeContains 1..100 when using a BloomFilterCheck" in new hundred {
      (1 to 100).map(value).map(BloomFilterCheck(_, filter.config)).map { v =>
        filter.maybeContains(v) must beTrue
      }
    }
    "have not have more than 120 false positive in 101 to 10000 using BloomFilter as check" in new hundred {
      val count = (101 to 100000).map(value).map(BloomFilterCheck(_, filter.config)).filter(filter.maybeContains).size
      count must beLessThanOrEqualTo(120)
    }

    "is the same as one filled with individual items" in new hundred {
      val data = (1 to 100).map(value)
      val f2 = data.foldLeft(BloomFilter(filter.config))(_ + _)
      f2 must_== filter
    }
  }

  "BloomFilter that contains [1..10] unioned with [8..15]" should {
    "contain approx 15 items" in new union {
      union.approxNumberOfItems must beCloseTo(15d, 1)
    }
    "return true for maybeContains 1..15" in new union {
      (1 to 15).map(value).map { v =>
        union.maybeContains(v) must beTrue
      }
    }
    "have not have more than 5 false positive in 16 to 10000" in new union {
      val count = (16 to 100000).map(value).filter(union.maybeContains).size
      count must beLessThanOrEqualTo(5)
    }
  }

  "BloomFilter that contains [1..10] intersected with [8..15]" should {
    "contain approx 3 items" in new intersect {
      intersect.approxNumberOfItems must beCloseTo(3d, 1)
    }
    "return true for maybeContains 8..10" in new intersect {
      (8 to 10).map(value).map { v =>
        intersect.maybeContains(v) must beTrue
      }
    }
    "have not have more than 5 false positive in 0..7 and 11 to 10000" in new intersect {
      val count = ((0 to 7) ++ (11 to 100000)).map(value).filter(intersect.maybeContains).size
      count must beLessThanOrEqualTo(5)
    }
  }

  "BloomFilter" should {
    "be fast to calculate batch adds (>3Mio adds / s)" in new xvalues {
      val config = BloomFilterConfig.forFalsePositives(1000, 0.001d)
      val datas = (1 to 1000).map(value)
      def testPerf(count: Int) = {
        val t0 = System.nanoTime
        (1 to count).foreach { i =>
          val filter = BloomFilter(config) ++ datas
          //use filter so JIT does not trick us
          filter.config must_== config
        }
        val d = System.nanoTime - t0
        count.toDouble * datas.size * 1E9 / d
      }
      testPerf(1000)
      val runs = 4
      val persecond = (1 to runs).map(_ => testPerf(1000)).sum / runs
      println(s"BloomFilter additions per second: $persecond (batches of ${datas.size})")
      persecond must beGreaterThan(3000000d)
    }
    "be fast to calculate single adds (>600k adds / s)" in new xvalues {
      val config = BloomFilterConfig.forFalsePositives(1000, 0.001d)
      val datas = (1 to 1000).map(value)
      def testPerf(count: Int) = {
        val t0 = System.nanoTime
        (1 to count).foreach { i =>
          val filter = datas.foldLeft(BloomFilter(config))(_ + _)
          //use filter so JIT does not trick us
          filter.config must_== config
        }
        val d = System.nanoTime - t0
        count.toDouble * datas.size * 1E9 / d
      }
      testPerf(100)
      val runs = 4
      val persecond = (1 to runs).map(_ => testPerf(100)).sum / runs
      println(s"BloomFilter additions per second $persecond (single adds)")
      persecond must beGreaterThan(600000d)
    }
    "be fast to compare (>4Mio compares / s)" in new xvalues {
      val config = BloomFilterConfig.forFalsePositives(1000, 0.001d)
      val filter = BloomFilter(config) ++ (1 to 1000).map(value)
      val inputsTrue = (1 to 1000).map(value).toVector
      val inputsFalse = (1001 to 2000).map(value).toVector
      def testPerf(count: Int) = {
        val t0 = System.nanoTime
        (1 to count).foreach { i =>
          inputsTrue.foreach(v => require(filter.maybeContains(v), "Failed assertion"))
          require(inputsFalse.view.filter(filter.maybeContains).size < 10, "Too many false positives")
        }
        val d = System.nanoTime - t0
        (inputsTrue.size + inputsFalse.size).toDouble * count * 1E9 / d
      }
      testPerf(1000)
      val runs = 4
      val persecond = (1 to runs).map(_ => testPerf(1000)).sum / runs
      println(s"BloomFilter compares per second $persecond")
      persecond must beGreaterThan(4000000d)
    }
    "be very fast to compare with precalculated checks (>6Mio compares / s)" in new xvalues {
      val config = BloomFilterConfig.forFalsePositives(1000, 0.001d)
      val filter = BloomFilter(config) ++ (1 to 1000).map(value)
      val inputsTrue = (1 to 1000).map(value).map(BloomFilterCheck(_, config)).toVector
      val inputsFalse = (1001 to 2000).map(value).map(BloomFilterCheck(_, config)).toVector
      def testPerf(count: Int) = {
        val t0 = System.nanoTime
        (1 to count).foreach { i =>
          inputsTrue.foreach(v => require(filter.maybeContains(v), "Failed assertion"))
          require(inputsFalse.view.filter(filter.maybeContains).size < 10, "Too many false positives")
        }
        val d = System.nanoTime - t0
        (inputsTrue.size + inputsFalse.size).toDouble * count * 1E9 / d
      }
      testPerf(1000)
      val runs = 3
      val persecond = (1 to runs).map(_ => testPerf(1000)).sum / runs
      println(s"BloomFilterCheck compares per second $persecond")
      persecond must beGreaterThan(6000000d)
    }
    "be ok fast to checks contains to other bloom filter (>300k compares / s)" in new xvalues {
      val config = BloomFilterConfig.forFalsePositives(1000, 0.001d)
      val filter = BloomFilter(config) ++ (1 to 1000).map(value)
      val inputsTrue = (1 to 1000).map(value).map(BloomFilter(config) + _).toVector
      val inputsFalse = (1001 to 2000).map(value).map(BloomFilter(config) + _).toVector
      def testPerf(count: Int) = {
        val t0 = System.nanoTime
        (1 to count).foreach { i =>
          inputsTrue.foreach(v => require(filter.maybeContains(v), "Failed assertion"))
          require(inputsFalse.view.filter(filter.maybeContains).size < 10, "Too many false positives")
        }
        val d = System.nanoTime - t0
        (inputsTrue.size + inputsFalse.size).toDouble * count * 1E9 / d
      }
      testPerf(100)
      val runs = 3
      val persecond = (1 to runs).map(_ => testPerf(100)).sum / runs
      println(s"BloomFilter contains with other BloomFilter per second $persecond")
      persecond must beGreaterThan(300000d)
    }
  }
}
