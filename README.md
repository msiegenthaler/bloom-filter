Bloom filter in Scala
=====================

Implements a classic bloom filter with Murmur 3 hashing in scala.
The bloom filter is implemented as an immutable data structure (based on a scala BitSet).

Credits go to the implementation by Ilya Sterin, that served as a baseline.

Usage
-----

    //Bloom Filter with 1000 items and 0.1% false positives
    val myData: Iterable[Array[Byte]]
    val filter = BloomFilter(1000, 0.001d) ++ myData
    
    //new filter that contains an additional element, filter will not change
    val filter2 = filter + Array[Byte](1,2,3,4)
    
    //use it
    if (filter.maybeContains(myElement)) {
      println("Element is contained with 99.9% probability")
    } else {
      println("Element is not contained in the filter.")
    }


Approx. Performance
-------------------
Measured on a 2009 MacBook Pro with a false-positive rate of 0.1%.
See ch.inventsoft.bloomfilter.BloomFilterBenchmark for details or use 'sbt test' to run yourself. 

    [info] Parameters(filter size -> 10): 0.008  => 1'250'000 per second
    [info] Parameters(filter size -> 100): 0.088  => 1'136'364 per second
    [info] Parameters(filter size -> 1000): 1.848  => 541'126 per second
    [info] Parameters(filter size -> 10000): 195.682  => 51'103 per second
    [info]
    [info] ::Benchmark BloomFilter.add (batch) (one-per-size)::
    [info] Parameters(filter size -> 10): 0.005  => 2'000'000 per second
    [info] Parameters(filter size -> 100): 0.033  => 3'030'303 per second
    [info] Parameters(filter size -> 1000): 0.311  => 3'215'434 per second
    [info] Parameters(filter size -> 10000): 3.115  => 3'210'273 per second
    [info]
    [info] ::Benchmark BloomFilter.maybeContains 1000 nonmatching with Byte-Array::
    [info] Parameters(filter size -> 10): 0.145  => 6'896'552 per second
    [info] Parameters(filter size -> 100): 0.154  => 6'493'506 per second
    [info] Parameters(filter size -> 1000): 0.143  => 6'993'007 per second
    [info] Parameters(filter size -> 10000): 0.153  => 6'535'948 per second
    [info]
    [info] ::Benchmark BloomFilter.maybeContains 1000 matching with Byte-Array::
    [info] Parameters(filter size -> 10): 0.15  => 6'666'667 per second
    [info] Parameters(filter size -> 100): 0.166  => 6'024'096 per second
    [info] Parameters(filter size -> 1000): 0.372  => 2'688'172 per second
    [info] Parameters(filter size -> 10000): 0.373  => 2'680'965 per second
