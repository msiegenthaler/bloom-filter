package support

import scala.collection.immutable.BitSet
import scala.util.hashing.MurmurHash3

// Based on the implementation by Ilya Sterin

case class BloomFilter private(bits: BitSet, config: BloomFilterConfig) {
  def +(data: Array[Byte]): BloomFilter = this ++ List(data)

  def ++(data: Iterable[Array[Byte]]) = {
    val newBits = scala.collection.mutable.BitSet.fromBitMask(bits.toBitMask)
    data.foreach(d => config.foreachHashValue(d, newBits += _))
    BloomFilter(newBits.toImmutable, config)
  }

  def ++(other: BloomFilter) = union(other)

  def union(other: BloomFilter) = {
    require(config == other.config, "Same configuration required")
    new BloomFilter(bits | other.bits, config)
  }

  def intersect(other: BloomFilter) = {
    require(config == other.config, "Same configuration required")
    new BloomFilter(bits & other.bits, config)
  }


  /** True if this filter might contain the element. False if the element is not contained for sure. */
  def maybeContains(data: Array[Byte]): Boolean = {
    config.forallHashValue(data, bits.contains)
  }
  def maybeContains(other: BloomFilter): Boolean = {
    require(other.config == config, "Non-matching configs")
    other.bits.subsetOf(bits)
  }
  def maybeContains(candidate: BloomFilterCheck): Boolean = {
    require(candidate.config == config, "Non-matching configs")
    candidate.checkBitSet(bits)
  }

  def approxNumberOfItems = {
    -(config.capacity.toDouble * Math.log(1d - (bits.size.toDouble / config.capacity))) / config.hashCount
  }

  override def toString = s"BloomFilter($config)"
}
object BloomFilter {
  def apply(config: BloomFilterConfig): BloomFilter = BloomFilter(BitSet.empty, config)
  def apply(expectedItemCount: Int, falsePositiveProbability: Double): BloomFilter = {
    val config = BloomFilterConfig.forFalsePositives(expectedItemCount, falsePositiveProbability)
    BloomFilter(config)
  }
}

/** Allows for faster checking against multiple bloom filters. */
case class BloomFilterCheck private(config: BloomFilterConfig, private val values: Traversable[Int]) {
  private[support] def checkBitSet(bits: BitSet) = values.forall(bits.contains)
  override def toString = s"BloomFilterCheck(${config})"
}
object BloomFilterCheck {
  def apply(data: Array[Byte], config: BloomFilterConfig): BloomFilterCheck = {
    BloomFilterCheck(data :: Nil, config)
  }
  def apply(datas: TraversableOnce[Array[Byte]], config: BloomFilterConfig): BloomFilterCheck = {
    val values = scala.collection.mutable.Set.empty[Int]
    datas.foreach { d =>
      val hash1 = MurmurHash3.bytesHash(d, 0)
      val hash2 = MurmurHash3.bytesHash(d, hash1)
      val capacity = config.capacity
      (0 until config.hashCount).foreach { i =>
        values += Math.abs((hash1 + i * hash2) % capacity)
      }
    }
    BloomFilterCheck(config, values.toVector)
  }
}

case class BloomFilterConfig(/** Number of bits in the filter. */
                             capacity: Int,

                             /** Number of hash functions. */
                             hashCount: Int) {
  /** Probability of false positives [0,1]. */
  def falsePositives(numberOfItemsInFilter: Int) = {
    Math.pow(1 - Math.exp(-hashCount.toDouble * (numberOfItemsInFilter + 0.5) / (capacity - 1)), hashCount)
  }

  private[support] def foreachHashValue(data: Array[Byte], f: Int => Unit): Unit = {
    // see "Less Hashing, Same Performance" by Adam Kirsch and Michael Mitzenmacher
    val hash1 = MurmurHash3.bytesHash(data, 0)
    val hash2 = MurmurHash3.bytesHash(data, hash1)
    (0 until hashCount).foreach { i =>
      val hash = Math.abs((hash1 + i * hash2) % capacity)
      f(hash)
    }
  }

  private[support] def forallHashValue(data: Array[Byte], f: Int => Boolean): Boolean = {
    // see "Less Hashing, Same Performance" by Adam Kirsch and Michael Mitzenmacher
    val hash1 = MurmurHash3.bytesHash(data, 0)
    val hash2 = MurmurHash3.bytesHash(data, hash1)
    (0 until hashCount).forall { i =>
      val hash = Math.abs((hash1 + i * hash2) % capacity)
      f(hash)
    }
  }
}
object BloomFilterConfig {
  def forFalsePositives(expectedItemCount: Int, falsePositiveProbability: Double) = {
    val capacity = Math.ceil(expectedItemCount.toDouble * Math.log(falsePositiveProbability) / Math.log(1d / Math.pow(2d, Math.log(2d)))).toInt
    val hashCount = Math.round(Math.log(2d) * capacity / expectedItemCount).toInt
    BloomFilterConfig(capacity, hashCount)
  }
}
