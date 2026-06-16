/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SQL_ENG
#include "ob_px_bloom_filter.h"
#include "share/ob_rpc_share.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace obrpc;

#define MIN_FILTER_SIZE 256
#define MAX_BIT_COUNT 17179869184// 2^34 due to the memory single alloc limit
#define BF_BLOCK_SIZE 256L
#define CACHE_LINE_SIZE 64      // 64 bytes
#define LOG_CACHE_LINE_SIZE 6   // = log2(CACHE_LINE_SIZE)

#define FIXED_HASH_COUNT 4
#define WORD_SIZE 64            // WORD_SIZE * FIXED_HASH_COUNT = BF_BLOCK_SIZE
#define BLOCK_FILTER_HASH_MASK 0x3F3F3F3F // for each 8 bits, we only use the last 6 bits

class BloomFilterPrefetchOP
{
public:
  BloomFilterPrefetchOP(ObPxBloomFilter *bloom_filter, uint64_t *hash_values)
      : bloom_filter_(bloom_filter), hash_values_(hash_values)
  {}
  OB_INLINE int operator()(int64_t i) {
    (void)bloom_filter_->prefetch_bits_block(hash_values_[i]);
    return OB_SUCCESS;
  }
private:
  ObPxBloomFilter *bloom_filter_;
  uint64_t *hash_values_;
};

// before assign, please set allocator for channel_ids_ first
int BloomFilterIndex::assign(const BloomFilterIndex &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    channel_id_ = other.channel_id_;
    begin_idx_ = other.begin_idx_;
    end_idx_ = other.end_idx_;
    if (OB_FAIL(channel_ids_.assign(other.channel_ids_))) {
      LOG_WARN("failed to assign channel_ids_");
    }
  }
  return ret;
}

const int64_t ObPxBfMemsetHelper::PARALLEL_MEMSET_THRESHOLD = 16L << 20;   // 16MB
const int64_t ObPxBfMemsetHelper::PARALLEL_MEMSET_SLICE_BYTES = 1L << 20;  // 1MB per slice

void ObPxBfMemsetHelper::publish_slice_info(void *buf, int64_t size)
{
  int64_t slice_bytes = PARALLEL_MEMSET_SLICE_BYTES;
  const int64_t slice_count =
      (size + slice_bytes - 1) / slice_bytes;
  // Stage all metadata before the release store that publishes buf to followers.
  total_size_ = size;
  slice_count_ = slice_count;
  ATOMIC_STORE(&claimed_, 0);
  ATOMIC_STORE(&done_, 0);
  ATOMIC_STORE(&buf_, buf);            // publish (release)
}

void ObPxBfMemsetHelper::leader_memset()
{
  void *buf = ATOMIC_LOAD(&buf_);
  if (nullptr != buf) {
    drain_slices();                      // leader participates
    while (ATOMIC_LOAD(&done_) < slice_count_) {
      ob_usleep(100);
    }
    copy_old_bits_array_to_new_bits_array();
    ATOMIC_STORE(&buf_, nullptr);        // retract: late followers bail
  }
}

void ObPxBfMemsetHelper::drain_slices()
{
  void *buf = ATOMIC_LOAD(&buf_);
  if (nullptr != buf) {
    const int64_t slice_count = slice_count_;
    const int64_t total_size = total_size_;
    while (true) {
      const int64_t idx = ATOMIC_FAA(&claimed_, 1);
      if (idx >= slice_count) {
        break;
      }
      int64_t slice_bytes = PARALLEL_MEMSET_SLICE_BYTES;
      const int64_t offset = idx * slice_bytes;
      const int64_t this_size = std::min(slice_bytes, total_size - offset);
      MEMSET(static_cast<char *>(buf) + offset, 0, this_size);
      (void)ATOMIC_FAA(&done_, 1);
    }
  }
}

void ObPxBfMemsetHelper::backup_old_bits_array(int64_t *bits_array, int64_t bits_array_length,
                                               int64_t begin_idx)
{
  old_bits_array_ = bits_array;
  old_bits_array_length_ = bits_array_length;
  old_begin_idx_ = begin_idx;
}

void ObPxBfMemsetHelper::copy_old_bits_array_to_new_bits_array()
{
  int64_t *new_bits_array = static_cast<int64_t *>(ATOMIC_LOAD(&buf_));
  if (nullptr != old_bits_array_ && nullptr != new_bits_array) {
    MEMCPY(new_bits_array + old_begin_idx_, old_bits_array_,
           old_bits_array_length_ * sizeof(int64_t));
    old_bits_array_ = nullptr;
    old_bits_array_length_ = 0;
    old_begin_idx_ = 0;
  }
}

ObPxBloomFilter::ObPxBloomFilter() : data_length_(0), max_bit_count_(0), bits_count_(0), fpp_(0.0),
    hash_func_count_(0), is_inited_(false), bits_array_length_(0),
    bits_array_(NULL), ser_version_(0), begin_idx_(0), end_idx_(0), fit_l3_cache_(false), allocator_()
{

}

int ObPxBloomFilter::init(int64_t data_length, ObIAllocator &allocator, int64_t tenant_id,
                          double fpp /*= 0.01 */, int64_t max_filter_size /* =2147483648 */,
                          ObPxBfMemsetHelper *memset_helper /* = nullptr */)
{
  int ret = OB_SUCCESS;
  set_allocator_attr(tenant_id);
  data_length = max(data_length, 1);
  if (fpp <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to init px bloom filter", K(ret), K(data_length), K(fpp));
  } else {
    data_length_ = data_length;
    fpp_ = fpp;
    align_max_bit_count(max_filter_size);
    (void)calc_num_of_bits();
    (void)calc_num_of_hash_func();
    ser_version_ = GET_MIN_CLUSTER_VERSION();
    bits_array_length_ = ceil((double)bits_count_ / 64);
    fit_l3_cache_ = bits_array_length_ * sizeof(int64_t) < get_level3_cache_size();
    void *bits_array_buf = NULL;
#ifdef __x86_64__
    const bool simd_support = common::is_arch_supported(ObTargetArch::AVX512);
#elif defined(__aarch64__)
    const bool simd_support = common::is_arch_supported(ObTargetArch::NEON);
#else
#error unsupported arch
#endif
    might_contain_ = simd_support ? &ObPxBloomFilter::might_contain_simd
                     : &ObPxBloomFilter::might_contain_nonsimd;
    if (OB_ISNULL(bits_array_buf = allocator.alloc(
                                       (CACHE_LINE_SIZE + bits_array_length_) * sizeof(int64_t)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc px bloom filter bits_array_", K(ret), K(bits_count_));
    } else {
      // cache line aligned address.
      int64_t align_addr = ((reinterpret_cast<int64_t>(bits_array_buf)
                            + CACHE_LINE_SIZE - 1) >> LOG_CACHE_LINE_SIZE) << LOG_CACHE_LINE_SIZE;
      bits_array_ = reinterpret_cast<int64_t *>(align_addr);
      const int64_t memset_size = bits_array_length_ * sizeof(int64_t);
      if (memset_helper != nullptr && memset_size >= ObPxBfMemsetHelper::PARALLEL_MEMSET_THRESHOLD) {
        (void) memset_helper->publish_slice_info(bits_array_, memset_size);
      } else {
        MEMSET(bits_array_, 0, memset_size);
      }
      is_inited_ = true;
      LOG_TRACE("init px bloom filter", K(data_length_), K(bits_array_buf),
                 K(bits_array_), K_(bits_array_length), K(hash_func_count_), K(simd_support));
    }
  }
  return ret;
}

int ObPxBloomFilter::assign(const ObPxBloomFilter &filter, int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  set_allocator_attr(tenant_id);
  data_length_ = filter.data_length_;
  max_bit_count_ = filter.max_bit_count_;
  block_mask_ = filter.block_mask_;
  bits_count_ = filter.bits_count_;
  fpp_ = filter.fpp_;
  hash_func_count_ = filter.hash_func_count_;
  is_inited_ = filter.is_inited_;
  bits_array_length_ = filter.bits_array_length_;
  ser_version_ = filter.ser_version_;
  might_contain_ = filter.might_contain_;
  void *bits_array_buf = NULL;
  begin_idx_ = filter.get_begin_idx();
  end_idx_ = filter.get_end_idx();
  fit_l3_cache_ = filter.fit_l3_cache_;
  if (OB_ISNULL(bits_array_buf = allocator_.alloc((bits_array_length_ + CACHE_LINE_SIZE)* sizeof(int64_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc filter", K(bits_array_length_), K(begin_idx_), K(end_idx_), K(ret));
  } else {
    int64_t align_addr = ((reinterpret_cast<int64_t>(bits_array_buf)
                          + CACHE_LINE_SIZE - 1) >> LOG_CACHE_LINE_SIZE) << LOG_CACHE_LINE_SIZE;
    bits_array_ = reinterpret_cast<int64_t *>(align_addr);
    MEMCPY(bits_array_, filter.bits_array_, sizeof(int64_t) * bits_array_length_);
  }
  return ret;
}

void ObPxBloomFilter::set_allocator_attr(int64_t tenant_id)
{
  ObMemAttr attr(tenant_id, "PxBfAlloc", ObCtxIds::DEFAULT_CTX_ID);
  allocator_.set_attr(attr);
}

int ObPxBloomFilter::init(const ObPxBloomFilter *filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the filter is null", K(ret));
  } else {
    data_length_ = filter->data_length_;
    max_bit_count_ = filter->max_bit_count_;
    block_mask_ = filter->block_mask_;
    bits_count_ = filter->bits_count_;
    fpp_ = filter->fpp_;
    hash_func_count_ = filter->hash_func_count_;
    is_inited_ = filter->is_inited_;
    bits_array_length_ = filter->bits_array_length_;
    bits_array_ = filter->bits_array_;
    ser_version_ = filter->ser_version_;
    might_contain_ = filter->might_contain_;
    fit_l3_cache_ = filter->fit_l3_cache_;
  }
  return ret;
}

void ObPxBloomFilter::reset_filter()
{
  MEMSET(bits_array_, 0, bits_array_length_ * sizeof(int64_t));
}

void ObPxBloomFilter::reset_for_rescan()
{
  // all the member inited should be reset
  data_length_ = 0;
  max_bit_count_ = 0;
  bits_count_ = 0;
  fpp_ = 0;
  hash_func_count_ = 0;
  is_inited_ = false;
  bits_array_length_ = 0;
  bits_array_ = nullptr;
  fit_l3_cache_ = false;
  allocator_.reset();
}

// previous version bits_num = - data_length * ln(p) / (ln2)^2
// close-to 2^n
// blocked bloom filter: fpp = (1 - (1 - 1 / w) ^ x) ^ 4.  x = n / block_count
void ObPxBloomFilter::calc_num_of_bits()
{
  int64_t old_n = ceil(-data_length_ * log(fpp_) / (log(2) * log(2)));
  int64_t n = ceil(data_length_ * BF_BLOCK_SIZE * log(1 - 1.0 / static_cast<double>(WORD_SIZE))
                    / log(1 - pow(fpp_, 1.0 / static_cast<double>(FIXED_HASH_COUNT))));
  int64_t ori_n = n;
  n = n - 1;
  n |= n >> 1;
  n |= n >> 2;
  n |= n >> 4;
  n |= n >> 8;
  n |= n >> 16;
  n |= n >> 32;

  // min size is block size = 256.
  bits_count_ = ((n < MIN_FILTER_SIZE) ? MIN_FILTER_SIZE : (n >= max_bit_count_) ? max_bit_count_ : n + 1);
  block_mask_ = (bits_count_ >> (LOG_HASH_COUNT + 6)) - 1;
  LOG_TRACE("calc num of bits", K(data_length_), K(fpp_), K(old_n), K(ori_n), K(bits_count_));
}

void ObPxBloomFilter::align_max_bit_count(int64_t max_filter_size)
{
  int64_t max_bit_count = max_filter_size * CHAR_BIT;
  if (MAX_BIT_COUNT == max_bit_count) {
    max_bit_count_ = max_bit_count;
  } else {
    max_bit_count_ = next_pow2(max_bit_count);
  }
}

// previous versino: hash_func_nums = bits_num / data_length * log(2)
// hash_func_count_ = BF_BLOCK_SIZE / REG_SIZE = 256 / 64 = 4
void ObPxBloomFilter::calc_num_of_hash_func()
{
  hash_func_count_ = FIXED_HASH_COUNT;
}

int ObPxBloomFilter::put(uint64_t hash)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("the px bloom filter is not inited", K(ret));
  } else {
    uint64_t block_begin = (hash & block_mask_) << LOG_HASH_COUNT;
    uint32_t hash_high = ((uint32_t)(hash >> 32) & BLOCK_FILTER_HASH_MASK);
    uint8_t *block_hash_vals = (uint8_t *)&hash_high;
    (void)set(block_begin, 1L << block_hash_vals[0]);
    (void)set(block_begin + 1, 1L << block_hash_vals[1]);
    (void)set(block_begin + 2, 1L << block_hash_vals[2]);
    (void)set(block_begin + 3, 1L << block_hash_vals[3]);
  }
  return ret;
}
int ObPxBloomFilter::put_batch(ObPxBFHashArray &hash_val_array)
{
  int ret = OB_SUCCESS;
  for (int i = 0; OB_SUCC(ret) && i < hash_val_array.count(); ++i) {
    if (OB_FAIL(put(hash_val_array.at(i)))) {
      LOG_WARN("fail to put hash value to px bloom filter", K(ret));
    }
  }
  return ret;
}

int ObPxBloomFilter::put_batch(uint64_t *batch_hash_values, const EvalBound &bound,
                               const ObBitVector &skip, bool &is_empty)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("the px bloom filter is not inited", K(ret));
  } else if (bound.get_all_rows_active()) {
    if (!fit_l3_cache()) {
      for (int64_t i = bound.start(); i < bound.end(); ++i) {
        (void)prefetch_bits_block(batch_hash_values[i]);
      }
    }
    uint32_t hash_high = 0;
    uint8_t *block_hash_vals = (uint8_t *)&hash_high;
    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      uint64_t block_begin = (batch_hash_values[i] & block_mask_) << LOG_HASH_COUNT;
      hash_high = ((uint32_t)(batch_hash_values[i] >> 32) & BLOCK_FILTER_HASH_MASK);
      (void)set(block_begin, 1L << block_hash_vals[0]);
      (void)set(block_begin + 1, 1L << block_hash_vals[1]);
      (void)set(block_begin + 2, 1L << block_hash_vals[2]);
      (void)set(block_begin + 3, 1L << block_hash_vals[3]);
    }
    if (is_empty && bound.end() - bound.start() > 0) {
      is_empty = false;
    }
  } else {
    if (!fit_l3_cache()) {
      BloomFilterPrefetchOP prefetch_op(this, batch_hash_values);
      (void)ObBitVector::flip_foreach(skip, bound, prefetch_op);
    }
    uint32_t hash_high = 0;
    uint8_t *block_hash_vals = (uint8_t *)&hash_high;
    for (int64_t i = bound.start(); i < bound.end(); ++i) {
      if (skip.at(i)) {
      } else {
        uint64_t block_begin = (batch_hash_values[i] & block_mask_) << LOG_HASH_COUNT;
        hash_high = ((uint32_t)(batch_hash_values[i] >> 32) & BLOCK_FILTER_HASH_MASK);
        (void)set(block_begin, 1L << block_hash_vals[0]);
        (void)set(block_begin + 1, 1L << block_hash_vals[1]);
        (void)set(block_begin + 2, 1L << block_hash_vals[2]);
        (void)set(block_begin + 3, 1L << block_hash_vals[3]);
        if (is_empty) {
          is_empty = false;
        }
      }
    }
  }
  return ret;
}

int ObPxBloomFilter::might_contain_nonsimd(uint64_t hash, bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  uint64_t block_begin = (hash & block_mask_) << LOG_HASH_COUNT;
  uint32_t hash_high = ((uint32_t)(hash >> 32) & BLOCK_FILTER_HASH_MASK);
  uint8_t *block_hash_vals = (uint8_t *)&hash_high;
  if (!get(block_begin, 1L << block_hash_vals[0])) {
    is_match = false;
  } else if (!get(block_begin + 1, 1L << block_hash_vals[1])) {
    is_match = false;
  } else if (!get(block_begin + 2, 1L << block_hash_vals[2])) {
    is_match = false;
  } else if (!get(block_begin + 3, 1L << block_hash_vals[3])) {
    is_match = false;
  }
  return ret;
}

bool ObPxBloomFilter::set(uint64_t word_index, uint64_t bit_index)
{
  if (!get(word_index, bit_index)) {
    int64_t old_v = 0, new_v = 0;
    do {
      old_v = bits_array_[word_index];
      new_v = old_v | bit_index;
    } while(ATOMIC_CAS(&bits_array_[word_index], old_v, new_v) != old_v);
    return true;
  }
  return false;
}

int ObPxBloomFilter::merge_filter(ObPxBloomFilter *filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("filer is null", K(ret));
  } else {
    int64_t old_v = 0, new_v = 0;
    for (int i = 0; i < filter->bits_array_length_; ++i) {
      // only merge non-zero bits
      if (filter->bits_array_[i] != 0) {
        do {
          old_v = bits_array_[i + filter->begin_idx_];
          new_v = old_v | filter->bits_array_[i];
        } while (old_v != new_v // do not write if old is equal to new
                && ATOMIC_CAS(&bits_array_[i + filter->begin_idx_], old_v, new_v) != old_v);
      }
    }
  }
  return ret;
}

int ObPxBloomFilter::regenerate(ObPxBfMemsetHelper *memset_helper)
{
  int ret = OB_SUCCESS;
  int64_t bits_array_length = ceil((double)bits_count_ / 64);
  void *bits_array_buf = NULL;
  if (bits_array_length <= 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected bits array length", K(ret));
  } else if (OB_ISNULL(bits_array_buf = allocator_.alloc((bits_array_length + CACHE_LINE_SIZE)* sizeof(int64_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc filter", K(bits_array_length), K(ret));
  } else {
    // cache line aligned address.
    int64_t align_addr = ((reinterpret_cast<int64_t>(bits_array_buf)
                          + CACHE_LINE_SIZE - 1) >> LOG_CACHE_LINE_SIZE) << LOG_CACHE_LINE_SIZE;
    int64_t *bits_array = reinterpret_cast<int64_t *>(align_addr);
    const int64_t memset_size = bits_array_length * sizeof(int64_t);
    if (memset_helper != nullptr && memset_size >= ObPxBfMemsetHelper::PARALLEL_MEMSET_THRESHOLD) {
      (void) memset_helper->publish_slice_info(bits_array, memset_size);
      // backup old bits array for merge after cooperative memset
      (void) memset_helper->backup_old_bits_array(bits_array_, bits_array_length_, begin_idx_);
    } else {
      MEMSET(bits_array, 0, memset_size);
      for (int i = 0; i < bits_array_length_; ++i) {
        bits_array[i + begin_idx_] |= bits_array_[i];
      }
    }
    bits_array_length_ = bits_array_length;
    bits_array_ = bits_array;
    begin_idx_ = 0;
    end_idx_ = bits_array_length - 1;
  }
  return ret;
}

void ObPxBloomFilter::reset()
{
  // need reset memory
  allocator_.reset();
}

OB_DEF_SERIALIZE(ObPxBloomFilter)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE,
              data_length_,
              bits_count_,
              fpp_,
              hash_func_count_,
              is_inited_,
              bits_array_length_,
              true_count_, // Union with ser_version_; legacy wire name, value is serialization version
              begin_idx_,
              end_idx_);
  bool use_memcpy = ser_version_ >= CLUSTER_VERSION_4_6_1_0;
  if (!use_memcpy) {
    // use encode to serialize bits_array_
    for (int i = begin_idx_; OB_SUCC(ret) && i <= end_idx_; ++i) {
      if (OB_FAIL(serialization::encode(buf, buf_len, pos, bits_array_[i]))) {
        LOG_WARN("fail to encode bits data", K(ret), K(bits_array_[i]));
      }
    }
  } else {
    // use memcpy to serialize bits_array_
    const int64_t real_len = end_idx_ - begin_idx_ + 1;
    const int64_t wire_bytes = real_len * sizeof(int64_t);
    if (OB_UNLIKELY(real_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid bloom filter piece for serialize", K(ret), K(begin_idx_), K(end_idx_));
    } else if (OB_UNLIKELY(buf_len - pos < wire_bytes)) {
      ret = OB_SIZE_OVERFLOW;
      LOG_WARN("no room for bits payload", K(ret), K(buf_len), K(pos), K(wire_bytes));
    } else {
      MEMCPY(buf + pos, bits_array_ + begin_idx_, wire_bytes);
      pos += wire_bytes;
    }
  }
  OB_UNIS_ENCODE(max_bit_count_);
  return ret;
}

OB_DEF_DESERIALIZE(ObPxBloomFilter)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE,
              data_length_,
              bits_count_,
              fpp_,
              hash_func_count_,
              is_inited_,
              bits_array_length_,
              true_count_, // Union with ser_version_; legacy wire name, value is serialization version
              begin_idx_,
              end_idx_);
  bool use_memcpy = ser_version_ >= CLUSTER_VERSION_4_6_1_0;
  int64_t real_len = end_idx_ - begin_idx_ + 1;
  bits_array_length_ = real_len;
  void *bits_array_buf = NULL;
  if (OB_ISNULL(bits_array_buf = allocator_.alloc((real_len + CACHE_LINE_SIZE)* sizeof(int64_t)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc filter", K(real_len), K(begin_idx_), K(end_idx_), K(ret));
  } else {
    // cache line aligned address.
    int64_t align_addr = ((reinterpret_cast<int64_t>(bits_array_buf)
                          + CACHE_LINE_SIZE - 1) >> LOG_CACHE_LINE_SIZE) << LOG_CACHE_LINE_SIZE;
    int64_t *bits_array = reinterpret_cast<int64_t *>(align_addr);
    if (!use_memcpy) {
      for (int i = 0; OB_SUCC(ret) && i < real_len; ++i) {
        if (OB_FAIL(serialization::decode(buf, data_len, pos, bits_array[i]))) {
          LOG_WARN("fail to decode bits data", K(ret));
        }
      }
    } else {
      const int64_t wire_bytes = real_len * sizeof(int64_t);
      if (OB_UNLIKELY(real_len <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid bloom filter piece for deserialize", K(ret), K(begin_idx_), K(end_idx_));
      } else if (OB_UNLIKELY(data_len - pos < wire_bytes)) {
        ret = OB_DESERIALIZE_ERROR;
        LOG_WARN("bits payload truncated", K(ret), K(data_len), K(pos), K(wire_bytes));
      } else {
        MEMCPY(bits_array, buf + pos, wire_bytes);
        pos += wire_bytes;
      }
    }

    if (OB_SUCC(ret)) {
      bits_array_ = bits_array;
      might_contain_ = common::is_arch_supported(ObTargetArch::AVX512) ? &ObPxBloomFilter::might_contain_simd
                       : &ObPxBloomFilter::might_contain_nonsimd;
    }
  }
  OB_UNIS_DECODE(max_bit_count_);
  block_mask_ = (bits_count_ >> (LOG_HASH_COUNT + 6)) - 1;
  fit_l3_cache_ = bits_array_length_ * sizeof(int64_t) < get_level3_cache_size();
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObPxBloomFilter)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN,
        data_length_,
        bits_count_,
        fpp_,
        hash_func_count_,
        is_inited_,
        bits_array_length_,
        true_count_, // Union with ser_version_; legacy wire name, value is serialization version
        begin_idx_,
        end_idx_);
  bool use_memcpy = ser_version_ >= CLUSTER_VERSION_4_6_1_0;
  if (!use_memcpy) {
    for (int i = begin_idx_; i <= end_idx_; ++i) {
      len += serialization::encoded_length(bits_array_[i]);
    }
  } else {
    const int64_t real_len = end_idx_ - begin_idx_ + 1;
    if (OB_LIKELY(real_len > 0)) {
      len += real_len * sizeof(int64_t);
    }
  }
  OB_UNIS_ADD_LEN(max_bit_count_);
  return len;
}

void ObPxBloomFilter::dump_filter()
{
  LOG_INFO("dump px bloom filter info:", K(*this));
}

namespace oceanbase
{
namespace common
{
class BloomFilterConvertSelectorOP
{
public:
  BloomFilterConvertSelectorOP(uint16_t *selector, int &cnt)
      : selector_(selector), cnt_(cnt)
  {}
  OB_INLINE int operator()(int64_t i)
  {
    selector_[cnt_++] = i;
    return OB_SUCCESS;
  }
private:
  uint16_t *__restrict__ selector_;
  int &cnt_;
};

OB_DECLARE_DEFAULT_AND_AVX512_CODE(
struct BloomFilterFixedVecWriteOP
{
  uint64_t *__restrict__ data_;
  OB_INLINE void operator()(int i, int v) const { data_[i] = v; }
};

struct BloomFilterUniVecWriteOP
{
  ObDatum *__restrict__ datums_;
  OB_INLINE void operator()(int i, int v) const { datums_[i].set_int(v); }
};

template <ObTargetArch Arch, typename ResVec>
class BloomFilterProbeOP
{
public:
  BloomFilterProbeOP(ResVec *res_vec, ObPxBloomFilter *bloom_filter, int64_t *bits_array,
                     int64_t block_mask, uint64_t *hash_values, int64_t &total_count,
                     int64_t &filter_count)
      : res_vec_(res_vec), bloom_filter_(bloom_filter), bits_array_(bits_array),
        block_mask_(block_mask), hash_values_(hash_values), total_count_(total_count),
        filter_count_(filter_count)
  {}
  int operator()(int64_t i)
  {
    bool is_match = false;
    constexpr int64_t is_match_payload = 1;
#if OB_USE_MULTITARGET_CODE
    if (Arch == ObTargetArch::AVX512) {
      (void)common::specific::avx512::might_contain_simd(bits_array_, block_mask_,
                                                         hash_values_[i], is_match);
    } else {
#elif OB_ARM_USE_MULTITARGET_CODE
    if (Arch == ObTargetArch::NEON) {
      (void)common::specific::neon::might_contain_simd(bits_array_, block_mask_,
                                                       hash_values_[i], is_match);
    } else {
#endif
      (void)bloom_filter_->might_contain_nonsimd(hash_values_[i], is_match);
#if OB_USE_MULTITARGET_CODE || OB_ARM_USE_MULTITARGET_CODE
    }
#endif
    ++total_count_;
    if (!is_match) {
      ++filter_count_;
      if (std::is_same<ResVec, IntegerUniVec>::value) {
        res_vec_->set_int(i, 0);
      }
    } else {
      if (std::is_same<ResVec, IntegerUniVec>::value) {
        res_vec_->set_int(i, 1);
      } else {
        res_vec_->set_payload(i, &is_match_payload, sizeof(int64_t));
      }
    }
    return OB_SUCCESS;
  }

private:
  ResVec *res_vec_;
  ObPxBloomFilter *bloom_filter_;
  int64_t *bits_array_;
  int64_t block_mask_;
  uint64_t *hash_values_;
  int64_t &total_count_;
  int64_t &filter_count_;
};

template<ObTargetArch Arch>
OB_INLINE
int might_contain_value(ObPxBloomFilter *bloom_filter, int64_t *bits_array, int64_t block_mask, uint64_t hash_value)
{
  bool is_match = true;
#if OB_USE_MULTITARGET_CODE
  if (Arch == ObTargetArch::AVX512) {
    (void)specific::avx512::might_contain_simd(bits_array, block_mask, hash_value, is_match);
  } else {
#elif OB_ARM_USE_MULTITARGET_CODE
  if (Arch == ObTargetArch::NEON) {
    (void)specific::neon::might_contain_simd(bits_array, block_mask, hash_value, is_match);
  } else {
#endif
    (void)bloom_filter->might_contain_nonsimd(hash_value, is_match);
#if OB_USE_MULTITARGET_CODE || OB_ARM_USE_MULTITARGET_CODE
  }
#endif
  return static_cast<int>(is_match);
}

template<bool ALL_ROWS_ACTIVE, ObTargetArch Arch, typename Op>
OB_NOINLINE
void inner_might_contain_common(
  ObPxBloomFilter *bloom_filter,
  int64_t *bits_array,
  int64_t block_mask,
  const ObBitVector &skip,
  const EvalBound &bound,
  uint64_t *__restrict__ hash_values,
  uint16_t *__restrict__ selector,
  int64_t &total_count,
  int64_t &filter_count,
  Op &op)
{
  static constexpr int PREFETCH_DISTANCE = ObPxBloomFilter::PREFETCH_DISTANCE;
  int cnt = 0;
  int hit_cnt = 0;
  if constexpr (ALL_ROWS_ACTIVE) {
    const int start = bound.start();
    const int end = bound.end();
    cnt = end - start;

    int i = start;
    if (cnt > PREFETCH_DISTANCE * 2) {
      for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
        bloom_filter->prefetch_bits_block(hash_values[i + j]);
      }
      // Batch prefetch + batch processing
      const int batch_prefetch_end = end - 2 * PREFETCH_DISTANCE;
      for (; i <= batch_prefetch_end; i += PREFETCH_DISTANCE) {
        for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
          bloom_filter->prefetch_bits_block(hash_values[i + PREFETCH_DISTANCE + j]);
        }
        for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
          const int v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[i + j]);
          op(i + j, v);
          hit_cnt += v;
        }
      }
      // Prefetch the remaining partial batch
      for (int j = i + PREFETCH_DISTANCE; j < end; ++j) {
        bloom_filter->prefetch_bits_block(hash_values[j]);
      }
      // Process the last prefetched batch
      for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
        const int v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[i + j]);
        op(i + j, v);
        hit_cnt += v;
      }
      i += PREFETCH_DISTANCE;
    } else {
      // Prefetch all
      for (int j = i; j < end; ++j) {
        bloom_filter->prefetch_bits_block(hash_values[j]);
      }
    }
    // Process the remaining partial batch
    for (; i < end; ++i) {
      const int v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[i]);
      op(i, v);
      hit_cnt += v;
    }
  } else {
    BloomFilterConvertSelectorOP convert_op(selector, cnt);
    (void)ObBitVector::flip_foreach(skip, bound, convert_op);

    int i = 0;
    if (cnt > PREFETCH_DISTANCE * 2) {
      for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
        bloom_filter->prefetch_bits_block(hash_values[selector[j]]);
      }
      // Batch prefetch + batch processing
      const int batch_prefetch_end = cnt - 2 * PREFETCH_DISTANCE;
      for (; i <= batch_prefetch_end; i += PREFETCH_DISTANCE) {
        for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
          bloom_filter->prefetch_bits_block(hash_values[selector[i + PREFETCH_DISTANCE + j]]);
        }
        for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
          const uint16_t s = selector[i + j];
          const int v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[s]);
          op(s, v);
          hit_cnt += v;
        }
      }
      // Prefetch the remaining partial batch
      for (int j = i + PREFETCH_DISTANCE; j < cnt; ++j) {
        bloom_filter->prefetch_bits_block(hash_values[selector[j]]);
      }
      // Process the last prefetched batch
      for (int j = 0; j < PREFETCH_DISTANCE; ++j) {
        const uint16_t s = selector[i + j];
        const int v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[s]);
        op(s, v);
        hit_cnt += v;
      }
      i += PREFETCH_DISTANCE;
    } else {
      // Prefetch all
      for (int j = 0; j < cnt; ++j) {
        bloom_filter->prefetch_bits_block(hash_values[selector[j]]);
      }
    }
    // Process the remaining partial batch
    for (; i < cnt; ++i) {
      const uint16_t s = selector[i];
      const int v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[s]);
      op(s, v);
      hit_cnt += v;
    }
  }

  total_count += cnt;
  filter_count += cnt - hit_cnt;
}

template<bool ALL_ROWS_ACTIVE, ObTargetArch Arch>
OB_NOINLINE
void inner_might_contain_const(
  ObPxBloomFilter *bloom_filter,
  int64_t *bits_array,
  int64_t block_mask,
  const ObBitVector &skip,
  const EvalBound &bound,
  uint64_t *__restrict__ hash_values,
  uint16_t *__restrict__ selector,
  int64_t &total_count,
  int64_t &filter_count,
  ObDatum &datum)
{
  int cnt = 0;
  int v = 0;
  if constexpr (ALL_ROWS_ACTIVE) {
    const int start = bound.start();
    const int end = bound.end();
    cnt = end - start;
    v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[start]);
  } else {
    BloomFilterConvertSelectorOP convert_op(selector, cnt);
    (void)ObBitVector::flip_foreach(skip, bound, convert_op);
    v = might_contain_value<Arch>(bloom_filter, bits_array, block_mask, hash_values[selector[0]]);
  }
  total_count += cnt;
  filter_count += cnt * (1 - v);
  datum.set_int(v);
}

template <bool ALL_ROWS_ACTIVE, ObTargetArch Arch, typename ResVec>
OB_NOINLINE
int inner_might_contain(
  ObPxBloomFilter *bloom_filter, int64_t *bits_array,
  int64_t block_mask, const ObExpr &expr, ObEvalCtx &ctx,
  const ObBitVector &skip, const EvalBound &bound,
  uint64_t *hash_values, uint16_t *selector,
  int64_t &total_count, int64_t &filter_count)
{
  int ret = OB_SUCCESS;
  ResVec *res_vec = static_cast<ResVec *>(expr.get_vector(ctx));
  if constexpr (std::is_same<ResVec, IntegerFixedVec>::value) {
    BloomFilterFixedVecWriteOP op{reinterpret_cast<uint64_t *>(res_vec->get_data())};
    inner_might_contain_common<ALL_ROWS_ACTIVE, Arch, BloomFilterFixedVecWriteOP>(bloom_filter, bits_array, block_mask, skip, bound, hash_values, selector, total_count, filter_count, op);
  } else if constexpr (std::is_same<ResVec, IntegerUniVec>::value) {
    BloomFilterUniVecWriteOP op{res_vec->get_datums()};
    inner_might_contain_common<ALL_ROWS_ACTIVE, Arch, BloomFilterUniVecWriteOP>(bloom_filter, bits_array, block_mask, skip, bound, hash_values, selector, total_count, filter_count, op);
  } else if constexpr (std::is_same<ResVec, IntegerUniCVec>::value) {
    ObDatum &datum = res_vec->get_datums()[0];
    inner_might_contain_const<ALL_ROWS_ACTIVE, Arch>(bloom_filter, bits_array, block_mask, skip, bound, hash_values, selector, total_count, filter_count, datum);
  }
  return ret;
}
)

} // namespace common
} // namespace oceanbase

#define BLOOM_FILTER_DISPATCH_ALL_ROWS_ACTIVATE(function, all_rows_active, arch,                   \
                                                res_format)                                        \
  if (all_rows_active) {                                                                           \
    BLOOM_FILTER_DISPATCH_RES_FORMAT(function, true, arch, res_format)                             \
  } else {                                                                                         \
    BLOOM_FILTER_DISPATCH_RES_FORMAT(function, false, arch, res_format)                            \
  }

#define BLOOM_FILTER_DISPATCH_RES_FORMAT(function, all_rows_active, arch, res_format)              \
  if (res_format == VEC_FIXED) {                                                                   \
    ret = function<all_rows_active, arch, IntegerFixedVec>(                                        \
        this, bits_array_, block_mask_, expr, ctx, skip, bound, hash_values, selector,             \
        total_count, filter_count);                                                                \
  } else if (res_format == VEC_UNIFORM_CONST) {                                                    \
    ret = function<all_rows_active, arch, IntegerUniCVec>(                                         \
        this, bits_array_, block_mask_, expr, ctx, skip, bound, hash_values, selector,             \
        total_count, filter_count);                                                                \
  } else {                                                                                         \
    ret = function<all_rows_active, arch, IntegerUniVec>(                                          \
        this, bits_array_, block_mask_, expr, ctx, skip, bound, hash_values, selector,             \
        total_count, filter_count);                                                                \
  }

int ObPxBloomFilter::might_contain_vector(const ObExpr &expr, ObEvalCtx &ctx,
                                          const ObBitVector &skip, const EvalBound &bound,
                                          uint64_t *hash_values, uint16_t *selector,
                                          int64_t &total_count, int64_t &filter_count)
{
  int ret = OB_SUCCESS;
  bool all_rows_active = bound.get_all_rows_active();
  VectorFormat res_format = expr.get_format(ctx);
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    constexpr ObTargetArch arch = ObTargetArch::AVX512;
    BLOOM_FILTER_DISPATCH_ALL_ROWS_ACTIVATE(common::specific::avx512::inner_might_contain,
                                            all_rows_active, arch, res_format);
  } else {
#elif OB_ARM_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::NEON)) {
    constexpr ObTargetArch arch = ObTargetArch::NEON;
    BLOOM_FILTER_DISPATCH_ALL_ROWS_ACTIVATE(common::specific::normal::inner_might_contain,
                                            all_rows_active, arch, res_format);
  } else {
#endif
    constexpr ObTargetArch arch = ObTargetArch::Default;
    BLOOM_FILTER_DISPATCH_ALL_ROWS_ACTIVATE(common::specific::normal::inner_might_contain,
                                            all_rows_active, arch, res_format)
#if OB_USE_MULTITARGET_CODE || OB_ARM_USE_MULTITARGET_CODE
  }
#endif
  return ret;
}

//-------------------------------------分割线----------------------------
int ObPxBFStaticInfo::init(int64_t tenant_id, int64_t filter_id,
    int64_t server_id, bool is_shared,
    bool skip_subpart, int64_t p2p_dh_id,
    bool is_shuffle, ObLogJoinFilter *log_join_filter_create_op)
{
  int ret = OB_SUCCESS;
  if (is_inited_){
    ret = OB_INIT_TWICE;
    LOG_WARN("twice init bf static info", K(ret));
  } else {
    tenant_id_ = tenant_id;
    filter_id_ = filter_id;
    server_id_ = server_id;
    is_shared_ = is_shared;
    skip_subpart_ = skip_subpart;
    p2p_dh_id_ = p2p_dh_id;
    is_shuffle_ = is_shuffle;
    log_join_filter_create_op_ = log_join_filter_create_op;
    is_inited_ = true;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPxBFStaticInfo, is_inited_, tenant_id_, filter_id_,
    server_id_, is_shared_, skip_subpart_, p2p_dh_id_, is_shuffle_);
