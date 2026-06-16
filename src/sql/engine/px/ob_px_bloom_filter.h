/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "rpc/obrpc/ob_rpc_proxy.h"
#include "lib/ob_define.h"
#include "lib/allocator/page_arena.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/container/ob_se_array.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/config/ob_server_config.h"
#include "observer/ob_server_struct.h"
#include "common/ob_target_specific.h"

#if OB_USE_MULTITARGET_CODE
#include <emmintrin.h>
#include <immintrin.h>
#endif

#ifndef __SQL_ENG_PX_BLOOM_FILTER_H__
#define __SQL_ENG_PX_BLOOM_FILTER_H__

namespace oceanbase
{
namespace sql
{

#define LOG_HASH_COUNT 2        // = log2(FIXED_HASH_COUNT)

typedef common::ObSEArray<uint64_t, 128> ObPxBFHashArray;

struct BloomFilterReceiveCount
{
  BloomFilterReceiveCount() : begin_idx_(0), reciv_count_(0) {}
  BloomFilterReceiveCount(int64_t begin_idx, int64_t reciv_count) :
      begin_idx_(begin_idx), reciv_count_(reciv_count) {}
  int64_t begin_idx_;
  int64_t reciv_count_;
  TO_STRING_KV(K_(begin_idx), K_(reciv_count));
};

struct BloomFilterIndex
{
  int assign(const BloomFilterIndex &other);
  BloomFilterIndex() : channel_id_(0), begin_idx_(0), end_idx_(0) {}
  BloomFilterIndex(int64_t channel_id, int64_t beigin_idx, int64_t end_idx) :
      channel_id_(channel_id), begin_idx_(beigin_idx),
      end_idx_(end_idx), channel_ids_() {}
  int64_t channel_id_;// join filter send channel id
  int64_t begin_idx_; // join filter begin position in full bloom filter
  int64_t end_idx_;   // join filter end position in full bloom filter
  ObFixedArray<int64_t, common::ObIAllocator> channel_ids_;
  TO_STRING_KV(K_(begin_idx), K_(end_idx), K_(channel_id), K_(channel_ids));
};

// Concrete helper that splits a large MEMSET into slices for cooperative zeroing.
// A single instance serves both the init path (shared by PX workers idle-spinning
// in wait_constructed) and the regenerate path (shared by RPC threads waiting in
// atomic_merge). Owners hold it by value (SharedJoinFilterConstructor) or pointer
// (ObRFBloomFilterMsg, lazily allocated); no inheritance required.
class ObPxBfMemsetHelper
{
public:
  ObPxBfMemsetHelper() : buf_(nullptr), total_size_(0), slice_count_(0),
                         claimed_(0), done_(0), old_bits_array_(nullptr),
                         old_bits_array_length_(0), old_begin_idx_(0) {}
  ~ObPxBfMemsetHelper() = default;

  // Leader entry: publish memset task, participate in drain, spin until every slice
  // is done, then retract. size < PARALLEL_MEMSET_THRESHOLD falls back to direct MEMSET
  // (no publish). Returns with [buf, buf+size) fully zeroed.
  void publish_slice_info(void *buf, int64_t size);
  void leader_memset();

  // Follower entry: if a task is currently published, drain one round of slices (non-blocking).
  // No-op if no task published. Callers should poll this from their wait loop.
  inline void follower_help() { drain_slices(); }

  void backup_old_bits_array(int64_t *bits_array, int64_t bits_array_length,
                             int64_t begin_idx);
  void copy_old_bits_array_to_new_bits_array();
  static const int64_t PARALLEL_MEMSET_THRESHOLD;
  static const int64_t PARALLEL_MEMSET_SLICE_BYTES;

private:
  void drain_slices();

  // Leader uses ATOMIC_STORE(&buf_, buf) to publish (release); followers
  // ATOMIC_LOAD(&buf_) to acquire. Fields behind buf_ are stable after publish.
  void    *buf_;
  int64_t  total_size_;
  int64_t  slice_count_;
  int64_t  claimed_;   // ATOMIC_FAA, next slice to claim
  int64_t  done_;      // ATOMIC_FAA, slices completed

  // for copy old bits array to new bits array after cooperative memset
  int64_t *old_bits_array_;
  int64_t old_bits_array_length_;
  int64_t old_begin_idx_;

  DISALLOW_COPY_AND_ASSIGN(ObPxBfMemsetHelper);
} CACHE_ALIGNED;

class ObPxBloomFilter
{
OB_UNIS_VERSION_V(1);
public:
  ObPxBloomFilter();
  virtual ~ObPxBloomFilter() {};
  int init(int64_t data_length, common::ObIAllocator &allocator, int64_t tenant_id,
           double fpp = 0.01, int64_t max_filter_size = 2147483648 /*2G*/,
           ObPxBfMemsetHelper *memset_helper = nullptr);
  int init(const ObPxBloomFilter *filter);
  inline bool is_inited() { return is_inited_; }
  // both reset_filter && reset_for_rescan are use in partition wise hash join scene
  // reset_filter is used in old fashion(bloom filter based on estimate row)
  // reset_for_rescan is used in old fashion(bloom filter based on real row)
  void reset_filter();

  void reset_for_rescan();
  inline int might_contain(uint64_t hash, bool &is_match) {
    return (this->*might_contain_)(hash, is_match);
  }
  int might_contain_vector(const ObExpr &expr, ObEvalCtx &ctx, const ObBitVector &skip,
                           const EvalBound &bound, uint64_t *hash_values, int64_t &total_count,
                           int64_t &filter_count);
  int put(uint64_t hash);
  int put_batch(ObPxBFHashArray &hash_val_array);
  int put_batch(uint64_t *batch_hash_values, const EvalBound &bound, const ObBitVector &skip, bool &is_empty);
  int merge_filter(ObPxBloomFilter *filter);
  void set_ser_version(int64_t version) { ser_version_ = version; }
  void dump_filter();      //for debug
  int64_t *get_bits_array() { return bits_array_; }
  int64_t get_bits_array_length() const { return bits_array_length_; }
  bool fit_l3_cache() { return fit_l3_cache_; }
  int64_t get_bits_count() const { return bits_count_; }
  void set_begin_idx(int64_t idx) { begin_idx_ = idx; }
  void set_end_idx(int64_t idx) { end_idx_ = idx; }
  int64_t get_begin_idx() const { return begin_idx_; }
  int64_t get_end_idx() const { return end_idx_; }
  inline void prefetch_bits_block(uint64_t hash)
  {
    uint64_t block_begin = (hash & block_mask_) << LOG_HASH_COUNT;
    __builtin_prefetch(&bits_array_[block_begin], 0);
  }
  typedef int (ObPxBloomFilter::*GetFunc)(uint64_t hash, bool &is_match);
  void reset();
  int assign(const ObPxBloomFilter &filter, int64_t tenant_id);
  int regenerate(ObPxBfMemsetHelper *memset_helper = nullptr);
  void set_allocator_attr(int64_t tenant_id);
  int might_contain_nonsimd(uint64_t hash, bool &is_match);
  TO_STRING_KV(K_(data_length), K_(bits_count), K_(fpp), K_(hash_func_count), K_(is_inited),
      K_(bits_array_length), K_(ser_version));
private:
  bool get(uint64_t pos, uint64_t index) { return (bits_array_[pos] & index) != 0; }
  bool set(uint64_t block_begin, uint64_t index);
  void calc_num_of_hash_func();
  void calc_num_of_bits();
  void align_max_bit_count(int64_t max_filter_size);
  int might_contain_simd(uint64_t hash, bool &is_match);

#ifdef unittest_bloom_filter
  int might_contain_batch(uint64_t *hash_values, int64_t batch_size);
#endif

private:
  int64_t data_length_;          //原始数据长度
  int64_t max_bit_count_;        // max filter size, default 2GB, so the max bit count = 17179869184;
  int64_t bits_count_;           //filter的位个数
  double  fpp_;                  //误判率
  int64_t hash_func_count_;      //哈希函数个数
  bool is_inited_;                //是否初始化
  int64_t bits_array_length_;    //数组长度
  int64_t *bits_array_;          //8字节位数组
  union {
    int64_t ser_version_;          // 序列化版本号，由 init() 置为当前版本号
    int64_t true_count_;          // true count, 不再使用，为了兼容序列化保存
  };
  int64_t begin_idx_;            // join filter begin position
  int64_t end_idx_;              // join filter end position
  bool fit_l3_cache_;           // whether the bloom filter fits l3 cache
  GetFunc might_contain_;       // function pointer for might contain
public:
  common::ObArenaAllocator allocator_;
public:
  //无需序列化
   int64_t block_mask_;          // for locating block
DISALLOW_COPY_AND_ASSIGN(ObPxBloomFilter);
};

class ObLogJoinFilter;

struct ObPxBFStaticInfo
{
  OB_UNIS_VERSION(1);
public:
  ObPxBFStaticInfo()
  : is_inited_(false), tenant_id_(common::OB_INVALID_TENANT_ID),
    filter_id_(common::OB_INVALID_ID), server_id_(common::OB_INVALID_ID),
    is_shared_(false), skip_subpart_(false),
    p2p_dh_id_(OB_INVALID_ID), is_shuffle_(false), log_join_filter_create_op_(nullptr)
  {}
  int init(int64_t tenant_id, int64_t filter_id,
           int64_t server_id, bool is_shared,
           bool skip_subpart, int64_t p2p_dh_id,
           bool is_shuffle, ObLogJoinFilter *log_join_filter_create_op);
  bool is_inited_;
  int64_t tenant_id_;
  int64_t filter_id_;
  int64_t server_id_;
  bool is_shared_;    // 执行期join filter内存是否共享, false代表线程级, true代表sqc级.
  bool skip_subpart_; // 是否忽略二级分区
  int64_t p2p_dh_id_;
  bool is_shuffle_;
  TO_STRING_KV(K(is_inited_), K(tenant_id_), K(filter_id_),
              K(server_id_), K(is_shared_), K(skip_subpart_),
              K(is_shuffle_), K(p2p_dh_id_));
  ObLogJoinFilter *log_join_filter_create_op_; // not need to serialize, only used in optimizor
};

} //end sql

namespace common
{
OB_DECLARE_AVX512_SPECIFIC_CODE(OB_INLINE void inline_might_contain_simd(
    int64_t *bits_array, int64_t block_mask, uint64_t hash, bool &is_match) {
  static const __m256i HASH_VALUES_MASK = _mm256_set_epi64x(24, 16, 8, 0);
  uint32_t hash_high = (uint32_t)(hash >> 32);
  uint64_t block_begin = (hash & block_mask) << LOG_HASH_COUNT;
  __m256i bit_ones = _mm256_set1_epi64x(1);
  __m256i hash_values = _mm256_set1_epi64x(hash_high);
  hash_values = _mm256_srlv_epi64(hash_values, HASH_VALUES_MASK);
  hash_values = _mm256_rolv_epi64(bit_ones, hash_values);
  __m256i bf_values = _mm256_load_si256(reinterpret_cast<__m256i *>(&bits_array[block_begin]));
  is_match = 1 == _mm256_testz_si256(~bf_values, hash_values);
})

#ifdef unittest_bloom_filter
OB_DECLARE_AVX512_SPECIFIC_CODE(void might_contain_batch_simd(
    sql::ObPxBloomFilter *bloom_filter, int64_t *bits_array, int64_t block_mask, uint64_t *hash_values,
    const int64_t &batch_size) {
  bool is_match;
  for (int64_t i = 0; i < batch_size; ++i) {
    common::specific::avx512::inline_might_contain_simd(bits_array, block_mask, hash_values[i],
                                                        is_match);
  }
})
#endif
} // namespace common

namespace sql {
#ifdef unittest_bloom_filter
int ObPxBloomFilter::might_contain_batch(uint64_t *hash_values, int64_t batch_size)
{
  int ret = OB_SUCCESS;
  bool is_match;
#if OB_USE_MULTITARGET_CODE
  if (common::is_arch_supported(ObTargetArch::AVX512)) {
    common::specific::avx512::might_contain_batch_simd(this, bits_array_, block_mask_, hash_values,
                                                       batch_size);
  } else {
#endif
    for (int64_t i = 0; i < batch_size; ++i) {
      might_contain_nonsimd(hash_values[i], is_match);
    }
#if OB_USE_MULTITARGET_CODE
  }
#endif
  return ret;
}
#endif
} // namespace sql

} //end oceanbase
#endif
