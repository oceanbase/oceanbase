/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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

class ObPxBloomFilter
{
OB_UNIS_VERSION_V(1);
public:
  ObPxBloomFilter();
  virtual ~ObPxBloomFilter() {};
  int init(int64_t data_length, common::ObIAllocator &allocator, int64_t tenant_id,
           double fpp = 0.01, int64_t max_filter_size = 2147483648 /*2G*/);
  int init(const ObPxBloomFilter *filter);
  void reset_filter();
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
  int64_t get_value_true_count() const { return true_count_; };
  void dump_filter();      //for debug
  bool check_ready();
  int process_recieve_count(int64_t whole_expect_size, int64_t cur_buf_size = 1);
  int process_first_phase_recieve_count(
      int64_t whole_expect_size,
      int64_t phase_expect_size,
      int64_t begin_idx,
      bool &first_phase_end);
  int64_t *get_bits_array() { return bits_array_; }
  void inc_merge_filter_count() { ATOMIC_INC(&px_bf_merge_filter_count_); }
  void dec_merge_filter_count() { ATOMIC_DEC(&px_bf_merge_filter_count_); }
  bool is_merge_filter_finish() const { return 0 == px_bf_merge_filter_count_; }
  int64_t get_bits_array_length() const { return bits_array_length_; }
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
  int generate_receive_count_array(int64_t piece_size);
  void reset();
  int assign(const ObPxBloomFilter &filter, int64_t tenant_id);
  int regenerate();
  void set_allocator_attr(int64_t tenant_id);
  int might_contain_nonsimd(uint64_t hash, bool &is_match);
  TO_STRING_KV(K_(data_length), K_(bits_count), K_(fpp), K_(hash_func_count), K_(is_inited),
      K_(bits_array_length), K_(true_count));
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
  int64_t true_count_;           //`1`数量
  int64_t begin_idx_;            // join filter begin position
  int64_t end_idx_;              // join filter end position
  GetFunc might_contain_;       // function pointer for might contain
public:
  common::ObArenaAllocator allocator_;
public:
  //无需序列化
   int64_t px_bf_recieve_count_;  // 当前收到bloom filter的个数
   int64_t px_bf_recieve_size_;   // 预期应该收到的个数
   volatile int64_t px_bf_merge_filter_count_; // 当前持有filter, 做merge filter操作的线程个数
   ObArray<BloomFilterReceiveCount> receive_count_array_;
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

class ObPXBloomFilterHashWrapper
{
  OB_UNIS_VERSION(1);
public:
  ObPXBloomFilterHashWrapper() : tenant_id_(common::OB_INVALID_TENANT_ID),
     filter_id_(common::OB_INVALID_ID), server_id_(common::OB_INVALID_ID),
     px_sequence_id_(common::OB_INVALID_ID), task_id_(common::OB_INVALID_ID) {}
  explicit ObPXBloomFilterHashWrapper(int64_t tenant_id, int64_t filter_id,
      int64_t server_id, int64_t px_sequence_id, int64_t task_id) :
      tenant_id_(tenant_id), filter_id_(filter_id),
      server_id_(server_id), px_sequence_id_(px_sequence_id), task_id_(task_id)   {}
  ~ObPXBloomFilterHashWrapper(){}
  void init(int64_t tenant_id, int64_t filter_id,
      int64_t server_id, int64_t px_sequence_id, int64_t task_id = 0)
  {
    tenant_id_ = tenant_id;
    filter_id_ = filter_id;
    server_id_ = server_id;
    px_sequence_id_ = px_sequence_id;
    task_id_ = task_id;
  }
  inline bool operator==(const ObPXBloomFilterHashWrapper &other) const
  {
    return (tenant_id_ == other.tenant_id_ &&
            filter_id_ == other.filter_id_ &&
            server_id_ == other.server_id_ &&
            px_sequence_id_ == other.px_sequence_id_ &&
            task_id_ == other.task_id_);
  }
  inline uint64_t hash() const;
  inline int hash(uint64_t &hash_ret) const;
  int64_t tenant_id_;
  int64_t filter_id_;
  int64_t server_id_;
  int64_t px_sequence_id_;
  int64_t task_id_;
  TO_STRING_KV(K_(tenant_id), K_(filter_id), K_(server_id), K_(px_sequence_id), K_(task_id))
};



inline uint64_t ObPXBloomFilterHashWrapper::hash() const
{
  uint64_t hash_ret = 0;
  hash_ret = common::murmurhash(&tenant_id_, sizeof(uint64_t), 0);
  hash_ret = common::murmurhash(&filter_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&server_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&px_sequence_id_, sizeof(uint64_t), hash_ret);
  hash_ret = common::murmurhash(&task_id_, sizeof(uint64_t), hash_ret);
  return hash_ret;
}
inline int ObPXBloomFilterHashWrapper::hash(uint64_t &hash_ret) const
{
  hash_ret = hash();
  return OB_SUCCESS;
}

class ObPxReadAtomicGetBFCall
{
public:
  ObPxReadAtomicGetBFCall() : bloom_filter_(NULL) {}
  ~ObPxReadAtomicGetBFCall() = default;
  void operator() (common::hash::HashMapPair<ObPXBloomFilterHashWrapper,
      ObPxBloomFilter *> &entry);
  ObPxBloomFilter *bloom_filter_;
};
class ObPxBloomFilterManager
{
public:
  static ObPxBloomFilterManager &instance();
  int init();
  int get_px_bloom_filter(ObPXBloomFilterHashWrapper &key, ObPxBloomFilter *&filter);
  // 这个get接口仅给merge filter定制.
  int get_px_bf_for_merge_filter(ObPXBloomFilterHashWrapper &key, ObPxBloomFilter *&filter);
  int set_px_bloom_filter(ObPXBloomFilterHashWrapper &key, ObPxBloomFilter *filter);
  int erase_px_bloom_filter(ObPXBloomFilterHashWrapper &key, ObPxBloomFilter *&filter);
  void destroy();
  static int init_px_bloom_filter(int64_t filter_size, common::ObIAllocator &allocator,
    ObPxBloomFilter *&filter);
private:
  typedef common::hash::ObHashMap<ObPXBloomFilterHashWrapper, ObPxBloomFilter *> MAP;
  static const int64_t DEFAULT_HASH_MAP_BUCKETS_COUNT = 100000; //10w
  static const int64_t BUCKET_NUM = DEFAULT_HASH_MAP_BUCKETS_COUNT;
private:
  MAP map_;
  bool is_inited_;
private:
  ObPxBloomFilterManager();
  ~ObPxBloomFilterManager();
  DISALLOW_COPY_AND_ASSIGN(ObPxBloomFilterManager);
};

typedef common::ObSEArray<ObPXBloomFilterHashWrapper, 2> ObPxBFSEArray;


enum ObSendBFPhase
{
  FIRST_LEVEL,
  SECOND_LEVEL
};
struct ObPxBFSendBloomFilterArgs
{
  OB_UNIS_VERSION(1);
public:
  ObPxBFSendBloomFilterArgs() : bf_key_(), bloom_filter_(), next_peer_addrs_(),
      expect_bloom_filter_count_(0), current_bloom_filter_count_(0),
      expect_phase_count_(0), phase_(FIRST_LEVEL), timeout_timestamp_(0) {}
  bool is_first_phase() { return FIRST_LEVEL == phase_; }
  ObPXBloomFilterHashWrapper bf_key_;
  ObPxBloomFilter bloom_filter_;
  common::ObSArray<common::ObAddr> next_peer_addrs_;
  int64_t expect_bloom_filter_count_;
  int64_t current_bloom_filter_count_;
  int64_t expect_phase_count_;
  ObSendBFPhase phase_;
  int64_t timeout_timestamp_;
};

} //end sql

namespace obrpc {

class ObPxBFProxy
    : public obrpc::ObRpcProxy
{
public:
  DEFINE_TO(ObPxBFProxy);
  RPC_AP(PR1 send_bloom_filter, OB_PX_SEND_BLOOM_FILTER, (sql::ObPxBFSendBloomFilterArgs));
};

}

namespace sql {

class ObSendBloomFilterP
    : public obrpc::ObRpcProcessor<obrpc::ObPxBFProxy::ObRpc<obrpc::OB_PX_SEND_BLOOM_FILTER> >
{
public:
  ObSendBloomFilterP() {}
  virtual ~ObSendBloomFilterP() = default;
  virtual int init() final;
  virtual void destroy() final;
  virtual int process() final;
private:
  int process_px_bloom_filter_data();
};

} // namespace sql

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
