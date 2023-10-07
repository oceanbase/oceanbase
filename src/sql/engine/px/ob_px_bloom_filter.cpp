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

#define USING_LOG_PREFIX SQL_ENG
//#include <immintrin.h>
#include "ob_px_bloom_filter.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/container/ob_se_array.h"
#include "share/config/ob_server_config.h"
#include "share/ob_rpc_share.h"
#include "storage/blocksstable/encoding/ob_encoding_query_util.h"

using namespace oceanbase;
using namespace common;
using namespace sql;
using namespace obrpc;

#define MIN_FILTER_SIZE 256
#define MAX_BIT_COUNT 17179869184// 2^34 due to the memory single alloc limit
#define BF_BLOCK_SIZE 256L
#define BLOCK_MASK 255L         // = size of block - 1
#define CACHE_LINE_SIZE 64      // 64 bytes
#define LOG_CACHE_LINE_SIZE 6   // = log2(CACHE_LINE_SIZE)

#define FIXED_HASH_COUNT 4
#define LOG_HASH_COUNT 2        // = log2(FIXED_HASH_COUNT)
#define WORD_SIZE 64            // WORD_SIZE * FIXED_HASH_COUNT = BF_BLOCK_SIZE

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

ObPxBloomFilter::ObPxBloomFilter() : data_length_(0), max_bit_count_(0), bits_count_(0), fpp_(0.0),
    hash_func_count_(0), is_inited_(false), bits_array_length_(0),
    bits_array_(NULL), true_count_(0), begin_idx_(0), end_idx_(0), allocator_(),
    px_bf_recieve_count_(0), px_bf_recieve_size_(0), px_bf_merge_filter_count_(0)
{

}

int ObPxBloomFilter::init(int64_t data_length, ObIAllocator &allocator, int64_t tenant_id,
                          double fpp /*= 0.01 */, int64_t max_filter_size /* =2147483648 */)
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
    bits_array_length_ = ceil((double)bits_count_ / 64);
    void *bits_array_buf = NULL;
    bool simd_support = blocksstable::is_avx512_valid();
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
      MEMSET(bits_array_, 0, bits_array_length_ * sizeof(int64_t));
      is_inited_ = true;
      LOG_TRACE("init px bloom filter", K(data_length_), K(bits_array_buf),
                 K(bits_array_), K(hash_func_count_), K(simd_support));
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
  bits_count_ = filter.bits_count_;
  fpp_ = filter.fpp_;
  hash_func_count_ = filter.hash_func_count_;
  is_inited_ = filter.is_inited_;
  bits_array_length_ = filter.bits_array_length_;
  true_count_ = filter.true_count_;
  might_contain_ = filter.might_contain_;
  void *bits_array_buf = NULL;
  begin_idx_ = filter.get_begin_idx();
  end_idx_ = filter.get_end_idx();
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
    bits_count_ = filter->bits_count_;
    fpp_ = filter->fpp_;
    hash_func_count_ = filter->hash_func_count_;
    is_inited_ = filter->is_inited_;
    bits_array_length_ = filter->bits_array_length_;
    bits_array_ = filter->bits_array_;
    true_count_ = filter->true_count_;
    might_contain_ = filter->might_contain_;
  }
  return ret;
}
void ObPxBloomFilter::reset_filter()
{
  MEMSET(bits_array_, 0, bits_array_length_ * sizeof(int64_t));
  px_bf_recieve_count_ = 0;
  px_bf_recieve_size_ = 0;
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
    uint32_t hash_high = (uint32_t)(hash >> 32);
  uint64_t block_begin = (hash & ((bits_count_ >> (LOG_HASH_COUNT + 6)) - 1)) << LOG_HASH_COUNT;
    (void)set(block_begin, 1L << hash_high);
    (void)set(block_begin + 1, 1L << (hash_high >> 8));
    (void)set(block_begin + 2, 1L << (hash_high >> 16));
    (void)set(block_begin + 3, 1L << (hash_high >> 24));
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

int ObPxBloomFilter::might_contain_nonsimd(uint64_t hash, bool &is_match)
{
  int ret = OB_SUCCESS;
  is_match = true;
  uint32_t hash_high = (uint32_t)(hash >> 32);
  uint64_t block_begin = (hash & ((bits_count_ >> (LOG_HASH_COUNT + 6)) - 1)) << LOG_HASH_COUNT;
  if (!get(block_begin, 1L << hash_high)) {
    is_match = false;
  } else if (!get(block_begin + 1, 1L << (hash_high >> 8))) {
    is_match = false;
  } else if (!get(block_begin + 2, 1L << (hash_high >> 16))) {
    is_match = false;
  } else if (!get(block_begin + 3, 1L << (hash_high >> 24))) {
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
      do {
        old_v = bits_array_[i + filter->begin_idx_];
        new_v = old_v | filter->bits_array_[i];
      } while(ATOMIC_CAS(&bits_array_[i + filter->begin_idx_], old_v, new_v) != old_v);
    }
  }
  return ret;
}

bool ObPxBloomFilter::check_ready()
{
  return px_bf_recieve_count_ > 0 &&
         px_bf_recieve_size_ > 0 &&
         px_bf_recieve_count_ == px_bf_recieve_size_;
}

int ObPxBloomFilter::process_recieve_count(int64_t whole_expect_size, int64_t cur_buf_size)
{
  int ret = OB_SUCCESS;
  if (whole_expect_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the size is not invalid", K(ret));
  } else {
    if (px_bf_recieve_size_ <= 0) {
      px_bf_recieve_size_ = whole_expect_size;
    }
    ATOMIC_AAF(&px_bf_recieve_count_, cur_buf_size);
    if (px_bf_recieve_count_ > px_bf_recieve_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to process recieve count", K(ret), K(px_bf_recieve_count_),
         K(px_bf_recieve_size_));
    }
  }
  return ret;
}

int ObPxBloomFilter::process_first_phase_recieve_count(int64_t whole_expect_size,
    int64_t phase_expect_size, int64_t begin_idx, bool &first_phase_end)
{
  int ret = OB_SUCCESS;
  first_phase_end = false;
  if (whole_expect_size <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("the size is not invalid", K(ret));
  } else {
    if (px_bf_recieve_size_ <= 0) {
      px_bf_recieve_size_ = whole_expect_size;
    }
    ATOMIC_INC(&px_bf_recieve_count_);
    if (px_bf_recieve_count_ > px_bf_recieve_size_) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to process recieve count", K(ret), K(px_bf_recieve_count_),
         K(px_bf_recieve_size_));
    } else if (receive_count_array_.empty()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("emptry receive count array", K(ret));
    } else {
      bool find = false;
      for (int i = 0; OB_SUCC(ret) && i < receive_count_array_.count(); ++i) {
        if (begin_idx == receive_count_array_.at(i).begin_idx_) {
          int64_t cur_count = ATOMIC_AAF(&receive_count_array_.at(i).reciv_count_, 1);
          first_phase_end = (cur_count == phase_expect_size);
          find = true;
          break;
        }
      }
      if (!find) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected process first phase", K(ret), K(receive_count_array_.count()));
      }
    }
  }
  return ret;
}

int ObPxBloomFilter::generate_receive_count_array(int64_t piece_size)
{
  int ret = OB_SUCCESS;
  int64_t count = ceil(bits_array_length_ / (double)piece_size);
  int64_t begin_idx = 0;
  for (int i = 0; OB_SUCC(ret) && i < count; ++i) {
    begin_idx = i * piece_size;
    if (begin_idx >= bits_array_length_) {
      begin_idx = bits_array_length_ - 1;
    }
    OZ(receive_count_array_.push_back(BloomFilterReceiveCount(begin_idx, 0)));
  }
  return ret;
}

int ObPxBloomFilter::regenerate()
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
    MEMSET(bits_array, 0, bits_array_length * sizeof(int64_t));
    for (int i = 0; i < bits_array_length_; ++i) {
      bits_array[i + begin_idx_] |= bits_array_[i];
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
  receive_count_array_.reset();
  allocator_.reset();
}

void ObPxBloomFilter::prefetch_bits_block(uint64_t hash)
{
  uint64_t block_begin = (hash & ((bits_count_ >> (LOG_HASH_COUNT + 6)) - 1)) << LOG_HASH_COUNT;
  __builtin_prefetch(&bits_array_[block_begin], 0);
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
              true_count_,
              begin_idx_,
              end_idx_);
  for (int i = begin_idx_; OB_SUCC(ret) && i <= end_idx_; ++i) {
    if (OB_FAIL(serialization::encode(buf, buf_len, pos, bits_array_[i]))) {
      LOG_WARN("fail to encode bits data", K(ret), K(bits_array_[i]));
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
              true_count_,
              begin_idx_,
              end_idx_);
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
    for (int i = 0; OB_SUCC(ret) && i < real_len; ++i) {
      if (OB_FAIL(serialization::decode(buf, data_len, pos, bits_array[i]))) {
        LOG_WARN("fail to decode bits data", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      bits_array_ = bits_array;
      might_contain_ = blocksstable::is_avx512_valid() ? &ObPxBloomFilter::might_contain_simd
                       : &ObPxBloomFilter::might_contain_nonsimd;
    }
  }
  OB_UNIS_DECODE(max_bit_count_);
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
        true_count_,
        begin_idx_,
        end_idx_);
  for (int i = begin_idx_; i <= end_idx_; ++i) {
    len += serialization::encoded_length(bits_array_[i]);
  }
  OB_UNIS_ADD_LEN(max_bit_count_);
  return len;
}

 void ObPxBloomFilter::dump_filter()
 {
   LOG_INFO("dump px bloom filter info:", K(*this));
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
//-------------------------------------分割线----------------------------
void ObPxReadAtomicGetBFCall::operator() (common::hash::HashMapPair<ObPXBloomFilterHashWrapper,
      ObPxBloomFilter *> &entry)
{
  bloom_filter_ = entry.second;
  bloom_filter_->inc_merge_filter_count();
}
//-------------------------------------分割线----------------------------
ObPxBloomFilterManager &ObPxBloomFilterManager::instance()
{
  static ObPxBloomFilterManager the_px_bloom_filter_manager;
  return the_px_bloom_filter_manager;
}
ObPxBloomFilterManager::~ObPxBloomFilterManager()
{
  destroy();
}
ObPxBloomFilterManager::ObPxBloomFilterManager() : map_(), is_inited_(false)
{
}
int ObPxBloomFilterManager::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("no need to init twice filter manager", K(ret));
  } else if (OB_FAIL(map_.create(BUCKET_NUM,
      ObModIds::OB_HASH_PX_BLOOM_FILTER_KEY,
      ObModIds::OB_HASH_NODE_PX_BLOOM_FILTER_KEY))) {
    LOG_WARN("create hash table failed", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObPxBloomFilterManager::destroy()
{
  if (IS_INIT) {
    map_.destroy();
  }
}

int ObPxBloomFilterManager::get_px_bloom_filter(ObPXBloomFilterHashWrapper &key,
    ObPxBloomFilter *&filter)
{
  int ret = OB_SUCCESS;
  ObPxBloomFilter *tmp_filter_ptr = NULL;
  if (OB_FAIL(map_.get_refactored(key, tmp_filter_ptr))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("fail to get px bloom filter in filter manager", K(ret));
    }
  } else {
    filter = tmp_filter_ptr;
  }
  return ret;
}
int ObPxBloomFilterManager::set_px_bloom_filter(ObPXBloomFilterHashWrapper &key,
    ObPxBloomFilter *filter)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(filter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("fail to set px bloom filter", K(ret));
  } else if (OB_FAIL(map_.set_refactored(key, filter, 1/*over_write*/))) {
    LOG_WARN("fail to set px bloom filter in filter manager", K(ret));
  }
  return ret;
}
int ObPxBloomFilterManager::erase_px_bloom_filter(ObPXBloomFilterHashWrapper &key,
  ObPxBloomFilter *&filter)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(map_.erase_refactored(key, &filter))) {
    LOG_TRACE("fail to erase px bloom filter in filter manager", K(ret));
  }
  return ret;
}

int ObPxBloomFilterManager::init_px_bloom_filter(int64_t filter_size, ObIAllocator &allocator,
    ObPxBloomFilter *&filter)
{
  int ret = OB_SUCCESS;
  void *ptr = NULL;
  filter_size = MAX(filter_size, 1);
  if (OB_ISNULL(ptr = allocator.alloc(sizeof(ObPxBloomFilter)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObPxBloomFilter", K(ret));
  } else if (OB_ISNULL(filter = new(ptr) ObPxBloomFilter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc ObPxBloomFilter", K(ret));
  } else if (OB_FAIL(filter->init(filter_size, allocator,
        (double)GCONF._bloom_filter_ratio / 100))) {
    LOG_WARN("fail to init ObPxBloomFilter", K(ret));
  }
  return ret;
}

int ObPxBloomFilterManager::get_px_bf_for_merge_filter(ObPXBloomFilterHashWrapper &key,
    ObPxBloomFilter *&filter)
{
  int ret = OB_SUCCESS;
  ObPxReadAtomicGetBFCall get_bf_call;
  if (OB_FAIL(map_.read_atomic(key, get_bf_call))) {
    LOG_WARN("fail to get row store in result manager", K(ret));
  } else {
    filter = get_bf_call.bloom_filter_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObPxBFStaticInfo, is_inited_, tenant_id_, filter_id_,
    server_id_, is_shared_, skip_subpart_, p2p_dh_id_, is_shuffle_);
OB_SERIALIZE_MEMBER(ObPXBloomFilterHashWrapper, tenant_id_, filter_id_,
    server_id_, px_sequence_id_, task_id_)
OB_SERIALIZE_MEMBER(ObPxBFSendBloomFilterArgs, bf_key_, bloom_filter_,
    next_peer_addrs_, expect_bloom_filter_count_,
    current_bloom_filter_count_, expect_phase_count_,
    phase_, timeout_timestamp_);

int ObSendBloomFilterP::init()
{
  return OB_SUCCESS;
}

int ObSendBloomFilterP::process_px_bloom_filter_data()
{
  int ret = OB_SUCCESS;
  bool phase_end = false;
  ObPxBloomFilter *filter = NULL;
  if (OB_FAIL(ObPxBloomFilterManager::instance().get_px_bf_for_merge_filter(
      arg_.bf_key_, filter))) {
    LOG_WARN("fail to get px bloom filter", K(ret));
  }

  if (OB_SUCC(ret) && OB_NOT_NULL(filter)) {
    if (OB_FAIL(filter->merge_filter(&arg_.bloom_filter_))) {
      LOG_WARN("fail to merge filter", K(ret));
    } else if (!arg_.is_first_phase() &&
        OB_FAIL(filter->process_recieve_count(arg_.expect_bloom_filter_count_,
        arg_.current_bloom_filter_count_))) {
      LOG_WARN("fail to process recieve count", K(ret));
    } else if (arg_.is_first_phase() && OB_FAIL(filter->process_first_phase_recieve_count(
        arg_.expect_bloom_filter_count_,
        arg_.expect_phase_count_,
        arg_.bloom_filter_.get_begin_idx(),
        phase_end))) {
      LOG_WARN("fail to process recieve count", K(ret));
    }
  }

  if (OB_SUCC(ret) && phase_end && arg_.is_first_phase() && !arg_.next_peer_addrs_.empty()) {
    ObPxBFProxy proxy;
    if (OB_FAIL(share::init_obrpc_proxy(proxy))) {
      LOG_WARN("fail to init obrpc proxy", K(ret));
    } else {
      ObPxBFSendBloomFilterArgs new_arg;
      new_arg.bf_key_ = arg_.bf_key_;
      if (OB_FAIL(new_arg.bloom_filter_.init(filter))) {
        LOG_WARN("fail to init arg bloom filter", K(ret));
      } else {
        new_arg.expect_bloom_filter_count_ = arg_.expect_bloom_filter_count_;
        new_arg.current_bloom_filter_count_ = arg_.expect_phase_count_;
        new_arg.phase_ = ObSendBFPhase::SECOND_LEVEL;
        new_arg.bloom_filter_.set_begin_idx(arg_.bloom_filter_.get_begin_idx());
        new_arg.bloom_filter_.set_end_idx(arg_.bloom_filter_.get_end_idx());
        new_arg.timeout_timestamp_ = arg_.timeout_timestamp_;
        for (int i = 0; OB_SUCC(ret) && i < arg_.next_peer_addrs_.count(); ++i) {
          if (arg_.next_peer_addrs_.at(i) != GCTX.self_addr()) {
            if (OB_FAIL(proxy.to(arg_.next_peer_addrs_.at(i))
                      .by(arg_.bf_key_.tenant_id_)
                      .timeout(arg_.timeout_timestamp_)
                      .compressed(ObCompressorType::LZ4_COMPRESSOR)
                      .send_bloom_filter(new_arg, NULL))) {
              LOG_WARN("fail to send bloom filter", K(ret));
            }
          }
        }
      }
    }
  }
  if (OB_NOT_NULL(filter)) {
    (void)filter->dec_merge_filter_count();
  }
  return ret;
}

void ObSendBloomFilterP::destroy() {}

int ObSendBloomFilterP::process()
{
  return process_px_bloom_filter_data();
}
