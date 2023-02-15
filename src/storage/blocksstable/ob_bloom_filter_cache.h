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

#ifndef OB_BLOOM_FILTER_CACHE_H_
#define OB_BLOOM_FILTER_CACHE_H_

#include "share/config/ob_server_config.h"
#include "storage/blocksstable/ob_block_sstable_struct.h"
#include "storage/ob_i_table.h"

namespace oceanbase
{
namespace blocksstable
{

class ObBloomFilter
{
public:
  ObBloomFilter();
  ~ObBloomFilter();
  int init(int64_t element_count, double false_positive_prob = BLOOM_FILTER_FALSE_POSITIVE_PROB);
  void destroy();
  void clear();
  int deep_copy(const ObBloomFilter &other);
  int deep_copy(const ObBloomFilter &other, char *buf);
  int64_t get_deep_copy_size() const;
  int insert(const uint32_t key_hash);
  int may_contain(const uint32_t key_hash, bool &is_contain) const;
  int64_t calc_nbyte(const int64_t nbit) const;
  OB_INLINE bool is_valid() const { return NULL != bits_ && nbit_ > 0 && nhash_ > 0; }
  OB_INLINE int64_t get_nhash() const { return nhash_; }
  OB_INLINE int64_t get_nbit() const { return nbit_; }
  OB_INLINE int64_t get_nbytes() const { return calc_nbyte(nbit_); }
  OB_INLINE uint8_t *get_bits() { return bits_; }
  OB_INLINE const uint8_t *get_bits() const { return bits_; }
  TO_STRING_KV(K_(nhash), K_(nbit), KP_(bits));
  INLINE_NEED_SERIALIZE_AND_DESERIALIZE;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilter);
  static constexpr double BLOOM_FILTER_FALSE_POSITIVE_PROB = 0.01;
  common::ObArenaAllocator allocator_;
  int64_t nhash_;
  int64_t nbit_;
  uint8_t *bits_;
};


class ObBloomFilterCacheKey : public common::ObIKVCacheKey
{
public:
  ObBloomFilterCacheKey(const uint64_t tenant_id, const MacroBlockId &block_id, const int8_t prefix_rowkey_len);
  virtual ~ObBloomFilterCacheKey();
  virtual bool operator ==(const common::ObIKVCacheKey &other) const;
  virtual uint64_t get_tenant_id() const;
  virtual uint64_t hash() const;
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheKey *&key) const;
  bool is_valid() const;
  inline int64_t get_prefix_rowkey_len() const { return prefix_rowkey_len_; }
  TO_STRING_KV(K_(tenant_id), K_(macro_block_id), K_(prefix_rowkey_len) );
private:
  uint64_t tenant_id_;
  MacroBlockId macro_block_id_;
  int8_t prefix_rowkey_len_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilterCacheKey);
};

//TODO @hanhui we need refactor bloomfilter with new hash insert method
// there is no need to use template with bloomfilter
class ObBloomFilterCacheValue : public common::ObIKVCacheValue
{
public:
  static const int64_t BLOOM_FILTER_CACHE_VALUE_VERSION = 1;
  ObBloomFilterCacheValue();
  virtual ~ObBloomFilterCacheValue();
  void reset();
  void reuse();
  virtual int64_t size() const;
  virtual int deep_copy(char *buf, const int64_t buf_len, common::ObIKVCacheValue *&value) const;
  virtual int deep_copy(ObBloomFilterCacheValue &bf_cache_value) const;
  int init(const int64_t rowkey_column_cnt, const int64_t row_cnt);
  int insert(const uint32_t hash);
  int may_contain(const uint32_t hash, bool &is_contain) const;
  bool is_valid() const;
  inline bool is_empty() const { return 0 == row_count_; }
  inline int64_t get_prefix_len() const { return rowkey_column_cnt_; }
  bool could_merge_bloom_filter(const ObBloomFilterCacheValue &bf_cache_value) const;
  int merge_bloom_filter(const ObBloomFilterCacheValue &bf_cache_value);
  OB_INLINE const uint8_t *get_bloom_filter_bits() const { return bloom_filter_.get_bits(); }
  OB_INLINE int32_t get_row_count() const { return row_count_; }
  OB_INLINE int64_t get_nhash() const { return bloom_filter_.get_nhash(); }
  OB_INLINE int64_t get_nbit() const { return bloom_filter_.get_nbit(); }
  OB_INLINE int64_t get_nbytes() const { return bloom_filter_.get_nbytes(); }
  TO_STRING_KV(K_(version), K_(rowkey_column_cnt), K_(row_count), K_(bloom_filter), K_(is_inited));
  OB_UNIS_VERSION(BLOOM_FILTER_CACHE_VALUE_VERSION);
private:
  int16_t version_;
  //TODO remove rowkey column cnt since bloomfilter cache key should already checked the rowkey count
  int16_t rowkey_column_cnt_;
  int32_t row_count_;
  ObBloomFilter bloom_filter_;
  bool is_inited_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilterCacheValue);
};

struct ObEmptyReadCell
{
  enum CellState {
    IDLE = 0,
    BUILDING = 1
  };
  ObEmptyReadCell(): state_(IDLE), count_(0), hashcode_(0), build_time_(0) {}
  virtual ~ObEmptyReadCell() {}
  void reset()
  {
    state_ = IDLE;
    count_ = 0;
    hashcode_ = 0;
    build_time_ = 0;
  }
  void set(const uint64_t hashcode)
  {
    state_ = IDLE;
    count_ = 1;
    hashcode_ = hashcode;
    build_time_ = ObTimeUtility::current_time();
  }
  bool is_valid() const
  {
    return build_time_ > 0 && count_ > 0;
  }
  int inc_and_fetch(const uint64_t hashcode, uint64_t &cur_cnt)
  {
    int ret = OB_SUCCESS;
    if(hashcode_ != hashcode) {
      if(ObTimeUtility::current_time() - ELIMINATE_TIMEOUT_US >  build_time_){
        set(hashcode);
      } else {
      //bucket is in use recently,ignore in 2min
      }
      cur_cnt = 1;
    } else {
      cur_cnt = ++count_;
    }
    return ret;
  }
  bool check_timeout()
  {
    bool bool_ret = false;
    int64_t cur_time = ObTimeUtility::current_time();
    if (cur_time - build_time_ > ELIMINATE_TIMEOUT_US) {
      bool_ret = true;
      count_ /= 2;
      build_time_ = cur_time;
    }
    return bool_ret;
  }
  inline bool is_building() const { return BUILDING == state_; }
  inline void set_building() { state_ = BUILDING; }
  TO_STRING_KV(K_(state), K_(count), K_(hashcode), K_(build_time));
  static const int64_t ELIMINATE_TIMEOUT_US = 1000 * 1000 * 120; //2min
  volatile int32_t state_; //0:init,1:building
  volatile int32_t count_;
  volatile uint64_t hashcode_;
  volatile int64_t build_time_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObEmptyReadCell);
};

class ObBFCacheKeyHashFunc
{
public:
  uint64_t operator()(const ObBloomFilterCacheKey &cacheKey, const uint64_t hash)
  {
    UNUSED(hash);
    return cacheKey.hash();
  }
};

class ObBloomFilterCache : public common::ObKVCache<ObBloomFilterCacheKey, ObBloomFilterCacheValue>
{
public:
  ObBloomFilterCache();
  virtual ~ObBloomFilterCache();
  int init(const char *cache_name, const int64_t priority, const int64_t size = DEFAULT_BUCKET_SIZE);
  void destroy();
  /**
   * put bloom filter to cache
   * @param [in] tenant_id
   * @param [in] macro_block_id
   * @param [in] bloom_filter
   */
  int put_bloom_filter(
      const uint64_t tenant_id,
      const MacroBlockId& macro_block_id,
      const ObBloomFilterCacheValue &bloom_filter,
      const bool adaptive = false);
  /**
   * check if the macro block contains the rowkey
   * @param [in] tenant_id
   * @param [in] macro_block_id
   * @param [in] rowkey
   * @param [out] is_contain
   * @return the error code
   */
  int may_contain(
      const uint64_t tenant_id,
      const MacroBlockId &macro_block_id,
      const ObDatumRowkey &rowkey,
      const ObStorageDatumUtils &datum_utils,
      bool &is_contain);
  /**
   * inc empty read count of the macro block, then try build build bloom filter for it if it is
   * necessary
   * @param [in] tenant_id
   * @param [in] macro_block_id
   * @param [in] macro_meta: meta of the macro block
   * @param [in] empty_read_prefix
   * @return the error code
   */
  int inc_empty_read(
      const uint64_t tenant_id,
      const uint64_t table_id,
      const MacroBlockId &macro_id,
      const int64_t empty_read_prefix);
  int get_sstable_bloom_filter(
      const uint64_t tenant_id,
      const MacroBlockId &macro_block_id,
      const uint64_t rowkey_column_number,
      const ObBloomFilterCacheValue *bloom_filter,
      ObKVCacheHandle &cache_handle);
  inline int set_bf_cache_miss_count_threshold(const int64_t threshold);
  inline void auto_bf_cache_miss_count_threshold(const int64_t qsize)
  {
    if(OB_UNLIKELY(bf_cache_miss_count_threshold_ <= 0)){
      //disable bf_cache_, do nothing
    }
    else {
      //newsize = base*(1 + (qsize / speed) * (qsize / speed))
      uint64_t newsize = static_cast<uint64_t>(qsize) >> BF_BUILD_SPEED_SHIFT;
      newsize = GCONF.bf_cache_miss_count_threshold * (1 + newsize * newsize);
      if (newsize != bf_cache_miss_count_threshold_) {
        bf_cache_miss_count_threshold_ = newsize < MAX_EMPTY_READ_CNT_THRESHOLD ? newsize : MAX_EMPTY_READ_CNT_THRESHOLD;
      }
    }
    if (REACH_TIME_INTERVAL(5 * 1000 * 1000)) { // print task queue size every 5s
      STORAGE_LOG(INFO, "current bloomfilter task queue size,", K(qsize), K_(bf_cache_miss_count_threshold));
    }
  }
  int check_need_build(const ObBloomFilterCacheKey &bf_key,
      bool &need_build);
  OB_INLINE bool is_valid() const { return NULL != buckets_; }
  TO_STRING_KV(K_(bf_cache_miss_count_threshold), KP_(buckets), K_(bucket_size), K_(bucket_magic));

private:
  int get_cell(const uint64_t hashcode, ObEmptyReadCell *&cell);
  OB_INLINE uint64_t get_bucket_size() const { return bucket_size_; }
  OB_INLINE uint64_t get_bucket_magic() const { return bucket_magic_; }
  static const int64_t BF_BUILD_SPEED_SHIFT = 4;
  static const int64_t DEFAULT_EMPTY_READ_CNT_THRESHOLD = 100;
  static const int64_t MAX_EMPTY_READ_CNT_THRESHOLD = 1000000;
  static const int64_t MIN_REBUILD_PERIOD_US = 1000 * 1000 * 120; //2min
  static constexpr int64_t DEFAULT_BUCKET_SIZE = 1<<20;//1048576
  volatile int64_t bf_cache_miss_count_threshold_;
  ObArenaAllocator allocator_;
  ObEmptyReadCell *buckets_;
  uint64_t bucket_size_;
  uint64_t bucket_magic_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObBloomFilterCache);
};

inline int ObBloomFilterCache::set_bf_cache_miss_count_threshold(const int64_t threshold)
{
  int ret = common::OB_SUCCESS;
  if (threshold < 0) {
    STORAGE_LOG(ERROR, "invalid cache_miss_count_threshold", K(ret), K(threshold));
  } else if (bf_cache_miss_count_threshold_ != threshold){
    STORAGE_LOG(INFO, "set bf_cache_miss_count_threshold",
        "old", bf_cache_miss_count_threshold_, "new", threshold);
    bf_cache_miss_count_threshold_ = threshold;
  }
  return ret;
}

class ObMacroBloomFilterCacheWriter
{
public:
  ObMacroBloomFilterCacheWriter();
  virtual ~ObMacroBloomFilterCacheWriter();
  int init(const int64_t rowkey_column_count, const int64_t row_count);
  void reset();
  void reuse();
  void set_not_need_build();
  int append(const common::ObArray<uint32_t> &hashs);
  bool can_merge(const ObMacroBloomFilterCacheWriter &other);
  int merge(const ObMacroBloomFilterCacheWriter &other);
  int flush_to_cache(const uint64_t tenant_id, const MacroBlockId& macro_id);
  OB_INLINE bool is_need_build() const { return is_inited_ && need_build_; }
  OB_INLINE bool is_valid() const { return is_inited_ && bf_cache_value_.is_valid(); }
  OB_INLINE int32_t get_row_count() const { return bf_cache_value_.get_row_count(); }
  OB_INLINE int64_t get_rowkey_column_count() const { return bf_cache_value_.get_prefix_len(); }
  TO_STRING_KV(K_(is_inited), K_(need_build), K_(max_row_count), K_(bf_cache_value));
private:
  ObBloomFilterCacheValue bf_cache_value_;
  int64_t max_row_count_;
  bool need_build_;
  bool is_inited_;
};

} /* namespace blocksstable */
} /* namespace oceanbase */

#endif /* OB_BLOOM_FILTER_CACHE_H_ */
