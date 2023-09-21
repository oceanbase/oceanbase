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

#ifndef _OB_OPT_COLUMN_STAT_H_
#define _OB_OPT_COLUMN_STAT_H_

#include <stdint.h>
#include "common/object/ob_object.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/hash_func/murmur_hash.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase {
namespace common {

class ObBorderFlag;
class ObOptColumnStat;
struct ObDataTypeCastParams;

enum ObHistType
{
  INVALID_TYPE,
  FREQUENCY,
  HEIGHT_BALANCED,
  TOP_FREQUENCY,
  HYBIRD,
};

struct ObHistBucket
{
public:
  ObHistBucket()
    : endpoint_repeat_count_(0), endpoint_num_(-1)
  {}
  ObHistBucket(int64_t repeat_count, int64_t endpoint_num)
    : endpoint_repeat_count_(repeat_count), endpoint_num_(endpoint_num)
  {}
  ObHistBucket(const ObObj &obj, int64_t repeat_count, int64_t endpoint_num)
    : endpoint_value_(obj),
      endpoint_repeat_count_(repeat_count),
      endpoint_num_(endpoint_num)
  {}

  int deep_copy(const ObHistBucket &src,
                char *buf,
                const int64_t buf_len,
                int64_t &pos);
  int deep_copy(ObIAllocator &alloc, const ObHistBucket &src);
  int64_t deep_copy_size() const { return endpoint_value_.get_deep_copy_size(); }

  TO_STRING_KV(K_(endpoint_value),
               K_(endpoint_repeat_count),
               K_(endpoint_num));

  common::ObObj endpoint_value_;
  int64_t endpoint_repeat_count_; // the frequence for the endpoint_value;
  int64_t endpoint_num_; // cumlative frequence
};


class ObHistogram
{
public:
  friend class ObOptColumnStat;

  typedef ObArrayWrap<ObHistBucket> Buckets;
  enum class BoundType {
    LOWER,
    UPPER,
    INVALID
  };

  ObHistogram() :
    type_(ObHistType::INVALID_TYPE),
    sample_size_(-1),
    density_(0),
    bucket_cnt_(0),
    buckets_(),
    pop_freq_(0),
    pop_count_(0)
    {}

  ~ObHistogram() { reset(); }

  void reset();

  int deep_copy(const ObHistogram &src, char *buf, const int64_t buf_len, int64_t &pos);
  int deep_copy(ObIAllocator &alloc, const ObHistogram &src);
  int assign(const ObHistogram &other);
  int64_t deep_copy_size() const;

  bool is_valid() const
  { return ObHistType::INVALID_TYPE != type_ && sample_size_ >= 0; }

  ObHistType get_type() const { return type_; }

  bool is_hybrid() const { return HYBIRD == type_; }
  bool is_frequency() const { return FREQUENCY == type_; }

  const char *get_type_name() const;
  void set_type(ObHistType type) { type_ = type; }

  int64_t get_sample_size() const { return sample_size_; }
  void set_sample_size(int64_t sample_size) { sample_size_ = sample_size; }

  double get_density() const { return density_; }
  void set_density(double density) { density_ = density; }

  int64_t get_bucket_cnt() const { return bucket_cnt_; }
  void set_bucket_cnt(int64_t bucket_cnt) { bucket_cnt_ = bucket_cnt; }

  int64_t get_bucket_size() const { return buckets_.count(); }

  ObHistBucket &get(int64_t i) { return buckets_.at(i); }
  const ObHistBucket &get(int64_t i) const { return buckets_.at(i); }
  Buckets &get_buckets() { return buckets_; }
  const Buckets &get_buckets() const { return buckets_; }
  int64_t get_pop_frequency() const { return pop_freq_; }
  void set_pop_frequency(int64_t pop_freq) { pop_freq_ = pop_freq; }
  int64_t get_pop_count() const { return pop_count_; }
  void set_pop_count(int64_t pop_count) { pop_count_ = pop_count; }

  int prepare_allocate_buckets(ObIAllocator &allocator, const int64_t bucket_size);
  int add_bucket(const ObHistBucket &bucket);
  int assign_buckets(const ObIArray<ObHistBucket> &buckets);

  void calc_density(ObHistType hist_type,
                    const int64_t row_count,
                    const int64_t pop_row_count,
                    const int64_t ndv,
                    const int64_t pop_ndv);
  TO_STRING_KV("Type", get_type_name(),
               K_(sample_size),
               K_(density),
               K_(bucket_cnt),
               K_(buckets));
protected:
  ObHistType type_;
  int64_t sample_size_;
  double density_;
  int64_t bucket_cnt_;
  Buckets buckets_;
  int64_t pop_freq_;  // only used during gather table stats
  int64_t pop_count_; // only used during gather table stats
};

class ObOptColumnStat : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION_V(1);
public:
  static const int64_t MAX_OBJECT_SERIALIZE_SIZE = 512;
  static const int64_t LARGE_NDV_NUMBER = 2LL << 61; // 2 << 64 is too large for int64_t, and 2 << 61 is enough for ndv
  static const int64_t BUCKET_BITS = 10; // ln2(1024) = 10;
  static const int64_t NUM_LLC_BUCKET =  (1 << BUCKET_BITS);

  struct Key : public common::ObIKVCacheKey
  {
    uint64_t tenant_id_;
    uint64_t table_id_;
    int64_t partition_id_;
    uint64_t column_id_;
    Key() : tenant_id_(0), table_id_(0), partition_id_(0), column_id_(0)
    {
    }
    Key(const uint64_t tenant_id,
        const uint64_t table_id,
        const uint64_t partition_id,
        const uint64_t column_id)
        : tenant_id_(tenant_id),
          table_id_(table_id),
          partition_id_(partition_id),
          column_id_(column_id)
    {
    }
    uint64_t hash() const
    {
      return common::murmurhash(this, sizeof(Key), 0);
    }
    int hash(uint64_t &hash_val) const
    {
      hash_val = hash();
      return OB_SUCCESS;
    }
    bool operator==(const ObIKVCacheKey &other) const
    {
      const Key &other_key = reinterpret_cast<const Key&>(other);
      return tenant_id_ == other_key.tenant_id_
          && table_id_ == other_key.table_id_
          && partition_id_ == other_key.partition_id_
          && column_id_ == other_key.column_id_;
    }
    uint64_t get_tenant_id() const
    {
      return tenant_id_;
    }
    int64_t size() const
    {
      return sizeof(*this);
    }
    int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheKey *&key) const
    {
      int ret = OB_SUCCESS;
      Key *tmp = NULL;
      if (NULL == buf || buf_len < size()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid arguments.",
                   KP(buf), K(buf_len), K(size()), K(ret));
      } else {
        tmp = new (buf) Key();
        *tmp = *this;
        key = tmp;
      }
      return ret;
    }
    bool is_valid() const
    {
      return tenant_id_ > 0 && table_id_ > 0 && column_id_ > 0;
    }
    TO_STRING_KV(K(tenant_id_),
                 K(table_id_),
                 K(partition_id_),
                 K(column_id_));
  };
  ObOptColumnStat();

  explicit ObOptColumnStat(common::ObIAllocator &allocator);

  ~ObOptColumnStat() { reset(); }
  void reset();

  uint64_t get_table_id() const { return table_id_; }
  void set_table_id(uint64_t tid) { table_id_ = tid; }

  int64_t get_partition_id() const { return partition_id_; }
  void set_partition_id(int64_t pid) { partition_id_ = pid; }

  uint64_t get_column_id() const { return column_id_; }
  void set_column_id(uint64_t cid) { column_id_ = cid; }

  const common::ObObj &get_max_value() const { return max_value_; }
  common::ObObj &get_max_value() { return max_value_; }
  void set_max_value(const common::ObObj &max) { max_value_ = max; }

  const common::ObObj &get_min_value() const { return min_value_; }
  common::ObObj &get_min_value() { return min_value_; }
  void set_min_value(const common::ObObj &min) { min_value_ = min; }

  int64_t get_num_distinct() const { return num_distinct_; }
  void set_num_distinct(int64_t num_distinct) { num_distinct_ = num_distinct; }

  int64_t get_num_null() const { return num_null_; }
  void set_num_null(int64_t num_null) { num_null_ = num_null; }

  void set_num_not_null(int64_t num_not_null) { num_not_null_ = num_not_null; }
  int64_t get_num_not_null() const { return num_not_null_; }

  void add_num_null(int64_t num_null) { num_null_ += num_null; }

  void add_num_not_null(int64_t num_not_null) { num_not_null_ += num_not_null; }

  int64_t get_num_rows() const { return num_null_ + num_not_null_; }

  void set_avg_len(int64_t avg_len) { avg_length_ = avg_len; }
  int64_t get_avg_len() const { return avg_length_; }
  // only used for osg
  void calc_avg_len() { avg_length_ = (get_num_rows() != 0) ? int64_t(round(total_col_len_ * 1.0 / get_num_rows())) : 0; }

  int64_t get_stat_level() const { return object_type_; }
  void set_stat_level(int64_t object_type) { object_type_ = object_type; }

  const ObHistogram &get_histogram() const { return histogram_; }
  ObHistogram &get_histogram() { return histogram_; }
  int64_t get_bucket_num() const { return histogram_.get_bucket_cnt(); }

  virtual int64_t size() const override;
  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const override;
  int deep_copy(const ObOptColumnStat &src, char *buf, const int64_t size, int64_t &pos);
  int deep_copy(const ObOptColumnStat &value);

  int64_t get_last_analyzed() const { return last_analyzed_; }
  void set_last_analyzed(int64_t last) { last_analyzed_ = last; }

  const char *get_llc_bitmap() const { return llc_bitmap_; }

  char *get_llc_bitmap() { return llc_bitmap_; }

  int64_t get_llc_bitmap_size() const { return llc_bitmap_size_; }

  void set_llc_bitmap_size(const int64_t size) { llc_bitmap_size_ = size; }

  void set_llc_bitmap(char *bitmap, const int64_t size) {
    llc_bitmap_ = bitmap; llc_bitmap_size_ = size; }

  bool is_valid() const
  {
    return common::OB_INVALID_ID != table_id_
        //&& partition_id_ >= 0
        //&& column_id_ >= 0
        && num_distinct_ >= 0
        && num_null_ >= 0;
  }

  void add_col_len(int64_t len) { total_col_len_ += len; }
  int64_t get_total_col_len() const { return total_col_len_; }

  int merge_column_stat(const ObOptColumnStat &other);

  common::ObCollationType get_collation_type() const { return cs_type_; }
  void set_collation_type(common::ObCollationType cs_type) { cs_type_ = cs_type; }

  static ObOptColumnStat *malloc_new_column_stat(common::ObIAllocator &allocator);

  TO_STRING_KV(K_(table_id),
               K_(partition_id),
               K_(column_id),
               K_(last_analyzed),
               K_(object_type),
               K_(num_distinct),
               K_(num_null),
               K_(min_value),
               K_(max_value),
               K_(num_not_null),
               K_(avg_length),
               K_(cs_type),
               K_(total_col_len),
               K_(llc_bitmap_size),
               K_(llc_bitmap),
               K_(histogram));
private:
  DISALLOW_COPY_AND_ASSIGN(ObOptColumnStat);
  int merge_min_max(ObObj &cur, const ObObj &other, bool is_cmp_min);
protected:
  uint64_t table_id_;
  int64_t partition_id_;
  uint64_t column_id_;
  int64_t object_type_;
  int64_t version_;
  int64_t num_null_;
  int64_t num_not_null_;
  int64_t num_distinct_;
  int64_t avg_length_;
  common::ObObj min_value_;
  common::ObObj max_value_;
  int64_t llc_bitmap_size_;
  char *llc_bitmap_;
  ObHistogram histogram_;

  /** last analyzed time */
  int64_t last_analyzed_;
  common::ObCollationType cs_type_;
  int64_t total_col_len_;
  common::ObArenaAllocator inner_allocator_;
  common::ObIAllocator &allocator_;
};

}
}

#endif /* _OB_OPT_COLUMN_STAT_H_ */
