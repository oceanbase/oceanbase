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
class ObDataTypeCastParams;

enum class StatLevel {
  INVALID_LEVEL,
  TABLE_LEVEL,
  PARTITION_LEVEL,
  SUBPARTITION_LEVEL,
};

class ObHistogram {
public:
  friend class ObOptColumnStat;
  static const int64_t MAX_NUM_BUCKETS = 1024;
  enum class Type {
    INVALID_TYPE,
    FREQUENCY,
    HEIGHT_BALANCED,
    TOP_FREQUENCY,
    HYBIRD,
  };
  struct Bucket {
  public:
    Bucket() : endpoint_repeat_count_(0), endpoint_num_(-1)
    {}
    Bucket(int64_t repeat_count, int64_t endpoint_num)
        : endpoint_repeat_count_(repeat_count), endpoint_num_(endpoint_num)
    {}
    int deep_copy(char* buf, const int64_t buf_len, Bucket*& value);
    int64_t deep_copy_size()
    {
      return sizeof(Bucket) + endpoint_value_.get_deep_copy_size();
    }
    common::ObObj endpoint_value_;
    int64_t endpoint_repeat_count_;
    int64_t endpoint_num_;
    TO_STRING_KV(K_(endpoint_value), K_(endpoint_repeat_count), K_(endpoint_num));

  private:
    DISALLOW_COPY_AND_ASSIGN(Bucket);
  };

  typedef ObArray<Bucket*> Buckets;
  enum class BoundType { LOWER, UPPER, INVALID };

  ObHistogram() : type_(Type::INVALID_TYPE), sample_size_(0), density_(0), bucket_cnt_(0)
  {}
  ~ObHistogram()
  {}

  int deep_copy(char* buf, const int64_t buf_len, ObHistogram*& value);
  int64_t deep_copy_size();

  Type get_type() const
  {
    return type_;
  }
  void set_type(Type type)
  {
    type_ = type;
  }

  double get_sample_size() const
  {
    return sample_size_;
  }
  void set_sample_size(double sample_size)
  {
    sample_size_ = sample_size;
  }

  double get_density() const
  {
    return density_;
  }
  void set_density(double density)
  {
    density_ = density;
  }

  int64_t get_bucket_cnt() const
  {
    return bucket_cnt_;
  }
  void set_bucket_cnt(int64_t bucket_cnt)
  {
    bucket_cnt_ = bucket_cnt;
  }

  const Buckets& get_buckets() const
  {
    return buckets_;
  }
  Buckets& get_buckets()
  {
    return buckets_;
  }

  /* Advanced access function */
  int get_bucket_bound_idx(
      const ObObj& value, BoundType btype, const ObDataTypeCastParams& dtc_params, int64_t& idx) const;

  int compare_bound(
      const ObObj& left_obj, const ObObj& right_obj, const ObDataTypeCastParams& dtc_params, int64_t& result) const;

  // sum(bucket[i]) where bucket[i] belong [left_value, right_value].
  int get_density_between_range(const ObObj& startobj, const ObObj& endobj, const ObBorderFlag& border,
      const ObDataTypeCastParams& dtc_params, double& range_density) const;

  int bucket_is_popular(const Bucket& bkt, bool& is_popular) const;
  TO_STRING_KV(K_(type), K_(sample_size), K_(bucket_cnt), K_(buckets));

protected:
  Type type_;
  double sample_size_;
  double density_;
  int64_t bucket_cnt_;
  Buckets buckets_;
};

class ObOptColumnStat : public common::ObIKVCacheValue {
public:
  static const int64_t MAX_OBJECT_SERIALIZE_SIZE = 512;
  typedef ObHistogram::Bucket Bucket;
  typedef ObHistogram::Type HistogramType;
  struct Key : public common::ObIKVCacheKey {
    uint64_t table_id_;
    int64_t partition_id_;
    uint64_t column_id_;
    Key() : table_id_(0), partition_id_(0), column_id_(0)
    {}
    Key(const uint64_t tid, const uint64_t pid, const uint64_t cid)
        : table_id_(tid), partition_id_(pid), column_id_(cid)
    {}
    uint64_t hash() const
    {
      return common::murmurhash(this, sizeof(Key), 0);
    }
    bool operator==(const ObIKVCacheKey& other) const
    {
      const Key& other_key = reinterpret_cast<const Key&>(other);
      return table_id_ == other_key.table_id_ && partition_id_ == other_key.partition_id_ &&
             column_id_ == other_key.column_id_;
    }
    uint64_t get_tenant_id() const
    {
      return extract_tenant_id(table_id_);
    }
    int64_t size() const
    {
      return sizeof(*this);
    }
    int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheKey*& key) const
    {
      int ret = OB_SUCCESS;
      Key* tmp = NULL;
      if (NULL == buf || buf_len < size()) {
        ret = OB_INVALID_ARGUMENT;
        COMMON_LOG(WARN, "invalid arguments.", KP(buf), K(buf_len), K(size()), K(ret));
      } else {
        tmp = new (buf) Key();
        *tmp = *this;
        key = tmp;
      }
      return ret;
    }
    bool is_valid() const
    {
      return table_id_ > 0 && column_id_ > 0;
    }
    TO_STRING_KV(K(table_id_), K(partition_id_), K(column_id_));
  };
  ObOptColumnStat();
  ObOptColumnStat(ObIAllocator& allocator);
  ~ObOptColumnStat()
  {}
  uint64_t get_column_id() const
  {
    return column_id_;
  }
  void set_column_id(uint64_t cid)
  {
    column_id_ = cid;
  }
  const common::ObObj& get_max_value() const
  {
    return max_value_;
  }
  void set_max_value(const common::ObObj& max)
  {
    max_value_ = max;
  }
  const common::ObObj& get_min_value() const
  {
    return min_value_;
  }
  void set_min_value(const common::ObObj& min)
  {
    min_value_ = min;
  }
  int store_min_value(const common::ObObj& min);
  int store_max_value(const common::ObObj& max);
  int64_t get_num_distinct() const
  {
    return num_distinct_;
  }
  void set_num_distinct(int64_t num_distinct)
  {
    num_distinct_ = num_distinct;
  }
  int64_t get_num_null() const
  {
    return num_null_;
  }
  void set_num_null(int64_t num_null)
  {
    num_null_ = num_null;
  }
  StatLevel get_stat_level() const
  {
    return object_type_;
  }
  void set_stat_level(StatLevel object_type)
  {
    object_type_ = object_type;
  }

  int64_t get_distinct_cnt() const
  {
    return num_distinct_;
  }
  void set_distinct_cnt(int64_t num_distinct)
  {
    num_distinct_ = num_distinct;
  }
  int64_t get_null_cnt() const
  {
    return num_null_;
  }
  void set_null_cnt(int64_t num_null)
  {
    num_null_ = num_null;
  }

  int64_t get_partition_id() const
  {
    return partition_id_;
  }
  void set_partition_id(int64_t pid)
  {
    partition_id_ = pid;
  }
  uint64_t get_table_id() const
  {
    return table_id_;
  }
  void set_table_id(uint64_t tid)
  {
    table_id_ = tid;
  }
  // check ObOptColumnStat Object if allocates object buffer for write.
  bool is_writable() const
  {
    return (nullptr != object_buf_);
  }
  const ObHistogram* get_histogram() const
  {
    return histogram_;
  }
  ObHistogram* get_histogram()
  {
    return histogram_;
  }
  void set_histogram(ObHistogram* histogram)
  {
    histogram_ = histogram;
  }
  bool has_histogram() const
  {
    return histogram_ != nullptr && histogram_->type_ != HistogramType::INVALID_TYPE;
  }
  virtual int64_t size() const override;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const override;
  int deep_copy(const ObOptColumnStat& src, char* buf, const int64_t size, int64_t& pos);
  int add_bucket(int64_t repeat_count, const ObObj& value, int64_t num_elements);
  int init_histogram(const ObHistogram& basic_histogram_info);

  bool is_valid() const
  {
    return common::OB_INVALID_ID != table_id_
           //&& partition_id_ >= 0
           //&& column_id_ >= 0
           && num_distinct_ >= 0 && num_null_ >= 0;
  }

  TO_STRING_KV(K_(table_id), K_(partition_id), K_(column_id), K_(object_type), K_(num_distinct), K_(num_null),
      K_(min_value), K_(max_value));

private:
  DISALLOW_COPY_AND_ASSIGN(ObOptColumnStat);

protected:
  uint64_t table_id_;
  int64_t partition_id_;
  uint64_t column_id_;
  StatLevel object_type_;
  int64_t version_;
  int64_t num_null_;
  int64_t num_distinct_;
  common::ObObj min_value_;
  common::ObObj max_value_;
  char* object_buf_;
  ObHistogram* histogram_;
  int64_t last_analyzed_;
  ObIAllocator* const allocator_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* _OB_OPT_COLUMN_STAT_H_ */
