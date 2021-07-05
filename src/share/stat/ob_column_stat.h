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

#ifndef _OB_COLUMN_STAT_H_
#define _OB_COLUMN_STAT_H_

#include <stdint.h>
#include "common/object/ob_object.h"
#include "lib/hash_func/murmur_hash.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase {
namespace common {

class ObColumnStat : public common::ObIKVCacheValue {
public:
  static const int64_t BUCKET_BITS = 10;        // ln2(1024) = 10;
  static const int64_t TOTAL_BUCKET_BITS = 40;  // 6 groups
  static const int64_t NUM_LLC_BUCKET = (1 << BUCKET_BITS);
  static const int64_t HASH_VALUE_MAX_BITS = 64;      // 64 bits hash value.
  static const int64_t LARGE_NDV_NUMBER = 2LL << 61;  // 2 << 64 is too large for int64_t, and 2 << 61 is enough for ndv
  static const int64_t MAX_OBJECT_SERIALIZE_SIZE = 512;

public:
  struct Key : public common::ObIKVCacheKey {
    uint64_t table_id_;
    uint64_t partition_id_;
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
      return common::OB_SYS_TENANT_ID;
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

public:
  ObColumnStat();
  // construct object to write, maybe failed when dont't have enough memory for llc_bitmap_ or object_buf_
  explicit ObColumnStat(common::ObIAllocator& allocator);
  ~ObColumnStat();
  void reset();
  virtual int64_t size() const;
  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const;

  TO_STRING_KV(K(table_id_), K(partition_id_), K(column_id_), K(version_), K(last_rebuild_version_), K(num_distinct_),
      K(num_null_), K(min_value_), K(max_value_), K(is_modified_));

  int add_value(const common::ObObj& value);
  int add(const ObColumnStat& other);
  int finish();

  int deep_copy(const ObColumnStat& src, char* buf, const int64_t size, int64_t& pos);
  int64_t get_deep_copy_size() const;

  int store_llc_bitmap(const char* bitmap, const int64_t size);
  int store_min_value(const common::ObObj& min);
  int store_max_value(const common::ObObj& max);

  bool is_valid() const
  {
    return common::OB_INVALID_ID != table_id_
           //&& partition_id_ >= 0
           //&& column_id_ >= 0
           && version_ >= 0 && last_rebuild_version_ >= 0 && num_distinct_ >= 0 && num_null_ >= 0;
  }
  // check ColumnStat Object if allocates object buffer for write.
  bool is_writable() const
  {
    return (NULL != llc_bitmap_ && 0 != llc_bitmap_size_ && NULL != object_buf_);
  }
  const Key get_key() const
  {
    return Key(table_id_, partition_id_, column_id_);
  }

  uint64_t get_column_id() const
  {
    return column_id_;
  }

  void set_column_id(uint64_t cid)
  {
    column_id_ = cid;
  }

  int64_t get_llc_bitmap_size() const
  {
    return llc_bitmap_size_;
  }

  const char* get_llc_bitmap() const
  {
    return llc_bitmap_;
  }

  void set_llc_bitmap(char* bitmap, const int64_t size)
  {
    llc_bitmap_ = bitmap;
    llc_bitmap_size_ = size;
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

  int64_t get_num_distinct() const
  {
    return num_distinct_;
  }

  void set_num_distinct(int64_t numDistinct)
  {
    num_distinct_ = numDistinct;
  }

  int64_t get_num_null() const
  {
    return num_null_;
  }

  void set_num_null(int64_t num_null)
  {
    num_null_ = num_null;
  }

  uint64_t get_partition_id() const
  {
    return partition_id_;
  }

  void set_partition_id(uint64_t pid)
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

  int64_t get_version() const
  {
    return version_;
  }

  void set_version(int64_t version)
  {
    version_ = version;
  }

  int64_t get_last_rebuild_version() const
  {
    return last_rebuild_version_;
  }

  void set_last_rebuild_version(int64_t last_rebuild_version)
  {
    last_rebuild_version_ = last_rebuild_version;
  }

  bool is_modified() const
  {
    return is_modified_;
  }

private:
  inline void calc_llc_value(const common::ObObj& value);
  inline uint64_t trailing_zeroes(const uint64_t num);
  inline double select_alpha_value(const int64_t num_bucket);

private:
  DISALLOW_COPY_AND_ASSIGN(ObColumnStat);
  uint64_t table_id_;
  uint64_t partition_id_;
  uint64_t column_id_;
  int64_t version_;
  int64_t last_rebuild_version_;
  int64_t num_null_;
  int64_t num_distinct_;
  common::ObObj min_value_;
  common::ObObj max_value_;
  int64_t llc_bitmap_size_;
  char* llc_bitmap_;
  char* object_buf_;
  bool is_modified_;
};  // end of class ObColumnStat

}  // end of namespace common
}  // end of namespace oceanbase

#endif /* _OB_COLUMN_STAT_H_ */
