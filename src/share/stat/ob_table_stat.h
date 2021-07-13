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

#ifndef _OB_TABLE_STAT_H_
#define _OB_TABLE_STAT_H_

#include <stdint.h>
#include <cstddef>
#include "common/ob_partition_key.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase {
namespace common {

class ObTableStat : public common::ObIKVCacheValue {
public:
  struct Key : public common::ObIKVCacheKey {
    ObPartitionKey pkey_;
    Key() : pkey_()
    {}
    explicit Key(const ObPartitionKey& pkey) : pkey_(pkey)
    {}
    uint64_t hash() const
    {
      return common::murmurhash(this, sizeof(Key), 0);
    }
    bool operator==(const ObIKVCacheKey& other) const
    {
      const Key& other_key = reinterpret_cast<const Key&>(other);
      return pkey_ == other_key.pkey_;
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
        COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
      } else {
        tmp = new (buf) Key();
        *tmp = *this;
        key = tmp;
      }
      return ret;
    }
    bool is_valid() const
    {
      return pkey_.is_valid();
    }
    TO_STRING_KV(K(pkey_));
  };
  ObTableStat()
      : row_count_(0), data_size_(0), macro_blocks_num_(0), micro_blocks_num_(0), average_row_size_(0), data_version_(0)
  {}
  ObTableStat(const int64_t row_count, const int64_t data_size, const int64_t macro_blocks_num,
      const int64_t micro_blocks_num, const int64_t average_row_size, const int64_t data_version = 0)
      : row_count_(row_count),
        data_size_(data_size),
        macro_blocks_num_(macro_blocks_num),
        micro_blocks_num_(micro_blocks_num),
        average_row_size_(average_row_size),
        data_version_(data_version)
  {}

  bool is_valid() const
  {
    return row_count_ >= 0 && data_size_ >= 0 && macro_blocks_num_ >= 0 && micro_blocks_num_ >= 0 &&
           average_row_size_ >= 0 && data_version_ >= 0;
  }

  void reset()
  {
    row_count_ = 0;
    data_size_ = 0;
    macro_blocks_num_ = 0;
    micro_blocks_num_ = 0;
    average_row_size_ = 0;
    data_version_ = 0;
  }

  virtual int64_t size() const
  {
    return sizeof(*this);
  }

  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
  {
    int ret = OB_SUCCESS;
    ObTableStat* tstat = NULL;
    if (NULL == buf || buf_len < size()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
    } else {
      tstat = new (buf) ObTableStat();
      *tstat = *this;
      value = tstat;
    }
    return ret;
  }

public:
  int64_t get_data_version() const
  {
    return data_version_;
  }
  void set_data_version(const int64_t data_version)
  {
    data_version_ = data_version;
  }
  int64_t get_average_row_size() const
  {
    return average_row_size_;
  }

  void set_average_row_size(const int64_t asz)
  {
    average_row_size_ = asz;
  }

  int64_t get_data_size() const
  {
    return data_size_;
  }

  void set_data_size(const int64_t dsz)
  {
    data_size_ = dsz;
  }

  int64_t get_macro_blocks_num() const
  {
    return macro_blocks_num_;
  }

  void set_macro_blocks_num(const int64_t num)
  {
    macro_blocks_num_ = num;
  }

  int64_t get_micro_blocks_num() const
  {
    return micro_blocks_num_;
  }

  void set_micro_blocks_num(const int64_t num)
  {
    micro_blocks_num_ = num;
  }

  int64_t get_row_count() const
  {
    return row_count_;
  }

  void set_row_count(const int64_t num)
  {
    row_count_ = num;
  }

  // return number of rows in the specific table.

  TO_STRING_KV(
      K_(row_count), K_(data_size), K_(macro_blocks_num), K_(micro_blocks_num), K_(average_row_size), K_(data_version));

private:
  int64_t row_count_;
  int64_t data_size_;
  int64_t macro_blocks_num_;
  int64_t micro_blocks_num_;
  int64_t average_row_size_;
  int64_t data_version_;
};  // end of class ObTableStat

}  // namespace common
}  // end of namespace oceanbase

#endif /* _OB_TABLE_STAT_H_ */
