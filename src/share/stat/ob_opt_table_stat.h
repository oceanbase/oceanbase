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

#ifndef _OB_OPT_TABLE_STAT_H_
#define _OB_OPT_TABLE_STAT_H_

#include <stdint.h>
#include <cstddef>
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "common/ob_partition_key.h"
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase {
namespace common {

/**
 * Optimizer Table Level Statistics
 */
class ObOptTableStat : public common::ObIKVCacheValue {
public:
  struct Key : public common::ObIKVCacheKey {
    Key() : table_id_(OB_INVALID_ID), partition_id_(OB_INVALID_INDEX)
    {}
    explicit Key(int64_t table_id, int64_t partition_id) : table_id_(table_id), partition_id_(partition_id)
    {}
    void init(int64_t table_id, int64_t partition_id)
    {
      table_id_ = table_id;
      partition_id_ = partition_id;
    }
    uint64_t hash() const
    {
      return common::murmurhash(this, sizeof(Key), 0);
    }
    bool operator==(const ObIKVCacheKey& other) const
    {
      const Key& other_key = reinterpret_cast<const Key&>(other);
      return table_id_ == other_key.table_id_ && partition_id_ == other_key.partition_id_;
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
      return table_id_ >= 0 && partition_id_ >= 0;
    }
    void reset()
    {
      table_id_ = OB_INVALID_ID;
      partition_id_ = OB_INVALID_INDEX;
    }
    TO_STRING_KV(K_(table_id), K_(partition_id));

    int64_t table_id_;
    int64_t partition_id_;
  };
  ObOptTableStat()
      : sstable_row_count_(0),
        memtable_row_count_(0),
        data_size_(0),
        macro_block_num_(0),
        micro_block_num_(0),
        sstable_avg_row_size_(0),
        memtable_avg_row_size_(0),
        data_version_(0),
        last_analyzed_(0)
  {}
  ObOptTableStat(int64_t sstable_row_count, int64_t memtable_row_count, int64_t data_size, int64_t macro_block_num,
      int64_t micro_block_num, int64_t sstable_avg_row_size, int64_t memtable_avg_row_size, int64_t data_version = 0)
      : sstable_row_count_(sstable_row_count),
        memtable_row_count_(memtable_row_count),
        data_size_(data_size),
        macro_block_num_(macro_block_num),
        micro_block_num_(micro_block_num),
        sstable_avg_row_size_(sstable_avg_row_size),
        memtable_avg_row_size_(memtable_avg_row_size),
        data_version_(data_version),
        last_analyzed_(0)
  {}

  virtual ~ObOptTableStat()
  {}
  int64_t get_data_version() const
  {
    return data_version_;
  }
  void set_data_version(int64_t data_version)
  {
    data_version_ = data_version;
  }

  int64_t get_sstable_avg_row_size() const
  {
    return sstable_avg_row_size_;
  }
  void set_sstable_avg_row_size(int64_t asz)
  {
    sstable_avg_row_size_ = asz;
  }

  int64_t get_memtable_avg_row_size() const
  {
    return memtable_avg_row_size_;
  }
  void set_memtable_avg_row_size(int64_t asz)
  {
    memtable_avg_row_size_ = asz;
  }

  int64_t get_data_size() const
  {
    return data_size_;
  }
  void set_data_size(int64_t dsz)
  {
    data_size_ = dsz;
  }

  int64_t get_macro_block_num() const
  {
    return macro_block_num_;
  }
  void set_macro_block_num(int64_t num)
  {
    macro_block_num_ = num;
  }

  int64_t get_micro_block_num() const
  {
    return micro_block_num_;
  }
  void set_micro_block_num(int64_t num)
  {
    micro_block_num_ = num;
  }

  int64_t get_sstable_row_count() const
  {
    return sstable_row_count_;
  }
  void set_sstable_row_count(int64_t num)
  {
    sstable_row_count_ = num;
  }

  int64_t get_memtable_row_count() const
  {
    return memtable_row_count_;
  }
  void set_memtable_row_count(int64_t num)
  {
    memtable_row_count_ = num;
  }

  int64_t get_row_count() const
  {
    return sstable_row_count_ + memtable_row_count_;
  }

  int64_t get_last_analyzed() const
  {
    return last_analyzed_;
  }
  void set_last_analyzed(int64_t last_analyzed)
  {
    last_analyzed_ = last_analyzed;
  }

  virtual int64_t size() const
  {
    return sizeof(*this);
  }

  virtual int deep_copy(char* buf, const int64_t buf_len, ObIKVCacheValue*& value) const
  {
    int ret = OB_SUCCESS;
    ObOptTableStat* tstat = nullptr;
    if (nullptr == buf || buf_len < size()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
    } else {
      tstat = new (buf) ObOptTableStat();
      *tstat = *this;
      value = tstat;
    }
    return ret;
  }

  void reset()
  {
    sstable_row_count_ = 0;
    memtable_row_count_ = 0;
    data_size_ = 0;
    macro_block_num_ = 0;
    micro_block_num_ = 0;
    sstable_avg_row_size_ = 0;
    memtable_avg_row_size_ = 0;
    data_version_ = 0;
    last_analyzed_ = 0;
  }

  TO_STRING_KV(K_(last_analyzed));

private:
  int64_t sstable_row_count_;
  int64_t memtable_row_count_;
  int64_t data_size_;
  int64_t macro_block_num_;
  int64_t micro_block_num_;
  int64_t sstable_avg_row_size_;
  int64_t memtable_avg_row_size_;
  int64_t data_version_;
  int64_t last_analyzed_;
};

}  // namespace common
}  // namespace oceanbase

#endif /* _OB_OPT_TABLE_STAT_H_ */
