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
#include "share/cache/ob_kvcache_struct.h"

namespace oceanbase {
namespace common {

/**
 * Optimizer Table Level Statistics
 */
class ObOptTableStat : public common::ObIKVCacheValue
{
  OB_UNIS_VERSION_V(1);
public:
  struct Key : public common::ObIKVCacheKey
  {
    Key() : tenant_id_(0),
            table_id_(OB_INVALID_ID),
            partition_id_(OB_INVALID_INDEX),
            tablet_id_(ObTabletID::INVALID_TABLET_ID)
    {
    }
    explicit Key(uint64_t tenant_id, uint64_t table_id, int64_t partition_id) :
      tenant_id_(tenant_id), table_id_(table_id), partition_id_(partition_id),
      tablet_id_(ObTabletID::INVALID_TABLET_ID)
    {
    }
    explicit Key(uint64_t tenant_id, uint64_t table_id, uint64_t tablet_id) :
      tenant_id_(tenant_id), table_id_(table_id), partition_id_(OB_INVALID_INDEX),
      tablet_id_(tablet_id)
    {
    }
    void init(uint64_t tenant_id, uint64_t table_id, int64_t partition_id)
    {
      tenant_id_ = tenant_id;
      table_id_ = table_id;
      partition_id_ = partition_id;
      tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    }
    uint64_t hash() const
    {
      return common::murmurhash(this, sizeof(Key), 0);
    }
    int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
    bool operator==(const ObIKVCacheKey &other) const
    {
      const Key &other_key = reinterpret_cast<const Key&>(other);
      return tenant_id_ == other_key.tenant_id_ &&
             table_id_ == other_key.table_id_ &&
             partition_id_ == other_key.partition_id_ &&
             tablet_id_ == other_key.tablet_id_;
    }
    uint64_t get_tenant_id() const
    {
      return tenant_id_;
    }

    uint64_t get_table_id() const
    {
      return table_id_;
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
        COMMON_LOG(WARN, "invalid argument.",
            K(ret), KP(buf), K(buf_len), K(size()));
      } else {
        tmp = new (buf) Key();
        *tmp = *this;
        key = tmp;
      }
      return ret;
    }

    bool is_valid() const
    {
      return tenant_id_ != 0 && table_id_ != OB_INVALID_ID;
    }

    void reset()
    {
      tenant_id_ = 0;
      table_id_ = OB_INVALID_ID;
      partition_id_ = OB_INVALID_INDEX;
      tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    }

    TO_STRING_KV(K_(tenant_id), K_(table_id), K_(partition_id), K_(tablet_id));

    uint64_t tenant_id_;
    uint64_t table_id_;
    int64_t partition_id_;
    uint64_t tablet_id_;
  };
  ObOptTableStat()
    : table_id_(OB_INVALID_ID),
      partition_id_(OB_INVALID_INDEX),
      object_type_(0),
      row_count_(0),
      avg_row_size_(0),
      sstable_row_count_(0),
      memtable_row_count_(0),
      data_size_(0),
      macro_block_num_(0),
      micro_block_num_(0),
      sstable_avg_row_size_(0),
      memtable_avg_row_size_(0),
      data_version_(0),
      last_analyzed_(0),
      stattype_locked_(0),
      modified_count_(0),
      sample_size_(0),
      tablet_id_(ObTabletID::INVALID_TABLET_ID),
      stat_expired_time_(-1) {}
  ObOptTableStat(uint64_t table_id,
                 int64_t partition_id,
                 int64_t object_type,
                 int64_t row_count,
                 int64_t avg_row_size,
                 int64_t sstable_row_count,
                 int64_t memtable_row_count,
                 int64_t data_size,
                 int64_t macro_block_num,
                 int64_t micro_block_num,
                 int64_t sstable_avg_row_size,
                 int64_t memtable_avg_row_size,
                 int64_t data_version = 0) :
      table_id_(table_id),
      partition_id_(partition_id),
      object_type_(object_type),
      row_count_(row_count),
      avg_row_size_(avg_row_size),
      sstable_row_count_(sstable_row_count),
      memtable_row_count_(memtable_row_count),
      data_size_(data_size),
      macro_block_num_(macro_block_num),
      micro_block_num_(micro_block_num),
      sstable_avg_row_size_(sstable_avg_row_size),
      memtable_avg_row_size_(memtable_avg_row_size),
      data_version_(data_version),
      last_analyzed_(0),
      stattype_locked_(0),
      modified_count_(0),
      sample_size_(0),
      tablet_id_(ObTabletID::INVALID_TABLET_ID),
      stat_expired_time_(-1) {}

  virtual ~ObOptTableStat() {}

  int merge_table_stat(const ObOptTableStat &other);

  uint64_t get_table_id() const { return table_id_; }
  void set_table_id(uint64_t table_id) { table_id_ = table_id; }

  uint64_t get_tablet_id() const { return tablet_id_; }
  void set_tablet_id(uint64_t tablet_id) { tablet_id_ = tablet_id; }

  int64_t get_partition_id() const { return partition_id_; }
  void set_partition_id(int64_t partition_id) { partition_id_ = partition_id; }

  int64_t get_object_type() const { return object_type_; }
  void set_object_type(int64_t type) { object_type_ = type; }

  int64_t get_data_version() const { return data_version_; }
  void set_data_version(int64_t data_version) { data_version_ = data_version; }

  int64_t get_row_count() const { return row_count_; }
  void set_row_count(int64_t rc) { row_count_ = rc; }

  int64_t get_avg_row_size() const { return (int64_t)avg_row_size_; }
  // can't be overload, so we just use the args to pass avg_len.
  void get_avg_row_size(double &avg_len) const { avg_len = avg_row_size_; }
  void set_avg_row_size(int64_t avg_len) { avg_row_size_ = avg_len; }

  int64_t get_sstable_avg_row_size() const { return sstable_avg_row_size_; }
  void set_sstable_avg_row_size(int64_t asz) { sstable_avg_row_size_ = asz; }

  int64_t get_memtable_avg_row_size() const { return memtable_avg_row_size_; }
  void set_memtable_avg_row_size(int64_t asz) { memtable_avg_row_size_ = asz; }

  int64_t get_data_size() const { return data_size_; }
  void set_data_size(int64_t dsz) { data_size_ = dsz; }

  int64_t get_macro_block_num() const { return macro_block_num_; }
  void set_macro_block_num(int64_t num) { macro_block_num_ = num; }

  int64_t get_micro_block_num() const { return micro_block_num_; }
  void set_micro_block_num(int64_t num) { micro_block_num_ = num; }

  int64_t get_sstable_row_count() const { return sstable_row_count_; }
  void set_sstable_row_count(int64_t num) { sstable_row_count_ = num; }

  int64_t get_memtable_row_count() const { return memtable_row_count_; }
  void set_memtable_row_count(int64_t num) { memtable_row_count_ = num; }

  int64_t get_last_analyzed() const { return last_analyzed_; }
  void set_last_analyzed(int64_t last_analyzed) {  last_analyzed_ = last_analyzed; }

  uint64_t get_stattype_locked() const { return stattype_locked_; }
  void set_stattype_locked(uint64_t stattype_locked) {  stattype_locked_ = stattype_locked; }

  int64_t get_modified_count() const { return modified_count_; }
  void set_modified_count(int64_t modified_count) {  modified_count_ = modified_count; }

  int64_t get_sample_size() const { return sample_size_; }
  void set_sample_size(int64_t sample_size) {  sample_size_ = sample_size; }

  bool is_arrived_expired_time() const {
    return stat_expired_time_ != -1 && stat_expired_time_ <= ObTimeUtility::current_time(); }

  void set_stat_expired_time(int64_t expired_time) {  stat_expired_time_ = expired_time; }

  bool is_locked() const { return stattype_locked_ > 0; }

  void add_row_count(int64_t rc) { row_count_ += rc; }

  // for multi rows
  void add_avg_row_size(double avg_row_size, int64_t rc) {
    SQL_LOG(DEBUG, "INFO", K(partition_id_));
    SQL_LOG(DEBUG, "MERGE TABLE AVG LEN", K(avg_row_size_), K(row_count_), K(avg_row_size), K(rc));
    if (row_count_ + rc != 0) {
      avg_row_size_ = (avg_row_size_ * row_count_ + avg_row_size * rc) / (row_count_ + rc);
      SQL_LOG(DEBUG, "avg size ", K(avg_row_size_));
    }
  }
  // for one row
  void add_avg_row_size(int64_t avg_row_size) {
    add_avg_row_size((double)avg_row_size, 1);
  }

  virtual int64_t size() const
  {
    return sizeof(*this);
  }

  virtual int deep_copy(char *buf, const int64_t buf_len, ObIKVCacheValue *&value) const
  {
    int ret = OB_SUCCESS;
    ObOptTableStat *tstat = nullptr;
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

  virtual int deep_copy(char *buf, const int64_t buf_len, ObOptTableStat *&stat) const
  {
    int ret = OB_SUCCESS;
    ObOptTableStat *tstat = nullptr;
    if (nullptr == buf || buf_len < size()) {
      ret = OB_INVALID_ARGUMENT;
      COMMON_LOG(WARN, "invalid argument.", K(ret), KP(buf), K(buf_len), K(size()));
    } else {
      tstat = new (buf) ObOptTableStat();
      *tstat = *this;
      stat = tstat;
    }
    return ret;
  }

  bool is_valid() const
  {
    return table_id_ != OB_INVALID_ID;
  }

  void reset() {
    table_id_ = OB_INVALID_ID;
    partition_id_ = OB_INVALID_INDEX;
    object_type_ = 0;

    row_count_ = 0;
    avg_row_size_ = 0;
    sstable_row_count_ = 0;
    memtable_row_count_ = 0;
    data_size_ = 0;
    macro_block_num_ = 0;
    micro_block_num_ = 0;
    sstable_avg_row_size_ = 0;
    memtable_avg_row_size_ = 0;
    data_version_ = 0;
    last_analyzed_ = 0;
    stattype_locked_ = 0;
    modified_count_ = 0;
    sample_size_ = 0;
    tablet_id_ = ObTabletID::INVALID_TABLET_ID;
    stat_expired_time_ = -1;
  }

  TO_STRING_KV(K(table_id_),
               K(partition_id_),
               K(object_type_),
               K(row_count_),
               K(avg_row_size_),
               K(sstable_row_count_),
               K(memtable_row_count_),
               K(data_size_),
               K(macro_block_num_),
               K(micro_block_num_),
               K(sstable_avg_row_size_),
               K(memtable_avg_row_size_),
               K(data_version_),
               K(last_analyzed_),
               K(stattype_locked_),
               K(modified_count_),
               K(sample_size_),
               K(tablet_id_),
               K(stat_expired_time_));

private:
  uint64_t table_id_;
  int64_t partition_id_;
  int64_t object_type_;

  int64_t row_count_;
  double avg_row_size_;

  int64_t sstable_row_count_;
  int64_t memtable_row_count_;
  int64_t data_size_;
  int64_t macro_block_num_;
  int64_t micro_block_num_;
  int64_t sstable_avg_row_size_;
  int64_t memtable_avg_row_size_;
  int64_t data_version_;
  int64_t last_analyzed_;
  uint64_t stattype_locked_;
  int64_t modified_count_;
  int64_t sample_size_;
  uint64_t tablet_id_;//now only use estimate table rowcnt by meta table.
  int64_t stat_expired_time_;//mark the stat in cache is arrived expired time, if arrived at expired time need reload, -1 meanings no expire forever.
};

}
}

#endif /* _OB_OPT_TABLE_STAT_H_ */
