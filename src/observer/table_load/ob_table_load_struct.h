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

#pragma once

#include "lib/hash_func/murmur_hash.h"
#include "lib/lock/ob_mutex.h"
#include "lib/utility/ob_print_utils.h"
#include "share/table/ob_table_load_array.h"
#include "sql/resolver/cmd/ob_load_data_stmt.h"

namespace oceanbase
{
namespace common
{
class ObObj;
}
namespace observer
{

struct ObTableLoadKey
{
public:
  ObTableLoadKey() : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID) {}
  ObTableLoadKey(uint64_t tenant_id, uint64_t table_id) : tenant_id_(tenant_id), table_id_(table_id) {}
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ && common::OB_INVALID_ID != table_id_;
  }
  bool operator==(const ObTableLoadKey &other) const
  {
    return (tenant_id_ == other.tenant_id_ && table_id_ == other.table_id_);
  }
  bool operator!=(const ObTableLoadKey &other) const
  {
    return !(*this == other);
  }
  uint64_t hash() const
  {
    uint64_t hash_val = common::murmurhash(&tenant_id_, sizeof(tenant_id_), 0);
    hash_val = common::murmurhash(&table_id_, sizeof(table_id_), hash_val);
    return hash_val;
  }
  int compare(const ObTableLoadKey &other) const
  {
    if (tenant_id_ != other.tenant_id_) {
      return (tenant_id_ > other.tenant_id_ ? 1 : -1);
    } else {
      return (table_id_ != other.table_id_ ? (table_id_ > other.table_id_ ? 1 : -1) : 0);
    }
  }
  TO_STRING_KV(K_(tenant_id), K_(table_id));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
};

struct ObTableLoadUniqueKey
{
  OB_UNIS_VERSION(1);

public:
  ObTableLoadUniqueKey() : table_id_(common::OB_INVALID_ID), task_id_(0) {}
  ObTableLoadUniqueKey(uint64_t table_id, int64_t task_id) : table_id_(table_id), task_id_(task_id)
  {
  }
  ObTableLoadUniqueKey(const ObTableLoadUniqueKey &key)
  {
    table_id_ = key.table_id_;
    task_id_ = key.task_id_;
  }

  bool is_valid() const { return common::OB_INVALID_ID != table_id_ && 0 != task_id_; }
  bool operator==(const ObTableLoadUniqueKey &other) const
  {
    return (table_id_ == other.table_id_ && task_id_ == other.task_id_);
  }
  bool operator!=(const ObTableLoadUniqueKey &other) const
  {
    return !(*this == other);
  }
  ObTableLoadUniqueKey &operator=(const ObTableLoadUniqueKey& other)
  {
    if (this == &other)
    {
      return *this;
    }
    this->table_id_ = other.table_id_;
    this->task_id_ = other.task_id_;
    return *this;
  }
  uint64_t hash() const
  {
    return common::murmurhash(this, sizeof(*this), 0);
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return OB_SUCCESS;
  }
  int compare(const ObTableLoadUniqueKey &other) const
  {
    if (table_id_ != other.table_id_) {
      return (table_id_ > other.table_id_ ? 1 : -1);
    } else {
      return (task_id_ != other.task_id_ ? (task_id_ > other.task_id_ ? 1 : -1) : 0);
    }
  }
  TO_STRING_KV(K_(table_id), K_(task_id));
public:
  uint64_t table_id_;
  int64_t task_id_;
};

enum class ObTableLoadExeMode {
  FAST_HEAP_TABLE = 0,
  GENERAL_TABLE_COMPACT = 1,
  MULTIPLE_HEAP_TABLE_COMPACT = 2,
  MEM_COMPACT = 3,
  MAX_TYPE
};

struct ObTableLoadParam
{
  ObTableLoadParam()
    : tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      parallel_(0),
      session_count_(0),
      batch_size_(0),
      max_error_row_count_(0),
      sql_mode_(0),
      column_count_(0),
      need_sort_(false),
      px_mode_(false),
      online_opt_stat_gather_(false),
      dup_action_(sql::ObLoadDupActionType::LOAD_INVALID_MODE),
      avail_memory_(0),
      write_session_count_(0),
      exe_mode_(ObTableLoadExeMode::MAX_TYPE)
  {
  }

  int normalize()
  {
    int ret = common::OB_SUCCESS;
    if (need_sort_) {
      if (session_count_ < 2) {
        session_count_ = 2; //排序至少要两个线程才能工作
      }
    }
    return ret;
  }

  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_ &&
           common::OB_INVALID_ID != table_id_ &&
           parallel_ > 0 &&
           session_count_ > 0 &&
           batch_size_ > 0 &&
           column_count_ > 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(parallel), K_(session_count), K_(batch_size),
               K_(max_error_row_count), K_(sql_mode), K_(column_count), K_(need_sort), K_(px_mode),
               K_(online_opt_stat_gather), K_(dup_action), K_(avail_memory), K_(write_session_count), K_(exe_mode));
public:
  uint64_t tenant_id_;
  uint64_t table_id_;
  int64_t parallel_;
  int32_t session_count_;
  int32_t batch_size_;
  uint64_t max_error_row_count_;
  uint64_t sql_mode_;
  int32_t column_count_;
  bool need_sort_;
  bool px_mode_;
  bool online_opt_stat_gather_;
  sql::ObLoadDupActionType dup_action_;
  int64_t avail_memory_;
  int32_t write_session_count_;
  ObTableLoadExeMode exe_mode_;
};

struct ObTableLoadDDLParam
{
public:
  ObTableLoadDDLParam()
    : dest_table_id_(common::OB_INVALID_ID),
      task_id_(0),
      schema_version_(0),
      snapshot_version_(0),
      data_version_(0),
      cluster_version_(0)
  {
  }
  void reset()
  {
    dest_table_id_ = common::OB_INVALID_ID;
    task_id_ = 0;
    schema_version_ = 0;
    snapshot_version_ = 0;
    data_version_ = 0;
  }
  bool is_valid() const
  {
    return common::OB_INVALID_ID != dest_table_id_ && 0 != task_id_ && 0 != schema_version_ &&
           0 != snapshot_version_ && 0 != data_version_ && 0 != cluster_version_;
  }
  TO_STRING_KV(K_(dest_table_id), K_(task_id), K_(schema_version), K_(snapshot_version),
               K_(data_version), K(cluster_version_));
public:
  uint64_t dest_table_id_;
  int64_t task_id_;
  int64_t schema_version_;
  int64_t snapshot_version_;
  int64_t data_version_;
  uint64_t cluster_version_;
};

class ObTableLoadMutexGuard
{
public:
  ObTableLoadMutexGuard() : mutex_(nullptr) {}
  ~ObTableLoadMutexGuard()
  {
    reset();
  }
  void reset()
  {
    int ret = common::OB_SUCCESS;
    if (nullptr != mutex_) {
      if (OB_FAIL(mutex_->unlock())) {
      }
      mutex_ = nullptr;
    }
  }
  int init(lib::ObMutex &mutex)
  {
    int ret = common::OB_SUCCESS;
    reset();
    if (OB_FAIL(mutex.lock())) {
      SERVER_LOG(WARN, "fail to lock mutex", KR(ret));
    } else {
      mutex_ = &mutex;
    }
    return ret;
  }
  bool is_valid() const { return nullptr != mutex_; }
  TO_STRING_KV(KP_(mutex));
private:
  lib::ObMutex *mutex_;
};

}  // namespace observer
}  // namespace oceanbase
