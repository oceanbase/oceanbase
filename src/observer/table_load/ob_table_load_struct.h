// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <suzhi.yt@oceanbase.com>

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

static const int32_t MAX_TABLE_LOAD_SESSION_COUNT = 128;

enum class ObTableLoadDataType : int32_t
{
  OBJ_ARRAY = 0,  //obobj[]
  STR_ARRAY = 1,  //string[]
  RAW_STRING = 2,  //string
  MAX_DATA_TYPE
};

struct ObTableLoadKey
{
public:
  ObTableLoadKey() : tenant_id_(common::OB_INVALID_ID), table_id_(common::OB_INVALID_ID) {}
  ObTableLoadKey(uint64_t tenant_id, uint64_t table_id) : tenant_id_(tenant_id), table_id_(table_id) {}
  uint64_t tenant_id_;
  uint64_t table_id_;
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
    return (tenant_id_ != other.tenant_id_ ? tenant_id_ - other.tenant_id_
                                           : table_id_ - other.table_id_);
  }
  TO_STRING_KV(K_(tenant_id), K_(table_id));
};

struct ObTableLoadParam
{
  ObTableLoadParam()
    : tenant_id_(common::OB_INVALID_ID),
      table_id_(common::OB_INVALID_ID),
      target_table_id_(common::OB_INVALID_ID),
      session_count_(0),
      batch_size_(0),
      max_error_row_count_(0),
      sql_mode_(0),
      column_count_(0),
      need_sort_(false),
      px_mode_(false),
      online_opt_stat_gather_(false),
      data_type_(ObTableLoadDataType::RAW_STRING),
      dup_action_(sql::ObLoadDupActionType::LOAD_INVALID_MODE)
  {
  }
  uint64_t tenant_id_;
  uint64_t table_id_;
  uint64_t target_table_id_;
  int32_t session_count_;
  int32_t batch_size_;
  uint64_t max_error_row_count_;
  uint64_t sql_mode_;
  int32_t column_count_;
  bool need_sort_;
  bool px_mode_;
  bool online_opt_stat_gather_;
  ObTableLoadDataType data_type_;
  sql::ObLoadDupActionType dup_action_;

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
           //common::OB_INVALID_ID != target_table_id_ &&
           session_count_ > 0 && session_count_ <= MAX_TABLE_LOAD_SESSION_COUNT &&
           batch_size_ > 0 &&
           column_count_ > 0;
  }

  TO_STRING_KV(K_(tenant_id), K_(table_id), K_(target_table_id), K_(session_count),
               K_(batch_size), K_(max_error_row_count), K_(column_count),
               K_(need_sort), K_(px_mode), K_(data_type), K_(dup_action));
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
        SERVER_LOG(WARN, "fail to unlock mutex", KR(ret));
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
