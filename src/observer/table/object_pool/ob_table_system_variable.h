/**
 * Copyright (c) 2025 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_SYSTEM_VARIABLE_H_
#define OCEANBASE_OBSERVER_OB_TABLE_SYSTEM_VARIABLE_H_

#include "observer/table/ob_table_mode_control.h"

namespace oceanbase
{
namespace table
{

struct ObTableRelatedSysVars
{
public:
  struct StaticSysVars
  {
  public:
    StaticSysVars()
        : tenant_kv_mode_(ObKvModeType::ALL)
    {}
    virtual ~StaticSysVars() {}
  public:
    OB_INLINE ObKvModeType get_kv_mode() const
    {
      return tenant_kv_mode_;
    }
    OB_INLINE void set_kv_mode(ObKvModeType val)
    {
      tenant_kv_mode_ = val;
    }
  private: 
    ObKvModeType tenant_kv_mode_;
  };

  struct DynamicSysVars
  {
  public:
    DynamicSysVars()
        : binlog_row_image_(-1),
          kv_group_commit_batch_size_(10),
          group_rw_mode_(0),
          query_record_size_limit_(-1),
          enable_query_response_time_stats_(true),
          support_distributed_execute_(false)
    {}
    virtual ~DynamicSysVars() {}
  public:
    OB_INLINE int64_t get_binlog_row_image() const
    {
      return ATOMIC_LOAD(&binlog_row_image_);
    }
    OB_INLINE void set_binlog_row_image(int64_t val)
    {
      ATOMIC_STORE(&binlog_row_image_, val);
    }
    OB_INLINE int64_t get_kv_group_commit_batch_size() const
    {
      return ATOMIC_LOAD(&kv_group_commit_batch_size_);
    }
    OB_INLINE void set_kv_group_commit_batch_size(int64_t val)
    {
      ATOMIC_STORE(&kv_group_commit_batch_size_, val);
    }
    OB_INLINE ObTableGroupRwMode get_group_rw_mode() const
    {
      return static_cast<ObTableGroupRwMode>(ATOMIC_LOAD(&group_rw_mode_));
    }
    OB_INLINE void set_group_rw_mode(ObTableGroupRwMode val)
    {
      ATOMIC_STORE(&group_rw_mode_, static_cast<int64_t>(val));
    }
    OB_INLINE int64_t get_query_record_size_limit() const
    {
      return ATOMIC_LOAD(&query_record_size_limit_);
    }
    OB_INLINE void set_query_record_size_limit(int64_t val)
    {
      ATOMIC_STORE(&query_record_size_limit_, val);
    }
    OB_INLINE bool is_enable_query_response_time_stats() const
    {
      uint64_t old_flag = ATOMIC_LOAD(&flag_);
      return ((old_flag >> 0) & 0x1) != 0;
    }
    OB_INLINE void set_enable_query_response_time_stats(bool val)
    {
      uint64_t old_flag, new_flag;
      do {
        old_flag = ATOMIC_LOAD(&flag_);
        if (val) {
          new_flag = old_flag | (1ULL << 0);
        } else {
          new_flag = old_flag & ~(1ULL << 0);
        }
      } while (!ATOMIC_CMP_AND_EXCHANGE(&flag_, &old_flag, new_flag));
    }
    OB_INLINE bool is_support_distributed_execute() const
    {
      uint64_t old_flag = ATOMIC_LOAD(&flag_);
      return ((old_flag >> 1) & 0x1) != 0;
    }
    OB_INLINE void set_support_distributed_execute(bool val)
    {
      uint64_t old_flag, new_flag;
      do {
        old_flag = ATOMIC_LOAD(&flag_);
        if (val) {
          new_flag = old_flag | (1ULL << 1);
        } else {
          new_flag = old_flag & ~(1ULL << 1);
        }
      } while (!ATOMIC_CMP_AND_EXCHANGE(&flag_, &old_flag, new_flag));
    }
  private: 
    int64_t binlog_row_image_;
    int64_t kv_group_commit_batch_size_;
    int64_t group_rw_mode_;
    int64_t query_record_size_limit_;
    union {
      uint64_t flag_;
      struct {
        bool enable_query_response_time_stats_ : 1;
        bool support_distributed_execute_ : 1;
        int64_t reserved : 62;
      };
    };
  };
public:
  ObTableRelatedSysVars()
      : is_inited_(false)
  {}
  virtual ~ObTableRelatedSysVars() {}
  int update_sys_vars(bool only_update_dynamic_vars);
  int init();
public:
  bool is_inited_;
  ObSpinLock lock_; // Avoid repeated initialization
  StaticSysVars static_vars_;
  DynamicSysVars dynamic_vars_;
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_SYSTEM_VARIABLE_H_ */
