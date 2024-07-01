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

#ifndef OB_SQL_MEM_MGR_PROCESSOR_H
#define OB_SQL_MEM_MGR_PROCESSOR_H

#include "share/rc/ob_tenant_base.h"
#include "ob_tenant_sql_memory_manager.h"
#include "sql/engine/basic/ob_chunk_row_store.h"

namespace oceanbase {
namespace sql {

class ObSqlMemMgrProcessor : public ObSqlMemoryCallback
{
private:
  using PredFunc = std::function<bool(int64_t)>;
public:
  ObSqlMemMgrProcessor(ObSqlWorkAreaProfile &profile, ObMonitorNode &op_monitor_info) :
    profile_(profile), op_monitor_info_(&op_monitor_info),
    sql_mem_mgr_(nullptr), mem_callback_(nullptr), tenant_id_(OB_INVALID_ID),
    periodic_cnt_(1024), origin_max_mem_size_(0), default_available_mem_size_(0),
    is_auto_mgr_(false), dir_id_(0),
    dummy_ptr_(nullptr), dummy_alloc_(nullptr)
  {
    // trace memory dump
    op_monitor_info_->otherstat_6_id_ = ObSqlMonitorStatIds::MEMORY_DUMP;
  }

  ObSqlMemMgrProcessor(ObSqlWorkAreaProfile &profile) :
    profile_(profile), op_monitor_info_(nullptr),
    sql_mem_mgr_(nullptr), mem_callback_(nullptr), tenant_id_(OB_INVALID_ID),
    periodic_cnt_(1024), origin_max_mem_size_(0), default_available_mem_size_(0),
    is_auto_mgr_(false), dir_id_(0),
    dummy_ptr_(nullptr), dummy_alloc_(nullptr) {}
  virtual ~ObSqlMemMgrProcessor() {}

  void set_sql_mem_mgr(ObTenantSqlMemoryManager *sql_mem_mgr)
  {
    sql_mem_mgr_ = sql_mem_mgr;
  }

  OB_INLINE ObTenantSqlMemoryManager *get_sql_mem_mgr()
  {
    if (nullptr == sql_mem_mgr_) {
      sql_mem_mgr_ = MTL(ObTenantSqlMemoryManager*);
      if (OB_NOT_NULL(sql_mem_mgr_)) {
        mem_callback_ = sql_mem_mgr_->get_sql_memory_callback();
      }
    }
    return sql_mem_mgr_;
  }

  // register and set cache size
  int init(
    ObIAllocator *allocator,
    uint64_t tenant_id,
    int64_t cache_size,
    const ObPhyOperatorType op_type,
    const uint64_t op_id,
    ObSqlProfileExecInfo exec_info);

  void destroy()
  {
    if (0 < profile_.mem_used_ && OB_NOT_NULL(mem_callback_)) {
      mem_callback_->free(profile_.mem_used_);
    }
    mem_callback_ = nullptr;
    sql_mem_mgr_ = nullptr;
    profile_.mem_used_ = 0;
  }
  void reset()
  {
    periodic_cnt_ = 1024;
    is_auto_mgr_ = false;
  }
  OB_INLINE bool is_auto_mgr() const { return is_auto_mgr_; }
  int get_max_available_mem_size(ObIAllocator *allocator);
  int update_max_available_mem_size_periodically(
    ObIAllocator *allocator,
    PredFunc predicate,
    bool &updated);
  int update_cache_size(ObIAllocator *allocator, int64_t cache_size);
  int extend_max_memory_size(ObIAllocator *allocator, PredFunc dump_fun, bool &need_dump,
    int64_t mem_used, int64_t max_times = 1024);

  int update_used_mem_size(int64_t used_size);

  void set_periodic_cnt(int64_t cnt) { periodic_cnt_ = cnt; }
  int64_t get_periodic_cnt() const { return periodic_cnt_; }

  double get_data_ratio() const { return profile_.data_ratio_; }
  void set_data_ratio(double ratio) { profile_.data_ratio_ = ratio; }

  void unregister_profile();
  void set_default_usable_mem_size(int64_t max_usable_mem_size)
  { default_available_mem_size_ = max_usable_mem_size; }
  int64_t get_mem_bound() const
  { return is_auto_mgr_ ? profile_.get_expect_size() : default_available_mem_size_; }

  void alloc(int64_t size)
  {
    profile_.delta_size_ += size;
    update_memory_delta_size(profile_.delta_size_);
  }

  void free(int64_t size)
  {
    profile_.delta_size_ -= size;
    update_memory_delta_size(profile_.delta_size_);
  }

  void dumped(int64_t delta_size)
  {
    profile_.dumped_size_ += delta_size;
    if (OB_NOT_NULL(op_monitor_info_)) {
      op_monitor_info_->otherstat_6_value_ += delta_size;
      op_monitor_info_->update_tempseg(delta_size);
    }
    profile_.max_dumped_size_ = MAX(profile_.max_dumped_size_, profile_.dumped_size_);
    if (OB_NOT_NULL(mem_callback_)) {
      mem_callback_->dumped(delta_size);
    }
  }
  int64_t get_dumped_size() const { return profile_.dumped_size_; }
  int64_t get_max_dumped_size() const { return profile_.max_dumped_size_; }
  void reset_delta_size() { profile_.delta_size_ = 0; }
  int64_t get_mem_used() const { return profile_.mem_used_; }
  int64_t get_delta_size() const { return profile_.delta_size_; }
  int64_t get_data_size() const { return profile_.data_size_ + profile_.delta_size_; }
  void update_memory_delta_size(int64_t delta_size)
  {
    if (delta_size > 0 && delta_size >= UPDATED_DELTA_SIZE) {
      if (OB_NOT_NULL(mem_callback_)) {
        mem_callback_->alloc(delta_size);
      }
      profile_.mem_used_ += delta_size;
      if (OB_NOT_NULL(op_monitor_info_)) {
        op_monitor_info_->update_memory(delta_size);
      }
      if (profile_.max_mem_used_ < profile_.mem_used_) {
        profile_.max_mem_used_ = profile_.mem_used_;
      }
      profile_.delta_size_ = 0;
      profile_.data_size_ += delta_size;
    } else if (delta_size < 0 && -delta_size >= UPDATED_DELTA_SIZE) {
      if (OB_NOT_NULL(mem_callback_)) {
        mem_callback_->free(-delta_size);
      }
      profile_.mem_used_ += delta_size;
      if (OB_NOT_NULL(op_monitor_info_)) {
        op_monitor_info_->update_memory(delta_size);
      }
      profile_.delta_size_ = 0;
      profile_.data_size_ += delta_size;
    }
  }

  ObSqlWorkAreaProfile &get_profile() const { return profile_; }

  int alloc_dir_id(int64_t &dir_id);
  int64_t get_dir_id()
  {
    if (dir_id_ < -1) {
      SQL_ENG_LOG_RET(ERROR, common::OB_ERR_UNEXPECTED, "get unexpected dir id", K(dir_id_));
    }
    return dir_id_;
  }
  void set_number_pass(int32_t num_pass) { profile_.set_number_pass(num_pass); }
  bool is_unregistered() const { return nullptr == sql_mem_mgr_; }
  void unregister_profile_if_necessary();
private:
  int try_upgrade_auto_mgr(ObIAllocator *allocator, int64_t mem_used);
private:
  static const int64_t MAX_SQL_MEM_SIZE = 2 * 1024 * 1024; // 2M
  static const int64_t UPDATED_DELTA_SIZE = 1 * 1024 * 1024;
  static const int64_t EXTEND_RATIO = 10;
  ObSqlWorkAreaProfile &profile_;
  ObMonitorNode *op_monitor_info_;
  ObTenantSqlMemoryManager *sql_mem_mgr_;
  ObSqlMemoryCallback *mem_callback_;
  uint64_t tenant_id_;
  int64_t periodic_cnt_;
  int64_t origin_max_mem_size_;
  int64_t default_available_mem_size_;
  bool is_auto_mgr_;
  int64_t dir_id_;
  char *dummy_ptr_;
  ObIAllocator *dummy_alloc_;
};

class ObSqlWorkareaUtil
{
public:
  static int get_workarea_size(
    const ObSqlWorkAreaType wa_type,
    const int64_t tenant_id,
    ObExecContext *exec_ctx,
    int64_t &value
  );

  static int get_workarea_size(
    const ObSqlWorkAreaType wa_type,
    const int64_t tenant_id,
    ObSqlProfileExecInfo &exec_info,
    int64_t &value
  );

  static int get_workarea_size(
    const ObSqlWorkAreaType wa_type,
    const int64_t tenant_id,
    int64_t &value,
    ObSQLSessionInfo *session = nullptr
  );
};

} // sql
} // oceanbase
#endif /* OB_SQL_MEM_MGR_PROCESSOR_H */
