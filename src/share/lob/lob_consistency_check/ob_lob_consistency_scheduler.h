/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_LOB_OB_LOB_CHECK_SCHEDULER_H_
#define OCEANBASE_SHARE_LOB_OB_LOB_CHECK_SCHEDULER_H_

#include "observer/table/ttl/ob_tenant_ttl_manager.h"
#include "observer/table/ttl/ob_tenant_tablet_ttl_mgr.h"
#include "ob_lob_consistency_util.h"

namespace oceanbase {
namespace share {
class ObLobCheckScheduler : public table::ObTTLTaskScheduler {
public:
  ObLobCheckScheduler()
    : ObTTLTaskScheduler(),
      last_lob_clean_time_(ObTimeUtility::current_time()),
      last_clean_ls_id_(0),
      last_clean_table_id_(0),
      in_processing_cleanup_(false)
  {
  }
  virtual ~ObLobCheckScheduler()
  {
  }

  virtual int try_add_periodic_task() override
  {
    return OB_SUCCESS;
  }
  virtual uint64_t get_tenant_task_table_id() override
  {
    return ObLobConsistencyUtil::LOB_CHECK_TENANT_TASK_TABLE_ID;
  }
  virtual uint64_t get_tenant_task_tablet_id() override
  {
    return ObLobConsistencyUtil::LOB_CHECK_TENANT_TASK_TABLET_ID;
  }
  virtual common::ObTTLType get_ttl_type() override
  {
    return common::ObTTLType::LOB_CHECK;
  }
  virtual int check_all_table_finished(bool &all_finished) override;
  virtual bool enable_scheduler() override
  {
    return true;
  }
  virtual int handle_user_ttl(const obrpc::ObTTLRequestArg &arg) override;

  virtual int move_all_task_to_history_table() override;
  virtual int calc_next_task_state(ObTTLTaskType user_cmd_type,
                                   ObTTLTaskStatus curr_state,
                                   ObTTLTaskStatus &next_state) override;

private:
  bool is_valid_lob_cmd(int32_t cmd_code)
  {
    return ObLobConsistencyUtil::is_lob_check_cmd(cmd_code);
  }
  virtual void clear_ttl_history_task_record() override;
  int clean_lob_exception_records();
  bool can_trigger_lob_clean_task() const;

private:
  int64_t last_lob_clean_time_;
  uint64_t last_clean_ls_id_;
  uint64_t last_clean_table_id_;
  bool in_processing_cleanup_;
};

class ObLobRepairScheduler : public table::ObTTLTaskScheduler {
public:
  ObLobRepairScheduler() : ObTTLTaskScheduler()
  {
  }
  virtual ~ObLobRepairScheduler()
  {
  }

  virtual int try_add_periodic_task() override
  {
    return OB_SUCCESS;
  }
  virtual uint64_t get_tenant_task_table_id() override
  {
    return ObLobConsistencyUtil::LOB_REPAIR_TENANT_TASK_TABLE_ID;
  }
  virtual uint64_t get_tenant_task_tablet_id() override
  {
    return ObLobConsistencyUtil::LOB_REPAIR_TENANT_TASK_TABLET_ID;
  }
  virtual common::ObTTLType get_ttl_type() override
  {
    return common::ObTTLType::LOB_REPAIR;
  }
  virtual int check_all_table_finished(bool &all_finished) override;
  virtual bool enable_scheduler() override
  {
    return true;
  }
  virtual int handle_user_ttl(const obrpc::ObTTLRequestArg &arg) override;
  virtual int move_all_task_to_history_table() override;
  virtual int calc_next_task_state(ObTTLTaskType user_cmd_type,
                                   ObTTLTaskStatus curr_state,
                                   ObTTLTaskStatus &next_state) override;

private:
  bool is_valid_lob_cmd(int32_t cmd_code)
  {
    return ObLobConsistencyUtil::is_lob_repair_cmd(cmd_code);
  }
};

class ObLobCheckTabletScheduler : public table::ObTabletTTLScheduler {
public:
  ObLobCheckTabletScheduler() : ObTabletTTLScheduler()
  {
  }
  virtual ~ObLobCheckTabletScheduler()
  {
  }

  virtual int64_t get_tenant_task_table_id() override
  {
    return ObLobConsistencyUtil::LOB_CHECK_TENANT_TASK_TABLE_ID;
  }

  virtual int64_t get_tenant_task_tablet_id() override
  {
    return ObLobConsistencyUtil::LOB_CHECK_TENANT_TASK_TABLET_ID;
  }

  virtual bool enable_scheduler() override
  {
    return true;
  }
  virtual common::ObTTLType get_ttl_type() const override
  {
    return common::ObTTLType::LOB_CHECK;
  }

  virtual int check_and_generate_tablet_tasks() override;
  virtual int construct_task_record_filter(const table::ObTTLTaskInfo &task_info,
                                           ObTTLStatusFieldArray &filters) override;
  virtual int generate_dag_task(table::ObTTLTaskInfo &task_info, table::ObTTLTaskParam &task_para) override;
  virtual int get_ctx_by_tablet(ObTabletID tablet_id, table::ObTTLTaskCtx *&ctx) override
  {
    return local_tenant_task_.tablet_task_map_.get_refactored(ObLobConsistencyUtil::LOB_CHECK_TABLET_ID, ctx);
  }
  virtual bool enable_dag_task() override
  {
    return enable_scheduler();
  }
  virtual bool need_cancel_task(const int ret) override;
  virtual int construct_sys_table_record(table::ObTTLTaskCtx* ctx, common::ObTTLStatus& ttl_record) override;

  // Override handle_exception_table_op to handle exception table operations
  virtual int handle_exception_table_op(table::ObTTLTaskCtx *ctx, common::ObMySQLTransaction &trans) override;
  virtual int delete_lob_record(table::ObTTLTaskInfo &task_info, common::ObMySQLTransaction &trans) override;
  virtual int set_ctx_by_tablet(ObTabletID tablet_id, table::ObTTLTaskCtx *ctx) override
  {
    return local_tenant_task_.tablet_task_map_.set_refactored(ObLobConsistencyUtil::LOB_CHECK_TABLET_ID, ctx);
  }

private:
  int handle_single_exception_table_op(ObArenaAllocator &allocator, ObJsonNode *exception_tablets_json,
                                       ObLobInconsistencyType inconsistency_type, common::ObMySQLTransaction &trans);
  int remove_consistent_exception_tablets(uint64_t ls_id, ObJsonNode *removed_tablets_json,
                                          ObLobInconsistencyType inconsistency_type, common::ObMySQLTransaction &trans);
};

class ObLobRepairTabletScheduler : public table::ObTabletTTLScheduler {
public:
  ObLobRepairTabletScheduler() : ObTabletTTLScheduler()
  {
  }
  virtual ~ObLobRepairTabletScheduler()
  {
  }

  virtual int64_t get_tenant_task_table_id() override
  {
    return ObLobConsistencyUtil::LOB_REPAIR_TENANT_TASK_TABLE_ID;
  }

  virtual int64_t get_tenant_task_tablet_id() override
  {
    return ObLobConsistencyUtil::LOB_REPAIR_TENANT_TASK_TABLET_ID;
  }
  virtual bool enable_scheduler() override
  {
    return true;
  }
  virtual common::ObTTLType get_ttl_type() const override
  {
    return common::ObTTLType::LOB_REPAIR;
  }

  virtual int check_and_generate_tablet_tasks() override;
  virtual int construct_task_record_filter(const table::ObTTLTaskInfo &task_info,
                                           ObTTLStatusFieldArray &filters) override;
  virtual int generate_dag_task(table::ObTTLTaskInfo &task_info, table::ObTTLTaskParam &task_para) override;

  virtual int get_ctx_by_tablet(ObTabletID tablet_id, table::ObTTLTaskCtx *&ctx) override
  {
    return local_tenant_task_.tablet_task_map_.get_refactored(ObLobConsistencyUtil::LOB_CHECK_TABLET_ID, ctx);
  }
  virtual bool enable_dag_task() override
  {
    return enable_scheduler();
  }
  virtual bool need_cancel_task(const int ret) override;
  // Override handle_exception_table_op to handle exception table operations
  virtual int handle_exception_table_op(table::ObTTLTaskCtx *ctx, common::ObMySQLTransaction &trans) override;
  virtual int delete_lob_record(table::ObTTLTaskInfo &task_info, common::ObMySQLTransaction &trans) override;
  virtual int set_ctx_by_tablet(ObTabletID tablet_id, table::ObTTLTaskCtx *ctx) override
  {
    return local_tenant_task_.tablet_task_map_.set_refactored(ObLobConsistencyUtil::LOB_CHECK_TABLET_ID, ctx);
  }
};

}  // namespace share
}  // namespace oceanbase
#endif
