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

#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/table_load/ob_table_load_exec_ctx.h"
#include "observer/table_load/ob_table_load_object_allocator.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_define.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "sql/session/ob_sql_session_info.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace observer
{
class ObITableLoadTaskScheduler;
class ObTableLoadCoordinatorCtx;
class ObTableLoadStoreCtx;
class ObTableLoadTask;
class ObTableLoadTransCtx;

class ObTableLoadTableCtx : public common::ObDLinkBase<ObTableLoadTableCtx>
{
public:
  ObTableLoadTableCtx();
  ~ObTableLoadTableCtx();
  int init(const ObTableLoadParam &param, const ObTableLoadDDLParam &ddl_param, sql::ObSQLSessionInfo *session_info);
  void stop();
  void destroy();
  bool is_valid() const { return is_inited_; }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
  bool is_assigned_resource() const { return is_assigned_resource_; }
  void set_assigned_resource() { is_assigned_resource_ = true; }
  bool is_assigned_memory() const { return is_assigned_memory_; }
  void set_assigned_memory() { is_assigned_memory_ = true; }
  bool is_dirty() const { return is_dirty_; }
  void set_dirty() { is_dirty_ = true; }
  bool is_mark_delete() const { return mark_delete_; }
  void mark_delete() { mark_delete_ = true; }
  bool is_stopped() const;
  TO_STRING_KV(K_(param),
               KP_(coordinator_ctx),
               KP_(store_ctx),
               "ref_count", get_ref_count(),
               K_(is_assigned_resource),
               K_(is_assigned_memory),
               K_(mark_delete),
               K_(is_dirty),
               K_(is_inited));
public:
  int init_coordinator_ctx(const common::ObIArray<uint64_t> &column_ids,
                           ObTableLoadExecCtx *exec_ctx);
  int init_store_ctx(
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
public:
  int alloc_task(ObTableLoadTask *&task);
  void free_task(ObTableLoadTask *task);
  ObTableLoadTransCtx *alloc_trans_ctx(const table::ObTableLoadTransId &trans_id);
  void free_trans_ctx(ObTableLoadTransCtx *trans_ctx);
private:
  int register_job_stat();
  void unregister_job_stat();
public:
  ObTableLoadParam param_;
  ObTableLoadDDLParam ddl_param_;
  ObTableLoadSchema schema_;
  ObTableLoadCoordinatorCtx *coordinator_ctx_; // 只在控制节点构造
  ObTableLoadStoreCtx *store_ctx_; // 只在数据节点构造
  sql::ObLoadDataGID gid_;
  sql::ObLoadDataStat *job_stat_;
  sql::ObSQLSessionInfo *session_info_;
  sql::ObFreeSessionCtx free_session_ctx_;
private:
  // 只在初始化的时候使用, 线程不安全
  common::ObArenaAllocator allocator_;
  ObTableLoadObjectAllocator<ObTableLoadTask> task_allocator_; // 多线程安全
  ObTableLoadObjectAllocator<ObTableLoadTransCtx> trans_ctx_allocator_; // 多线程安全
  int64_t ref_count_ CACHE_ALIGNED;
  bool is_assigned_resource_;
  bool is_assigned_memory_;
  bool mark_delete_;
  volatile bool is_dirty_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadTableCtx);
};

}  // namespace observer
}  // namespace oceanbase
