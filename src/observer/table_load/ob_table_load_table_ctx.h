// Copyright (c) 2022-present Oceanbase Inc. All Rights Reserved.
// Author:
//   suzhi.yt <>

#pragma once

#include "lib/list/ob_dlink_node.h"
#include "lib/utility/ob_print_utils.h"
#include "observer/table_load/ob_table_load_schema.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_define.h"
#include "sql/engine/cmd/ob_load_data_utils.h"
#include "observer/table_load/ob_table_load_object_allocator.h"
#include "sql/session/ob_sql_session_info.h"

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
  int init(const ObTableLoadParam &param, const ObTableLoadDDLParam &ddl_param);
  void stop();
  void destroy();
  bool is_valid() const { return is_inited_; }
  int64_t get_ref_count() const { return ATOMIC_LOAD(&ref_count_); }
  int64_t inc_ref_count() { return ATOMIC_AAF(&ref_count_, 1); }
  int64_t dec_ref_count() { return ATOMIC_AAF(&ref_count_, -1); }
  bool is_dirty() const { return is_dirty_; }
  void set_dirty() { is_dirty_ = true; }
  TO_STRING_KV(K_(param), KP_(coordinator_ctx), KP_(store_ctx), "ref_count", get_ref_count(),
               K_(is_dirty), K_(is_inited));
public:
  int init_coordinator_ctx(const common::ObIArray<int64_t> &idx_array, uint64_t user_id);
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
private:
  // 只在初始化的时候使用, 线程不安全
  common::ObArenaAllocator allocator_;
  ObTableLoadObjectAllocator<ObTableLoadTask> task_allocator_; // 多线程安全
  ObTableLoadObjectAllocator<ObTableLoadTransCtx> trans_ctx_allocator_; // 多线程安全
  int64_t ref_count_ CACHE_ALIGNED;
  volatile bool is_dirty_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadTableCtx);
};

}  // namespace observer
}  // namespace oceanbase
