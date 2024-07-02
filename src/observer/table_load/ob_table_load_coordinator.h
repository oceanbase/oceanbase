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

#include "lib/ob_define.h"
#include "share/io/ob_io_define.h"
#include "common/object/ob_object.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_dml_stat.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "share/table/ob_table_load_row_array.h"
#include "observer/table_load/resource/ob_table_load_resource_rpc_struct.h"
#include "observer/table_load/resource/ob_table_load_resource_rpc_proxy.h"
#include "observer/table_load/resource/ob_table_load_resource_service.h"
#include "observer/table_load/ob_table_load_assigned_memory_manager.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadExecCtx;
class ObTableLoadTableCtx;
class ObTableLoadCoordinatorCtx;
class ObTableLoadCoordinatorTrans;

class ObTableLoadCoordinator
{
  static const int64_t WAIT_INTERVAL_US = 3 * 1000 * 1000; // 3s
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000; // 10s
  static const int64_t HEART_BEAT_RPC_TIMEOUT_US = 1LL * 1000 * 1000; // 1s
  // 申请和释放资源失败等待间隔时间
  static const int64_t RESOURCE_OP_WAIT_INTERVAL_US = 5 * 1000LL * 1000LL; // 5s
  static const int64_t SSTABLE_BUFFER_SIZE = 20 * 1024LL;;  // 20KB
  static const int64_t MACROBLOCK_BUFFER_SIZE = 10 * 1024LL * 1024LL;  // 10MB
public:
  ObTableLoadCoordinator(ObTableLoadTableCtx *ctx);
  static bool is_ctx_inited(ObTableLoadTableCtx *ctx);
  static int init_ctx(ObTableLoadTableCtx *ctx,
                      const common::ObIArray<uint64_t> &column_ids,
                      ObTableLoadExecCtx *exec_ctx);
  static void abort_ctx(ObTableLoadTableCtx *ctx);
  int init();
  bool is_valid() const { return is_inited_; }
private:
  static int abort_active_trans(ObTableLoadTableCtx *ctx);
  static int abort_peers_ctx(ObTableLoadTableCtx *ctx);

// table load ctrl interface
public:
  int begin();
  int finish();
  int commit(table::ObTableLoadResultInfo &result_info);
  int get_status(table::ObTableLoadStatusType &status, int &error_code);
  int heart_beat();
private:
  int gen_apply_arg(ObDirectLoadResourceApplyArg &apply_arg);
  int pre_begin_peers(ObDirectLoadResourceApplyArg &apply_arg);
  int confirm_begin_peers();
  int commit_peers(table::ObTableLoadSqlStatistics &sql_statistics,
                   table::ObTableLoadDmlStat &dml_stats);
  int build_table_stat_param(ObTableStatParam &param,
                             common::ObIAllocator &allocator);
  int write_sql_stat(table::ObTableLoadSqlStatistics &sql_statistics,
                     table::ObTableLoadDmlStat &dml_stats);
  int heart_beat_peer();
private:
  int add_check_begin_result_task();
  int check_peers_begin_result(bool &is_finish);
  class CheckBeginResultTaskProcessor;
  class CheckBeginResultTaskCallback;
public:
  int pre_merge_peers();
  int start_merge_peers();
private:
  int add_check_merge_result_task();
  int check_peers_merge_result(bool &is_finish);
  class CheckMergeResultTaskProcessor;
  class CheckMergeResultTaskCallback;

// trans ctrl interface
public:
  int start_trans(const table::ObTableLoadSegmentID &segment_id,
                  table::ObTableLoadTransId &trans_id);
  int finish_trans(const table::ObTableLoadTransId &trans_id);
  int commit_trans(ObTableLoadCoordinatorTrans *trans);
  int abandon_trans(const table::ObTableLoadTransId &trans_id);
  int get_trans_status(const table::ObTableLoadTransId &trans_id,
                       table::ObTableLoadTransStatusType &trans_status,
                       int &error_code);
private:
  int pre_start_trans_peers(ObTableLoadCoordinatorTrans *trans);
  int confirm_start_trans_peers(ObTableLoadCoordinatorTrans *trans);
  // - OB_SUCCESS: 成功
  // - OB_EAGAIN: 重试
  // - else: 失败
  int pre_finish_trans_peers(ObTableLoadCoordinatorTrans *trans);
  int confirm_finish_trans_peers(ObTableLoadCoordinatorTrans *trans);
  int abandon_trans_peers(ObTableLoadCoordinatorTrans *trans);
public:
  int finish_trans_peers(ObTableLoadCoordinatorTrans *trans);
private:
  int check_peers_trans_commit(ObTableLoadCoordinatorTrans *trans, bool &is_commit);
  int check_trans_commit(ObTableLoadCoordinatorTrans *trans);

// write interface
public:
  int write(const table::ObTableLoadTransId &trans_id, int32_t session_id, uint64_t sequence_no,
            const table::ObTableLoadObjRowArray &obj_rows);
  // for client
  int write(const table::ObTableLoadTransId &trans_id, const table::ObTableLoadObjRowArray &obj_rows);
  int flush(ObTableLoadCoordinatorTrans *trans);
  // 只写到主节点
  int write_peer_leader(const table::ObTableLoadTransId &trans_id, int32_t session_id,
                        uint64_t sequence_no, const table::ObTableLoadTabletObjRowArray &tablet_obj_rows,
                        const common::ObAddr &addr);
private:
  class WriteTaskProcessor;
  class WriteTaskCallback;
  class FlushTaskProcessor;
  class FlushTaskCallback;

private:
  ObTableLoadTableCtx * const ctx_;
  const ObTableLoadParam &param_;
  ObTableLoadCoordinatorCtx * const coordinator_ctx_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadCoordinator);
};

}  // namespace observer
}  // namespace oceanbase
