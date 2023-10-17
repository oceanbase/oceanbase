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

#include "common/object/ob_object.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_sql_statistics.h"
#include "share/table/ob_table_load_row_array.h"

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
  static const int64_t WAIT_INTERVAL_US = 1LL * 1000 * 1000; // 1s
  static const int64_t DEFAULT_TIMEOUT_US = 10LL * 1000 * 1000; // 10s
  static const int64_t HEART_BEAT_RPC_TIMEOUT_US = 1LL * 1000 * 1000; // 1s
public:
  ObTableLoadCoordinator(ObTableLoadTableCtx *ctx);
  static bool is_ctx_inited(ObTableLoadTableCtx *ctx);
  static int init_ctx(ObTableLoadTableCtx *ctx, const common::ObIArray<int64_t> &idx_array,
                      ObTableLoadExecCtx *exec_ctx);
  static void abort_ctx(ObTableLoadTableCtx *ctx);
  int init();
  bool is_valid() const { return is_inited_; }
private:
  static int abort_active_trans(ObTableLoadTableCtx *ctx);
  static int abort_peers_ctx(ObTableLoadTableCtx *ctx);
  static int abort_redef_table(ObTableLoadTableCtx *ctx);

// table load ctrl interface
public:
  int begin();
  int finish();
  int commit(table::ObTableLoadResultInfo &result_info);
  int px_commit_data();
  int px_commit_ddl();
  int get_status(table::ObTableLoadStatusType &status, int &error_code);
  int heart_beat();
private:
  int pre_begin_peers();
  int confirm_begin_peers();
  int pre_merge_peers();
  int start_merge_peers();
  int commit_peers();
  int commit_redef_table();
  int drive_sql_stat(sql::ObExecContext *ctx);
  int heart_beat_peer();
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
  int add_check_peers_trans_commit_task(ObTableLoadCoordinatorTrans *trans);
  int check_peers_trans_commit(ObTableLoadCoordinatorTrans *trans, bool &is_commit);
  class CheckPeersTransCommitTaskProcessor;
  class CheckPeersTransCommitTaskCallback;

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
