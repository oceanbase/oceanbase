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

#include "lib/net/ob_addr.h"
#include "observer/table_load/ob_table_load_struct.h"
#include "share/table/ob_table_load_array.h"
#include "share/table/ob_table_load_define.h"
#include "share/table/ob_table_load_row_array.h"
#include "share/table/ob_table_load_sql_statistics.h"

namespace oceanbase
{
namespace observer
{
class ObTableLoadTableCtx;
class ObTableLoadStoreCtx;
class ObTableLoadStoreTrans;

class ObTableLoadStore
{
public:
  ObTableLoadStore(ObTableLoadTableCtx *ctx);
  static int init_ctx(
    ObTableLoadTableCtx *ctx,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &partition_id_array,
    const table::ObTableLoadArray<table::ObTableLoadLSIdAndPartitionId> &target_partition_id_array);
  static void abort_ctx(ObTableLoadTableCtx *ctx, bool &is_stopped);
  int init();
private:
  static int abort_active_trans(ObTableLoadTableCtx *ctx);

// table load ctrl interface
public:
  int pre_begin();
  int confirm_begin();
  int pre_merge(const table::ObTableLoadArray<table::ObTableLoadTransId> &committed_trans_id_array);
  int start_merge();
  int commit(table::ObTableLoadResultInfo &result_info);
  int get_status(table::ObTableLoadStatusType &status, int &error_code);
  int heart_beat();
private:
  int commit_sql_statistics(const table::ObTableLoadSqlStatistics &sql_statistics);
private:
  class MergeTaskProcessor;
  class MergeTaskCallback;

// trans ctrl interface
public:
  int pre_start_trans(const table::ObTableLoadTransId &trans_id);
  int confirm_start_trans(const table::ObTableLoadTransId &trans_id);
  int pre_finish_trans(const table::ObTableLoadTransId &trans_id);
  int confirm_finish_trans(const table::ObTableLoadTransId &trans_id);
  int abandon_trans(const table::ObTableLoadTransId &trans_id);
  int get_trans_status(const table::ObTableLoadTransId &trans_id,
                       table::ObTableLoadTransStatusType &trans_status,
                       int &error_code);
private:
  int clean_up_trans(ObTableLoadStoreTrans *trans);
  class CleanUpTaskProcessor;
  class CleanUpTaskCallback;

// write interface
public:
  int write(const table::ObTableLoadTransId &trans_id, int32_t session_id, uint64_t sequence_no,
            const table::ObTableLoadTabletObjRowArray &row_array);
  int flush(ObTableLoadStoreTrans *trans);
private:
  class WriteTaskProcessor;
  class WriteTaskCallback;
  class FlushTaskProcessor;
  class FlushTaskCallback;

// px trans interface
public:
  int px_start_trans(const table::ObTableLoadTransId &trans_id);
  int px_finish_trans(const table::ObTableLoadTransId &trans_id);
  int px_write(const table::ObTableLoadTransId &trans_id,
               const ObTabletID &tablet_id,
               const common::ObIArray<common::ObNewRow> &row_array);
  static int px_abandon_trans(ObTableLoadTableCtx *ctx, const table::ObTableLoadTransId &trans_id);
private:
  int px_flush(ObTableLoadStoreTrans *trans);
  static int px_clean_up_trans(ObTableLoadStoreTrans *trans);

private:
  ObTableLoadTableCtx * const ctx_;
  const ObTableLoadParam &param_;
  ObTableLoadStoreCtx * const store_ctx_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadStore);
};

}  // namespace observer
}  // namespace oceanbase
