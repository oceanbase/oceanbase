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

#include "share/table/ob_table_load_row_array.h"
#include "share/table/ob_table_load_define.h"
#include "sql/engine/cmd/ob_load_data_utils.h"

namespace oceanbase
{
namespace transaction
{
class ObTxDesc;
struct ObTxParam;
}
namespace observer
{
class ObTableLoadParam;
class ObTableLoadTableCtx;
class ObTableLoadExecCtx;
class ObTableLoadDDLParam;

class ObTableLoadInstance
{
  static const int64_t WAIT_INTERVAL_US = 3 * 1000 * 1000; // 3s
public:
  ObTableLoadInstance();
  ~ObTableLoadInstance();
  void destroy();
  int init(ObTableLoadParam &param, const common::ObIArray<int64_t> &idx_array,
           ObTableLoadExecCtx *execute_ctx);
  int write(int32_t session_id, const table::ObTableLoadObjRowArray &obj_rows);
  int commit();
  int px_commit_data();
  int px_commit_ddl();
  const ObTableLoadTableCtx* get_table_ctx() { return table_ctx_; }
  sql::ObLoadDataStat *get_job_stat() const { return job_stat_; }
  const table::ObTableLoadResultInfo &get_result_info() const { return result_info_; }
private:
  int start_stmt(const ObTableLoadParam &param);
  int end_stmt(const bool commit);
  // incremental
  static int64_t get_stmt_expire_ts(sql::ObSQLSessionInfo *session_info);
  int build_tx_param();
  int start_sql_tx();
  int end_sql_tx(const bool commit);
  // abort tx is async, use rollback savepoint to sync release table lock
  int create_implicit_savepoint();
  int rollback_to_implicit_savepoint();
  int lock_table_in_tx();
  int init_ddl_param_for_inc_direct_load();
  // full
  int start_redef_table(const ObTableLoadParam &param);
  int commit_redef_table();
  int abort_redef_table();
private:
  // direct load
  int start_direct_load(const ObTableLoadParam &param, const common::ObIArray<int64_t> &idx_array);
  int end_direct_load(const bool commit);
  int add_tx_result_to_user_session();
  int start_trans();
  int write_trans(int32_t session_id, const table::ObTableLoadObjRowArray &obj_rows);
  int commit_trans();
private:
  struct StmtCtx
  {
  public:
    StmtCtx()
      : tenant_id_(OB_INVALID_TENANT_ID),
        table_id_(OB_INVALID_ID),
        session_info_(nullptr),
        tx_desc_(nullptr),
        is_incremental_(false),
        use_insert_into_select_tx_(false),
        is_started_(false),
        has_added_tx_result_(false)
    {
    }
    void reset()
    {
      tenant_id_ = OB_INVALID_TENANT_ID;
      table_id_ = OB_INVALID_ID;
      ddl_param_.reset();
      session_info_ = nullptr;
      tx_desc_ = nullptr;
      // tx_param_.reset();
      savepoint_.reset();
      is_incremental_ = false;
      use_insert_into_select_tx_ = false;
      is_started_ = false;
      has_added_tx_result_ = false;
    }
    bool is_started() const { return is_started_; }
    TO_STRING_KV(K_(tenant_id),
                 K_(table_id),
                 K_(ddl_param),
                 KP_(session_info),
                 KPC_(tx_desc),
                 K_(tx_param),
                 K_(savepoint),
                 KP_(is_incremental),
                 KP_(use_insert_into_select_tx),
                 KP_(is_started),
                 KP_(has_added_tx_result));
  public:
    uint64_t tenant_id_;
    uint64_t table_id_;
    ObTableLoadDDLParam ddl_param_;
    sql::ObSQLSessionInfo *session_info_;
    transaction::ObTxDesc *tx_desc_;
    transaction::ObTxParam tx_param_;
    transaction::ObTxSEQ savepoint_;
    bool is_incremental_;
    bool use_insert_into_select_tx_; // whether use the transaction of insert into select
    bool is_started_;
    bool has_added_tx_result_;
  };
  struct TransCtx
  {
  public:
    void reset()
    {
      trans_id_.reset();
      next_sequence_no_array_.reset();
    }
  public:
    table::ObTableLoadTransId trans_id_;
    table::ObTableLoadArray<uint64_t> next_sequence_no_array_;
  };
private:
  static const int64_t DEFAULT_SEGMENT_ID = 1;
  ObTableLoadExecCtx *execute_ctx_;
  common::ObIAllocator *allocator_;
  ObTableLoadTableCtx *table_ctx_;
  sql::ObLoadDataStat *job_stat_;
  StmtCtx stmt_ctx_;
  TransCtx trans_ctx_;
  table::ObTableLoadResultInfo result_info_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObTableLoadInstance);
};

} // namespace observer
} // namespace oceanbase
