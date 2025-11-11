/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

 #ifndef OCEANBASE_OBSERVER_TABLE_OB_HYBRID_VECTOR_REFRESH_TASK_H_
 #define OCEANBASE_OBSERVER_TABLE_OB_HYBRID_VECTOR_REFRESH_TASK_H_

#include "lib/string/ob_string.h"
#include "share/scn.h"
#include "lib/thread/thread_mgr_interface.h"
#include "storage/access/ob_dml_param.h"
#include "storage/tx/ob_trans_define_v4.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"
#include "share/vector_index/ob_vector_index_async_task.h"
#include "share/vector_index/ob_vector_embedding_handler.h"
#include "share/ai_service/ob_ai_service_struct.h"
#include "storage/ob_value_row_iterator.h"
#include "src/observer/omt/ob_tenant_ai_service.h"

namespace oceanbase
{
namespace share
{

enum ObHybridVectorRefreshTaskStatus
{
  INVALID_TASK_STATUS = 0,
  TASK_PREPARE = 1,
  PREPARE_EMBEDDING = 2,
  WAITING_EMBEDDING = 3,
  TASK_FINISH = 4,
};

class ObVecEmbeddingAsyncTaskExecutor final : public ObVecAsyncTaskExector
{
public:
  ObVecEmbeddingAsyncTaskExecutor() : ObVecAsyncTaskExector()
  {}
  virtual ~ObVecEmbeddingAsyncTaskExecutor() {}
  virtual int load_task(uint64_t &task_trace_base_num) override;
private:
  bool check_operation_allow() override;
};

// vector index async task ctx
struct ObHybridVectorRefreshTaskCtx : public ObVecIndexAsyncTaskCtx
{
public:
  ObHybridVectorRefreshTaskCtx()
      : ObVecIndexAsyncTaskCtx(),
        status_(ObHybridVectorRefreshTaskStatus::TASK_PREPARE),
        scan_iter_(nullptr),
        delta_delete_iter_(),
        table_scan_param_(nullptr),
        table_param_(nullptr),
        scan_column_ids_(),
        embedding_vids_(),
        delete_vids_(),
        embedding_task_(nullptr),
        index_id_column_ids_(),
        embedded_table_column_ids_(),
        embedded_table_update_ids_(),
        ai_service_(),
        endpoint_(),
        adp_guard_(),
        task_started_(false),
        part_key_num_(0)
  {}
  virtual ~ObHybridVectorRefreshTaskCtx() override {
    check_task_free();
  }
  void check_task_free();
  void set_task_finish();

  TO_STRING_KV(K_(tenant_id), KP_(ls), K_(task_status), K_(sys_task_id), K_(in_thread_pool));

  ObHybridVectorRefreshTaskStatus status_;
  ObTableScanIterator *scan_iter_; // [vid][type][vector][chunk][other key columns]
  storage::ObValueRowIterator delta_delete_iter_;
  storage::ObTableScanParam *table_scan_param_;
  schema::ObTableParam *table_param_;
  ObSEArray<uint64_t, 4> scan_column_ids_; // [vid][type][vector][chunk][other key columns]
  ObSEArray<int64_t, 4> embedding_vids_;
  ObSEArray<int64_t, 4> delete_vids_;
  ObEmbeddingTask *embedding_task_;
  ObSEArray<uint64_t, 4> index_id_column_ids_;
  ObSEArray<uint64_t, 4> embedded_table_column_ids_;
  ObSEArray<uint64_t, 4> embedded_table_update_ids_;
  omt::ObAiServiceGuard ai_service_;
  const ObAiModelEndpointInfo *endpoint_;
  ObPluginVectorIndexAdapterGuard adp_guard_;
  bool task_started_;
  uint32_t part_key_num_; // is part key but rowkey
};

class ObHybridVectorRefreshTask : public ObVecIndexIAsyncTask
{
public:
  ObHybridVectorRefreshTask() : ObVecIndexIAsyncTask(ObMemAttr(MTL_ID(), "VecIdxASyTask")) {}
  virtual ~ObHybridVectorRefreshTask() {}
  virtual void check_task_free() override {
    all_finished_ = true;
    ObHybridVectorRefreshTaskCtx *ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
    if (OB_NOT_NULL(ctx)) {
      ctx->check_task_free();
    }
  }
  virtual int do_work() override;
  ObHybridVectorRefreshTaskStatus current_status() {
    ObHybridVectorRefreshTaskStatus status = ObHybridVectorRefreshTaskStatus::INVALID_TASK_STATUS;
    ObHybridVectorRefreshTaskCtx *ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
    if (OB_NOT_NULL(ctx)) {
      status = ctx->status_;
    }
    return status;
  }

  void reset_status() {
    ObHybridVectorRefreshTaskCtx *ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
    if (OB_NOT_NULL(ctx)) {
      ctx->status_ = ObHybridVectorRefreshTaskStatus::TASK_PREPARE;
    }
  }
  DISALLOW_COPY_AND_ASSIGN(ObHybridVectorRefreshTask);
private:
  static const int BATCH_CNT = 2000;
  static const int ORA_ROWSCN_COL_ID = 3;
  int check_embedding_finish(bool &finish);
  int prepare_for_task();
  int get_index_id_column_ids(ObPluginVectorIndexAdaptor &adaptor);
  int get_embedded_table_column_ids(ObPluginVectorIndexAdaptor &adaptor);
  int init_dml_param(
      uint64_t table_id,
      ObDMLBaseParam &dml_param,
      share::schema::ObTableDMLParam &table_param,
      ObIArray<uint64_t> &dml_column_ids,
      transaction::ObTxDesc *tx_desc,
      oceanbase::transaction::ObTxReadSnapshot &snapshot,
      storage::ObStoreCtxGuard &store_ctx_guard);
  int init_endpoint(ObPluginVectorIndexAdaptor &adaptor);
  int prepare_for_embedding(ObPluginVectorIndexAdaptor &adaptor);
  int prepare_index_id_data(storage::ObValueRowIterator &index_id_iter, storage::ObValueRowIterator &delta_delete_iter);
  int do_refresh_only(
      ObPluginVectorIndexAdaptor &adaptor,
      transaction::ObTxDesc *tx_desc,
      oceanbase::transaction::ObTxReadSnapshot &snapshot,
      storage::ObStoreCtxGuard &store_ctx_guard,
      storage::ObValueRowIterator &index_id_iter,
      storage::ObValueRowIterator &delta_delete_iter);
  int delete_embedded_table(ObPluginVectorIndexAdaptor &adaptor, transaction::ObTxDesc *tx_desc, oceanbase::transaction::ObTxReadSnapshot &snapshot, storage::ObStoreCtxGuard &store_ctx_guard);
  int after_embedding(ObPluginVectorIndexAdaptor &adaptor);
};

} // end namespace share
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_TABLE_OB_HYBRID_VECTOR_REFRESH_TASK_H_ */