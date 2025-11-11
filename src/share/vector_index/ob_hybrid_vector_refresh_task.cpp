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

#define USING_LOG_PREFIX SERVER
#include "ob_hybrid_vector_refresh_task.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "src/storage/ls/ob_ls.h"
#include "src/storage/tx/ob_trans_service.h"
#include "sql/das/ob_das_dml_vec_iter.h"

namespace oceanbase
{
namespace share
{

int ObVecEmbeddingAsyncTaskExecutor::load_task(uint64_t &task_trace_base_num)
{
  int ret = OB_SUCCESS;
  ObPluginVectorIndexMgr *index_ls_mgr = nullptr;
  ObArray<ObVecIndexAsyncTaskCtx*> task_ctx_array;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("vector async task not init", KR(ret));
  } else if (!check_operation_allow()) { // skip
  } else if (OB_FAIL(get_index_ls_mgr(index_ls_mgr))) { // skip
    LOG_WARN("fail to get index ls mgr", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  } else {
    ObVecIndexAsyncTaskOption &task_opt = index_ls_mgr->get_async_task_opt();
    ObIAllocator *allocator = task_opt.get_allocator();
    const int64_t current_task_cnt = ObVecIndexAsyncTaskUtil::get_processing_task_cnt(task_opt);
    ObSEArray<ObAdapterMapKeyValue, 4> adapter_array;
    ObAdapterMapFunc adapter_map_func(adapter_array);

    RWLock::RLockGuard lock_guard(index_ls_mgr->get_adapter_map_lock());
    if (OB_FAIL(index_ls_mgr->get_complete_adapter_map().foreach_refactored(adapter_map_func))) {
      LOG_WARN("fail to foreach adapter map", KR(ret));
    }
    FOREACH_X(iter, adapter_array, OB_SUCC(ret) && (task_ctx_array.count() + current_task_cnt <= MAX_ASYNC_TASK_PROCESSING_COUNT)) {
      ObTabletID tablet_id = iter->tablet_id_;
      ObPluginVectorIndexAdaptor *adapter = iter->adapter_;
      int64_t index_table_id = OB_INVALID_ID;
      if (OB_ISNULL(adapter)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::get_table_id_from_adapter(adapter, tablet_id, index_table_id))) { // only get table 3 table_id to generate new task
        LOG_WARN("fail to get table id from adapter", K(ret), K(tablet_id));
      } else if (OB_INVALID_ID == index_table_id) {
        LOG_DEBUG("index table id is invalid, skip", K(ret)); // skip to next
      } else if (adapter->is_hybrid_index() && adapter->check_need_embedding()) {
        int64_t new_task_id = OB_INVALID_ID;
        bool inc_new_task = false;
        common::ObCurTraceId::TraceId new_trace_id;

        char *task_ctx_buf = static_cast<char *>(allocator->alloc(sizeof(ObHybridVectorRefreshTaskCtx)));
        ObHybridVectorRefreshTaskCtx* task_ctx = nullptr;
        if (OB_ISNULL(task_ctx_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("async task ctx is null", K(ret));
        } else if (FALSE_IT(task_ctx = new(task_ctx_buf) ObHybridVectorRefreshTaskCtx())) {
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_task_id(tenant_id_, new_task_id))) {
          LOG_WARN("fail to fetch new task id", K(ret), K(tenant_id_));
        } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::fetch_new_trace_id(++task_trace_base_num, allocator, new_trace_id))) {
          LOG_WARN("fail to fetch new trace id", K(ret), K(tablet_id));
        } else {
          LOG_DEBUG("start load task", K(ret), K(tablet_id), K(tenant_id_), K(task_trace_base_num), K(ls_->get_ls_id()));
          // 1. update task_ctx to async task map
          task_ctx->tenant_id_ = tenant_id_;
          task_ctx->ls_ = ls_;
          task_ctx->task_status_.tablet_id_ = tablet_id.id();
          task_ctx->task_status_.tenant_id_ = tenant_id_;
          task_ctx->task_status_.table_id_ = index_table_id;
          task_ctx->task_status_.task_id_ = new_task_id;
          task_ctx->task_status_.task_type_ = ObVecIndexAsyncTaskType::OB_VECTOR_ASYNC_HYBRID_VECTOR_EMBEDDING;
          task_ctx->task_status_.trigger_type_ = ObVecIndexAsyncTaskTriggerType::OB_VEC_TRIGGER_AUTO;
          task_ctx->task_status_.status_ = ObVecIndexAsyncTaskStatus::OB_VECTOR_ASYNC_TASK_PREPARE;
          task_ctx->task_status_.trace_id_ = new_trace_id;
          task_ctx->task_status_.target_scn_.convert_from_ts(ObTimeUtility::current_time());
          task_ctx->allocator_.set_tenant_id(tenant_id_);
          if (OB_FAIL(index_ls_mgr->get_async_task_opt().add_task_ctx(tablet_id, task_ctx, inc_new_task))) { // not overwrite
            LOG_WARN("fail to add task ctx", K(ret));
          } else if (inc_new_task && OB_FAIL(task_ctx_array.push_back(task_ctx))) {
            LOG_WARN("fail to push back task status", K(ret), K(task_ctx));
          }
        }
        if (OB_FAIL(ret) || !inc_new_task) { // release memory when fail
          if (OB_NOT_NULL(task_ctx)) {
            task_ctx->~ObHybridVectorRefreshTaskCtx();
            allocator->free(task_ctx); // arena need free
            task_ctx = nullptr;
          }
        }
      }
    }
    LOG_INFO("finish load async task", K(ret), K(ls_->get_ls_id()), K(task_ctx_array.count()), K(current_task_cnt));
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(insert_new_task(task_ctx_array))) {
    LOG_WARN("fail to insert new tasks", K(ret), K(tenant_id_), K(ls_->get_ls_id()));
  }
  // clear on fail
  if (OB_FAIL(ret) && !task_ctx_array.empty()) {
    if (OB_FAIL(clear_task_ctxs(index_ls_mgr->get_async_task_opt(), task_ctx_array))) {
      LOG_WARN("fail to clear task ctx", K(ret));
    }
  }
  return ret;
}

bool ObVecEmbeddingAsyncTaskExecutor::check_operation_allow()
{
  int ret = OB_SUCCESS;
  uint64_t tenant_data_version = 0;
  bool bret = true;
  if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id_, tenant_data_version))) {
    bret = false;
    LOG_WARN("get tenant data version failed", K(ret));
  } else if (tenant_data_version < DATA_VERSION_4_4_1_0) {
    bret = false;
    LOG_DEBUG("vector index async task can not work with data version less than 4_4_1_0", K(tenant_data_version));
  }
  return bret;
}

/*
ObHybridVectorRefreshTask::do_work()
├── prepare_for_task()
│   ├── get_adapter_inst_guard()
│   └── has_doing_vector_index_task()
├── prepare_for_embedding()
│   ├── get_index_id_column_ids()
│   ├── get_embedded_table_column_ids()
│   ├── ObPluginVectorIndexUtils::read_local_tablet()
│   ├── init_endpoint()
│   │   ├── get_ai_service_guard()
│   │   └── get_ai_endpoint()
│   └── ObEmbeddingTask::init()
└── after_embedding()
    ├── check_embedding_finish()
    │   └── ObEmbeddingTask::is_completed()
    ├── prepare_index_id_data()                                 // 生成删除4号表的数据
    ├── ObInsertLobColumnHelper::start_trans()
    ├── txs->get_ls_read_snapshot()
    ├── do_refresh_only()
    │   ├── init_dml_param()
    │   │   ├── table_param.convert()
    │   │   ├── schema_guard.get_schema_version()
    │   │   └── oas->get_write_store_ctx_guard()
    │   ├── oas->insert_rows()                                   // 插入数据到4号表中
    │   └── oas->delete_rows()                                   // 删除3号表数据
    ├── delete_embedded_table()                                  // 删除6号表数据
    │   ├── ObPluginVectorIndexUtils::read_local_tablet()
    │   ├── ObPluginVectorIndexUtils::add_key_ranges()
    │   ├── ObPluginVectorIndexUtils::iter_table_rescan()
    │   └── oas->delete_rows()
    ├── ObEmbeddingTask::get_async_result()
    ├── oas->insert_rows()                                        // 插入数据到6号表中
    └── ObInsertLobColumnHelper::end_trans()
*/

int ObHybridVectorRefreshTask::do_work()
{
  int ret = OB_SUCCESS;
  int exec_finish = false;
  ObPluginVectorIndexAdapterGuard adpt_guard;
  ObPluginVectorIndexService *vector_index_service = MTL(ObPluginVectorIndexService *);
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  if (OB_ISNULL(vector_index_service) || OB_ISNULL(task_ctx) || OB_ISNULL(vec_idx_mgr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx));
  } else {
    LOG_INFO("start do_work", K(ret), K(task_ctx->task_status_), K(ls_id_));
  }
  while (OB_SUCC(ret) && !exec_finish) {
    switch (current_status()) {
      case ObHybridVectorRefreshTaskStatus::TASK_PREPARE:
      {
        if (OB_FAIL(prepare_for_task())) {
          LOG_WARN("failed to prepare task", K(ret));
        }
        break;
      }
      case ObHybridVectorRefreshTaskStatus::PREPARE_EMBEDDING:
      {
        if (OB_ISNULL(task_ctx->adp_guard_.get_adatper())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get vector index adapter", KR(ret), KPC(ctx_));
        } else if (OB_FAIL(prepare_for_embedding(*task_ctx->adp_guard_.get_adatper()))) {
          LOG_WARN("failed to prepare embedding", K(ret));
        }
        break;
      }
      case ObHybridVectorRefreshTaskStatus::WAITING_EMBEDDING:
      {
        if (OB_ISNULL(task_ctx->adp_guard_.get_adatper())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail to get vector index adapter", KR(ret), KPC(ctx_));
        } else if (OB_FAIL(after_embedding(*task_ctx->adp_guard_.get_adatper()))) {
          LOG_WARN("failed to exec task after embedding", K(ret));
        } else {
          exec_finish = (task_ctx->status_ == ObHybridVectorRefreshTaskStatus::WAITING_EMBEDDING);
        }
        break;
      }
      case ObHybridVectorRefreshTaskStatus::TASK_FINISH:
      {
        exec_finish = true;
        break;
      }
      default :
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task status", K(ret), K(current_status()), KPC(get_task_ctx()));
        break;
    }
  }

  if (OB_NOT_NULL(ctx_)) {
    common::ObSpinLockGuard ctx_guard(ctx_->lock_);
    ctx_->task_status_.ret_code_ = ret;
    LOG_INFO("end do_work", K(ret), K(ctx_->task_status_));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected error: null context pointer", K(ret), KPC(this));
  }
  if (OB_FAIL(ret) || current_status() == ObHybridVectorRefreshTaskStatus::TASK_FINISH) {
    check_task_free();
  }
  return ret;
}

int ObHybridVectorRefreshTask::prepare_for_task()
{
  int ret = OB_SUCCESS;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_ISNULL(task_ctx)) {
    ret =  OB_ERR_UNEXPECTED;
    LOG_WARN("get null pointer", K(ret), KPC(task_ctx));
  } else if (OB_ISNULL(task_ctx->adp_guard_.get_adatper()) && OB_FAIL(vec_idx_mgr_->get_adapter_inst_guard(ctx_->task_status_.tablet_id_, task_ctx->adp_guard_))) {
    if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_EAGAIN;
      LOG_INFO("can not get adapter, need wait", K(ret), KPC(ctx_));
    } else {
      LOG_WARN("fail to get adapter instance", KR(ret), KPC(ctx_));
    }
  } else if (OB_ISNULL(task_ctx->adp_guard_.get_adatper())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get vector index adapter", KR(ret), KPC(ctx_));
  } else if (task_ctx->adp_guard_.get_adatper()->has_doing_vector_index_task()) {
    ret = OB_EAGAIN;
    LOG_INFO("there is other vector index task running", K(ret), KP(task_ctx->adp_guard_.get_adatper()));
  } else if (!task_ctx->adp_guard_.get_adatper()->is_complete()) {
    ret = OB_EAGAIN;
    LOG_INFO("adapter not complete, need wait", K(ret), KP(task_ctx->adp_guard_.get_adatper()));
  } else if (FALSE_IT(task_ctx->task_started_ = true)) {
  } else {
    task_ctx->status_ = ObHybridVectorRefreshTaskStatus::PREPARE_EMBEDDING;
  }
  return ret;
}

int ObHybridVectorRefreshTask::get_index_id_column_ids(ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObTableSchema *data_table_schema = NULL;
  uint64_t scn_column_id = 0;
  uint64_t vid_column_id = 0;
  uint64_t type_column_id = 0;
  uint64_t vector_column_id = 0;
  ObSEArray<uint64_t, 4> part_column_ids;
  ObArray<uint64_t> tmp_column_ids;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());

  if (OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, adaptor.get_vbitmap_table_id(), table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), K(adaptor));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("get null table schema", KR(ret), K(adaptor));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), KR(table_schema->get_data_table_id()));
  } else if ( OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), K(adaptor), K(table_schema->get_data_table_id()));
  } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
    LOG_WARN("fail to get index table all column ids", K(table_schema), K(adaptor));
  } else if (tmp_column_ids.count() < 4) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", K(tmp_column_ids.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
      const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
      uint64_t col_id = col_schema->get_column_id();
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (col_schema->is_vec_hnsw_scn_column()) {
        scn_column_id = col_schema->get_column_id();
      } else if (adaptor.get_is_need_vid() && col_schema->is_vec_hnsw_vid_column()) {
        vid_column_id = col_schema->get_column_id();
      } else if (!adaptor.get_is_need_vid() && col_schema->is_hidden_pk_column_id(col_id)) {
        vid_column_id = col_schema->get_column_id();
      } else if (col_schema->is_vec_hnsw_type_column()) {
        type_column_id = col_schema->get_column_id();
      } else if (col_schema->is_vec_hnsw_vector_column()) {
        vector_column_id = col_schema->get_column_id();
      } else if (OB_FAIL(part_column_ids.push_back(col_schema->get_column_id()))) {
        LOG_WARN("failed to push back column id", K(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (scn_column_id == 0 || vid_column_id == 0 || type_column_id == 0 || vector_column_id == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get valid column id", K(ret), K(scn_column_id), K(vid_column_id), K(type_column_id), K(vector_column_id));
  } else if (OB_FAIL(task_ctx->index_id_column_ids_.push_back(scn_column_id))) {
    LOG_WARN("failed to push 1st column id.", K(ret));
  } else if (OB_FAIL(task_ctx->index_id_column_ids_.push_back(vid_column_id))) {
    LOG_WARN("failed to push 2nd column id.", K(ret));
  } else if (OB_FAIL(task_ctx->index_id_column_ids_.push_back(type_column_id))) {
    LOG_WARN("failed to push 3rd column id.", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_column_ids.count(); ++i) {
    if (OB_FAIL(task_ctx->index_id_column_ids_.push_back(part_column_ids.at(i)))) {
      LOG_WARN("failed to push back column id.", K(ret));
    }
  }
  if (OB_SUCC(ret) && OB_FAIL(task_ctx->index_id_column_ids_.push_back(vector_column_id))) {
    LOG_WARN("failed to push 4th column id.", K(ret));
  }

  return ret;
}

int ObHybridVectorRefreshTask::get_embedded_table_column_ids(ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  const ObTableSchema *data_table_schema = NULL;
  uint64_t vid_column_id = 0;
  uint64_t vector_column_id = 0;
  ObArray<uint64_t> tmp_column_ids;
  ObArray<uint64_t> part_key_column_ids; // is part ket and not rowkey
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());

  if (OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, adaptor.get_embedded_table_id(), table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), K(adaptor));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("get null table schema", KR(ret), K(adaptor));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_schema->get_data_table_id(), data_table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), KR(table_schema->get_data_table_id()));
  } else if ( OB_ISNULL(data_table_schema)) {
    ret = OB_TABLE_NOT_EXIST; // table may be removed, handle in scheduler routine
    LOG_WARN("get null table schema", KR(ret), K(adaptor), K(table_schema->get_data_table_id()));
  } else if (OB_FAIL(table_schema->get_column_ids(tmp_column_ids))) {
    LOG_WARN("fail to get index table all column ids", K(table_schema), K(adaptor));
  } else if (tmp_column_ids.count() < 2) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected column count", K(tmp_column_ids.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
      const ObColumnSchemaV2 *col_schema = data_table_schema->get_column_schema(tmp_column_ids[i]);
      uint64_t col_id = col_schema->get_column_id();
      if (OB_ISNULL(col_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null column schema ptr", K(ret));
      } else if (adaptor.get_is_need_vid() && col_schema->is_vec_hnsw_vid_column()) {
        vid_column_id = col_schema->get_column_id();
      } else if (!adaptor.get_is_need_vid() && col_schema->is_hidden_pk_column_id(col_id)) {
        vid_column_id = col_schema->get_column_id();
      } else if (col_schema->is_vec_hnsw_vector_column()) {
        vector_column_id = col_schema->get_column_id();
      } else if (col_schema->is_part_key_column() && !col_schema->is_rowkey_column()) {
        part_key_column_ids.push_back(col_schema->get_column_id());
      }
    }
  }
  // col order of 6th table is rowkey vid vector or pk(vid) part_key vector
  if (OB_FAIL(ret)) {
  } else if (vid_column_id == 0 || vector_column_id == 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get valid column id", K(ret), K(vid_column_id), K(vector_column_id));
  } else if (FALSE_IT(tmp_column_ids.reuse())) {
  } else if (adaptor.get_is_need_vid() && OB_FAIL(data_table_schema->get_rowkey_column_ids(tmp_column_ids))) {
    LOG_WARN("failed to get data table rowkey column id", K(ret), KPC(data_table_schema));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < tmp_column_ids.count(); ++i) {
    if (OB_FAIL(task_ctx->embedded_table_column_ids_.push_back(tmp_column_ids.at(i)))) {
      LOG_WARN("failed to push back column id.", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(task_ctx->embedded_table_column_ids_.push_back(vid_column_id))) {
    LOG_WARN("failed to push vid column id.", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < part_key_column_ids.count(); ++i) {
    if (OB_FAIL(task_ctx->embedded_table_column_ids_.push_back(part_key_column_ids.at(i)))) {
      LOG_WARN("failed to push back part column id.", K(ret));
    }
  }
  task_ctx->part_key_num_ = part_key_column_ids.count();
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(task_ctx->embedded_table_column_ids_.push_back(vector_column_id))) {
    LOG_WARN("failed to push vector column id.", K(ret));
  } else if (OB_FAIL(task_ctx->embedded_table_update_ids_.push_back(vector_column_id))) {
    LOG_WARN("failed to push vector column id.", K(ret));
  }

  return ret;
}

int ObHybridVectorRefreshTask::init_dml_param(uint64_t table_id,
    ObDMLBaseParam &dml_param,
    share::schema::ObTableDMLParam &table_param,
    ObIArray<uint64_t> &dml_column_ids,
    transaction::ObTxDesc *tx_desc,
    oceanbase::transaction::ObTxReadSnapshot &snapshot,
    storage::ObStoreCtxGuard &store_ctx_guard)
{
  int ret = OB_SUCCESS;
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = NULL;
  ObAccessService *oas = MTL(ObAccessService *);
  const uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());

  if (OB_ISNULL(task_ctx) || OB_ISNULL(oas) || OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx), K(oas), K(tx_desc));
  } else if (OB_FAIL(ObMultiVersionSchemaService::get_instance().get_tenant_schema_guard(tenant_id_, schema_guard))) {
    LOG_WARN("fail to get schema guard", KR(ret), K(tenant_id_));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id_, table_id, table_schema))) {
    LOG_WARN("fail to get schema", KR(ret), K(table_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_TABLE_NOT_EXIST;
    LOG_WARN("get null table schema", KR(ret), K(table_id));
  } else if (OB_FAIL(table_param.convert(table_schema, table_schema->get_schema_version(), dml_column_ids))) {
    LOG_WARN("failed to convert table dml param.", K(ret));
  } else if (OB_FAIL(schema_guard.get_schema_version(tenant_id_, dml_param.tenant_schema_version_))) {
    LOG_WARN("failed to get schema version", K(ret));
  } else {
    dml_param.sql_mode_ = SMO_DEFAULT;
    dml_param.write_flag_.reset();
    dml_param.write_flag_.set_is_insert_up();
    dml_param.table_param_ = &table_param;
    dml_param.encrypt_meta_ = &dml_param.encrypt_meta_legacy_;
    dml_param.timeout_ = timeout_us;
    dml_param.snapshot_.assign(snapshot);
    dml_param.branch_id_ = 0;
    dml_param.store_ctx_guard_ = &store_ctx_guard;
    dml_param.schema_version_ = table_schema->get_schema_version();
    dml_param.dml_allocator_ = &allocator_;
    if (OB_FAIL(oas->get_write_store_ctx_guard(ls_id_, timeout_us, *tx_desc, snapshot, 0, dml_param.write_flag_, store_ctx_guard))) {
      LOG_WARN("failed to get write store context guard", K(ret));
    }
  }
  return ret;
}

int ObHybridVectorRefreshTask::init_endpoint(ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService *);
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  if (OB_ISNULL(ai_service) || OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx), K(ai_service));
  } else if (OB_FAIL(ai_service->get_ai_service_guard(task_ctx->ai_service_))) {
    LOG_WARN("failed to get ai service guard", K(ret), KPC(task_ctx));
  } else if (OB_FAIL(task_ctx->ai_service_.get_ai_endpoint_by_ai_model_name(adaptor.get_endpoint(), task_ctx->endpoint_, false /*need_check*/))) {
    LOG_WARN("failed to get endpoint info", K(ret), K(adaptor));
  }
  return ret;
}

int ObHybridVectorRefreshTask::prepare_for_embedding(ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  common::ObNewRowIterator *scan_iter = nullptr;
  ObTableScanIterator *&tsc_iter = task_ctx->scan_iter_;
  storage::ObTableScanParam *&table_scan_param = task_ctx->table_scan_param_;
  schema::ObTableParam *&table_param = task_ctx->table_param_;
  storage::ObValueRowIterator &delta_delete_iter = task_ctx->delta_delete_iter_;
  ObCollationType col_type = CS_TYPE_INVALID;
  int64_t dim = 0;
  int64_t loop_cnt = 0;
  uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx));
  } else if (OB_FAIL(adaptor.get_dim(dim))) {
    LOG_WARN("get dim failed", K(ret));
  } else {
    if (OB_NOT_NULL(tsc_iter) || OB_NOT_NULL(table_scan_param) || OB_NOT_NULL(table_param)) {
      if (OB_ISNULL(tsc_iter) || OB_ISNULL(table_scan_param) || OB_ISNULL(table_param)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null error", K(ret), KPC(task_ctx), K(tsc_iter), K(table_scan_param), K(table_param));
      }
    } else if (OB_ISNULL(table_scan_param = static_cast<storage::ObTableScanParam *>(task_ctx->allocator_.alloc(sizeof(storage::ObTableScanParam))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(table_scan_param));
    } else if (FALSE_IT(table_scan_param = new(table_scan_param)storage::ObTableScanParam())) {
    } else if (OB_ISNULL(table_param = static_cast<schema::ObTableParam *>(task_ctx->allocator_.alloc(sizeof(schema::ObTableParam))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory failed", K(ret), K(table_param));
    } else if (FALSE_IT(table_param = new(table_param)schema::ObTableParam(task_ctx->allocator_))) {
    } else if (FALSE_IT(ctx_->task_status_.target_scn_.convert_from_ts(ObTimeUtility::current_time()))) {
    } else if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
        &adaptor,
        ctx_->task_status_.target_scn_,
        INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
        task_ctx->allocator_,
        task_ctx->allocator_,
        *table_scan_param,
        *table_param,
        scan_iter,
        &task_ctx->scan_column_ids_,
        true,
        false,
        true))) {
      LOG_WARN("failed to read vid id table local tablet.", K(ret));
    } else if (FALSE_IT(tsc_iter = static_cast<ObTableScanIterator *>(scan_iter))) {
    } else if (OB_ISNULL(tsc_iter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get null table scan iter", K(ret), KPC(task_ctx), K(tsc_iter));
    } else if (task_ctx->index_id_column_ids_.empty() && OB_FAIL(get_index_id_column_ids(adaptor))) {
      LOG_WARN("failed to get index id table column ids", K(ret), K(adaptor));
    } else if (task_ctx->embedded_table_column_ids_.empty() && OB_FAIL(get_embedded_table_column_ids(adaptor))) {
      LOG_WARN("failed to get embedded table column ids", K(ret), K(adaptor));
    } else if (OB_FAIL(ObVectorIndexUtil::get_index_column_collation_type(tenant_id_, adaptor.get_embedded_table_id(), col_type))) {
      LOG_WARN("failed to get chunc column col_type", K(ret), K(adaptor));
    }

    int cur_row_count = 0;
    ObSEArray<ObString, 4> chunk_array;
    ObSEArray<ObString, 4> tmp_chunk_array;
    ObSEArray<int64_t, 4> tmp_embedding_vids;
    ObSEArray<blocksstable::ObDatumRow *, 4> tmp_embedding_rows;
    delta_delete_iter.init();
    while (OB_SUCC(ret) && cur_row_count < BATCH_CNT) {
      blocksstable::ObDatumRow *datum_row = nullptr;
      blocksstable::ObDatumRow *copied_row = nullptr;
      if (OB_FAIL(tsc_iter->get_next_row(datum_row))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next row failed.", K(ret));
        }
      } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row invalid.", K(ret));
      } else if (datum_row->get_column_count() < 4) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
      } else if (OB_FAIL(delta_delete_iter.add_row(*datum_row))) {
        LOG_WARN("failed to add row to iter", K(ret));
      } else if (OB_FAIL(delta_delete_iter.get_next_row(copied_row))) {
        LOG_WARN("get next row failed.", K(ret));
      } else {
        ObString op;
        op = copied_row->storage_datums_[1].get_string();
        if (op.length() != 1) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get invalid op length.", K(ret), K(op));
        } else {
          if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_INSERT[0] && !copied_row->storage_datums_[2].is_null()) {
            if (OB_FAIL(tmp_chunk_array.push_back(copied_row->storage_datums_[2].get_string()))) {
              LOG_WARN("failed to push back chunk str", K(ret), K(copied_row->storage_datums_[2].get_string()));
            } else if (OB_FAIL(tmp_embedding_vids.push_back(copied_row->storage_datums_[0].get_int()))) {
              LOG_WARN("failed to push back vid", K(ret), K(copied_row->storage_datums_[0].get_int()));
            } else if (OB_FAIL(tmp_embedding_rows.push_back(copied_row))) {
              LOG_WARN("failed to push back vid", K(ret), K(copied_row));
            }
          } else if (op.ptr()[0] == sql::ObVecIndexDMLIterator::VEC_DELTA_DELETE[0]) {
            if (OB_FAIL(task_ctx->delete_vids_.push_back(copied_row->storage_datums_[0].get_int()))) {
              LOG_WARN("failed to push back vid", K(ret), K(copied_row->storage_datums_[0].get_int()));
            }
          }
        }
        cur_row_count ++;
        CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    delta_delete_iter.rescan();
    // remove vids which will be deleted soon.
    for (int i = 0; i < tmp_embedding_vids.count() && OB_SUCC(ret); i++) {
      if (!is_contain(task_ctx->delete_vids_, tmp_embedding_vids.at(i))) {
        if (OB_FAIL(task_ctx->embedding_vids_.push_back(tmp_embedding_vids.at(i)))) {
          LOG_WARN("failed to push back vid", K(ret), K(tmp_embedding_vids.at(i)));
        } else if (OB_FAIL(chunk_array.push_back(tmp_chunk_array.at(i)))) {
          LOG_WARN("failed to push back chunk str", K(ret), K(tmp_chunk_array.at(i)));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (chunk_array.empty()) {
      if (cur_row_count == 0) {
        task_ctx->status_ = ObHybridVectorRefreshTaskStatus::TASK_FINISH;
      } else {
        task_ctx->status_ = ObHybridVectorRefreshTaskStatus::WAITING_EMBEDDING;
      }
    } else {
      if (OB_ISNULL(task_ctx->endpoint_) && OB_FAIL(init_endpoint(adaptor))) {
        LOG_WARN("failed to init endpoint", K(ret));
      } else {
        void *task_buf = task_ctx->allocator_.alloc(sizeof(ObEmbeddingTask));
        if (OB_ISNULL(task_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("fail to alloc memory of ObEmbeddingTask", K(ret), K(tenant_id_));
        } else {
          ObString access_key;
          ObString url;
          const ObAiModelEndpointInfo *endpoint = task_ctx->endpoint_; // endpoint should not be null after init.
          task_ctx->embedding_task_ = new(task_buf)ObEmbeddingTask(task_ctx->allocator_);
          ObPluginVectorIndexService *service = MTL(ObPluginVectorIndexService *);

          if (OB_ISNULL(service)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null ptr", K(ret), KPC(service));
          } else if (OB_FAIL(endpoint->get_unencrypted_access_key(task_ctx->allocator_, access_key))) {
            LOG_WARN("failed to get access key", K(ret));
          } else if (OB_FAIL(ob_write_string(task_ctx->allocator_, endpoint->get_url(), url, true))) {
            LOG_WARN("fail to write string", K(ret));
          } else if (OB_FAIL(task_ctx->embedding_task_->init(url, endpoint->get_request_model_name(),
                             endpoint->get_provider(), access_key, chunk_array, col_type, dim, timeout_us))) {
            LOG_WARN("failed to init embedding task", K(ret), KPC(endpoint));
          } else {
            ObEmbeddingTaskHandler *embedding_handler = nullptr;
            if (OB_FAIL(service->get_embedding_task_handler(embedding_handler))) {
              LOG_WARN("failed to get embedding task handler", K(ret));
            } else if (OB_FAIL(embedding_handler->push_task(*task_ctx->embedding_task_))) {
              LOG_WARN("failed to push embedding task", K(ret));
            }
          }
          if (OB_FAIL(ret) && OB_NOT_NULL(task_ctx->embedding_task_)) {
            task_ctx->embedding_task_->~ObEmbeddingTask();
            task_ctx->allocator_.free(task_ctx->embedding_task_);
            task_ctx->embedding_task_ = nullptr;
          }
        }
      }
      task_ctx->status_ = ObHybridVectorRefreshTaskStatus::WAITING_EMBEDDING;
    }
  }
  return ret;
}

int ObHybridVectorRefreshTask::check_embedding_finish(bool &finish)
{
  int ret = OB_SUCCESS;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  if (OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx));
  } else if (OB_ISNULL(task_ctx->embedding_task_)) {
    finish = true;
  } else {
    finish = task_ctx->embedding_task_->is_completed();
  }
  return ret;
}

int ObHybridVectorRefreshTask::do_refresh_only(
    ObPluginVectorIndexAdaptor &adaptor,
    transaction::ObTxDesc *tx_desc,
    oceanbase::transaction::ObTxReadSnapshot &snapshot,
    storage::ObStoreCtxGuard &store_ctx_guard,
    storage::ObValueRowIterator &index_id_iter,
    storage::ObValueRowIterator &delta_delete_iter)
{
  int ret = OB_SUCCESS;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  if (OB_ISNULL(task_ctx) || OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx), KPC(tx_desc));
  } else {
    // insert into 4 table.
    int64_t affected_rows = 0;
    ObDMLBaseParam dml_param;
    share::schema::ObTableDMLParam table_dml_param(allocator_);
    share::schema::ObTableDMLParam index_id_dml_param(allocator_);
    ObAccessService *oas = MTL(ObAccessService *);
    ObSEArray<uint64_t, 4> delta_delete_column_id;

    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(oas)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), KPC(task_ctx), K(oas));
    } else if (OB_FAIL(init_dml_param(adaptor.get_vbitmap_table_id(), dml_param, table_dml_param, task_ctx->index_id_column_ids_, tx_desc, snapshot, store_ctx_guard))) {
      LOG_WARN("failed to init dml param", K(ret), K(dml_param), K(table_dml_param));
    } else if (OB_FAIL(oas->insert_rows(ls_id_, adaptor.get_vbitmap_tablet_id(), *tx_desc, dml_param, task_ctx->index_id_column_ids_, &index_id_iter, affected_rows))) {
      LOG_WARN("failed to insert rows to index id table", K(ret), K(adaptor.get_vbitmap_table_id()));
    }
    store_ctx_guard.reset();

    // delete from 3 table.
    affected_rows = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(delta_delete_column_id.assign(task_ctx->scan_column_ids_))) {
      LOG_WARN("failed to assign column ids", K(ret));
    } else if (OB_FAIL(delta_delete_column_id.remove(ORA_ROWSCN_COL_ID))) {
      LOG_WARN("failed to remove ora_rowscn column", K(ret));
    } else if (OB_FAIL(init_dml_param(adaptor.get_inc_table_id(), dml_param, index_id_dml_param, delta_delete_column_id, tx_desc, snapshot, store_ctx_guard))) {
      LOG_WARN("failed to init dml param", K(ret), K(dml_param), K(index_id_dml_param));
    } else if (OB_FAIL(oas->delete_rows(ls_id_, adaptor.get_inc_tablet_id(), *tx_desc, dml_param, delta_delete_column_id, &delta_delete_iter, affected_rows))) {
      LOG_WARN("failed to insert rows to index id table", K(ret), K(adaptor.get_inc_table_id()));
    }
    store_ctx_guard.reset();
    delta_delete_iter.reset();
  }
  index_id_iter.reset();
  return ret;
}

int ObHybridVectorRefreshTask::prepare_index_id_data(storage::ObValueRowIterator &index_id_iter, storage::ObValueRowIterator &delta_delete_iter)
{
  int ret = OB_SUCCESS;
  int64_t loop_cnt = 0;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  if (OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx));
  } else {
    storage::ObValueRowIterator &delta_iter = task_ctx->delta_delete_iter_;
    index_id_iter.init();
    delta_delete_iter.init();
    HEAP_VARS_2((blocksstable::ObDatumRow, index_id_row, tenant_id_), (blocksstable::ObDatumRow, delta_row, tenant_id_)) {
      if (OB_FAIL(index_id_row.init(task_ctx->index_id_column_ids_.count()))) {
        LOG_WARN("fail to init datum row", K(ret), K(task_ctx->index_id_column_ids_.count()), K(index_id_row));
      } else if (OB_FAIL(delta_row.init(task_ctx->scan_column_ids_.count() - 1))) {
        LOG_WARN("fail to init datum row", K(ret), K(task_ctx->scan_column_ids_.count()), K(delta_row));
      }
      while (OB_SUCC(ret)) {
        blocksstable::ObDatumRow *datum_row = nullptr;
        if (OB_FAIL(delta_iter.get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get next row failed.", K(ret));
          }
        } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row invalid.", K(ret));
        } else if (datum_row->get_column_count() < 4) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row column cnt invalid.", K(ret), K(datum_row->get_column_count()));
        } else {
          // col order is scn vid type rowkey part_key vector
          int storage_idx = 0;
          index_id_row.storage_datums_[storage_idx++].set_int(datum_row->storage_datums_[ORA_ROWSCN_COL_ID].get_int()); // scn
          index_id_row.storage_datums_[storage_idx++].set_int(datum_row->storage_datums_[0].get_int()); // vid
          index_id_row.storage_datums_[storage_idx++].set_string(datum_row->storage_datums_[1].get_string()); // type
          for (int64_t i = 4; OB_SUCC(ret) && i < task_ctx->index_id_column_ids_.count(); i++) {
            index_id_row.storage_datums_[storage_idx++].shallow_copy_from_datum(datum_row->storage_datums_[i]);
          }
          index_id_row.storage_datums_[storage_idx++].set_null(); // vector
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(index_id_iter.add_row(index_id_row))) {
            LOG_WARN("failed to add row to index id iter", K(ret));
          }
          index_id_row.reuse();

          int col_id = 0;
          for (int64_t i = 0; OB_SUCC(ret) && i < task_ctx->scan_column_ids_.count(); i++) {
            if (ORA_ROWSCN_COL_ID != i) {
              delta_row.storage_datums_[col_id].shallow_copy_from_datum(datum_row->storage_datums_[i]);
              col_id ++;
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(delta_delete_iter.add_row(delta_row))) {
            LOG_WARN("failed to add row to index id iter", K(ret));
          }
          delta_row.reuse();
          CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
        }
      } // end while.
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      delta_iter.reset();
    } // end HEAP_VAR.
  }
  return ret;
}

int ObHybridVectorRefreshTask::delete_embedded_table(ObPluginVectorIndexAdaptor &adaptor, transaction::ObTxDesc *tx_desc, oceanbase::transaction::ObTxReadSnapshot &snapshot, storage::ObStoreCtxGuard &store_ctx_guard)
{
  int ret = OB_SUCCESS;
  int64_t loop_cnt = 0;
  ObAccessService *tsc_service = MTL(ObAccessService *);
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  if (OB_ISNULL(task_ctx) || OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx), KPC(tx_desc));
  } else if (task_ctx->delete_vids_.empty()) {
  } else {
    storage::ObValueRowIterator delete_iter;
    ObAccessService *oas = MTL(ObAccessService *);
    ObSEArray<uint64_t, 4> dml_column_ids;
    HEAP_VARS_2((storage::ObTableScanParam, table_scan_param), (schema::ObTableParam, table_param, allocator_)) {
      common::ObNewRowIterator *scan_iter = nullptr;
      ObArenaAllocator scan_allocator("VecEmbedding", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
          &adaptor,
          snapshot.version(),
          INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL,
          allocator_,
          scan_allocator,
          table_scan_param,
          table_param,
          scan_iter,
          &dml_column_ids,
          true))) {
        LOG_WARN("failed to read data table local tablet.", K(ret));
      } else if (OB_FAIL(delete_iter.init())) {
        LOG_WARN("fail to init iter", K(ret), K(delete_iter));
      }
      bool scan_finish = false;
      const int32_t data_table_rowkey_count = table_param.get_output_projector().count() - 2;
      storage::ObTableScanIterator *table_scan_iter = dynamic_cast<storage::ObTableScanIterator *>(scan_iter);
      while (OB_SUCC(ret) && !scan_finish) {
        blocksstable::ObDatumRow *datum_row = nullptr;
        int64_t vid;
        if (OB_ISNULL(table_scan_iter)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to cast to vid iter.", K(ret));
        } else if (OB_FAIL(table_scan_iter->get_next_row(datum_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next row from next table.", K(ret));
          } else {
            scan_finish = true;
            ret = OB_SUCCESS;
          }
        } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("get row invalid.", K(ret));
        } else if (FALSE_IT(vid = datum_row->storage_datums_[data_table_rowkey_count].get_int())) {
        } else if (!is_contain(task_ctx->delete_vids_, vid)) {
        } else if (OB_FAIL(delete_iter.add_row(*datum_row))) {
          LOG_WARN("fail to add row", K(ret), KPC(datum_row));
        }
        CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
      }
      if (OB_NOT_NULL(tsc_service)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_NOT_NULL(scan_iter)) {
          tmp_ret = tsc_service->revert_scan_iter(scan_iter);
          if (tmp_ret != OB_SUCCESS) {
            LOG_WARN("revert scan_iter failed", K(ret));
          }
        }
        scan_iter = nullptr;
      }
    } // end heap var.

    int64_t affected_rows = 0;
    ObDMLBaseParam dml_param;
    share::schema::ObTableDMLParam table_dml_param(allocator_);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(oas)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), KPC(task_ctx), K(oas));
    } else if (OB_FAIL(init_dml_param(adaptor.get_embedded_table_id(), dml_param, table_dml_param, dml_column_ids, tx_desc, snapshot, store_ctx_guard))) {
      LOG_WARN("failed to init dml param", K(ret), K(dml_param), K(table_dml_param));
    } else if (OB_FAIL(oas->delete_rows(ls_id_, adaptor.get_embedded_tablet_id(), *tx_desc, dml_param, dml_column_ids, &delete_iter, affected_rows))) {
      LOG_WARN("failed to delete rows from embedded table", K(ret), K(adaptor.get_embedded_tablet_id()));
    }
    store_ctx_guard.reset();
    delete_iter.reset();
  }
  return ret;
}

int ObHybridVectorRefreshTask::after_embedding(ObPluginVectorIndexAdaptor &adaptor)
{
  int ret = OB_SUCCESS;
  int64_t dim = 0;
  bool embedding_finish = false;
  ObArray<float*> output_vector;
  float *vector_buf = nullptr;
  ObHybridVectorRefreshTaskCtx *task_ctx = static_cast<ObHybridVectorRefreshTaskCtx *>(get_task_ctx());
  ObVecIndexATaskUpdIterator embedded_iter;
  storage::ObValueRowIterator index_id_iter;
  storage::ObValueRowIterator delta_delete_iter;
  embedded_iter.init();
  bool trans_start = false;
  transaction::ObTxDesc *tx_desc = nullptr;
  oceanbase::transaction::ObTxReadSnapshot snapshot;
  storage::ObStoreCtxGuard store_ctx_guard;
  ObAccessService *oas = MTL(ObAccessService *);
  ObAccessService *tsc_service = MTL(ObAccessService *);
  oceanbase::transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  uint64_t timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_ISNULL(task_ctx)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error", K(ret), KPC(task_ctx));
  } else if (OB_FAIL(check_embedding_finish(embedding_finish))) {
    LOG_WARN("failed to check embedding task finish", K(ret));
  } else if (!embedding_finish) {
  } else if (OB_FAIL(prepare_index_id_data(index_id_iter, delta_delete_iter))) {
    LOG_WARN("failed to prepare index id table data", K(ret), K(index_id_iter));
  } else if (OB_ISNULL(txs)) {
    ret =  OB_ERR_UNEXPECTED;
    LOG_WARN("get null ptr", K(ret), KPC(task_ctx), K(txs));
  } else if (OB_FAIL(ObInsertLobColumnHelper::start_trans(ls_id_, false/*is_for_read*/, timeout_us, tx_desc))) {
    LOG_WARN("fail to start trans", K(ret));
  } else if (FALSE_IT(trans_start = true)) {
  } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ls_id_, timeout_us, snapshot))) {
    LOG_WARN("fail to get snapshot", K(ret));
  } else if (OB_FAIL(do_refresh_only(adaptor, tx_desc, snapshot, store_ctx_guard, index_id_iter, delta_delete_iter))) {
    LOG_WARN("fail to do refresh", K(ret));
  } else if (OB_FAIL(delete_embedded_table(adaptor, tx_desc, snapshot, store_ctx_guard))) {
    LOG_WARN("failed to delete data from embedded table", K(ret));
  } else if (OB_ISNULL(task_ctx->embedding_task_)) {                               // skip next if no need to embedding.
  } else if (OB_FAIL(task_ctx->embedding_task_->get_async_result(output_vector))) {
    LOG_WARN("failed to get async embedding result", K(ret));
  } else if (output_vector.count() != task_ctx->embedding_vids_.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("embedding result error", K(ret), K(output_vector), K(task_ctx->embedding_vids_));
  } else if (OB_FAIL(adaptor.get_dim(dim))) {
    LOG_WARN("get dim failed", K(ret));
  } else if (OB_ISNULL(vector_buf = static_cast<float*>(allocator_.alloc(dim * sizeof(float))))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc mem.", K(ret), K(dim));
  } else {
    HEAP_VARS_3((blocksstable::ObDatumRow, new_row, tenant_id_), (storage::ObTableScanParam, vid_rowkey_scan_param), (schema::ObTableParam, vid_rowkey_table_param, allocator_)) {
      ObArenaAllocator scan_allocator("VecEmbedding", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
      common::ObNewRowIterator *vid_rowkey_iter = nullptr;
      ObTableScanIterator *table_scan_iter = nullptr;
      blocksstable::ObDatumRow *datum_row = nullptr;
      int64_t loop_cnt = 0;
      if (OB_FAIL(new_row.init(task_ctx->embedded_table_column_ids_.count()))) {
        LOG_WARN("fail to init datum row", K(ret), K(task_ctx->embedded_table_column_ids_), K(new_row));
      } else if (adaptor.get_is_need_vid() && OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
              &adaptor,
              ctx_->task_status_.target_scn_,
              INDEX_TYPE_VEC_VID_ROWKEY_LOCAL,
              allocator_,
              scan_allocator,
              vid_rowkey_scan_param,
              vid_rowkey_table_param,
              vid_rowkey_iter))) {
        LOG_WARN("failed to read vid rowkey tablet.", K(ret));
      }
      // col order of 6th table is rowkey vid vector or pk(vid) part_key vector
      const int64_t embedded_rowkey_count = task_ctx->embedded_table_column_ids_.count() - task_ctx->part_key_num_ - 1;
      void *buf = nullptr;
      ObObj *obj_ptr =  nullptr;
      if (OB_FAIL(ret)) {
      } else if (embedded_rowkey_count <= 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get embedded_rowkey_count invalid.", K(ret), K(embedded_rowkey_count));
      } else {
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObObj) * (embedded_rowkey_count)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc mem.", K(ret), K(embedded_rowkey_count));
        } else {
          obj_ptr = new (buf) ObObj[embedded_rowkey_count];
        }
      }
      for (int64_t row_id = 0; row_id < task_ctx->embedding_vids_.count() && OB_SUCC(ret); row_id++) {
        for (int64_t i = 0; i < dim; i++) {
          vector_buf[i] = output_vector.at(row_id)[i];
        }
        obj_ptr[embedded_rowkey_count - 1].set_uint64(task_ctx->embedding_vids_.at(row_id));
        if (adaptor.get_is_need_vid()) {
          ObObj vid_obj;
          vid_obj.set_int(task_ctx->embedding_vids_.at(row_id));
          ObRowkey rowkey(&vid_obj, 1);
          blocksstable::ObDatumRow *vid_rowkey_datum = nullptr;
          obj_ptr[embedded_rowkey_count - 1].set_int(task_ctx->embedding_vids_.at(row_id));
          if (OB_FAIL(ObPluginVectorIndexUtils::add_key_ranges(adaptor.get_vid_rowkey_table_id(), rowkey, vid_rowkey_scan_param))) {
            LOG_WARN("failed to set vid id key", K(ret));
          } else if (OB_FAIL(ObPluginVectorIndexUtils::iter_table_rescan(vid_rowkey_scan_param, vid_rowkey_iter))) {
            LOG_WARN("failed to recan vid id scan param.", K(ret));
          } else if (OB_ISNULL(table_scan_iter = dynamic_cast<storage::ObTableScanIterator *>(vid_rowkey_iter))) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("failed to get vid rowkey iter.", K(ret));
          } else if (OB_FAIL(table_scan_iter->get_next_row(vid_rowkey_datum))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("failed to get next row from next table.", K(ret));
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("fail to get rowkey from vid rowkey table", K(ret), K(rowkey));
            }
          } else if (OB_ISNULL(vid_rowkey_datum) || !vid_rowkey_datum->is_valid()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("get row invalid.", K(ret));
          } else if (vid_rowkey_datum->get_column_count() != task_ctx->embedded_table_column_ids_.count() - 1) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("column count mismatch", K(ret), K(vid_rowkey_datum), K(task_ctx->embedded_table_column_ids_), K(row_id));
          } else {
            const ObIArray<share::schema::ObColumnParam *> *out_col_param  = vid_rowkey_scan_param.table_param_->get_read_info().get_columns();
            for (int64_t i = 0; OB_SUCC(ret) && i < task_ctx->embedded_table_column_ids_.count() - 2; i++) {
              ObObj tmp_obj;
              ObObjMeta meta_type = out_col_param->at(i + 1)->get_meta_type();
              if (OB_FAIL(vid_rowkey_datum->storage_datums_[i + 1].to_obj(tmp_obj, meta_type))) {
                LOG_WARN("failed to convert datum to obj.", K(ret), K(i), K(vid_rowkey_datum->storage_datums_[i + 1]));
              } else if (OB_FAIL(ob_write_obj(allocator_, tmp_obj, obj_ptr[i]))) {
                LOG_WARN("failed to write obj.", K(ret), K(i), K(tmp_obj));
              }
            }
          }
        }
        ObSEArray<uint64_t, 4> dml_column_ids;
        if (OB_SUCC(ret)) {
          HEAP_VARS_2((storage::ObTableScanParam, embedded_scan_param), (schema::ObTableParam, embedded_table_param, allocator_)) {
            common::ObNewRowIterator *embedded_scan_iter = nullptr;
            ObTableScanIterator *embedded_table_scan_iter = nullptr;
            ObArenaAllocator embedde_scan_allocator("VecEmbedding", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
            ObRowkey rowkey(obj_ptr, embedded_rowkey_count);
            if (OB_FAIL(ObPluginVectorIndexUtils::read_local_tablet(ls_id_,
                &adaptor,
                ctx_->task_status_.target_scn_,
                INDEX_TYPE_HYBRID_INDEX_EMBEDDED_LOCAL,
                allocator_,
                embedde_scan_allocator,
                embedded_scan_param,
                embedded_table_param,
                embedded_scan_iter,
                &dml_column_ids,
                true))) {
              LOG_WARN("failed to read embeded vector table", K(ret));
            } else if (OB_FAIL(ObPluginVectorIndexUtils::add_key_ranges(adaptor.get_embedded_table_id(), rowkey, embedded_scan_param))) {
              LOG_WARN("failed to set embedded row key", K(ret));
            } else if (OB_FAIL(ObPluginVectorIndexUtils::iter_table_rescan(embedded_scan_param, embedded_scan_iter))) {
              LOG_WARN("failed to rescan embedded scan param.", K(ret));
            } else if (OB_ISNULL(embedded_table_scan_iter = dynamic_cast<storage::ObTableScanIterator *>(embedded_scan_iter))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("failed to get embedded scan iter.", K(ret));
            } else if (OB_FAIL(embedded_table_scan_iter->get_next_row(datum_row))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("failed to get next row from next table.", K(ret));
              }
            } else if (OB_ISNULL(datum_row) || !datum_row->is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("get row invalid.", K(ret));
            } else if (OB_FAIL(new_row.deep_copy(*datum_row, allocator_))) {
              LOG_WARN("failed to copy row", K(ret), KP(datum_row));
            } else if (FALSE_IT(new_row.storage_datums_[task_ctx->embedded_table_column_ids_.count() - 1].set_string(reinterpret_cast<char *>(vector_buf), dim * sizeof(float)))) {
            } else if (OB_FAIL(embedded_iter.add_row(*datum_row, new_row))) {
              LOG_WARN("failed to add row to index id iter", K(ret));
            }
            new_row.reuse();
            int tmp_ret = OB_SUCCESS;
            if (OB_NOT_NULL(oas) && OB_NOT_NULL(embedded_scan_iter)) {
              tmp_ret = oas->revert_scan_iter(embedded_scan_iter);
              if (tmp_ret != OB_SUCCESS) {
                LOG_WARN("revert scan iter failed", K(tmp_ret));
              }
              embedded_scan_iter = nullptr;
            }
          }
        }

        CHECK_TASK_CANCELLED_IN_PROCESS(ret, loop_cnt, ctx_);
      }
      if (OB_NOT_NULL(tsc_service)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_NOT_NULL(vid_rowkey_iter)) {
          tmp_ret = tsc_service->revert_scan_iter(vid_rowkey_iter);
          if (tmp_ret != OB_SUCCESS) {
            LOG_WARN("revert vid_rowkey_iter failed", K(ret));
          }
        }
        vid_rowkey_iter = nullptr;
      }
    }

    // insert into 6 table.
    int64_t affected_rows = 0;
    ObDMLBaseParam dml_param;
    share::schema::ObTableDMLParam table_dml_param(allocator_);
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(oas)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), KPC(task_ctx), K(oas));
    } else if (OB_FAIL(init_dml_param(adaptor.get_embedded_table_id(), dml_param, table_dml_param, task_ctx->embedded_table_column_ids_, tx_desc, snapshot, store_ctx_guard))) {
      LOG_WARN("failed to init dml param", K(ret), K(dml_param), K(table_dml_param));
    } else if (OB_FAIL(oas->update_rows(ls_id_, adaptor.get_embedded_tablet_id(), *tx_desc, dml_param, task_ctx->embedded_table_column_ids_, task_ctx->embedded_table_update_ids_, &embedded_iter, affected_rows))) {
      LOG_WARN("failed to insert rows to embedded table", K(ret), K(adaptor.get_embedded_tablet_id()));
    }
    store_ctx_guard.reset();
    task_ctx->embedding_task_->~ObEmbeddingTask();
    task_ctx->allocator_.free(task_ctx->embedding_task_);
    task_ctx->embedding_task_ = nullptr;
    task_ctx->embedding_vids_.reuse();
  }

  embedded_iter.reset();
  int tmp_ret = OB_SUCCESS;
  timeout_us = ObTimeUtility::current_time() + ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
  if (OB_NOT_NULL(tx_desc) && trans_start && OB_SUCCESS != (tmp_ret = ObInsertLobColumnHelper::end_trans(tx_desc, ret != OB_SUCCESS, timeout_us))) {
    ret = tmp_ret;
    LOG_WARN("fail to end trans", K(ret), KPC(tx_desc));
  }
  if (OB_FAIL(ret) || !embedding_finish) {
    // do nothing.
  } else {
    task_ctx->delete_vids_.reuse();
    task_ctx->status_ = ObHybridVectorRefreshTaskStatus::PREPARE_EMBEDDING;
  }
  return ret;
}

void ObHybridVectorRefreshTaskCtx::check_task_free()
{
  if (OB_NOT_NULL(embedding_task_)) {
    while (!embedding_task_->is_completed()) {
      ob_usleep(5LL * 1000 * 1000);
    }
    embedding_task_->~ObEmbeddingTask();
    allocator_.free(embedding_task_);
    embedding_task_ = nullptr;
  }
  set_task_finish();
}

void ObHybridVectorRefreshTaskCtx::set_task_finish()
{
  int ret = OB_SUCCESS;
  task_status_.all_finished_ = true;
  status_ = ObHybridVectorRefreshTaskStatus::TASK_FINISH;
  delta_delete_iter_.reset();
  ObAccessService *oas = MTL(ObAccessService *);
  if (OB_NOT_NULL(oas) && OB_NOT_NULL(scan_iter_)) {
    ret = oas->revert_scan_iter(scan_iter_);
    if (ret != OB_SUCCESS) {
      LOG_WARN("revert scan iter failed", K(ret));
    }
    scan_iter_ = nullptr;
    table_scan_param_ = nullptr;
    table_param_ = nullptr;
  }
  if (task_started_) {
    adp_guard_.get_adatper()->vector_index_task_finish();
    adp_guard_.~ObPluginVectorIndexAdapterGuard();
    task_started_ = false;
  }
}

}
}
