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

#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_inc_task.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_cg_macro_block_write_task.h"
#include "storage/ddl/ob_group_write_macro_block_task.h"
#include "storage/ddl/ob_ddl_pipeline.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/column_store/ob_column_store_replica_util.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/ddl/ob_ddl_merge_task_v2.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "share/ob_server_struct.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase;
using namespace oceanbase::storage;
using namespace oceanbase::sql;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

ObDDLIndependentDag::ObDDLIndependentDag()
  : ObIndependentDag(share::ObDagType::DAG_TYPE_DDL),
    is_inited_(false),
    arena_(ObMemAttr(MTL_ID(), "ddl_dag")),
    direct_load_type_(ObDirectLoadType::DIRECT_LOAD_INVALID),
    ddl_thread_count_(0),
    pipeline_count_(0),
    ret_code_(OB_SUCCESS),
    is_inc_major_log_(false)
{

}

void free_tablet_context(ObIAllocator &allocator, ObDDLTabletContext *tablet_context)
{
  if (OB_NOT_NULL(tablet_context)) {
    tablet_context->~ObDDLTabletContext();
    allocator.free(tablet_context);
  }
}

ObDDLIndependentDag::~ObDDLIndependentDag()
{
  reuse();
}

void ObDDLIndependentDag::reuse()
{
  FLOG_INFO("ddl independent dag reuse");
  is_inited_ = false;
  direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  ddl_thread_count_ = 0;
  ddl_task_param_.reset();
  ObTabletObjLoadHelper::free(arena_, ddl_table_schema_.storage_schema_);
  ObTabletObjLoadHelper::free(arena_, ddl_table_schema_.lob_meta_storage_schema_);
  ddl_table_schema_.reset();
  tx_info_.reset();
  ls_tablet_ids_.reset();
  FOREACH(tc_it, tablet_context_map_) {
    ObDDLTabletContext *tablet_context = tc_it->second;
    free_tablet_context(arena_,  tablet_context);
  }
  IGNORE_RETURN tablet_context_map_.destroy();
  pipeline_count_ = 0;
  ret_code_ = OB_SUCCESS;
  is_inc_major_log_ = false;
  arena_.reset();
}

int ObDDLIndependentDag::init_by_param(const share::ObIDagInitParam *param)
{
  int ret = OB_SUCCESS;
  const  ObDDLIndependentDagInitParam *init_param = static_cast<const ObDDLIndependentDagInitParam *>(param);
  if (OB_UNLIKELY(nullptr == init_param || !init_param->is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KPC(init_param));
  } else if (init_param->ddl_task_param_.tenant_data_version_ < DDL_IDEM_DATA_FORMAT_VERSION) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("reject execute dag when request comes from old version", K(ret), KPC(init_param));
  } else if (OB_FAIL(ls_tablet_ids_.assign(init_param->ls_tablet_ids_))) {
    LOG_WARN("assign ls tablet id array failed", K(ret), K(init_param->ls_tablet_ids_));
  } else {
    direct_load_type_ = init_param->direct_load_type_;
    ddl_thread_count_ = init_param->ddl_thread_count_;
    ddl_task_param_ = init_param->ddl_task_param_;
    tx_info_ = init_param->tx_info_;
    is_inc_major_log_ = init_param->is_inc_major_log_;
    if (OB_FAIL(init_ddl_table_schema())) {
      LOG_WARN("init ddl table schema failed", K(ret));
    } else if (OB_FAIL(init_tablet_context_map())) {
      LOG_WARN("init tablet context failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  FLOG_INFO("ddl independent dag init", K(ret), KPC(this), K(ddl_table_schema_), K(tx_info_), K(ls_tablet_ids_), K(tablet_context_map_.size()));
  return ret;
}

int ObDDLIndependentDag::init_ddl_table_schema()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObDDLTableSchema::fill_ddl_table_schema(MTL_ID(), ddl_task_param_.target_table_id_, ddl_task_param_.tenant_data_version_, arena_, ddl_table_schema_))) {
    LOG_WARN("fill ddl table schema failed", K(ret));
  }
  return ret;
}

int ObDDLIndependentDag::init_tablet_context_map()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tablet_context_map_.create(ls_tablet_ids_.count(), ObMemAttr(MTL_ID(), "ddl_dag_ctx_map")))) {
    LOG_WARN("create tablet context map failed", K(ret), K(ls_tablet_ids_.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids_.count(); ++i) {
    const ObLSID &ls_id = ls_tablet_ids_.at(i).first;
    const ObTabletID &tablet_id = ls_tablet_ids_.at(i).second;
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_ISNULL(tablet_context = OB_NEWx(ObDDLTabletContext, &arena_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for tablet context failed", K(ret));
    } else if (OB_FAIL(tablet_context->init(ls_id, tablet_id, ddl_thread_count_, ddl_task_param_.snapshot_version_, direct_load_type_, ddl_table_schema_, ddl_task_param_.ddl_task_id_))) {
      LOG_WARN("init ddl tablet context failed", K(ret), K(ls_id), K(tablet_id), K(ddl_thread_count_));
    } else if (use_tablet_mode() && OB_FAIL(alloc_task(tablet_context->scan_task_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("alloc tablet scan task failed", K(ret), K(tablet_id));
    } else if (OB_FAIL(tablet_context_map_.set_refactored(tablet_id, tablet_context))) {
      LOG_WARN("set tablet context into map failed", K(ret), K(tablet_id), KPC(tablet_context));
    } else {
      FLOG_INFO("init ddl tablet context", K(tablet_id), KPC(tablet_context));
    }
    if (OB_FAIL(ret) && nullptr != tablet_context) {
      free_tablet_context(arena_, tablet_context);
      tablet_context = nullptr;
    }
  }
  return ret;
}

int ObDDLIndependentDag::get_tablet_context(const ObTabletID &tablet_id, ObDDLTabletContext *&tablet_context)
{
  int ret = OB_SUCCESS;
  tablet_context = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_FAIL(tablet_context_map_.get_refactored(tablet_id, tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
  }
  return ret;
}

int ObDDLIndependentDag::schedule_tablet_merge_task()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids_.count(); ++i) {
      share::SCN mock_start_scn;
      const ObTabletID &tablet_id = ls_tablet_ids_.at(i).second;

      ObDDLTabletContext *tablet_context = nullptr;

      if (OB_FAIL(mock_start_scn.convert_for_tx(SS_DDL_START_SCN_VAL))) {
        LOG_WARN("failed to convert for tx", K(ret));
      } else if (OB_FAIL(get_tablet_context(tablet_id, tablet_context))) {
        LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
      }
      /* create merge task for data tablet*/

      ObDDLTabletMergeDagParamV2 merge_param;
      ObDDLMergePrepareTask *ddl_merge_task = nullptr;
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(merge_param.init(true  /*for major*/,
                                          false /* for lob*/,
                                          false /* for replay*/,
                                          mock_start_scn,
                                          direct_load_type_,
                                          ddl_task_param_,
                                          tablet_context))) {
        LOG_WARN("failed to init  ddl merge task param", K(ret));
      } else if (OB_FAIL(create_task(nullptr /* parent task*/, ddl_merge_task, merge_param))) {
        LOG_WARN("failed to create ddl merge taks ", K(ret));
      } else if (OB_FAIL(add_task(*ddl_merge_task))) {
        LOG_WARN("failed to add task", K(ret));
      }

      /* create merge task for lob tablet*/
      ObDDLTabletMergeDagParamV2 lob_merge_param;
      ObDDLMergePrepareTask *lob_merge_task = nullptr;
      if (OB_FAIL(ret)) {
      } else if (!tablet_context->lob_meta_tablet_id_.is_valid()) {
        /* skip */
      } else if (OB_FAIL(lob_merge_param.init(true  /*for major*/,
                                          true /* for lob*/,
                                          false /* for replay*/,
                                          mock_start_scn,
                                          direct_load_type_,
                                          ddl_task_param_,
                                          tablet_context))) {
        LOG_WARN("failed to init  ddl merge task param", K(ret));
      } else if (OB_FAIL(create_task(nullptr /* parent task*/, lob_merge_task, lob_merge_param))) {
        LOG_WARN("failed to create ddl merge taks ", K(ret));
      } else if (OB_FAIL(add_task(*lob_merge_task))) {
        LOG_WARN("failed to add task", K(ret));
      }
    }
  }
  return ret;
}

int ObDDLIndependentDag::add_scan_chunk(ObDDLChunk &ddl_chunk, const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!ddl_chunk.is_valid() || timeout_us < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ddl_chunk), K(timeout_us));
  } else {
    ObDDLTabletContext *tablet_context = nullptr;
    ObDDLSlice *ddl_slice = nullptr;
    bool is_new_slice = false;
    const bool need_end_chunk = ddl_chunk.is_slice_end_ && (nullptr == ddl_chunk.chunk_data_ ||
                                                            !ddl_chunk.chunk_data_->is_end_chunk());

    if (OB_UNLIKELY(nullptr != ddl_chunk.chunk_data_ &&
                    !(ddl_chunk.chunk_data_->is_cg_row_tmp_files_type() || ddl_chunk.chunk_data_->is_end_chunk()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid chunk data", K(ret), KPC(ddl_chunk.chunk_data_));
    } else if (OB_FAIL(get_tablet_context(ddl_chunk.tablet_id_, tablet_context))) {
      LOG_WARN("get tablet context failed", K(ret), K(ddl_chunk));
    } else if (OB_FAIL(tablet_context->get_or_create_slice(ddl_chunk.slice_idx_, ddl_slice, is_new_slice))) {
      LOG_WARN("get ddl slice failed", K(ret));
    } else if (nullptr != ddl_chunk.chunk_data_ &&
               OB_FAIL(push_chunk(ddl_slice, ddl_chunk.chunk_data_))) {
      LOG_WARN("push chunk failed", K(ret), KPC(ddl_slice));
    } else if (FALSE_IT(ddl_chunk.chunk_data_ = nullptr)) {
    } else if (need_end_chunk) {
      ObChunk *end_chunk = OB_NEW(ObChunk, ObMemAttr(MTL_ID(), "ddl_end_chunk"));
      if (OB_ISNULL(end_chunk)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret));
      } else {
        end_chunk->set_end_chunk();
        if (OB_FAIL(push_chunk(ddl_slice, end_chunk))) {
          LOG_WARN("push end chunk failed", K(ret), KPC(ddl_slice));
          int tmp_ret = OB_SUCCESS;
          // ignore ret
          (void)finish_chunk(end_chunk);
        }
      }
    }
    if (OB_SUCC(ret) && is_new_slice) {
      const ObIndexType index_type = tablet_context->tablet_param_.storage_schema_->get_index_type();
      LOG_INFO("add pipeline", K(ret), K(index_type));
      if (OB_FAIL(add_pipeline(tablet_context, ddl_slice, index_type))) {
        LOG_WARN("fail to add pipeline", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      // ignore ret
      (void)finish_chunk(ddl_chunk.chunk_data_);
    }
  }
  return ret;
}

int ObDDLIndependentDag::push_chunk(ObDDLSlice *ddl_slice, ObChunk *&chunk_data)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_slice || nullptr == chunk_data)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", KR(ret), KP(ddl_slice), KP(chunk_data));
  } else {
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(is_final_status())) {
        ret = get_dag_ret();
        ret = COVER_SUCC(OB_CANCELED);
        LOG_WARN("dag is stoped", K(ret));
      } else if (OB_FAIL(ddl_slice->push_chunk(chunk_data))) {
        if (OB_UNLIKELY(OB_EAGAIN != ret)) {
          LOG_WARN("push chunk failed", K(ret), KPC(chunk_data));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        break;
      }
    }
  }
  return ret;
}

int ObDDLIndependentDag::add_pipeline(
    ObDDLTabletContext *tablet_context,
    ObDDLSlice *ddl_slice,
    const ObIndexType &index_type)
{
  int ret = OB_SUCCESS;
  if (ObDDLUtil::is_vector_index_complement(index_type)) {
    if (OB_FAIL(add_vector_index_append_pipeline(index_type, tablet_context, ddl_slice))) {
      LOG_WARN("add vector index pipeline failed", K(ret));
    }
  } else {
    ObDDLMemoryFriendWriteMacroBlockPipeline *pipeline = nullptr;
    if (OB_FAIL(add_pipeline(tablet_context, ddl_slice, pipeline))) {
      LOG_WARN("fail to add pipeline", K(ret), KPC(ddl_slice));
    }
  }
  return ret;
}

int ObDDLIndependentDag::add_vector_index_append_pipeline(const ObIndexType &index_type, ObDDLTabletContext *tablet_context, ObDDLSlice *ddl_slice)
{
  int ret = OB_SUCCESS;
  if (schema::is_vec_index_snapshot_data_type(index_type)) {
    ObHNSWAppendPipeline *pipeline = nullptr;
    if (OB_FAIL(add_pipeline(tablet_context, ddl_slice, pipeline))) {
      LOG_WARN("init hnsw index failed", K(ret));
    }
  } else if (schema::is_local_vec_ivf_centroid_index(index_type)) {
    ObIVFCenterAppendPipeline *pipeline = nullptr;
    if (OB_FAIL(add_pipeline(tablet_context, ddl_slice, pipeline))) {
      LOG_WARN("init hnsw index failed", K(ret));
    }
  } else if (schema::is_vec_ivfsq8_meta_index(index_type)) {
    ObIVFSq8MetaAppendPipeline *pipeline = nullptr;
    if (OB_FAIL(add_pipeline(tablet_context, ddl_slice, pipeline))) {
      LOG_WARN("init hnsw index failed", K(ret));
    }
  } else if (schema::is_vec_ivfpq_pq_centroid_index(index_type)) {
    ObIVFPqAppendPipeline *pipeline = nullptr;
    if (OB_FAIL(add_pipeline(tablet_context, ddl_slice, pipeline))) {
      LOG_WARN("init hnsw index failed", K(ret));
    }
  } else if (schema::is_hybrid_vec_index_embedded_type(index_type)) {
    ObHNSWEmbeddingAppendAndWritePipeline *pipeline = nullptr;
    if (OB_FAIL(add_pipeline(tablet_context, ddl_slice, pipeline))) {
      LOG_WARN("init hnsw index failed", K(ret));
    }
  }
  return ret;
}

int ObDDLIndependentDag::alloc_vector_index_write_and_build_pipeline(
    const ObIndexType &index_type,
    const ObIArray<std::pair<share::ObLSID, ObTabletID>> &ls_tablet_ids,
    ObIArray<ObITask *> &vector_index_task_array)
{
  int ret = OB_SUCCESS;
  vector_index_task_array.reuse();
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids.count(); ++i) {
    const ObTabletID &tablet_id = ls_tablet_ids.at(i).second;
    ObITask *vector_index_task = nullptr;
    if (schema::is_vec_index_snapshot_data_type(index_type)) {
      bool is_vec_tablet_rebuild = ddl_table_schema_.table_item_.is_vec_tablet_rebuild_;
      if (is_vec_tablet_rebuild) {
        ObHNSWBuildAndDMLWritePipeline *pipeline = nullptr;
        if (OB_FAIL(alloc_task(pipeline))) {
          LOG_WARN("alloc task failed", K(ret));
        } else if (OB_FAIL(pipeline->init(tablet_id))) {
          LOG_WARN("init pipeline failed", K(ret));
        } else {
          vector_index_task = pipeline;
        }
      } else {
        ObHNSWBuildAndWritePipeline *pipeline = nullptr;
        if (OB_FAIL(alloc_task(pipeline))) {
          LOG_WARN("alloc task failed", K(ret));
        } else if (OB_FAIL(pipeline->init(tablet_id))) {
          LOG_WARN("init pipeline failed", K(ret));
        } else {
          vector_index_task = pipeline;
        }
      }
    } else if (schema::is_local_vec_ivf_centroid_index(index_type)) {
      ObIVFCenterBuildAndWritePipeline *pipeline = nullptr;
      if (OB_FAIL(alloc_task(pipeline))) {
        LOG_WARN("alloc task failed", K(ret));
      } else if (OB_FAIL(pipeline->init(tablet_id))) {
        LOG_WARN("init pipeline failed", K(ret));
      } else {
        vector_index_task = pipeline;
      }
    } else if (schema::is_vec_ivfsq8_meta_index(index_type)) {
      ObIVFSq8MetaBuildAndWritePipeline *pipeline = nullptr;
      if (OB_FAIL(alloc_task(pipeline))) {
        LOG_WARN("alloc task failed", K(ret));
      } else if (OB_FAIL(pipeline->init(tablet_id))) {
        LOG_WARN("init pipeline failed", K(ret));
      } else {
        vector_index_task = pipeline;
      }
    } else if (schema::is_vec_ivfpq_pq_centroid_index(index_type)) {
      ObIVFPqBuildAndWritePipeline *pipeline = nullptr;
      if (OB_FAIL(alloc_task(pipeline))) {
        LOG_WARN("init hnsw index failed", K(ret));
      } else if (OB_FAIL(pipeline->init(tablet_id))) {
        LOG_WARN("init pipeline failed", K(ret));
      } else {
        vector_index_task = pipeline;
      }
    }
    if (OB_SUCC(ret) && nullptr != vector_index_task) {
      if (OB_FAIL(vector_index_task_array.push_back(vector_index_task))) {
        LOG_WARN("push back vector index task failed", K(ret));
      } else {
        LOG_INFO("alloc vector index write and build pipeline", K(index_type), K(*vector_index_task));
      }
    }
  }
  return ret;
}


template<typename T>
int ObDDLIndependentDag::add_pipeline(ObDDLTabletContext *tablet_context, ObDDLSlice *ddl_slice, T *&pipeline)
{
  int ret = OB_SUCCESS;
  pipeline = nullptr;
  if (OB_UNLIKELY(nullptr == tablet_context || nullptr == ddl_slice)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KP(tablet_context), KP(ddl_slice));
  } else if (OB_FAIL(alloc_task(pipeline))) {
    LOG_WARN(" alloc pipeline failed", K(ret));
  } else if (OB_FAIL(pipeline->init(ddl_slice))) {
    LOG_WARN("init pipeline failed", K(ret));
  } else if (nullptr != tablet_context->scan_task_ &&
             OB_FAIL(pipeline->add_child(*tablet_context->scan_task_))) {
    LOG_WARN("fail to add child", K(ret));
  } else {
    inc_pipeline_count();
    if (OB_FAIL(add_task(*pipeline))) {
      LOG_WARN("add pipeline failed", K(ret));
      dec_pipeline_count();
    }
  }
  return ret;
}

void ObDDLIndependentDag::set_ret_code(const int ret_code)
{
  if (OB_SUCCESS == ret_code_) {
    ATOMIC_SET(&ret_code_, ret_code);
  }
}

int ObDDLIndependentDag::generate_start_tasks(ObIArray<ObITask *> &start_tasks, ObITask *parent_task)
{
  int ret = OB_SUCCESS;
  start_tasks.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIndependentDag not init", KR(ret), KP(this));
  } else if (is_incremental_direct_load(direct_load_type_)) { // 增量
    ObDDLIncPrepareTask *inc_prepare_task = nullptr;
    ObDDLIncStartTask *inc_start_task = nullptr;
    if (OB_FAIL(alloc_task(inc_start_task, 0 /*tablet_idx*/))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(start_tasks.push_back(inc_start_task))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(alloc_task(inc_prepare_task))) {
      LOG_WARN("fail to alloc task", KR(ret));
    } else if (OB_FAIL(start_tasks.push_back(inc_prepare_task))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (OB_FAIL(inc_prepare_task->add_child(*inc_start_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_NOT_NULL(parent_task) && OB_FAIL(parent_task->copy_children_to(*inc_start_task))) {
      LOG_WARN("fail to copy children", KR(ret));
    }
  }
  return ret;
}

int ObDDLIndependentDag::check_is_first_ddl_kv(bool &is_first)
{
  int ret = OB_SUCCESS;
  is_first = true;
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids_.count(); ++i) {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObTabletMapKey key;

    key.ls_id_ = ls_tablet_ids_.at(i).first;
    key.tablet_id_ = ls_tablet_ids_.at(i).second;

    bool tmp_is_first = false;
    ObDDLKvMgrHandle ddl_kv_mgr_handle;
    if (OB_FAIL(t3m->get_tablet_ddl_kv_mgr(key, ddl_kv_mgr_handle))) {
      LOG_WARN("get tablet ddl kv mgr failed", K(ret), K(key));
    } else if (OB_FAIL(check_is_first_ddl_kv(*(ddl_kv_mgr_handle.get_obj()), tmp_is_first))) {
      LOG_WARN("fail to check_is_first_ddl_kv", KR(ret));
    } else if (!tmp_is_first) {
      is_first = false;
      break;
    }
  }
  return ret;
}


int ObDDLIndependentDag::check_is_first_ddl_kv(ObTabletDDLKvMgr &ddl_kv_mgr,
                                              bool &is_first)
{
 int ret = OB_SUCCESS;
  ObArray<ObDDLKVHandle> ddl_kv_handles;
  ObDDLKVQueryParam query_param;
  query_param.ddl_kv_type_ = ObDDLKVType::DDL_KV_INC_MAJOR;
  query_param.trans_id_ = transaction::ObTransID();
  query_param.seq_no_ = transaction::ObTxSEQ();

  is_first = false;

  if (OB_FAIL(ddl_kv_mgr.get_ddl_kvs(false/*frozen_only*/,
                                    ddl_kv_handles,
                                    query_param))) {
    LOG_WARN("failed to get ddl kvs", K(ret));
  } else {
    if (ddl_kv_handles.count() <= 0) {
      is_first = true;
    } else {
      ObDDLKV *ddl_kv = ddl_kv_handles.at(0).get_obj();
      if (ddl_kv->get_trans_id() == tx_info_.trans_id_ &&
          ddl_kv->get_seq_no() == transaction::ObTxSEQ::cast_from_int(tx_info_.seq_no_)) {
        is_first = true;
      }
    }
  }
  return ret;
}

int ObDDLIndependentDag::inc_generate_write_macro_block_tasks(ObIArray<ObITask *> &write_macro_block_tasks, ObITask *next_task)
{
  int ret = OB_SUCCESS;
  // scan_task -> group_write_task -> inc_commit_task -> [next_task]
  ObDDLScanTask *scan_task = nullptr;
  ObGroupWriteMacroBlockTask *group_write_task = nullptr;
  ObDDLIncCommitTask *inc_commit_task = nullptr;
  if (OB_FAIL(alloc_task(scan_task))) {
    LOG_WARN("fail to alloc scan task", KR(ret));
  } else if (OB_FAIL(scan_task->init(this))) {
    LOG_WARN("fail to init scan task", K(ret));
  } else if (OB_FAIL(write_macro_block_tasks.push_back(scan_task))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  // group_write_task
  else if (OB_FAIL(alloc_task(group_write_task))) {
    LOG_WARN("fail to alloc group write task", K(ret));
  } else if (OB_FAIL(group_write_task->init(this))) {
    LOG_WARN("fail to init group write task", K(ret));
  } else if (OB_FAIL(write_macro_block_tasks.push_back(group_write_task))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(scan_task->add_child(*group_write_task))) {
    LOG_WARN("fail to add child", KR(ret));
  }
  // inc_commit_task
  else if (OB_FAIL(alloc_task(inc_commit_task, 0/*tablet_idx*/))) {
    LOG_WARN("fail to alloc inc commit task", KR(ret));
  } else if (OB_FAIL(write_macro_block_tasks.push_back(inc_commit_task))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (OB_FAIL(group_write_task->add_child(*inc_commit_task))) {
    LOG_WARN("fail to add child", KR(ret));
  }


  bool wait_dump = false;
  if (OB_SUCC(ret)) {
    // inc major direct load required foreground dump:
    if (is_incremental_major_direct_load(direct_load_type_)) {
      wait_dump = true;
    }
  }

  if (OB_SUCC(ret) && (wait_dump)) {
    ObArray<ObITask*> data_merge_tasks;
    ObArray<ObITask*> lob_merge_tasks;

    // merge_tasks
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(init_merge_tasks(false/*for_major*/, data_merge_tasks, lob_merge_tasks))) {
      LOG_WARN("fail to init merge tasks", KR(ret));
    } else if (OB_UNLIKELY(data_merge_tasks.empty() ||
                           (!lob_merge_tasks.empty() &&
                            data_merge_tasks.count() != lob_merge_tasks.count()))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected merge tasks", KR(ret), K(data_merge_tasks.count()),
               K(lob_merge_tasks.count()));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < data_merge_tasks.count(); ++i) {
        ObITask *data_merge_task = data_merge_tasks.at(i);
        ObITask *lob_merge_task = lob_merge_tasks.empty() ? nullptr : lob_merge_tasks.at(i);
        if (OB_FAIL(write_macro_block_tasks.push_back(data_merge_task))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (nullptr != lob_merge_task &&
                   OB_FAIL(write_macro_block_tasks.push_back(lob_merge_task))) {
          LOG_WARN("fail to push back", KR(ret));
        }
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < data_merge_tasks.count(); ++i) {
      ObITask *data_merge_task = data_merge_tasks.at(i);
      ObITask *lob_merge_task = lob_merge_tasks.empty() ? nullptr : lob_merge_tasks.at(i);
      if (OB_FAIL(inc_commit_task->add_child(*data_merge_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != lob_merge_task && OB_FAIL(inc_commit_task->add_child(*lob_merge_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != next_task) {
        if (OB_FAIL(data_merge_task->add_child(*next_task))) {
          LOG_WARN("fail to add child", K(ret));
        } else if (nullptr != lob_merge_task && OB_FAIL(lob_merge_task->add_child(*next_task))) {
          LOG_WARN("fail to add child", K(ret));
        }
      }
    }
  } else {
    if (OB_FAIL(ret)) {
    } else if (nullptr != next_task && OB_FAIL(inc_commit_task->add_child(*next_task))) {
      LOG_WARN("fail to add child", K(ret));
    }
  }
  return ret;
}

int ObDDLIndependentDag::full_generate_write_macro_block_tasks(ObIArray<ObITask *> &write_macro_block_tasks, ObITask *next_task)
{
  int ret = OB_SUCCESS;
  // scan_task -> group_write_task|vector_index_tasks -> merge_tasks -> [next_task]
  ObDDLScanTask *scan_task = nullptr;
  ObGroupWriteMacroBlockTask *group_write_task = nullptr;
  ObArray<ObITask *> vector_index_tasks;
  ObArray<ObITask*> data_merge_tasks;
  ObArray<ObITask*> lob_merge_tasks;
  // scan_task
  if (OB_FAIL(alloc_task(scan_task))) {
    LOG_WARN("fail to alloc scan task", KR(ret));
  } else if (OB_FAIL(scan_task->init(this))) {
    LOG_WARN("fail to init scan task", K(ret));
  } else if (OB_FAIL(write_macro_block_tasks.push_back(scan_task))) {
    LOG_WARN("fail to push back", KR(ret));
  }
  // vector_index_task|group_write_task
  else if (OB_FAIL(alloc_vector_index_write_and_build_pipeline(ddl_table_schema_.table_item_.index_type_, ls_tablet_ids_, vector_index_tasks))) {
    LOG_WARN("alloc vector index failed", K(ret));
  } else if (!vector_index_tasks.empty()) {
    for (int64_t i = 0; OB_SUCC(ret) && i < vector_index_tasks.count(); ++i) {
      ObITask *vector_index_task = vector_index_tasks.at(i);
      if (OB_FAIL(write_macro_block_tasks.push_back(vector_index_task))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  } else {
    if (OB_FAIL(alloc_task(group_write_task))) {
      LOG_WARN("fail to alloc group write task", K(ret));
    } else if (OB_FAIL(group_write_task->init(this))) {
      LOG_WARN("fail to init group write task", K(ret));
    } else if (OB_FAIL(write_macro_block_tasks.push_back(group_write_task))) {
      LOG_WARN("fail to push back", KR(ret));
    }
  }

  const bool is_vec_tablet_rebuild = ddl_table_schema_.table_item_.is_vec_tablet_rebuild_;

  // merge_tasks
  if (OB_FAIL(ret)) {
  } else if (is_vec_tablet_rebuild) {
    LOG_INFO("skip vec table rebuild generate merge task", K(ddl_table_schema_));
  } else if (OB_FAIL(init_merge_tasks(true, data_merge_tasks, lob_merge_tasks))) {
    LOG_WARN("fail to init merge tasks", KR(ret));
  } else if (OB_UNLIKELY(data_merge_tasks.empty() ||
                         (!lob_merge_tasks.empty() &&
                          data_merge_tasks.count() != lob_merge_tasks.count()))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected merge tasks", KR(ret), K(data_merge_tasks.count()),
             K(lob_merge_tasks.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < data_merge_tasks.count(); ++i) {
      ObITask *data_merge_task = data_merge_tasks.at(i);
      ObITask *lob_merge_task = lob_merge_tasks.empty() ? nullptr : lob_merge_tasks.at(i);
      if (OB_FAIL(write_macro_block_tasks.push_back(data_merge_task))) {
        LOG_WARN("fail to push back", KR(ret));
      } else if (nullptr != lob_merge_task &&
                 OB_FAIL(write_macro_block_tasks.push_back(lob_merge_task))) {
        LOG_WARN("fail to push back", KR(ret));
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (vector_index_tasks.empty()) {
    if (OB_FAIL(scan_task->add_child(*group_write_task))) {
      LOG_WARN("fail to add child", KR(ret));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < data_merge_tasks.count(); ++i) {
      ObITask *data_merge_task = data_merge_tasks.at(i);
      ObITask *lob_merge_task = lob_merge_tasks.empty() ? nullptr : lob_merge_tasks.at(i);
      if (OB_FAIL(group_write_task->add_child(*data_merge_task))) {
        LOG_WARN("fail to add child", K(ret));
      } else if (nullptr != lob_merge_task && OB_FAIL(group_write_task->add_child(*lob_merge_task))) {
        LOG_WARN("fail to add child", K(ret));
      } else if (nullptr != next_task) {
        if (OB_FAIL(data_merge_task->add_child(*next_task))) {
          LOG_WARN("fail to add child", K(ret));
        } else if (nullptr != lob_merge_task && OB_FAIL(lob_merge_task->add_child(*next_task))) {
          LOG_WARN("fail to add child", K(ret));
        }
      }
    }
  } else {
    if (is_vec_tablet_rebuild) {
      if (nullptr != next_task || !data_merge_tasks.empty() || !lob_merge_tasks.empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected task pointer", K(ret), KP(next_task), K(data_merge_tasks.count()), K(lob_merge_tasks.count()));
      }
    } else if (OB_UNLIKELY(data_merge_tasks.count() != vector_index_tasks.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected task count not match", KR(ret), K(data_merge_tasks.count()), K(vector_index_tasks.count()));
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < vector_index_tasks.count(); ++i) {
      ObITask *vector_index_task = vector_index_tasks.at(i);
      ObITask *data_merge_task = data_merge_tasks.empty() ? nullptr : data_merge_tasks.at(i);
      ObITask *lob_merge_task = lob_merge_tasks.empty() ? nullptr : lob_merge_tasks.at(i);
      if (OB_FAIL(scan_task->add_child(*vector_index_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (is_vec_tablet_rebuild) {
        if (nullptr != next_task || nullptr != data_merge_task || nullptr != lob_merge_task) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected task pointer", K(ret), KP(next_task), KP(data_merge_task), KP(lob_merge_task));
        }
      } else if (OB_FAIL(vector_index_task->add_child(*data_merge_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != lob_merge_task && OB_FAIL(vector_index_task->add_child(*lob_merge_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != next_task) {
        if (OB_FAIL(data_merge_task->add_child(*next_task))) {
          LOG_WARN("fail to add child", K(ret));
        } else if (nullptr != lob_merge_task && OB_FAIL(lob_merge_task->add_child(*next_task))) {
          LOG_WARN("fail to add child", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObDDLIndependentDag::generate_write_macro_block_tasks(ObIArray<ObITask *> &write_macro_block_tasks, ObITask *next_task)
{
  int ret = OB_SUCCESS;
  write_macro_block_tasks.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIndependentDag not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(use_tablet_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", KR(ret), KPC(this));
  } else if (is_incremental_direct_load(direct_load_type_)) { // 增量
    if (OB_FAIL(inc_generate_write_macro_block_tasks(write_macro_block_tasks, next_task))) {
      LOG_WARN("fail to inc_generate_write_macro_block_tasks", KR(ret));
    }
  } else { // 全量
    if (OB_FAIL(full_generate_write_macro_block_tasks(write_macro_block_tasks, next_task))) {
      LOG_WARN("fail to full_generate_write_macro_block_tasks", KR(ret));
    }
  }
  return ret;
}

ERRSIM_POINT_DEF(INC_MAJOR_DIRECT_LOAD_DISABLE_WAIT_DUMP);
int ObDDLIndependentDag::generate_tablet_write_macro_block_tasks(
    const ObTabletID &tablet_id,
    ObIArray<share::ObITask *> &write_macro_block_tasks,
    ObITask *parent_task)
{
  int ret = OB_SUCCESS;
  write_macro_block_tasks.reset();
  ObDDLTabletContext *tablet_context = nullptr;
  ObDDLTabletScanTask *scan_task = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDDLIndependentDag not init", KR(ret), KP(this));
  } else if (OB_UNLIKELY(!use_tablet_mode())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", KR(ret), KPC(this));
  } else if (OB_FAIL(get_tablet_context(tablet_id, tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
  } else if (OB_ISNULL(scan_task = tablet_context->scan_task_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scan task is null", K(ret), K(tablet_id), KPC(tablet_context));
  } else if (FALSE_IT(tablet_context->scan_task_ = nullptr)) {
  } else if (OB_FAIL(write_macro_block_tasks.push_back(scan_task))) {
    LOG_WARN("fail to push back", KR(ret));
  } else if (is_incremental_direct_load(direct_load_type_)) { // 增量
    const bool for_major = GCTX.is_shared_storage_mode();
    ObGroupWriteMacroBlockTask *group_write_task = nullptr;
    ObDDLIncCommitTask *inc_commit_task = nullptr;
    ObITask *data_merge_task = nullptr;
    ObITask *lob_merge_task = nullptr;
    bool wait_dump = true;
    if (OB_UNLIKELY(INC_MAJOR_DIRECT_LOAD_DISABLE_WAIT_DUMP)) {
      wait_dump = false;
      LOG_INFO("inc major direct load disable wait dump", K(wait_dump));
    }
    // group_write_task
    if (OB_FAIL(alloc_task(group_write_task))) {
      LOG_WARN("fail to alloc group write task", K(ret));
    } else if (OB_FAIL(group_write_task->init(this, tablet_id))) {
      LOG_WARN("fail to init group write task", K(ret));
    } else if (OB_FAIL(write_macro_block_tasks.push_back(group_write_task))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    // inc_commit_task
    else if (OB_FAIL(alloc_task(inc_commit_task, tablet_id))) {
      LOG_WARN("fail to alloc inc commit task", KR(ret));
    } else if (OB_FAIL(write_macro_block_tasks.push_back(inc_commit_task))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    // merge_task
    else if (is_incremental_major_direct_load(direct_load_type_) && wait_dump &&
             OB_FAIL(init_tablet_merge_task(tablet_id, for_major, data_merge_task, lob_merge_task))) {
      LOG_WARN("fail to init tablet merge task", KR(ret));
    } else if (nullptr != data_merge_task &&
               OB_FAIL(write_macro_block_tasks.push_back(data_merge_task))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (nullptr != lob_merge_task &&
               OB_FAIL(write_macro_block_tasks.push_back(lob_merge_task))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    // 依赖关系
    else if (OB_FAIL(scan_task->add_child(*group_write_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (!GCTX.is_shared_storage_mode()) {
      // scan_task -> group_write_task -> inc_commit_task -> [merge_tasks] -> [parent_task::children]
      if (OB_FAIL(group_write_task->add_child(*inc_commit_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != data_merge_task && OB_FAIL(inc_commit_task->add_child(*data_merge_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != lob_merge_task && OB_FAIL(inc_commit_task->add_child(*lob_merge_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != parent_task) {
        if (nullptr == data_merge_task && nullptr == lob_merge_task) {
          if (OB_FAIL(parent_task->copy_children_to(*inc_commit_task))) {
            LOG_WARN("fail to copy children", KR(ret));
          }
        } else if (nullptr != data_merge_task && OB_FAIL(parent_task->copy_children_to(*data_merge_task))) {
          LOG_WARN("fail to copy children", KR(ret));
        } else if (nullptr != lob_merge_task && OB_FAIL(parent_task->copy_children_to(*lob_merge_task))) {
          LOG_WARN("fail to copy children", KR(ret));
        }
      }
    }
#ifdef OB_BUILD_SHARED_STORAGE
    else {
      // scan_task -> group_write_task -> [wait_dump_task] -> [merge_tasks] -> inc_commit_task -> [parent_task::children]
      if (OB_NOT_NULL(data_merge_task)) {
        ObDDLIncWaitDumpTask *wait_dump_task = nullptr;
        ObDDLIncWaitDumpTask *lob_wait_dump_task = nullptr;
        transaction::ObTxSEQ seq_no = transaction::ObTxSEQ::cast_from_int(tx_info_.seq_no_);
        if (OB_FAIL(alloc_task(wait_dump_task, tablet_context->ls_id_, tablet_id, tx_info_.trans_id_, seq_no))) {
          LOG_WARN("fail to alloc wait dump task", KR(ret), K(tx_info_));
        } else if (OB_FAIL(write_macro_block_tasks.push_back(wait_dump_task))) {
          LOG_WARN("fail to push back", KR(ret));
        } else if (OB_FAIL(group_write_task->add_child(*wait_dump_task))) {
          LOG_WARN("fail to add child", KR(ret));
        } else if (OB_FAIL(wait_dump_task->add_child(*data_merge_task))) {
          LOG_WARN("fail to add child", KR(ret));
        } else if (OB_FAIL(data_merge_task->add_child(*inc_commit_task))) {
          LOG_WARN("fail to add child", KR(ret));
        } else if (OB_NOT_NULL(parent_task) && OB_FAIL(parent_task->copy_children_to(*inc_commit_task))) {
          LOG_WARN("fail to copy children", KR(ret));
        } else if (OB_NOT_NULL(lob_merge_task)) {
          if (OB_FAIL(alloc_task(lob_wait_dump_task, tablet_context->ls_id_, tablet_context->lob_meta_tablet_id_, tx_info_.trans_id_, seq_no))) {
            LOG_WARN("fail to alloc lob wait dump task", KR(ret), K(tx_info_));
          } else if (OB_FAIL(write_macro_block_tasks.push_back(lob_wait_dump_task))) {
            LOG_WARN("fail to push back", KR(ret));
          } else if (OB_FAIL(group_write_task->add_child(*lob_wait_dump_task))) {
            LOG_WARN("fail to add child", KR(ret));
          } else if (OB_FAIL(lob_wait_dump_task->add_child(*lob_merge_task))) {
            LOG_WARN("fail to add child", KR(ret));
          } else if (OB_FAIL(lob_merge_task->add_child(*inc_commit_task))) {
            LOG_WARN("fail to add child", KR(ret));
          }
        }
      } else {
        if (OB_FAIL(group_write_task->add_child(*inc_commit_task))) {
          LOG_WARN("fail to add child", KR(ret));
        } else if (OB_NOT_NULL(parent_task) && OB_FAIL(parent_task->copy_children_to(*inc_commit_task))) {
          LOG_WARN("fail to copy children", KR(ret));
        }
      }
    }
#endif
  } else { // 全量
    // scan_task -> group_write_task -> merge_tasks -> [parent_task::children]
    ObGroupWriteMacroBlockTask *group_write_task = nullptr;
    ObITask *data_merge_task = nullptr;
    ObITask *lob_merge_task = nullptr;
    // group_write_task
    if (OB_FAIL(alloc_task(group_write_task))) {
      LOG_WARN("fail to alloc group write task", K(ret));
    } else if (OB_FAIL(group_write_task->init(this, tablet_id))) {
      LOG_WARN("fail to init group write task", K(ret));
    } else if (OB_FAIL(write_macro_block_tasks.push_back(group_write_task))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    // merge_task
    else if (OB_FAIL(init_tablet_merge_task(tablet_id, true/*for_major*/, data_merge_task, lob_merge_task))) {
      LOG_WARN("fail to init tablet merge task", KR(ret));
    } else if (OB_FAIL(write_macro_block_tasks.push_back(data_merge_task))) {
      LOG_WARN("fail to push back", KR(ret));
    } else if (nullptr != lob_merge_task &&
               OB_FAIL(write_macro_block_tasks.push_back(lob_merge_task))) {
      LOG_WARN("fail to push back", KR(ret));
    }
    // 依赖关系
    else if (OB_FAIL(scan_task->add_child(*group_write_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (OB_FAIL(group_write_task->add_child(*data_merge_task))) {
      LOG_WARN("fail to add child", KR(ret));
    } else if (nullptr != parent_task && OB_FAIL(parent_task->copy_children_to(*data_merge_task))) {
      LOG_WARN("fail to copy children", KR(ret));
      LOG_WARN("fail to add child", KR(ret));
    } else if (nullptr != lob_merge_task) {
      if (OB_FAIL(group_write_task->add_child(*lob_merge_task))) {
        LOG_WARN("fail to add child", KR(ret));
      } else if (nullptr != parent_task && OB_FAIL(parent_task->copy_children_to(*lob_merge_task))) {
        LOG_WARN("fail to copy children", KR(ret));
      }
    }
  }
  return ret;
}

int ObDDLIndependentDag::init_tablet_merge_task(
    const ObTabletID &tablet_id,
    const bool for_major,
    ObITask *&data_task,
    ObITask *&lob_task)
{
  int ret = OB_SUCCESS;
  data_task = nullptr;
  lob_task = nullptr;

  share::SCN mock_start_scn;
  ObDDLTabletContext *tablet_context = nullptr;
  ObDDLTabletMergeDagParamV2 merge_param;
  ObDDLMergePrepareTask *ddl_merge_task = nullptr;
  if (OB_FAIL(mock_start_scn.convert_for_tx(SS_DDL_START_SCN_VAL))) {
    LOG_WARN("failed to convert for tx", K(ret));
  } else if (OB_FAIL(get_tablet_context(tablet_id, tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(merge_param.init(for_major  /*for major*/,
                                      false /* for lob*/,
                                      false /* for replay*/,
                                      mock_start_scn,
                                      direct_load_type_,
                                      ddl_task_param_,
                                      tablet_context,
                                      tx_info_.trans_id_,
                                      transaction::ObTxSEQ::cast_from_int(tx_info_.seq_no_)))) {
    LOG_WARN("failed to init  ddl merge task param", K(ret));
  } else if (!for_major && FALSE_IT(merge_param.set_merge_all_slice())) {
  } else if (OB_FAIL(alloc_task(ddl_merge_task))) {
    LOG_WARN("failed to alloc ddl merge task", K(ret));
  } else if (OB_FAIL(ddl_merge_task->init(merge_param))) {
    LOG_WARN("failed to init ddl merge task", K(ret));
  } else {
    data_task = ddl_merge_task;
  }

  /* create merge task for lob tablet*/
  ObDDLTabletMergeDagParamV2 lob_merge_param;
  ObDDLMergePrepareTask *lob_merge_task = nullptr;
  if (OB_FAIL(ret)) {
  } else if (tablet_context->lob_meta_tablet_id_.is_valid()) {
    if (OB_FAIL(lob_merge_param.init(for_major  /*for major*/,
                                      true /* for lob*/,
                                      false /* for replay*/,
                                      mock_start_scn,
                                      direct_load_type_,
                                      ddl_task_param_,
                                      tablet_context,
                                      tx_info_.trans_id_,
                                      transaction::ObTxSEQ::cast_from_int(tx_info_.seq_no_)))) {
      LOG_WARN("failed to init  ddl merge task param", K(ret));
    } else if (!for_major && FALSE_IT(lob_merge_param.set_merge_all_slice())) {
    } else if (OB_FAIL(alloc_task(lob_merge_task))) {
      LOG_WARN("failed to create ddl merge taks ", K(ret));
    } else if (OB_FAIL(lob_merge_task->init(lob_merge_param))) {
      LOG_WARN("failed to init task", K(ret));
    } else {
      lob_task = lob_merge_task;
    }
  }
  return ret;
}

int ObDDLIndependentDag::init_merge_tasks(bool for_major, ObArray<ObITask*> &data_merge_tasks, ObArray<ObITask*> &lob_merge_tasks)
{
  int ret = OB_SUCCESS;
  data_merge_tasks.reset();
  lob_merge_tasks.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < ls_tablet_ids_.count(); ++i) {
    const ObTabletID &tablet_id = ls_tablet_ids_.at(i).second;
    ObITask *data_merge_task = nullptr;
    ObITask *lob_merge_task = nullptr;
    if (OB_FAIL(init_tablet_merge_task(tablet_id, for_major, data_merge_task, lob_merge_task))) {
      LOG_WARN("fail to init tablet merge task", KR(ret));
    } else if (OB_ISNULL(data_merge_task)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected data merge task is null", KR(ret));
    } else if (OB_FAIL(data_merge_tasks.push_back(data_merge_task))) {
      LOG_WARN("failed to push back merge task", K(ret));
    } else if (nullptr != lob_merge_task && OB_FAIL(lob_merge_tasks.push_back(lob_merge_task))) {
      LOG_WARN("failed to push back merge task", K(ret));
    }
  }
  return ret;
}

int ObDDLIndependentDag::finish_chunk(ObChunk *&chunk)
{
  int ret = OB_SUCCESS;
  if (nullptr != chunk) {
    chunk->~ObChunk();
    ob_free(chunk);
    chunk = nullptr;
  }
  return ret;
}
