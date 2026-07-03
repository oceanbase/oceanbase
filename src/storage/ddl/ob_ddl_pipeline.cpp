/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_ddl_pipeline.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/ddl/ob_cg_macro_block_write_task.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"
#include "share/vector_index/ob_ai_access_service.h"
#include "share/ai_service/ob_ai_batch_file_manager.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "lib/random/ob_random.h"
#include "lib/json_type/ob_json_base.h"
#include "lib/json_type/ob_json_parse.h"
#include "lib/charset/ob_charset.h"

using namespace oceanbase::storage;
using namespace oceanbase::common;
using namespace oceanbase::share;

int ObIDDLPipeline::init(
    const ObTabletID &tablet_id,
    const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(slice_idx));
  } else {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
  }
  return ret;
}

int ObIDDLPipeline::process()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(preprocess())) {
    LOG_WARN("preprocess failed", K(ret));
  } else {
    static const int64_t timeout_us = 1000L; // 1ms
    ObChunk *chunk = nullptr;
    while (OB_SUCC(ret)) {
      if (OB_UNLIKELY(dag_->is_final_status())) {
        ret = dag_->get_dag_ret();
        FLOG_INFO("dag is stoped", K(ret));
        break;
      } else if (OB_FAIL(get_next_chunk(chunk))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_DAG_TASK_IS_SUSPENDED;
          break;
        }
      } else if (OB_ISNULL(chunk)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("chunk is null", K(ret), KP(chunk));
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_FAIL(push(*chunk))) {
          LOG_WARN("excute chunk failed", K(ret), KPC(chunk));
        } else if (chunk->is_end_chunk()) {
          ret = OB_ITER_END;
        }
        // ignore ret, always finish chunk
        if (OB_TMP_FAIL(finish_chunk(chunk))) {
          LOG_WARN("finish chunk failed", K(tmp_ret), KPC(chunk));
        }
      }
    }
  }
  postprocess(ret);
  return ret;
}

ObVectorIndexTabletContext::ObVectorIndexTabletContext()
    : row_cnt_(0), vec_dim_(0), tenant_id_(MTL_ID()), ls_id_(), tablet_id_(), vec_idx_param_(), ctx_(),
      vector_vid_col_idx_(-1), vector_col_idx_(-1), vector_key_col_idx_(-1), vector_data_col_idx_(-1), center_id_col_idx_(-1), center_vector_col_idx_(-1),
      meta_id_col_idx_(-1), meta_vector_col_idx_(-1), pq_center_id_col_idx_(-1), pq_center_vector_col_idx_(-1), extra_column_idx_types_(),
      lob_inrow_threshold_(0), rowkey_cnt_(0), column_cnt_(0), snapshot_version_(0), index_type_(share::VIAT_MAX), helper_(nullptr),
      allocator_("VecIndexCtx", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      memory_context_(MTL(ObPluginVectorIndexService *)->get_memory_context()),
      all_vsag_use_mem_(MTL(ObPluginVectorIndexService *)->get_all_vsag_use_mem()),
      table_id_(0)
{

}

int ObVectorIndexTabletContext::init(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObIndexType &index_type,
    const int64_t snapshot_version,
    const int64_t ddl_task_id,
    const ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !tablet_id.is_valid() || snapshot_version <= 0 || ddl_task_id <=0 || !(ddl_table_schema.table_item_.vec_dim_ > 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(ls_id), K(tablet_id), K(ddl_table_schema), K(snapshot_version));
  } else {
    row_cnt_ = 0;
    vec_dim_ = ddl_table_schema.table_item_.vec_dim_;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    vec_idx_param_ = ddl_table_schema.table_item_.vec_idx_param_;
    ctx_.ls_id_ = ls_id_;
    lob_inrow_threshold_ = ddl_table_schema.table_item_.lob_inrow_threshold_;
    rowkey_cnt_ = ddl_table_schema.table_item_.rowkey_column_num_;
    column_cnt_ = ddl_table_schema.column_items_.count();
    snapshot_version_ = snapshot_version;
    ddl_task_id_ = ddl_task_id;
    table_id_ = ddl_table_schema.table_id_;

    if (schema::is_vec_index_snapshot_data_type(index_type)) {
      if (OB_FAIL(init_hnsw_index(ddl_table_schema))) {
        LOG_WARN("init hnsw index failed", K(ret));
      }
    } else if (schema::is_local_vec_ivf_centroid_index(index_type)) {
      if (OB_FAIL(init_ivf_center_index(ddl_table_schema))) {
        LOG_WARN("init ivf center index failed", K(ret));
      }
    } else if (schema::is_vec_ivfsq8_meta_index(index_type)) {
      if (OB_FAIL(init_ivf_sq8_meta_index(ddl_table_schema))) {
        LOG_WARN("init ivf sq8 meta index failed", K(ret));
      }
    } else if (schema::is_vec_ivfpq_pq_centroid_index(index_type)) {
      if (OB_FAIL(init_ivf_pq_center_index(ddl_table_schema))) {
        LOG_WARN("init ivf pq center index", K(ret));
      }
    } else if (schema::is_hybrid_vec_index_embedded_type(index_type)) {
      if (OB_FAIL(init_hnsw_embedding_index(ddl_table_schema))) {
        LOG_WARN("init hnsw embedding index failed", K(ret));
      }
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected index type", K(ret), K(index_type));
    }
  }
  return ret;
}

int ObVectorIndexTabletContext::init_hnsw_index(const ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  // get data tablet id and lob tablet id
  ObLSHandle ls_handle;
  ObTabletHandle five_tablet_handle;
  ObTabletHandle data_tablet_handle;
  ObTabletBindingMdsUserData ddl_data;
  const ObIArray<ObColumnSchemaItem> &col_array = ddl_table_schema.column_items_;
  const ObIArray<ObColDesc> &col_desc_array = ddl_table_schema.column_descs_;
  index_type_ = VIAT_MAX;
  vector_visible_col_idx_ = -1;
  vector_key_col_idx_ = -1;
  vector_vid_col_idx_ = -1;
  vector_col_idx_ = -1;
  vector_data_col_idx_ = -1;
  int64_t pk_increment_col_idx = -1;

  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id_));
  } else if (OB_ISNULL(ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(tablet_id_, five_tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id_));
  } else if (FALSE_IT(ctx_.data_tablet_id_ = five_tablet_handle.get_obj()->get_data_tablet_id())) {
  } else if (OB_FAIL(ls_handle.get_ls()->get_tablet(ctx_.data_tablet_id_, data_tablet_handle))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(ctx_.data_tablet_id_));
  } else if (OB_FAIL(data_tablet_handle.get_obj()->get_ddl_data(ddl_data))) {
    LOG_WARN("failed to get ddl data from tablet", K(ret), K(data_tablet_handle));
  } else {
    ctx_.snap_tablet_id_ = tablet_id_;
    ctx_.lob_meta_tablet_id_ = ddl_data.lob_meta_tablet_id_;
    ctx_.lob_piece_tablet_id_ = ddl_data.lob_piece_tablet_id_;
  }
  // get vid col and vector col
  for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
    // version control col is not valid
    if (!col_array.at(i).is_valid_) {
    } else if (ObSchemaUtils::is_vec_hnsw_vid_column(col_array.at(i).column_flags_)) {
      vector_vid_col_idx_ = i;
    } else if (col_desc_array.at(i).col_id_ == OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
      pk_increment_col_idx = i;
    } else if (ObSchemaUtils::is_vec_hnsw_vector_column(col_array.at(i).column_flags_)) {
      vector_col_idx_ = i;
    } else if (ObSchemaUtils::is_vec_hnsw_key_column(col_array.at(i).column_flags_)) {
      vector_key_col_idx_ = i;
    } else if (ObSchemaUtils::is_vec_hnsw_data_column(col_array.at(i).column_flags_)) {
      vector_data_col_idx_ = i;
    } else if (ObSchemaUtils::is_vec_hnsw_visible_column(col_array.at(i).column_flags_)) {
      vector_visible_col_idx_ = i;
    } else if (OB_FAIL(extra_column_idx_types_.push_back(ObExtraInfoIdxType(i, col_array.at(i).col_type_)))) {
      LOG_WARN("failed to push back extra info col idx", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (vector_vid_col_idx_ == -1 && pk_increment_col_idx == -1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_vid_col_idx_), K(pk_increment_col_idx), K(col_array));
  } else if (vector_vid_col_idx_ == -1 && pk_increment_col_idx != -1) {
    vector_vid_col_idx_ = pk_increment_col_idx;
  } else if (vector_vid_col_idx_ != -1 && pk_increment_col_idx != -1) {
    if (OB_FAIL(extra_column_idx_types_.push_back(ObExtraInfoIdxType(pk_increment_col_idx, col_array.at(pk_increment_col_idx).col_type_)))) {
      LOG_WARN("failed to push back extra info col idx", K(ret), K(pk_increment_col_idx));
    }
  }

  if (OB_SUCC(ret)) {
    if (vector_vid_col_idx_ == -1 || vector_col_idx_ == -1 || vector_key_col_idx_ == -1 || vector_data_col_idx_ == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_col_idx_), K(vector_vid_col_idx_),
               K(vector_key_col_idx_), K(vector_data_col_idx_), K(col_array));
    }
  }
  if (OB_SUCC(ret)) {
    is_vec_tablet_rebuild_ = ddl_table_schema.table_item_.is_vec_tablet_rebuild_;
    if (is_vec_tablet_rebuild_) { // async task need
      ObVectorIndexTmpInfo *tmp_info = nullptr;
      if (OB_FAIL(MTL(ObPluginVectorIndexService *)->get_vector_index_tmp_info(ddl_task_id_, tmp_info))) {
        LOG_WARN("fail to get vector index tmp info", K(ret), K(tablet_id_));
      } else if (OB_FAIL(adapter_guard_.set_adapter(tmp_info->adapter_))) {
        LOG_WARN("fail to set new adapter guard", K(ret));
      }
      LOG_INFO("init_hnsw_index", KPC(this), K(is_vec_tablet_rebuild_), K(ddl_table_schema));
    }
  }
  return ret;
}

int ObVectorIndexTabletContext::init_ivf_center_index(const ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  index_type_ = VIAT_MAX;
  const ObIArray<ObColumnSchemaItem> &col_array = ddl_table_schema.column_items_;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
    if (ObSchemaUtils::is_vec_ivf_center_id_column(col_array.at(i).column_flags_)) {
      center_id_col_idx_ = i;
    } else if (ObSchemaUtils::is_vec_ivf_center_vector_column(col_array.at(i).column_flags_)) {
      center_vector_col_idx_ = i;
    }
  }
  if (OB_SUCC(ret)) {
    ObIvfFlatBuildHelper *helper = nullptr;
    if (center_id_col_idx_ == -1 || center_vector_col_idx_ == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(center_id_col_idx_), K(center_vector_col_idx_), K(col_array));
    } else if (OB_FAIL(create_ivf_build_helper(ObIndexType::INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL, vec_idx_param_))) {
      LOG_WARN("create ivf build helper failed", K(ret));
    } else {
      helper = static_cast<ObIvfFlatBuildHelper *>(helper_);
      if (OB_FAIL(helper->init_ctx(vec_dim_))) {
        LOG_WARN("init kmeans ctx failed", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexTabletContext::init_ivf_sq8_meta_index(const ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  index_type_ = VIAT_MAX;
  const ObIArray<ObColumnSchemaItem> &col_array = ddl_table_schema.column_items_;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
    if (ObSchemaUtils::is_vec_ivf_meta_id_column(col_array.at(i).column_flags_)) {
      meta_id_col_idx_ = i;
    } else if (ObSchemaUtils::is_vec_ivf_meta_vector_column(col_array.at(i).column_flags_)) {
      meta_vector_col_idx_ = i;
    }
  }
  if (OB_SUCC(ret)) {
    ObIvfSq8BuildHelper *helper = nullptr;
    if (OB_FAIL(create_ivf_build_helper(ObIndexType::INDEX_TYPE_VEC_IVFSQ8_META_LOCAL, vec_idx_param_))) {
      LOG_WARN("create ivf build helper", K(ret));
    } else {
      helper = static_cast<ObIvfSq8BuildHelper *>(helper_);
      if (OB_FAIL(helper->init_ctx(vec_dim_))) {
        LOG_WARN("init result vectors failed", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexTabletContext::init_ivf_pq_center_index(const ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  index_type_ = VIAT_MAX;
  const ObIArray<ObColumnSchemaItem> &col_array = ddl_table_schema.column_items_;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
    if (ObSchemaUtils::is_vec_ivf_pq_center_id_column(col_array.at(i).column_flags_)) {
      pq_center_id_col_idx_ = i;
    } else if (ObSchemaUtils::is_vec_ivf_center_vector_column(col_array.at(i).column_flags_)) {
      pq_center_vector_col_idx_ = i;
    }
  }
  if (OB_SUCC(ret)) {
    ObIvfPqBuildHelper *helper = nullptr;
    if (pq_center_id_col_idx_ == -1 || pq_center_vector_col_idx_ == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid vector index col idx", K(ret), K(pq_center_id_col_idx_), K(pq_center_vector_col_idx_), K(col_array));
    } else if (OB_FAIL(create_ivf_build_helper(ObIndexType::INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL, vec_idx_param_))) {
      LOG_WARN("create ivf build helper failed", K(ret));
    } else {
      helper = static_cast<ObIvfPqBuildHelper *>(helper_);
      if (OB_FAIL(helper->init_ctx(vec_dim_))) {
        LOG_WARN("failed to init kmeans ctx", K(ret));
      }
    }
  }
  return ret;
}

int ObVectorIndexTabletContext::init_hnsw_embedding_index(const ObDDLTableSchema &ddl_table_schema)
{
  int ret = OB_SUCCESS;
  const ObIArray<ObColumnSchemaItem> &col_array = ddl_table_schema.column_items_;
  const ObIArray<ObColDesc> &col_desc_array = ddl_table_schema.column_descs_;
  index_type_ = VIAT_MAX;
  vector_chunk_col_idx_ = -1;
  extra_column_idx_types_.reset();

  for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
    if (!col_array.at(i).is_valid_) {
      // skip invalid column
    } else if (ObSchemaUtils::is_vec_hnsw_vector_column(col_array.at(i).column_flags_)) {
      vector_chunk_col_idx_ = static_cast<int32_t>(i);
    } else if (OB_FAIL(extra_column_idx_types_.push_back(ObExtraInfoIdxType(i, col_array.at(i).col_type_)))) {
      LOG_WARN("failed to push back extra info col idx", K(ret), K(i));
    }
  }

  if (OB_SUCC(ret)) {
    if (vector_chunk_col_idx_ == -1) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get valid embedding index col idx", K(ret), K(vector_chunk_col_idx_), K(col_array));
    }
  }
  return ret;
}


int ObVectorIndexTabletContext::create_ivf_build_helper(
    const ObIndexType type,
    ObString &vec_index_param)
{
  int ret = OB_SUCCESS;
  ObIvfBuildHelper *tmp_ivf_build_helper = nullptr;
  void *helper_buff = nullptr;
  if (INDEX_TYPE_VEC_IVFFLAT_CENTROID_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator_.alloc(sizeof(ObIvfFlatBuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfFlatBuildHelper(&allocator_, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param, memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init ivf build helper", K(ret));
      }
    }
  } else if (INDEX_TYPE_VEC_IVFSQ8_META_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator_.alloc(sizeof(ObIvfSq8BuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfSq8BuildHelper(&allocator_, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param, memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init ivf build helper", K(ret), K(vec_index_param));
      }
    }
  } else if (INDEX_TYPE_VEC_IVFPQ_PQ_CENTROID_LOCAL == type) {
    if (OB_ISNULL(helper_buff = allocator_.alloc(sizeof(ObIvfPqBuildHelper)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate memory for ivf index build helper", KR(ret));
    } else {
      tmp_ivf_build_helper = new(helper_buff)ObIvfPqBuildHelper(&allocator_, tenant_id_);
      if (OB_FAIL(tmp_ivf_build_helper->init(vec_index_param, memory_context_, all_vsag_use_mem_))) {
        LOG_WARN("failed to init ivf build helper", K(ret), K(vec_index_param));
      }
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported index type", K(ret), K(type));
  }

  if (OB_SUCC(ret)) {
    helper_ = tmp_ivf_build_helper;
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_ivf_build_helper)) {
    tmp_ivf_build_helper->~ObIvfBuildHelper();
    allocator_.free(helper_buff);
    tmp_ivf_build_helper = nullptr;
    helper_buff = nullptr;
  }
  return ret;
}

int ObVectorIndexTabletContext::build_extra_column_idxs(const int32_t chunk_col_idx,
                                                        common::ObSEArray<int32_t, 4> &extra_column_idxs) const
{
  int ret = OB_SUCCESS;
  extra_column_idxs.reset();
  if (OB_FAIL(extra_column_idxs.reserve(extra_column_idx_types_.count()))) {
    LOG_WARN("reserve extra idxs failed", K(ret), K(extra_column_idx_types_.count()));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idx_types_.count(); ++i) {
      const int32_t idx = extra_column_idx_types_.at(i).idx_;
      if (idx != chunk_col_idx) {
        if (OB_FAIL(extra_column_idxs.push_back(idx))) {
          LOG_WARN("push extra idx failed", K(ret), K(idx));
        }
      }
    }
  }
  return ret;
}

void ObVectorIndexTabletContext::destroy_ivf_build_helper()
{
  int ret = OB_SUCCESS;
  if (nullptr != helper_) {
    ObIAllocator *allocator = helper_->get_allocator();
    if (OB_ISNULL(allocator)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null allocator", K(ret));
    } else {
      helper_->~ObIvfBuildHelper();
      allocator->free(helper_);
    }
    helper_ = nullptr;
  }
}

int ObHNSWIndexRowIterator::init(
    ObVectorIndexTabletContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    rowkey_cnt_ = context.rowkey_cnt_;
    column_cnt_ = context.column_cnt_;
    snapshot_version_ = context.snapshot_version_;
    index_type_ = context.index_type_;
    row_cnt_ = context.row_cnt_;
    vec_dim_ = context.vec_dim_;
    tablet_id_ = context.tablet_id_;
    vec_idx_param_ = context.vec_idx_param_;
    ctx_ = &context.ctx_;
    cur_row_pos_ = 0;
    vector_vid_col_idx_ = context.vector_vid_col_idx_;
    vector_col_idx_ = context.vector_col_idx_;
    vector_key_col_idx_ = context.vector_key_col_idx_;
    vector_data_col_idx_ = context.vector_data_col_idx_;
    vector_visible_col_idx_ = context.vector_visible_col_idx_;
    is_vec_tablet_rebuild_ = context.is_vec_tablet_rebuild_;
    if (OB_FAIL(extra_column_idx_types_.assign(context.extra_column_idx_types_))) {
      LOG_WARN("assign extra column idx types failed", K(ret));
    } else {
      is_inited_ = true;
    }
  }
  return ret;
}

bool ObHNSWIndexRowIterator::is_vec_idx_col_invalid(const int64_t column_cnt) const
{
  return vector_key_col_idx_ < 0 || vector_key_col_idx_ >= column_cnt ||
    vector_data_col_idx_ < 0 || vector_data_col_idx_ >= column_cnt ||
    vector_vid_col_idx_ < 0 || vector_vid_col_idx_ >= column_cnt ||
    vector_col_idx_ < 0 || vector_col_idx_ >= column_cnt;
}

int ObHNSWIndexRowIterator::get_next_row(
    blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t request_cnt = column_cnt_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(iter_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= ctx_->vals_.count()) {
    ret = OB_ITER_END;
  } else if (index_type_ >= VIAT_MAX) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get index type invalid.", K(ret), K(index_type_));
  } else if (is_vec_idx_col_invalid(current_row_.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, vec col idx error", K(ret), K(vector_key_col_idx_), K(vector_data_col_idx_),
             K(vector_vid_col_idx_), K(vector_col_idx_));
  } else {
    // set vec key
    ObString key;
    ObString data;
    row_allocator_.reuse();
    if (OB_FAIL(ctx_->get_key_and_data(index_type_, tablet_id_, snapshot_version_, cur_row_pos_, row_allocator_, key, data))) {
      LOG_WARN("fail to build vec snapshot key str", K(ret), K_(index_type));
    } else {
      current_row_.storage_datums_[vector_key_col_idx_].set_string(key);
    }
    // set vec data
    if (OB_FAIL(ret)) {
    } else {
      // TODO @lhd maybe we should do deep copy
      current_row_.storage_datums_[vector_data_col_idx_].set_string(data);
    }

    // set vid and vec to null
    if (OB_SUCC(ret) && (vector_visible_col_idx_ >= 0 && vector_visible_col_idx_ < current_row_.get_column_count())) {
      if (is_vec_tablet_rebuild_) {
        current_row_.storage_datums_[vector_visible_col_idx_].set_false();
      } else {
        current_row_.storage_datums_[vector_visible_col_idx_].set_true();
      }
    }

    if (OB_SUCC(ret)) {
      current_row_.storage_datums_[vector_vid_col_idx_].set_null();
      current_row_.storage_datums_[vector_col_idx_].set_null();
      // set extra_info to null
      if (extra_column_idx_types_.count() > 0) {
        for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idx_types_.count(); i++) {
          current_row_.storage_datums_[extra_column_idx_types_[i].idx_].set_null();
        }
      }
    }
    if (OB_SUCC(ret)) {
      // add extra rowkey
      // TODO how to get snapshot
      current_row_.storage_datums_[rowkey_cnt_].set_int(-snapshot_version_);
      current_row_.storage_datums_[rowkey_cnt_ + 1].set_int(0);
      current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row = &current_row_;
      cur_row_pos_++;
    }
  }
  return ret;
}

int ObIVFCenterRowIterator::init(
    ObVectorIndexTabletContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    rowkey_cnt_ = context.rowkey_cnt_;
    column_cnt_ = context.column_cnt_;
    snapshot_version_ = context.snapshot_version_;
    index_type_ = context.index_type_;
    center_id_col_idx_ = context.center_id_col_idx_;
    center_vector_col_idx_ = context.center_vector_col_idx_;
    tablet_id_ = context.tablet_id_;
    lob_inrow_threshold_ = context.lob_inrow_threshold_;
    helper_ = static_cast<ObIvfFlatBuildHelper *>(context.helper_);
    vec_dim_ = context.vec_dim_;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFCenterRowIterator::get_next_row(
    blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  ObSingleKmeansExecutor *executor = nullptr;
  const int64_t extra_rowkey_cnt = storage::ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
  const int64_t request_cnt = column_cnt_;
  ObIvfFlatBuildHelper *helper = helper_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctx", K(ret));
  } else if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(iter_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= executor->get_centers_count()) {
    ret = OB_ITER_END;
  } else if (center_id_col_idx_ < 0 || center_id_col_idx_ >= current_row_.get_column_count() ||
             center_vector_col_idx_ < 0 || center_vector_col_idx_ >= current_row_.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, center col idx error", K(ret), K(center_id_col_idx_), K(center_vector_col_idx_));
  } else {
    ObString data_str;
    ObString vec_res;
    float *center_vector = nullptr;
    int64_t dim = executor->get_centers_dim();
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = nullptr;
    row_allocator_.reuse();
    if (OB_FAIL(executor->get_center(cur_row_pos_, center_vector))) {
      LOG_WARN("upexpected nullptr center_vector", K(ret), K(cur_row_pos_));
    } else {
      data_str.assign(reinterpret_cast<char *>(center_vector), static_cast<int64_t>(sizeof(float) * dim));
      if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(nullptr, data_str.length(), row_allocator_, vec_res, data_str.ptr()))) {
        LOG_WARN("failed to set array res", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(row_allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc cid", K(ret));
      } else {
        ObString cid_str(buf_len, 0, buf);
        ObCenterId center_id(tablet_id_.id(), cur_row_pos_ + 1);
        if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, cid_str))) {
          LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(cid_str));
        } else if (vec_res.length() > lob_inrow_threshold_ || cid_str.length() > lob_inrow_threshold_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected outrow datum in ivf vector index",
                    K(ret), K(vec_res.length()), K(cid_str.length()), K(lob_inrow_threshold_));
        } else {
          for (int64_t i = 0; i < current_row_.get_column_count(); ++i) {
            if (center_vector_col_idx_ == i) {
              current_row_.storage_datums_[center_vector_col_idx_].set_string(vec_res);
            } else if (center_id_col_idx_ == i) {
              current_row_.storage_datums_[center_id_col_idx_].set_string(cid_str);
            } else if (rowkey_cnt_ == i) {
              current_row_.storage_datums_[i].set_int(-snapshot_version_);
            } else if (rowkey_cnt_ + 1 == i) {
              current_row_.storage_datums_[i].set_int(0);
            } else {
              current_row_.storage_datums_[i].set_null(); // set part key null
            }
          }
          current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
          datum_row = &current_row_;
          cur_row_pos_++;
        }
      }
    }
  }
  return ret;
}

int ObIVFSq8MetaRowIterator::init(ObVectorIndexTabletContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    rowkey_cnt_ = context.rowkey_cnt_;
    column_cnt_ = context.column_cnt_;
    snapshot_version_ = context.snapshot_version_;
    meta_id_col_idx_ = context.meta_id_col_idx_;
    meta_vector_col_idx_ = context.meta_vector_col_idx_;
    tablet_id_ = context.tablet_id_;
    vec_dim_ = context.vec_dim_;
    lob_inrow_threshold_ = context.lob_inrow_threshold_;
    helper_ = static_cast<ObIvfSq8BuildHelper *>(context.helper_);
    is_inited_ = true;
  }
  return ret;
}

int ObIVFSq8MetaRowIterator::get_next_row(
    blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  const int64_t request_cnt = column_cnt_;
  ObIvfSq8BuildHelper *helper = helper_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(iter_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= ObIvfConstant::SQ8_META_ROW_COUNT) {
    ret = OB_ITER_END;
  } else if (meta_id_col_idx_ < 0 || meta_id_col_idx_ >= current_row_.get_column_count() ||
             meta_vector_col_idx_ < 0 || meta_vector_col_idx_ >= current_row_.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, center col idx error", K(ret), K(meta_id_col_idx_), K(meta_vector_col_idx_));
  } else {
    ObString data_str;
    ObString vec_res;
    float *cur_vector = nullptr;
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = nullptr;
    row_allocator_.reuse();
    if (OB_FAIL(helper->get_result(cur_row_pos_, cur_vector))) {
      LOG_WARN("fail to get result", K(ret));
    } else {
      data_str.assign(reinterpret_cast<char *>(cur_vector), static_cast<int64_t>(sizeof(float) * vec_dim_));
      if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(nullptr, data_str.length(), row_allocator_, vec_res, data_str.ptr()))) {
        LOG_WARN("failed to set array res", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(row_allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc cid", K(ret));
      } else {
        ObString cid_str(buf_len, 0, buf);
        // reuse center_id encode, min: 1, max: 2, step: 3
        ObCenterId center_id(tablet_id_.id(), cur_row_pos_ + 1);
        if (OB_FAIL(ObVectorClusterHelper::set_center_id_to_string(center_id, cid_str))) {
          LOG_WARN("failed to set center_id to string", K(ret), K(center_id), K(cid_str));
        } else if (vec_res.length() > lob_inrow_threshold_ || cid_str.length() > lob_inrow_threshold_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected outrow datum in ivf vector index",
                    K(ret), K(vec_res.length()), K(cid_str.length()), K(lob_inrow_threshold_));
        } else {
          for (int64_t i = 0; i < current_row_.get_column_count(); ++i) {
            if (meta_vector_col_idx_ == i) {
              current_row_.storage_datums_[meta_vector_col_idx_].set_string(vec_res);
            } else if (meta_id_col_idx_ == i) {
              current_row_.storage_datums_[meta_id_col_idx_].set_string(cid_str);
            } else if (rowkey_cnt_ == i) {
              current_row_.storage_datums_[i].set_int(-snapshot_version_);
            } else if (rowkey_cnt_ + 1 == i) {
              current_row_.storage_datums_[i].set_int(0);
            } else {
              current_row_.storage_datums_[i].set_null(); // set part key null
            }
          }
          current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
          datum_row = &current_row_;
          cur_row_pos_++;
        }
      }
    }
  }
  return ret;
}

int ObIVFPqRowIterator::init(
    ObVectorIndexTabletContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    rowkey_cnt_ = context.rowkey_cnt_;
    column_cnt_ = context.column_cnt_;
    snapshot_version_ = context.snapshot_version_;
    pq_center_vector_col_idx_ = context.pq_center_vector_col_idx_;
    pq_center_id_col_idx_ = context.pq_center_id_col_idx_;
    vec_dim_ = context.vec_dim_;
    helper_ = static_cast<ObIvfPqBuildHelper *>(context.helper_);
    tablet_id_ = context.tablet_id_;
    lob_inrow_threshold_ = context.lob_inrow_threshold_;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFPqRowIterator::get_next_row(
    blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  ObMultiKmeansExecutor *executor = nullptr;
  const int64_t request_cnt = column_cnt_;
  ObIvfPqBuildHelper *helper = helper_;
  if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected nullptr ctx", K(ret));
  } else if (current_row_.get_column_count() <= 0
    && OB_FAIL(current_row_.init(iter_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (cur_row_pos_ >= executor->get_total_centers_count()) {
    ret = OB_ITER_END;
  } else if (pq_center_id_col_idx_ < 0 || pq_center_id_col_idx_ >= current_row_.get_column_count() ||
             pq_center_vector_col_idx_ < 0 || pq_center_vector_col_idx_ >= current_row_.get_column_count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, center col idx error", K(ret), K(pq_center_id_col_idx_), K(pq_center_vector_col_idx_));
  } else {
    ObString data_str;
    ObString vec_res;
    float *center_vector = nullptr;
    int64_t dim = executor->get_centers_dim();
    int64_t buf_len = OB_DOC_ID_COLUMN_BYTE_LENGTH;
    char *buf = nullptr;
    int64_t center_count_per_kmeans = executor->get_centers_count_per_kmeans();
    row_allocator_.reuse();
    if (center_count_per_kmeans == 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("upexpected zero center count", K(ret), K(center_count_per_kmeans));
    } else if (OB_FAIL(executor->get_center(cur_row_pos_, center_vector))) {
      LOG_WARN("upexpected nullptr center_vector", K(ret), K(cur_row_pos_), K(center_count_per_kmeans));
    } else {
      data_str.assign(reinterpret_cast<char *>(center_vector), static_cast<int64_t>(sizeof(float) * dim));
      if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(nullptr, data_str.length(), row_allocator_, vec_res, data_str.ptr()))) {
        LOG_WARN("failed to set array res", K(ret));
      } else if (OB_ISNULL(buf = static_cast<char*>(row_allocator_.alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc cid", K(ret));
      } else {
        ObString pq_cid_str(buf_len, 0, buf);
        // row_i = pq_centers[m_id - 1][center_id - 1] since m_id and center_id start from 1
        ObPqCenterId pq_center_id(tablet_id_.id(), cur_row_pos_ / center_count_per_kmeans + 1, cur_row_pos_ % center_count_per_kmeans + 1);
        if (OB_FAIL(ObVectorClusterHelper::set_pq_center_id_to_string(pq_center_id, pq_cid_str))) {
          LOG_WARN("failed to set center_id to string", K(ret), K(pq_center_id), K(pq_cid_str));
        } else if (vec_res.length() > lob_inrow_threshold_ || pq_cid_str.length() > lob_inrow_threshold_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected outrow datum in ivf vector index",
                    K(ret), K(vec_res.length()), K(pq_cid_str.length()), K(lob_inrow_threshold_));
        } else {
          for (int64_t i = 0; i < current_row_.get_column_count(); ++i) {
            if (pq_center_vector_col_idx_ == i) {
              current_row_.storage_datums_[i].set_string(vec_res);
            } else if (pq_center_id_col_idx_ == i) {
              current_row_.storage_datums_[i].set_string(pq_cid_str);
            } else if (rowkey_cnt_ == i) {
              current_row_.storage_datums_[i].set_int(-snapshot_version_);
            } else if (rowkey_cnt_ + 1 == i) {
              current_row_.storage_datums_[i].set_int(0);
            } else {
              current_row_.storage_datums_[i].set_null(); // set part key null
            }
          }
          current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
          datum_row = &current_row_;
          cur_row_pos_++;
        }
      }
    }
  }
  return ret;
}

ObVectorIndexBaseOperator::ObVectorIndexBaseOperator(ObPipeline *pipeline)
  : ObPipelineOperator(pipeline), is_inited_(false), tablet_id_(), slice_idx_(0),
    op_allocator_("VecIndexOp", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
    row_allocator_("VecIndexRow", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID())
{
}

int ObVectorIndexBaseOperator::init(const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid() || slice_idx < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(slice_idx));
  } else {
    tablet_id_ = tablet_id;
    slice_idx_ = slice_idx;
    is_inited_ = true;
  }
  return ret;
}

bool ObVectorIndexBaseOperator::is_valid() const
{
  return tablet_id_.is_valid();
}

int ObIVFIndexBaseOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  tablet_id_ = tablet_id;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else {
    table_id_ = tablet_context->vector_index_ctx_->table_id_;
    helper_ = tablet_context->vector_index_ctx_->helper_;
    is_inited_ = true;
  }
  return ret;
}

int ObVectorIndexBaseOperator::get_ddl_tablet_context(ObDDLTabletContext *&tablet_context)
{
  int ret = OB_SUCCESS;
  ObDDLIndependentDag *dag = nullptr;
  tablet_context = nullptr;
  if (OB_ISNULL(get_dag())) {
    ret = OB_ERR_SYS;
    LOG_WARN("get dag failed", K(ret));
  } else if (OB_FALSE_IT(dag = static_cast<ObDDLIndependentDag *>(get_dag()))) {
  } else if (OB_FAIL(dag->get_tablet_context(tablet_id_, tablet_context))) {
    LOG_WARN("get tablet context failed", K(ret), K(tablet_id_));
  } else if (OB_ISNULL(tablet_context)) {
    ret = OB_ERR_SYS;
    LOG_WARN("error sys, invalid tablet context", K(ret));
  }
  return ret;
}

int ObHNSWIndexAppendBufferOperator::init(
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  ObVectorIndexTabletContext *vector_index_ctx = nullptr;
  tablet_id_ = tablet_id;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
  } else if (OB_ISNULL(vector_index_ctx = tablet_context->vector_index_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, vector index ctx is null", K(ret));
  } else {
    is_inited_ = true;
    vec_idx_param_ = vector_index_ctx->vec_idx_param_;
    vec_dim_ = vector_index_ctx->vec_dim_;
    vector_vid_col_idx_ = vector_index_ctx->vector_vid_col_idx_;
    vector_col_idx_ = vector_index_ctx->vector_col_idx_;
    vector_key_col_idx_ = vector_index_ctx->vector_key_col_idx_;
    vector_data_col_idx_ = vector_index_ctx->vector_data_col_idx_;
    vector_visible_col_idx_ = vector_index_ctx->vector_visible_col_idx_;
    if (OB_FAIL(extra_column_idx_types_.assign(vector_index_ctx->extra_column_idx_types_))) {
      LOG_WARN("assign extra column idx types failed", K(ret));
    }
  }
  return ret;
}

int ObHNSWIndexAppendBufferOperator::append_row(
    const int64_t row_pos,
    const common::ObIArray<common::ObIVector *> &vectors,
    ObDDLTabletContext *tablet_context)
{
  int ret = OB_SUCCESS;
  // get vid and vector
  ObString vec_str;
  int64_t vec_vid;
  ObVecExtraInfoObj *extra_obj = nullptr;
  int64_t extra_column_count = extra_column_idx_types_.count();
  row_allocator_.reuse();
  if (vectors.count() <= vector_vid_col_idx_ || vectors.count() <= vector_col_idx_ || row_pos < 0) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_col_idx_), K(vector_vid_col_idx_), K(row_pos));
  } else if (vectors.at(vector_col_idx_)->is_null(row_pos)) {
    // do nothing
  } else if (FALSE_IT(vec_vid = vectors.at(vector_vid_col_idx_)->get_int(row_pos))) {
  } else if (FALSE_IT(vec_str = vectors.at(vector_col_idx_)->get_string(row_pos))) {
  } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&row_allocator_,
                                                                ObLongTextType,
                                                                CS_TYPE_BINARY,
                                                                true,
                                                                vec_str))) {
    LOG_WARN("fail to get real data.", K(ret), K(vec_str));
  } else if (vec_str.length() == 0) {
    // do nothing
  } else {
    const bool is_vec_tablet_rebuild = tablet_context->vector_index_ctx_->is_vec_tablet_rebuild_;
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ObPluginVectorIndexAdaptor *adapter = nullptr;
    int64_t extra_info_actual_size = 0;

    if (OB_ISNULL(vec_index_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("error unexpected, vector index service is nullptr", K(ret));
    } else if (!is_vec_tablet_rebuild && OB_FAIL(vec_index_service->acquire_adapter_guard(tablet_context->vector_index_ctx_->ls_id_,
                                                      tablet_id_,
                                                      ObIndexType::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                      adaptor_guard,
                                                      &tablet_context->vector_index_ctx_->vec_idx_param_,
                                                      tablet_context->vector_index_ctx_->vec_dim_))) {
      LOG_WARN("fail to get ObMockPluginVectorIndexAdapter", K(ret), K(tablet_context->vector_index_ctx_->ls_id_), K(tablet_id_));
    } else if (OB_ISNULL(adapter = is_vec_tablet_rebuild ? tablet_context->vector_index_ctx_->adapter_guard_.get_adatper() : adaptor_guard.get_adatper())) {
      LOG_WARN("error unexpected, adapter is nullptr", K(ret), K(tablet_context->vector_index_ctx_->ls_id_), K(tablet_id_));
    } else if (OB_FAIL(adapter->get_extra_info_actual_size(extra_info_actual_size))) {
      LOG_WARN("failed to get extra info actual size", K(ret));
    } else if (extra_column_count > 0 && extra_info_actual_size > 0) { //no primary key /cluster table not support extra info right now
      char *buf = nullptr;
      if (OB_ISNULL(buf = static_cast<char *>(row_allocator_.alloc(sizeof(ObVecExtraInfoObj) * extra_column_count)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory failed", K(ret), K(extra_column_count));
      } else if (OB_FALSE_IT(extra_obj = new (buf) ObVecExtraInfoObj[extra_column_count])) {
      }
      int64_t datum_row_count = vectors.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_count; ++i) {
        if (datum_row_count <= extra_column_idx_types_.at(i).idx_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("failed to get valid extra_info idx", K(ret), K(extra_column_idx_types_.at(i).idx_), K(datum_row_count));
        } else {
          const ObIVector &extra_vector = *vectors.at(extra_column_idx_types_.at(i).idx_);
          if (OB_FAIL(extra_obj[i].from_vector(extra_vector, row_pos, extra_column_idx_types_.at(i).type_, &row_allocator_))) {
            LOG_WARN("failed to from obj.", K(ret), K(extra_column_idx_types_), K(i));
          }
        }
      }
    }
    uint32_t vec_length = vec_str.length();
    if (OB_FAIL(ret)) {
    } else if (OB_ISNULL(adapter)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr", K(ret), KP(adapter), K(tablet_context->vector_index_ctx_));
    } else if (OB_FAIL(adapter->add_snap_index(reinterpret_cast<float *>(vec_str.ptr()),
                                                      &vec_vid, extra_obj, extra_column_count, 1, &vec_length))) {
      LOG_WARN("fail to build index to adaptor", K(ret), KPC(this));
    } else {
      LOG_DEBUG("[vec index debug] add into snap index success", K(tablet_id_), K(vec_vid), K(vec_str));
    }
  }
  return ret;
}

int ObHNSWIndexAppendBufferOperator::append_row_file(ObCGRowFile *row_file, ObDDLTabletContext *tablet_context)
{
  int ret = OB_SUCCESS;
  ObBatchDatumRows *datum_rows = nullptr;
  if (OB_ISNULL(row_file) || OB_ISNULL(tablet_context)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(row_file), KP(tablet_context));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(row_file->get_next_batch(datum_rows))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get next batch failed", K(ret));
      }
    } else {
      const ObArray<common::ObIVector *> &vectors = datum_rows->vectors_;
      const int64_t total_row_count = datum_rows->row_count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < total_row_count; ++i) {
        if (OB_FAIL(append_row(i, vectors, tablet_context))) {
          LOG_WARN("append row failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObHNSWIndexAppendBufferOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (input_chunk.is_end_chunk()) {
    // do nothing
  } else {
    ObDDLIndependentDag *dag = nullptr;
    ObDDLTabletContext *tablet_context = nullptr;
    if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
      LOG_WARN("get ddl tablet context failed", K(ret));
    } else if (OB_ISNULL(tablet_context->vector_index_ctx_)) {
      ret = OB_ERR_SYS;
      LOG_WARN("error sys, invalid vector index ctx", K(ret));
    } else {
      if (OB_UNLIKELY(!input_chunk.is_valid() || !input_chunk.is_cg_row_tmp_files_type())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid arguments", K(ret), K(input_chunk));
      } else {
        ObArray<ObCGRowFile *> *cg_row_file_arr = input_chunk.cg_row_file_arr_;
        for (int64_t i = 0; OB_SUCC(ret) && i < cg_row_file_arr->count(); ++i) {
          ObCGRowFile *&row_file = cg_row_file_arr->at(i);
          if (OB_UNLIKELY(nullptr == row_file)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("error unexpected, cg row file is nullptr", K(ret), K(*cg_row_file_arr));
          } else if (OB_FAIL(append_row_file(row_file, tablet_context))) {
            LOG_WARN("append row file failed", K(ret));
          }
          if (nullptr != row_file) {
            row_file->~ObCGRowFile();
            ob_free(row_file);
            row_file = nullptr;
          }
        }
      }
    }
  }

  return ret;
}

int ObHNSWIndexBuildOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  is_inited_ = true;
  return ret;
}

int ObHNSWIndexBuildOperator::serialize_vector_index(
    ObIAllocator *allocator,
    transaction::ObTxDesc *tx_desc,
    int64_t lob_inrow_threshold,
    ObVectorIndexAlgorithmType &type,
    ObVectorIndexTabletContext &ctx,
    const bool is_vec_tablet_rebuild)
{
  int ret = OB_SUCCESS;
  // first we do vsag serialize
  ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
  ObPluginVectorIndexAdapterGuard adaptor_guard;
  row_allocator_.reuse();
  if (OB_ISNULL(vec_index_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get null ObPluginVectorIndexService ptr", K(ret), K(MTL_ID()));
  } else if (!is_vec_tablet_rebuild &&
             OB_FAIL(vec_index_service->acquire_adapter_guard(ctx.ls_id_,
                                                              tablet_id_,
                                                              ObIndexType::INDEX_TYPE_VEC_INDEX_SNAPSHOT_DATA_LOCAL,
                                                              adaptor_guard,
                                                              &ctx.vec_idx_param_,
                                                              ctx.vec_dim_))) {
    LOG_WARN("fail to get ObMockPluginVectorIndexAdapter", K(ret), K(ctx.ls_id_), K(tablet_id_));
  } else {
    ObHNSWSerializeCallback::CbParam param;
    param.vctx_ = &ctx.ctx_;
    param.allocator_ = allocator;
    param.tmp_allocator_ = &row_allocator_;
    param.lob_inrow_threshold_ = lob_inrow_threshold;
    // build tx
    oceanbase::transaction::ObTransService *txs = MTL(transaction::ObTransService*);
    oceanbase::transaction::ObTxReadSnapshot snapshot;
    int64_t timeout = ObTimeUtility::fast_current_time() + storage::ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
    if (OB_ISNULL(tx_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tx desc, get nullptr", K(ret));
    } else if (OB_FAIL(txs->get_ls_read_snapshot(*tx_desc, transaction::ObTxIsolationLevel::RC, ctx.ls_id_, timeout, snapshot))) {
      LOG_WARN("fail to get snapshot", K(ret));
    } else {
      param.timeout_ = timeout;
      param.snapshot_ = &snapshot;
      param.tx_desc_ = tx_desc;
      param.tablet_id_ = tablet_id_;
      param.snapshot_version_ = ctx.snapshot_version_;
      param.is_vec_tablet_rebuild_ = is_vec_tablet_rebuild;
      ObPluginVectorIndexAdaptor *adp = is_vec_tablet_rebuild ? ctx.adapter_guard_.get_adatper() : adaptor_guard.get_adatper();
      if (OB_ISNULL(adp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), KP(adp), K(is_vec_tablet_rebuild));
      } else if (OB_FAIL(adp->check_snap_index())) {
        LOG_WARN("failed to check snap hnswsq index", K(ret));
      } else if (OB_FAIL(adp->set_snapshot_key_prefix(tablet_id_.id(), ctx.snapshot_version_, ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH))) {
        LOG_WARN("failed to set snapshot key prefix", K(ret), K(tablet_id_.id()), K(ctx.snapshot_version_));
      } else if (OB_FAIL(adp->serialize_snapshot(param))) {
        if (OB_NOT_INIT == ret) {
          // ignore // no data in slice store
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to do vsag serialize", K(ret));
        }
      } else {
        type = adp->get_snap_index_type();
        ctx.index_type_ = type;
        LOG_INFO("HgraphIndex finish vsag serialize for tablet", K(tablet_id_), K(ctx.ctx_.get_vals().count()), K(type), K(tx_desc->get_tx_id()));
      }
      if (OB_SUCC(ret)) {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(adp->get_tenant_id()));
        if (!tenant_config.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail get tenant_config", KR(ret), K(adp->get_tenant_id()));
        } else if (OB_FAIL(adp->renew_single_snap_index((type == VIAT_HNSW_BQ || type == VIAT_IPIVF || type == VIAT_IPIVF_SQ)
            || (tenant_config->vector_index_memory_saving_mode && (type == VIAT_HNSW || type == VIAT_HNSW_SQ || type == VIAT_HGRAPH))))) {
          LOG_WARN("fail to renew single snap index", K(ret));
        }
      }
    }
  }
  row_allocator_.reuse();
  return ret;
}

int ObHNSWIndexBuildOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  int end_trans_ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (input_chunk.is_end_chunk()) {
    // do nothing
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else {
    const bool is_vec_tablet_rebuild = tablet_context->vector_index_ctx_->is_vec_tablet_rebuild_;
    ObVectorIndexAlgorithmType index_type = VIAT_MAX;
    const uint64_t timeout_us = ObTimeUtility::current_time() + storage::ObInsertLobColumnHelper::LOB_TX_TIMEOUT;
    transaction::ObTxDesc *tx_desc = nullptr;
    if (OB_FAIL(ObInsertLobColumnHelper::start_trans(tablet_context->ls_id_, false/*is_for_read*/, timeout_us, tx_desc))) {
      LOG_WARN("fail to get tx_desc", K(ret));
    } else if (OB_FAIL(serialize_vector_index(&op_allocator_, tx_desc, tablet_context->vector_index_ctx_->lob_inrow_threshold_, index_type, *tablet_context->vector_index_ctx_, is_vec_tablet_rebuild))) {
      LOG_WARN("serialize vector index failed", K(ret));
    }
    if (OB_NOT_NULL(tx_desc)) {
      tablet_context->vector_index_ctx_->tx_desc_ = tx_desc;
      if (OB_SUCC(ret) && is_vec_tablet_rebuild) {
        // skip end trans, will end trans in ObHNSWIndexDMLWriteOperator;
        // TODO@xiajin: 调用到 ObHNSWIndexDMLWriteOperator 之前就失败了，没有调用 end trans
        LOG_INFO("async task build will end trans in other operator", K(ret), K(*tablet_context->vector_index_ctx_));
      } else if (OB_SUCCESS != (end_trans_ret = storage::ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, INT64_MAX))) {
        LOG_WARN("fail to end read trans", K(ret), K(end_trans_ret));
        ret = end_trans_ret;
      } else if (OB_SUCC(ret) && OB_FAIL(OB_E(EventTable::EN_TRANS_AFTER_COMMIT) OB_SUCCESS)) {
        LOG_WARN("mock hnsw build fail after end trans", K(is_vec_tablet_rebuild), K_(tablet_id));
      }
    }
    if (OB_SUCC(ret)) {
      output_chunk.type_ = ObChunk::DAG_TABLET_CONTEXT;
      output_chunk.data_ptr_ = input_chunk.data_ptr_;
    }
  }

  return ret;
}

int ObVectorIndexWriteMacroBaseOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  is_inited_ = true;
  return ret;
}

int ObVectorIndexWriteMacroBaseOperator::write(const ObChunk &input_chunk, ObVectorIndexRowIterator &iter)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  if (input_chunk.is_end_chunk()) {
    // do nothing
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else {
    ObTabletSliceWriter *slice_writer = nullptr;
    if (OB_FAIL(iter.init(*tablet_context->vector_index_ctx_))) {
      LOG_WARN("fail to init iterator", K(ret));
    } else {
      blocksstable::ObDatumRow *datum_row = nullptr;
      ObWriteMacroParam write_param;
      ObDDLIndependentDag *ddl_dag = nullptr;
      if (OB_ISNULL(slice_writer = OB_NEWx(ObTabletSliceWriter, &op_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for tablet slice writer failed", K(ret));
      } else if (OB_ISNULL(ddl_dag = static_cast<ObDDLIndependentDag *>(get_dag()))) {
        ret = OB_ERR_SYS;
        LOG_WARN("get dag failed", K(ret));
      } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, slice_idx_, -1/*cg_idx*/, ddl_dag, write_param))) {
        LOG_WARN("fill writer param failed", K(ret));
      } else if (OB_FAIL(slice_writer->init(write_param))) {
        LOG_WARN("init macro block slice store failed", K(ret));
      } else {
        // do write
        while (OB_SUCC(ret)) {
          // build row
          if (OB_FAIL(iter.get_next_row(datum_row))) {
            if (ret != OB_ITER_END) {
              LOG_WARN("fail to get next vector data row", K(ret));
            }
          } else if (OB_FAIL(slice_writer->append_row(*datum_row))) {
            LOG_WARN("fail to append row to macro block slice store", K(ret));
          } else {
            /*if (OB_NOT_NULL(insert_monitor)) {
              insert_monitor->inserted_row_cnt_ =  insert_monitor->inserted_row_cnt_ + 1;
            }*/
          }
        }
        if (ret == OB_ITER_END) {
          ret = OB_SUCCESS;
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(slice_writer->close())) {
            LOG_WARN("fail to close macro_block_slice_store", K(ret));
          }
        }
      }
    }
    if (OB_NOT_NULL(slice_writer)) {
      slice_writer->~ObTabletSliceWriter();
      slice_writer = nullptr;
    }
  }
  return ret;
}

int ObHNSWIndexWriteMacroOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  is_inited_ = true;
  return ret;
}

int ObHNSWIndexWriteMacroOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_FAIL(write(input_chunk, iter_))) {
    LOG_WARN("write macro failed", K(ret));
  }
  return ret;
}

int ObHNSWIndexDMLWriteOperator::dml_write(const ObChunk &input_chunk, ObVectorIndexRowIterator &iter)
{
  int ret = OB_SUCCESS;
  int end_trans_ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  if (input_chunk.is_end_chunk()) {
    // do nothing
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else if (!tablet_context->vector_index_ctx_->is_vec_tablet_rebuild_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected dml write chunk", K(ret), K(tablet_context->vector_index_ctx_));
  } else {
    transaction::ObTxDesc *tx_desc = tablet_context->vector_index_ctx_->tx_desc_;
    ObPluginVectorIndexAdaptor *adapter = tablet_context->vector_index_ctx_->adapter_guard_.get_adatper();

    if (OB_ISNULL(tx_desc)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tx desc", K(ret), K(*tablet_context->vector_index_ctx_));
    } else {
      const int64_t tenant_id = tablet_context->vector_index_ctx_->tenant_id_;
      const ObTabletID tablet_id = tablet_context->vector_index_ctx_->tablet_id_;
      const ObLSID ls_id = tablet_context->vector_index_ctx_->ls_id_;
      const int64_t snapshot_version = tablet_context->vector_index_ctx_->snapshot_version_;
      const int64_t key_col_idx = tablet_context->vector_index_ctx_->vector_key_col_idx_;
      const int64_t data_col_idx = tablet_context->vector_index_ctx_->vector_data_col_idx_;
      const int64_t visible_col_idx = tablet_context->vector_index_ctx_->vector_visible_col_idx_;
      const int64_t ddl_task_id = tablet_context->vector_index_ctx_->ddl_task_id_;

      ObVecIndexAsyncTask async_task_handle(tenant_id, ls_id, adapter);

      if (OB_FAIL(iter.init(*tablet_context->vector_index_ctx_))) {
        LOG_WARN("fail to init iterator", K(ret));
      } else if (OB_FAIL(async_task_handle.execute_write_snap_index(tx_desc, iter, tablet_id, key_col_idx,
                                                                    data_col_idx, visible_col_idx, snapshot_version))) {
        LOG_WARN("fail to execute dml write", K(ret), K(tablet_id));
      } else if (OB_FAIL(ObVecIndexAsyncTaskUtil::set_inner_sql_ret_code(ddl_task_id, ret))) {
        LOG_WARN("fail to set ret code", K(ret), K(ddl_task_id));
      }
      // end trans
      if (OB_SUCCESS != (end_trans_ret = storage::ObInsertLobColumnHelper::end_trans(tx_desc, OB_SUCCESS != ret, INT64_MAX))) {
        LOG_WARN("fail to end read trans", K(ret), K(end_trans_ret));
        ret = end_trans_ret;
      } else {
        LOG_DEBUG("end trans success", K(ret), K(ddl_task_id), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObHNSWIndexDMLWriteOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  is_inited_ = true;
  return ret;
}

int ObHNSWIndexDMLWriteOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_FAIL(dml_write(input_chunk, iter_))) {
    LOG_WARN("write macro failed", K(ret));
  }
  return ret;
}

int ObIVFIndexAppendBufferBaseOperator::append_row_file(ObCGRowFile *row_file)
{
  int ret = OB_SUCCESS;
  ObBatchDatumRows *datum_rows = nullptr;
  if (OB_ISNULL(row_file)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KP(row_file));
  }
  while (OB_SUCC(ret)) {
    if (OB_FAIL(row_file->get_next_batch(datum_rows))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get next batch failed", K(ret));
      }
    } else {
      const ObArray<common::ObIVector *> &vectors = datum_rows->vectors_;
      const int64_t total_row_count = datum_rows->row_count_;
      for (int64_t i = 0; OB_SUCC(ret) && i < total_row_count; ++i) {
        if (OB_FAIL(append_row(i, *vectors.at(vector_col_idx_)))) {
          LOG_WARN("append row failed", K(ret), K(i));
        }
      }
    }
  }
  return ret;
}

int ObIVFIndexAppendBufferBaseOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (input_chunk.is_end_chunk()) {
    // do nothing
  } else if (OB_UNLIKELY(!input_chunk.is_valid() || !input_chunk.is_cg_row_tmp_files_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(input_chunk));
  } else {
    ObArray<ObCGRowFile *> *cg_row_file_arr = input_chunk.cg_row_file_arr_;
    for (int64_t i = 0; OB_SUCC(ret) && i < cg_row_file_arr->count(); ++i) {
      ObCGRowFile *&row_file = cg_row_file_arr->at(i);
      if (OB_UNLIKELY(nullptr == row_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, cg row file is nullptr", K(ret), K(*cg_row_file_arr));
      } else if (OB_FAIL(append_row_file(row_file))) {
        LOG_WARN("append row file failed", K(ret));
      }
      if (nullptr != row_file) {
        row_file->~ObCGRowFile();
        ob_free(row_file);
        row_file = nullptr;
      }
    }
  }
  return ret;
}

int ObIVFCenterAppendBufferOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  ObVectorIndexTabletContext *vector_index_ctx = nullptr;
  tablet_id_ = tablet_id;
  if (OB_FAIL(ObIVFIndexBaseOperator::init(tablet_id))) {
    LOG_WARN("init ivf base operator failed", K(ret));
  } else if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else if (OB_ISNULL(vector_index_ctx = tablet_context->vector_index_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, vector index ctx is null", K(ret));
  } else {
    vector_col_idx_ = vector_index_ctx->center_vector_col_idx_;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFCenterAppendBufferOperator::append_row(
    const int64_t row_pos,
    const ObIVector &vector)
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // get vid and vector
    ObString vec_str;
    ObSingleKmeansExecutor *executor = nullptr;
    ObIvfFlatBuildHelper *helper = nullptr;
    if (vector.is_null(row_pos)) {
      // do nothing // ignore
    } else if (FALSE_IT(vec_str = vector.get_string(row_pos))) {
    } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&row_allocator_,
                                                                  ObLongTextType,
                                                                  CS_TYPE_BINARY,
                                                                  true,
                                                                  vec_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(vec_str), K(vector.get_string(row_pos)), K(row_pos));
    } else if (OB_FAIL(get_spec_ivf_helper<ObIvfFlatBuildHelper>(helper_, helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else if (OB_FAIL(executor->append_sample_vector(reinterpret_cast<float*>(vec_str.ptr())))) {
      LOG_WARN("failed to append sample vector", K(ret));
    } else {
      LOG_DEBUG("[vec index debug] append sample vector", K(tablet_id_), K(vec_str));
    }
  }
  return ret;
}

int ObIVFCenterIndexBuildOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (input_chunk.is_end_chunk()) {
    // do nothing
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("get dag tablet context failed", K(ret));
  } else {
    ObSingleKmeansExecutor *executor = nullptr;
    ObIvfFlatBuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper(helper_, helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else if (OB_FAIL(executor->build(nullptr /* insert monitor */))) {
      LOG_WARN("failed to build clusters", K(ret));
    } else {
      output_chunk = input_chunk;
    }
  }
  return ret;
}

int ObIVFCenterWriteMacroOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_FAIL(write(input_chunk, iter_))) {
    LOG_WARN("write macro failed", K(ret));
  }
  return ret;
}

int ObIVFSq8MetaAppendBufferOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  ObVectorIndexTabletContext *vector_index_ctx = nullptr;
  tablet_id_ = tablet_id;
  if (OB_FAIL(ObIVFIndexBaseOperator::init(tablet_id))) {
    LOG_WARN("init ivf index base operator failed", K(ret));
  } else if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else if (OB_ISNULL(vector_index_ctx = tablet_context->vector_index_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, vector index ctx is null", K(ret));
  } else {
    vector_col_idx_ = vector_index_ctx->meta_vector_col_idx_;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFSq8MetaAppendBufferOperator::append_row(
    const int64_t row_pos,
    const ObIVector &vector)
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    // get vid and vector
    ObString vec_str;
    ObSingleKmeansExecutor *ctx = nullptr;
    ObIvfSq8BuildHelper *helper = nullptr;
    int64_t vec_dim = 0;
    if (vector.is_null(row_pos)) {
      // do nothing // ignore
    } else if (FALSE_IT(vec_str = vector.get_string(row_pos))) {
    } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&row_allocator_,
                                                                  ObLongTextType,
                                                                  CS_TYPE_BINARY,
                                                                  true,
                                                                  vec_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(vec_str));
    } else if (OB_FAIL(get_spec_ivf_helper<ObIvfSq8BuildHelper>(helper_, helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (FALSE_IT(vec_dim = vec_str.length() / sizeof(float))) {
    } else if (OB_FAIL(helper->update(reinterpret_cast<float*>(vec_str.ptr()), vec_dim))) {
      LOG_WARN("failed to update helper", K(ret));
    } else {
      LOG_DEBUG("[vec index debug] append sample vector", K(tablet_id_), K(vec_str));
    }
  }
  return ret;
}

int ObIVFSq8MetaIndexBuildOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (input_chunk.is_end_chunk()) {
    // do nothing
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("get dag tablet context failed", K(ret));
  } else {
    ObSingleKmeansExecutor *executor = nullptr;
    ObIvfSq8BuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper<ObIvfSq8BuildHelper>(helper_, helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_FAIL(helper->build())) {
      LOG_WARN("fail to do helper build", K(ret), KPC(helper));
    } else {
      output_chunk = input_chunk;
    }
  }
  return ret;
}

int ObIVFSq8MetaWriteMacroOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_FAIL(write(input_chunk, iter_))) {
    LOG_WARN("write macro failed", K(ret));
  }
  return ret;
}

int ObIVFPqAppendBufferOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  ObVectorIndexTabletContext *vector_index_ctx = nullptr;
  tablet_id_ = tablet_id;
  if (OB_FAIL(ObIVFIndexBaseOperator::init(tablet_id))) {
    LOG_WARN("init ivf index base operator failed", K(ret));
  } else if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else if (OB_ISNULL(vector_index_ctx = tablet_context->vector_index_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, vector index ctx is null", K(ret));
  } else {
    vector_col_idx_ = vector_index_ctx->pq_center_vector_col_idx_;
    is_inited_ = true;
  }
  return ret;
}

int ObIVFPqAppendBufferOperator::append_row(
    const int64_t row_pos,
    const ObIVector &vector)
{
  int ret = OB_SUCCESS;
  row_allocator_.reuse();
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    ObString residual_str;
    ObMultiKmeansExecutor *executor = nullptr;
    ObIvfPqBuildHelper *helper = nullptr;
    if (vector.is_null(row_pos)) {
      // do nothing // ignore
    } else if (FALSE_IT(residual_str = vector.get_string(row_pos))) {
    } else if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&row_allocator_,
                                                                  ObLongTextType,
                                                                  CS_TYPE_BINARY,
                                                                  true,
                                                                  residual_str))) {
      LOG_WARN("fail to get real data.", K(ret), K(residual_str));
    } else if (OB_FAIL(get_spec_ivf_helper(helper_, helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_ISNULL(executor = helper->get_kmeans_ctx())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected nullptr ctx", K(ret));
    } else if (OB_FAIL(executor->append_sample_vector(reinterpret_cast<float*>(residual_str.ptr())))) {
      LOG_WARN("failed to append sample vector", K(ret));
    } else {
      LOG_DEBUG("[vec index debug] append sample vector", K(tablet_id_), K(residual_str));
    }
  }
  return ret;
}

int ObIVFPqIndexBuildOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (input_chunk.is_end_chunk()) {
    // do nothing
  } else if (OB_FAIL(input_chunk.get_dag_tablet_context(tablet_context))) {
    LOG_WARN("get dag tablet context failed", K(ret));
  } else {
    ObIvfPqBuildHelper *helper = nullptr;
    if (OB_FAIL(get_spec_ivf_helper<ObIvfPqBuildHelper>(helper_, helper))) {
      LOG_WARN("fail to get ivf flat helper", K(ret));
    } else if (OB_FAIL(helper->build(table_id_, tablet_id_, nullptr/*insert_monitor*/))) {
      LOG_WARN("failed to build clusters", K(ret));
    } else {
      output_chunk = input_chunk;
    }
  }
  return ret;
}

int ObIVFPqWriteMacroOperator::execute(
    const ObChunk &input_chunk,
    ResultState &result_state,
    ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_FAIL(write(input_chunk, iter_))) {
    LOG_WARN("write macro failed", K(ret));
  }
  return ret;
}

// -------------------------------- ObEmbeddingBufferOperator --------------------------------
ObHNSWEmbeddingOperator::~ObHNSWEmbeddingOperator()
{
  if (nullptr != current_batch_) {
    current_batch_->~ObTaskBatchInfo();
    ob_free(current_batch_);
    current_batch_ = nullptr;
  }
  if (nullptr != embedmgr_) {
    embedmgr_->~ObEmbeddingTaskMgr();
    op_allocator_.free(embedmgr_);
    embedmgr_ = nullptr;
  }
}

int ObHNSWEmbeddingOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObDDLTabletContext *tablet_context = nullptr;
  ObVectorIndexTabletContext *vector_index_ctx = nullptr;
  tablet_id_ = tablet_id;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id));
  } else if (OB_ISNULL(vector_index_ctx = tablet_context->vector_index_ctx_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("error unexpected, vector index ctx is null", K(ret));
  } else {
    const uint64_t table_id = vector_index_ctx->table_id_;
    vec_dim_ = vector_index_ctx->vec_dim_;
    rowkey_cnt_ = vector_index_ctx->rowkey_cnt_;
    text_col_idx_ = vector_index_ctx->vector_chunk_col_idx_;
    extra_column_idxs_.reset();
    ObVectorIndexParam index_param;
    ObSchemaGetterGuard schema_guard;
    ObCollationType col_type = CS_TYPE_INVALID;

    if (OB_FAIL(ObVectorIndexUtil::get_index_column_collation_type(MTL_ID(), table_id, col_type))) {
      LOG_WARN("fail to get vector column collation type", K(ret), K(text_col_idx_), K(table_id));
    } else if (OB_FAIL(vector_index_ctx->build_extra_column_idxs(static_cast<int32_t>(text_col_idx_), extra_column_idxs_))) {
      LOG_WARN("build_extra_column_idxs failed", K(ret), K(text_col_idx_));
    } else if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(vector_index_ctx->vec_idx_param_, ObVectorIndexType::VIT_HNSW_INDEX, index_param, false))) {
      LOG_WARN("failed to parser params from string", K(ret));
    } else if (!ObVectorIndexUtil::is_valid_content_type(index_param.content_type_)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid vector index content type", K(ret), K(index_param.content_type_));
    } else if (OB_FAIL(ob_write_string(op_allocator_, ObString(index_param.endpoint_), model_id_))) {
      LOG_WARN("failed to copy endpoint to model_id", K(ret), K(ObString(index_param.endpoint_)));
    } else if (OB_ISNULL(embedmgr_)) {
      void *buf = op_allocator_.alloc(sizeof(ObEmbeddingTaskMgr));
      if (OB_ISNULL(buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc ObEmbeddingTaskMgr", K(ret));
      } else {
        embedmgr_ = new (buf) ObEmbeddingTaskMgr(*this);
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_FAIL(embedmgr_->init(model_id_, col_type, index_param.content_type_))) {
        embedmgr_->~ObEmbeddingTaskMgr();
        op_allocator_.free(embedmgr_);
        embedmgr_ = nullptr;
        LOG_WARN("failed to init embedding task manager", K(ret));
      } else {
        batch_size_ = 64; // TODO(fanfangyao.ffy): To be tuned
        void *batch_buf = ob_malloc(sizeof(ObTaskBatchInfo), ObMemAttr(MTL_ID(), "TaskBatch"));
        if (OB_ISNULL(batch_buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate batch context", K(ret));
        } else {
          current_batch_ = new (batch_buf) ObTaskBatchInfo();
          if (OB_FAIL(current_batch_->init(batch_size_, vec_dim_))) {
            LOG_WARN("failed to init batch context", K(ret), K(batch_size_), K(vec_dim_));
            current_batch_->~ObTaskBatchInfo();
            ob_free(current_batch_);
            current_batch_ = nullptr;
          }
        }

        if (OB_SUCC(ret)) {
          is_inited_ = true;
          cur_file_idx_ = 0;
          cur_datum_rows_ = nullptr;
          cur_row_in_batch_ = 0;
          chunk_exhausted_ = false;
        }
      }
    }
  }
  return ret;
}

int ObHNSWEmbeddingOperator::execute(const ObChunk &input_chunk,
                                       ResultState &result_state,
                                       ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (embedmgr_->get_failed()) {
    ret = error_ret_code_;
    LOG_WARN("fail to embedding", K(ret));
  } else {
    if (input_chunk.is_end_chunk()) {
      // submit the last batch of data
      if (OB_NOT_NULL(current_batch_) && current_batch_->get_count() > 0 && OB_FAIL(flush_current_batch())) {
        if (OB_EAGAIN == ret) {
          //submit queue is full, record position and return
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("flush current batch failed", K(ret));
        }
      }
    } else {
      if (OB_FAIL(process_input_chunk(input_chunk))) {
        if (OB_EAGAIN == ret) {
          //submit queue is full, record position and return
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("process input chunk failed", K(ret));
        }
      }
    }

    //wait for task completion
    if (OB_SUCC(ret)) {
      if (OB_FAIL(embedmgr_->wait_for_completion())) {
        LOG_WARN("wait for completion failed", K(ret));
      } else if (OB_FAIL(get_ready_results(output_chunk, result_state))) {
        LOG_WARN("get ready results failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && embedmgr_->get_failed()) {
    ret = error_ret_code_;
    LOG_WARN("fail to embedding", K(ret));
  }

  if (OB_SUCC(ret) && !input_chunk.is_end_chunk() && is_chunk_exhausted()) {
    reset_chunk_exhausted();
    reset_scan_state();
    result_state = ObPipelineOperator::NEED_MORE_INPUT;
  }
  // if ret is not success, free output_chunk
  if (OB_FAIL(ret) && output_chunk.is_valid()) {
    output_chunk.batch_info_->~ObTaskBatchInfo();
    ob_free(output_chunk.batch_info_);
    output_chunk.batch_info_ = nullptr;
    output_chunk.reset();
  }

  return ret;
}

int ObHNSWEmbeddingOperator::try_execute_finish(const ObChunk &input_chunk,
  ResultState &result_state,
  ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  if (input_chunk.is_end_chunk() && output_chunk.is_valid()) {
    //do nothing
  } else if (OB_FAIL(ObVectorIndexBaseOperator::try_execute_finish(input_chunk, result_state, output_chunk))) {
    LOG_WARN("fail to try execute finish", K(ret));
  }
  return ret;
}

int ObHNSWEmbeddingOperator::get_ready_results(ObChunk &output_chunk, ResultState &result_state)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWEmbeddingOperator not init", K(ret), K(is_inited_));
  } else {
    ObTaskBatchInfo *batch_info = nullptr;
    int ret_code = OB_SUCCESS;

    if (OB_FAIL(embedmgr_->get_ready_batch_info(batch_info, ret_code))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("fetch ready batch info failed", K(ret));
      }
    } else if (OB_SUCCESS != ret_code) {
      error_ret_code_ = ret_code;
      // Still need to cleanup batch_info
      if (OB_NOT_NULL(batch_info)) {
        batch_info->~ObTaskBatchInfo();
        ob_free(batch_info);
      }
      LOG_WARN("embedding task failed", K(error_ret_code_));
    } else if (OB_NOT_NULL(batch_info)) {
      // Transfer batch_info to output_chunk (ownership transfer)
      output_chunk.type_ = ObChunk::TASK_BATCH_INFO;
      output_chunk.batch_info_ = batch_info;
      result_state = ObPipelineOperator::HAVE_MORE_OUTPUT;
    }
  }
  return ret;
}

int ObHNSWEmbeddingOperator::process_input_chunk(const ObChunk &input_chunk)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!input_chunk.is_valid() || !input_chunk.is_cg_row_tmp_files_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(input_chunk));
  } else if (OB_ISNULL(current_batch_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current batch is null", K(ret));
  } else {
    ObArray<ObCGRowFile *> *cg_row_file_arr = input_chunk.cg_row_file_arr_;
    if (OB_ISNULL(cg_row_file_arr)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("cg row file array is null", K(ret));
    } else {
      while (OB_SUCC(ret) && !chunk_exhausted_) {
        blocksstable::ObStorageDatum text;
        common::ObArray<blocksstable::ObStorageDatum> extras;
        bool has_row = false;
        if (current_batch_->is_full()) {
          if (OB_FAIL(flush_current_batch())) {
            if (OB_EAGAIN == ret) {
              LOG_INFO("embed mgr is full, record position and return", K(ret), "batch_count", current_batch_->get_count());
            } else {
              LOG_WARN("submit batch failed", K(ret), "batch_count", current_batch_->get_count());
            }
          }
        } else if (OB_FAIL(get_next_row_from_tmp_files(cg_row_file_arr, text, extras, has_row))) {
          LOG_WARN("get_next_row_from_tmp_files failed", K(ret));
        } else if (!has_row) {
          chunk_exhausted_ = true;
        } else {
          if (OB_FAIL(current_batch_->add_item(text, extras))) {
            LOG_WARN("add item to batch failed", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObHNSWEmbeddingOperator::get_next_row_from_tmp_files(ObArray<ObCGRowFile *> *cg_row_file_arr,
                                                          blocksstable::ObStorageDatum &text,
                                                          common::ObArray<blocksstable::ObStorageDatum> &extras,
                                                          bool &has_row)
{
  int ret = OB_SUCCESS;
  has_row = false;
  if (OB_ISNULL(cg_row_file_arr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cg_row_file_arr", K(ret), K(cg_row_file_arr));
  } else {
    while (OB_SUCC(ret) && cur_file_idx_ < cg_row_file_arr->count() && !has_row) {
      ObCGRowFile *&row_file = cg_row_file_arr->at(cur_file_idx_);
      if (OB_ISNULL(row_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row file null", K(ret), K(cur_file_idx_));
      }
      while (OB_SUCC(ret) && !has_row) {
        if (OB_FAIL(get_next_batch_from_tmp_files(row_file))) {
          LOG_WARN("get next batch failed", K(ret));
        } else if (OB_ISNULL(cur_datum_rows_)) {
          // current file end, switch to next file here
          cur_file_idx_++;
          break;
        } else {
          // scan each row in current batch
          const int64_t total_row_count = cur_datum_rows_->row_count_;
          const int64_t total_column_count = cur_datum_rows_->get_column_count();
          if (total_column_count <= text_col_idx_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("column index out of range", K(ret), K(total_column_count), K(text_col_idx_));
          } else {
            while (OB_SUCC(ret) && !has_row && cur_row_in_batch_ < total_row_count) {
              blocksstable::ObDatumRow current_row;
              if (OB_FAIL(current_row.init(cur_datum_rows_->get_column_count()))) {
                LOG_WARN("init datum row failed", K(ret), K(cur_datum_rows_->get_column_count()));
              } else if (OB_FAIL(cur_datum_rows_->to_datum_row(cur_row_in_batch_, current_row))) {
                STORAGE_LOG(WARN, "to_datum_row failed", K(ret), K(cur_row_in_batch_));
              } else if (OB_FAIL(parse_row(current_row, text, extras))) {
                LOG_WARN("parse row failed", K(ret));
              } else {
                cur_row_in_batch_++;
                has_row = true;
              }
            }
            if (OB_SUCC(ret) && !has_row) {
              // current batch finished, reset to fetch next batch
              cur_datum_rows_ = nullptr;
              cur_row_in_batch_ = 0;
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObHNSWEmbeddingOperator::get_next_batch_from_tmp_files(ObCGRowFile *&row_file)
{
  int ret = OB_SUCCESS;
  if (nullptr == cur_datum_rows_) { // current batch is empty, get next batch
    if (OB_FAIL(row_file->get_next_batch(cur_datum_rows_))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        // current file end, release file and switch to next file
        row_file->~ObCGRowFile();
        ob_free(row_file);
        row_file = nullptr;
        cur_datum_rows_ = nullptr;
        cur_row_in_batch_ = 0;
      } else {
        LOG_WARN("get next batch failed", K(ret));
      }
    } else {
      cur_row_in_batch_ = 0;
    }
  }
  return ret;
}

int ObHNSWEmbeddingOperator::parse_row(const blocksstable::ObDatumRow &current_row,
                                       blocksstable::ObStorageDatum &text,
                                       common::ObArray<blocksstable::ObStorageDatum> &extras)
{
  int ret = OB_SUCCESS;
  text.reset();
  extras.reset();
  if (OB_UNLIKELY(current_row.get_column_count() <= text_col_idx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", K(ret), K(current_row), K(text_col_idx_));
  } else {
    const blocksstable::ObStorageDatum &chunk_cell = current_row.storage_datums_[text_col_idx_];
    text.shallow_copy_from_datum(chunk_cell);

    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); ++i) {
      int32_t col_idx = extra_column_idxs_.at(i);
      if (col_idx < 0 || col_idx >= current_row.get_column_count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("extra column index out of range", K(ret), K(col_idx), K(current_row.get_column_count()));
      } else if (OB_FAIL(extras.push_back(current_row.storage_datums_[col_idx]))) {
        LOG_WARN("push extra datum failed", K(ret), K(col_idx));
      }
    }
  }
  return ret;
}

int ObHNSWEmbeddingOperator::flush_current_batch()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(current_batch_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("current batch is null", K(ret));
  } else if (OB_UNLIKELY(current_batch_->get_count() <= 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no items in current batch", K(ret));
  } else if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObHNSWEmbeddingOperator not init", K(ret), K(is_inited_));
  } else {
    // Submit batch_info (ownership transferred to embedmgr slot ring)
    if (OB_FAIL(embedmgr_->submit_batch_info(current_batch_))) {
      if (OB_EAGAIN == ret) {
        LOG_INFO("embed mgr is full, record position and return", K(ret), "batch_count", current_batch_->get_count());
      } else {
        LOG_WARN("submit batch failed", K(ret), "batch_count", current_batch_->get_count());
      }
    } else {
      // Create new batch for next round
      void *batch_buf = ob_malloc(sizeof(ObTaskBatchInfo), ObMemAttr(MTL_ID(), "TaskBatch"));
      if (OB_ISNULL(batch_buf)) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to allocate new batch context", K(ret));
      } else {
        current_batch_ = new (batch_buf) ObTaskBatchInfo();
        if (OB_FAIL(current_batch_->init(batch_size_, vec_dim_))) {
          LOG_WARN("failed to init new batch context", K(ret), K(batch_size_), K(vec_dim_));
          current_batch_->~ObTaskBatchInfo();
          ob_free(current_batch_);
          current_batch_ = nullptr;
        }
      }
    }
  }
  return ret;
}

// -------------------------------- ObEmbeddingWriteMacroOperator --------------------------------
int ObHNSWEmbeddingRowIterator::init(ObVectorIndexTabletContext &context)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObHNSWEmbeddingRowIterator::init(
    ObVectorIndexTabletContext &context,
    ObTaskBatchInfo *batch_info)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("Iterator init twice", K(ret));
  } else if (OB_ISNULL(batch_info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("batch info is null", K(ret));
  } else {
    batch_info_ = batch_info;
    rowkey_cnt_ = context.rowkey_cnt_;
    column_cnt_ = context.column_cnt_;
    snapshot_version_ = context.snapshot_version_;
    tablet_id_ = context.tablet_id_;
    vec_dim_ = context.vec_dim_;
    vector_col_idx_ = context.vector_chunk_col_idx_;
    extra_column_idxs_.reset();
    if (OB_FAIL(context.build_extra_column_idxs(static_cast<int32_t>(vector_col_idx_), extra_column_idxs_))) {
      LOG_WARN("build_extra_column_idxs failed", K(ret), K(vector_col_idx_));
    } else {
      cur_result_pos_ = 0;
      if (vector_col_idx_ < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected embedding column index", K(ret), K(vector_col_idx_));
      } else {
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObHNSWEmbeddingRowIterator::get_next_row(blocksstable::ObDatumRow *&datum_row)
{
  int ret = OB_SUCCESS;
  datum_row = nullptr;
  const int64_t request_cnt = column_cnt_;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (current_row_.get_column_count() <= 0 && OB_FAIL(current_row_.init(iter_allocator_, request_cnt))) {
    LOG_WARN("init datum row failed", K(ret), K(request_cnt));
  } else if (OB_UNLIKELY(current_row_.get_column_count() != request_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(request_cnt), "datum_row_cnt", current_row_.get_column_count());
  } else if (OB_ISNULL(batch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("batch info is null", K(ret));
  } else if (cur_result_pos_ >= batch_info_->get_count()) {
    ret = OB_ITER_END;
  } else if (is_embedding_col_invalid(current_row_.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, embedding col idx error", K(ret), K(vector_col_idx_));
  } else {
    ObString data_str;
    ObString vec_res;
    ObEmbeddingResult *result = batch_info_->get_results().at(cur_result_pos_);
    if (OB_ISNULL(result)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("null embedding result", K(ret), K(cur_result_pos_));
    } else {
      if (!result->need_embedding()) {
        current_row_.storage_datums_[vector_col_idx_].set_null();
      } else {
        if (OB_ISNULL(result->get_vector()) || result->get_vector_dim() <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected error, vector is null or dim is 0", K(ret), K(result->get_vector()), K(result->get_vector_dim()));
        } else {
          data_str.assign(reinterpret_cast<char *>(result->get_vector()), static_cast<int32_t>(sizeof(float) * result->get_vector_dim()));
          if (OB_FAIL(sql::ObArrayExprUtils::set_array_res(nullptr, data_str.length(), row_allocator_, vec_res, data_str.ptr()))) {
            LOG_WARN("failed to set array res", K(ret));
          } else {
            current_row_.storage_datums_[vector_col_idx_].set_string(vec_res);
          }
        }
      }
      if (OB_SUCC(ret)) {
        const common::ObArray<blocksstable::ObStorageDatum> &extras = result->get_extra_cols();
        if (extra_column_idxs_.count() != extras.count()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("extras count mismatch", K(extra_column_idxs_.count()), K(extras.count()), K(ret));
        } else {
          for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); ++i) {
            int32_t col_idx = extra_column_idxs_.at(i);
            if (col_idx < 0 || col_idx >= current_row_.get_column_count()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("col idx not valid", K(col_idx), K(current_row_.get_column_count()), K(ret));
            } else {
              current_row_.storage_datums_[col_idx].shallow_copy_from_datum(extras.at(i));
            }
          }
        }
        current_row_.storage_datums_[rowkey_cnt_].set_int(-snapshot_version_);
        current_row_.storage_datums_[rowkey_cnt_ + 1].set_int(0);
      }

      current_row_.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      datum_row = &current_row_;
      cur_result_pos_++;
    }
  }
  return ret;
}

ObHNSWEmbeddingWriteMacroOperator::~ObHNSWEmbeddingWriteMacroOperator()
{
  if (OB_NOT_NULL(slice_writer_)) {
    slice_writer_->~ObTabletSliceWriter();
    op_allocator_.free(slice_writer_);
    slice_writer_ = nullptr;
  }
}

int ObHNSWEmbeddingWriteMacroOperator::init(const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  slice_idx_ = slice_idx;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else {
    ObDDLIndependentDag *ddl_dag = nullptr;
    ObWriteMacroParam write_param;
    if (OB_ISNULL(ddl_dag = static_cast<ObDDLIndependentDag *>(get_dag()))) {
      ret = OB_ERR_SYS;
      LOG_WARN("get dag failed", K(ret));
    } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, slice_idx_, -1/*cg_idx*/, ddl_dag, write_param))) {
      LOG_WARN("fill writer param failed", K(ret));
    } else {
      if (OB_ISNULL(slice_writer_ = OB_NEWx(ObTabletSliceWriter, &op_allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("allocate memory for tablet slice writer failed", K(ret));
      } else {
        if (OB_FAIL(slice_writer_->init(write_param))) {
          LOG_WARN("init macro block slice store failed", K(ret));
        } else {
          is_inited_ = true;
        }
      }
    }

    if (OB_FAIL(ret) && OB_NOT_NULL(slice_writer_)) {
      slice_writer_->~ObTabletSliceWriter();
      op_allocator_.free(slice_writer_);
      slice_writer_ = nullptr;
    }
  }

  return ret;
}

int ObHNSWEmbeddingWriteMacroOperator::execute(const ObChunk &input_chunk,
                                           ResultState &result_state,
                                           ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;

  ObDDLTabletContext *tablet_context = nullptr;

  if (input_chunk.is_end_chunk()) {
    if (OB_NOT_NULL(slice_writer_)) {
      int close_ret = OB_SUCCESS;
      if (OB_SUCCESS != (close_ret = slice_writer_->close())) {
        LOG_WARN("embedding writer close failed", K(close_ret), K_(tablet_id), K_(slice_idx));
        ret = close_ret;
      }
      slice_writer_->~ObTabletSliceWriter();
      op_allocator_.free(slice_writer_);
      slice_writer_ = nullptr;
    }
  } else if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
    LOG_WARN("get ddl tablet context failed", K(ret));
  } else if (!input_chunk.is_task_batch_info_type() || OB_ISNULL(input_chunk.batch_info_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid input chunk", K(ret), K(input_chunk));
  } else if (input_chunk.batch_info_->get_count() == 0) {
    // empty batch, skip write
  } else if (OB_ISNULL(slice_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slice writer is not initialized", K(ret), K_(tablet_id), K_(slice_idx));
  } else {
    iter_.reuse();
    if (OB_FAIL(iter_.init(*tablet_context->vector_index_ctx_, input_chunk.batch_info_))) {
      LOG_WARN("init embedding row iterator with batch info failed", K(ret));
    } else {
      blocksstable::ObDatumRow *datum_row = nullptr;
      int64_t row_idx = 0;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter_.get_next_row(datum_row))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to get next embedding data row", K(ret));
          }
        } else {
          if (OB_FAIL(slice_writer_->append_row(*datum_row))) {
            LOG_WARN("fail to append row to macro block slice store", K(ret), K(row_idx));
          } else {
            row_idx++;
          }
        }
      }
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
    }

    input_chunk.batch_info_->~ObTaskBatchInfo();
    ob_free(input_chunk.batch_info_);
  }
  return ret;
}

// -------------------------------- SlotContext methods --------------------------------
// -------------------------------- ObBatchFileEmbeddingOperator --------------------------------
ObBatchFileEmbeddingOperator::ObBatchFileEmbeddingOperator(ObPipeline *pipeline)
    : ObVecEmbeddingBaseOp(pipeline),
      service_(nullptr),
      embed_provider_(nullptr),
      model_id_(),
      ai_execution_mode_(share::OB_AI_ACCESS_MODE_BATCH_FILE),
      allow_null_on_failure_(false),
      vec_dim_(-1),
      text_col_idx_(-1),
      rowkey_cnt_(-1),
      dir_id_(-1),
      error_ret_code_(OB_SUCCESS),
      endpoint_info_(nullptr),
      request_model_name_(),
      allocator_("BatchFileEmb", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      text_col_collation_type_(CS_TYPE_INVALID),
      extra_column_idxs_(),
      slot_ring_(),
      slot_contexts_(),
      ddl_task_id_(0),
      writer_(),
      current_task_row_count_(0),
      current_batch_info_(nullptr),
      current_cg_row_files_(nullptr),
      cur_file_idx_(0),
      cur_datum_rows_(nullptr),
      cur_row_in_batch_(0),
      chunk_exhausted_(false),
      total_rows_collected_(0),
      all_data_collected_(false),
      all_tasks_submitted_(false),
      end_chunk_sent_(false),
      drain_start_ts_(0),
      drain_poll_count_(0)
  {}

ObBatchFileEmbeddingOperator::~ObBatchFileEmbeddingOperator()
{
  cancel_inflight_tasks_();
  if (OB_NOT_NULL(endpoint_info_)) {
    endpoint_info_->~ObAiModelEndpointInfo();
    allocator_.free(endpoint_info_);
    endpoint_info_ = nullptr;
  }
  destroy_current_batch_info_();
  // Clean up batch_info in slot contexts
  for (int64_t i = 0; i < slot_contexts_.count(); ++i) {
    ObTaskBatchInfo *bi = slot_contexts_.at(i).batch_info_;
    if (OB_NOT_NULL(bi)) {
      bi->~ObTaskBatchInfo();
      ob_free(bi);
      slot_contexts_.at(i).batch_info_ = nullptr;
    }
  }
  slot_ring_.destroy();
}

int ObBatchFileEmbeddingOperator::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet_id", K(ret), K(tablet_id));
  } else {
    tablet_id_ = tablet_id;
    ai_execution_mode_ = share::OB_AI_ACCESS_MODE_BATCH_FILE;
    LOG_INFO("[BATCH-FILE] ObBatchFileEmbeddingOperator::init", K(tablet_id), K(tablet_id_));

    // Get DDL context for column info and model_id
    ObDDLTabletContext *tablet_context = nullptr;
    ObVectorIndexTabletContext *vector_index_ctx = nullptr;
    if (OB_FAIL(get_ddl_tablet_context(tablet_context))) {
      LOG_WARN("get ddl tablet context failed", K(ret), K(tablet_id_), K(tablet_id));
    } else if (OB_ISNULL(vector_index_ctx = tablet_context->vector_index_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector index context is null", K(ret));
    } else {
      // Resolve model_id from vec_idx_param
      ObVectorIndexParam index_param;
      if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
          vector_index_ctx->vec_idx_param_, ObVectorIndexType::VIT_HNSW_INDEX, index_param, false))) {
        LOG_WARN("failed to parse vector index param", K(ret), K(vector_index_ctx->vec_idx_param_));
      } else if (OB_FAIL(ob_write_string(allocator_, ObString(index_param.endpoint_), model_id_))) {
        LOG_WARN("failed to copy model_id from endpoint", K(ret));
      } else {
        allow_null_on_failure_ = index_param.allow_null_on_failure_;
      }
    }

    if (OB_SUCC(ret)) {
      vec_dim_ = vector_index_ctx->vec_dim_;
      rowkey_cnt_ = vector_index_ctx->rowkey_cnt_;
      text_col_idx_ = vector_index_ctx->vector_chunk_col_idx_;
      ddl_task_id_ = vector_index_ctx->ddl_task_id_;
      // Initialize extra_column_idxs_ from vector_index_ctx
      extra_column_idxs_.reset();
      for (int64_t i = 0; OB_SUCC(ret) && i < vector_index_ctx->extra_column_idx_types_.count(); ++i) {
        if (OB_FAIL(extra_column_idxs_.push_back(static_cast<int32_t>(vector_index_ctx->extra_column_idx_types_.at(i).idx_)))) {
          LOG_WARN("failed to push back extra column idx", K(ret), K(i));
        }
      }
      // Get collation type for text column
      if (OB_FAIL(ret)) {
        // already logged
      } else if (OB_FAIL(ObVectorIndexUtil::get_index_column_collation_type(MTL_ID(), vector_index_ctx->table_id_, text_col_collation_type_))) {
        LOG_WARN("failed to get index column collation type", K(ret), K(text_col_idx_));
      }
    }

    if (OB_SUCC(ret)) {
      // Get ObAiAccessService instance from ObPluginVectorIndexService
      vector_index::ObAiAccessService *service = nullptr;
      ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
      if (OB_ISNULL(vec_index_service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObPluginVectorIndexService is null", K(ret));
      } else if (OB_FAIL(vec_index_service->get_ai_execution_service(service))) {
        LOG_WARN("failed to get ai execution service", K(ret));
      } else if (OB_ISNULL(service)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ObAiAccessService is null", K(ret));
      } else {
        service_ = service;
      }
    }

    // Get AI config from model_id (endpoint info, api key, etc.)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(get_ai_config_(model_id_))) {
        LOG_WARN("failed to get AI config", K(ret), K_(model_id));
      }
    }

    // Create embed provider from endpoint provider name
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(endpoint_info_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("endpoint_info is null after get_ai_config_", K(ret));
      } else {
        common::ObAIFuncIEmbed *embed_provider_tmp = nullptr;
        if (OB_FAIL(common::ObAIFuncUtils::get_embed_provider(
                     allocator_, endpoint_info_->get_provider(), embed_provider_tmp))) {
          LOG_WARN("failed to get embed provider", K(ret),
                   "provider", endpoint_info_->get_provider());
        } else {
          embed_provider_ = embed_provider_tmp;
        }
      }
    }

    // Initialize SlotRing
    if (OB_SUCC(ret)) {
      if (OB_FAIL(slot_ring_.init(DEFAULT_MAX_CONCURRENT_TASKS))) {
        LOG_WARN("failed to init slot ring", K(ret));
      }
    }

    if (OB_SUCC(ret)) {
      is_inited_ = true;
      LOG_INFO("ObBatchFileEmbeddingOperator initialized",
               K(tablet_id), K_(model_id), K_(ai_execution_mode), K_(ddl_task_id));
    }
  }
  return ret;
}

int ObBatchFileEmbeddingOperator::get_ai_config_(const common::ObString &model_id)
{
  int ret = OB_SUCCESS;

  if (model_id.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("model_id is empty", K(ret));
  } else {
    ObAIFuncExprInfo *info = nullptr;
    const share::ObAiModelEndpointInfo *endpoint_info = nullptr;
    omt::ObAiServiceGuard ai_service_guard;
    omt::ObTenantAiService *ai_service = MTL(omt::ObTenantAiService *);
    if (OB_ISNULL(ai_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai service is null", K(ret));
    } else if (OB_FAIL(ObAIFuncUtils::get_ai_func_info(op_allocator_, const_cast<common::ObString &>(model_id), info))) {
      LOG_WARN("failed to get ai func info", K(ret), K(model_id));
    } else if (OB_ISNULL(info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ai func info is null", K(ret));
    } else if (OB_FAIL(ai_service->get_ai_service_guard(ai_service_guard))) {
      LOG_WARN("failed to get ai service guard", K(ret));
    } else if (OB_FAIL(ai_service_guard.get_ai_endpoint_by_ai_model_name(model_id, endpoint_info))) {
      LOG_WARN("failed to get endpoint info", K(ret), K(model_id));
    } else if (OB_ISNULL(endpoint_info)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("endpoint info is null", K(ret));
    } else {
      // Allocate endpoint_info_ if not already allocated
      if (OB_ISNULL(endpoint_info_)) {
        void *buf = allocator_.alloc(sizeof(share::ObAiModelEndpointInfo));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to allocate memory for endpoint info", K(ret));
        } else {
          endpoint_info_ = new (buf) share::ObAiModelEndpointInfo();
        }
      }
      if (OB_SUCC(ret) && OB_FAIL(endpoint_info_->deep_copy(allocator_, *endpoint_info))) {
        LOG_WARN("failed to deep copy endpoint info", K(ret));
      } else if (OB_SUCC(ret)) {
        // Use endpoint's request_model_name if set, otherwise AI Model's model_name
        const common::ObString &request_model =
            endpoint_info_->get_request_model_name().empty()
            ? info->model_
            : endpoint_info_->get_request_model_name();
        if (OB_FAIL(ob_write_string(allocator_, request_model, request_model_name_, true /*c_style*/))) {
          LOG_WARN("failed to copy request model name", K(ret));
        } else {
          LOG_INFO("get AI config successfully", K(model_id), "endpoint_id", endpoint_info_->get_endpoint_id(),
                   K_(request_model_name), "ai_model_name", info->model_);
        }
      }
    }
  }
  return ret;
}

/*
 * Batch-file embedding flow (Writer-based):
 *
 *   open_batch_task(endpoint, cmd, ddl_task_id, provider, writer)
 *     → writer.append(line) × N
 *       - returns OB_ITER_END when threshold reached (50K lines / 100MB);
 *         line NOT written. Commit current writer, re-open, retry same line.
 *     → writer.commit(task_id)
 *       - synchronously registers task as RUNNING and schedules it
 *       - Poller picks up the task for upload → batch submit → poll → download
 *
 *   DDL reads results via query_task_status() + get_next_result()
 */

int ObBatchFileEmbeddingOperator::execute(const ObChunk &input_chunk,
                                            ResultState &result_state,
                                            ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;

  LOG_DEBUG("[BATCH-FILE] execute start",
           K_(is_inited), KP_(service), K(input_chunk.type_),
           "is_cg_row_tmp_files", input_chunk.is_cg_row_tmp_files_type(),
           K_(tablet_id), K_(all_data_collected), K_(all_tasks_submitted),
           K_(end_chunk_sent), K_(total_rows_collected), K_(slot_ring));

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (need_stop_()) {
    cancel_inflight_tasks_();
    ret = OB_CANCELED;
    LOG_WARN("[BATCH-FILE] dag stopped, exiting execute", K(ret), K_(tablet_id));
  } else if (OB_ISNULL(service_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("BatchFile mode not supported - ObAiAccessService not available", K(ret));
  } else if (input_chunk.is_end_chunk()) {
    if (OB_FAIL(handle_end_chunk_(output_chunk, result_state))) {
      LOG_WARN("handle_end_chunk_ failed", K(ret));
    }
  } else if (!input_chunk.is_cg_row_tmp_files_type() || OB_ISNULL(input_chunk.cg_row_file_arr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid input chunk type for BatchFile mode", K(ret), K(input_chunk.type_));
  } else {
    if (input_chunk.cg_row_file_arr_ != current_cg_row_files_) {
      all_data_collected_ = false;
      chunk_exhausted_ = false;
      cur_file_idx_ = 0;
      cur_datum_rows_ = nullptr;
      cur_row_in_batch_ = 0;
      current_cg_row_files_ = input_chunk.cg_row_file_arr_;
    }

    if (all_data_collected_ || all_tasks_submitted_) {
      // Current chunk already collected or tasks already submitted.
      // Just poll and check for results (don't re-process data).
      // NOTE: !slot_ring_.is_empty() must NOT be included here — the slot ring only
      // enforces output ordering; it must not block collection of new data chunks.
      // New chunks arriving while AI tasks are in-flight must still be collected,
      // otherwise their data is permanently lost.
      if (OB_FAIL(poll_submitted_slots_())) {
        LOG_WARN("poll_submitted_slots_ failed", K(ret));
      } else if (OB_FAIL(check_and_output_result_(output_chunk, result_state))) {
        LOG_WARN("check_and_output_result_ failed", K(ret));
      }
      if (OB_SUCC(ret) && ObPipelineOperator::NEED_MORE_INPUT == result_state) {
        if (OB_FAIL(do_yield_())) {
          LOG_WARN("do_yield_ failed", K(ret));
        }
      }
    } else {
      // Collect data from input chunk and send to AiAccessService
      if (OB_FAIL(collect_data_to_service_(input_chunk))) {
        LOG_WARN("collect_data_to_service_ failed", K(ret));
      }
      // Poll and check for results from previously submitted tasks
      if (OB_SUCC(ret)) {
        if (OB_FAIL(poll_submitted_slots_())) {
          LOG_WARN("poll_submitted_slots_ failed", K(ret));
        } else if (OB_FAIL(check_and_output_result_(output_chunk, result_state))) {
          LOG_WARN("check_and_output_result_ failed", K(ret));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
    error_ret_code_ = ret;
    cancel_inflight_tasks_();
  }

  return ret;
}

int ObBatchFileEmbeddingOperator::try_execute_finish(const ObChunk &input_chunk,
                                                       ResultState &result_state,
                                                       ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  if (input_chunk.is_end_chunk() && output_chunk.is_valid()) {
    // Bypass base class check that would error on HAVE_MORE_OUTPUT + end_chunk.
    // We legitimately produce multiple outputs during end_chunk draining.
  } else if (OB_FAIL(ObVectorIndexBaseOperator::try_execute_finish(input_chunk, result_state, output_chunk))) {
    LOG_WARN("fail to try execute finish", K(ret));
  }
  return ret;
}

int ObBatchFileEmbeddingOperator::collect_data_to_service_(const ObChunk &input_chunk)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(input_chunk.cg_row_file_arr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg_row_file_arr is null", K(ret));
  } else if (OB_ISNULL(service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service_ is null", K(ret));
  } else {
    LOG_DEBUG("[BATCH-FILE] collect_data_to_service_ start",
             "file_count", current_cg_row_files_->count(),
             "is_end_chunk", input_chunk.is_end_chunk());

    const common::ObString &model_name = request_model_name_;
    blocksstable::ObStorageDatum text;
    common::ObArray<blocksstable::ObStorageDatum> extras;
    bool has_row = false;

    while (OB_SUCC(ret) && !chunk_exhausted_) {
      extras.reset();
      if (OB_FAIL(get_next_row_from_tmp_files_(current_cg_row_files_, text, extras, has_row))) {
        LOG_WARN("get_next_row_from_tmp_files_ failed", K(ret));
      } else if (!has_row) {
        chunk_exhausted_ = true;
        all_data_collected_ = true;
      } else if (text.is_null()) {
        // NULL text: add to batch_info as SKIP_EMBEDDING so domain index gets a row
        // for every base table row (required by concat_rows in HNSW build scan).
        if (OB_FAIL(ensure_batch_info_())) {
          LOG_WARN("ensure_batch_info_ failed for null row", K(ret));
        } else if (OB_FAIL(current_batch_info_->add_item(text, extras, false))) {
          LOG_WARN("add_item failed for null row", K(ret));
        } else {
          total_rows_collected_++;
          LOG_DEBUG("[BATCH-FILE] added null row to batch_info as SKIP_EMBEDDING", K(total_rows_collected_));
        }
      } else {
        // Open writer and batch_info if needed (deferred until we have a valid row)
        if (!writer_.is_inited()) {
          if (OB_FAIL(open_new_task_())) {
            LOG_WARN("failed to open batch task", K(ret));
          } else {
            LOG_DEBUG("[BATCH-FILE] opened batch task writer", K_(ddl_task_id));
          }
        }

#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          ret = OB_E(common::EventTable::EN_BATCH_FILE_OP_COLLECT_DATA_ERR) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            LOG_WARN("[ERRSIM] fail to collect data to service", KR(ret), K_(ddl_task_id));
          }
        }
#endif
        if (OB_FAIL(ret)) {
          // already failed
        } else if (OB_ISNULL(current_batch_info_)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("current_batch_info_ is null", K(ret));
        } else {
          // Extract text from StorageDatum (LOB deref + charset convert)
          common::ObArenaAllocator tmp_alloc("BatchBodyTmp",
                                             OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
          common::ObString text_str;
          common::ObString raw_text = text.get_string();
          if (OB_FAIL(sql::ObTextStringHelper::read_real_string_data(&tmp_alloc,
                                                                      ObLongTextType,
                                                                      true /*has_lob_header*/,
                                                                      raw_text,
                                                                      nullptr /*exec_ctx*/))) {
            LOG_WARN("failed to read real string data for text", K(ret), K(raw_text.length()));
          } else if (raw_text.length() == 0) {
            // Empty text: add to batch_info as SKIP_EMBEDDING (same reason as null rows)
            if (OB_FAIL(current_batch_info_->add_item(text, extras, false))) {
              LOG_WARN("add_item failed for empty row", K(ret));
            } else {
              total_rows_collected_++;
              LOG_DEBUG("[BATCH-FILE] added empty row to batch_info as SKIP_EMBEDDING", K(total_rows_collected_));
            }
          } else if (text_col_collation_type_ != CS_TYPE_UTF8MB4_BIN
                     && text_col_collation_type_ != CS_TYPE_UTF8MB4_GENERAL_CI
                     && text_col_collation_type_ != CS_TYPE_INVALID) {
            if (OB_FAIL(ObCharset::charset_convert(tmp_alloc, raw_text,
                                                   text_col_collation_type_,
                                                   CS_TYPE_UTF8MB4_GENERAL_CI,
                                                   text_str))) {
              LOG_WARN("failed to convert text charset to UTF-8", K(ret),
                       K(text_col_collation_type_), K(raw_text.length()));
            }
          } else {
            text_str = raw_text;
          }

          // Build batch line via provider and append with retry.
          // Skip rows where text_str is empty (NULL or empty-string rows were handled above).
          // OB_ITER_END means file threshold reached (line NOT written): commit, re-open, retry.
          // encode_batch_line is called inside the retry loop so index is correct after re-open.
          if (OB_FAIL(ret) || text_str.empty()) {
            // already failed or row was skipped (NULL/empty text)
          } else if (OB_ISNULL(embed_provider_)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("embed_provider_ is null", K(ret));
          } else {
            bool need_retry = false;
            do {
              need_retry = false;
              common::ObArenaAllocator line_alloc("BatchBodyLine",
                                                   OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
              common::ObAiBatchFileLine line;
              if (OB_FAIL(embed_provider_->encode_batch_line(line_alloc, request_model_name_,
                                                              current_batch_info_->get_count(),
                                                              text_str, line))) {
                LOG_WARN("failed to encode batch line from provider", K(ret));
              } else if (OB_UNLIKELY(line.body_.empty())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("[BATCH-FILE] provider produced empty body", K(ret));
              } else {
                ret = writer_.append(line);
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  if (OB_FAIL(finish_and_submit_current_task_())) {
                    LOG_WARN("failed to commit writer on threshold", K(ret));
                  } else if (OB_FAIL(open_new_task_())) {
                    LOG_WARN("failed to re-open batch task", K(ret));
                  } else {
                    need_retry = true;
                    LOG_DEBUG("[BATCH-FILE] committed full writer, re-opened new one",
                              K_(ddl_task_id));
                  }
                } else if (OB_FAIL(ret)) {
                  LOG_WARN("failed to append line to writer", K(ret));
                } else {
                  if (OB_FAIL(current_batch_info_->add_item(text, extras, false))) {
                    LOG_WARN("failed to add item to batch_info", K(ret));
                  } else {
                    current_task_row_count_++;
                    total_rows_collected_++;
                    if (current_task_row_count_ >= DEFAULT_SLICE_SIZE) {
                      if (OB_FAIL(finish_and_submit_current_task_())) {
                        LOG_WARN("failed to finish and submit current task", K(ret));
                      } else if (OB_FAIL(open_new_task_())) {
                        LOG_WARN("failed to re-open batch task after row-count submission",
                                 K(ret));
                      }
                    }
                  }
                }
              }
            } while (OB_SUCC(ret) && need_retry);
          }
        }
      }
    }

    LOG_INFO("[BATCH-FILE] collect_data_to_service_ done",
             K_(total_rows_collected), K_(all_data_collected), K_(current_task_row_count));
  }

  return ret;
}

int ObBatchFileEmbeddingOperator::open_new_task_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(service_->open_batch_task(*endpoint_info_, share::OB_AI_COMMAND_EMBED,
                                         ddl_task_id_, writer_,
                                         allow_null_on_failure_))) {
    LOG_WARN("failed to open batch task", K(ret));
  } else if (OB_FAIL(ensure_batch_info_())) {
    LOG_WARN("failed to ensure batch_info in open_new_task_", K(ret));
  } else {
    current_task_row_count_ = 0;
  }
  return ret;
}

int ObBatchFileEmbeddingOperator::ensure_batch_info_()
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(current_batch_info_)) {
    void *bi_buf = ob_malloc(sizeof(ObTaskBatchInfo), ObMemAttr(MTL_ID(), "BatchFileBatch"));
    if (OB_ISNULL(bi_buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to allocate batch_info", K(ret));
    } else {
      current_batch_info_ = new (bi_buf) ObTaskBatchInfo();
      if (OB_FAIL(current_batch_info_->init(DEFAULT_SLICE_SIZE, vec_dim_))) {
        LOG_WARN("failed to init batch_info", K(ret));
        current_batch_info_->~ObTaskBatchInfo();
        ob_free(bi_buf);
        current_batch_info_ = nullptr;
      }
    }
  }
  return ret;
}

void ObBatchFileEmbeddingOperator::destroy_current_batch_info_()
{
  if (OB_NOT_NULL(current_batch_info_)) {
    current_batch_info_->~ObTaskBatchInfo();
    ob_free(current_batch_info_);
    current_batch_info_ = nullptr;
  }
}

void ObBatchFileEmbeddingOperator::reset_current_task_state_()
{
  current_task_row_count_ = 0;
  writer_.reset();
}

void ObBatchFileEmbeddingOperator::mark_slot_failed_best_effort_(int64_t slot_idx,
                                                                 int error_code,
                                                                 const char *reason)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(slot_ring_.mark_slot_failed(slot_idx, error_code))) {
    LOG_WARN("failed to mark slot failed", K(ret), K(slot_idx), K(error_code), "reason", reason);
  }
}

bool ObBatchFileEmbeddingOperator::is_query_task_status_retryable_(int ret) const
{
  return OB_TIMEOUT == ret
      || OB_TRANS_TIMEOUT == ret
      || OB_CONNECT_ERROR == ret
      || OB_RPC_POST_ERROR == ret
      || OB_EAGAIN == ret;
}

int ObBatchFileEmbeddingOperator::submit_skip_only_batch_()
{
  int ret = OB_SUCCESS;
  int64_t slot_idx = -1;

  // Skip-only batch: batch_info has SKIP_EMBEDDING rows but no valid rows for the API.
  // Reserve a slot and mark it immediately ready so concat_rows still sees one output row per input row.
  if (OB_FAIL(slot_ring_.reserve_slot(slot_idx))) {
    LOG_WARN("failed to reserve slot for skip-only batch", K(ret), K(slot_idx));
    destroy_current_batch_info_();
    reset_current_task_state_();
  } else {
    if (slot_idx >= slot_contexts_.count()) {
      SlotContext ctx;
      ctx.reset();
      if (OB_FAIL(slot_contexts_.push_back(ctx))) {
        LOG_WARN("failed to expand slot_contexts_ for skip-only batch", K(ret), K(slot_idx));
        mark_slot_failed_best_effort_(slot_idx, ret, "skip-only slot context expansion");
        destroy_current_batch_info_();
        reset_current_task_state_();
      }
    }
    if (OB_SUCC(ret)) {
      SlotContext &ctx = slot_contexts_.at(slot_idx);
      ctx.task_id_.reset();   // empty: no API task was submitted
      ctx.row_count_ = 0;     // 0 valid rows for get_next_results
      ctx.batch_info_ = current_batch_info_;
      if (OB_FAIL(slot_ring_.mark_slot_directly_ready(slot_idx))) {
        LOG_WARN("mark_slot_directly_ready failed for skip-only batch", K(ret), K(slot_idx));
        ctx.batch_info_ = nullptr;  // retain ownership in current_batch_info_
        mark_slot_failed_best_effort_(slot_idx, ret, "skip-only directly-ready");
        destroy_current_batch_info_();
        reset_current_task_state_();
      } else {
        LOG_INFO("[BATCH-FILE] skip-only batch submitted as immediately-ready slot",
                 K(slot_idx), "skip_count", current_batch_info_->get_count());
        current_batch_info_ = nullptr;
        reset_current_task_state_();
      }
    }
  }
  return ret;
}

int ObBatchFileEmbeddingOperator::submit_api_batch_()
{
  int ret = OB_SUCCESS;
  int64_t slot_idx = -1;
  common::ObString task_id;
  common::ObString task_id_copy;

  LOG_INFO("[BATCH-FILE] finishing and submitting task", K_(current_task_row_count));

  if (OB_FAIL(slot_ring_.reserve_slot(slot_idx))) {
    LOG_WARN("failed to reserve slot", K(ret));
    destroy_current_batch_info_();
    reset_current_task_state_();
  } else {
    if (OB_FAIL(writer_.commit(task_id))) {
      LOG_WARN("failed to commit writer", K(ret));
      mark_slot_failed_best_effort_(slot_idx, ret, "writer commit");
    } else if (OB_FAIL(ob_write_string(allocator_, task_id, task_id_copy))) {
      LOG_WARN("failed to deep copy task_id", K(ret));
      mark_slot_failed_best_effort_(slot_idx, ret, "task id copy");
    }
#ifdef ERRSIM
    if (OB_SUCC(ret)) {
      ret = OB_E(common::EventTable::EN_BATCH_FILE_OP_SUBMIT_TASK_ERR) OB_SUCCESS;
      if (OB_FAIL(ret)) {
        LOG_WARN("[ERRSIM] fail to submit task", KR(ret), K(task_id_copy), K(slot_idx));
        mark_slot_failed_best_effort_(slot_idx, ret, "submit task errsim");
      }
    }
#endif
    if (OB_SUCC(ret)) {
      if (slot_idx >= slot_contexts_.count()) {
        SlotContext ctx;
        ctx.reset();
        if (OB_FAIL(slot_contexts_.push_back(ctx))) {
          LOG_WARN("failed to expand slot_contexts_", K(ret), K(slot_idx));
          mark_slot_failed_best_effort_(slot_idx, ret, "slot context expansion");
        }
      }
      if (OB_SUCC(ret)) {
        SlotContext &ctx = slot_contexts_.at(slot_idx);
        ctx.task_id_ = task_id_copy;  // deep copy, backed by allocator_
        ctx.row_count_ = current_task_row_count_;
        ctx.batch_info_ = current_batch_info_;  // Transfer ownership to slot

        if (OB_FAIL(slot_ring_.mark_slot_submitted(slot_idx, task_id_copy))) {
          LOG_WARN("mark_slot_submitted failed", K(ret), K(slot_idx), K(task_id_copy));
          ctx.batch_info_ = nullptr;  // retain ownership in current_batch_info_
          mark_slot_failed_best_effort_(slot_idx, ret, "mark slot submitted");
        } else {
          LOG_INFO("[BATCH-FILE] submitted task",
                   K(task_id_copy), K(slot_idx), K_(current_task_row_count));
        }
      }
    }

    if (OB_SUCC(ret)) {
      current_batch_info_ = nullptr;  // Ownership transferred to slot
      reset_current_task_state_();
    } else {
      const common::ObString submitted_task_id = task_id_copy.empty() ? task_id : task_id_copy;
      if (OB_NOT_NULL(service_) && !submitted_task_id.empty()) {
        int cancel_ret = service_->abandon_task(submitted_task_id);
        if (OB_SUCCESS != cancel_ret) {
          LOG_WARN("[BATCH-FILE] failed to abandon task after submit bookkeeping failure",
                   K(cancel_ret), K(submitted_task_id), K(slot_idx));
        }
      }
      destroy_current_batch_info_();
      reset_current_task_state_();
    }
  }
  return ret;
}

int ObBatchFileEmbeddingOperator::finish_and_submit_current_task_()
{
  int ret = OB_SUCCESS;

  if (OB_NOT_NULL(current_batch_info_) && current_batch_info_->get_count() > 0
      && (!writer_.is_inited() || writer_.is_empty())) {
    if (OB_FAIL(submit_skip_only_batch_())) {
      LOG_WARN("failed to submit skip-only batch", K(ret));
    }
  } else if (!writer_.is_inited() || writer_.is_empty()) {
    LOG_DEBUG("no current task to submit");
  } else if (OB_ISNULL(service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service_ is null", K(ret));
  } else if (OB_FAIL(submit_api_batch_())) {
    LOG_WARN("failed to submit api batch", K(ret));
  }
  return ret;
}


int ObBatchFileEmbeddingOperator::poll_submitted_slots_()
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("service is null", K(ret));
  } else {
    // Iterate over all slots from head to next, check SUBMITTED ones
    const int64_t head = slot_ring_.get_head_idx();
    const int64_t next = slot_ring_.get_next_idx();

    for (int64_t idx = head; OB_SUCC(ret) && idx < next; ++idx) {
      if (need_stop_()) {
        cancel_inflight_tasks_();
        ret = OB_CANCELED;
        LOG_WARN("[BATCH-FILE] dag stopped during poll, exiting", K(ret), K_(tablet_id));
        break;
      }
      ObBatchFileSlotRing::SlotStatus status;
      if (OB_FAIL(slot_ring_.get_slot_status(idx, status))) {
        LOG_WARN("get_slot_status failed", K(ret), K(idx));
      } else if (ObBatchFileSlotRing::SLOT_SUBMITTED == status) {
        // Query task status
        common::ObString task_id;
        if (OB_FAIL(slot_ring_.get_slot_task_id(idx, task_id))) {
          LOG_WARN("get_slot_task_id failed", K(ret), K(idx));
        } else {
          share::ObAiTaskInfo task_info;
          if (OB_FAIL(service_->query_task_status(task_id, task_info))) {
            const int query_ret = ret;
            if (is_query_task_status_retryable_(query_ret)) {
              LOG_WARN("[BATCH-FILE] query_task_status failed, will retry",
                       K(query_ret), K(task_id), K(idx), K_(tablet_id));
              // The task might still be processing; retry transient status-query failures.
              ret = OB_SUCCESS;
            } else {
              LOG_WARN("[BATCH-FILE] query_task_status failed permanently",
                       K(query_ret), K(task_id), K(idx), K_(tablet_id));
              mark_slot_failed_best_effort_(idx, query_ret, "query task status");
              // Slot is already marked failed; continue processing remaining slots.
              ret = OB_SUCCESS;
            }
          } else if (task_info.status_ == share::OB_AI_TASK_STATUS_FINISHED) {
            LOG_INFO("[BATCH-FILE] task FINISHED",
                     K(idx), K(task_id), K_(tablet_id));
#ifdef ERRSIM
            ret = OB_E(common::EventTable::EN_BATCH_FILE_OP_POLL_SLOT_ERR) OB_SUCCESS;
            if (OB_FAIL(ret)) {
              LOG_WARN("[ERRSIM] fail to poll submitted slot", KR(ret), K(idx), K(task_id));
            }
#endif
            // Task completed - just mark slot as ready
            if (OB_FAIL(ret)) {
            } else if (OB_FAIL(slot_ring_.mark_slot_ready(idx))) {
              LOG_WARN("mark_slot_ready failed", K(ret), K(idx));
            } else {
              LOG_DEBUG("[BATCH-FILE] slot ready", K(idx), K(task_id));
            }
          } else if (task_info.status_ == share::OB_AI_TASK_STATUS_FAILED) {
            int err = OB_ERR_UNEXPECTED;
            const char *msg = "task failed";
            char msg_buf[OB_AI_MAX_ERROR_MESSAGE_LENGTH];
            if (!task_info.error_detail_.empty()) {
              common::ObArenaAllocator json_alloc("BFPolJson", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
              common::ObJsonNode *root = nullptr;
              const int parse_ret = common::ObJsonParser::get_tree(&json_alloc,
                                                                   task_info.error_detail_,
                                                                   root);
              if (OB_SUCCESS != parse_ret || OB_ISNULL(root)) {
                LOG_WARN("[BATCH-FILE] failed to parse error_detail json",
                         K(parse_ret), K(task_info.error_detail_));
              } else if (root->json_type() == common::ObJsonNodeType::J_OBJECT) {
                common::ObJsonObject *obj = static_cast<common::ObJsonObject *>(root);
                common::ObJsonNode *code_node = obj->get_value("ob_error_code");
                if (OB_NOT_NULL(code_node)
                    && code_node->json_type() == common::ObJsonNodeType::J_INT) {
                  err = static_cast<int>(static_cast<common::ObJsonInt *>(code_node)->value());
                }
                common::ObJsonNode *msg_node = obj->get_value("message");
                if (OB_NOT_NULL(msg_node)
                    && msg_node->json_type() == common::ObJsonNodeType::J_STRING) {
                  const common::ObString &s = static_cast<common::ObJsonString *>(msg_node)->value();
                  const int64_t len = s.length();
                  const int64_t cap = static_cast<int64_t>(sizeof(msg_buf) - 1);
                  const int64_t copy_len = (len < cap) ? len : cap;
                  if (copy_len > 0) {
                    MEMCPY(msg_buf, s.ptr(), copy_len);
                    msg_buf[copy_len] = '\0';
                    msg = msg_buf;
                  }
                }
              }
            }
            LOG_WARN("[BATCH-FILE] task FAILED", K(idx), K(task_id), K(err),
                     K(task_info.error_detail_), K_(tablet_id));
            mark_slot_failed_best_effort_(idx, err, msg);
          } else if (task_info.status_ == share::OB_AI_TASK_STATUS_CANCELLED) {
            LOG_WARN("[BATCH-FILE] task CANCELLED", K(idx), K(task_id), K_(tablet_id));
            mark_slot_failed_best_effort_(idx, OB_CANCELED, "task cancelled");
          } else {
            // Task still running, will retry on next poll cycle
            LOG_DEBUG("[BATCH-FILE] task still pending",
                      K(idx), K(task_id), "task_status", task_info.status_, K_(tablet_id));
          }
        }
      }
    }
  }

  return ret;
}

int ObBatchFileEmbeddingOperator::check_and_output_result_(ObChunk &output_chunk,
                                                            ResultState &result_state)
{
  int ret = OB_SUCCESS;
  int error_code = OB_SUCCESS;

  int64_t target_slot_idx = -1;
  if (slot_ring_.head_is_ready(error_code)) {
    target_slot_idx = slot_ring_.get_head_idx();
  } else if (OB_SUCCESS != error_code) {
    ret = error_code;
    LOG_WARN("head slot failed", K(ret));
  } else {
    // Non-head slot failure: abort early instead of waiting for end_chunk drain.
    // This prevents creating more tasks for subsequent chunks when previous ones
    // have already failed, avoiding an explosion of duplicate AI tasks.
    int any_error_code = OB_SUCCESS;
    if (slot_ring_.has_any_failed(any_error_code)) {
      ret = any_error_code;
      LOG_WARN("non-head slot failed, aborting early to prevent duplicate tasks",
               K(ret), K_(slot_ring));
    }
  }

  if (OB_SUCC(ret) && target_slot_idx != -1) {
    if (OB_UNLIKELY(target_slot_idx >= slot_contexts_.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("slot_contexts_ index out of range", K(ret), K(target_slot_idx),
               "count", slot_contexts_.count());
    } else {
      SlotContext &ctx = slot_contexts_.at(target_slot_idx);
      common::ObString task_id = ctx.task_id_;
      ObTaskBatchInfo *batch_info = ctx.batch_info_;

      if (OB_ISNULL(batch_info)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("batch_info in slot context is null", K(ret), K(target_slot_idx));
      } else {
        // Get all embedding results from Service for this task.
        // Skip-only batches have no API task (task_id is empty) — embedding_results stays empty.
        const int64_t total_count = ctx.row_count_;
        common::ObSEArray<share::ObAiResultRow, 1024> embedding_results;
        bool has_more = false;
        if (!task_id.empty() && total_count > 0) {
          if (OB_FAIL(service_->get_next_results(task_id, total_count,
                                                   embedding_results, has_more))) {
            LOG_WARN("failed to get result rows from service", K(ret), K(task_id));
          } else if (has_more) {
            LOG_WARN("[BATCH-FILE] has_more=true from get_next_results; results may be incomplete",
                     K(task_id), K(total_count), "got", embedding_results.count());
          }
        }
#ifdef ERRSIM
        if (OB_SUCC(ret)) {
          ret = OB_E(common::EventTable::EN_BATCH_FILE_OP_OUTPUT_RESULT_ERR) OB_SUCCESS;
          if (OB_FAIL(ret)) {
            LOG_WARN("[ERRSIM] fail to output result", KR(ret), K(task_id));
          }
        }
#endif
        if (OB_SUCC(ret)) {
          // Fill embedding vectors into pre-existing results (which already have extras from add_item)
          common::ObArray<ObEmbeddingResult*> &results = batch_info->get_results();
          int64_t success_count = 0;

          for (int64_t i = 0; OB_SUCC(ret) && i < embedding_results.count(); ++i) {
            const share::ObAiResultRow &row = embedding_results.at(i);
            const int64_t idx = row.original_index_;
            if (idx < 0 || idx >= results.count()) {
              LOG_WARN("original_index out of range", K(idx), "results_count", results.count());
              continue;
            }
            ObEmbeddingResult *result = results.at(idx);
            if (OB_ISNULL(result)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("null result at index", K(ret), K(idx));
            } else if (OB_SUCCESS != row.ret_code_) {
              ret = row.ret_code_;
              LOG_WARN("embedding row failed in batch result", K(ret), K(i), K(idx),
                       K(row.error_detail_));
            } else if (OB_ISNULL(row.embedding_vector_)) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("embedding vector is null", K(ret), K(i), K(idx));
            } else {
              // Allocate vector buffer and copy embedding
              float *vec_buf = static_cast<float*>(
                  batch_info->get_allocator().alloc(vec_dim_ * sizeof(float)));
              if (OB_ISNULL(vec_buf)) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to allocate vector buffer", K(ret), K(idx), K_(vec_dim));
              } else {
                MEMCPY(vec_buf, row.embedding_vector_, vec_dim_ * sizeof(float));
                result->set_vector(vec_buf, vec_dim_);
                result->set_status(ObEmbeddingResult::NEED_EMBEDDING);
                success_count++;
              }
            }
          }

          if (OB_SUCC(ret) && allow_null_on_failure_
              && success_count < batch_info->get_need_embedding_count()) {
            // Degraded-finish or partial result: API returned fewer successful
            // embeddings than expected.  Rows that were NOT filled by the fill loop
            // have null vectors (cleared above).  Demote them to SKIP_EMBEDDING so
            // downstream outputs NULL instead of erroring.
            for (int64_t i = 0; i < results.count(); ++i) {
              ObEmbeddingResult *result = results.at(i);
              if (OB_NOT_NULL(result) && result->need_embedding()
                  && OB_ISNULL(result->get_vector())) {
                result->set_status(ObEmbeddingResult::SKIP_EMBEDDING);
              }
            }
          } else if (OB_SUCC(ret) && success_count < batch_info->get_need_embedding_count()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("embedding results count mismatch", K(ret),
                     K(success_count), "expected", batch_info->get_need_embedding_count());
          }

          if (OB_SUCC(ret)) {
            // batch_info already has correct count from add_item; update need_embedding_count
            batch_info->set_filled(batch_info->get_count(), success_count);

            // Transfer batch_info ownership to output_chunk
            output_chunk.type_ = ObChunk::TASK_BATCH_INFO;
            output_chunk.batch_info_ = batch_info;
            ctx.batch_info_ = nullptr;  // Ownership transferred
            result_state = ObPipelineOperator::HAVE_MORE_OUTPUT;

            // Pop slot
            if (OB_FAIL(slot_ring_.pop_head())) {
              LOG_WARN("pop_head failed", K(ret));
            } else {
              // Notify service that DDL has consumed this task's results.
              // release_task archives to history table and destroys the task object.
              if (OB_NOT_NULL(service_)) {
                int tmp_ret = service_->release_task(task_id);
                if (OB_SUCCESS != tmp_ret) {
                  // Non-fatal: slot already consumed by pop_head, ctx.reset() must execute.
                  LOG_WARN("[BATCH-FILE] release_task failed after pop_head",
                           K(tmp_ret), K(task_id));
                }
              }
              ctx.reset();
              LOG_INFO("[BATCH-FILE] output result for slot",
                       "result_count", batch_info->get_count(), K(success_count),
                       K(task_id), K_(slot_ring));
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObBatchFileEmbeddingOperator::handle_end_chunk_(ObChunk &output_chunk,
                                                      ResultState &result_state)
{
  int ret = OB_SUCCESS;

  // Submit remaining data if any (including skip-only batches with no writer)
  if (!all_tasks_submitted_) {
    const bool has_pending = (writer_.is_inited() && !writer_.is_empty())
                             || (OB_NOT_NULL(current_batch_info_)
                                 && current_batch_info_->get_count() > 0);
    if (has_pending) {
      if (OB_FAIL(finish_and_submit_current_task_())) {
        LOG_WARN("finish_and_submit_current_task_ failed on end_chunk", K(ret));
      }
    }
    if (OB_SUCC(ret)) {
      all_tasks_submitted_ = true;
      LOG_INFO("[BATCH-FILE] all tasks submitted", K_(total_rows_collected), K_(slot_ring));
    }
  }

  // Drain the slot ring: poll, check, yield in a loop
  if (OB_SUCC(ret)) {
    // Record drain start time on first entry
    if (0 == drain_start_ts_) {
      drain_start_ts_ = common::ObTimeUtility::current_time();
      drain_poll_count_ = 0;
      LOG_INFO("[BATCH-FILE] drain loop started", K_(slot_ring), K_(tablet_id));
    }

    while (OB_SUCC(ret) && !slot_ring_.is_empty()) {
      ++drain_poll_count_;
      const int64_t elapsed_us = common::ObTimeUtility::current_time() - drain_start_ts_;

      // Periodic progress logging (WARN level so it's always visible)
      if (drain_poll_count_ % DRAIN_LOG_INTERVAL == 0) {
        LOG_WARN("[BATCH-FILE] drain loop still waiting for tasks",
                 K_(slot_ring), K_(tablet_id), K_(drain_poll_count),
                 "elapsed_sec", elapsed_us / 1000000);
      }

      if (OB_FAIL(poll_submitted_slots_())) {
        LOG_WARN("poll_submitted_slots_ failed", K(ret));
      } else {
        int error_code = OB_SUCCESS;
        if (slot_ring_.head_is_ready(error_code)) {
          // Output this result using check_and_output_result_
          if (OB_FAIL(check_and_output_result_(output_chunk, result_state))) {
            LOG_WARN("check_and_output_result_ failed", K(ret));
          } else if (ObPipelineOperator::HAVE_MORE_OUTPUT == result_state) {
            return ret; // Return this batch; pipeline will call us again
          }
        } else if (OB_SUCCESS != error_code) {
          ret = error_code;
          LOG_WARN("head slot failed during end_chunk drain", K(ret));
        } else if (need_stop_()) {
          cancel_inflight_tasks_();
          ret = OB_CANCELED;
          LOG_WARN("dag stopped during end_chunk drain", K(ret));
        } else {
          // Check if any non-head slot has failed (early abort)
          int any_error_code = OB_SUCCESS;
          if (slot_ring_.has_any_failed(any_error_code)) {
            cancel_inflight_tasks_();
            ret = any_error_code;
            LOG_WARN("non-head slot failed during end_chunk drain", K(ret));
          } else {
            // Still waiting, yield to allow other tasks to run
            if (OB_FAIL(do_yield_())) {
              LOG_WARN("do_yield_ failed", K(ret));
            }
          }
        }
      }
    }
  }

  // All results drained, send ITER_END
  if (OB_SUCC(ret) && slot_ring_.is_empty()) {
    if (!end_chunk_sent_) {
      output_chunk.type_ = ObChunk::ITER_END_TYPE;
      result_state = ObPipelineOperator::HAVE_MORE_OUTPUT;
      end_chunk_sent_ = true;
      LOG_INFO("[BATCH-FILE] all results drained, sending ITER_END_TYPE");
    } else {
      result_state = ObPipelineOperator::NEED_MORE_INPUT;
      LOG_DEBUG("[BATCH-FILE] ITER_END already sent");
    }
  }

  return ret;
}

int ObBatchFileEmbeddingOperator::do_yield_()
{
  int ret = OB_SUCCESS;
  ob_usleep(CHECK_INTERVAL_US);
  if (OB_FAIL(share::dag_yield())) {
    if (OB_CANCELED == ret) {
      LOG_WARN("dag yield cancelled", K(ret));
    } else {
      LOG_WARN("dag yield failed", K(ret));
    }
  }
  return ret;
}

int ObBatchFileEmbeddingOperator::get_next_row_from_tmp_files_(
    common::ObArray<ObCGRowFile *> *cg_row_file_arr,
    blocksstable::ObStorageDatum &text,
    common::ObArray<blocksstable::ObStorageDatum> &extras,
    bool &has_row)
{
  int ret = OB_SUCCESS;
  has_row = false;

  LOG_DEBUG("[BATCH-FILE] get_next_row_from_tmp_files_ start",
            K(cur_file_idx_), "file_count", cg_row_file_arr ? cg_row_file_arr->count() : -1);

  if (OB_ISNULL(cg_row_file_arr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid cg_row_file_arr", K(ret));
  } else {
    while (OB_SUCC(ret) && cur_file_idx_ < cg_row_file_arr->count() && !has_row) {
      ObCGRowFile *&row_file = cg_row_file_arr->at(cur_file_idx_);
      LOG_DEBUG("[BATCH-FILE] checking row_file", K(cur_file_idx_), KP(row_file));
      if (OB_ISNULL(row_file)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row file null", K(ret), K(cur_file_idx_));
      }

      while (OB_SUCC(ret) && !has_row) {
        if (OB_FAIL(get_next_batch_from_tmp_files_(row_file))) {
          LOG_WARN("get next batch failed", K(ret));
        } else if (OB_ISNULL(cur_datum_rows_)) {
          // Current file end, switch to next file
          cur_file_idx_++;
          break;
        } else {
          const int64_t total_row_count = cur_datum_rows_->row_count_;
          const int64_t total_column_count = cur_datum_rows_->get_column_count();

          if (total_column_count <= text_col_idx_) {
            ret = OB_INVALID_ARGUMENT;
            LOG_WARN("column index out of range", K(ret), K(total_column_count), K(text_col_idx_));
          } else {
            while (OB_SUCC(ret) && !has_row && cur_row_in_batch_ < total_row_count) {
              blocksstable::ObDatumRow current_row;
              if (OB_FAIL(current_row.init(cur_datum_rows_->get_column_count()))) {
                LOG_WARN("init datum row failed", K(ret));
              } else if (OB_FAIL(cur_datum_rows_->to_datum_row(cur_row_in_batch_, current_row))) {
                LOG_WARN("to_datum_row failed", K(ret), K(cur_row_in_batch_));
              } else if (OB_FAIL(parse_row_(current_row, text, extras))) {
                LOG_WARN("parse row failed", K(ret));
              } else {
                cur_row_in_batch_++;
                has_row = true;
              }
            }

            if (OB_SUCC(ret) && !has_row) {
              // Current batch finished, reset to fetch next batch
              cur_datum_rows_ = nullptr;
              cur_row_in_batch_ = 0;
            }
          }
        }
      }
    }
  }

  return ret;
}

int ObBatchFileEmbeddingOperator::get_next_batch_from_tmp_files_(ObCGRowFile *&row_file)
{
  int ret = OB_SUCCESS;

  LOG_DEBUG("[BATCH-FILE] get_next_batch_from_tmp_files_ start", KP(row_file), KP(cur_datum_rows_));

  if (nullptr == cur_datum_rows_) {
    if (OB_FAIL(row_file->get_next_batch(cur_datum_rows_))) {
      if (OB_ITER_END == ret) {
        LOG_INFO("[BATCH-FILE] get_next_batch returned ITER_END", K(cur_file_idx_));
        ret = OB_SUCCESS;
        row_file->~ObCGRowFile();
        ob_free(row_file);
        row_file = nullptr;
        cur_datum_rows_ = nullptr;
        cur_row_in_batch_ = 0;
      } else {
        LOG_WARN("get next batch failed", K(ret));
      }
    } else {
      cur_row_in_batch_ = 0;
      LOG_DEBUG("[BATCH-FILE] get_next_batch success",
                "row_count", cur_datum_rows_->row_count_,
                "col_count", cur_datum_rows_->get_column_count());
    }
  }

  return ret;
}

int ObBatchFileEmbeddingOperator::parse_row_(const blocksstable::ObDatumRow &current_row,
                                              blocksstable::ObStorageDatum &text,
                                              common::ObArray<blocksstable::ObStorageDatum> &extras)
{
  int ret = OB_SUCCESS;
  text.reset();
  extras.reset();

  if (OB_UNLIKELY(current_row.get_column_count() <= text_col_idx_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid datum row", K(ret), K(current_row), K(text_col_idx_));
  } else {
    // Use shallow copy like ObHNSWEmbeddingOperator::parse_row
    // Deep copy happens in add_item() when saving to batch_info_
    const blocksstable::ObStorageDatum &chunk_cell = current_row.storage_datums_[text_col_idx_];
    text.shallow_copy_from_datum(chunk_cell);

    for (int64_t i = 0; OB_SUCC(ret) && i < extra_column_idxs_.count(); ++i) {
      int32_t col_idx = extra_column_idxs_.at(i);
      if (col_idx < 0 || col_idx >= current_row.get_column_count()) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("extra column index out of range", K(ret), K(col_idx), K(current_row.get_column_count()));
      } else if (OB_FAIL(extras.push_back(current_row.storage_datums_[col_idx]))) {
        LOG_WARN("push extra datum failed", K(ret), K(col_idx));
      }
    }
  }

  return ret;
}

bool ObBatchFileEmbeddingOperator::need_stop_()
{
  bool bret = false;
  ObIDag *dag = get_dag();
  if (OB_ISNULL(dag)) {
    bret = true;
  } else if (dag->is_final_status()) {
    bret = true;
  }
  return bret;
}

void ObBatchFileEmbeddingOperator::cancel_inflight_tasks_()
{
  if (OB_ISNULL(service_)) {
    return;
  }
  for (int64_t i = 0; i < slot_contexts_.count(); ++i) {
    const common::ObString &task_id = slot_contexts_.at(i).task_id_;
    if (!task_id.empty()) {
      int tmp_ret = service_->abandon_task(task_id);
      LOG_INFO("[BATCH-FILE] abandon inflight task on DDL exit",
               K(task_id), K(tmp_ret), K_(tablet_id), K_(ddl_task_id));
    }
  }
}

// -------------------------------- ObHNSWEmbeddingAppendAndWritePipeline --------------------------------
ObHNSWEmbeddingAppendAndWritePipeline::ObHNSWEmbeddingAppendAndWritePipeline()
  : ObDDLWriteMacroBlockBasePipeline(TASK_TYPE_DDL_VECTOR_INDEX_APPEND_PIPELINE),
    cg_row_file_writer_op_(nullptr), embedding_op_(nullptr),
    ai_execution_mode_(VIAM_SYNC_HTTP), embedding_write_op_(this)
{}

ObHNSWEmbeddingAppendAndWritePipeline::~ObHNSWEmbeddingAppendAndWritePipeline()
{
  if (OB_NOT_NULL(cg_row_file_writer_op_)) {
    cg_row_file_writer_op_->~ObCGRowFileWriterOp();
    ob_free(cg_row_file_writer_op_);
    cg_row_file_writer_op_ = nullptr;
  }
  if (OB_NOT_NULL(embedding_op_)) {
    if (VIAM_BATCH_FILE == ai_execution_mode_) {
      static_cast<ObBatchFileEmbeddingOperator *>(embedding_op_)->~ObBatchFileEmbeddingOperator();
    } else {
      static_cast<ObHNSWEmbeddingOperator *>(embedding_op_)->~ObHNSWEmbeddingOperator();
    }
    ob_free(embedding_op_);
    embedding_op_ = nullptr;
  }
}

int ObHNSWEmbeddingAppendAndWritePipeline::init(ObDDLSlice *ddl_slice)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_slice || !ddl_slice->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), KPC(ddl_slice));
  } else {
    ddl_slice_ = ddl_slice;
    const int64_t MAX_BATCH_SIZE = 1024;
    const ObTabletID &tablet_id = ddl_slice->get_tablet_id();
    const int64_t slice_idx = ddl_slice->get_slice_idx();

    // Read ai_execution_mode from tablet context
    ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(get_dag());
    ObDDLTabletContext *tablet_context = nullptr;
    ObVectorIndexTabletContext *vector_index_ctx = nullptr;
    if (OB_ISNULL(dag)) {
      ret = OB_ERR_SYS;
      LOG_WARN("get dag failed", K(ret));
    } else if (OB_FAIL(dag->get_tablet_context(tablet_id, tablet_context))) {
      LOG_WARN("get tablet context failed", K(ret), K(tablet_id));
    } else if (OB_ISNULL(vector_index_ctx = tablet_context->vector_index_ctx_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("vector index context is null", K(ret));
    } else {
      const ObString &vec_idx_param = vector_index_ctx->vec_idx_param_;
      if (!vec_idx_param.empty()) {
        ObVectorIndexParam param;
        if (OB_FAIL(ObVectorIndexUtil::parser_params_from_string(
            vec_idx_param, ObVectorIndexType::VIT_HNSW_INDEX, param, false))) {
          LOG_WARN("fail to parse vector index param", K(ret), K(vec_idx_param));
        } else {
          ai_execution_mode_ = param.ai_execution_mode_;
        }
      }
    }

    // Allocate embedding operator based on ai_execution_mode
    // The upstream DDL scan always produces CG_ROW_TMP_FILES chunks for all access modes.
    if (OB_SUCC(ret)) {
      if (VIAM_BATCH_FILE == ai_execution_mode_) {
        void *buf = ob_malloc(sizeof(ObBatchFileEmbeddingOperator), ObMemAttr(MTL_ID(), "BatchFileEmbed"));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc BatchFile embedding operator failed", K(ret));
        } else {
          embedding_op_ = new (buf) ObBatchFileEmbeddingOperator(this);
          if (OB_FAIL(embedding_op_->init(tablet_id))) {
            LOG_WARN("init BatchFile embedding operator failed", K(ret), K(tablet_id));
          }
        }
      } else {
        void *buf = ob_malloc(sizeof(ObHNSWEmbeddingOperator), ObMemAttr(MTL_ID(), "HNSWEmbed"));
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("alloc HNSW embedding operator failed", K(ret));
        } else {
          embedding_op_ = new (buf) ObHNSWEmbeddingOperator(this);
          if (OB_FAIL(embedding_op_->init(tablet_id))) {
            LOG_WARN("init HNSW embedding operator failed", K(ret), K(tablet_id));
          }
        }
      }
    }

    // Add ops and init write operator (both modes)
    if (OB_SUCC(ret)) {
      if (OB_FAIL(add_op(embedding_op_))) {
        LOG_WARN("add embedding op failed", K(ret));
      } else if (OB_FAIL(embedding_write_op_.init(tablet_id, slice_idx))) {
        LOG_WARN("init embedding write operator failed", K(ret));
      } else if (OB_FAIL(add_op(&embedding_write_op_))) {
        LOG_WARN("add embedding write op failed", K(ret));
      } else {
        LOG_INFO("ObHNSWEmbeddingAppendAndWritePipeline initialized",
                 K(tablet_id), K(slice_idx), K_(ai_execution_mode));
      }
    }
  }
  return ret;
}
