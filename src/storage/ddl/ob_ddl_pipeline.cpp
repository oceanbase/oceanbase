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

#define USING_LOG_PREFIX STORAGE
#include "ob_ddl_pipeline.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "storage/ddl/ob_direct_load_struct.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/expr/ob_array_expr_utils.h"
#include "sql/engine/expr/ob_expr_ai/ob_ai_func_utils.h"

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
    ctx_.lob_meta_tablet_id_ = ddl_data.lob_meta_tablet_id_;
    ctx_.lob_piece_tablet_id_ = ddl_data.lob_piece_tablet_id_;
  }
  // get vid col and vector col
  for (int64_t i = 0; OB_SUCC(ret) && i < col_array.count(); i++) {
    // version control col is not valid
    if (!col_array.at(i).is_valid_) {
    } else if (ObSchemaUtils::is_vec_hnsw_vid_column(col_array.at(i).column_flags_) ||
      col_desc_array.at(i).col_id_ == OB_HIDDEN_PK_INCREMENT_COLUMN_ID) {
      if (vector_vid_col_idx_ == -1) {
        vector_vid_col_idx_ = i;
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get valid vector index col idx", K(ret), K(vector_vid_col_idx_), K(i), K(col_array));
      }
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
      } else {
        adapter_ = tmp_info->adapter_;
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
    int64_t key_pos = 0;
    row_allocator_.reuse();
    char *key_str = static_cast<char*>(row_allocator_.alloc(OB_VEC_IDX_SNAPSHOT_KEY_LENGTH));
    if (OB_ISNULL(key_str)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to alloc vec key", K(ret));
    } else if (index_type_ == VIAT_HNSW && OB_FAIL(databuff_printf(key_str, OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hnsw_data_part%05ld", tablet_id_.id(), snapshot_version_, cur_row_pos_))) {
      LOG_WARN("fail to build vec snapshot key str", K(ret), K_(index_type));
    } else if (index_type_ == VIAT_HGRAPH &&
      OB_FAIL(databuff_printf(key_str, OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hgraph_data_part%05ld", tablet_id_.id(), snapshot_version_, cur_row_pos_))) {
      LOG_WARN("fail to build vec hgraph snapshot key str", K(ret), K_(index_type));
    } else if (index_type_ == VIAT_HNSW_SQ && OB_FAIL(databuff_printf(key_str, OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hnsw_sq_data_part%05ld", tablet_id_.id(), snapshot_version_, cur_row_pos_))) {
      LOG_WARN("fail to build sq vec snapshot key str", K(ret), K_(index_type));
    } else if (index_type_ == VIAT_HNSW_BQ && OB_FAIL(databuff_printf(key_str, OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_hnsw_bq_data_part%05ld", tablet_id_.id(), snapshot_version_, cur_row_pos_))) {
      LOG_WARN("fail to build bq vec snapshot key str", K(ret), K_(index_type));
    } else if (index_type_ == VIAT_IPIVF && OB_FAIL(databuff_printf(key_str, OB_VEC_IDX_SNAPSHOT_KEY_LENGTH, key_pos, "%lu_%ld_ipivf_data_part%05ld", tablet_id_.id(), snapshot_version_, cur_row_pos_))) {
      LOG_WARN("fail to build ipivf vec snapshot key str", K(ret), K_(index_type));
    } else {
      current_row_.storage_datums_[vector_key_col_idx_].set_string(key_str, key_pos);
    }
    // set vec data
    if (OB_FAIL(ret)) {
    } else {
      // TODO @lhd maybe we should do deep copy
      current_row_.storage_datums_[vector_data_col_idx_].set_string(ctx_->vals_.at(cur_row_pos_));
    }

    // set vid and vec to null
    if (OB_SUCC(ret) && (vector_visible_col_idx_ >= 0 && vector_visible_col_idx_ < current_row_.get_column_count())) {
      if (is_vec_tablet_rebuild_) {
        current_row_.storage_datums_[vector_visible_col_idx_].set_false();
      } else {
        current_row_.storage_datums_[vector_visible_col_idx_].set_true();
      }

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
    LOG_WARN("get tablet context failed", K(ret));
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
    } else if (OB_ISNULL(adapter = is_vec_tablet_rebuild ? tablet_context->vector_index_ctx_->adapter_ : adaptor_guard.get_adatper())) {
      LOG_WARN("error unexpected, adapter is nullptr", K(ret), K(tablet_context->vector_index_ctx_->ls_id_), K(tablet_id_));
    } else if (is_vec_tablet_rebuild && OB_FAIL(adaptor_guard.set_adapter(tablet_context->vector_index_ctx_->adapter_))) {
      LOG_WARN("fail to set new adapter guard", K(ret));
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
    ObHNSWSerializeCallback callback;
    ObOStreamBuf::Callback cb = callback;

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
      ObPluginVectorIndexAdaptor *adp = is_vec_tablet_rebuild ? ctx.adapter_ : adaptor_guard.get_adatper();
      if (OB_ISNULL(adp)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected nullptr", K(ret), KP(adp), K(is_vec_tablet_rebuild));
      } else if (is_vec_tablet_rebuild && OB_FAIL(adaptor_guard.set_adapter(adp))) {
        LOG_WARN("fail to set new adapter guard", K(ret));
      } else if (OB_FAIL(adp->check_snap_hnswsq_index())) {
        LOG_WARN("failed to check snap hnswsq index", K(ret));
      } else if (OB_FAIL(adp->set_snapshot_key_prefix(tablet_id_.id(), ctx.snapshot_version_, ObVectorIndexSliceStore::OB_VEC_IDX_SNAPSHOT_KEY_LENGTH))) {
        LOG_WARN("failed to set snapshot key prefix", K(ret), K(tablet_id_.id()), K(ctx.snapshot_version_));
      } else if (OB_FAIL(adp->serialize(&row_allocator_, param, cb))) {
        if (OB_NOT_INIT == ret) {
          // ignore // no data in slice store
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to do vsag serialize", K(ret));
        }
      } else {
        type = adp->get_snap_index_type();
        ctx.index_type_ = type;
        LOG_INFO("HgraphIndex finish vsag serialize for tablet", K(tablet_id_), K(ctx.ctx_.get_vals().count()), K(type));
      }
      if (OB_SUCC(ret)) {
        omt::ObTenantConfigGuard tenant_config(TENANT_CONF(adp->get_tenant_id()));
        if (!tenant_config.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("fail get tenant_config", KR(ret), K(adp->get_tenant_id()));
        } else if (OB_FAIL(adp->renew_single_snap_index((type == VIAT_HNSW_BQ || type == VIAT_IPIVF)
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
      } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, slice_idx_, -1/*cg_idx*/, ddl_dag, 0/*max_batch_size*/, write_param))) {
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
    ObPluginVectorIndexAdaptor *adapter = tablet_context->vector_index_ctx_->adapter_;

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
      if (OB_FAIL(embedmgr_->init(model_id_, col_type))) {
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
    } else if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id_, slice_idx_, -1/*cg_idx*/, ddl_dag, 0/*max_batch_size*/, write_param))) {
      LOG_WARN("fill writer param failed", K(ret));
    } else if (OB_ISNULL(slice_writer_ = OB_NEWx(ObTabletSliceWriter, &op_allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("allocate memory for tablet slice writer failed", K(ret));
    } else if (OB_FAIL(slice_writer_->init(write_param))) {
      LOG_WARN("init macro block slice store failed", K(ret));
    } else {
      is_inited_ = true;
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
    // do nothing
  } else if (OB_ISNULL(slice_writer_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("slice writer is not initialized", K(ret));
  } else {
    iter_.reuse();
    if (OB_FAIL(iter_.init(*tablet_context->vector_index_ctx_, input_chunk.batch_info_))) {
      LOG_WARN("init embedding row iterator with batch info failed", K(ret));
    } else {
      blocksstable::ObDatumRow *datum_row = nullptr;
      while (OB_SUCC(ret)) {
        if (OB_FAIL(iter_.get_next_row(datum_row))) {
          if (ret != OB_ITER_END) {
            LOG_WARN("fail to get next embedding data row", K(ret));
          }
        } else if (OB_FAIL(slice_writer_->append_row(*datum_row))) {
          LOG_WARN("fail to append row to macro block slice store", K(ret));
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
