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

#ifndef OB_OCEANBASE_STORAGE_DDL_DDL_PIPELINE_H
#define OB_OCEANBASE_STORAGE_DDL_DDL_PIPELINE_H

#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "common/ob_tablet_id.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_vector_kmeans_ctx.h"
#include "share/vector_index/ob_vector_embedding_handler.h"
#include "storage/ddl/ob_hnsw_embedmgr.h"
#include "lib/lock/ob_spin_lock.h"

namespace oceanbase
{

namespace share
{
class ObPluginVectorIndexAdapterGuard;
class ObPluginVectorIndexAdaptor;
class ObEmbeddingTask;
class ObEmbeddingTaskHandler;
}

namespace common
{
class ObIVector;
}

namespace storage
{

class ObTabletSliceWriter;

template<typename HelperType>
int get_spec_ivf_helper(ObIvfBuildHelper *ihelper, HelperType *&helper)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ihelper)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KP(ihelper));
  } else {
    helper = reinterpret_cast<HelperType *>(ihelper);
  }
  return ret;
}

class ObIDDLPipeline : public ObPipeline
{
public:
  explicit ObIDDLPipeline(const share::ObITask::ObITaskType &task_type)
    : ObPipeline(task_type)
  {}
  virtual ~ObIDDLPipeline() = default;
  int init(const ObTabletID &tablet_id, const int64_t slice_idx);
  virtual int preprocess() { return OB_SUCCESS; }
  virtual void postprocess(int &ret_code) { UNUSED(ret_code); }
  virtual int get_next_chunk(ObChunk *&chunk) = 0;
  virtual int finish_chunk(ObChunk *chunk) { UNUSED(chunk); return OB_SUCCESS; }
  virtual int process() override;
private:
  ObTabletID tablet_id_;
  int64_t slice_idx_;
};

class ObWriteMacroPipeline : public ObIDDLPipeline
{
public:
  explicit ObWriteMacroPipeline(const share::ObITask::ObITaskType &task_type)
    : ObIDDLPipeline(task_type)
  {}
  virtual ~ObWriteMacroPipeline() = default;
protected:
  virtual int fill_writer_param(ObWriteMacroParam &param) = 0;
protected:
  ObWriteMacroParam write_param_;
};

class ObDDLWriteMacroBlockBasePipeline : public ObWriteMacroPipeline
{
public:
  explicit ObDDLWriteMacroBlockBasePipeline(const share::ObITask::ObITaskType &task_type) :
    ObWriteMacroPipeline(task_type), ddl_slice_(nullptr) { }
  virtual ~ObDDLWriteMacroBlockBasePipeline() = default;
  virtual int get_next_chunk(ObChunk *&chunk) override;
  virtual int finish_chunk(ObChunk *chunk) override;
  virtual void postprocess(int &ret_code) override;
  virtual int set_remain_block() { return common::OB_SUCCESS; }
  virtual ObITaskPriority get_priority() override;

protected:
  virtual int fill_writer_param(ObWriteMacroParam &param) override;

protected:
  ObDDLSlice *ddl_slice_;
};

struct ObVectorIndexTabletContext
{
public:
  ObVectorIndexTabletContext();
  ~ObVectorIndexTabletContext()
  {
    destroy_ivf_build_helper();
  }
  int init(
      const ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const ObIndexType &index_type,
      const int64_t snapshot_version,
      const int64_t ddl_task_id,
      const ObDDLTableSchema &ddl_table_schema);

TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(tablet_id), K_(snapshot_version), K_(index_type), K_(is_vec_tablet_rebuild));

private:
  int init_hnsw_index(const ObDDLTableSchema &ddl_table_schema);
  int init_ivf_center_index(const ObDDLTableSchema &ddl_table_schema);
  int init_ivf_sq8_meta_index(const ObDDLTableSchema &ddl_table_schema);
  int init_ivf_pq_center_index(const ObDDLTableSchema &ddl_table_schema);
  int init_hnsw_embedding_index(const ObDDLTableSchema &ddl_table_schema);
  int create_ivf_build_helper(
      const ObIndexType index_type,
      ObString &vec_index_param);
  void destroy_ivf_build_helper();
public:
  int64_t row_cnt_;
  int64_t vec_dim_;
  int64_t tenant_id_;
  share::ObLSID ls_id_;
  ObTabletID tablet_id_;
  common::ObString vec_idx_param_;
  share::ObVecIdxSnapshotDataWriteCtx ctx_;
  int32_t vector_vid_col_idx_;
  int32_t vector_col_idx_;
  int32_t vector_key_col_idx_;
  int32_t vector_data_col_idx_;
  int32_t vector_visible_col_idx_;
  int32_t vector_chunk_col_idx_;
  int32_t center_id_col_idx_;
  int32_t center_vector_col_idx_;
  int32_t meta_id_col_idx_;
  int32_t meta_vector_col_idx_;
  int32_t pq_center_id_col_idx_;
  int32_t pq_center_vector_col_idx_;
  ObSEArray<share::ObExtraInfoIdxType, 4> extra_column_idx_types_;
  int64_t lob_inrow_threshold_;
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  int64_t snapshot_version_;
  ObVectorIndexAlgorithmType index_type_;
  ObIvfBuildHelper *helper_;
  int64_t ddl_task_id_;
  bool is_vec_tablet_rebuild_;
  ObPluginVectorIndexAdaptor *adapter_;
  transaction::ObTxDesc *tx_desc_;
  common::ObArenaAllocator allocator_;
  lib::MemoryContext &memory_context_;
  uint64_t *all_vsag_use_mem_;
};

class ObVectorIndexRowIterator
{
public:
  ObVectorIndexRowIterator()
    : is_inited_(false), cur_row_pos_(0), current_row_(), iter_allocator_("VectoIndeIter", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      row_allocator_("VectoRow", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()),
      tablet_id_(), vec_dim_(0)
  {}
  ~ObVectorIndexRowIterator() = default;
  virtual int init(
      ObVectorIndexTabletContext &context) = 0;
  virtual int get_next_row(
      blocksstable::ObDatumRow *&datum_row) = 0;
protected:
  bool is_inited_;
  int64_t cur_row_pos_;
  blocksstable::ObDatumRow current_row_;
  ObArenaAllocator iter_allocator_;
  common::ObArenaAllocator row_allocator_;
  ObTabletID tablet_id_;
  int64_t vec_dim_;
};

class ObHNSWIndexRowIterator : public ObVectorIndexRowIterator
{
public:
  ObHNSWIndexRowIterator()
    : rowkey_cnt_(0), column_cnt_(0), snapshot_version_(0), index_type_(),
      row_cnt_(0), ls_id_(),vec_idx_param_(),
      vector_vid_col_idx_(-1), vector_col_idx_(-1), vector_key_col_idx_(-1), vector_data_col_idx_(-1), vector_visible_col_idx_(-1), is_vec_tablet_rebuild_(false),
      ctx_(nullptr), extra_column_idx_types_()
  {}
  ~ObHNSWIndexRowIterator() = default;
  int init(
      ObVectorIndexTabletContext &context);
  virtual int get_next_row(
      blocksstable::ObDatumRow *&datum_row) override;
private:
  bool is_vec_idx_col_invalid(const int64_t column_cnt) const;
private:
  static const int64_t OB_VEC_IDX_SNAPSHOT_KEY_LENGTH = 256;
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  int64_t snapshot_version_;
  ObVectorIndexAlgorithmType index_type_;
  int64_t row_cnt_;
  share::ObLSID ls_id_;
  common::ObString vec_idx_param_;
  int32_t vector_vid_col_idx_;
  int32_t vector_col_idx_;
  int32_t vector_key_col_idx_;
  int32_t vector_data_col_idx_;
  int32_t vector_visible_col_idx_;
  bool is_vec_tablet_rebuild_;
  ObVecIdxSnapshotDataWriteCtx *ctx_;
  ObSEArray<share::ObExtraInfoIdxType, 4> extra_column_idx_types_;
};

class ObIVFBaseRowIterator : public ObVectorIndexRowIterator
{
public:
  ObIVFBaseRowIterator()
    : lob_inrow_threshold_(0)
  {}
  ~ObIVFBaseRowIterator() = default;
protected:
  int64_t lob_inrow_threshold_;
};

class ObIVFCenterRowIterator : public ObIVFBaseRowIterator
{
public:
  ObIVFCenterRowIterator()
    : rowkey_cnt_(0), column_cnt_(0), snapshot_version_(0), index_type_(),
      center_id_col_idx_(-1), center_vector_col_idx_(-1), tablet_id_(), helper_(nullptr)
  {}
  ~ObIVFCenterRowIterator() = default;
  int init(
      ObVectorIndexTabletContext &context);
  virtual int get_next_row(
      blocksstable::ObDatumRow *&datum_row) override;
private:
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  int64_t snapshot_version_;
  ObVectorIndexAlgorithmType index_type_;
  int32_t center_id_col_idx_;
  int32_t center_vector_col_idx_;
  ObTabletID tablet_id_;
  ObIvfFlatBuildHelper *helper_;
};

class ObIVFSq8MetaRowIterator : public ObIVFBaseRowIterator
{
public:
  ObIVFSq8MetaRowIterator()
    : rowkey_cnt_(0), column_cnt_(0), snapshot_version_(0), meta_id_col_idx_(-1), meta_vector_col_idx_(-1),
      helper_(nullptr)
  {}
  ~ObIVFSq8MetaRowIterator() = default;
  int init(
      ObVectorIndexTabletContext &context);
  virtual int get_next_row(
      blocksstable::ObDatumRow *&datum_row) override;
private:
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  int64_t snapshot_version_;
  int32_t meta_id_col_idx_;
  int32_t meta_vector_col_idx_;
  ObIvfSq8BuildHelper *helper_;
};

class ObIVFPqRowIterator : public ObIVFBaseRowIterator
{
public:
  ObIVFPqRowIterator()
    : rowkey_cnt_(0), column_cnt_(0), snapshot_version_(0), pq_center_id_col_idx_(0),
      pq_center_vector_col_idx_(0), vec_dim_(0), helper_(nullptr)
  {}
  virtual ~ObIVFPqRowIterator() = default;
  int init(
      ObVectorIndexTabletContext &context);
  virtual int get_next_row(
      blocksstable::ObDatumRow *&datum_row) override;
private:
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  int64_t snapshot_version_;
  int32_t pq_center_id_col_idx_;
  int32_t pq_center_vector_col_idx_;
  int64_t vec_dim_;
  ObIvfPqBuildHelper *helper_;
};

class ObVectorIndexBaseOperator : public ObPipelineOperator
{
public:
  explicit ObVectorIndexBaseOperator(ObPipeline *pipeline);
  virtual bool is_valid() const override;
  int init(const common::ObTabletID &tablet_id, const int64_t slice_idx);
  virtual ~ObVectorIndexBaseOperator() = default;
  int get_ddl_tablet_context(ObDDLTabletContext *&tablet_context);
  TO_STRING_KV(K_(tablet_id), K_(slice_idx));
protected:
  bool is_inited_;
  common::ObTabletID tablet_id_;
  int64_t slice_idx_;
  ObArenaAllocator op_allocator_;
  ObArenaAllocator row_allocator_;
};

class ObHNSWIndexAppendBufferOperator : public ObVectorIndexBaseOperator
{
public:
  explicit ObHNSWIndexAppendBufferOperator(ObPipeline *pipeline)
    : ObVectorIndexBaseOperator(pipeline)
  {}
  virtual int init(
      const ObTabletID &tablet_id);
  TO_STRING_KV(K_(vec_dim), K_(vec_idx_param), K_(vector_vid_col_idx), K_(vector_col_idx),
      K_(vector_key_col_idx), K_(vector_data_col_idx), K_(vector_visible_col_idx), K_(extra_column_idx_types));
protected:
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override;
  virtual int try_execute_finish(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override
  {
    UNUSED(input_chunk);
    UNUSED(result_state);
    UNUSED(output_chunk);
    return OB_SUCCESS;
  }
private:
  int append_row(
      const int64_t row_pos,
      const common::ObIArray<common::ObIVector *> &vectors,
      ObDDLTabletContext *tablet_context);
  int append_row_file(ObCGRowFile *cg_row_file, ObDDLTabletContext *tablet_context);
private:
  int64_t vec_dim_;
  common::ObString vec_idx_param_;
  int32_t vector_vid_col_idx_;
  int32_t vector_col_idx_;
  int32_t vector_key_col_idx_;
  int32_t vector_data_col_idx_;
  int32_t vector_visible_col_idx_;
  ObSEArray<share::ObExtraInfoIdxType, 4> extra_column_idx_types_;
};

class ObHNSWIndexBuildOperator : public ObVectorIndexBaseOperator
{
public:
  explicit ObHNSWIndexBuildOperator(ObPipeline *pipeline)
    : ObVectorIndexBaseOperator(pipeline)
  {}
  virtual ~ObHNSWIndexBuildOperator() = default;
  int init(const ObTabletID &tablet_id);
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override;
  TO_STRING_KV(K_(tablet_id));
private:
  int serialize_vector_index(
      ObIAllocator *allocator,
      transaction::ObTxDesc *tx_desc,
      int64_t lob_inrow_threshold,
      ObVectorIndexAlgorithmType &type,
      ObVectorIndexTabletContext &ctx,
      const bool is_vec_tablet_rebuild);
};

class ObVectorIndexWriteMacroBaseOperator : public ObVectorIndexBaseOperator
{
public:
  explicit ObVectorIndexWriteMacroBaseOperator(ObPipeline *pipeline)
    : ObVectorIndexBaseOperator(pipeline)
  {}
  ~ObVectorIndexWriteMacroBaseOperator() = default;
  int init(const ObTabletID &tablet_id);
protected:
  int write(const ObChunk &input_chunk, ObVectorIndexRowIterator &iter);
};

class ObHNSWIndexWriteMacroOperator : public ObVectorIndexWriteMacroBaseOperator
{
public:
  explicit ObHNSWIndexWriteMacroOperator(ObPipeline *pipeline)
    : ObVectorIndexWriteMacroBaseOperator(pipeline), iter_()
  {}
  virtual ~ObHNSWIndexWriteMacroOperator() = default;
  int init(const ObTabletID &tablet_id);
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override;
  TO_STRING_KV(K_(tablet_id), K_(slice_idx));
private:
  ObHNSWIndexRowIterator iter_;
};


class ObHNSWIndexDMLWriteOperator : public ObVectorIndexWriteMacroBaseOperator
{
public:
  explicit ObHNSWIndexDMLWriteOperator(ObPipeline *pipeline)
    : ObVectorIndexWriteMacroBaseOperator(pipeline), iter_()
  {}
  virtual ~ObHNSWIndexDMLWriteOperator() = default;
  int init(const ObTabletID &tablet_id);
  int dml_write(const ObChunk &input_chunk, ObVectorIndexRowIterator &iter);
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override;
  TO_STRING_KV(K_(tablet_id), K_(slice_idx));
private:
  ObHNSWIndexRowIterator iter_;
};

template<typename AppendOP>
class ObVectorIndexAppendPipeline : public ObDDLWriteMacroBlockBasePipeline
{
public:
  ObVectorIndexAppendPipeline()
    : ObDDLWriteMacroBlockBasePipeline(TASK_TYPE_DDL_VECTOR_INDEX_APPEND_PIPELINE), append_op_(this)
  {}
  virtual ~ObVectorIndexAppendPipeline() = default;
  int init(ObDDLSlice *ddl_slice);
private:
  AppendOP append_op_;
};

template<typename AppendOP>
int ObVectorIndexAppendPipeline<AppendOP>::init(ObDDLSlice *ddl_slice)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == ddl_slice || !ddl_slice->is_inited())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid arguments", K(ret), KPC(ddl_slice));
  } else {
    ddl_slice_ = ddl_slice;
    if (OB_FAIL(append_op_.init(ddl_slice->get_tablet_id()))) {
      STORAGE_LOG(WARN, "init write operator failed", K(ret));
    } else if (OB_FAIL(add_op(&append_op_))) {
      STORAGE_LOG(WARN, "add op failed", K(ret));
    }
  }
  return ret;
}

template<typename BuildOp, typename WriteOp>
class ObVectorIndexBuildAndWritePipeline : public ObIDDLPipeline
{
public:
  ObVectorIndexBuildAndWritePipeline()
    : ObIDDLPipeline(TASK_TYPE_DDL_VECTOR_INDEX_BUILD_AND_WRITE_PIPELINE), is_chunk_generated_(false),
      tablet_id_(), build_op_(this), write_op_(this)
  {}
  int init(const ObTabletID &tablet_id);
  virtual ~ObVectorIndexBuildAndWritePipeline() = default;
protected:
  virtual int get_next_chunk(ObChunk *&next_chunk) override;
  virtual void postprocess(int &ret_code) override;
private:
  bool is_chunk_generated_;
  ObTabletID tablet_id_;
  BuildOp build_op_;
  WriteOp write_op_;
  ObChunk chunk_;
};

template<typename BuildOp, typename WriteOp>
int ObVectorIndexBuildAndWritePipeline<BuildOp, WriteOp>::init(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  tablet_id_ = tablet_id;
  if (OB_FAIL(build_op_.init(tablet_id))) {
    STORAGE_LOG(WARN, "init build operator failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(write_op_.init(tablet_id))) {
    STORAGE_LOG(WARN, "init write operator failed", K(ret), K(tablet_id));
  } else if (OB_FAIL(add_op(&build_op_))) {
    STORAGE_LOG(WARN, "add operator failed", K(ret));
  } else if (OB_FAIL(add_op(&write_op_))) {
    STORAGE_LOG(WARN, "add operator failed", K(ret));
  }
  return ret;
}

template<typename BuildOp, typename WriteOp>
int ObVectorIndexBuildAndWritePipeline<BuildOp, WriteOp>::get_next_chunk(ObChunk *&next_chunk)
{
  int ret = OB_SUCCESS;
  next_chunk = nullptr;
  if (is_chunk_generated_) {
    chunk_.type_ = ObChunk::ITER_END_TYPE;
    next_chunk = &chunk_;
  } else {
    chunk_.type_ = ObChunk::DAG_TABLET_CONTEXT;
    ObDDLTabletContext *tablet_context = nullptr;
    ObDDLIndependentDag *dag = nullptr;
    if (OB_ISNULL(get_dag())) {
      ret = OB_ERR_SYS;
      STORAGE_LOG(WARN, "get dag failed", K(ret));
    } else if (OB_FALSE_IT(dag = static_cast<ObDDLIndependentDag *>(get_dag()))) {
    } else if (OB_FAIL(dag->get_tablet_context(tablet_id_, tablet_context))) {
      STORAGE_LOG(WARN, "get tablet context failed", K(ret));
    } else {
      chunk_.data_ptr_ = tablet_context;
      next_chunk = &chunk_;
      is_chunk_generated_ = true;
    }
  }
  return ret;
}

template<typename BuildOp, typename WriteOp>
void ObVectorIndexBuildAndWritePipeline<BuildOp, WriteOp>::postprocess(int &ret_code)
{
  if (OB_ITER_END == ret_code) {
    ret_code = OB_SUCCESS;
  }
  if (OB_SUCCESS != ret_code && OB_NOT_NULL(get_dag())) {
    ObDDLIndependentDag *dag = static_cast<ObDDLIndependentDag *>(get_dag());
    dag->set_ret_code(ret_code);
    ret_code = OB_SUCCESS;
  }
}

class ObIVFIndexBaseOperator : public ObVectorIndexBaseOperator
{
public:
  explicit ObIVFIndexBaseOperator(ObPipeline *pipeline)
    : ObVectorIndexBaseOperator(pipeline),
      helper_(nullptr)
  {}
  ~ObIVFIndexBaseOperator() = default;
  int init(const ObTabletID &tablet_id);
protected:
  ObIvfBuildHelper *helper_;
};

class ObIVFIndexAppendBufferBaseOperator : public ObIVFIndexBaseOperator
{
public:
  explicit ObIVFIndexAppendBufferBaseOperator(ObPipeline *pipeline)
    : ObIVFIndexBaseOperator(pipeline), vector_col_idx_(-1)
  {}
  ~ObIVFIndexAppendBufferBaseOperator() = default;
protected:
  virtual int append_row(
      const int64_t row_pos,
      const ObIVector &vector) = 0;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override;
  virtual int try_execute_finish(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override
  {
    UNUSED(input_chunk);
    UNUSED(result_state);
    UNUSED(output_chunk);
    return OB_SUCCESS;
  }
  int append_row_file(ObCGRowFile *row_file);
  INHERIT_TO_STRING_KV("ObIVFIndexBaseOperator", ObIVFIndexBaseOperator, K_(vector_col_idx));
protected:
  int32_t vector_col_idx_;
};

class ObIVFCenterAppendBufferOperator : public ObIVFIndexAppendBufferBaseOperator
{
public:
  explicit ObIVFCenterAppendBufferOperator(ObPipeline *pipeline)
    : ObIVFIndexAppendBufferBaseOperator(pipeline)
  {}
  ~ObIVFCenterAppendBufferOperator() = default;
  int init(const ObTabletID &tablet_id);
protected:
  virtual int append_row(const int64_t row_pos, const ObIVector &vector) override;
};

class ObIVFCenterIndexBuildOperator : public ObIVFIndexBaseOperator
{
public:
  explicit ObIVFCenterIndexBuildOperator(ObPipeline *pipeline)
    : ObIVFIndexBaseOperator(pipeline)
  {}
  ~ObIVFCenterIndexBuildOperator() = default;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &ouput_chunk) override;
};

class ObIVFCenterWriteMacroOperator : public ObVectorIndexWriteMacroBaseOperator
{
public:
  explicit ObIVFCenterWriteMacroOperator(ObPipeline *pipeline)
    : ObVectorIndexWriteMacroBaseOperator(pipeline)
  {}
  ~ObIVFCenterWriteMacroOperator() = default;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &ouput_chunk) override;
  virtual int try_execute_finish(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override
  {
    return OB_SUCCESS;
  }
private:
  ObIVFCenterRowIterator iter_;
};

class ObIVFSq8MetaAppendBufferOperator : public ObIVFIndexAppendBufferBaseOperator
{
public:
  explicit ObIVFSq8MetaAppendBufferOperator(ObPipeline *pipeline)
    : ObIVFIndexAppendBufferBaseOperator(pipeline)
  {}
  ~ObIVFSq8MetaAppendBufferOperator() = default;
  int init(const ObTabletID &tablet_id);
protected:
  virtual int append_row(const int64_t row_pos, const ObIVector &vector) override;
};

class ObIVFSq8MetaIndexBuildOperator : public ObIVFIndexBaseOperator
{
public:
  explicit ObIVFSq8MetaIndexBuildOperator(ObPipeline *pipeline)
    : ObIVFIndexBaseOperator(pipeline)
  {}
  ~ObIVFSq8MetaIndexBuildOperator() = default;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &ouput_chunk) override;
};

class ObIVFSq8MetaWriteMacroOperator : public ObVectorIndexWriteMacroBaseOperator
{
public:
  explicit ObIVFSq8MetaWriteMacroOperator(ObPipeline *pipeline)
    : ObVectorIndexWriteMacroBaseOperator(pipeline), iter_()
  {}
  ~ObIVFSq8MetaWriteMacroOperator() = default;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &ouput_chunk) override;
  virtual int try_execute_finish(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override
  {
    return OB_SUCCESS;
  }
private:
  ObIVFSq8MetaRowIterator iter_;
};

class ObIVFPqAppendBufferOperator : public ObIVFIndexAppendBufferBaseOperator
{
public:
  explicit ObIVFPqAppendBufferOperator(ObPipeline *pipeline)
    : ObIVFIndexAppendBufferBaseOperator(pipeline)
  {}
  ~ObIVFPqAppendBufferOperator() = default;
  int init(const ObTabletID &tablet_id);
protected:
  virtual int append_row(const int64_t row_pos, const ObIVector &vector) override;
};

class ObIVFPqIndexBuildOperator : public ObIVFIndexBaseOperator
{
public:
  explicit ObIVFPqIndexBuildOperator(ObPipeline *pipeline)
    : ObIVFIndexBaseOperator(pipeline)
  {}
  ~ObIVFPqIndexBuildOperator() = default;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &ouput_chunk) override;
};

class ObIVFPqWriteMacroOperator : public ObVectorIndexWriteMacroBaseOperator
{
public:
  explicit ObIVFPqWriteMacroOperator(ObPipeline *pipeline)
    : ObVectorIndexWriteMacroBaseOperator(pipeline), iter_()
  {}
  ~ObIVFPqWriteMacroOperator() = default;
  virtual int execute(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &ouput_chunk) override;
  virtual int try_execute_finish(
      const ObChunk &input_chunk,
      ResultState &result_state,
      ObChunk &output_chunk) override
  {
    return OB_SUCCESS;
  }
private:
  ObIVFPqRowIterator iter_;
};

typedef ObVectorIndexAppendPipeline<ObHNSWIndexAppendBufferOperator> ObHNSWAppendPipeline;
typedef ObVectorIndexAppendPipeline<ObIVFCenterAppendBufferOperator> ObIVFCenterAppendPipeline;
typedef ObVectorIndexAppendPipeline<ObIVFSq8MetaAppendBufferOperator> ObIVFSq8MetaAppendPipeline;
typedef ObVectorIndexAppendPipeline<ObIVFPqAppendBufferOperator> ObIVFPqAppendPipeline;
typedef ObVectorIndexBuildAndWritePipeline<ObHNSWIndexBuildOperator, ObHNSWIndexDMLWriteOperator> ObHNSWBuildAndDMLWritePipeline;
typedef ObVectorIndexBuildAndWritePipeline<ObHNSWIndexBuildOperator, ObHNSWIndexWriteMacroOperator> ObHNSWBuildAndWritePipeline;
typedef ObVectorIndexBuildAndWritePipeline<ObIVFCenterIndexBuildOperator, ObIVFCenterWriteMacroOperator> ObIVFCenterBuildAndWritePipeline;
typedef ObVectorIndexBuildAndWritePipeline<ObIVFSq8MetaIndexBuildOperator, ObIVFSq8MetaWriteMacroOperator> ObIVFSq8MetaBuildAndWritePipeline;
typedef ObVectorIndexBuildAndWritePipeline<ObIVFPqIndexBuildOperator, ObIVFPqWriteMacroOperator> ObIVFPqBuildAndWritePipeline;

// ==================== Vector Embedding ====================
class ObEmbeddingTaskMgr;

class ObHNSWEmbeddingOperator : public ObVectorIndexBaseOperator
{
public:
  explicit ObHNSWEmbeddingOperator(ObPipeline *pipeline)
    : ObVectorIndexBaseOperator(pipeline), embedmgr_(nullptr), vec_dim_(-1), rowkey_cnt_(-1),
      vid_col_idx_(-1), text_col_idx_(-1), is_inited_(false), error_ret_code_(OB_SUCCESS),
      batch_size_(0), current_batch_(nullptr), http_timeout_us_(20 * 1000 * 1000) /* 20s */
  {}
  ~ObHNSWEmbeddingOperator();
  int init(const ObTabletID &tablet_id);
  virtual int execute(const ObChunk &input_chunk,
                     ResultState &result_state,
                     ObChunk &output_chunk) override;
  virtual int try_execute_finish(const ObChunk &input_chunk,
                                ResultState &result_state,
                                ObChunk &output_chunk) override;

private:
  int get_ready_results(ObChunk &output_chunk, ResultState &result_state);
  int process_input_chunk(const ObChunk &input_chunk);
  int get_next_row_from_tmp_files(common::ObArray<ObCGRowFile *> *cg_row_file_arr,
                                  int64_t &vid,
                                  common::ObString &text,
                                  common::ObArray<blocksstable::ObStorageDatum> &rowkeys,
                                  bool &has_row);
  int get_next_batch_from_tmp_files(ObCGRowFile *&row_file);
  int parse_row(const blocksstable::ObDatumRow &current_row,
                int64_t &vid,
                common::ObString &text,
                common::ObArray<blocksstable::ObStorageDatum> &rowkeys);
  int flush_current_batch();
  bool is_chunk_exhausted() const { return chunk_exhausted_; }
  void reset_chunk_exhausted() { chunk_exhausted_ = false; }
  void reset_scan_state() { cur_file_idx_ = 0; cur_datum_rows_ = nullptr; cur_row_in_batch_ = 0; }

private:
  ObEmbeddingTaskMgr *embedmgr_;
  common::ObString model_id_;
  int64_t vec_dim_;
  int64_t rowkey_cnt_;
  int64_t vid_col_idx_;
  int64_t text_col_idx_;
  bool is_inited_;
  int error_ret_code_;
  // batch submit
  int64_t batch_size_;
  ObTaskBatchInfo *current_batch_;  // Current batching

  // resumable scan state for CG_ROW_TMP_FILES
  int64_t cur_file_idx_;
  blocksstable::ObBatchDatumRows *cur_datum_rows_;
  int64_t cur_row_in_batch_;
  bool chunk_exhausted_;
  int64_t http_timeout_us_;
  DISALLOW_COPY_AND_ASSIGN(ObHNSWEmbeddingOperator);
};

class ObHNSWEmbeddingRowIterator : public ObVectorIndexRowIterator
{
public:
  ObHNSWEmbeddingRowIterator() : rowkey_cnt_(0), column_cnt_(0), snapshot_version_(0),
                            vid_col_idx_(-1), vector_col_idx_(-1),
                            batch_info_(nullptr), cur_result_pos_(0)
  {}

  ~ObHNSWEmbeddingRowIterator() = default;

  virtual int init(ObVectorIndexTabletContext &context) override;
  int init(ObVectorIndexTabletContext &context, ObTaskBatchInfo *batch_info);
  virtual int get_next_row(blocksstable::ObDatumRow *&datum_row) override;
  void reuse() {
    is_inited_ = false;
    rowkey_cnt_ = 0;
    column_cnt_ = 0;
    snapshot_version_ = 0;
    vid_col_idx_ = -1;
    vector_col_idx_ = -1;
    batch_info_ = nullptr;
    cur_result_pos_ = 0;
  }
private:
  bool is_embedding_col_invalid(const int64_t column_cnt) const {
    return vid_col_idx_ < 0 || vid_col_idx_ >= column_cnt ||
           vector_col_idx_ < 0 || vector_col_idx_ >= column_cnt;
  }
private:
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  int64_t snapshot_version_;
  int32_t vid_col_idx_;
  int32_t vector_col_idx_;
  ObTaskBatchInfo *batch_info_;  // Not owned, just a reference
  int64_t cur_result_pos_;
};

class ObHNSWEmbeddingWriteMacroOperator : public ObVectorIndexWriteMacroBaseOperator
{
public:
  explicit ObHNSWEmbeddingWriteMacroOperator(ObPipeline *pipeline)
    : ObVectorIndexWriteMacroBaseOperator(pipeline), iter_(), slice_writer_(nullptr)
  {}
  ~ObHNSWEmbeddingWriteMacroOperator();

  int init(const ObTabletID &tablet_id, const int64_t slice_idx);
  virtual int execute(const ObChunk &input_chunk,
                     ResultState &result_state,
                     ObChunk &output_chunk) override;

  TO_STRING_KV(K_(tablet_id), K_(slice_idx));

private:
  ObHNSWEmbeddingRowIterator iter_;
  // persistent writer across multiple input chunks for the same slice
  ObTabletSliceWriter *slice_writer_;
  DISALLOW_COPY_AND_ASSIGN(ObHNSWEmbeddingWriteMacroOperator);
};

class ObHNSWEmbeddingAppendAndWritePipeline : public ObDDLWriteMacroBlockBasePipeline
{
public:
  ObHNSWEmbeddingAppendAndWritePipeline()
    : ObDDLWriteMacroBlockBasePipeline(TASK_TYPE_DDL_VECTOR_INDEX_APPEND_PIPELINE),
      embedding_buffer_op_(this), embedding_write_op_(this)
  {}
  virtual ~ObHNSWEmbeddingAppendAndWritePipeline() = default;

  int init(ObDDLSlice *ddl_slice)
  {
    int ret = OB_SUCCESS;
    if (OB_UNLIKELY(nullptr == ddl_slice || !ddl_slice->is_inited())) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid arguments", K(ret), KPC(ddl_slice));
    } else {
      ddl_slice_ = ddl_slice;
      if (OB_FAIL(embedding_buffer_op_.init(ddl_slice->get_tablet_id()))) {
        STORAGE_LOG(WARN, "init embedding buffer operator failed", K(ret));
      } else if (OB_FAIL(embedding_write_op_.init(ddl_slice->get_tablet_id(), ddl_slice->get_slice_idx()))) {
        STORAGE_LOG(WARN, "init embedding write operator failed", K(ret));
      } else if (OB_FAIL(add_op(&embedding_buffer_op_))) {
        STORAGE_LOG(WARN, "add embedding buffer op failed", K(ret));
      } else if (OB_FAIL(add_op(&embedding_write_op_))) {
        STORAGE_LOG(WARN, "add embedding write op failed", K(ret));
      }
    }
    return ret;
  }

  virtual int set_remain_block() override {
    if (OB_ISNULL(ddl_slice_)) {
      return OB_NOT_INIT;
    } else {
      ddl_slice_->set_block_flushed(0);
      return OB_SUCCESS;
    }
  }

private:
  ObHNSWEmbeddingOperator embedding_buffer_op_;
  ObHNSWEmbeddingWriteMacroOperator embedding_write_op_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_OCEANBASE_STORAGE_DDL_DDL_PIPELINE_H
