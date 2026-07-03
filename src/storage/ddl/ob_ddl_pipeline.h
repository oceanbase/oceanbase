/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_OCEANBASE_STORAGE_DDL_DDL_PIPELINE_H
#define OB_OCEANBASE_STORAGE_DDL_DDL_PIPELINE_H

#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_ddl_independent_dag.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "common/ob_tablet_id.h"
#include "share/vector_index/ob_vector_index_util.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_vector_kmeans_ctx.h"
#include "share/vector_index/ob_vector_embedding_handler.h"
#include "share/vector_index/ob_ai_access_service.h"
#include "storage/ddl/ob_hnsw_embedmgr.h"
#include "storage/ddl/ob_batch_file_slot_ring.h"
#include "lib/lock/ob_spin_lock.h"
#include "share/ai_service/ob_ai_batch_file_writer.h"

// Forward declaration to avoid circular dependency
namespace oceanbase
{
namespace storage
{
class ObCGRowFileWriterOp;
}
}

namespace oceanbase
{

namespace share
{
class ObPluginVectorIndexAdapterGuard;
class ObPluginVectorIndexAdaptor;
class ObEmbeddingTask;
class ObEmbeddingTaskHandler;
}

namespace vector_index
{
class ObAiAccessService;
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
  // Accessors for monitoring / debugging.
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  int64_t get_slice_idx() const { return slice_idx_; }
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

  int build_extra_column_idxs(const int32_t chunk_col_idx, common::ObSEArray<int32_t, 4> &extra_column_idxs) const;

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
  ObPluginVectorIndexAdapterGuard adapter_guard_;
  transaction::ObTxDesc *tx_desc_;
  common::ObArenaAllocator allocator_;
  lib::MemoryContext &memory_context_;
  uint64_t *all_vsag_use_mem_;
  uint64_t table_id_;
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

class ObVecEmbeddingBaseOp : public ObVectorIndexBaseOperator
{
public:
  explicit ObVecEmbeddingBaseOp(ObPipeline *pipeline)
    : ObVectorIndexBaseOperator(pipeline)
  {}
  virtual ~ObVecEmbeddingBaseOp() = default;
  virtual int init(const ObTabletID &tablet_id) = 0;
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
  }
}

class ObIVFIndexBaseOperator : public ObVectorIndexBaseOperator
{
public:
  explicit ObIVFIndexBaseOperator(ObPipeline *pipeline)
    : ObVectorIndexBaseOperator(pipeline),
      table_id_(), helper_(nullptr)
  {}
  ~ObIVFIndexBaseOperator() = default;
  int init(const ObTabletID &tablet_id);
protected:
  ObTableID table_id_;
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

class ObHNSWEmbeddingOperator : public ObVecEmbeddingBaseOp
{
public:
  explicit ObHNSWEmbeddingOperator(ObPipeline *pipeline)
    : ObVecEmbeddingBaseOp(pipeline), embedmgr_(nullptr), vec_dim_(-1), rowkey_cnt_(-1),
      text_col_idx_(-1), is_inited_(false), error_ret_code_(OB_SUCCESS),
      batch_size_(0), current_batch_(nullptr)
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
                                  blocksstable::ObStorageDatum &text,
                                  common::ObArray<blocksstable::ObStorageDatum> &extras,
                                  bool &has_row);
  int get_next_batch_from_tmp_files(ObCGRowFile *&row_file);
  int parse_row(const blocksstable::ObDatumRow &current_row,
                blocksstable::ObStorageDatum &text,
                common::ObArray<blocksstable::ObStorageDatum> &extras);
  int flush_current_batch();
  bool is_chunk_exhausted() const { return chunk_exhausted_; }
  void reset_chunk_exhausted() { chunk_exhausted_ = false; }
  void reset_scan_state() { cur_file_idx_ = 0; cur_datum_rows_ = nullptr; cur_row_in_batch_ = 0; }

private:
  ObEmbeddingTaskMgr *embedmgr_;
  common::ObString model_id_;
  int64_t vec_dim_;
  int64_t rowkey_cnt_;
  int64_t text_col_idx_;
  // extras carry all non-embedding columns
  ObSEArray<int32_t, 4> extra_column_idxs_;
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
  DISALLOW_COPY_AND_ASSIGN(ObHNSWEmbeddingOperator);
};

class ObHNSWEmbeddingRowIterator : public ObVectorIndexRowIterator
{
public:
  ObHNSWEmbeddingRowIterator() : rowkey_cnt_(0), column_cnt_(0), snapshot_version_(0),
                            vector_col_idx_(-1),
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
    vector_col_idx_ = -1;
    batch_info_ = nullptr;
    cur_result_pos_ = 0;
    extra_column_idxs_.reset();
  }
private:
  bool is_embedding_col_invalid(const int64_t column_cnt) const {
    return vector_col_idx_ < 0 || vector_col_idx_ >= column_cnt;
  }
private:
  int64_t rowkey_cnt_;
  int64_t column_cnt_;
  int64_t snapshot_version_;
  int32_t vector_col_idx_;
  ObTaskBatchInfo *batch_info_;  // Not owned, just a reference
  int64_t cur_result_pos_;
  // extras carry all non-embedding columns
  ObSEArray<int32_t, 4> extra_column_idxs_;
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

// Pipeline for HNSW embedding index build (supports SYNC_HTTP and BATCH_FILE access modes)
// Data flow: cg_row_tmp_files (from upstream DDL scan) -> embedding_op -> embedding_write_op -> results
class ObHNSWEmbeddingAppendAndWritePipeline : public ObDDLWriteMacroBlockBasePipeline
{
public:
  ObHNSWEmbeddingAppendAndWritePipeline();
  virtual ~ObHNSWEmbeddingAppendAndWritePipeline();

  int init(ObDDLSlice *ddl_slice);

  virtual int set_remain_block() override {
    if (OB_ISNULL(ddl_slice_)) {
      return OB_NOT_INIT;
    } else {
      ddl_slice_->set_block_flushed(0);
      return OB_SUCCESS;
    }
  }

private:
  ObCGRowFileWriterOp *cg_row_file_writer_op_;
  ObVecEmbeddingBaseOp *embedding_op_;           // polymorphic, heap-alloc
  ObVectorIndexAccessMode ai_execution_mode_;
  ObHNSWEmbeddingWriteMacroOperator embedding_write_op_;
};

// ==================== BatchFile Embedding Operator ====================
// Non-blocking BatchFile embedding operator with multi-file support.
// Collects data incrementally across execute() calls, submits BatchFile tasks
// when files are full, and uses dag_yield() to avoid blocking DDL threads.
// SlotRing guarantees results are output in submission order.
//
// execute() flow:
//   1. Collect data from input_chunk into JSONL file
//   2. When file is full, submit BatchFile task and reserve SlotRing slot
//   3. Poll SUBMITTED slots for completion (query_task_status)
//   4. If head slot is READY, pop result and return HAVE_MORE_OUTPUT
//   5. If no result ready, sleep + dag_yield() and return NEED_MORE_INPUT
//   6. On end_chunk: submit remaining, stream out all results in order

class ObBatchFileEmbeddingOperator : public ObVecEmbeddingBaseOp
{
public:
  explicit ObBatchFileEmbeddingOperator(ObPipeline *pipeline);
  ~ObBatchFileEmbeddingOperator();

  virtual int init(const ObTabletID &tablet_id) override;

  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) override;

  virtual int try_execute_finish(const ObChunk &input_chunk,
                                  ResultState &result_state,
                                  ObChunk &output_chunk) override;

  TO_STRING_KV(K_(tablet_id), K_(model_id), K_(ai_execution_mode), K_(is_inited),
               K_(all_data_collected), K_(all_tasks_submitted), K_(end_chunk_sent),
               K_(total_rows_collected), K_(slot_ring));

private:
  // ==================== Core flow methods ====================
  // Collect rows from input_chunk and send to AiAccessService
  int collect_data_to_service_(const ObChunk &input_chunk);
  // Finish current task's data collection and submit it
  int finish_and_submit_current_task_();
  // Poll SUBMITTED slots for completion
  int poll_submitted_slots_();
  // Check head slot and output result if ready
  int check_and_output_result_(ObChunk &output_chunk, ResultState &result_state);
  // Handle end_chunk: submit remaining data, then stream out all results
  int handle_end_chunk_(ObChunk &output_chunk, ResultState &result_state);
  // Sleep + dag_yield() combo to release DDL thread
  int do_yield_();

  // ==================== Helper methods ====================
  int get_ai_config_(const common::ObString &model_id);
  int get_next_row_from_tmp_files_(common::ObArray<ObCGRowFile *> *cg_row_file_arr,
                                    blocksstable::ObStorageDatum &text,
                                    common::ObArray<blocksstable::ObStorageDatum> &extra_cols,
                                    bool &has_row);
  int get_next_batch_from_tmp_files_(ObCGRowFile *&row_file);
  int parse_row_(const blocksstable::ObDatumRow &current_row,
                 blocksstable::ObStorageDatum &text,
                 common::ObArray<blocksstable::ObStorageDatum> &extras);
  bool need_stop_();
  int open_new_task_();
  int ensure_batch_info_();
  void destroy_current_batch_info_();
  void reset_current_task_state_();
  int submit_skip_only_batch_();
  int submit_api_batch_();
  bool is_query_task_status_retryable_(int ret) const;
  void mark_slot_failed_best_effort_(int64_t slot_idx, int error_code, const char *reason);
  // Cancel all submitted tasks in slot_contexts_ via AiAccessService.
  // Called when DDL exits before all tasks complete.
  void cancel_inflight_tasks_();

private:
  // ==================== Service and config ====================
  vector_index::ObAiAccessService *service_;
  common::ObAIFuncBase *embed_provider_;
  common::ObString model_id_;
  share::ObAiAccessMode ai_execution_mode_;
  bool allow_null_on_failure_;
  int64_t vec_dim_;
  int64_t text_col_idx_;
  int64_t rowkey_cnt_;
  int64_t dir_id_;
  int error_ret_code_;
  share::ObAiModelEndpointInfo *endpoint_info_;
  common::ObString request_model_name_;
  common::ObArenaAllocator allocator_;
  ObCollationType text_col_collation_type_;
  common::ObSEArray<int32_t, 4> extra_column_idxs_;

  // ==================== SlotRing for multi-task ordering ====================
  ObBatchFileSlotRing slot_ring_;

  // Slot context: maps slot_idx -> task_id + batch_info (with extras)
  struct SlotContext {
    common::ObString task_id_;
    int64_t row_count_;
    ObTaskBatchInfo *batch_info_;  // Stores extras collected during data phase

    SlotContext() : task_id_(), row_count_(0), batch_info_(nullptr) {}
    void reset() { task_id_.reset(); row_count_ = 0; batch_info_ = nullptr; }
    TO_STRING_KV(K_(task_id), K_(row_count), KP_(batch_info));
  };
  common::ObSEArray<SlotContext, 8> slot_contexts_;

  // DDL task ID for batch task creation
  int64_t ddl_task_id_;

  // RAII writer for current batch task
  share::ObAiBatchTaskWriter writer_;

  // Current task being collected
  int64_t current_task_row_count_;
  ObTaskBatchInfo *current_batch_info_;  // Batch info being filled during data collection

  // ==================== Data collection state ====================
  common::ObArray<ObCGRowFile *> *current_cg_row_files_;
  int64_t cur_file_idx_;
  blocksstable::ObBatchDatumRows *cur_datum_rows_;
  int64_t cur_row_in_batch_;
  bool chunk_exhausted_;
  int64_t total_rows_collected_;

  // ==================== State flags ====================
  bool all_data_collected_;
  bool all_tasks_submitted_;
  bool end_chunk_sent_;

  // ==================== Drain loop state ====================
  int64_t drain_start_ts_;
  int64_t drain_poll_count_;

  // ==================== Constants ====================
  static const int64_t DEFAULT_MAX_CONCURRENT_TASKS = 8;
  static const int64_t RESULT_BATCH_SIZE = 1024;  // Rows per batch when reading results
#ifndef NDEBUG
  static const int64_t DEFAULT_SLICE_SIZE = 100;            // 100 rows per task
  static const int64_t CHECK_INTERVAL_US = 2 * 1000 * 1000; // 2 seconds
  static const int64_t DRAIN_LOG_INTERVAL = 30;             // Log every 30 iterations
#else
  static const int64_t DEFAULT_SLICE_SIZE = 10000;           // 10000 rows per task
  static const int64_t CHECK_INTERVAL_US = 10 * 1000 * 1000; // 10 seconds
  static const int64_t DRAIN_LOG_INTERVAL = 30;             // Log every 30 iterations
#endif

  DISALLOW_COPY_AND_ASSIGN(ObBatchFileEmbeddingOperator);
};


}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_OCEANBASE_STORAGE_DDL_DDL_PIPELINE_H
