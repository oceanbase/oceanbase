/**
 * Copyright (c) 2023 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_DDL_OB_CG_MACRO_BLOCK_WRITE_TASK_H_
#define OCEANBASE_STORAGE_DDL_OB_CG_MACRO_BLOCK_WRITE_TASK_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ddl/ob_ddl_pipeline.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "storage/ddl/ob_cg_micro_block_write_op.h"
#include "src/storage/ddl/ob_cg_macro_block_write_op.h"
#include "observer/table_load/dag/ob_table_load_dag_task.h"

namespace oceanbase
{
namespace blocksstable
{
struct ObDatumRow;
struct ObMacroDataSeq;
}

namespace observer
{
class ObTableLoadDag;
}

namespace storage
{
class ObStorageSchema;
class ObDDLIndependentDag;
class ObColumnClusteredDag;
class ObCgMacroBlockWriter;
struct ObWriteMacroParam;
struct ObStorageColumnGroupSchema;
class ObITabletSliceRowIterator;
class ObDDLSlice;

// this task is used to directly write cg macro blocks in the scan task.
// This task uses a macro block writer for each column group (CG) to write macro blocks
// and can only be used when there is sufficient memory.
class ObCgMacroBlockWriteTask : public share::ObITask
{
public:
  ObCgMacroBlockWriteTask(const ObITaskType type);
  ObCgMacroBlockWriteTask();
  virtual ~ObCgMacroBlockWriteTask();
  int init(ObDDLIndependentDag *ddl_dag,
           ObITabletSliceRowIterator *row_iter,
           const ObTabletID &tablet_id);
  int process() override;

private:
  int project_cg_row(const ObStorageColumnGroupSchema &cg_schema,
                     const blocksstable::ObDatumRow &row,
                     blocksstable::ObDatumRow &cg_row);
  DISABLE_COPY_ASSIGN(ObCgMacroBlockWriteTask);

private:
  bool is_inited_;
  ObArenaAllocator allocator_;
  common::ObTabletID tablet_id_;
  const ObStorageSchema *storage_schema_;
  ObITabletSliceRowIterator *row_iter_;
  ObArray<ObCgMacroBlockWriter *> cg_macro_block_writers_;
};

class ObDDLScanTask : public share::ObITask
{
public:
  ObDDLScanTask(const ObITaskType type);
  ObDDLScanTask();
  virtual ~ObDDLScanTask();
  int init(ObDDLIndependentDag *ddl_dag);
  virtual share::ObITask::ObITaskPriority get_priority() override;
  int process();
private:
  ObDDLIndependentDag *ddl_dag_;
};

class ObDDLTabletScanTask final : public share::ObITask
{
public:
  ObDDLTabletScanTask();
  virtual ~ObDDLTabletScanTask() = default;
  int process() override { return OB_SUCCESS; }
};

class ObWriteMacroBaseOperator : public ObPipelineOperator
{
public:
  explicit ObWriteMacroBaseOperator(ObPipeline *pipeline)
    : ObPipelineOperator(pipeline), is_inited_(false), slice_writer_()
  {}
  virtual ~ObWriteMacroBaseOperator() = default;
  int init(const ObWriteMacroParam &param);
  virtual bool is_valid() const override;
  VIRTUAL_TO_STRING_KV(K_(slice_writer));
protected:
  bool is_inited_;
  ObTabletSliceWriter slice_writer_;
};

class ObDDLWriteMacroBlockOperator : public ObWriteMacroBaseOperator
{
public:
  explicit ObDDLWriteMacroBlockOperator(ObPipeline *pipeline)
    : ObWriteMacroBaseOperator(pipeline)
  {}
  virtual ~ObDDLWriteMacroBlockOperator() = default;
  INHERIT_TO_STRING_KV("ObWriteMacroBaseOperator", ObWriteMacroBaseOperator, KP_(pipeline));

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
    return OB_SUCCESS;
  }
};

class ObCGRowFileWriterOp : public ObPipelineOperator
{
public:
  ObCGRowFileWriterOp(ObPipeline *pipeline) :
    ObPipelineOperator(pipeline),
    is_inited_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    cg_row_files_generater_() { }
  virtual ~ObCGRowFileWriterOp() = default;
  int init(const ObTabletID &tablet_id,
           const int64_t slice_idx,
           const int64_t max_batch_size);
  virtual bool is_valid() const override
  {
    return is_inited_ && tablet_id_.is_valid() && slice_idx_ >= 0;
  }
  VIRTUAL_TO_STRING_KV(K(is_inited_), K(tablet_id_), K(slice_idx_));

protected:
  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) override;

private:
  bool is_inited_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  ObCGRowFilesGenerater cg_row_files_generater_;
};

class ObDDLMemoryFriendWriteMacroBlockPipeline : public ObDDLWriteMacroBlockBasePipeline
{
public:
  ObDDLMemoryFriendWriteMacroBlockPipeline() :
    ObDDLWriteMacroBlockBasePipeline(TASK_TYPE_DDL_WRITE_USING_TMP_FILE_PIPELINE),
    micro_write_op_(this),
    macro_write_op_(this) { }
  ObDDLMemoryFriendWriteMacroBlockPipeline(const share::ObITask::ObITaskType &task_type) :
    ObDDLWriteMacroBlockBasePipeline(task_type),
    micro_write_op_(this),
    macro_write_op_(this) { }
  virtual ~ObDDLMemoryFriendWriteMacroBlockPipeline() = default;
  int init(ObDDLSlice *ddl_slice);
  virtual int set_remain_block() override;

protected:
  ObCGMicroBlockWriteOp micro_write_op_;
  ObDAGCGMacroBlockWriteOp macro_write_op_;
};


class ObBatchDatumRowsWriteOp : public ObPipelineOperator
{
public:
  ObBatchDatumRowsWriteOp(ObPipeline *pipeline) :
    ObPipelineOperator(pipeline),
    is_inited_(false),
    buffer_need_reuse_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    buffer_(),
    bdrs_() { }
  virtual ~ObBatchDatumRowsWriteOp() = default;
  int init(const ObTabletID &tablet_id, const int64_t slice_idx);
  virtual bool is_valid() const
  {
    return is_inited_;
  }
  VIRTUAL_TO_STRING_KV(K(is_inited_), K(buffer_need_reuse_), K(tablet_id_), K(slice_idx_));

public:
  static const int64_t MAX_BATCH_SIZE = 256;

protected:
 virtual int execute(const ObChunk &input_chunk,
                     ResultState &result_state,
                     ObChunk &output_chunk) override;
  int generate_data_chunk(ObChunk &output_chunk);

protected:
  bool is_inited_;
  bool buffer_need_reuse_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  ObDirectLoadBatchRows buffer_;
  blocksstable::ObBatchDatumRows bdrs_;
};

} // end namespace storage
} // end namespace oceanbase

#endif//OCEANBASE_STORAGE_DDL_OB_CG_MACRO_BLOCK_WRITE_TASK_H_
