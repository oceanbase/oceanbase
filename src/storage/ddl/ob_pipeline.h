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

#ifndef _OCEANBASE_STORAGE_DDL_OB_PIPELINE_H_
#define _OCEANBASE_STORAGE_DDL_OB_PIPELINE_H_

#include "lib/utility/ob_print_utils.h"
#include "lib/container/ob_array.h"
#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "share/table/ob_table_load_row_array.h"
#include "storage/direct_load/ob_direct_load_batch_datum_rows.h"
#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/ddl/ob_cg_row_tmp_file.h"

namespace oceanbase
{
namespace blocksstable
{
class ObBatchDatumRows;
struct ObDatumRow;
}

namespace common
{
class ObIVector;
}

namespace storage
{
class ObTaskBatchInfo;
class ObDDLTabletContext;
class ObDDLSlice;
class ObPipeline;

struct ObChunk
{
public:
  enum ChunkType
  {
    INVALID_TYPE = 0,
    ITER_END_TYPE,
    DATUM_ROW,
    MACRO_BUFFER,
    DAG_TABLET_CONTEXT,
    DIRECT_LOAD_BATCH_DATUM_ROWS,
    CG_ROW_TMP_FILES,
    BATCH_DATUM_ROWS,
    DIRECT_LOAD_ROW_ARRAY,
    TASK_BATCH_INFO,
    MAX_TYPE
  };
public:
  ObChunk() : type_(INVALID_TYPE), data_ptr_(nullptr) {}
  ~ObChunk();
  void reset();
  bool is_valid() const;
  void set_end_chunk() { type_ = ChunkType::ITER_END_TYPE; }
  bool is_end_chunk() const { return ChunkType::ITER_END_TYPE == type_; }
  int get_dag_tablet_context(ObDDLTabletContext *&tablet_context) const;
  OB_INLINE bool is_macro_buffer_type() const { return ChunkType::MACRO_BUFFER == type_; }
  OB_INLINE bool is_direct_load_batch_datum_rows_type() const { return ChunkType::DIRECT_LOAD_BATCH_DATUM_ROWS == type_; }
  OB_INLINE bool is_batch_datum_rows_type() const { return ChunkType::BATCH_DATUM_ROWS == type_; }
  OB_INLINE bool is_cg_row_tmp_files_type() const { return ChunkType::CG_ROW_TMP_FILES == type_; }
  OB_INLINE bool is_datum_row_type() const { return ChunkType::DATUM_ROW == type_; }
  OB_INLINE bool is_direct_load_row_array_type() const { return ChunkType::DIRECT_LOAD_ROW_ARRAY == type_; }
  OB_INLINE bool is_task_batch_info_type() const { return ChunkType::TASK_BATCH_INFO == type_; }
  TO_STRING_KV(K_(type), KP_(data_ptr));
public:
  ChunkType type_;
  union {
    void *data_ptr_;
    storage::ObDirectLoadBatchDatumRows *direct_load_batch_rows_;
    blocksstable::ObDatumRow *datum_row_;
    ObArray<ObCGBlockFile *> *cg_block_file_arr_;
    ObArray<ObCGRowFile *> *cg_row_file_arr_;
    blocksstable::ObBatchDatumRows *bdrs_;
    table::ObTableLoadTabletObjRowArray *row_array_;
    storage::ObTaskBatchInfo *batch_info_;
  };
};

class ObPipelineOperator
{
public:
  enum ResultState
  {
    INVALID_VALUE = 0, // deafult invalid val
    NEED_MORE_INPUT = 1, // means the input chunk is fully processed, the output chunk maybe valid
    HAVE_MORE_OUTPUT = 2, // means the input chunk is partial processed, the output chunk is valid and need execute again to get next output chunk
  };
public:
  explicit ObPipelineOperator(ObPipeline *pipeline):
    pipeline_(pipeline)
  {}
  virtual ~ObPipelineOperator() {}
  virtual bool is_valid() const { return false; }
  virtual int execute_op(const ObChunk &input_chunk,
                         ResultState &result_state,
                         ObChunk &output_chunk);
  share::ObIDag *get_dag();

  DECLARE_PURE_VIRTUAL_TO_STRING;

protected:
  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) = 0;
  virtual int try_execute_finish(const ObChunk &input_chunk,
                                 ResultState &result_state,
                                 ObChunk &output_chunk);

protected:
  ObPipeline *pipeline_;
};

class ObPipeline: public share::ObITask
{
public:
  explicit ObPipeline(const share::ObITask::ObITaskType &task_type)
    : ObITask(task_type)
  {}
  ~ObPipeline() {}
  int add_op(ObPipelineOperator *op);
  int push(const ObChunk &chunk_data);
  virtual int process() override { return common::OB_NOT_IMPLEMENT; }

private:
  int execute_ops(const int64_t start_pos, const ObChunk &chunk_data);

protected:
  common::ObArray<ObPipelineOperator *> ops_;
};

}  // end namespace storage
}  // end namespace oceanbase
#endif//_OCEANBASE_STORAGE_DDL_OB_PIPELINE_H_
