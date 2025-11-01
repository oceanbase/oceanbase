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

#ifndef OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITE_OP_H_
#define OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITE_OP_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ddl/ob_cg_block_tmp_files_iterator.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/blocksstable/ob_dag_macro_block_writer.h"
#include "storage/ddl/ob_writer_args_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDAGCGMacroBlockWriter final
{
public:
  ObDAGCGMacroBlockWriter() :
    is_inited_(false),
    writer_args_(),
    cg_macro_block_writer_() { }
  ~ObDAGCGMacroBlockWriter() = default;
  int open(const ObWriteMacroParam &param);
  int close();
  void reset();
  int append_cg_block(ObCGBlock &cg_block);
  OB_INLINE int64_t get_last_macro_seq()
  {
    return cg_macro_block_writer_.get_last_macro_seq();
  }
  OB_INLINE int64_t get_written_row_count()
  {
    return cg_macro_block_writer_.get_written_row_count();
  }
public:
  static const int64_t MACRO_BLOCK_REUSE_THRESHOLD = 100;

private:
  DISALLOW_COPY_AND_ASSIGN(ObDAGCGMacroBlockWriter);

private:
  bool is_inited_;
  ObWriterArgs writer_args_;
  blocksstable::ObDagMacroBlockWriter cg_macro_block_writer_;
};

class ObDAGCGMacroBlockWriteOp : public ObPipelineOperator
{
public:
  explicit ObDAGCGMacroBlockWriteOp(ObPipeline *pipeline) :
    ObPipelineOperator(pipeline),
    is_inited_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    allocator_(ObMemAttr(MTL_ID(), "DAGCGMBWOp")),
    cg_block_files_iter_arr_(),
    flushed_bitmap_(allocator_),
    cg_macro_block_writer_(),
    start_seqences_(),
    row_offsets_()
    {
      cg_block_files_iter_arr_.set_block_allocator(ModulePageAllocator(allocator_));
      start_seqences_.set_block_allocator(ModulePageAllocator(allocator_));
      row_offsets_.set_block_allocator(ModulePageAllocator(allocator_));
    }
  virtual ~ObDAGCGMacroBlockWriteOp()
  {
    reset();
  }
  virtual bool is_valid() const override;
  int init(const ObTabletID &tablet_id, const int64_t slice_idx);
  void reset();
  int set_remain_block(ObDDLSlice *ddl_slice);
  TO_STRING_KV(K(is_inited_), K(tablet_id_), K(slice_idx_), K(cg_block_files_iter_arr_));

protected:
  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) override;
  virtual int try_execute_finish(const ObChunk &input_chunk,
                                 ResultState &result_state,
                                 ObChunk &output_chunk) override
  {
    UNUSED(input_chunk);
    UNUSED(result_state);
    UNUSED(output_chunk);
    return OB_SUCCESS;
  }

private:
  int append_cg_block_files_to_writer(ObCGBlockFilesIterator *cg_block_files_iter);
  DISALLOW_COPY_AND_ASSIGN(ObDAGCGMacroBlockWriteOp);

private:
  bool is_inited_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  ObArenaAllocator allocator_;
  ObArray<ObCGBlockFilesIterator *> cg_block_files_iter_arr_;
  common::ObBitmap flushed_bitmap_;
  ObDAGCGMacroBlockWriter cg_macro_block_writer_;
  ObArray<blocksstable::ObMacroDataSeq> start_seqences_;
  ObArray<int64_t> row_offsets_;
};

} //end storage
} // end oceanbase

#endif //OCEANBASE_STORAGE_OB_CG_MACRO_BLOCK_WRITE_OP_H_
