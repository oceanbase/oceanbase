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

#ifndef OCEANBASE_STORAGE_OB_CG_MICRO_BLOCK_WRITE_OP_H_
#define OCEANBASE_STORAGE_OB_CG_MICRO_BLOCK_WRITE_OP_H_

#include "lib/ob_define.h"
#include "storage/blocksstable/ob_dag_macro_block_writer.h"
#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/ddl/ob_writer_args_struct.h"

namespace oceanbase
{
namespace storage
{

class ObCGMicroBlockWriter final
{
public:
  explicit ObCGMicroBlockWriter() :
    is_inited_(false),
    cg_block_file_writer_(),
    writer_args_(),
    cg_micro_block_writer_() { }
  ~ObCGMicroBlockWriter() = default;
  int open(const ObWriteMacroParam &param,
           ObCGBlockFile *cg_block_file);
  int close();
  void reset();
  int append_row(const blocksstable::ObDatumRow &row,
                 const blocksstable::ObMacroBlockDesc *curr_macro_desc = nullptr);
  int append_batch(const blocksstable::ObBatchDatumRows &datum_rows,
                   const blocksstable::ObMacroBlockDesc *curr_macro_desc = nullptr);

private:
  DISALLOW_COPY_AND_ASSIGN(ObCGMicroBlockWriter);

private:
  bool is_inited_;
  ObCGBlockFileWriter cg_block_file_writer_;
  ObWriterArgs writer_args_;
  ObDagTempMacroBlockWriter cg_micro_block_writer_;
};

class ObCGMicroBlockWriteOp : public ObPipelineOperator
{
public:
  explicit ObCGMicroBlockWriteOp(ObPipeline *pipeline) :
    ObPipelineOperator(pipeline),
    is_inited_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    output_data_(nullptr),
    allocator_(ObMemAttr(MTL_ID(), "CGMBWriteOp")),
    cg_block_file_arr_(),
    cg_micro_block_writer_(),
    unique_index_id_(0)
  {
    cg_block_file_arr_.set_block_allocator(ModulePageAllocator(allocator_));
  }
  virtual ~ObCGMicroBlockWriteOp()
  {
    reset();
  }
  virtual bool is_valid() const override;
  int init(const ObTabletID &tablet_id, const int64_t slice_idx);
  void reset();
  TO_STRING_KV(K(is_inited_), K(tablet_id_), K(slice_idx_), K(cg_block_file_arr_), K(unique_index_id_));

public:
  static constexpr int64_t WRITE_MACRO_THRESHOLD = OB_DEFAULT_MACRO_BLOCK_SIZE;

protected:
  virtual int execute(const ObChunk &input_chunk,
                      ResultState &result_state,
                      ObChunk &output_chunk) override;

private:
  int append_cg_row_file_to_writer(ObCGRowFile *&cg_row_file);
  int try_generate_output_chunk(const ObChunk &input_chunk,
                                ResultState &result_state,
                                ObChunk &output_chunk);
  DISALLOW_COPY_AND_ASSIGN(ObCGMicroBlockWriteOp);

private:
  bool is_inited_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  ObArray<ObCGBlockFile *> *output_data_;
  ObArenaAllocator allocator_;
  ObArray<ObCGBlockFile *> cg_block_file_arr_;
  ObCGMicroBlockWriter cg_micro_block_writer_;
  uint64_t unique_index_id_; // for report conflict key if need
};

} //end storage
} // end oceanbase

#endif //OCEANBASE_STORAGE_OB_CG_MICRO_BLOCK_WRITE_OP_H_
