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

#ifndef OCEANBASE_STORAGE_OB_FTS_MACRO_BLOCK_WRITE_OP_H_
#define OCEANBASE_STORAGE_OB_FTS_MACRO_BLOCK_WRITE_OP_H_

#include "share/scheduler/ob_tenant_dag_scheduler.h"
#include "storage/ddl/ob_cg_block_tmp_files_iterator.h"
#include "storage/ddl/ob_cg_macro_block_writer.h"
#include "storage/ddl/ob_column_clustered_dag.h"
#include "storage/ddl/ob_pipeline.h"
#include "storage/blocksstable/ob_dag_macro_block_writer.h"
#include "storage/ddl/ob_writer_args_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDAGFtsMacroBlockWriteOp : public ObPipelineOperator
{
public:
  explicit ObDAGFtsMacroBlockWriteOp(ObPipeline *pipeline) :
    ObPipelineOperator(pipeline),
    is_inited_(false),
    tablet_id_(ObTabletID::INVALID_TABLET_ID),
    slice_idx_(-1),
    start_seq_(-1),
    row_cnt_(0),
    macro_block_writer_()
  {
  }
  virtual ~ObDAGFtsMacroBlockWriteOp() = default;
  virtual bool is_valid() const override;
  int init(const ObTabletID &tablet_id, const int64_t slice_idx);
  TO_STRING_KV(K(is_inited_), K(tablet_id_), K(slice_idx_), K(start_seq_), K(row_cnt_), K(macro_block_writer_));

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
  DISALLOW_COPY_AND_ASSIGN(ObDAGFtsMacroBlockWriteOp);

private:
  bool is_inited_;
  ObTabletID tablet_id_;
  int64_t slice_idx_;
  int64_t start_seq_;
  int64_t row_cnt_;
  ObCgMacroBlockWriter macro_block_writer_;
};

} //end storage
} // end oceanbase

#endif //OCEANBASE_STORAGE_OB_FTS_MACRO_BLOCK_WRITE_OP_H_
