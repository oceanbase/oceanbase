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

#include "storage/ddl/ob_fts_macro_block_write_op.h"
#include "storage/ddl/ob_ddl_tablet_context.h"
#include "storage/ddl/ob_cg_micro_block_write_op.h"
#include "storage/ddl/ob_macro_meta_store_manager.h"
#include "storage/ddl/ob_tablet_ddl_kv_mgr.h"
#include "storage/ddl/ob_tablet_slice_writer.h"
#include "share/schema/ob_schema_struct.h"

namespace oceanbase
{
using namespace oceanbase::blocksstable;
namespace storage
{
bool ObDAGFtsMacroBlockWriteOp::is_valid() const
{
  return is_inited_ && tablet_id_.is_valid() && slice_idx_ >= 0;
}

int ObDAGFtsMacroBlockWriteOp::init(const ObTabletID &tablet_id, const int64_t slice_idx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("initialized twice", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()) || slice_idx < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("there are invalid argument", K(ret), K(tablet_id), K(slice_idx));
  } else {
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    if (OB_ISNULL(ddl_dag)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ddl dag is null", K(ret), K(tablet_id), K(slice_idx));
    } else {
      const ObDDLTaskParam &ddl_task_param = ddl_dag->get_ddl_task_param();
      const int64_t cg_idx = 0;
      const int64_t row_offset = 0;
      ObMacroDataSeq start_seq;
      ObITable::TableKey table_key;
      table_key.tablet_id_ = tablet_id;
      table_key.version_range_.snapshot_version_ = ddl_task_param.snapshot_version_;
      table_key.column_group_idx_ = cg_idx;
      table_key.table_type_ = ObITable::MAJOR_SSTABLE;
      if (FAILEDx(ObDDLUtil::init_macro_block_seq(ddl_dag->get_direct_load_type(),
                                                  tablet_id,
                                                  slice_idx,
                                                  start_seq))) {
        LOG_WARN("fail to initialize start seqence", K(ret), K(ddl_dag->get_direct_load_type()), K(tablet_id), K(slice_idx));
      } else {
        ObWriteMacroParam write_macro_param;
        write_macro_param.start_sequence_ = start_seq;
        write_macro_param.row_offset_ = row_offset;
        if (OB_FAIL(ObDDLUtil::fill_writer_param(tablet_id,
                                                 slice_idx,
                                                 cg_idx,
                                                 ddl_dag,
                                                 write_macro_param))) {
          LOG_WARN("fail to fill write macro param",
              K(ret), KPC(ddl_dag), K(tablet_id), K(slice_idx), K(cg_idx));
        } else if (OB_FAIL(macro_block_writer_.init(write_macro_param, table_key, start_seq, row_offset))) {
          LOG_WARN("failed to init macro block writer", K(ret), K(tablet_id), K(slice_idx), K(write_macro_param));
        } else {
          tablet_id_ = tablet_id;
          slice_idx_ = slice_idx;
          start_seq_ = macro_block_writer_.get_last_macro_seq();
          row_cnt_ = 0;
          is_inited_ = true;
        }
      }
    }
  }
  return ret;
}

int ObDAGFtsMacroBlockWriteOp::execute(const ObChunk &input_chunk,
                                       ResultState &result_state,
                                       ObChunk &output_chunk)
{
  int ret = OB_SUCCESS;
  output_chunk.reset();
  result_state = ObPipelineOperator::NEED_MORE_INPUT;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObDAGFtsMacroBlockWriteOp is not initialized", K(ret));
  } else if (OB_UNLIKELY(!input_chunk.is_valid()) ||
                         (!input_chunk.is_end_chunk() && !input_chunk.is_direct_load_batch_datum_rows_type()
                         && !input_chunk.is_batch_datum_rows_type())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("input chunk is not valid", K(ret), K(input_chunk));
  } else if (!input_chunk.is_end_chunk()) {
    // For fulltext index, accumulate written row count here.
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    if (OB_NOT_NULL(ddl_dag)) {
      const ObDDLTableSchema *use_schema = nullptr;
      if (OB_SUCC(ddl_dag->get_ddl_table_schema(tablet_id_, use_schema)) && OB_NOT_NULL(use_schema)) {
        const share::schema::ObIndexType index_type = static_cast<share::schema::ObIndexType>(use_schema->table_item_.index_type_);
        int64_t row_cnt = 0;
        if (input_chunk.is_batch_datum_rows_type() && OB_NOT_NULL(input_chunk.bdrs_)) {
          row_cnt = input_chunk.bdrs_->row_count_;
        } else if (input_chunk.is_direct_load_batch_datum_rows_type() && OB_NOT_NULL(input_chunk.direct_load_batch_rows_)) {
          row_cnt = input_chunk.direct_load_batch_rows_->datum_rows_.row_count_;
        }
        ObFTSBuildStat fts_stat;
        if (share::schema::is_fts_doc_word_aux(index_type)) {
          fts_stat.forward_written_row_cnt_ = row_cnt;
        } else if (share::schema::is_fts_index_aux(index_type)) {
          fts_stat.inverted_written_row_cnt_ = row_cnt;
        }
        ddl_dag->update_fts_build_stat(fts_stat);
      }
    }
    if (input_chunk.is_batch_datum_rows_type() && OB_FAIL(macro_block_writer_.append_batch(*input_chunk.bdrs_))) {
      LOG_WARN("failed to append batch", K(ret), K(tablet_id_), K(slice_idx_), K(input_chunk.bdrs_));
    } else if (input_chunk.is_direct_load_batch_datum_rows_type() && OB_FAIL(macro_block_writer_.append_batch(input_chunk.direct_load_batch_rows_->datum_rows_))) {
      LOG_WARN("failed to append batch", K(ret), K(tablet_id_), K(slice_idx_), K(input_chunk.direct_load_batch_rows_->datum_rows_));
    } else {
      row_cnt_ += input_chunk.is_batch_datum_rows_type() ? input_chunk.bdrs_->row_count_ :
        (input_chunk.is_direct_load_batch_datum_rows_type() ? input_chunk.direct_load_batch_rows_->datum_rows_.row_count_ : 0);
    }
  } else {
    ObDDLIndependentDag *ddl_dag = dynamic_cast<ObDDLIndependentDag *>(get_dag());
    ObSEArray<ObCgMacroBlockWriter *, 1> macro_block_writers;
    ObSEArray<int64_t, 1> start_seqs;
    if (OB_FAIL(macro_block_writers.push_back(&macro_block_writer_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(start_seqs.push_back(start_seq_))) {
      LOG_WARN("failed to push back", K(ret));
    } else if (OB_FAIL(ObTabletSliceWriter::close_and_set_remain_block(tablet_id_, slice_idx_, macro_block_writers, start_seqs, ddl_dag))) {
      LOG_WARN("failed to close writer", K(ret), K(tablet_id_), K(slice_idx_));
    } else {
      LOG_INFO("fts macro block op close", K(ret), K(tablet_id_), K(slice_idx_), K(start_seq_), K(row_cnt_));
    }
  }
  return ret;
}

} // end namespace storage
} // end namespace oceanbase
