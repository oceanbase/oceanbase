/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */



#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_DAG_MACRO_BLOCK_WRITER_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_DAG_MACRO_BLOCK_WRITER_H_
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/blocksstable/ob_macro_block.h"
#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/blocksstable/ob_dag_micro_block_iterator.h"
#include "storage/blocksstable/ob_simplified_sstable_macro_block_header.h"



namespace oceanbase
{
namespace blocksstable
{
// simplified macro block store struct
//  |- ObSimplifiedSSTableMacroBlockHeader
//  |- MicroBlock 1
//  |- MicroBlock 2
//  |- MicroBlock N
//  |- IndexMicroBlock

/**
* ---------------------------------------ObDagTempMacroBlockWriter-----------------------------------
*/

// ObDagTempMacroBlockWriter is used in the first stage of DAG to write a batch of rows
// into a CgBlock and generate temporary file.
class ObDagTempMacroBlockWriter final : public ObMacroBlockWriter
{
public:
  ObDagTempMacroBlockWriter();
  virtual ~ObDagTempMacroBlockWriter();
  virtual void reset() override;
  int open(
    const ObDataStoreDesc &data_store_desc,
    const int64_t parallel_idx,
    const blocksstable::ObMacroSeqParam &macro_seq_param,
    ObSSTablePrivateObjectCleaner &object_cleaner,
    ObCGBlockFileWriter *cg_block_writer);
protected:
  virtual bool is_alloc_block_needed() const override { return false; }
  virtual bool need_write_macro_meta() const override { return false; }
private:
  ObDagTempMacroFlusher dag_temp_macro_flusher_;
};


/**
* -----------------------------------------ObDagMacroBlockWriter----------------------------------------
*/

// ObDagMacroBlockWriter is responsible for aggregating multiple cgblocks into a macro block
// and flush to disk during the second stage of DAG.
class ObDagMacroBlockWriter final : protected ObMacroBlockWriter
{
public:
  ObDagMacroBlockWriter();
  virtual ~ObDagMacroBlockWriter();
  virtual void reset() override;
  int open(
      const ObDataStoreDesc &data_store_desc,
      const int64_t parallel_idx,
      const blocksstable::ObMacroSeqParam &macro_seq_param,
      ObSSTablePrivateObjectCleaner &object_cleaner,
      ObIMacroBlockFlushCallback *callback);
  int open_for_ss_ddl(
      const ObDataStoreDesc &data_store_desc,
      const int64_t parallel_idx,
      const blocksstable::ObMacroSeqParam &macro_seq_param,
      ObSSTablePrivateObjectCleaner &object_cleaner,
      ObIMacroBlockFlushCallback *callback);
  int append_cg_block(ObCGBlock &cg_block, const int64_t macro_block_fill_ratio);

  using ObMacroBlockWriter::close;
  using ObMacroBlockWriter::get_last_macro_seq;
  OB_INLINE int64_t get_written_row_count() const { return macro_blocks_[current_index_].get_row_count(); }
public:
  static const int64_t DEFAULT_MACRO_BLOCK_FILL_RATIO = 80;
protected:
  virtual int on_buffer_not_enough() override;
private:
  enum class ObDagMacroWriterStage
  {
    NOT_INITED = 0,
    WAITTING_APPEND_CG_BLOCK = 1,
    WAITTING_CLOSE = 2,
  };
private:
  inline void set_dag_stage(const ObDagMacroWriterStage stage) { dag_stage_ = stage; }
  inline ObDagMacroWriterStage get_dag_stage() const { return dag_stage_; }
  int append_macro_block(const ObCGBlock &cg_block);
  int reuse_micro_blocks(ObDagMicroBlockIterator &micro_block_iter, ObIAllocator &allocator);
  bool is_reuse_macro_block(const ObCGBlock &cg_block, const int64_t macro_block_fill_ratio) const;
  bool is_almost_full_macro_block(const int64_t macro_block_fill_threshold) const;
private:
  ObDagMacroWriterStage dag_stage_;
};

}//end namespace blocksstable
}//end namespace oceanbase
#endif