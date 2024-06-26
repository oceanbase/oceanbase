/**
 * Copyright (c) 2022 OceanBase
 * OceanBase is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_STORAGE_OB_CO_MERGE_WRITER_H_
#define OB_STORAGE_OB_CO_MERGE_WRITER_H_

#include "storage/blocksstable/ob_sstable.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/compaction/ob_partition_merger.h"
#include "storage/access/ob_table_read_info.h"


namespace oceanbase
{
namespace compaction
{
struct ObMergeLog {
  enum OpType{
    INSERT,
    UPDATE,
    DELETE
  };
  TO_STRING_KV(K_(op), K_(row_id))

  OpType op_;
  int64_t row_id_;
};

class ObCOMergeProjector final
{
public:
  ObCOMergeProjector()
    : is_inited_(false),
      projector_(),
      project_row_()
    {}
  ~ObCOMergeProjector() = default;
  int init(const ObStorageColumnGroupSchema &cg_schema);
  const blocksstable::ObDatumRow &get_project_row() const { return project_row_; }
  bool is_all_nop(const blocksstable::ObDatumRow &row) const;
  int project(const blocksstable::ObDatumRow &row);
  int project(const blocksstable::ObDatumRow &row, blocksstable::ObDatumRow &result_row, bool &is_all_nop) const;
  TO_STRING_KV(K_(is_inited), K_(projector), K_(project_row))
private:
  void clean_project_row();
private:
  int is_inited_;
  ObArray<uint16_t> projector_;
  blocksstable::ObDatumRow project_row_;
};

class ObWriteHelper final
{
public:
  ObWriteHelper()
    : merge_info_(),
      data_store_desc_(),
      macro_writer_(),
      projector_(),
      skip_project_(false)
  {}
  ~ObWriteHelper() = default;
  int init(
      const ObMergeParameter &merge_param,
      const int64_t parallel_idx,
      const int64_t cg_idx,
      const ObStorageColumnGroupSchema &cg_schema,
      ObTabletMergeInfo &merge_info);
  int append(const blocksstable::ObDatumRow &row, const bool direct_append = false);
  int append_micro_block(const blocksstable::ObMicroBlock &micro_block) { return macro_writer_.append_micro_block(micro_block); }
  int append_macro_block(const ObMacroBlockDesc &macro_desc) { return macro_writer_.append_macro_block(macro_desc); }
  int project(const blocksstable::ObDatumRow &row, blocksstable::ObDatumRow &result_row, bool &is_all_nop) const
  {
    return projector_.project(row, result_row, is_all_nop);
  }
  bool need_project() const { return !skip_project_; }
  int check_data_macro_block_need_merge(const ObMacroBlockDesc &macro_desc, bool &need_rewrite)
  {
    return macro_writer_.check_data_macro_block_need_merge(macro_desc, need_rewrite);
  }
  const common::ObIArray<share::schema::ObColDesc>& get_col_desc_array() const { return data_store_desc_.get_col_desc_array(); }
  bool is_cg() const { return data_store_desc_.is_cg(); }
  int end_write(ObTabletMergeInfo &merge_info);
  TO_STRING_KV(K_(merge_info), K_(data_store_desc), K_(projector), K_(skip_project))
private:
  ObSSTableMergeInfo merge_info_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  blocksstable::ObMacroBlockWriter macro_writer_;
  ObCOMergeProjector projector_;
  bool skip_project_;
};

class ObCOMergeWriter
{
public:
  ObCOMergeWriter(bool iter_co_build_row_store = false)
    : allocator_("MergeWriter"),
      fuser_(allocator_),
      iter_(nullptr),
      default_row_(),
      is_inited_(false),
      iter_co_build_row_store_(iter_co_build_row_store),
      error_location_(nullptr)
  {}
  virtual ~ObCOMergeWriter();
  virtual int init(
        const blocksstable::ObDatumRow &default_row,
        const ObMergeParameter &merge_param,
        const int64_t parallel_idx,
        const ObITableReadInfo *read_info,
        const ObStorageColumnGroupSchema &cg_schema,
        const int64_t cg_idx,
        ObTabletMergeInfo &merge_info,
        ObITable *table = nullptr,
        const bool add_column = false)
  { return OB_NOT_SUPPORTED; }

  virtual int init(
      const blocksstable::ObDatumRow &default_row,
      const ObMergeParameter &merge_param,
      const ObITableReadInfo *full_read_info,
      const int64_t parallel_idx,
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
      const int64_t start_cg_idx,
      const int64_t end_cg_idx,
      ObTabletMergeInfo **merge_infos,
      ObSSTable *row_table)
  { return OB_NOT_SUPPORTED; }
  virtual int replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row);
  bool is_init() const { return is_inited_; }
  int append_residual_data();
  virtual int end_write(const int64_t task_idx, ObTabletMergeInfo &merge_info) { return OB_NOT_SUPPORTED; }
  virtual int end_write(const int64_t start, const int64_t end, ObTabletMergeInfo **merge_infos)
  { return OB_NOT_SUPPORTED; }
  VIRTUAL_TO_STRING_KV(K_(is_inited), KPC_(iter), K_(default_row))

protected:
  int basic_init(
        const blocksstable::ObDatumRow &default_row,
        const ObMergeParameter &merge_param,
        const ObITableReadInfo *read_info,
        const int64_t column_cnt,
        ObITable *table,
        const bool add_column = false,
        const bool only_use_row_table = false);
  void dump_info() const;
  int process_macro_rewrite();
  int append_iter_curr_row_or_range();
private:
  int compare(const ObMergeLog &mergelog, int64_t &cmp_ret, const blocksstable::ObDatumRow &row, bool &skip_curr_row) const;
  int process_mergelog_row(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row);
  virtual int process(const ObMacroBlockDesc &macro_desc) = 0;
  virtual int process(const blocksstable::ObMicroBlock &micro_block) = 0;
  virtual int process(const blocksstable::ObDatumRow &row) = 0;
  virtual bool is_cg() const { return false; } //temp code
protected:
  compaction::ObLocalArena allocator_;
  ObDefaultMergeFuser fuser_;
  ObMergeIter *iter_;
  blocksstable::ObDatumRow default_row_;
  bool is_inited_;
  bool iter_co_build_row_store_;
  share::ObDiagnoseLocation *error_location_;
};

class ObCOMergeRowWriter : public ObCOMergeWriter
{
public:
  ObCOMergeRowWriter(bool iter_co_build_row_store = false)
    : ObCOMergeWriter(iter_co_build_row_store),
      progressive_merge_helper_(nullptr),
      write_helper_(),
      row_(),
      single_read_info_()
  {}
  ~ObCOMergeRowWriter();
  virtual int init(
        const blocksstable::ObDatumRow &default_row,
        const ObMergeParameter &merge_param,
        const int64_t idx,
        const ObITableReadInfo *read_info,
        const ObStorageColumnGroupSchema &cg_schema,
        const int64_t cg_idx,
        ObTabletMergeInfo &merge_info,
        ObITable *table = nullptr,
        const bool add_column = false);
  virtual int replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row) override;
  virtual int end_write(const int64_t task_idx, ObTabletMergeInfo &merge_info) override { return write_helper_.end_write(merge_info); }
  INHERIT_TO_STRING_KV("ObCOMergeRowWriter", ObCOMergeWriter, K_(write_helper))
private:
  virtual int process(const ObMacroBlockDesc &macro_desc) override;
  virtual int process(const blocksstable::ObMicroBlock &micro_block) override;
  virtual int process(const blocksstable::ObDatumRow &row) override;
  virtual bool is_cg() const override { return write_helper_.is_cg(); }

private:
  ObProgressiveMergeHelper *progressive_merge_helper_;
  ObWriteHelper write_helper_;
  blocksstable::ObDatumRow row_;
  ObTableReadInfo single_read_info_;
};

class ObCOMergeSingleWriter : public ObCOMergeWriter
{
public:
  ObCOMergeSingleWriter()
    : ObCOMergeWriter(),
      write_helpers_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
  {}
  virtual ~ObCOMergeSingleWriter();
  virtual int init(
      const blocksstable::ObDatumRow &default_row,
      const ObMergeParameter &merge_param,
      const ObITableReadInfo *full_read_info,
      const int64_t parallel_idx,
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
      const int64_t start_cg_idx,
      const int64_t end_cg_idx,
      ObTabletMergeInfo **merge_infos,
      ObSSTable *row_table) override;
  virtual int end_write(const int64_t start, const int64_t end, ObTabletMergeInfo **merge_infos) override;
private:
  virtual int process(const ObMacroBlockDesc &macro_desc) override { return OB_NOT_SUPPORTED; }
  virtual int process(const blocksstable::ObMicroBlock &micro_block) override { return OB_NOT_SUPPORTED; };
  virtual int process(const blocksstable::ObDatumRow &row) override;

private:
  const static int64_t DEFAULT_CG_BATCH_SIZE = 10;
private:
  ObSEArray<ObWriteHelper*, DEFAULT_CG_BATCH_SIZE, common::ObIAllocator&> write_helpers_;
};
} //compaction
} //oceanbase


#endif
