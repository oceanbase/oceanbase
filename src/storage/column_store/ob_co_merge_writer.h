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
#include "ob_co_merge_log.h"


namespace oceanbase
{
namespace compaction
{
class ObWriteHelper final
{
public:
  ObWriteHelper()
    : data_store_desc_(),
      macro_writer_(false/*is_need_macro_buffer*/),
      projector_(),
      skip_project_(false),
      sstable_merge_block_info_array_()
  {}
  ~ObWriteHelper() = default;
  int init(
      ObBasicTabletMergeCtx &ctx,
      const ObMergeParameter &merge_param,
      const int64_t parallel_idx,
      const ObStorageColumnGroupSchema &cg_schema,
      ObTabletMergeInfo &merge_info,
      ObIAllocator &allocator);
  int append(const blocksstable::ObDatumRow &row, const bool direct_append = false);
  int append_micro_block(const blocksstable::ObMicroBlock &micro_block, const int64_t sstable_idx);
  int append_macro_block(const ObMacroBlockDesc &macro_desc, const ObMicroBlockData *micro_block_data, const int64_t sstable_idx);
  int project(const blocksstable::ObDatumRow &row, blocksstable::ObDatumRow &result_row, bool &is_all_nop) const
  {
    return projector_.project(row, result_row, is_all_nop);
  }
  int project(const blocksstable::ObDatumRow &row, const blocksstable::ObDatumRow *&result_row);
  bool need_project() const { return !skip_project_; }
  int check_data_macro_block_need_merge(const ObMacroBlockDesc &macro_desc, bool &need_rewrite)
  {
    return macro_writer_.check_data_macro_block_need_merge(macro_desc, need_rewrite);
  }
  const common::ObIArray<share::schema::ObColDesc>& get_col_desc_array() const { return data_store_desc_.get_col_desc_array(); }
  bool is_cg() const { return data_store_desc_.is_cg(); }
  int end_write(ObTabletMergeInfo &merge_info);
  TO_STRING_KV(K_(data_store_desc), K_(projector), K_(skip_project))
private:
  blocksstable::ObDataStoreDesc data_store_desc_;
  blocksstable::ObMacroBlockWriter macro_writer_;
  ObCOMergeProjector projector_;
  bool skip_project_; // skip default_row or major_row project
  ObSEArray<ObSSTableMergeBlockInfo, 1> sstable_merge_block_info_array_;
};

struct ObCOMajorMergeIter
{
  ObCOMajorMergeIter(ObMergeIter* iter, const bool need_project, ObIAllocator &allocator)
    : allocator_(allocator),
      iter_(iter),
      need_project_(need_project)
  {}
  ~ObCOMajorMergeIter() { reset(); }
  void reset()
  {
    if (OB_NOT_NULL(iter_)) {
      iter_->~ObMergeIter();
      allocator_.free(iter_);
      iter_ = nullptr;
    }
  }
  int next();
  OB_INLINE bool is_iter_end()
  {
    return nullptr == iter_ ? true : iter_->is_iter_end();
  }
  TO_STRING_KV(K_(iter), K_(need_project))
  ObIAllocator &allocator_;
  ObMergeIter* iter_;
  const bool need_project_;
};

static const int64_t DEFAULT_CO_MERGE_ITER_COUNT = 16;
static const int64_t DEFAULT_CO_MERGE_ITER_ARRAY_SIZE = DEFAULT_CO_MERGE_ITER_COUNT * sizeof(ObCOMajorMergeIter *);
typedef common::ObSEArray<ObCOMajorMergeIter*, DEFAULT_CO_MERGE_ITER_COUNT> CO_MAJOR_MERGE_ITER_ARRAY;
class ObCOMergeWriter
{
public:
  ObCOMergeWriter(bool need_co_scan = false)
    : allocator_("MergeWriter"),
      fuser_(allocator_),
      iters_(DEFAULT_CO_MERGE_ITER_ARRAY_SIZE, ModulePageAllocator(allocator_)),
      default_row_(),
      is_inited_(false),
      need_co_scan_(need_co_scan),
      error_location_(nullptr),
      last_skip_major_idx_(-1),
      last_skip_major_row_id_(-1)
  {}
  virtual ~ObCOMergeWriter();
  // for rows writer
  virtual int init(
      ObBasicTabletMergeCtx &ctx,
      const blocksstable::ObDatumRow &default_row,
      const ObMergeParameter &merge_param,
      const int64_t parallel_idx,
      const ObITableReadInfo *full_read_info,
      const int64_t cg_idx,
      ObTabletMergeInfo &merge_info,
      ObIArray<ObITable*> &tables)
  { return OB_NOT_SUPPORTED; }

  // for single writer
  virtual int init(
      ObBasicTabletMergeCtx &ctx,
      const blocksstable::ObDatumRow &default_row,
      const ObMergeParameter &merge_param,
      const ObITableReadInfo *full_read_info,
      const int64_t parallel_idx,
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
      ObTabletMergeInfo **merge_infos,
      ObIArray<ObITable*> &tables)
  { return OB_NOT_SUPPORTED; }
  bool is_init() const { return is_inited_; }
  virtual int replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow *&row);
  virtual int end_write(ObTabletMergeInfo &merge_info) { return OB_NOT_SUPPORTED; }
  virtual int end_write(ObTabletMergeInfo **merge_infos)
  { return OB_NOT_SUPPORTED; }
  static int init_default_row(
    ObIAllocator &allocator,
    const ObMergeParameter &merge_param,
    ObTabletMergeInfo &merge_info,
    blocksstable::ObDatumRow &default_row);

  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(default_row))

protected:
  virtual int replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row);
  // replay major without incremental row
  virtual int replay_mergelog(const ObMergeLog &mergelog);
  int basic_init(
      const blocksstable::ObDatumRow &default_row,
      const ObMergeParameter &merge_param,
      const int64_t column_cnt);
  int init_merge_iter(
      const ObMergeParameter &merge_param,
      const ObITableReadInfo *read_info,
      const ObStorageColumnGroupSchema *cg_schema,
      ObITable *table,
      const int64_t sstable_idx,
      const bool add_column = false,
      const bool major_need_project = false,
      const bool need_full_merge = false);
  int move_iters_next();
  int get_curr_major_iter(const ObMergeLog &mergelog, ObCOMajorMergeIter *&merge_iter);
  void dump_info() const;
  int process_macro_rewrite(ObCOMajorMergeIter *iter);
  int append_iter_curr_row_or_range(ObCOMajorMergeIter *iter);
  int process_mergelog_row(ObCOMajorMergeIter *iter, const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row);
  bool check_is_all_nop(const blocksstable::ObDatumRow &row);
  virtual bool is_base_cg_writer() const { return false; }
  int replay_last_skip_major(const int64_t current_major_idx);
private:
  int compare(
      ObCOMajorMergeIter *iter,
      const ObMergeLog &mergelog,
      int64_t &cmp_ret,
      const blocksstable::ObDatumRow &row,
      bool &skip_curr_row) const;
  virtual int get_curr_major_row(ObCOMajorMergeIter &iter, const blocksstable::ObDatumRow *&row);
  virtual int process(ObCOMajorMergeIter *iter,
                      const ObMacroBlockDesc &macro_desc,
                      const ObMicroBlockData *micro_block_data) = 0;
  virtual int process(const blocksstable::ObMicroBlock &micro_block, const int64_t sstable_idx) = 0;
  virtual int process(const blocksstable::ObDatumRow &row) = 0;
  virtual bool is_cg() const { return false; } //temp code
protected:
  compaction::ObLocalArena allocator_;
  ObDefaultMergeFuser fuser_;
  CO_MAJOR_MERGE_ITER_ARRAY iters_;
  blocksstable::ObDatumRow default_row_;
  bool is_inited_;
  bool need_co_scan_;
  share::ObDiagnoseLocation *error_location_;
  // for skip update nop or no need cg opt
  int64_t last_skip_major_idx_;
  int64_t last_skip_major_row_id_;
  uint64_t compat_version_;
};

class ObCOMergeRowWriter : public ObCOMergeWriter
{
public:
  ObCOMergeRowWriter(bool need_co_scan = false)
    : ObCOMergeWriter(need_co_scan),
      progressive_merge_helper_(nullptr),
      write_helper_(),
      row_(),
      single_read_info_(),
      is_using_column_tmp_file_(false),
      cg_wrappers_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_)
  {}
  virtual ~ObCOMergeRowWriter();
  virtual int init(
      ObBasicTabletMergeCtx &ctx,
      const blocksstable::ObDatumRow &default_row,
      const ObMergeParameter &merge_param,
      const int64_t parallel_idx,
      const ObITableReadInfo *full_read_info,
      const int64_t cg_idx,
      ObTabletMergeInfo &merge_info,
      ObIArray<ObITable*> &tables);
  virtual int end_write(ObTabletMergeInfo &merge_info) override;
  INHERIT_TO_STRING_KV("ObCOMergeRowWriter", ObCOMergeWriter, K_(write_helper));
protected:
  virtual int replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row) override;
private:
  virtual int get_curr_major_row(ObCOMajorMergeIter &iter, const blocksstable::ObDatumRow *&row) override;
  virtual int process(ObCOMajorMergeIter *iter, const ObMacroBlockDesc &macro_desc, const ObMicroBlockData *micro_block_data) override;
  virtual int process(const blocksstable::ObMicroBlock &micro_block, const int64_t sstable_idx) override;
  virtual int process(const blocksstable::ObDatumRow &row) override;
  virtual bool is_cg() const override { return write_helper_.is_cg(); }
  int choose_read_info_for_old_major(
   const ObMergeParameter &merge_param,
   const ObITableReadInfo &full_read_info,
   const ObStorageColumnGroupSchema &cg_schema,
   const ObITableReadInfo *&read_info);
  bool is_single_major() const { return iters_.count() == 1; }
  int get_writer_param(
      const ObMergeParameter &merge_param,
      const ObStorageColumnGroupSchema *cg_schema,
      const int64_t cg_idx,
      ObSSTable *sstable,
      ObITable *&table,
      bool &add_column);
protected:
  ObProgressiveMergeHelper *progressive_merge_helper_;
  ObWriteHelper write_helper_;
  blocksstable::ObDatumRow row_;
  ObTableReadInfo single_read_info_;
  bool is_using_column_tmp_file_;
  ObSEArray<storage::ObSSTableWrapper, 16, common::ObIAllocator&> cg_wrappers_;
};

class ObCOMergeBaseRowWriter : public ObCOMergeRowWriter
{
public:
  ObCOMergeBaseRowWriter(bool need_co_scan = false)
    : ObCOMergeRowWriter(need_co_scan),
      merge_iter_(nullptr, false, allocator_)
  {}
  virtual ~ObCOMergeBaseRowWriter()
  {
    merge_iter_.iter_ = nullptr; // don't reset iter here
  }
  void set_merge_iter(ObMergeIter *iter) { merge_iter_.iter_ = iter; }
protected:
  virtual int replay_mergelog(const ObMergeLog &mergelog, const blocksstable::ObDatumRow &row) override;
  virtual int replay_mergelog(const ObMergeLog &mergelog) override;
  virtual bool is_base_cg_writer() const override { return true; }
private:
  ObCOMajorMergeIter merge_iter_;
};

// loop cgs to project & write row into cg
class ObCOMergeSingleWriter : public ObCOMergeWriter
{
public:
  ObCOMergeSingleWriter(const int64_t start_cg_idx, const int64_t end_cg_idx, const bool need_co_scan)
    : ObCOMergeWriter(need_co_scan),
      start_cg_idx_(start_cg_idx),
      end_cg_idx_(end_cg_idx),
      write_helpers_(OB_MALLOC_NORMAL_BLOCK_SIZE, allocator_),
      base_cg_idx_(-1),
      ignore_base_cg_(false)
  {}
  virtual ~ObCOMergeSingleWriter();
  virtual int init(
      ObBasicTabletMergeCtx &ctx,
      const blocksstable::ObDatumRow &default_row,
      const ObMergeParameter &merge_param,
      const ObITableReadInfo *full_read_info,
      const int64_t parallel_idx,
      const common::ObIArray<ObStorageColumnGroupSchema> &cg_array,
      ObTabletMergeInfo **merge_infos,
      ObIArray<ObITable*> &tables) override;
  virtual int end_write(ObTabletMergeInfo **merge_infos) override;
private:
  virtual int process(ObCOMajorMergeIter *iter,
                      const ObMacroBlockDesc &macro_desc,
                      const ObMicroBlockData *micro_block_data) override
  {
    return OB_NOT_SUPPORTED;
  }
  virtual int process(const blocksstable::ObMicroBlock &micro_block, const int64_t sstable_idx) override { return OB_NOT_SUPPORTED; };
  virtual int process(const blocksstable::ObDatumRow &row) override;

private:
  const static int64_t DEFAULT_CG_BATCH_SIZE = 10;
private:
  const int64_t start_cg_idx_;
  const int64_t end_cg_idx_;
  ObSEArray<ObWriteHelper*, DEFAULT_CG_BATCH_SIZE, common::ObIAllocator&> write_helpers_;
  int64_t base_cg_idx_;
  bool ignore_base_cg_;
};
} //compaction
} //oceanbase


#endif
