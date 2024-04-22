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

#ifndef OB_COMPACTION_PARTITION_MERGE_ITER_H_
#define OB_COMPACTION_PARTITION_MERGE_ITER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_raw_se_array.h"
#include "share/schema/ob_table_schema.h"
#include "storage/ob_i_store.h"
#include "storage/access/ob_sstable_row_whole_scanner.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/compaction/ob_index_block_micro_iterator.h"
#include "storage/blocksstable/index_block/ob_index_block_dual_meta_iterator.h"
#include "storage/blocksstable/index_block/ob_sstable_sec_meta_iterator.h"
#include "storage/blocksstable/ob_datum_row.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/access/ob_table_access_context.h"
#include "storage/access/ob_micro_block_handle_mgr.h"

namespace oceanbase
{
using namespace blocksstable;

namespace storage
{
struct ObTransNodeDMLStat;
}

namespace compaction
{
struct ObMergeParameter;
class ObMergeIter
{
public:
  ObMergeIter() : is_inited_(false) {}
  virtual ~ObMergeIter() = default;
  virtual void reset() { is_inited_ = false; }
  virtual int init(const ObMergeParameter &merge_param,
           const int64_t iter_idx,
           const ObITableReadInfo *read_info) { return OB_NOT_SUPPORTED; }
  virtual int init(const ObMergeParameter &merge_param, ObITable *table, const ObITableReadInfo *read_info) { return OB_NOT_SUPPORTED; }
  virtual OB_INLINE const storage::ObITable *get_table() const = 0;
  virtual int next() = 0;
  virtual bool is_iter_end() const = 0;
  virtual bool is_macro_block_opened() const { return true; };
  virtual bool is_micro_block_opened() const { return true; }
  virtual const blocksstable::ObDatumRow *get_curr_row() const = 0;
  virtual int get_curr_macro_block(const blocksstable::ObMacroBlockDesc *&macro_desc) const
  {
    UNUSEDx(macro_desc);
    return OB_NOT_SUPPORTED;
  }
  virtual int get_curr_micro_block(const blocksstable::ObMicroBlock *&micro_block) {UNUSED(micro_block);  return OB_NOT_SUPPORTED; };
  virtual int64_t get_last_row_id() const = 0;
  virtual int get_curr_row_id(int64_t& row_id) const = 0;

  virtual int get_curr_range_end_rowid(int64_t &row_id) const { UNUSED(row_id); return OB_NOT_SUPPORTED; };
  virtual int open_curr_range(const bool for_rewrite, const bool for_compare = false) { UNUSEDx(for_rewrite, for_compare); return OB_NOT_SUPPORTED; }
  virtual int need_open_curr_range(const blocksstable::ObDatumRow &row, bool &need_open, const int64_t row_id_for_cg = 0)
  { UNUSEDx(row, need_open, row_id_for_cg); return OB_NOT_SUPPORTED; }
  VIRTUAL_TO_STRING_KV(K_(is_inited));
protected:
  bool is_inited_;
};

class ObDefaultRowIter : public ObMergeIter
{
public:
  ObDefaultRowIter(const blocksstable::ObDatumRow &default_row)
    : curr_row_count_(0),
      total_row_count_(0),
      default_row_(default_row)
  {}
  virtual ~ObDefaultRowIter() { reset(); }
  virtual void reset() override;
  virtual int init(
    const ObMergeParameter &merge_param,
    ObITable *table,
    const ObITableReadInfo *read_info) override final;
  virtual OB_INLINE const storage::ObITable *get_table() const override { return nullptr; }
  virtual int next() override;
  virtual bool is_iter_end() const override { return curr_row_count_ > total_row_count_; };
  virtual const blocksstable::ObDatumRow *get_curr_row() const override;
  virtual int get_curr_row_id(int64_t& row_id) const override;
  virtual int64_t get_last_row_id() const override
  {
    return curr_row_count_ > 0 ? curr_row_count_ - 1 : -1;
  }
  INHERIT_TO_STRING_KV("ObMergeIter", ObMergeIter, K_(curr_row_count), K_(total_row_count), K_(default_row));
private:
  int64_t curr_row_count_;
  int64_t total_row_count_;
  const blocksstable::ObDatumRow &default_row_;
};
//iterator base
// - major row iter
//   - major macro iter
//     - major micro iter
//  - minor row iter
//    - minor macro iter

class ObPartitionMergeIter : public ObMergeIter
{
public:
  ObPartitionMergeIter(common::ObIAllocator &allocator);
  virtual ~ObPartitionMergeIter();
  virtual void reset();
  virtual int init(const ObMergeParameter &merge_param,
           const int64_t iter_idx,
           const ObITableReadInfo *read_info) override final;
  virtual int init(const ObMergeParameter &merge_param, ObITable *table, const ObITableReadInfo *read_info) override final;
  virtual OB_INLINE bool is_iter_end() const override { return iter_end_; }
  virtual int multi_version_compare(const ObPartitionMergeIter &other, int &cmp_ret)
  { UNUSEDx(other, cmp_ret); return OB_NOT_SUPPORTED;}
  virtual OB_INLINE const storage::ObITable *get_table() const override { return table_; }
  virtual OB_INLINE bool is_rowkey_first_row_already_output() { return is_rowkey_first_row_already_output_; }
  virtual OB_INLINE bool is_rowkey_shadow_row_already_output() { return is_rowkey_shadow_row_reused_; }

  OB_INLINE bool is_base_iter() const { return is_base_iter_; }
  OB_INLINE bool is_sstable_iter() const { return nullptr != table_ && table_->is_sstable(); }
  OB_INLINE bool is_major_sstable_iter() const { return nullptr != table_ && table_->is_major_sstable(); }
  OB_INLINE bool is_base_sstable_iter() const { return is_base_iter() && is_sstable_iter(); }
  virtual OB_INLINE bool is_multi_version_minor_iter() const { return false; }
  virtual OB_INLINE bool is_macro_merge_iter() const { return false; }

  virtual OB_INLINE const blocksstable::ObDatumRow *get_curr_row() const override final { return curr_row_; }
  virtual int get_curr_row_id(int64_t& row_id) const override;
  virtual int64_t get_last_row_id() const override {  return curr_row_ == nullptr ? iter_row_id_ : iter_row_id_ - 1; }
  virtual int get_curr_range_end_rowid(int64_t &row_id) const override { UNUSED(row_id); return OB_NOT_SUPPORTED; }
  virtual int get_curr_range(blocksstable::ObDatumRange &range) const { UNUSED(range); return OB_NOT_SUPPORTED; }
  virtual int64_t get_iter_row_count() const { return iter_row_count_; }
  virtual int64_t get_ghost_row_count() const { return 0; }
  virtual int collect_tnode_dml_stat(storage::ObTransNodeDMLStat &tnode_stat) const { UNUSED(tnode_stat); return OB_NOT_SUPPORTED; }
  OB_INLINE bool is_compact_completed_row() const
  {
    bool bret = false;
    if (nullptr != curr_row_) {
      bret = curr_row_->is_shadow_row() ||
          (curr_row_->is_last_multi_version_row() && !curr_row_->is_uncommitted_row());
    }
    return bret;
  }
  int check_merge_range_cross(ObDatumRange &data_range, bool &range_cross);

  INHERIT_TO_STRING_KV("ObMergeIter", ObMergeIter, K_(tablet_id), K_(iter_end), K_(schema_rowkey_column_cnt), K_(schema_version), K_(merge_range),
      KPC(table_), K_(store_ctx), KPC_(read_info), KPC(row_iter_), K_(iter_row_count), KPC(curr_row_), K_(is_inited),
      K_(iter_row_id), K_(last_macro_block_reused), K_(is_rowkey_first_row_already_output), K_(is_base_iter), K(access_context_));
protected:
  virtual bool inner_check(const ObMergeParameter &merge_param) = 0;
  virtual int inner_init(const ObMergeParameter &merge_param) = 0;
  void revise_macro_range(ObDatumRange &range) const;
private:
  int init(const ObMergeParameter &merge_param);
  int init_query_base_params(const ObMergeParameter &merge_param);
protected:
  ObTabletID tablet_id_;
  const ObITableReadInfo *read_info_;
  int64_t schema_rowkey_column_cnt_;
  // major merge use schema_version to check whether we should reuse micro block
  int64_t schema_version_;
  blocksstable::ObDatumRange merge_range_;
  storage::ObITable *table_;
  storage::ObStoreCtx store_ctx_;
  storage::ObTableAccessParam access_param_;
  storage::ObTableAccessContext access_context_;
  storage::ObStoreRowIterator *row_iter_;

  int64_t iter_row_count_;
  int64_t iter_row_id_;
  bool is_base_iter_;

  const blocksstable::ObDatumRow *curr_row_;

  bool iter_end_;
  common::ObIAllocator &allocator_;
  bool last_macro_block_reused_;
  bool is_rowkey_first_row_already_output_;
  bool is_rowkey_shadow_row_reused_;
  bool is_reserve_mode_;
};

class ObPartitionRowMergeIter : public ObPartitionMergeIter
{
public:
  ObPartitionRowMergeIter(common::ObIAllocator &allocator, const bool iter_co_build_row_store = false , const bool &ignore_shadow_row = false);
  virtual ~ObPartitionRowMergeIter();
  virtual int next() override;
  INHERIT_TO_STRING_KV("ObPartitionRowMergeIter", ObPartitionMergeIter, KPC(row_iter_), K_(ignore_shadow_row));
protected:
  virtual int inner_init(const ObMergeParameter &merge_param) override;
  virtual bool inner_check(const ObMergeParameter &merge_param) override;
  int construct_out_cols_project(const ObMergeParameter &merge_param);
  int inner_init_row_iter(const ObMergeParameter &merge_param);
private:
  const bool iter_co_build_row_store_;
  const bool ignore_shadow_row_;
  ObFixedArray<int32_t, ObIAllocator> out_cols_project_;
};

class ObPartitionMacroMergeIter : public ObPartitionMergeIter
{
public:
  ObPartitionMacroMergeIter(common::ObIAllocator &allocator);
  virtual ~ObPartitionMacroMergeIter();
  virtual void reset() override;
  virtual int next() override;
  virtual OB_INLINE bool is_macro_merge_iter() const { return true; }
  virtual bool is_macro_block_opened() const override { return macro_block_opened_; }
  virtual int open_curr_range(const bool for_rewrite, const bool for_compare = false) override;
  virtual int get_curr_range_end_rowid(int64_t &row_id) const override;
  virtual int get_curr_range(blocksstable::ObDatumRange &range) const override;
  virtual int get_curr_macro_block(
      const blocksstable::ObMacroBlockDesc *&macro_desc) const override
  {
    macro_desc = &curr_block_desc_;
    return OB_SUCCESS;
  }
  virtual int need_open_curr_range(const blocksstable::ObDatumRow &row, bool &need_open, const int64_t row_id_for_cg = 0) override final;
  INHERIT_TO_STRING_KV("ObPartitionMacroMergeIter", ObPartitionMergeIter, K_(macro_block_opened),
                       KP_(macro_block_iter), K_(curr_block_desc), K(curr_block_meta_));
protected:
  virtual int inner_init(const ObMergeParameter &merge_param) override;
  virtual bool inner_check(const ObMergeParameter &merge_param) override;
  void reset_macro_block_desc() { curr_block_desc_.reset(); curr_block_meta_.reset(); curr_block_desc_.macro_meta_ = &curr_block_meta_; }
  virtual int next_range();
  virtual int check_row_changed(const blocksstable::ObDatumRow &row, const int64_t row_id, bool &is_changed);
  int exist(const blocksstable::ObDatumRow &row, bool &is_exist);
protected:
  blocksstable::ObIMacroBlockIterator *macro_block_iter_;
  blocksstable::ObMacroBlockDesc curr_block_desc_;
  blocksstable::ObDataMacroBlockMeta curr_block_meta_;
  ObCSDatumRange cs_datum_range_;
  bool macro_block_opened_;
  bool macro_block_opened_for_cmp_;
};

class ObPartitionMicroMergeIter : public ObPartitionMacroMergeIter
{
public:
  ObPartitionMicroMergeIter(common::ObIAllocator &allocator);
  virtual ~ObPartitionMicroMergeIter();
  virtual void reset() override;
  virtual int next() override;
  virtual int open_curr_range(const bool for_rewrite, const bool for_compare = false) override;
  virtual bool is_micro_block_opened() const override { return micro_block_opened_; }
  virtual int get_curr_range_end_rowid(int64_t &row_id) const override;
  virtual int get_curr_range(blocksstable::ObDatumRange &range) const override;
  virtual int get_curr_micro_block(const blocksstable::ObMicroBlock *&micro_block)
  {
    micro_block = curr_micro_block_;
    return OB_SUCCESS;
  }
  virtual int check_row_changed(const blocksstable::ObDatumRow &row, const int64_t row_id, bool &is_changed) override;
  INHERIT_TO_STRING_KV("ObPartitionMicroMergeIter", ObPartitionMacroMergeIter, K_(micro_block_opened),
                       K_(need_reuse_micro_block), KPC(curr_micro_block_), KP_(micro_row_scanner));
private:
  virtual int inner_init(const ObMergeParameter &merge_param) override;
  virtual bool inner_check(const ObMergeParameter &merge_param) override;
  virtual int next_range() override;
  int open_curr_micro_block(const int64_t start_row_id = -1);
  void check_need_reuse_micro_block();
private:
  ObIndexBlockMicroIterator micro_block_iter_;
  blocksstable::ObIMicroBlockRowScanner *micro_row_scanner_;
  const blocksstable::ObMicroBlock *curr_micro_block_;
  bool micro_block_opened_;
  blocksstable::ObMacroBlockReader macro_reader_;
  bool need_reuse_micro_block_;
};

class ObPartitionMinorRowMergeIter : public ObPartitionMergeIter
{
public:
  ObPartitionMinorRowMergeIter(common::ObIAllocator &allocator);
  virtual ~ObPartitionMinorRowMergeIter();
  virtual void reset() override;
  virtual int next() override;
  virtual int multi_version_compare(const ObPartitionMergeIter &other, int &cmp_ret) override;
  virtual int64_t get_ghost_row_count() const override { return ghost_row_count_; }
  virtual OB_INLINE bool is_multi_version_minor_iter() const { return true; }
  virtual bool is_curr_row_commiting() const;
  virtual int collect_tnode_dml_stat(storage::ObTransNodeDMLStat &tnode_stat) const override;
  INHERIT_TO_STRING_KV("ObPartitionMinorRowMergeIter", ObPartitionMergeIter, K_(ghost_row_count),
                       K_(check_committing_trans_compacted), K_(row_queue));
protected:
  enum CompactRowIndex {
    CRI_FIRST_ROW = 0,
    CRI_LAST_ROW = 1,
    CRI_MAX,
  };
  virtual int inner_init(const ObMergeParameter &merge_param) override;
  virtual bool inner_check(const ObMergeParameter &merge_param) override;
  virtual int common_minor_inner_init(const ObMergeParameter &merge_param);

  virtual int inner_next(const bool open_macro);
  virtual int try_make_committing_trans_compacted();
  virtual int compact_border_row(const bool last_row);
  virtual int check_meet_another_trans();
  virtual int check_compact_finish(bool &finish);
  virtual int compare_multi_version_col(const ObPartitionMergeIter &other,
                                        const int64_t multi_version_col,
                                        int &cmp_ret);
  bool need_recycle_mv_row()
  {
    return curr_row_ != nullptr && !curr_row_->is_uncommitted_row() && !curr_row_->is_last_multi_version_row() &&
                 -curr_row_->storage_datums_[schema_rowkey_column_cnt_].get_int() <=
                      access_context_.trans_version_range_.multi_version_start_;
  }
  int skip_ghost_row();
  int compact_old_row();
protected:
  common::ObArenaAllocator obj_copy_allocator_;
  storage::ObNopPos *nop_pos_[CRI_MAX];
  blocksstable::ObRowQueue row_queue_;
  bool check_committing_trans_compacted_;
  int64_t ghost_row_count_;
};


class ObPartitionMinorMacroMergeIter : public ObPartitionMinorRowMergeIter
{
public:
  ObPartitionMinorMacroMergeIter(common::ObIAllocator &allocator, bool reuse_uncommit_row = false);
  virtual ~ObPartitionMinorMacroMergeIter();
  virtual void reset() override;
  virtual int next() override;
  virtual int open_curr_range(const bool for_rewrite, const bool for_compare = false) override;
  virtual int get_curr_range(blocksstable::ObDatumRange &range) const override;
  virtual OB_INLINE bool is_macro_merge_iter() const { return true; }
  virtual bool is_macro_block_opened() const override { return macro_block_opened_; }
  virtual int get_curr_macro_block(
      const blocksstable::ObMacroBlockDesc *&macro_desc) const override
  {
    macro_desc = &curr_block_desc_;
    return OB_SUCCESS;
  }

  INHERIT_TO_STRING_KV("ObPartitionMinorMacroMergeIter", ObPartitionMinorRowMergeIter,
      K_(macro_block_opened), K_(curr_block_desc), K_(curr_block_meta), K_(macro_block_iter),
      K_(last_macro_block_reused), K_(last_macro_block_recycled), K_(last_mvcc_row_already_output), K_(have_macro_output_row), K_(reuse_uncommit_row));
protected:
  virtual int inner_init(const ObMergeParameter &merge_param) override;
  virtual bool inner_check(const ObMergeParameter &merge_param) override;
  virtual int inner_next(const bool open_macro) override;
  virtual int next_range();
  virtual int open_curr_macro_block();
  void reset_macro_block_desc() { curr_block_desc_.reset(); curr_block_meta_.reset(); curr_block_desc_.macro_meta_ = &curr_block_meta_; }
  int check_need_open_curr_macro_block(bool &need);
  int check_macro_block_recycle(const ObMacroBlockDesc &macro_desc, bool &can_recycle);
  int recycle_last_rowkey_in_macro_block(ObSSTableRowWholeScanner &iter);
private:
  OB_INLINE bool last_macro_block_reused() const { return 1 == last_macro_block_reused_; }
  blocksstable::ObIMacroBlockIterator *macro_block_iter_;
  blocksstable::ObMacroBlockDesc curr_block_desc_;
  blocksstable::ObDataMacroBlockMeta curr_block_meta_;
  bool macro_block_opened_;
  int8_t last_macro_block_reused_;
  bool last_macro_block_recycled_;
  bool last_mvcc_row_already_output_;
  bool have_macro_output_row_;
  const bool reuse_uncommit_row_;
};

static const int64_t DEFAULT_ITER_COUNT = 16;
static const int64_t DEFAULT_ITER_ARRAY_SIZE = DEFAULT_ITER_COUNT * sizeof(ObPartitionMergeIter *);
typedef common::ObSEArray<ObPartitionMergeIter*, DEFAULT_ITER_COUNT> MERGE_ITER_ARRAY;

} //compaction
} //oceanbase


#endif /* OB_COMPACTION_PARTITION_MERGE_ITER_H_ */
