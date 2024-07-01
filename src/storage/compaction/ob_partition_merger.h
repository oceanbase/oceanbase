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

#ifndef OB_COMPACTION_PARTITION_MERGER_H_
#define OB_COMPACTION_PARTITION_MERGER_H_

#include "lib/container/ob_iarray.h"
#include "lib/container/ob_raw_se_array.h"
#include "storage/ob_i_store.h"
#include "storage/blocksstable/ob_bloom_filter_data_writer.h"
#include "storage/blocksstable/ob_bloom_filter_data_reader.h"
#include "ob_partition_merge_iter.h"
#include "ob_partition_merge_fuser.h"
#include "ob_partition_merge_progress.h"
#include "storage/blocksstable/ob_macro_block_writer.h"
#include "storage/ob_sstable_struct.h"
#include "storage/blocksstable/ob_sstable.h"
#include "ob_tablet_merge_ctx.h"
#include "lib/container/ob_loser_tree.h"
#include "storage/compaction/ob_partition_rows_merger.h"
#include "storage/compaction/ob_compaction_trans_cache.h"
#include "ob_tablet_merge_task.h"
#include "storage/compaction/ob_compaction_memory_context.h"
#include "storage/blocksstable/ob_data_store_desc.h"
#include "storage/compaction/ob_tablet_merge_info.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
struct ObSSTableMergeInfo;
}

namespace compaction
{
struct ObMergeParameter;
class ObPartitionMergeHelper;
class ObPartitionMinorMergeHelper;
class ObTabletMergeInfo;

struct ObMacroBlockOp {
  enum BlockOp: uint8_t {
    OP_NONE = 0,
    OP_REORG = 1,
    OP_REWRITE = 2
  };

  ObMacroBlockOp() = default;
  ~ObMacroBlockOp() = default;
  OB_INLINE void reset() { block_op_ = OP_NONE; }
  OB_INLINE bool is_none() const { return block_op_ == OP_NONE; }
  OB_INLINE bool is_rewrite() const { return block_op_ == OP_REWRITE; }
  OB_INLINE bool is_reorg() const { return block_op_ == OP_REORG; }
  OB_INLINE bool is_open() const { return is_reorg() || is_rewrite(); }
  OB_INLINE void set_rewrite() { block_op_ = OP_REWRITE; }
  OB_INLINE void set_reorg() { block_op_ = OP_REORG; }
  OB_INLINE bool is_valid() const { return block_op_ <= OP_REWRITE && block_op_ >= OP_NONE; }
  const char* get_block_op_str() const;

  TO_STRING_KV("op_type", get_block_op_str());

  BlockOp block_op_;
private:
  const static char * block_op_str_[];
};

class ObDataDescHelper final {
public:
  static int build(
      const ObMergeParameter &merge_param,
      ObTabletMergeInfo &input_merge_info,
      blocksstable::ObDataStoreDesc &data_store_desc,
      ObSSTableMergeInfo &output_merge_info);
};

class ObProgressiveMergeHelper final
{
public:
  ObProgressiveMergeHelper(const int64_t table_idx = 0)
    : table_idx_(table_idx),
      progressive_merge_round_(0),
      rewrite_block_cnt_(0),
      need_rewrite_block_cnt_(0),
      data_version_(0),
      full_merge_(false),
      check_macro_need_merge_(false),
      is_inited_(false)
    {}
  ~ObProgressiveMergeHelper() = default;
  int init(const ObSSTable &sstable, const ObMergeParameter &merge_param, ObIAllocator &allocator);
  void reset();
  inline bool is_valid() const { return is_inited_; }
  int check_macro_block_op(const ObMacroBlockDesc &macro_desc, ObMacroBlockOp &block_op) const;
  inline void inc_rewrite_block_cnt() { rewrite_block_cnt_++; }
  inline bool is_progressive_merge_finish() { return need_rewrite_block_cnt_ == 0 || rewrite_block_cnt_ >= need_rewrite_block_cnt_; }
  TO_STRING_KV(K_(table_idx), K_(progressive_merge_round), K_(rewrite_block_cnt), K_(need_rewrite_block_cnt), K_(data_version), K_(full_merge), K_(check_macro_need_merge), K_(is_inited));
private:
  const static int64_t CG_TABLE_CHECK_REWRITE_CNT_ = 4;
  const static int64_t DEFAULT_MACRO_BLOCK_REWRTIE_THRESHOLD = 30;
  const int64_t table_idx_;
  int64_t progressive_merge_round_;
  int64_t rewrite_block_cnt_;
  int64_t need_rewrite_block_cnt_;
  int64_t data_version_;
  bool full_merge_;
  bool check_macro_need_merge_;
  bool is_inited_;
};

class ObMerger
{
public:
  ObMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  virtual ~ObMerger() { reset(); }
  void virtual reset();
  virtual int merge_partition(
      ObBasicTabletMergeCtx &ctx,
      const int64_t idx) = 0;
  void force_flat_format() { force_flat_format_ = true; }
  VIRTUAL_TO_STRING_KV(K_(task_idx), K_(merge_param));
protected:
  int prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx);
  int get_base_iter_curr_macro_block(const blocksstable::ObMacroBlockDesc *&macro_desc);
  void set_base_iter(const MERGE_ITER_ARRAY &minimum_iters) {
    const int64_t count = minimum_iters.count();
    if (!minimum_iters.empty() && minimum_iters[count - 1]->is_base_sstable_iter()
      && minimum_iters[count - 1]->is_macro_merge_iter()) {
      base_iter_ = minimum_iters[count - 1];
    } else {
      base_iter_ = nullptr;
    }
  }
  static const int64_t CACHED_TRANS_STATE_MAX_CNT = 10 * 1024l;
private:
  virtual int inner_prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx) = 0;
  virtual int close() = 0;
protected:
  compaction::ObLocalArena &merger_arena_;
  ObBasicTabletMergeCtx *merge_ctx_;
  int64_t task_idx_;
  bool force_flat_format_;
  ObMergeParameter merge_param_;
  ObIPartitionMergeFuser *partition_fuser_;
  ObPartitionMergeHelper *merge_helper_;
  ObPartitionMergeIter *base_iter_;
  ObCachedTransStateMgr trans_state_mgr_;
};

class ObPartitionMerger : public ObMerger
{
public:
  ObPartitionMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  virtual ~ObPartitionMerger();
  virtual void reset();
  INHERIT_TO_STRING_KV("ObPartitionMerger", ObMerger, KPC_(merge_progress), K_(data_store_desc), K_(minimum_iters), K_(merge_info));
protected:
  virtual int inner_process(const blocksstable::ObDatumRow &row) = 0;
  virtual int open_macro_writer(ObMergeParameter &merge_param);
  virtual int close() override;
  virtual int process(const blocksstable::ObMicroBlock &micro_block);
  virtual int process(const blocksstable::ObMacroBlockDesc &macro_meta);
  virtual int process(const blocksstable::ObDatumRow &row);
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) = 0;
  virtual int merge_macro_block_iter(MERGE_ITER_ARRAY &minimum_iters, int64_t &reuse_row_cnt);
  virtual int check_macro_block_op(const ObMacroBlockDesc &macro_desc, ObMacroBlockOp &block_op);
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters) = 0;
  int check_row_columns(const blocksstable::ObDatumRow &row);
  int try_filter_row(const blocksstable::ObDatumRow &row, ObICompactionFilter::ObFilterRet &filter_ret);

private:
  virtual int inner_prepare_merge(ObBasicTabletMergeCtx &ctx, const int64_t idx) override final;
  virtual int inner_init() = 0;
protected:
  static const int64_t DEFAULT_ITER_ARRAY_SIZE = DEFAULT_ITER_COUNT * sizeof(ObPartitionMergeIter *);
protected:
  ObPartitionMergeProgress *merge_progress_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  ObSSTableMergeInfo merge_info_; // record merge info of cur Merger(one of the parallel task)
  blocksstable::ObMacroBlockWriter *macro_writer_;
  MERGE_ITER_ARRAY minimum_iters_;
  ObProgressiveMergeHelper progressive_merge_helper_;
};

class ObPartitionMajorMerger : public ObPartitionMerger
{
public:
  ObPartitionMajorMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  ~ObPartitionMajorMerger();
  virtual int merge_partition(
      ObBasicTabletMergeCtx &ctx,
      const int64_t idx) override;
  INHERIT_TO_STRING_KV("ObPartitionMajorMerger", ObPartitionMerger, "curr merger", "major merger");
protected:
  virtual int inner_process(const blocksstable::ObDatumRow &row) override;

private:
  virtual int inner_init() override;
  int init_progressive_merge_helper();
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) override;
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters) override;
  int merge_micro_block_iter(ObPartitionMergeIter &iter, int64_t &reuse_row_cnt);
  int reuse_base_sstable(ObPartitionMergeHelper &merge_helper);
};

class ObPartitionMinorMerger : public ObPartitionMerger
{
public:
  ObPartitionMinorMerger(
    compaction::ObLocalArena &allocator,
    const ObStaticMergeParam &static_param);
  ~ObPartitionMinorMerger();
  virtual void reset() override;
  virtual int merge_partition(
      ObBasicTabletMergeCtx &ctx,
      const int64_t idx) override;
  INHERIT_TO_STRING_KV("ObPartitionMinorMerger", ObPartitionMerger, K_(minimum_iter_idxs));
protected:
  virtual int open_macro_writer(ObMergeParameter &merge_param) override;
  virtual int inner_process(const blocksstable::ObDatumRow &row) override;
  int find_minimum_iters_with_same_rowkey(MERGE_ITER_ARRAY &merge_iters,
                                          MERGE_ITER_ARRAY &minimum_iters,
                                          common::ObIArray<int64_t> &iter_idxs);
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters) override;
  int try_remove_ghost_iters(MERGE_ITER_ARRAY &merge_iters,
                             const bool shadow_already_output,
                             MERGE_ITER_ARRAY &minimum_iters,
                             common::ObIArray<int64_t> &iter_idxs);
  virtual int move_and_remove_unused_iters(MERGE_ITER_ARRAY &merge_iters,
                                           MERGE_ITER_ARRAY &minimum_iters,
                                           common::ObIArray<int64_t> &iter_idxs);
  int skip_shadow_row(MERGE_ITER_ARRAY &merge_iters);
  int check_need_prebuild_bloomfilter();
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) override;
private:
  virtual int inner_init() override;
  int init_progressive_merge_helper();
  int collect_merge_stat(
      const compaction::ObMergeType &merge_type,
      compaction::ObPartitionMinorMergeHelper &merge_helper,
      ObBasicTabletMergeCtx &ctx);
  int check_add_shadow_row(MERGE_ITER_ARRAY &merge_iters, const bool contain_multi_trans, bool &add_shadow_row);
  int merge_single_iter(ObPartitionMergeIter &merge_ite);
  int check_first_committed_row(const MERGE_ITER_ARRAY &merge_iters);
  int set_result_flag(MERGE_ITER_ARRAY &fuse_iters,
                      const bool rowkey_first_row,
                      const bool add_shadow_row,
                      const bool need_check_last);

protected:
  common::ObSEArray<int64_t, DEFAULT_ITER_COUNT> minimum_iter_idxs_;
};

class ObPartitionMergeDumper
{
public:
  static void print_error_info(const int err_no,
                               const MERGE_ITER_ARRAY &merge_iters,
                               ObBasicTabletMergeCtx &ctx);
  static int generate_dump_table_name(
      const char *dir_name,
      const storage::ObITable *table,
      char *file_name);
  static int judge_disk_free_space(const char *dir_name, storage::ObITable *table);
  static int check_disk_free_space(const char *dir_name);

  static constexpr const double DUMP_TABLE_DISK_FREE_PERCENTAGE = 0.2;
  static constexpr const double MEMTABLE_DUMP_SIZE_PERCENTAGE = 0.2;
  static const int64_t ROW_COUNT_CHECK_INTERVAL = 10000;
  static int64_t free_space;
  static lib::ObMutex lock;
private:
  static bool need_dump_table(int err_no);
};


} //compaction
} //oceanbase


#endif /* OB_COMPACTION_PARTITION_MERGER_H_ */
