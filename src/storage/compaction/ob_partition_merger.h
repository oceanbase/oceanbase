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
#include "lib/container/ob_loser_tree.h"
#include "storage/compaction/ob_partition_rows_merger.h"
#include "storage/compaction/ob_compaction_trans_cache.h"

namespace oceanbase
{
using namespace blocksstable;
namespace storage
{
struct ObSSTableMergeInfo;
}

namespace compaction
{
struct ObTabletMergeCtx;
struct ObMergeParameter;
class ObPartitionMinorMergeHelper;

class ObPartitionMerger
{
public:
  ObPartitionMerger();
  virtual ~ObPartitionMerger();
  virtual void reset();
  virtual int merge_partition(
      ObTabletMergeCtx &ctx,
      const int64_t idx,
      const bool force_flat_format = false) = 0;
  VIRTUAL_TO_STRING_KV(K_(is_inited), K_(task_idx), K_(data_store_desc), K_(minimum_iters), K_(merge_info));
protected:
  virtual int open(ObTabletMergeCtx &ctx, const int64_t idx, const bool force_flat_format) = 0;
  virtual int inner_process(const blocksstable::ObDatumRow &row) = 0;
  virtual int close();
  virtual int process(const blocksstable::ObMicroBlock &micro_block);
  virtual int process(const blocksstable::ObMacroBlockDesc &macro_meta);
  virtual int process(const blocksstable::ObDatumRow &row);
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) = 0;
  virtual int merge_macro_block_iter(MERGE_ITER_ARRAY &minimum_iters, int64_t &reuse_row_cnt);
  virtual int try_rewrite_macro_block(const ObMacroBlockDesc &macro_block, bool &rewrite);
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters) = 0;
  template <typename T> T *alloc_merge_helper();
  virtual int init_partition_fuser(const ObMergeParameter &merge_param) = 0;
  int init_data_store_desc(ObTabletMergeCtx &ctx);
  int open_macro_writer(ObMergeParameter &merge_param);
  int prepare_merge_partition(ObMergeParameter &merge_param,
                              ObPartitionMergeHelper &merge_helper);
  int get_macro_block_count_to_rewrite(const ObMergeParameter &merge_param,
                                       int64_t &need_rewrite_block_cnt);
  int check_row_columns(const blocksstable::ObDatumRow &row);
  int try_filter_row(const blocksstable::ObDatumRow &row, ObICompactionFilter::ObFilterRet &filter_ret);
  int get_base_iter_curr_macro_block(const blocksstable::ObMacroBlockDesc *&macro_desc);
  void set_base_iter(const MERGE_ITER_ARRAY &minimum_iters);
protected:
  static const int64_t DEFAULT_ITER_ARRAY_SIZE = DEFAULT_ITER_COUNT * sizeof(ObPartitionMergeIter *);
  static const int64_t CACHED_TRANS_STATE_MAX_CNT = 10 * 1024l;
protected:
  common::ObArenaAllocator allocator_;
  ObTabletMergeCtx *merge_ctx_;
  ObPartitionMergeProgress *merge_progress_;
  ObIPartitionMergeFuser *partition_fuser_;
  blocksstable::ObDataStoreDesc data_store_desc_;
  ObSSTableMergeInfo merge_info_;
  blocksstable::ObMacroBlockWriter *macro_writer_;
  MERGE_ITER_ARRAY minimum_iters_;
  ObPartitionMergeIter *base_iter_;
  ObCachedTransStateMgr trans_state_mgr_;
  int64_t task_idx_;
  bool check_macro_need_merge_;
  bool is_inited_;
};

class ObPartitionMajorMerger : public ObPartitionMerger
{
public:
  ObPartitionMajorMerger();
  ~ObPartitionMajorMerger();
  virtual void reset() override;
  virtual int merge_partition(
      ObTabletMergeCtx &ctx,
      const int64_t idx,
      const bool force_flat_format = false) override;
  INHERIT_TO_STRING_KV("ObPartitionMajorMerger", ObPartitionMerger, KPC(merge_progress_));
protected:
  virtual int open(ObTabletMergeCtx &ctx, const int64_t idx, const bool force_flat_format) override;
  virtual int inner_process(const blocksstable::ObDatumRow &row) override;
  virtual int init_partition_fuser(const ObMergeParameter &merge_param) override;
  virtual int try_rewrite_macro_block(const ObMacroBlockDesc &macro_desc, bool &rewrite) override;
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) override;
  virtual int merge_same_rowkey_iters(MERGE_ITER_ARRAY &merge_iters) override;
private:
  int merge_micro_block_iter(ObPartitionMergeIter &iter, int64_t &reuse_row_cnt);
  int reuse_base_sstable(ObPartitionMajorMergeHelper &merge_helper);
private:
  int64_t rewrite_block_cnt_;
  int64_t need_rewrite_block_cnt_;
};

class ObPartitionMinorMerger : public ObPartitionMerger
{
public:
  ObPartitionMinorMerger();
  ~ObPartitionMinorMerger();
  virtual void reset() override;
  virtual int merge_partition(
      ObTabletMergeCtx &ctx,
      const int64_t idx,
      const bool force_flat_format = false) override;
  INHERIT_TO_STRING_KV("ObPartitionMinorMerger", ObPartitionMerger, K_(minimum_iter_idxs),
                       K_(need_build_bloom_filter), KP_(cols_id_map));
protected:
  virtual int open(ObTabletMergeCtx &ctx, const int64_t idx, const bool force_flat_format) override;
  virtual int close() override;
  virtual int inner_process(const blocksstable::ObDatumRow &row) override;
  virtual int init_partition_fuser(const ObMergeParameter &merge_param) override;
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
  virtual int check_need_prebuild_bloomfilter();
  virtual int init_bloomfilter_writer();
  virtual int append_bloom_filter(const blocksstable::ObDatumRow &row);
  virtual int rewrite_macro_block(MERGE_ITER_ARRAY &minimum_iters) override;
private:
  int collect_merge_stat(
      const storage::ObMergeType &merge_type,
      compaction::ObPartitionMinorMergeHelper &merge_helper,
      ObTabletMergeCtx &ctx);
  int check_add_shadow_row(MERGE_ITER_ARRAY &merge_iters, const bool contain_multi_trans, bool &add_shadow_row);
  int merge_single_iter(ObPartitionMergeIter &merge_ite);
  int check_first_committed_row(const MERGE_ITER_ARRAY &merge_iters);
  int set_result_flag(MERGE_ITER_ARRAY &fuse_iters,
                      const bool rowkey_first_row,
                      const bool add_shadow_row,
                      const bool need_check_last);

protected:
  common::ObSEArray<int64_t, DEFAULT_ITER_COUNT> minimum_iter_idxs_;
  share::schema::ColumnMap *cols_id_map_;
  blocksstable::ObBloomFilterDataWriter bf_macro_writer_;
  bool need_build_bloom_filter_;
};

class ObPartitionMergeDumper
{
public:
  static void print_error_info(const int err_no,
                               const MERGE_ITER_ARRAY &merge_iters,
                               ObTabletMergeCtx &ctx);
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
