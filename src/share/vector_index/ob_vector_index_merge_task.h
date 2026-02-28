/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_VECTOR_INDEX_MERGE_TASK_H_
#define OCEANBASE_SHARE_VECTOR_INDEX_MERGE_TASK_H_

#include "share/vector_index/ob_vector_index_async_task.h"
#include "share/vector_index/ob_vector_index_async_task_util.h"
#include "share/vector_index/ob_vector_index_i_task_executor.h"

namespace oceanbase
{
namespace share
{

class ObVecIdxMergeTaskExecutor : public ObVecAsyncTaskExector
{
public:
  ObVecIdxMergeTaskExecutor()
    : ObVecAsyncTaskExector(),
    merge_base_percentage_(OB_VECTOR_INDEX_MERGE_BASE_PERCENTAGE)
  {}
  virtual ~ObVecIdxMergeTaskExecutor() {}
  int load_task(uint64_t &task_trace_base_num) override;
private:
  bool check_operation_allow() override;

  int64_t merge_base_percentage_;
};

class ObVecIdxMergeTask : public ObVecIndexAsyncTask
{
public:
  ObVecIdxMergeTask() :
    ObVecIndexAsyncTask(ObMemAttr(MTL_ID(), "VecMergTask")),
    no_need_replace_adaptor_(false),
    merge_from_base_(false),
    pre_alloc_size_(0)
  {}
  virtual ~ObVecIdxMergeTask() {}
  int do_work() override;

private:
  int process_merge();
  int check_and_merge(ObPluginVectorIndexAdapterGuard &adpt_guard);
  int execute_merge();
  int execute_exchange();
  int prepare_merge_segment(const ObVecIdxSnapshotDataHandle& old_snap_data);
  int upadte_task_merge_segments_info();
  int upadte_task_result_segments_info(const ObVectorIndexSegmentMeta *new_meta);
  int calculate_max_merge_vec_cnt(int64_t &max_merge_vec_cnt, int64_t incr_count);
  int build_filter_clause(const ObTableSchema &data_table_schema, const ObTableSchema &snapshot_table_schema);
  int merge_bitmap(ObPluginVectorIndexAdaptor *adaptor);
  int execute_insert(const ObTableSchema *data_schema);
  int clean_snap_index_rows(
      const ObTableSchema &data_table_schema, const ObTableSchema &snapshot_table_schema,
      transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot, const uint64_t timeout_us);
  int rescan(
      const ObVectorIndexSegmentMeta& seg_meta, storage::ObTableScanParam &scan_param,
      const uint64_t timeout, ObTableScanIterator *table_scan_iter);
  int build_rowkey_range(const ObVectorIndexSegmentMeta& seg_meta, ObNewRange &range);
  int delete_segment_rows(
      const ObVectorIndexSegmentMeta& seg_meta, storage::ObTableScanParam &scan_param,
      const uint64_t timeout, ObTableScanIterator *table_scan_iter,
      ObIArray<uint64_t> &dml_column_ids, ObIArray<uint64_t> &extra_column_idxs,
      transaction::ObTxDesc *tx_desc, transaction::ObTxReadSnapshot &snapshot,
      share::schema::ObTableDMLParam &table_dml_param, const ObTabletID &tablet_id,
      const uint64_t schema_version);
  int exchange_snap_index_rows(
      const ObVectorIndexSegmentMeta& seg_meta,
      const ObTableSchema &data_table_schema,
      const ObTableSchema &snapshot_table_schema,
      transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot,
      const uint64_t timeout_us);
  int update_meta(
      const ObVecIdxSnapshotDataHandle& new_snap, transaction::ObTxDesc *tx_desc,
      transaction::ObTxReadSnapshot &snapshot, const uint64_t timeout_us);
  int prepare_new_meta(
      ObVectorIndexMeta &new_meta,
      ObVectorIndexSegmentMeta &new_seg_meta,
      const ObVectorIndexMeta &old_meta);
  int refresh_adaptor();
  int check_and_wait_write();

private:
  int64_t min_vid_ {INT64_MAX};
  int64_t max_vid_ {0};
  // rowkey for scan range
  ObArray<ObObj> rowkey_objs_;
  ObSEArray<const ObVectorIndexSegmentMeta *, 10> merge_segments_;
  bool no_need_replace_adaptor_;
  bool merge_from_base_;
  int64_t pre_alloc_size_;

  DISALLOW_COPY_AND_ASSIGN(ObVecIdxMergeTask);
};


} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_VECTOR_INDEX_MERGE_TASK_H_
