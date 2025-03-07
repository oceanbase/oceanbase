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
#pragma once

#include "share/ob_tablet_autoincrement_param.h"
#include "share/schema/ob_table_param.h"
#include "share/table/ob_table_load_define.h"
#include "storage/direct_load/ob_direct_load_i_table.h"
#include "storage/direct_load/ob_direct_load_origin_table.h"
#include "storage/direct_load/ob_direct_load_table_data_desc.h"
#include "storage/direct_load/ob_direct_load_trans_param.h"
#include "storage/direct_load/ob_direct_load_i_merge_task.h"

namespace oceanbase
{
namespace common
{
class ObOptOSGColumnStat;
class ObOptTableStat;
} // namespace common
namespace observer
{
class ObTableLoadTableCtx;
} // namespace observer
namespace storage
{
class ObDirectLoadInsertTableContext;
class ObDirectLoadInsertTabletContext;
class ObDirectLoadTabletMergeCtx;
class ObDirectLoadMultipleMergeRangeSplitter;
class ObDirectLoadDMLRowHandler;
class ObIDirectLoadPartitionTableBuilder;
class ObDirectLoadTmpFileManager;
class ObDirectLoadTableStore;

// NORMAL : 将导入数据直接构造成sstable
// MERGE_WITH_ORIGIN_DATA : 将导入数据与已有数据合并, 场景: full+normal写数据表
// MERGE_WITH_CONFLICT_CHECK : 将导入数据与已有数据进行冲突检测, 场景:
// incremental或inc_replace但是表上有lob或索引 MERGE_WITH_ORIGIN_QUERY_FOR_LOB : 目前是给del_lob用的,
// 根据lob_id去原表查询
struct ObDirectLoadMergeMode
{
#define OB_DIRECT_LOAD_MERGE_MODE_DEF(DEF)    \
  DEF(INVALID_MERGE_MODE, = 0)                \
  DEF(NORMAL, )                               \
  DEF(MERGE_WITH_ORIGIN_DATA, )               \
  DEF(MERGE_WITH_CONFLICT_CHECK, )            \
  DEF(MERGE_WITH_ORIGIN_QUERY_FOR_LOB, )              \
  DEF(MERGE_WITH_ORIGIN_QUERY_FOR_DATA, )     \
  DEF(MAX_MERGE_MODE, )

  DECLARE_ENUM(Type, type, OB_DIRECT_LOAD_MERGE_MODE_DEF, static);

  static bool is_type_valid(const Type type)
  {
    return type > INVALID_MERGE_MODE && type < MAX_MERGE_MODE;
  }
  static bool is_normal(const Type type) { return type == NORMAL; }
  static bool is_merge_with_origin_data(const Type type) { return type == MERGE_WITH_ORIGIN_DATA; }
  static bool is_merge_with_conflict_check(const Type type)
  {
    return type == MERGE_WITH_CONFLICT_CHECK;
  }
  static bool is_merge_with_origin_query_for_lob(const Type type)
  {
    return type == MERGE_WITH_ORIGIN_QUERY_FOR_LOB;
  }
  static bool is_merge_with_origin_query_for_data(const Type type)
  {
    return MERGE_WITH_ORIGIN_QUERY_FOR_DATA == type;
  }
  static bool merge_need_origin_table(const Type type)
  {
    return is_merge_with_origin_data(type) || is_merge_with_conflict_check(type) ||
           is_merge_with_origin_query_for_lob(type) || is_merge_with_origin_query_for_data(type);
  }
};

struct ObDirectLoadMergeParam
{
public:
  ObDirectLoadMergeParam();
  ~ObDirectLoadMergeParam();
  bool is_valid() const;
  TO_STRING_KV(K_(table_id), K_(rowkey_column_num), K_(column_count), KP_(col_descs),
               KP_(datum_utils), KP_(lob_column_idxs), "merge_mode",
               ObDirectLoadMergeMode::get_type_string(merge_mode_), K_(use_batch_mode),
               KP_(dml_row_handler), KP_(insert_table_ctx), K_(trans_param), KP_(file_mgr),
               KP_(ctx));

public:
  // 本次合并要写入的表的属性, 可能是数据表、索引表、lob表
  uint64_t table_id_; // origin table id
  int64_t rowkey_column_num_;
  int64_t column_count_;
  const common::ObIArray<share::schema::ObColDesc> *col_descs_;
  const blocksstable::ObStorageDatumUtils *datum_utils_;
  const common::ObArray<int64_t> *lob_column_idxs_;
  // 合并模式
  ObDirectLoadMergeMode::Type merge_mode_;
  bool use_batch_mode_;
  ObDirectLoadDMLRowHandler *dml_row_handler_; // rescan时为nullptr
  ObDirectLoadInsertTableContext *insert_table_ctx_;
  // 任务级参数
  ObDirectLoadTransParam trans_param_;
  ObDirectLoadTmpFileManager *file_mgr_;
  // TODO 更新进度信息
  observer::ObTableLoadTableCtx *ctx_;
};

class ObDirectLoadMergeCtx
{
  friend class ObDirectLoadTabletMergeCtx;
  friend class ObDirectLoadMergeTaskIterator;

public:
  ObDirectLoadMergeCtx();
  ~ObDirectLoadMergeCtx();
  void reset();
  int init(const ObDirectLoadMergeParam &param,
           const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids);
  int build_merge_task(ObDirectLoadTableStore &table_store, int64_t thread_cnt);
  int build_del_lob_task(ObDirectLoadTableStore &table_store, int64_t thread_cnt);
  int build_rescan_task(int64_t thread_cnt);
  const common::ObIArray<ObDirectLoadTabletMergeCtx *> &get_tablet_merge_ctxs() const
  {
    return tablet_merge_ctx_array_;
  }

private:
  int create_all_tablet_ctxs(
    const common::ObIArray<table::ObTableLoadLSIdAndPartitionId> &ls_partition_ids);

private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMergeParam param_;
  common::ObArray<ObDirectLoadTabletMergeCtx *> tablet_merge_ctx_array_;
  bool is_inited_;
};

class ObDirectLoadTabletMergeCtx
{
  friend class ObDirectLoadMergeTaskIterator;

public:
  ObDirectLoadTabletMergeCtx();
  ~ObDirectLoadTabletMergeCtx();
  void reset();
  int init(ObDirectLoadMergeCtx *merge_ctx,
           const table::ObTableLoadLSIdAndPartitionId &ls_partition_id);

  // 无导入数据合并任务
  int build_empty_data_merge_task(const ObDirectLoadTableDataDesc &table_data_desc,
                                  int64_t max_parallel_degree);
  // 有主键数据合并任务, 按分区排序
  int build_merge_task_for_sstable(const ObDirectLoadTableDataDesc &table_data_desc,
                                   const ObDirectLoadTableHandleArray &sstable_array,
                                   int64_t max_parallel_degree);
  // 有主键数据合并任务, 分区混合排序
  int build_merge_task_for_multiple_sstable(
    const ObDirectLoadTableDataDesc &table_data_desc,
    const ObDirectLoadTableHandleArray &multiple_sstable_array,
    ObDirectLoadMultipleMergeRangeSplitter &range_splitter, int64_t max_parallel_degree);
  // 无主键数据排序合并任务
  int build_merge_task_for_multiple_heap_table(
    const ObDirectLoadTableDataDesc &table_data_desc,
    const ObDirectLoadTableHandleArray &multiple_heap_table_array, int64_t max_parallel_degree);
  // 无主键数据聚合合并任务, 用于分区比较多的场景, 以分区为粒度进行并行合并
  int build_aggregate_merge_task_for_multiple_heap_table(
    const ObDirectLoadTableDataDesc &table_data_desc,
    const ObDirectLoadTableHandleArray &multiple_heap_table_array);

  // del lob task
  int build_del_lob_task(const ObDirectLoadTableDataDesc &table_data_desc,
                         const ObDirectLoadTableHandleArray &multiple_sstable_array,
                         ObDirectLoadMultipleMergeRangeSplitter &range_splitter,
                         const int64_t max_parallel_degree);

  // rescan task
  int build_rescan_task(int64_t thread_count);

  int inc_finish_count(int ret_code, bool &is_ready);

  bool merge_with_origin_data() const
  {
    return (nullptr != param_ &&
            ObDirectLoadMergeMode::is_merge_with_origin_data(param_->merge_mode_));
  }
  bool merge_with_conflict_check() const
  {
    return (nullptr != param_ &&
            ObDirectLoadMergeMode::is_merge_with_conflict_check(param_->merge_mode_));
  }
  bool merge_with_origin_query_for_data() const
  {
    return (nullptr != param_ &&
            ObDirectLoadMergeMode::is_merge_with_origin_query_for_data(param_->merge_mode_));
  }
  bool is_valid() const { return is_inited_; }
  const ObDirectLoadMergeParam *get_param() const { return param_; }
  const common::ObTabletID &get_tablet_id() const { return tablet_id_; }
  ObDirectLoadInsertTabletContext *get_insert_tablet_ctx() const { return insert_tablet_ctx_; }
  TO_STRING_KV(KP_(merge_ctx), KPC_(param), K_(tablet_id), KP_(insert_tablet_ctx), K_(range_array),
               K_(merge_task_array), K_(parallel_idx), K_(task_finish_cnt));

private:
  int build_empty_merge_task();
  int build_origin_data_merge_task(const ObDirectLoadTableDataDesc &table_data_desc,
                                   const int64_t max_parallel_degree);
  int build_origin_data_unrescan_merge_task(const ObDirectLoadTableDataDesc &table_data_desc,
                                            int64_t max_parallel_degree);
  int get_autoincrement_value(uint64_t count, share::ObTabletCacheInterval &interval);

private:
  common::ObArenaAllocator allocator_;
  ObDirectLoadMergeCtx *merge_ctx_;
  ObDirectLoadMergeParam *param_;
  common::ObTabletID tablet_id_;
  ObDirectLoadInsertTabletContext *insert_tablet_ctx_;
  ObDirectLoadOriginTable origin_table_;
  common::ObArray<blocksstable::ObDatumRange> range_array_;
  common::ObArray<ObDirectLoadIMergeTask *> merge_task_array_;
  int64_t parallel_idx_;
  mutable lib::ObMutex mutex_;
  int64_t task_finish_cnt_ CACHE_ALIGNED;
  int task_ret_code_;
  bool is_inited_;
};

class ObDirectLoadMergeTaskIterator
{
public:
  ObDirectLoadMergeTaskIterator();
  ~ObDirectLoadMergeTaskIterator();
  int init(ObDirectLoadMergeCtx *merge_ctx);
  int get_next_task(ObDirectLoadIMergeTask *&task);

private:
  ObDirectLoadMergeCtx *merge_ctx_;
  ObDirectLoadTabletMergeCtx *tablet_merge_ctx_;
  int64_t tablet_pos_;
  int64_t task_pos_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
