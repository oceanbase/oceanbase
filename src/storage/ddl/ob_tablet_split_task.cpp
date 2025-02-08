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
#include "ob_tablet_split_task.h"
#include "logservice/ob_log_service.h"
#include "share/ob_ddl_common.h"
#include "share/scn.h"
#include "storage/compaction/ob_tablet_merge_ctx.h"
#include "storage/ob_i_store.h"
#include "storage/ob_partition_range_spliter.h"
#include "storage/ddl/ob_ddl_merge_task.h"
#include "storage/ddl/ob_ddl_clog.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/access/ob_multiple_scan_merge.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace storage
{
// For split util.
// get all sstables that need to split.
int ObTabletSplitUtil::get_participants(
    const share::ObSplitSSTableType &split_sstable_type,
    const ObTableStoreIterator &const_table_store_iter,
    const bool is_table_restore,
    ObIArray<ObITable *> &participants)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  participants.reset();
  ObTableStoreIterator table_store_iter;
  if (OB_FAIL(table_store_iter.assign(const_table_store_iter))) {
    LOG_WARN("failed to assign table store iter", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      ObITable *table = nullptr;
      ObSSTableMetaHandle sstable_meta_hdl;
      if (OB_FAIL(table_store_iter.get_next(table))) {
        if (OB_UNLIKELY(OB_ITER_END == ret)) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("get next table failed", K(ret), K(table_store_iter));
        }
      } else if (OB_UNLIKELY(nullptr == table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(table));
      } else if (is_table_restore) {
        if (table->is_minor_sstable() || table->is_major_sstable()
            || ObITable::TableType::DDL_DUMP_SSTABLE == table->get_table_type()) {
          if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back major failed", K(ret));
          }
        } else {
          LOG_INFO("skip table", K(ret), KPC(table));
        }
      } else {
        if (!table->is_sstable()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), KPC(table));
        } else if (table->is_minor_sstable()) {
          if (share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type) {
            // Split with major only.
          } else if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back failed", K(ret));
          }
        } else if (table->is_major_sstable()) {
          if (share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type) {
            // Split with minor only.
          } else if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back major failed", K(ret));
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), KPC(table));
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::split_task_ranges(
    ObIAllocator &allocator,
    const share::ObDDLType ddl_type,
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t user_parallelism,
    const int64_t schema_tablet_size,
    ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list)
{
  int ret = OB_SUCCESS;
  parallel_datum_rowkey_list.reset();
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTableStoreIterator table_store_iterator;
  ObSEArray<ObStoreRange, 32> store_ranges;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> tables;
  const bool is_table_restore = ObDDLType::DDL_TABLE_RESTORE == ddl_type;
  common::ObArenaAllocator tmp_arena("SplitRange", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
    tablet_id, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_all_sstables(table_store_iterator))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      share::ObSplitSSTableType::SPLIT_BOTH, table_store_iterator, is_table_restore, tables))) {
    LOG_WARN("get participants failed", K(ret));
  } else if (is_table_restore && tables.empty()) {
    ObDatumRowkey tmp_min_key;
    ObDatumRowkey tmp_max_key;
    tmp_min_key.set_min_rowkey();
    tmp_max_key.set_max_rowkey();
    if (OB_FAIL(parallel_datum_rowkey_list.prepare_allocate(2))) { // min key and max key.
      LOG_WARN("reserve failed", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tmp_min_key.deep_copy(parallel_datum_rowkey_list.at(0), allocator))) {
      LOG_WARN("failed to push min rowkey", K(ret));
    } else if (OB_FAIL(tmp_max_key.deep_copy(parallel_datum_rowkey_list.at(1), allocator))) {
      LOG_WARN("failed to push min rowkey", K(ret));
    }
  } else {
    const ObITableReadInfo &rowkey_read_info = tablet_handle.get_obj()->get_rowkey_read_info();
    const int64_t tablet_size = RECOVER_TABLE_PARALLEL_MIN_TASK_SIZE; /*2M*/
    ObRangeSplitInfo range_info;
    ObPartitionRangeSpliter range_spliter;
    ObStoreRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(range_spliter.get_range_split_info(tables,
      rowkey_read_info, whole_range, range_info))) {
      LOG_WARN("init range split info failed", K(ret));
    } else if (OB_FALSE_IT(range_info.parallel_target_count_
      = MAX(1, MIN(user_parallelism, (range_info.total_size_ + tablet_size - 1) / tablet_size)))) {
    } else if (OB_FAIL(range_spliter.split_ranges(range_info,
      tmp_arena, false /*for_compaction*/, store_ranges))) {
      LOG_WARN("split ranges failed", K(ret), K(range_info));
    } else if (OB_UNLIKELY(store_ranges.count() <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error", K(ret), K(range_info));
    } else {
      const int64_t rowkey_arr_cnt = store_ranges.count() + 1; // with min and max.
      if (OB_FAIL(parallel_datum_rowkey_list.prepare_allocate(rowkey_arr_cnt))) {
        LOG_WARN("reserve failed", K(ret), K(rowkey_arr_cnt), K(store_ranges));
      } else {
        ObDatumRowkey tmp_key;
        for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_arr_cnt; i++) {
          if (i == 0) {
            tmp_key.set_min_rowkey();
            if (OB_FAIL(tmp_key.deep_copy(parallel_datum_rowkey_list.at(i), allocator))) {
              LOG_WARN("failed to push min rowkey", K(ret));
            }
          } else if (OB_FAIL(tmp_key.from_rowkey(store_ranges.at(i - 1).get_end_key().get_rowkey(), tmp_arena))) {
            LOG_WARN("failed to shallow copy from obj", K(ret));
          } else if (OB_FAIL(tmp_key.deep_copy(parallel_datum_rowkey_list.at(i), allocator))) {
            LOG_WARN("failed to deep copy end key", K(ret), K(i), "src_key", store_ranges.at(i - 1).get_end_key());
          }
        }
      }
    }
  }
  tmp_arena.reset();
  LOG_INFO("prepare task split ranges finished", K(ret), K(user_parallelism), K(schema_tablet_size), K(tablet_id), K(parallel_datum_rowkey_list));
  return ret;
}

int ObTabletSplitUtil::convert_datum_rowkey_to_range(
    ObIAllocator &allocator,
    const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list,
    ObIArray<ObDatumRange> &datum_ranges_array)
{
  int ret = OB_SUCCESS;
  datum_ranges_array.reset();
  ObDatumRange schema_rowkey_range;
  schema_rowkey_range.set_left_open();
  schema_rowkey_range.set_right_closed();
  for (int64_t i = 0; OB_SUCC(ret) && i < parallel_datum_rowkey_list.count() - 1; i++) {
    const ObDatumRowkey &start_key = parallel_datum_rowkey_list.at(i);
    const ObDatumRowkey &end_key = parallel_datum_rowkey_list.at(i + 1);
    if (OB_FAIL(start_key.deep_copy(schema_rowkey_range.start_key_, allocator))) { // deep copy necessary, to hold datum buffer.
      LOG_WARN("failed to deep copy start_key", K(ret), K(start_key));
    } else if (OB_FAIL(end_key.deep_copy(schema_rowkey_range.end_key_, allocator))) {
      LOG_WARN("failed to deep copy end_key", K(ret), K(end_key));
    } else if (OB_FAIL(datum_ranges_array.push_back(schema_rowkey_range))) { // buffer is kept by the schema_rowkey_range.
      LOG_WARN("failed to push back merge range to array", K(ret), K(schema_rowkey_range));
    }
  }
  LOG_INFO("change to datum range array finished", K(ret), K(parallel_datum_rowkey_list), K(datum_ranges_array));
  return ret;
}

} //end namespace stroage
} //end namespace oceanbase
