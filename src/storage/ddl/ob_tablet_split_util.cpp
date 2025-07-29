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

#define USING_LOG_PREFIX SHARE

#include "ob_tablet_split_util.h"
#include "storage/ob_partition_range_spliter.h"
#include "storage/tablet/ob_mds_scan_param_helper.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/tablet/ob_tablet_medium_info_reader.h"
#include "storage/truncate_info/ob_tablet_truncate_info_reader.h"
#include "storage/tx_storage/ob_ls_service.h"

#ifdef OB_BUILD_SHARED_STORAGE
#include "close_modules/shared_storage/meta_store/ob_shared_storage_obj_meta.h"
#include "storage/meta_store/ob_tenant_storage_meta_service.h"
#include "close_modules/shared_storage/share/compaction/ob_shared_storage_compaction_util.h"
#include "close_modules/shared_storage/storage/incremental/ob_ss_minor_compaction.h"
#include "storage/incremental/ob_shared_meta_service.h"
#include "storage/compaction_v2/ob_ss_compact_helper.h"
#endif

using namespace oceanbase::common;
using namespace oceanbase::obrpc;
using namespace oceanbase::share;
using namespace oceanbase::storage;
using namespace oceanbase::compaction;

bool ObTabletSplitRegisterMdsArg::is_valid() const
{
  /*      exec_tenant_id_(common::OB_INVALID_TENANT_ID),*/
  bool is_valid = !split_info_array_.empty() && OB_INVALID_TENANT_ID != tenant_id_
      && parallelism_ > 0 && ls_id_.is_valid() && is_tablet_split(task_type_) && table_schema_ != nullptr;
  for (int64_t i = 0; is_valid && i < lob_schema_versions_.count(); ++i) {
    is_valid = is_valid && lob_schema_versions_.at(i);
  }
  for (int64_t i = 0; is_valid && i < split_info_array_.count(); i++) {
    is_valid = is_valid && split_info_array_.at(i).is_valid();
  }
  return is_valid;
}

int ObTabletSplitRegisterMdsArg::assign(const ObTabletSplitRegisterMdsArg &other)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(other));
  } else if (OB_FAIL(split_info_array_.assign(other.split_info_array_))) {
    LOG_WARN("assign tablet split info failed", K(ret));
  } else if (OB_FAIL(lob_schema_versions_.assign(other.lob_schema_versions_))) {
    LOG_WARN("assign tablet lob_schema_versions_ failed", K(ret));
  } else {
    parallelism_ = other.parallelism_;
    is_no_logging_ = other.is_no_logging_;
    tenant_id_ = other.tenant_id_;
    src_local_index_tablet_count_ = other.src_local_index_tablet_count_;
    ls_id_ = other.ls_id_;
    task_type_ = other.task_type_;
    table_schema_ = other.table_schema_;
  }
  return ret;
}


int ObTabletSplitUtil::check_split_mds_can_be_accepted(
    const bool is_shared_storage_mode,
    const share::SCN &split_start_scn,
    const ObSSTableArray &old_store_mds_sstables,
    const ObIArray<ObITable *> &tables_array,
    bool &need_put_split_mds)
{
  int ret = OB_SUCCESS;
  need_put_split_mds = true;
  ObITable *new_mds_table = nullptr;
  if (OB_UNLIKELY(!split_start_scn.is_valid() || tables_array.count() != 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(split_start_scn), K(tables_array));
  } else if (OB_ISNULL(new_mds_table = tables_array.at(0))) {
    ret = OB_ERR_SYS;
    LOG_WARN("nullptr table", K(ret));
  } else if (OB_UNLIKELY(new_mds_table->get_start_scn() != SCN::base_scn()
        || (is_shared_storage_mode && new_mds_table->get_end_scn() < split_start_scn)
        || (!is_shared_storage_mode && new_mds_table->get_end_scn() != split_start_scn))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected scn_range", K(ret), K(is_shared_storage_mode), K(split_start_scn), KPC(new_mds_table));
  } else if (!old_store_mds_sstables.empty() && old_store_mds_sstables[0]->get_start_scn() < split_start_scn) {
    if (OB_LIKELY(old_store_mds_sstables[0]->get_start_scn() == SCN::base_scn()
        && old_store_mds_sstables[0]->get_end_scn() >= split_start_scn)) {
      need_put_split_mds = false; // put split_mds again.
      FLOG_INFO("ignore to push split-mds sstable repeatedly", K(ret), K(split_start_scn),
        "first_sstable", PC(old_store_mds_sstables[0]));
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err to push mds", K(ret), K(split_start_scn), "first_sstable", PC(old_store_mds_sstables[0]));
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_split_minors_can_be_accepted(
    const bool is_shared_storage_mode,
    const share::SCN &split_start_scn,
    const ObSSTableArray &old_store_minors,
    const ObIArray<ObITable *> &tables_array,
    share::ObScnRange &new_input_range,
    share::ObScnRange &old_store_range)
{
  int ret = OB_SUCCESS;
  new_input_range.reset();
  old_store_range.reset();
  const int64_t old_store_minors_cnt = old_store_minors.count();
  if (OB_UNLIKELY(!split_start_scn.is_valid() || tables_array.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(split_start_scn), K(tables_array));
  } else {
    new_input_range.start_scn_ = tables_array.at(0)->get_start_scn();
    new_input_range.end_scn_ = tables_array.at(tables_array.count() - 1)->get_end_scn();
    old_store_range.start_scn_ = old_store_minors_cnt == 0 ? split_start_scn : old_store_minors[0]->get_start_scn();
    old_store_range.end_scn_ = old_store_minors_cnt == 0 ? split_start_scn : old_store_minors[old_store_minors_cnt - 1]->get_end_scn();
    if (OB_UNLIKELY(!new_input_range.is_valid() || !old_store_range.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected scn_range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
    } else if (OB_UNLIKELY(split_start_scn < old_store_range.start_scn_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected old_store range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
    } else if (split_start_scn == old_store_range.start_scn_) { // update firstly.
      if (!is_shared_storage_mode) { // SN.
        if (OB_LIKELY(new_input_range.start_scn_ <= split_start_scn
                   && new_input_range.end_scn_ == split_start_scn
                   && new_input_range.end_scn_ <= old_store_range.end_scn_)) {
          LOG_TRACE("expected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        }
      } else { // SS.
        if (OB_LIKELY(new_input_range.start_scn_ <= split_start_scn
                   && new_input_range.end_scn_ >= split_start_scn
                   && new_input_range.end_scn_ <= old_store_range.end_scn_)) {
          LOG_TRACE("expected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        }
      }
    } else { // split_start_scn > old_store_range.start_scn_, update repeatedly.
      if (!is_shared_storage_mode) { // SN.
        if (OB_LIKELY(new_input_range.start_scn_ >= old_store_range.start_scn_
                   && new_input_range.start_scn_ <= split_start_scn
                   && new_input_range.end_scn_ == split_start_scn
                   && new_input_range.end_scn_ <= old_store_range.end_scn_)) {
          LOG_TRACE("expected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        }
      } else { // SS.
        if (OB_LIKELY(new_input_range.start_scn_ == old_store_range.start_scn_
                   && new_input_range.start_scn_ <= split_start_scn
                   && new_input_range.end_scn_ >= split_start_scn
                   && new_input_range.end_scn_ <= old_store_range.end_scn_)) {
          LOG_TRACE("expected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected scn range", K(ret), K(split_start_scn), K(new_input_range), K(old_store_range));
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::get_tablet(
    common::ObArenaAllocator &allocator,
    const ObLSHandle &ls_handle,
    const ObTabletID &tablet_id,
    const bool is_shared_mode,
    ObTabletHandle &tablet_handle,
    const ObMDSGetTabletMode mode)
{
  int ret = OB_SUCCESS;
  tablet_handle.reset();
  ObTabletHandle local_tablet_hdl;
  ObLS *ls = nullptr;
  const int64_t DDL_GET_TABLET_RETRY_TIMEOUT = 30 * 1000 * 1000; // 30s
  const int64_t timeout_ts = ObTimeUtility::current_time() + DDL_GET_TABLET_RETRY_TIMEOUT;
  if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ls should not be null", K(ret));
  } else if (OB_FAIL(ls->get_tablet_svr()->get_tablet_with_timeout(tablet_id,
                                                                   local_tablet_hdl,
                                                                   timeout_ts,
                                                                   mode))) {
    LOG_WARN("fail to get tablet handle", K(ret), K(tablet_id));
    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ret = OB_TIMEOUT;
    }
  } else if (!is_shared_mode) { // SN
    if (OB_FAIL(tablet_handle.assign(local_tablet_hdl))) {
      LOG_WARN("assign failed", K(ret));
    }
  } else { // SS
  #ifdef OB_BUILD_SHARED_STORAGE
    const share::ObLSID &ls_id = ls->get_ls_id();
    if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(ls_id, tablet_id,
          local_tablet_hdl.get_obj()->get_reorganization_scn()/*transfer_scn*/,
          allocator,
          tablet_handle))) {
      LOG_WARN("get ss tablet fail", K(ret));
    }
  #endif
  }
  if (OB_SUCC(ret) && OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(is_shared_mode), K(tablet_id), K(tablet_handle));
  }
  return ret;
}

// get all sstables that need to split.
int ObTabletSplitUtil::get_participants(
    const share::ObSplitSSTableType &split_sstable_type,
    const ObTableStoreIterator &const_table_store_iter,
    const bool is_table_restore,
    const ObIArray<ObITable::TableKey> &skipped_table_keys,
    ObIArray<ObITable *> &participants)
{
  int ret = OB_SUCCESS;
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
      } else if (OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected err", K(ret), KPC(table));
      } else if (is_contain(skipped_table_keys, table->get_key())) {
        FLOG_INFO("no need to split for the table", "table_key", table->get_key(), K(skipped_table_keys));
      } else if (table->is_minor_sstable()) {
        if (share::ObSplitSSTableType::SPLIT_MINOR == split_sstable_type
            || share::ObSplitSSTableType::SPLIT_BOTH == split_sstable_type) {
          if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back failed", K(ret));
          }
        }
      } else if (table->is_major_sstable()) {
        if (share::ObSplitSSTableType::SPLIT_MAJOR == split_sstable_type
          || share::ObSplitSSTableType::SPLIT_BOTH == split_sstable_type) {
          if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back major failed", K(ret));
          }
        }
      } else if (table->is_mds_sstable()) {
        if (share::ObSplitSSTableType::SPLIT_MDS == split_sstable_type) {
          if (OB_FAIL(participants.push_back(table))) {
            LOG_WARN("push back mds failed", K(ret));
          }
        }
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unknown table type", K(ret), KPC(table));
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
  ObSEArray<ObITable::TableKey, 1> empty_skipped_keys;
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
      share::ObSplitSSTableType::SPLIT_BOTH, table_store_iterator, is_table_restore, empty_skipped_keys, tables))) {
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
    ObRangeSplitInfo range_info;
    ObPartitionRangeSpliter range_spliter;
    ObStoreRange whole_range;
    whole_range.set_whole_range();
    if (OB_FAIL(range_spliter.get_range_split_info(tables,
       rowkey_read_info, whole_range, range_info))) {
      LOG_WARN("init range split info failed", K(ret));
    } else if (OB_FALSE_IT(range_info.parallel_target_count_
      = MAX(1, MIN(user_parallelism, (range_info.total_size_ + schema_tablet_size - 1) / schema_tablet_size)))) {
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
    LOG_INFO("prepare task split ranges finished", K(ret), K(user_parallelism), K(schema_tablet_size), K(tablet_id),
        K(parallel_datum_rowkey_list), K(range_info));
  }
  tmp_arena.reset();
  return ret;
}

int ObTabletSplitUtil::convert_rowkey_to_range(
    ObIAllocator &allocator,
    const ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list,
    ObIArray<ObDatumRange> &datum_ranges_array)
{
  int ret = OB_SUCCESS;
  datum_ranges_array.reset();
  ObDatumRange schema_rowkey_range;
  ObDatumRange multi_version_range;
  schema_rowkey_range.set_left_open();
  schema_rowkey_range.set_right_closed();
  for (int64_t i = 0; OB_SUCC(ret) && i < parallel_datum_rowkey_list.count() - 1; i++) {
    schema_rowkey_range.start_key_ = parallel_datum_rowkey_list.at(i); // shallow copy.
    schema_rowkey_range.end_key_ = parallel_datum_rowkey_list.at(i + 1); // shallow copy.
    multi_version_range.reset();
    if (OB_FAIL(schema_rowkey_range.to_multi_version_range(allocator, multi_version_range))) { // deep copy necessary, to hold datum buffer.
      LOG_WARN("failed to convert multi_version range", K(ret), K(schema_rowkey_range));
    } else if (OB_FAIL(datum_ranges_array.push_back(multi_version_range))) { // buffer is kept by the to_multi_version_range.
      LOG_WARN("failed to push back merge range to array", K(ret), K(multi_version_range));
    }
  }
  LOG_INFO("change to datum range array finished", K(ret), K(parallel_datum_rowkey_list), K(datum_ranges_array));
  return ret;
}

// only used for table recovery to build parallel tasks cross tenants.
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

// check whether the data tablet split finished.
int ObTabletSplitUtil::check_dest_data_completed(
    const ObLSHandle &ls_handle,
    const ObIArray<ObTabletID> &check_tablets_id,
    const bool check_remote,
    bool &is_completed)
{
  int ret = OB_SUCCESS;
  is_completed = true;
  if (OB_UNLIKELY(!ls_handle.is_valid() || check_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(check_tablets_id));
  } else {
    ObArenaAllocator tmp_arena("SplitChkData", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTabletHandle tmp_tablet_handle;
    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    for (int64_t i = 0; OB_SUCC(ret) && i < check_tablets_id.count() && is_completed; i++) {
      table_store_wrapper.reset();
      tmp_tablet_handle.reset();
      const ObTabletID &tablet_id = check_tablets_id.at(i);
      if (OB_FAIL(ObTabletSplitUtil::get_tablet(tmp_arena, ls_handle,
          tablet_id, check_remote/*is_shared_mode*/, tmp_tablet_handle,
          ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("get local tablet failed", K(ret), K(tablet_id), K(check_remote));
      } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_WARN("tablet handle is null", K(ret), K(tablet_id));
      } else if (tmp_tablet_handle.get_obj()->get_tablet_meta().split_info_.is_data_incomplete()) {
        is_completed = false;
      } else if (OB_FAIL(tmp_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
        LOG_WARN("fail to fetch table store", K(ret));
      } else if (OB_ISNULL(table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(false/*first*/))) {
        ret = OB_EAGAIN;
        LOG_WARN("wait for migration to copy sstable finished", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_data_split_finished(
    const share::ObLSID &ls_id,
    const ObTabletID &source_tablet_id,
    const ObIArray<ObTabletID> &dest_tablets_id,
    const bool can_reuse_macro_block,
    bool &is_split_finish_with_meta_flag)
{
  int ret = OB_SUCCESS;
  is_split_finish_with_meta_flag = false;
  ObLSHandle ls_handle;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || !source_tablet_id.is_valid()
      || dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(source_tablet_id), K(dest_tablets_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObTabletSplitUtil::check_dest_data_completed(
      ls_handle,
      dest_tablets_id,
      false/*check_remote*/,
      is_split_finish_with_meta_flag))) {
    LOG_WARN("check all major exist failed", K(ret), K(dest_tablets_id));
  } else if (!is_split_finish_with_meta_flag) {
  } else if (GCTX.is_shared_storage_mode() && can_reuse_macro_block) {
    // check source tablet cant_gc_macro_blks additionally.
    ObArenaAllocator tmp_arena("SplitGetTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTabletHandle ss_tablet_handle;
    if (OB_FAIL(ObTabletSplitUtil::get_tablet(tmp_arena,
        ls_handle, source_tablet_id, true/*is_shared_mode*/, ss_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("get ss tablet failed", K(ret), K(ls_id), K(source_tablet_id));
    } else if (OB_UNLIKELY(!ss_tablet_handle.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected err", K(ret), K(ls_id), K(source_tablet_id));
    } else if (!ss_tablet_handle.get_obj()->get_tablet_meta().split_info_.can_not_gc_macro_blks()) {
      is_split_finish_with_meta_flag = false;
      FLOG_INFO("CHANGE TO TRACE LATER, wait dag set cant gc blks", K(ret), K(ls_id), K(source_tablet_id));
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_src_tablet_table_store_ready(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &local_source_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTableStoreIterator table_store_iter;
  ObArray<ObTableHandleV2> memtable_handles;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !local_source_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(local_source_tablet_handle));
  } else if (OB_FAIL(local_source_tablet_handle.get_obj()->get_all_memtables_from_memtable_mgr(memtable_handles))) {
    LOG_WARN("failed to get_memtable_mgr for get all memtable", K(ret), "tablet", PC(local_source_tablet_handle.get_obj()));
  } else if (!memtable_handles.empty()) {
    ret = OB_NEED_RETRY;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("need retry to wait memtable dump", K(ret), "tablet_id", local_source_tablet_handle.get_obj()->get_tablet_id(), K(memtable_handles));
    }
  } else if (OB_FAIL(local_source_tablet_handle.get_obj()->get_all_tables(table_store_iter))) {
    LOG_WARN("failed to get all sstables", K(ret));
  } else {
    bool with_major_sstable = false;
    ObITable *table = nullptr;
    while (OB_SUCC(ret)) {
      table = nullptr;
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
      } else if (table->is_major_sstable()) {
        with_major_sstable = true;
      } else if (OB_LIKELY(table->is_minor_sstable() || table->is_mds_sstable())) {
        // do nothing.
      } else {
        ret = OB_NEED_RETRY;
        if (REACH_COUNT_INTERVAL(1000L)) {
          LOG_INFO("need retry to wait tables ready", K(ret),
            "tablet_id", local_source_tablet_handle.get_obj()->get_tablet_id(), "table_key", table->get_key());
        }
      }
    }
    if (OB_SUCC(ret) && !with_major_sstable) {
      ret = OB_NEED_RETRY;
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_INFO("need retry to wait tables ready, without major sstable", K(ret),
          "tablet_id", local_source_tablet_handle.get_obj()->get_tablet_id());
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_satisfy_split_condition(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &local_source_tablet_handle,
    const ObArray<ObTabletID> &dest_tablets_id,
    const int64_t compaction_scn,
    const share::SCN &min_split_start_scn
#ifdef OB_BUILD_SHARED_STORAGE
    , bool &is_data_split_executor
#endif
    )
{
  int ret = OB_SUCCESS;
  UNUSED(compaction_scn);
  ObArray<ObTableHandleV2> memtable_handles;
  ObTablet *tablet = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  bool is_tablet_status_need_to_split = false;
  share::SCN max_decided_scn;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !local_source_tablet_handle.is_valid()
      || dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(local_source_tablet_handle), K(dest_tablets_id));
  } else if (OB_ISNULL(tablet = local_source_tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(local_source_tablet_handle));
  } else if (OB_FAIL(check_tablet_restore_status(dest_tablets_id, ls_handle, local_source_tablet_handle, is_tablet_status_need_to_split))) {
    LOG_WARN("failed to check tablet status", K(ret), K(local_source_tablet_handle));
  } else if (OB_UNLIKELY(!is_tablet_status_need_to_split)) {
    ret = OB_TABLET_STATUS_NO_NEED_TO_SPLIT;
    LOG_WARN("there is no need to split, because of the special restore status of src tablet or des tablets",
      K(ret), K(local_source_tablet_handle), K(dest_tablets_id));
  } else if (OB_FAIL(check_tablet_ha_status(ls_handle, local_source_tablet_handle, dest_tablets_id))) {
    if (OB_NEED_RETRY != ret) {
      LOG_WARN("check tablet ha status failed", K(ret), K(ls_handle), K(local_source_tablet_handle), K(dest_tablets_id));
    }
  } else if (OB_FAIL(check_src_tablet_table_store_ready(ls_handle, local_source_tablet_handle))) {
    LOG_WARN("check src tablet table store tables failed", K(ret), K(ls_handle), K(local_source_tablet_handle));
  } else if (OB_FAIL(ls_handle.get_ls()->get_max_decided_scn(max_decided_scn))) {
    LOG_WARN("get max decided log ts failed", K(ret), "ls_id", ls_handle.get_ls()->get_ls_id(),
      "source_tablet_id", tablet->get_tablet_meta().tablet_id_);
    if (OB_STATE_NOT_MATCH == ret) {
      ret = OB_NEED_RETRY;
    }
  } else if (min_split_start_scn.is_valid_and_not_min() && SCN::plus(max_decided_scn, 1) < min_split_start_scn) {
    ret = OB_NEED_RETRY;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("need retry to wait max decided scn reach", K(ret), "ls_id", ls_handle.get_ls()->get_ls_id(),
        "source_tablet_id", tablet->get_tablet_meta().tablet_id_, K(max_decided_scn), K(min_split_start_scn));
    }
  } else if (MTL_TENANT_ROLE_CACHE_IS_RESTORE()) {
    if (GCTX.is_shared_storage_mode()) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("not support now", K(ret));
    } else {
      LOG_INFO("dont check compaction in restore progress", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_);
    }
  } else {
    const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
    ObArenaAllocator tmp_allocator("SplitGetMedium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID()); // for load medium info
    if (OB_FAIL(tablet->read_medium_info_list(tmp_allocator, medium_list))) {
      LOG_WARN("failed to load medium info list", K(ret), K(tablet));
    } else if (medium_list->size() > 0) {
      ret = OB_NEED_RETRY;
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_INFO("need retry to wait compact end", K(ret), "tablet_id", tablet->get_tablet_meta().tablet_id_, KPC(medium_list));
      }
  #ifdef OB_BUILD_SHARED_STORAGE
    } else if (GCTX.is_shared_storage_mode() &&
        OB_FAIL(ObSSDataSplitHelper::check_satisfy_ss_split_condition(ls_handle, local_source_tablet_handle, dest_tablets_id, is_data_split_executor))) {
      LOG_WARN("check satisfy ss split condition failed", K(ret), K(dest_tablets_id));
  #endif
    }
  }
  return ret;
}

int ObTabletSplitUtil::get_split_dest_tablets_info(
    const share::ObLSID &ls_id,
    const ObTabletID &source_tablet_id,
    ObIArray<ObTabletID> &dest_tablets_id,
    lib::Worker::CompatMode &compat_mode)
{
  int ret = OB_SUCCESS;
  dest_tablets_id.reset();
  compat_mode = lib::Worker::CompatMode::INVALID;
  ObTabletSplitMdsUserData data;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  if (OB_UNLIKELY(!ls_id.is_valid() || !source_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(source_tablet_id));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
                                               source_tablet_id,
                                               tablet_handle,
                                               ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("get tablet handle failed", K(ret), K(ls_id), K(source_tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(data.get_split_dst_tablet_ids(dest_tablets_id))) {
    LOG_WARN("failed to get split dst tablet ids", K(ret));
  } else if (OB_UNLIKELY(dest_tablets_id.count() < 2)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mds data", K(ret), K(ls_id), K(source_tablet_id), K(dest_tablets_id));
  } else {
    compat_mode = tablet_handle.get_obj()->get_tablet_meta().compat_mode_;
  }
  return ret;
}

int ObTabletSplitUtil::check_medium_compaction_info_list_cnt(
    const obrpc::ObCheckMediumCompactionInfoListArg &arg,
    obrpc::ObCheckMediumCompactionInfoListResult &result)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(arg));
  } else if (OB_FAIL(MTL(ObLSService *)->get_ls(arg.ls_id_, ls_handle, ObLSGetMod::DDL_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(arg));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
        arg.tablet_id_, tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet handle failed", K(ret), K(arg));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null", K(ret), K(arg));
  } else {
    ObSEArray<ObITable::TableKey, 1> empty_skipped_keys;
    common::ObArenaAllocator allocator("ChkMediumList", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTableStoreIterator table_store_iterator;
    ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_tables;
    const compaction::ObMediumCompactionInfoList *medium_info_list = nullptr;
    if (OB_FAIL(tablet->read_medium_info_list(allocator, medium_info_list))) {
      LOG_WARN("failed to get mediumn info list", K(ret));
    } else if (medium_info_list->size() > 0) {
      // compaction still ongoing
      result.info_list_cnt_ = medium_info_list->size();
      result.primary_compaction_scn_ = -1;
    } else if (OB_FAIL(tablet->get_all_sstables(table_store_iterator))) {
      LOG_WARN("fail to fetch table store", K(ret));
    } else if (OB_FAIL(ObTabletSplitUtil::get_participants(ObSplitSSTableType::SPLIT_MAJOR, table_store_iterator, false/*is_table_restore*/,
        empty_skipped_keys, major_tables))) {
      LOG_WARN("get participant sstables failed", K(ret));
    } else {
      result.info_list_cnt_ = 0;
      result.primary_compaction_scn_ = -1;
      ObSSTable *sstable = nullptr;
      for (int64_t i = 0; OB_SUCC(ret) && i < major_tables.count(); i++) {
        if (OB_ISNULL(sstable = static_cast<ObSSTable *>(major_tables.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected err", K(ret), KPC(sstable));
        } else {
          const int64_t snapshot_version = sstable->get_snapshot_version();
          result.primary_compaction_scn_ = MAX(result.primary_compaction_scn_, snapshot_version);
        }
      }
      if (OB_SUCC(ret) && -1 == result.primary_compaction_scn_) {
        ret = OB_EAGAIN;
        LOG_WARN("retry to wait major sstables ready", K(ret), K(major_tables));
      }
    }
  }
  LOG_INFO("receive check medium compaction info list", K(ret), K(arg), K(result));
  return ret;
}

int ObTabletSplitUtil::check_tablet_restore_status(
    const ObIArray<ObTabletID> &dest_tablets_id,
    const ObLSHandle &ls_handle,
    const ObTabletHandle &source_tablet_handle,
    bool &is_tablet_status_need_to_split)
{
  int ret = OB_SUCCESS;
  is_tablet_status_need_to_split = true;
  ObTablet *source_tablet = nullptr;
  ObTabletRestoreStatus::STATUS source_restore_status = ObTabletRestoreStatus::STATUS::RESTORE_STATUS_MAX;
  ObLS *ls = nullptr;
  if (OB_UNLIKELY(dest_tablets_id.count() <= 0 || !ls_handle.is_valid() || !source_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(dest_tablets_id), K(source_tablet_handle), K(ls_handle));
  } else if (OB_ISNULL(source_tablet = source_tablet_handle.get_obj()) || OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_NULL_CHECK_ERROR;
    LOG_WARN("unexpected nullptr", K(ret), KP(source_tablet), KP(ls));
  } else if (OB_FAIL(source_tablet->get_restore_status(source_restore_status))) {
    LOG_WARN("failed  to get restore status", K(ret), KP(source_tablet));
  } else if (OB_UNLIKELY(ObTabletRestoreStatus::STATUS::UNDEFINED == source_restore_status
      || ObTabletRestoreStatus::STATUS::EMPTY == source_restore_status || ObTabletRestoreStatus::STATUS::REMOTE == source_restore_status)) {
    is_tablet_status_need_to_split = false;
    ObTabletHandle t_handle;
    ObTablet *tablet = nullptr;
    ObTabletRestoreStatus::STATUS des_restore_status;
    ObArray<ObTabletRestoreStatus::STATUS> restore_status_arr; // for log print.
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablets_id.count() && !is_tablet_status_need_to_split; ++i) {
      t_handle.reset();
      des_restore_status = ObTabletRestoreStatus::STATUS::RESTORE_STATUS_MAX;
      const ObTabletID &t_id = dest_tablets_id.at(i);
      if ((OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, t_id, t_handle, ObMDSGetTabletMode::READ_ALL_COMMITED)))) {
        LOG_WARN("failed to get table", K(ret), K(t_id));
      } else if (OB_ISNULL(tablet = t_handle.get_obj())) {
        ret = OB_NULL_CHECK_ERROR;
        LOG_WARN("unexpected null ptr of tablet", K(ret), KPC(tablet));
      } else if (OB_FAIL(tablet->get_restore_status(des_restore_status))) {
        LOG_WARN("failed to get restore status of tablet", K(ret), K(tablet));
      } else if (OB_FAIL(restore_status_arr.push_back(des_restore_status))) {
        LOG_WARN("failed to push back into des_tablet_status", K(ret), K(i), K(des_restore_status));
      } else if (ObTabletRestoreStatus::STATUS::EMPTY == source_restore_status || ObTabletRestoreStatus::STATUS::REMOTE == source_restore_status) {
        if (ObTabletRestoreStatus::STATUS::FULL == des_restore_status) {
          is_tablet_status_need_to_split = true;
        } else if (ObTabletRestoreStatus::STATUS::REMOTE == des_restore_status) {
          // There are two scenarios,
          // 1. backup the source tablet and the dest tablets, and then restore with empty/remote status and data-completed status. (No need data-split)
          // 2. backup the source tablet only, and then restore replay data-split log to change the dest tablet status from `FULL` to `REMOTE` when data splitting. (Need data-split)
          if (tablet->get_tablet_meta().split_info_.is_data_incomplete()) {
            is_tablet_status_need_to_split = true;
            LOG_INFO("a tablet with REMOTE status but incomplete data, continue data splitting", "tablet_id", t_id);
          } else { /* do nothing. */ }
        }
      }
    }
    if (OB_SUCC(ret) && !is_tablet_status_need_to_split) {
      LOG_INFO("No need to execute data-split with expected completed data", "src_tablet_id", source_tablet_handle.get_obj()->get_tablet_id(), K(dest_tablets_id),
        K(source_restore_status), K(restore_status_arr));
    }
  }
  return ret;
}

int ObTabletSplitUtil::build_mds_sstable(
    common::ObArenaAllocator &allocator,
    const ObLSHandle &ls_handle,
    const ObTabletHandle &source_tablet_handle,
    const ObTabletID &dest_tablet_id,
    const share::SCN &reorganization_scn,
  #ifdef OB_BUILD_SHARED_STORAGE
    const ObSSDataSplitHelper &mds_ss_split_helper,
  #endif
    ObTableHandleV2 &mds_table_handle)
{
  int ret = OB_SUCCESS;
  mds_table_handle.reset();
  ObTabletHandle dest_tablet_handle;
  if (OB_UNLIKELY(!ls_handle.is_valid()
      || !source_tablet_handle.is_valid()
      || !dest_tablet_id.is_valid()
      || !reorganization_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(source_tablet_handle), K(dest_tablet_id), K(reorganization_scn));
  } else if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle,
      dest_tablet_id, dest_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
    LOG_WARN("get tablet failed", K(ret), K(dest_tablet_id));
  } else {
    const share::ObLSID &ls_id = ls_handle.get_ls()->get_ls_id();
    const ObTabletID &source_tablet_id = source_tablet_handle.get_obj()->get_tablet_id();
    HEAP_VARS_4 ((compaction::ObTabletMergeDagParam, param),
                 (compaction::ObTabletMergeCtx, tablet_merge_ctx, param, allocator),
		 (ObTabletDumpMediumMds2MiniOperator, op),
                 (ObMdsTableMiniMerger, mds_mini_merger)) {
    HEAP_VARS_2 ((ObTableScanParam, medium_info_scan_param),
                 (ObTabletMediumInfoReader, medium_info_reader)) {
    HEAP_VARS_2 ((ObTableScanParam, truncate_info_scan_param),
                 (ObTabletTruncateInfoReader, truncate_info_reader)) {
      ObMacroSeqParam macro_seq_param;
      macro_seq_param.seq_type_ = ObMacroSeqParam::SEQ_TYPE_INC;
      int64_t index_tree_start_seq = 0;
      ObMdsReadInfoCollector unused_collector;
      if (OB_FAIL(check_and_build_mds_sstable_merge_ctx(ls_handle, dest_tablet_handle, reorganization_scn, tablet_merge_ctx))) {
        LOG_WARN("prepare medium mds merge ctx failed", K(ret), K(ls_handle), K(dest_tablet_id));
      } else if (tablet_merge_ctx.static_param_.scn_range_.end_scn_.is_base_scn()) { // = 1
        LOG_INFO("no need to build lost mds sstable again", K(ls_id), K(source_tablet_id), K(dest_tablet_id));
    #ifdef OB_BUILD_SHARED_STORAGE
      } else if (GCTX.is_shared_storage_mode() && OB_FAIL(mds_ss_split_helper.generate_minor_macro_seq_info(
          0/*index in dest_tables_id*/,
          0/*the index in the generated minors*/,
          1/*the parallel cnt in one sstable*/,
          0/*the parallel idx in one sstable*/,
          macro_seq_param.start_))) {
        LOG_WARN("generate macro start seq failed", K(ret));
    #endif
      } else if (!GCTX.is_shared_storage_mode()
          && OB_FAIL(ObMdsTableMiniMerger::prepare_macro_seq_param(tablet_merge_ctx, macro_seq_param))) {
        LOG_WARN("prepare macro seq param failed", K(ret));
            } else if (OB_FAIL(mds_mini_merger.init(macro_seq_param, tablet_merge_ctx, op))) {
        LOG_WARN("fail to init mds mini merger", K(ret), K(tablet_merge_ctx), K(ls_id), K(dest_tablet_id));
      } else if (OB_FAIL((ObMdsScanParamHelper::build_customized_scan_param<compaction::ObMediumCompactionInfoKey, compaction::ObMediumCompactionInfo>(
          allocator,
          ls_id,
          source_tablet_id,
          ObMdsScanParamHelper::get_whole_read_version_range(),
          unused_collector,
          medium_info_scan_param)))) {
        LOG_WARN("fail to build scan param", K(ret), K(ls_id), K(source_tablet_id));
      } else if (OB_FAIL(medium_info_reader.init(*source_tablet_handle.get_obj(), medium_info_scan_param))) {
        LOG_WARN("failed to init medium info reader", K(ret));
      } else if (OB_FAIL((ObMdsScanParamHelper::build_customized_scan_param<ObTruncateInfoKey, ObTruncateInfo>(
          allocator,
          ls_id,
          source_tablet_id,
          ObMdsScanParamHelper::get_whole_read_version_range(),
          unused_collector,
          truncate_info_scan_param)))) {
        LOG_WARN("fail to build scan param", K(ret), K(ls_id), K(source_tablet_id));
      } else if (OB_FAIL(truncate_info_reader.init(*source_tablet_handle.get_obj(), truncate_info_scan_param))) {
        LOG_WARN("failed to init truncate info reader", K(ret));
      } else {
        bool has_mds_row = false;
        mds::MdsDumpKV *kv = nullptr;
        common::ObArenaAllocator iter_arena("SplitIterMedium", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
        // append medium info mds rows, mds_unit_id=3
        if (MTL_TENANT_ROLE_CACHE_IS_RESTORE()) {
          while (OB_SUCC(ret)) {
            iter_arena.reuse();
            if (OB_FAIL(medium_info_reader.get_next_mds_kv(iter_arena, kv))) {
              if (OB_ITER_END != ret) {
                LOG_WARN("iter medium mds failed", K(ret), K(ls_id), K(source_tablet_id));
              } else {
                ret = OB_SUCCESS;
                break;
              }
            } else if (OB_FAIL(op(*kv))) {
              LOG_WARN("write medium row failed", K(ret));
            } else {
              kv->mds::MdsDumpKV::~MdsDumpKV();
              iter_arena.free(kv);
              kv = nullptr;
              has_mds_row = true;
            }
          }
        } else {
          LOG_INFO("not restore tenant, no medium info lost", "tenant_id", MTL_ID(),
              "source_tablet_id", source_tablet_handle.get_obj()->get_tablet_id(), K(dest_tablet_id));
        }
        // append truncate info mds rows, mds_unit_id=5
        while (OB_SUCC(ret)) {
          iter_arena.reuse();
          if (OB_FAIL(truncate_info_reader.get_next_mds_kv(iter_arena, kv))) {
            if (OB_ITER_END != ret) {
              LOG_WARN("iter medium mds failed", K(ret), K(ls_id), K(source_tablet_id));
            } else {
              ret = OB_SUCCESS;
              break;
            }
          } else if (OB_FAIL(op(*kv))) {
            LOG_WARN("write medium row failed", K(ret));
          } else {
            kv->mds::MdsDumpKV::~MdsDumpKV();
            iter_arena.free(kv);
            kv = nullptr;
            has_mds_row = true;
          }
        }
        if (OB_SUCC(ret)) {
          if (!has_mds_row) {
            LOG_INFO("no need to build lost mds sstable", K(ls_id), K(source_tablet_id), K(dest_tablet_id));
          } else if (OB_FAIL(op.finish())) {
            LOG_WARN("finish failed", K(ret));
        #ifdef OB_BUILD_SHARED_STORAGE
          } else if (GCTX.is_shared_storage_mode()) {
            if (OB_FAIL(mds_ss_split_helper.generate_minor_macro_seq_info(
                    0/*index in dest_tables_id*/,
                    0/*the index in the generated minors*/,
                    1/*the parallel cnt in one sstable*/,
                    1/*the parallel idx in one sstable*/,
                    index_tree_start_seq))) {
                LOG_WARN("get macro seq failed", K(ret));
              } else if (OB_FAIL(mds_mini_merger.generate_ss_mds_mini_sstable(allocator, mds_table_handle, index_tree_start_seq))) {
                LOG_WARN("fail to generate ss mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
              }
        #endif
          } else if (OB_FAIL(mds_mini_merger.generate_mds_mini_sstable(allocator, mds_table_handle))) {
            LOG_WARN("fail to generate mds mini sstable with mini merger", K(ret), K(mds_mini_merger));
          }
        }
      }
    }
    }
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_and_build_mds_sstable_merge_ctx(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &dest_tablet_handle,
    const share::SCN &reorganization_scn,
    compaction::ObTabletMergeCtx &tablet_merge_ctx)
{
  int ret = OB_SUCCESS;
  ObLSService *ls_service = nullptr;
  share::SCN end_scn = reorganization_scn; // split_start_scn.
  share::SCN rec_scn = reorganization_scn; // split_start_scn.
  if (OB_UNLIKELY(!ls_handle.is_valid()
      || !dest_tablet_handle.is_valid()
      || !reorganization_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(reorganization_scn), K(ls_handle), K(dest_tablet_handle));
  } else if (OB_FAIL(tablet_merge_ctx.tablet_handle_.assign(dest_tablet_handle))) {
    LOG_WARN("failed to assign tablet_handle", K(ret), K(dest_tablet_handle));
  } else {
    compaction::ObStaticMergeParam &static_param = tablet_merge_ctx.static_param_;
    static_param.ls_handle_             = ls_handle;
    static_param.dag_param_.ls_id_      = ls_handle.get_ls()->get_ls_id();
    static_param.dag_param_.merge_type_ = compaction::ObMergeType::MDS_MINI_MERGE;
    static_param.dag_param_.tablet_id_  = dest_tablet_handle.get_obj()->get_tablet_id();
    static_param.pre_warm_param_.type_  = ObPreWarmerType::MEM_PRE_WARM;
    static_param.scn_range_.start_scn_               = SCN::base_scn(); // 1
    static_param.scn_range_.end_scn_                 = end_scn;
    static_param.version_range_.snapshot_version_    = end_scn.get_val_for_tx();
    static_param.version_range_.multi_version_start_ = dest_tablet_handle.get_obj()->get_multi_version_start();
    static_param.merge_scn_                          = end_scn;
    static_param.create_snapshot_version_            = 0;
    static_param.need_parallel_minor_merge_          = false;
    static_param.tablet_transfer_seq_                  = dest_tablet_handle.get_obj()->get_transfer_seq();
    static_param.rec_scn_ = rec_scn;
    tablet_merge_ctx.static_desc_.tablet_transfer_seq_ = dest_tablet_handle.get_obj()->get_transfer_seq();
    if (GCTX.is_shared_storage_mode()) {
      static_param.dag_param_.exec_mode_                = ObExecMode::EXEC_MODE_OUTPUT;
      tablet_merge_ctx.static_desc_.exec_mode_          = ObExecMode::EXEC_MODE_OUTPUT;
      tablet_merge_ctx.static_desc_.reorganization_scn_ = reorganization_scn;
    }

    if (OB_FAIL(tablet_merge_ctx.init_tablet_merge_info())) {
      LOG_WARN("failed to init tablet merge info", K(ret), K(ls_handle), K(dest_tablet_handle), K(tablet_merge_ctx));
    }
  }
  return ret;
}

int ObTabletSplitUtil::check_sstables_skip_data_split(
    const ObLSHandle &ls_handle,
    const ObTableStoreIterator &source_table_store_iter,
    const ObIArray<ObTabletID> &dest_tablets_id,
    const int64_t lob_major_snapshot/*OB_INVALID_VERSION for non lob tablets*/,
    ObIArray<ObITable::TableKey> &skipped_split_major_keys)
{
  int ret = OB_SUCCESS;
  skipped_split_major_keys.reset();
  ObSEArray<ObITable::TableKey, 1> empty_skipped_keys;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> major_sstables;
  if (OB_UNLIKELY(dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablets_id));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      share::ObSplitSSTableType::SPLIT_MAJOR, source_table_store_iter, false/*is_table_restore*/, empty_skipped_keys, major_sstables))) {
    LOG_WARN("get all majors failed", K(ret), K(dest_tablets_id));
  } else if (OB_UNLIKELY(major_sstables.empty())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no major in source tablet", K(ret));
  } else {
    const bool is_lob_tablet = OB_INVALID_VERSION != lob_major_snapshot;
    const int64_t checked_table_cnt = is_lob_tablet ? 1 : major_sstables.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < checked_table_cnt; i++) {
      bool need_data_split = false;
      ObITable::TableKey this_key = major_sstables.at(i)->get_key();
      this_key.version_range_.snapshot_version_ = is_lob_tablet ? lob_major_snapshot : this_key.version_range_.snapshot_version_;
      for (int64_t j = 0; OB_SUCC(ret) && !need_data_split && j < dest_tablets_id.count(); j++) {
        ObSSTableWrapper sstable_wrapper;
        ObTabletHandle tmp_tablet_handle;
        const ObTabletID &this_tablet_id = dest_tablets_id.at(j);
        this_key.tablet_id_ = this_tablet_id;
        ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
        if (OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, this_tablet_id,
            tmp_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
          LOG_WARN("get tablet handle failed", K(ret), K(this_tablet_id));
        } else if (OB_UNLIKELY(nullptr == tmp_tablet_handle.get_obj())) {
          ret = OB_ERR_SYS;
          LOG_WARN("tablet handle is null", K(ret), K(this_tablet_id));
        } else if (OB_FAIL(tmp_tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
          LOG_WARN("fail to fetch table store", K(ret));
        } else if (OB_FAIL(table_store_wrapper.get_member()->get_sstable(this_key, sstable_wrapper))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            need_data_split = true;
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("fail to get table from table store", K(ret), K(this_key));
          }
        }
      }
      if (OB_SUCC(ret) && !need_data_split) {
        if (OB_FAIL(skipped_split_major_keys.push_back(this_key))) {
          LOG_WARN("push back failed", K(ret), K(this_key));
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::get_storage_schema_from_mds(
    const ObTabletHandle &tablet_handle,
    ObStorageSchema *&storage_schema,
    ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  uint64_t tenant_id = MTL_ID();
  uint64_t data_version = 0;
  const ObStorageSchema *tmp_storage_schema = nullptr;
  void *buf = nullptr;
  storage_schema = nullptr;
  if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObStorageSchema)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("alloc mem failed", K(ret));
  } else if (FALSE_IT(storage_schema = new (buf) ObStorageSchema())) {
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(tenant_id, data_version))) {
    LOG_WARN("failed to get min data version", K(ret), K(tenant_id));
  } else if (data_version >= DATA_VERSION_4_4_0_0) {
    ObTabletSplitInfoMdsUserData split_info_data;
    if (OB_FAIL(tablet_handle.get_obj()->get_split_info_data(share::SCN::max_scn(), split_info_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
      LOG_WARN("failed to get split data", K(ret));
    } else if (OB_FAIL(split_info_data.get_storage_schema(tmp_storage_schema))) {
    } else if (OB_FAIL(storage_schema->init(allocator, *tmp_storage_schema))) {
      LOG_WARN("failed to init storage schema", K(ret));
    }
  } else {
    ObTabletSplitMdsUserData split_data;
    if (OB_FAIL(tablet_handle.get_obj()->get_split_data(split_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
      LOG_WARN("failed to get split data", K(ret));
    } else if (OB_FAIL(split_data.get_storage_schema(tmp_storage_schema))) {
      LOG_WARN("failed ot get storage schema", K(ret));
    } else if (OB_FAIL(storage_schema->init(allocator, *tmp_storage_schema))) {
      LOG_WARN("failed to init storage schema", K(ret));
    }
  }
  return ret;
}

int ObTabletSplitUtil::register_split_info_mds(rootserver::ObDDLService &ddl_service, const ObTabletSplitRegisterMdsArg &arg)
{
  int ret = OB_SUCCESS;
  int64_t refreshed_schema_version = 0;
  ObSchemaGetterGuard schema_guard;
  ObTabletSplitInfoMdsArg split_info_arg;
  common::ObArenaAllocator allocator("regissplimds", OB_MALLOC_NORMAL_BLOCK_SIZE, common::OB_SERVER_TENANT_ID, ObCtxIds::DEFAULT_CTX_ID);
  common::ObMySQLTransaction trans;
  if (OB_UNLIKELY(!arg.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.schema_service_));
  } else if (OB_FAIL(ddl_service.get_tenant_schema_guard_with_version_in_inner_table(arg.tenant_id_, schema_guard))) {
    LOG_WARN("get schema guard failed", K(ret));
  } else if (OB_ISNULL(GCTX.sql_proxy_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(GCTX.sql_proxy_));
  } else {
    split_info_arg.ls_id_ = arg.ls_id_;
    split_info_arg.tenant_id_ = arg.tenant_id_;
    const common::ObSArray<ObTabletSplitArg> &tmp_split_info_array = arg.split_info_array_;
    ObTabletSplitInfoMdsUserData split_info_mds_data;
    const ObTableSchema *table_schema = nullptr;
    bool is_oracle_mode = false;
    lib::Worker::CompatMode compat_mode = lib::Worker::CompatMode::INVALID;
    const int64_t lob_tablet_start_idx = 1 /*data_tablet*/ + arg.src_local_index_tablet_count_;
    const int64_t index_tablet_start_idx = 1 /*data_tablet*/;

    for (int64_t i = 0; OB_SUCC(ret) && i < tmp_split_info_array.count(); ++i) {
      split_info_mds_data.reset();
      const obrpc::ObTabletSplitArg &split_info = tmp_split_info_array.at(i);
      bool is_lob = i >= lob_tablet_start_idx;
      bool is_index = (i >= index_tablet_start_idx) && (i < lob_tablet_start_idx);
      bool is_data_table = i == 0;
      if (is_data_table) {
        table_schema = arg.table_schema_;
      } else if ((is_lob && OB_FAIL(schema_guard.get_table_schema(arg.tenant_id_, split_info.lob_table_id_, table_schema)))) {
        LOG_WARN("failed to get table schema", K(ret), K(arg.tenant_id_), K(split_info.lob_table_id_));
      } else if (is_index && OB_FAIL(schema_guard.get_table_schema(arg.tenant_id_, split_info.table_id_, table_schema))) {
        table_schema = arg.table_schema_;
      }
      if (OB_FAIL(ret)) {
      } else if (OB_ISNULL(table_schema)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table schema is null", K(ret), K(arg.tenant_id_), K(split_info.table_id_));
      } else if (OB_FAIL((split_info_mds_data.assign_datum_rowkey_list(split_info.parallel_datum_rowkey_list_)))) {
        LOG_WARN("failed to assign datum rowkey list", K(ret), K(split_info.parallel_datum_rowkey_list_));
      } else if (!is_lob && OB_FAIL(split_info_mds_data.dest_tablets_id_.assign(split_info.dest_tablets_id_))) {
        LOG_WARN("failed to assign dest_tablets_id_", K(ret), K(split_info.dest_tablets_id_));
      } else if (is_lob && OB_FAIL(split_info_mds_data.dest_tablets_id_.push_back(split_info.source_tablet_id_))) {
        LOG_WARN("failed to push back into dest_tablets_id_", K(split_info.source_tablet_id_));
      } else {
        split_info_mds_data.table_id_ = split_info.table_id_;
        split_info_mds_data.lob_table_id_ = split_info.lob_table_id_;
        split_info_mds_data.schema_version_ = split_info.schema_version_;
        split_info_mds_data.dest_schema_version_ = is_lob ? arg.lob_schema_versions_.at(i - lob_tablet_start_idx) : split_info.schema_version_;
        split_info_mds_data.tablet_task_id_ = is_index ? (i) : (is_lob ? i - lob_tablet_start_idx + 1 : 1);
        split_info_mds_data.task_id_ = split_info.task_id_;
        split_info_mds_data.source_tablet_id_ = split_info.source_tablet_id_;
        split_info_mds_data.data_format_version_ = split_info.data_format_version_;
        split_info_mds_data.consumer_group_id_ = split_info.consumer_group_id_;
        split_info_mds_data.can_reuse_macro_block_ = is_lob ? tmp_split_info_array.at(0).can_reuse_macro_block_ /*data_tablet*/ : split_info.can_reuse_macro_block_;
        split_info_mds_data.is_no_logging_ = arg.is_no_logging_;
        split_info_mds_data.split_sstable_type_ = split_info.split_sstable_type_;
        split_info_mds_data.task_type_ = arg.task_type_;
        split_info_mds_data.parallelism_ = arg.parallelism_;
        if (OB_FAIL(table_schema->check_if_oracle_compat_mode(is_oracle_mode))) {
          LOG_WARN("failed to check oracle mode", KR(ret), K(arg.tenant_id_), KPC(table_schema));
        } else {
          compat_mode = is_oracle_mode ? lib::Worker::CompatMode::ORACLE : lib::Worker::CompatMode::MYSQL;
          if (OB_FAIL(split_info_mds_data.lob_col_idxs_.assign(split_info.lob_col_idxs_))) {
            LOG_WARN("failed to assign lob_col_idxs_", K(ret), K(split_info.lob_col_idxs_));
          } else if (OB_FAIL(split_info_mds_data.storage_schema_.init(allocator, *table_schema, compat_mode, false/*skip_column_info*/))) {
            LOG_WARN("failed to assign partkey projector", K(ret));
          } else if (OB_FAIL(split_info_arg.split_info_datas_.push_back(split_info_mds_data))) {
            LOG_WARN("failed to push back into split_info_arg", K(ret));
          }
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(GCTX.sql_proxy_, arg.tenant_id_))) {
      LOG_WARN("start transaction failed", K(ret));
    } else if (OB_FAIL(ObTabletSplitInfoMdsHelper::register_mds(split_info_arg, trans))) {
      LOG_WARN("failed to register mds", KR(ret), K(split_info_arg));
    }
    const bool is_commit = OB_SUCC(ret);
    if (trans.is_started()) {
      int temp_ret = OB_SUCCESS;
      if (OB_SUCCESS != (temp_ret = trans.end(is_commit))) {
        LOG_WARN("trans end failed", K(is_commit), K(temp_ret));
        ret = is_commit ? temp_ret : ret;
      }
    }
  }
  return ret;
}

// For migration, wait data status COMPLETE.
// For restore, wait restore status FULL/REMOTE.
int ObTabletSplitUtil::check_tablet_ha_status(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &source_tablet_handle,
    const ObIArray<ObTabletID> &dest_tablets_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_handle.is_valid() || !source_tablet_handle.is_valid() || dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(source_tablet_handle), K(dest_tablets_id));
  } else if (!source_tablet_handle.get_obj()->get_tablet_meta().ha_status_.check_allow_read()) {
    ret = OB_NEED_RETRY;
    if (REACH_COUNT_INTERVAL(1000L)) {
      LOG_INFO("need retry to wait data complete", K(ret),
          "tablet_meta", source_tablet_handle.get_obj()->get_tablet_meta());
    }
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablets_id.count(); i++) {
      ObTabletHandle tmp_tablet_handle;
      const ObTabletID &tablet_id = dest_tablets_id.at(i);
      if ((OB_FAIL(ObDDLUtil::ddl_get_tablet(ls_handle, tablet_id, tmp_tablet_handle, ObMDSGetTabletMode::READ_ALL_COMMITED)))) {
        LOG_WARN("get tablet failed", K(ret), K(tablet_id));
      } else if (OB_UNLIKELY(!tmp_tablet_handle.is_valid())) {
        ret = OB_ERR_SYS;
        LOG_WARN("invalid tablet", K(ret), K(tablet_id), K(tmp_tablet_handle));
      } else if (!tmp_tablet_handle.get_obj()->get_tablet_meta().ha_status_.check_allow_read()) {
        ret = OB_NEED_RETRY;
        if (REACH_COUNT_INTERVAL(1000L)) {
          LOG_INFO("need retry to wait data complete", K(ret),
              "tablet_meta", tmp_tablet_handle.get_obj()->get_tablet_meta());
        }
      }
    }
  }
  return ret;
}

int ObTabletSplitUtil::build_update_table_store_param(
    const share::SCN &reorg_scn,
    const int64_t ls_rebuild_seq,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObTabletID &dst_tablet_id,
    const ObTablesHandleArray &tables_handle,
    const compaction::ObMergeType &merge_type,
    const ObIArray<ObITable::TableKey> &skipped_split_major_keys,
    ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  param.reset();
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> batch_tables;

  if (OB_UNLIKELY(!dst_tablet_id.is_valid()
      || !is_valid_merge_type(merge_type)
      || (!is_major_merge_type(merge_type) && tables_handle.empty())
      || (is_major_merge_type(merge_type) && snapshot_version < 0))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(snapshot_version), K(multi_version_start),
      K(dst_tablet_id), K(merge_type), K(tables_handle));
  } else if (!is_major_merge_type(merge_type)) { // minor merge or mds mini merge.
    if (OB_FAIL(param.tables_handle_.assign(tables_handle))) {
      LOG_WARN("assign failed", K(ret), K(tables_handle));
    }
  } else if (OB_FAIL(tables_handle.get_tables(batch_tables))) {
    LOG_WARN("get batch sstables failed", K(ret));
  } else {
    // ATTENTION, Meta major sstable should be placed at the end of the array.
    ObTableHandleV2 meta_major_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < batch_tables.count(); i++) {
      const ObITable *table = batch_tables.at(i);
      ObTableHandleV2 table_handle;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error", K(ret), K(i), K(batch_tables));
      } else if (OB_UNLIKELY(table->is_meta_major_sstable())) {
        if (OB_UNLIKELY(meta_major_handle.is_valid())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("more than 1 meta major", K(ret), K(meta_major_handle), K(batch_tables));
        } else if (OB_FAIL(tables_handle.get_table(i, meta_major_handle))) {
          LOG_WARN("get handle failed", K(ret), K(i), K(batch_tables));
        }
      } else if (OB_FAIL(tables_handle.get_table(i, table_handle))) {
        LOG_WARN("get handle failed", K(ret));
      } else if (OB_FAIL(param.tables_handle_.add_table(table_handle))) {
        LOG_WARN("add table failed", K(ret));
      }
    }
    if (OB_SUCC(ret) && meta_major_handle.is_valid()) {
      if (OB_FAIL(param.tables_handle_.add_table(meta_major_handle))) {
        LOG_WARN("add table failed", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && is_major_merge_type(merge_type)) {
    if (OB_FAIL(param.tablet_split_param_.skip_split_keys_.assign(skipped_split_major_keys))) {
      LOG_WARN("assign failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    param.tablet_split_param_.snapshot_version_         = snapshot_version;
    param.tablet_split_param_.multi_version_start_      = multi_version_start;
    param.tablet_split_param_.merge_type_               = merge_type;
    param.rebuild_seq_                                  = ls_rebuild_seq; // old rebuild seq.
    param.release_mds_scn_.set_min();
    param.reorg_scn_ = reorg_scn;
  }
  FLOG_INFO("DEBUG CODE, CHANGE TO TRACE LATER", K(ret), K(param));
  return ret;
}

int ObTabletSplitUtil::persist_tablet_mds_on_demand(
    ObLS *ls,
    const ObTabletHandle &local_tablet_handle,
    bool &has_mds_table_for_dump)
{
  int ret = OB_SUCCESS;
  has_mds_table_for_dump = false;
  SCN decided_scn;
  mds::MdsTableHandle mds_table;
  ObTabletSplitInfoMdsUserData split_info_data;
  if (OB_UNLIKELY(nullptr == ls || !local_tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), KPC(ls), K(local_tablet_handle));
  } else if (OB_FAIL(local_tablet_handle.get_obj()->get_split_info_data(share::SCN::max_scn(), split_info_data,
      ObTabletCommon::DEFAULT_GET_TABLET_DURATION_10_S))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(local_tablet_handle.get_obj()->get_mds_table_for_dump(mds_table))) {
    if (OB_EMPTY_RESULT != ret) {
      LOG_WARN("get mds table failed", K(ret), KPC(local_tablet_handle.get_obj()));
    } else { // no mds table.
      ret = OB_SUCCESS;
    }
  } else if (OB_FAIL(ls->get_max_decided_scn(decided_scn))) {
    LOG_WARN("decide_max_decided_scn failed", K(ret), "ls_id", ls->get_ls_id());
  } else if (OB_FAIL(local_tablet_handle.get_obj()->mds_table_flush(decided_scn))) {
    LOG_WARN("persist mds table failed", K(ret), KPC(local_tablet_handle.get_obj()));
  } else {
    has_mds_table_for_dump = true;
    LOG_INFO("CHANGE TO TRACE LATER", K(ret), K(has_mds_table_for_dump),
      "ls_id", ls->get_ls_id(), "tablet_id", local_tablet_handle.get_obj()->get_tablet_meta().tablet_id_);
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
template <typename T>
void ObSSDataSplitHelper::release_handles_array(
    ObIArray<T *> &arr)
{
  for (int64_t i = 0; i < arr.count(); i++) {
    if (OB_NOT_NULL(arr.at(i))) {
      arr.at(i)->~T();
      allocator_.free(arr.at(i));
    }
  }
}

void ObSSDataSplitHelper::reset()
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < split_sstable_list_handle_.count(); i++) {
    if (split_sstable_list_handle_[i]->is_valid()
        && i < add_minor_op_handle_.count() && add_minor_op_handle_[i]->is_valid()
        && !add_minor_op_handle_[i]->get_atomic_op()->is_committed()) {
      if (OB_FAIL(split_sstable_list_handle_[i]->get_atomic_file()->abort_op(*add_minor_op_handle_[i]))) {
        LOG_ERROR("abort failed", K(ret));
      }
    } else {
      LOG_INFO("abort op", K(ret), K(i));
    }
  }
  release_handles_array(add_minor_op_handle_);
  release_handles_array(split_sstable_list_handle_);
  add_minor_op_handle_.reset();
  split_sstable_list_handle_.reset();
  allocator_.reset();
}

int ObSSDataSplitHelper::get_op_id(
  const int64_t dest_tablet_index,
  int64_t &op_id) const
{
  int ret = OB_SUCCESS;
  op_id = 0;
  if (OB_UNLIKELY(dest_tablet_index >= add_minor_op_handle_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), "op_count", add_minor_op_handle_.count());
  } else if (OB_ISNULL(add_minor_op_handle_[dest_tablet_index])) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected err", K(ret), K(dest_tablet_index));
  } else {
    op_id = add_minor_op_handle_[dest_tablet_index]->get_atomic_op()->get_op_id();
  }
  return ret;
}

int ObSSDataSplitHelper::prepare_minor_gc_info_list(
    const int64_t parallel_cnt_of_each_sstable,
    const int64_t sstables_cnt_of_each_tablet,
    const ObAtomicOpHandle<ObAtomicSSTableListAddOp> &op_handle,
    ObSSMinorGCInfo &minor_gc_info)
{
  int ret = OB_SUCCESS;
  const int64_t op_id = op_handle.get_atomic_op()->get_op_id();
  const int64_t gc_reply_parallel_cnt = parallel_cnt_of_each_sstable/*parallel child task*/ + 1/*IndexTree*/;
  const int64_t start_macro_seq = op_id << SPLIT_MINOR_MACRO_DATA_SEQ_BITS;
  minor_gc_info.start_seq_ = start_macro_seq;
  minor_gc_info.seq_step_ = compaction::MACRO_STEP_SIZE;
  minor_gc_info.parallel_cnt_ = gc_reply_parallel_cnt * sstables_cnt_of_each_tablet;
  return ret;
}

int ObSSDataSplitHelper::start_add_op(
    const ObLSID &ls_id,
    const share::SCN &split_scn,
    const ObAtomicFileType &file_type,
    const int64_t parallel_cnt_of_each_sstable,
    const int64_t sstables_cnt_of_each_tablet,
    const ObIArray<ObTabletID> &dest_tablets_id)
{
  int ret = OB_SUCCESS;
  const int64_t dest_tablets_count = dest_tablets_id.count();
  if (OB_UNLIKELY(!ls_id.is_valid()
      || !split_scn.is_valid()
      || parallel_cnt_of_each_sstable < 1
      || sstables_cnt_of_each_tablet < 1
      || dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(split_scn), K(file_type),
        K(parallel_cnt_of_each_sstable), K(sstables_cnt_of_each_tablet), K(dest_tablets_id));
  } else if (OB_FAIL(split_sstable_list_handle_.prepare_allocate(dest_tablets_count))) {
    LOG_WARN("init failed", K(ret));
  } else if (OB_FAIL(add_minor_op_handle_.prepare_allocate(dest_tablets_count))) {
    LOG_WARN("init failed", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablets_count; i++) {
      ObAtomicFileHandle<ObAtomicSSTableListFile> *&file_handle = split_sstable_list_handle_[i];
      ObAtomicOpHandle<ObAtomicSSTableListAddOp> *&op_handle = add_minor_op_handle_[i];
      if (OB_ISNULL(file_handle = OB_NEWx(ObAtomicFileHandle<ObAtomicSSTableListFile>, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret));
      } else if (OB_ISNULL(op_handle = OB_NEWx(ObAtomicOpHandle<ObAtomicSSTableListAddOp>, &allocator_))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("alloc failed", K(ret));
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < dest_tablets_count; i++) {
      ObSSMinorGCInfo ss_minor_gc_info;
      const ObTabletID &dest_tablet_id = dest_tablets_id.at(i);
      ObAtomicFileHandle<ObAtomicSSTableListFile> &file_handle = *split_sstable_list_handle_[i];
      ObAtomicOpHandle<ObAtomicSSTableListAddOp> &op_handle = *add_minor_op_handle_[i];
      if (OB_FAIL(MTL(ObAtomicFileMgr*)->get_tablet_file_handle(
                  ls_id, dest_tablet_id, file_type, split_scn, file_handle))) {
        LOG_WARN("get tablet file handle failed", K(ret));
      } else if (OB_FAIL(file_handle.get_atomic_file()->create_op(op_handle))) {
        LOG_WARN("create op handle failed", K(ret));
      } else if (OB_FAIL(prepare_minor_gc_info_list(parallel_cnt_of_each_sstable,
        sstables_cnt_of_each_tablet, op_handle, ss_minor_gc_info))) {
        LOG_WARN("prepare minor task info list failed", K(ret));
      } else if (OB_FAIL(op_handle.get_atomic_op()->update_gc_info(ss_minor_gc_info))) {
        LOG_WARN("write task state fail", K(ret));
      }
    }
  }
  return ret;
}

int ObSSDataSplitHelper::generate_major_macro_seq_info(
    const int64_t major_index /*from 0, ith*/,
    const int64_t parallel_cnt,
    const int64_t parallel_idx /*from 0, kth*/,
    int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  macro_start_seq = 0;
  if (OB_UNLIKELY(major_index < 0
      || parallel_cnt < 1
      || parallel_idx < 0
      || parallel_idx > parallel_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret),  K(major_index), K(parallel_cnt), K(parallel_idx));
  } else {
    macro_start_seq = (parallel_cnt + 1/*tree placeholder*/) * major_index * MACRO_STEP_SIZE /*start_seq of the ith major*/
      + parallel_idx * MACRO_STEP_SIZE /*data blk's start_seq of the kth child task in the ith major*/;
  }
  return ret;
}

int ObSSDataSplitHelper::get_major_macro_seq_by_stage(
    const ObGetMacroSeqStage &stage,
    const int64_t majors_cnt /*cnt of the dst_majors */,
    const int64_t parallel_cnt,
    int64_t &macro_start_seq)
{
  int ret = OB_SUCCESS;
  macro_start_seq = 0;
  if (OB_UNLIKELY(majors_cnt < 1 || parallel_cnt < 1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(majors_cnt), K(parallel_cnt));
  } else if (OB_UNLIKELY(ObGetMacroSeqStage::GET_NEW_ROOT_MACRO_SEQ != stage)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(stage));
  } else if (ObGetMacroSeqStage::GET_NEW_ROOT_MACRO_SEQ == stage) {
    if (OB_UNLIKELY(parallel_cnt < 1 || majors_cnt < 1)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid arg", K(ret), K(parallel_cnt), K(majors_cnt));
    } else {
      macro_start_seq = ((parallel_cnt + 1) * majors_cnt) * MACRO_STEP_SIZE;
    }
  }
  return ret;
}

int ObSSDataSplitHelper::generate_minor_macro_seq_info(
    const int64_t dest_tablet_index,
    const int64_t minor_index,
    const int64_t parallel_cnt,
    const int64_t parallel_idx,
    int64_t &macro_start_seq) const
{
  int ret = OB_SUCCESS;
  macro_start_seq = 0;
  int64_t op_id = 0;
  const int64_t delta = (parallel_cnt + 1/*tree placeholder*/) * minor_index * MACRO_STEP_SIZE /*start_seq of the ith minor*/
    + parallel_idx * MACRO_STEP_SIZE/*data blk's start_seq of the kth child task in the ith minor*/;
  const int64_t SPLIT_MAX_MINOR_MACRO_DATA_SEQ = 0x1L << SPLIT_MINOR_MACRO_DATA_SEQ_BITS;
  const int64_t SPLIT_MAX_OP_ID = 0x1L << (64 - SPLIT_MINOR_MACRO_DATA_SEQ_BITS);
  if (OB_UNLIKELY(dest_tablet_index < 0
      || minor_index < 0
      || parallel_cnt < 1
      || parallel_idx < 0
      || parallel_idx > parallel_cnt
      || delta >= SPLIT_MAX_MINOR_MACRO_DATA_SEQ)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index), K(minor_index),
      K(parallel_cnt), K(parallel_idx), K(delta));
  } else if (OB_FAIL(get_op_id(dest_tablet_index, op_id))) {
    LOG_WARN("get op id failed", K(ret), K(dest_tablet_index));
  } else if (OB_UNLIKELY(op_id >= SPLIT_MAX_OP_ID)) {
    ret = OB_SIZE_OVERFLOW;
    LOG_WARN("macro seq overflow", K(ret), K(op_id), K(SPLIT_MAX_OP_ID));
  } else {
    macro_start_seq = (op_id << SPLIT_MINOR_MACRO_DATA_SEQ_BITS) + delta;
  }
  return ret;
}

int ObSSDataSplitHelper::start_add_minor_op(
    const ObLSID &ls_id,
    const share::SCN &split_scn,
    const int64_t parallel_cnt_of_each_sstable,
    const ObTableStoreIterator &src_table_store_iterator,
    const ObIArray<ObTabletID> &dest_tablets_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObITable *, MAX_SSTABLE_CNT_IN_STORAGE> source_minors;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || !split_scn.is_valid()
      || parallel_cnt_of_each_sstable < 1
      || !src_table_store_iterator.is_valid()
      || dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(split_scn), K(parallel_cnt_of_each_sstable),
        K(src_table_store_iterator), K(dest_tablets_id));
  } else if (OB_FAIL(ObTabletSplitUtil::get_participants(
      ObSplitSSTableType::SPLIT_MINOR,
      src_table_store_iterator,
      false/*is_table_restore*/,
      ObArray<ObITable::TableKey>()/*skip_split_majors*/, source_minors))) {
    LOG_WARN("get participants failed", K(ret));
  } else if (OB_UNLIKELY(source_minors.empty())) {
    // do nothing.
    LOG_INFO("empty mini/minors in the source tablet", K(ret), K(dest_tablets_id));
  } else {
    if (OB_FAIL(start_add_op(ls_id,
                              split_scn,
                              ObAtomicFileType::SPLIT_MINOR_SSTABLE,
                              parallel_cnt_of_each_sstable,
                              source_minors.count()/*sstables_cnt_of_each_tablet*/,
                              dest_tablets_id))) {
      LOG_WARN("start add op failed", K(ret));
    }
  }
  return ret;
}

int ObSSDataSplitHelper::start_add_mds_op(
    const ObLSID &ls_id,
    const share::SCN &split_scn,
    const int64_t parallel_cnt_of_each_sstable,
    const int64_t sstables_cnt_of_each_tablet,
    const ObTabletID &dst_tablet_id)
{
  int ret = OB_SUCCESS;
  ObSEArray<ObTabletID, 1> dest_tablets_id;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || !split_scn.is_valid()
      || parallel_cnt_of_each_sstable < 1
      || sstables_cnt_of_each_tablet != 1
      || !dst_tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(split_scn), K(parallel_cnt_of_each_sstable),
        K(sstables_cnt_of_each_tablet), K(dst_tablet_id));
  } else if (OB_FAIL(dest_tablets_id.push_back(dst_tablet_id))) {
    LOG_WARN("push back failed", K(ret));
  } else if (OB_FAIL(start_add_op(ls_id,
                                  split_scn,
                                  ObAtomicFileType::SPLIT_MDS_MINOR_SSTABLE,
                                  parallel_cnt_of_each_sstable,
                                  sstables_cnt_of_each_tablet,
                                  dest_tablets_id))) {
    LOG_WARN("start add op failed", K(ret));
  }
  return ret;
}

int ObSSDataSplitHelper::finish_add_op(
    const int64_t dest_tablet_index,
    const bool need_finish)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(dest_tablet_index < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index));
  } else if (split_sstable_list_handle_.empty()) {
    if (need_finish) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("defensive check", K(ret), K(dest_tablet_index));
    } else {
      // do nothing when start failed.
    }
  } else if (OB_UNLIKELY(dest_tablet_index >= split_sstable_list_handle_.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(dest_tablet_index));
  } else if (need_finish) {
    if (OB_FAIL(split_sstable_list_handle_[dest_tablet_index]->get_atomic_file()->finish_op(*add_minor_op_handle_[dest_tablet_index]))) {
      LOG_WARN("abort failed", K(ret));
    }
  } else {
    if (OB_FAIL(split_sstable_list_handle_[dest_tablet_index]->get_atomic_file()->abort_op(*add_minor_op_handle_[dest_tablet_index]))) {
      LOG_WARN("abort failed", K(ret));
    }
  }
  FLOG_INFO("DEBUG CODE, CHANGE TO TRACE LATER, finish add op finished", K(ret), K(dest_tablet_index));
  return ret;
}

int ObSSDataSplitHelper::persist_majors_gc_rely_info(
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &dst_tablet_ids,
    const share::SCN &transfer_scn,
    const int64_t generated_majors_cnt,
    const int64_t max_majors_snapshot,
    const int64_t parallel_cnt_of_each_sstable)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || dst_tablet_ids.empty()
      || !transfer_scn.is_valid()
      || generated_majors_cnt < 1
      || max_majors_snapshot < 0
      || parallel_cnt_of_each_sstable <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_id), K(dst_tablet_ids), K(transfer_scn),
        K(generated_majors_cnt), K(max_majors_snapshot), K(parallel_cnt_of_each_sstable));
  } else {
    const int64_t gc_rely_parallel =
        parallel_cnt_of_each_sstable * generated_majors_cnt /*total steps of the parallel child tasks*/
      + ObGetMacroSeqStage::GET_NEW_ROOT_MACRO_SEQ * generated_majors_cnt; /*total steps of IndexTree*/
    for (int64_t i = 0; OB_SUCC(ret) && i < dst_tablet_ids.count(); i++) {
      const ObTabletID &tablet_id = dst_tablet_ids.at(i);
      if (OB_FAIL(share::SSCompactHelper::start_major_task(ls_id,
                                                           tablet_id,
                                                           transfer_scn,
                                                           max_majors_snapshot,
                                                           0, /*start seq*/
                                                           gc_rely_parallel,
                                                           1/*cg_cnt*/))) {
        LOG_WARN("write major gc info fail", K(ret), K(tablet_id));
      }
      // TODO YIREN, optimization, add local tablet cache or check firstly.
    }
  }
  return ret;
}

int ObSSDataSplitHelper::set_source_tablet_split_status(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &local_src_tablet_hdl,
    const ObSplitTabletInfoStatus &expected_status)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator arena_alloc("SplitUpdSS", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle ss_tablet_handle;
  ObTabletMeta new_ss_tablet_meta;
  if (OB_UNLIKELY(!ls_handle.is_valid()
      || !local_src_tablet_hdl.is_valid()
      || !is_valid_tablet_split_info_status(expected_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(local_src_tablet_hdl), K(expected_status));
  } else if (OB_FAIL(ObTabletSplitUtil::get_tablet(arena_alloc, ls_handle,
      local_src_tablet_hdl.get_obj()->get_tablet_id(),
      true/*is_shared_mode*/, ss_tablet_handle))) {
    LOG_WARN("get ss tablet failed", K(ret), "src_tablet_id", local_src_tablet_hdl.get_obj()->get_tablet_id());
  } else if (ObSplitTabletInfoStatus::CANT_EXEC_MINOR == expected_status
      && ss_tablet_handle.get_obj()->get_tablet_meta().split_info_.can_not_execute_ss_minor()) {
    // do nothing, already updated.
  } else if (ObSplitTabletInfoStatus::CANT_GC_MACROS == expected_status
      && ss_tablet_handle.get_obj()->get_tablet_meta().split_info_.can_not_gc_macro_blks()) {
    // do nothing, already updated.
  } else if (OB_FAIL(new_ss_tablet_meta.assign(ss_tablet_handle.get_obj()->get_tablet_meta()))) {
    LOG_WARN("assign failed", K(ret), "tablet_meta", ss_tablet_handle.get_obj()->get_tablet_meta());
  } else {
    if (ObSplitTabletInfoStatus::CANT_EXEC_MINOR == expected_status) {
      new_ss_tablet_meta.split_info_.set_can_not_execute_ss_minor(true);
    } else if (ObSplitTabletInfoStatus::CANT_GC_MACROS == expected_status) {
      new_ss_tablet_meta.split_info_.set_can_not_gc_data_blks(true);
    }
    if (FAILEDx(MTL(ObSSMetaService*)->update_tablet_meta(
        ls_handle.get_ls()->get_ls_id(),
        local_src_tablet_hdl.get_obj()->get_tablet_id(),
        local_src_tablet_hdl.get_obj()->get_reorganization_scn(),
        new_ss_tablet_meta))) {
      LOG_WARN("update tablet split info failed", K(ret),
        "ls_id", ls_handle.get_ls()->get_ls_id(),
        "src_tablet_id", local_src_tablet_hdl.get_obj()->get_tablet_id(),
        "transfer_scn", local_src_tablet_hdl.get_obj()->get_reorganization_scn());
    }
  }
  return ret;
}


int ObSSDataSplitHelper::check_at_sswriter_lease(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    int64_t &epoch,
    bool &is_data_split_executor)
{
  int ret = OB_SUCCESS;
  epoch = 0;
  is_data_split_executor = true;
  ObSSWriterKey key(ObSSWriterType::COMPACTION, ls_id, tablet_id);
  bool is_sswriter = false;
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(key));
  } else if (OB_FAIL(MTL(ObSSWriterService*)->check_lease(key, is_sswriter, epoch))) {
    LOG_WARN("check lease fail", K(ret));
  } else {
    is_data_split_executor = is_sswriter ? true : false;
  }
  return ret;
}

// check if satisfy the shared-storage split condition.
int ObSSDataSplitHelper::check_satisfy_ss_split_condition(
    const ObLSHandle &ls_handle,
    const ObTabletHandle &local_source_tablet_handle,
    const ObArray<ObTabletID> &dest_tablets_id,
    bool &is_data_split_executor)
{
  int ret = OB_SUCCESS;
  ObLSID ls_id;
  ObTabletID source_tablet_id;
  int64_t epoch = 0;
  is_data_split_executor = false;
  ObArenaAllocator tmp_arena("SplitSSTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  if (OB_UNLIKELY(!ls_handle.is_valid()
      || !local_source_tablet_handle.is_valid()
      || dest_tablets_id.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(ls_handle), K(local_source_tablet_handle), K(dest_tablets_id));
  } else if (OB_FALSE_IT(ls_id = ls_handle.get_ls()->get_ls_id())) {
  } else if (OB_FALSE_IT(source_tablet_id = local_source_tablet_handle.get_obj()->get_tablet_meta().tablet_id_)) {
  } else if (OB_FAIL(ObSSDataSplitHelper::check_at_sswriter_lease(
      ls_id, source_tablet_id, epoch, is_data_split_executor))) {
    LOG_WARN("check executor failed", K(ret));
  } else if (is_data_split_executor) {
    // data-split executor wait source tablet sstables shared.
    ObTabletHandle ss_tablet_handle;
    mds::MdsTableHandle mds_table;
    bool need_upload = false;
    ObLSIncSSTableUploader &upload_handler =
        ls_handle.get_ls()->get_inc_sstable_uploader();
    if (OB_FAIL(local_source_tablet_handle.get_obj()->get_mds_table_for_dump(mds_table))) {
      if (OB_EMPTY_RESULT != ret) {
        LOG_WARN("get mds table failed", K(ret), K(source_tablet_id));
      } else { // override ret_code is expected.
        ObSSLSMeta ss_ls_meta;
        const SCN &clog_checkpoint_scn = local_source_tablet_handle.get_obj()->get_clog_checkpoint_scn();
        const SCN &mds_checkpoint_scn = local_source_tablet_handle.get_obj()->get_mds_checkpoint_scn();
        const SCN &transfer_scn = local_source_tablet_handle.get_obj()->get_reorganization_scn();
        if (OB_FAIL(MTL(ObSSMetaService*)->get_ls_meta(ls_id, ss_ls_meta))) {
          LOG_WARN("get ss ls meta failed", K(ret), K(ls_id));
        } else if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(
            ls_id, source_tablet_id,
            transfer_scn,
            tmp_arena, ss_tablet_handle))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            need_upload = true;
            LOG_WARN("need retry to wait sstables upload", K(ret), K(source_tablet_id));
          } else {
            LOG_WARN("get tablet fail", K(ret), K(ls_id), K(source_tablet_id));
          }
        } else {
          const SCN &ls_ss_checkpoint_scn = ss_ls_meta.get_ss_checkpoint_scn();
          need_upload = clog_checkpoint_scn > SCN::max(ss_tablet_handle.get_obj()->get_clog_checkpoint_scn(), ls_ss_checkpoint_scn)
                     || mds_checkpoint_scn > SCN::max(ss_tablet_handle.get_obj()->get_mds_checkpoint_scn(), ls_ss_checkpoint_scn);
          FLOG_INFO("debug for ss-split", K(ret), K(ls_id), K(source_tablet_id), K(ls_ss_checkpoint_scn),
            "local_clog_ckpt", clog_checkpoint_scn,
            "local_mds_ckpt", mds_checkpoint_scn,
            "rem_clog_ckpt", ss_tablet_handle.get_obj()->get_clog_checkpoint_scn(),
            "rem_mds_ckpt", ss_tablet_handle.get_obj()->get_mds_checkpoint_scn());
        }
      }
    } else {
      ret = OB_NEED_RETRY;
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_INFO("CHANGE TO TRACE LATER, need retry to wait mds table dump", K(ret), K(source_tablet_id));
      }
    }

    if (need_upload) {
      ret = OB_SUCC(ret) ? OB_NEED_RETRY : ret; // to retry.
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(upload_handler.schedule_emergency_tablet_upload(source_tablet_id))) {
        LOG_WARN("schedule failed", K(ret), K(tmp_ret), K(source_tablet_id));
      }
    } else if (OB_FAIL(ret)) {
      // do nothing.
    } else if (OB_FAIL(ObSSDataSplitHelper::set_source_tablet_split_status(
        ls_handle, local_source_tablet_handle, ObSplitTabletInfoStatus::CANT_EXEC_MINOR))) {
      LOG_WARN("set can not exec minor failed", K(ret));
    }
  } else {
    // non-data-split executor wait dest tablets sstables shared.
    bool is_data_completed = false;
    if (OB_FAIL(ObSSDataSplitHelper::check_ss_data_completed(ls_handle, dest_tablets_id, is_data_completed))) {
      LOG_WARN("check ss split data completed failed", K(ret), K(dest_tablets_id));
    } else if (!is_data_completed) {
      ret = OB_NEED_RETRY;
      if (REACH_COUNT_INTERVAL(1000L)) {
        LOG_INFO("need retry to wait sstables shared", K(ret), K(dest_tablets_id));
      }
    }
  }
  return ret;
}

int ObSSDataSplitHelper::check_ss_data_completed(
    const ObLSHandle &ls_handle,
    const ObIArray<ObTabletID> &dest_tablet_ids,
    bool &is_completed)
{
  int ret = OB_SUCCESS;
  is_completed = true;
  ObArenaAllocator tmp_arena("SplitGetSSTa", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
  ObTabletHandle local_tablet_handle;
  ObTabletHandle ss_tablet_handle;
  if (OB_UNLIKELY(!ls_handle.is_valid() || dest_tablet_ids.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_handle), K(dest_tablet_ids));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && is_completed && i < dest_tablet_ids.count(); i++) {
      ss_tablet_handle.reset();
      tmp_arena.reset();
      const ObTabletID &tablet_id = dest_tablet_ids.at(i);
      if (OB_FAIL(ObTabletSplitUtil::get_tablet(tmp_arena, ls_handle,
          tablet_id, true/*is_shared_mode*/, ss_tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          is_completed = false;
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("get local tablet failed", K(ret), K(tablet_id));
        }
      } else if (OB_UNLIKELY(!ss_tablet_handle.is_valid())) {
        ret = OB_ERR_UNDEFINED;
        LOG_WARN("invalid ss tablet", K(ret), K(tablet_id), K(ss_tablet_handle));
      } else if (ss_tablet_handle.get_obj()->get_tablet_meta().split_info_.is_data_incomplete()) {
        is_completed = false;
      }
    }
  }
  return ret;
}

int ObSSDataSplitHelper::create_shared_tablet_if_not_exist(
    const ObLSID &ls_id,
    const ObIArray<ObTabletID> &tablet_ids,
    const share::SCN &transfer_scn)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ls_id.is_valid() || tablet_ids.empty() || !transfer_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_ids), K(transfer_scn));
  } else {
    ObArenaAllocator tmp_arena("SplitCreateTab", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
    ObTabletHandle ss_tablet_handle;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); i++) {
      ss_tablet_handle.reset();
      tmp_arena.reuse();
      const ObTabletID &dst_tablet_id = tablet_ids.at(i);
      if (OB_FAIL(MTL(ObSSMetaService*)->get_tablet(
            ls_id,
            dst_tablet_id,
            transfer_scn,
            tmp_arena,
            ss_tablet_handle))) {
        if (OB_TABLET_NOT_EXIST == ret) {
          LOG_INFO("tablet meta not exist now, create shared tablet", K(ret), K(ls_id), K(dst_tablet_id));
          if (OB_FAIL(MTL(ObSSMetaService*)->create_tablet(
              ls_id,
              dst_tablet_id,
              transfer_scn))) {
            LOG_WARN("create tablet fail", K(ret), K(ls_id), K(dst_tablet_id));
          }
        } else {
          LOG_WARN("get shared tablet fail", K(ret));
        }
      } else { /* do nothing. */}
    }
  }
  return ret;
}
#endif
