/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "storage/ob_gc_upper_trans_helper.h"
#include "storage/compaction/ob_partition_merge_policy.h"
#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

#define CALC_UPPER_WARN_LOG(LOG_LEVEL)                          \
  LOG_##LOG_LEVEL("find a sstable without upper_trans_version", \
                  K(ls_id),                                     \
                  K(tablet_id),                                 \
                  K(end_scn_ts),                                \
                  KTIME(end_scn_ts),                            \
                  K(current_ts),                                \
                  KTIME(current_ts));

int ObGCUpperTransHelper::try_get_sstable_upper_trans_version(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    int64_t &new_upper_trans_version)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_id();
  const ObTabletID &tablet_id = sstable.get_key().get_tablet_id();
  new_upper_trans_version = INT64_MAX;

  if (INT64_MAX == sstable.get_upper_trans_version()) {
    int tmp_ret = OB_SUCCESS;
    // try get new upper trans version from uncommit tx info firstly, if failed, continue to the following steps
    if (OB_TMP_FAIL(check_uncommit_tx_info_state(ls, sstable, new_upper_trans_version))) {
      LOG_WARN("failed to get new upper trans version from uncommit tx info", K(tmp_ret), K(sstable));
    }

    int64_t max_trans_version = INT64_MAX;
    SCN tmp_scn = SCN::max_scn();
    SCN end_scn = sstable.get_end_scn();
    if (new_upper_trans_version != INT64_MAX) {
      // succ to get new upper trans version from uncommit tx info
    } else if (OB_FAIL(ls.get_upper_trans_version_before_given_scn(end_scn, tmp_scn))) {
      LOG_WARN("failed to get upper trans version before given log ts", K(ret), K(ls_id), K(sstable));
    } else if (FALSE_IT(max_trans_version = tmp_scn.get_val_for_tx())) {
    } else if (0 == max_trans_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max trans version should not be 0", K(ret), K(sstable));
    } else if (INT64_MAX != max_trans_version) {
      new_upper_trans_version = MAX(max_trans_version, sstable.get_max_merged_trans_version());
      FLOG_INFO("success to get new upper trans version", K(ret), K(ls_id), K(tablet_id), K(max_trans_version), K(sstable));
    } else {
      LOG_TRACE("can not get upper trans version", K(ret), K(ls_id), K(tablet_id), K(sstable));
    }

    if (INT64_MAX == new_upper_trans_version) {
      const int64_t WARN_LOG_THREASHOLD = 24LL * 60LL * 60LL  * 1000LL * 1000LL; // 24 hours
      const int64_t current_ts = ObClockGenerator::getClock();
      const int64_t end_scn_ts = end_scn.convert_to_ts();
      if (current_ts - end_scn_ts > 2LL * WARN_LOG_THREASHOLD) {
        CALC_UPPER_WARN_LOG(ERROR);
        (void)ls.get_upper_trans_version_before_given_scn(end_scn, tmp_scn, true/* force_print_log */);
      } else if (current_ts - end_scn_ts > WARN_LOG_THREASHOLD) {
        CALC_UPPER_WARN_LOG(WARN);
      }
    }
  }
  return ret;
}
#undef CALC_UPPER_WARN_LOG

int ObGCUpperTransHelper::check_need_gc_or_update_upper_trans_version(
    ObLS &ls,
    ObTablet &tablet,
    int64_t &multi_version_start,
    UpdateUpperTransParam &upper_trans_param,
    bool &need_update)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls.get_ls_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  bool is_paused = false; // TODO(DanLing) get is_paused
  need_update = false;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObStorageSnapshotInfo snapshot_info;
  ObIArray<int64_t> *new_upper_trans = upper_trans_param.new_upper_trans_;
  ObIArray<int64_t> *gc_inc_major_ddl_scns = upper_trans_param.gc_inc_major_ddl_scns_;

  if (OB_UNLIKELY(new_upper_trans == nullptr || !new_upper_trans->empty() || gc_inc_major_ddl_scns == nullptr || !gc_inc_major_ddl_scns->empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_upper_trans is nullpr or empty", K(ret), K(ls_id), K(tablet_id), K(new_upper_trans), K(gc_inc_major_ddl_scns));
  } else if (is_paused) {
    ret = OB_EAGAIN;
    LOG_INFO("paused, cannot update trans version now", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (tablet.get_tablet_meta().ha_status_.is_data_status_complete()) {
    ObITable *table = nullptr;
    ObSSTable *sstable = nullptr;
    int64_t new_upper_trans_version = INT64_MAX;
    ObTableStoreIterator iter(false/*is_reverse*/, true/*need_load_sstable*/);
    if (OB_FAIL(table_store_wrapper.get_member()->get_mini_minor_sstables(iter))) {
      LOG_WARN("fail to get mini minor sstable", K(ret), K(table_store_wrapper));
    }
    while (OB_SUCC(ret) && OB_SUCC(iter.get_next(table))) {
      if (OB_ISNULL(table) || OB_UNLIKELY(!table->is_sstable())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, table is nullptr", K(ret), KPC(table));
      } else if (FALSE_IT(sstable = reinterpret_cast<ObSSTable *>(table))) {
      } else if (FALSE_IT(upper_trans_param.last_minor_end_scn_ = sstable->get_end_scn())) {
      } else if (FALSE_IT(new_upper_trans_version = sstable->get_upper_trans_version())) {
      } else if (INT64_MAX != new_upper_trans_version) {
        if (OB_FAIL(new_upper_trans->push_back(new_upper_trans_version))) {
          LOG_WARN("failed to push back new upper_trans_version", K(ret), K(new_upper_trans_version), KPC(sstable));
        }
      } else if (OB_FAIL(try_get_sstable_upper_trans_version(ls, *sstable, new_upper_trans_version))) {
        LOG_WARN("failed to update upper trans version", K(ret), KPC(sstable));
      } else {
        need_update = need_update || (INT64_MAX != new_upper_trans_version);
        if (OB_FAIL(new_upper_trans->push_back(new_upper_trans_version))) {
          LOG_WARN("failed to push back new upper_trans_version", K(ret), K(new_upper_trans_version), KPC(sstable));
        }
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    // only check inc major ddl dump sstable, cannot gc ddl memtable here!
    if (OB_SUCC(ret)) {
      int tmp_ret = OB_SUCCESS;
      const ObSSTableArray &inc_major_ddl_dump_sstable = table_store_wrapper.get_member()->get_inc_major_ddl_sstables();
      for (int64_t idx = 0; (OB_SUCCESS == tmp_ret) && idx < inc_major_ddl_dump_sstable.count(); ++idx) {
        ObSSTable *cur_table = inc_major_ddl_dump_sstable[idx];
        int64_t trans_state = ObTxData::UNKOWN;
        int64_t commit_version = cur_table->get_upper_trans_version();
        bool need_gc = false;

        if (OB_LIKELY(INT64_MAX == commit_version)) {
          if (OB_TMP_FAIL(compaction::ObIncMajorTxHelper::get_inc_major_commit_version(ls, *cur_table, SCN::max_scn(), trans_state, commit_version))) {
            LOG_WARN("failed to get commit version for inc major", K(tmp_ret), KPC(cur_table));
          } else if (ObTxData::ABORT != trans_state) {
            // do nothing
          } else if (OB_TMP_FAIL(compaction::ObIncMajorTxHelper::check_need_gc_ddl_dump(tablet, *cur_table, need_gc))) {
            LOG_WARN("fail to check ddlkv exist", K(tmp_ret), K(tablet_id));
          } else if (!need_gc) {
            // do nothing
          } else if (OB_TMP_FAIL(gc_inc_major_ddl_scns->push_back(cur_table->get_end_scn().get_val_for_tx()))) {
            LOG_WARN("failed to add ddl end scn", K(tmp_ret));
          } else {
            need_update = true;
          }
        } else {
          // convert to warn log later
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("get unexpected commit version from inc ddl dump sstable", K(tmp_ret), KPC(cur_table));
        }
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (need_update) {
    // need recycle minor or inc major, no need to check major table
  } else if (FALSE_IT(upper_trans_param.reset())) {
  } else if (OB_FAIL(tablet.get_kept_snapshot_info(ls.get_min_reserved_snapshot(), snapshot_info))) {
    LOG_WARN("failed to get multi version start", K(ret), K(tablet_id));
  } else if (FALSE_IT(multi_version_start = snapshot_info.snapshot_)) {
  } else if (OB_FAIL(table_store_wrapper.get_member()->need_remove_old_table(multi_version_start, need_update))) {
    LOG_WARN("failed to check need rebuild table store", K(ret), K(multi_version_start));
  }
  return ret;
}


/*
  This function has two effects:
  1. compute upper trans version, use to speed up sstable gc process;
  2. TODO(xiekaige.xkg) check sstable tx status, use to decide if reuse macro block.
*/
int ObGCUpperTransHelper::check_uncommit_tx_info_state(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    int64_t &new_upper_trans_version)
{
  int ret = OB_SUCCESS;
  if (sstable.get_key().get_tablet_id().is_inner_tablet() || !sstable.is_minor_sstable()) { //skip
  } else {
    blocksstable::ObSSTableMetaHandle meta_handle;
    if (OB_FAIL(sstable.get_meta(meta_handle))) {
      LOG_WARN("get uncommit tx info failed", K(ret), K(meta_handle));
    } else {
      const compaction::ObMetaUncommitTxInfo &uncommit_tx_info = meta_handle.get_sstable_meta().get_uncommit_tx_info();
      if (OB_UNLIKELY(!uncommit_tx_info.is_valid())) {
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("uncommit tx info is invalid", K(ret), K(uncommit_tx_info));
      } else if (uncommit_tx_info.is_empty()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("sstable upper tran version == max, uncommit tx info should not be empty", K(ret), K(uncommit_tx_info), K(sstable));
      } else if (uncommit_tx_info.is_info_overflow()) { //skip;
      } else {
        ObTxTableGuard tx_table_guard;
        if (OB_FAIL(ls.get_tx_table_guard(tx_table_guard))) {
          LOG_WARN("get_tx_table_guard from log stream fail.", K(ret), K(ls), K(tx_table_guard));
        } else {
          // no need to worry about transfer, cause we only focus on sstable level, sstable which is in progress of transfer cannot be accessed.
          SCN scn_commit_trans_version = SCN::max_scn();
          int64_t state = -1;
          new_upper_trans_version = sstable.get_max_merged_trans_version();
          for (int64_t i = 0; OB_SUCC(ret) && i < uncommit_tx_info.uncommit_tx_desc_count_; ++i) {
            // step 1 : read trans state from sstable meta
            if (uncommit_tx_info.tx_infos_[i].is_commit_version() ) {
              new_upper_trans_version = max(new_upper_trans_version, uncommit_tx_info.tx_infos_[i].commit_version_);
            // step 2 : read trans state from tx table
            } else if (OB_FAIL(tx_table_guard.get_tx_state_with_scn(uncommit_tx_info.tx_infos_[i].tx_id_, SCN::max_scn(), state, scn_commit_trans_version))) {
              LOG_WARN("read tx state failed", K(ret), K(tx_table_guard), K(state), K(uncommit_tx_info.tx_infos_[i]));
            } else if (state == ObTxData::RUNNING) {
              // not all tx commit, can not set upper trans version
              new_upper_trans_version = INT64_MAX;
              break;
            } else if (state == ObTxData::COMMIT) {
              new_upper_trans_version = max(new_upper_trans_version, scn_commit_trans_version.get_val_for_tx());
            } else if (state == ObTxData::ABORT) { // when tx abort, scn_commit_trans_version = SCN::min_scn();
            } else {
              // state like ELR_COMMIT are not support
              ret = OB_NOT_SUPPORTED;
              LOG_WARN("state not supported", K(ret), K(tx_table_guard), K(state), K(uncommit_tx_info.tx_infos_[i]));
            }
          }
        }
      }
    }

    if (OB_FAIL(ret)) {
      new_upper_trans_version = INT64_MAX;
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
