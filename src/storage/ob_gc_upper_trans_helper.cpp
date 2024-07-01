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
#define USING_LOG_PREFIX STORAGE

namespace oceanbase
{
namespace storage
{

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
    int64_t max_trans_version = INT64_MAX;
    SCN tmp_scn = SCN::max_scn();
    if (OB_FAIL(ls.get_upper_trans_version_before_given_scn(sstable.get_end_scn(), tmp_scn))) {
      LOG_WARN("failed to get upper trans version before given log ts", K(ret), K(sstable));
    } else if (FALSE_IT(max_trans_version = tmp_scn.get_val_for_tx())) {
    } else if (0 == max_trans_version) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("max trans version should not be 0", K(sstable));
    } else if (INT64_MAX != max_trans_version) {
      new_upper_trans_version = max_trans_version;
      FLOG_INFO("success to get new upper trans version", K(ret), K(ls_id), K(tablet_id), K(max_trans_version), K(sstable));
    } else {
      LOG_TRACE("can not get upper trans version", K(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObGCUpperTransHelper::check_need_gc_or_update_upper_trans_version(
    ObLS &ls,
    const ObTablet &tablet,
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
  if (OB_UNLIKELY(new_upper_trans == nullptr || !new_upper_trans->empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("new_upper_trans is nullpr or empty", K(ret), K(ls_id), K(tablet_id), K(new_upper_trans));
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
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet.get_kept_snapshot_info(ls.get_min_reserved_snapshot(), snapshot_info))) {
    LOG_WARN("failed to get multi version start", K(ret), K(tablet_id));
  } else if (FALSE_IT(multi_version_start = snapshot_info.snapshot_)) {
  } else if (need_update) {
    // need to update table store so skip checking gc status
  } else if (OB_FAIL(table_store_wrapper.get_member()->need_remove_old_table(multi_version_start, need_update))) {
    LOG_WARN("failed to check need rebuild table store", K(ret), K(multi_version_start));
  } else {
    upper_trans_param.reset(); // no need to update upper trans version
  }

  return ret;
}

} // namespace storage
} // namespace oceanbase