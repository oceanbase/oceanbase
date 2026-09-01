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

#include "storage/tablet/ob_sstable_truncate_filter.h"

#include "share/ob_define.h"
#include "storage/ob_i_table.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/ob_storage_struct.h"
#include "storage/high_availability/ob_tablet_ha_status.h"

namespace oceanbase
{
namespace storage
{
/*static*/ObSSTableTruncateFilter ObSSTableTruncateFilter::dummy_filter()
{
  ObSSTableTruncateFilter filter;
  filter.is_inited_ = true;
  filter.truncate_commit_scn_.set_min();
  filter.truncate_commit_version_ = 0;
  return filter;
}

ObSSTableTruncateFilter::ObSSTableTruncateFilter()
  : truncate_commit_scn_(),
    truncate_commit_version_(common::OB_INVALID_VERSION),
    is_inited_(false)
{
}

int ObSSTableTruncateFilter::init(const share::SCN &truncate_commit_scn,
                                  const int64_t truncate_commit_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), KPC(this));
  } else if (OB_UNLIKELY(!truncate_commit_scn.is_valid()
                         || OB_INVALID_VERSION == truncate_commit_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(truncate_commit_scn), K(truncate_commit_version));
  } else {
    truncate_commit_scn_ = truncate_commit_scn;
    truncate_commit_version_ = truncate_commit_version;
    is_inited_ = true;
  }
  return ret;
}

void ObSSTableTruncateFilter::reset()
{
  truncate_commit_scn_.reset();
  truncate_commit_version_ = common::OB_INVALID_VERSION;
  is_inited_ = false;
}

int ObSSTableTruncateFilter::check_if_need_merge(const ObUpdateTableStoreParam &param) const
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("filter not inited", K(ret));
  } else if (!need_filter_() || OB_ISNULL(param.sstable_)) {
    // no truncate data or no sstable to check, merge is needed
  } else {
    bool should_keep = true;
    if (OB_FAIL(check_sstable_(*param.sstable_, should_keep))) {
      LOG_WARN("failed to check sstable", K(ret), KPC(param.sstable_));
    } else if (!should_keep) {
      ret = OB_NO_NEED_MERGE;
      LOG_INFO("merge result sstable would be filtered by truncate, skip merge",
          KPC(param.sstable_), KPC(this));
    }
  }
  return ret;
}

int ObSSTableTruncateFilter::rebuild_param_for_transfer_replace(
  ObArenaAllocator &allocator,
  const ObTablet &old_tablet,
  const ObBatchUpdateTableStoreParam &param,
  ObBatchUpdateTableStoreParam &new_param) const
{
  int ret = OB_SUCCESS;
  const bool is_transfer_replace = param.is_transfer_replace_;
  new_param.reset();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("filter not inited", K(ret));
  } else if (OB_UNLIKELY(!old_tablet.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(old_tablet), K(param));
  } else if (OB_FAIL(new_param.assign(param))) {
    LOG_WARN("failed to assign param", K(ret));
  } else if (!is_transfer_replace) {
    // only filter sstables for transfer replace
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("unexpected call", K(ret), K(param), K(common::lbt()));
  } else if (!need_filter_()) {
    // do nothing
  } else {
    // reconstruct tables array
    new_param.tables_handle_.reset();
    bool has_major = false;
    const bool is_restore_status_full = ObTabletRestoreStatus::is_full(param.restore_status_);

    for (int64_t i = 0; OB_SUCC(ret) && i < param.tables_handle_.get_count(); ++i) {
      ObITable *table = param.tables_handle_.get_table(i);
      bool should_keep = true;
      if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("null table in param", K(ret), K(i));
      } else if (OB_FAIL(check_sstable_(*table, should_keep))) {
        LOG_WARN("failed to check sstable", K(ret), KPC(table));
      } else if (!should_keep) {
        LOG_INFO("truncate filter dropped sstable from ha replace param",
            "table_key", table->get_key(), KPC(this));
      } else {
        ObTableHandleV2 table_handle;
        if (OB_FAIL(param.tables_handle_.get_table(i, table_handle))) {
          LOG_WARN("failed to get table handle", K(ret), K(i));
        } else if (OB_FAIL(new_param.tables_handle_.add_table(table_handle))) {
          LOG_WARN("failed to add table to new param", K(ret));
        } else if (table->is_major_sstable()) {
          has_major = true;
        }
      }
    }
    /// NOTE: In case of restore, if restore status is FULL, major sstable must be exist
    /// after replace(defensive code at @interface ObTablet::handle_transfer_replace_).
    const int64_t snapshot_version = OB_ISNULL(param.tablet_meta_) ?
        old_tablet.get_snapshot_version() : MAX(old_tablet.get_snapshot_version(), param.tablet_meta_->snapshot_version_);
    if (OB_FAIL(ret)) {
    } else if (!is_restore_status_full || has_major) {
      // do nothing
    } else if (OB_UNLIKELY(OB_INVALID_VERSION == snapshot_version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected invalid snapshot version", K(ret), K(snapshot_version));
    } else if (OB_FAIL(create_empty_major_for_new_param_(
                allocator,
                old_tablet,
                snapshot_version,
                new_param))) {
      LOG_WARN("failed to create empty major for new param", K(ret), K(param),
        K(snapshot_version));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(!new_param.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected invalid new param", K(ret), KPC(this), K(param), K(new_param));
  } else {
    LOG_INFO("succeed to rebuild batch update table store param", K(ret), KPC(this),
      K(param), K(new_param));
  }
  return ret;
}

bool ObSSTableTruncateFilter::need_filter_() const
{
  return is_inited_
         && (truncate_commit_scn_.is_valid_and_not_min()
         || truncate_commit_version_ > 0);
}

int ObSSTableTruncateFilter::check_sstable_(const ObITable &table, bool &should_keep) const
{
  int ret = OB_SUCCESS;
  should_keep = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("filter not inited", K(ret));
  } else if (!need_filter_()) {
    // noop
  } else if (table.is_mds_sstable()) {
    // mds sstable carries truncate info, never filtered
  } else if (table.is_major_sstable()) {
    if (table.get_snapshot_version() < truncate_commit_version_) {
      should_keep = false;
    }
  } else if (table.is_minor_sstable() || table.is_ddl_sstable()) {
    const share::SCN start_scn = table.get_start_scn();
    const share::SCN end_scn = table.get_end_scn();
    if (end_scn <= truncate_commit_scn_) {
      should_keep = false;
    } else {
      // keep
    }
  }
  return ret;
}

int ObSSTableTruncateFilter::create_empty_major_for_new_param_(
    ObArenaAllocator &allocator,
    const ObTablet &tablet,
    const int64_t snapshot_version,
    ObBatchUpdateTableStoreParam &new_param) const
{
  int ret = OB_SUCCESS;
  ObStorageSchema *storage_schema = nullptr;
  ObTableHandleV2 empty_major_hdl;

  if (OB_FAIL(tablet.load_storage_schema(allocator, storage_schema))) {
    LOG_WARN("failed to load tablet storage schema", K(ret), K(tablet));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_empty_sstable(
                        allocator,
                        *storage_schema,
                        tablet.get_tablet_id(),
                        snapshot_version,
                        empty_major_hdl))) {
    LOG_WARN("failed to create empty major", K(ret), KPC(storage_schema),
      K(tablet.get_tablet_id()), K(snapshot_version));
  } else if (OB_FAIL(new_param.tables_handle_.add_table(empty_major_hdl))) {
    LOG_WARN("failed to add table", K(ret), K(new_param), K(empty_major_hdl));
  } else {
    LOG_INFO("succeed to create empty major for new param", K(ret), K(tablet.get_tablet_id()),
      K(snapshot_version), K(empty_major_hdl));
  }
  ObTabletObjLoadHelper::free(allocator, storage_schema);
  return ret;
}

} // namespace storage
} // namespace oceanbase
