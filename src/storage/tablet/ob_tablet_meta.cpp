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

#include "storage/tablet/ob_tablet_meta.h"

#include "lib/ob_define.h"
#include "lib/utility/serialization.h"
#include "share/scn.h"
#include "share/schema/ob_table_schema.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/tablet/ob_tablet_binding_info.h"
#include "storage/tablet/ob_tablet_create_delete_mds_user_data.h"

namespace oceanbase
{
using namespace share;
using namespace blocksstable;
using namespace palf;
namespace storage
{
const SCN ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN = SCN::base_scn();
const SCN ObTabletMeta::INVALID_CREATE_SCN = SCN::min_scn();
// multi source transaction leader has no log scn when first time register
// create tablet buffer, so the init create scn for tablet could be -1.
const SCN ObTabletMeta::INIT_CREATE_SCN = SCN::invalid_scn();

ObTabletMeta::ObTabletMeta()
  : version_(TABLET_META_VERSION),
    length_(0),
    ls_id_(),
    tablet_id_(),
    data_tablet_id_(),
    ref_tablet_id_(),
    create_scn_(ObTabletMeta::INVALID_CREATE_SCN),
    start_scn_(),
    clog_checkpoint_scn_(),
    ddl_checkpoint_scn_(SCN::min_scn()),
    snapshot_version_(OB_INVALID_TIMESTAMP),
    multi_version_start_(OB_INVALID_TIMESTAMP),
    ha_status_(),
    report_status_(),
    table_store_flag_(),
    ddl_start_scn_(SCN::min_scn()),
    ddl_snapshot_version_(OB_INVALID_TIMESTAMP),
    max_sync_storage_schema_version_(0),
    ddl_execution_id_(-1),
    ddl_data_format_version_(0),
    max_serialized_medium_scn_(0),
    ddl_commit_scn_(SCN::min_scn()),
    mds_checkpoint_scn_(),
    transfer_info_(),
    extra_medium_info_(),
    last_persisted_committed_tablet_status_(),
    space_usage_(),
    create_schema_version_(0),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    has_next_tablet_(false),
    is_inited_(false)
{
}

ObTabletMeta::~ObTabletMeta()
{
  reset();
}

int ObTabletMeta::init(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &data_tablet_id,
    const share::SCN create_scn,
    const int64_t snapshot_version,
    const lib::Worker::CompatMode compat_mode,
    const ObTabletTableStoreFlag &table_store_flag,
    const int64_t create_schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!data_tablet_id.is_valid())
      //|| OB_UNLIKELY(create_scn <= OB_INVALID_TIMESTAMP)
      || OB_UNLIKELY(OB_INVALID_VERSION == snapshot_version)
      || OB_UNLIKELY(lib::Worker::CompatMode::INVALID == compat_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
        K(create_scn), K(snapshot_version), K(compat_mode));
  } else if (OB_FAIL(ha_status_.init_status())) {
    LOG_WARN("failed to init ha status", K(ret));
  } else if (OB_FAIL(transfer_info_.init())) {
    LOG_WARN("failed to init transfer info", K(ret));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    data_tablet_id_ = data_tablet_id;
    create_scn_ = create_scn;
    create_schema_version_ = create_schema_version;
    start_scn_ = INIT_CLOG_CHECKPOINT_SCN;
    clog_checkpoint_scn_ = INIT_CLOG_CHECKPOINT_SCN;
    ddl_checkpoint_scn_ = INIT_CLOG_CHECKPOINT_SCN;
    compat_mode_ = compat_mode;
    snapshot_version_ = snapshot_version;
    multi_version_start_ = snapshot_version;
    table_store_flag_ = table_store_flag;
    ddl_start_scn_.set_min();
    ddl_commit_scn_.set_min();
    ddl_snapshot_version_ = 0;
    max_sync_storage_schema_version_ = create_schema_version;
    ddl_execution_id_ = -1;
    ddl_data_format_version_ = 0;
    mds_checkpoint_scn_ = INIT_CLOG_CHECKPOINT_SCN;

    report_status_.merge_snapshot_version_ = snapshot_version;
    report_status_.cur_report_version_ = snapshot_version;
    report_status_.data_checksum_ = 0;
    report_status_.row_count_ = 0;
    if (tablet_id_.is_ls_inner_tablet()) {
      last_persisted_committed_tablet_status_.tablet_status_ = ObTabletStatus::NORMAL;
      last_persisted_committed_tablet_status_.data_type_ = ObTabletMdsUserDataType::CREATE_TABLET;
    } else {
      last_persisted_committed_tablet_status_.reset();
    }
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::init(
    const ObTabletMeta &old_tablet_meta,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const int64_t max_sync_storage_schema_version,
    const SCN clog_checkpoint_scn,
    const ObDDLTableStoreParam &ddl_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!old_tablet_meta.is_valid())
      || OB_UNLIKELY(OB_INVALID_VERSION == max_sync_storage_schema_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(old_tablet_meta), K(max_sync_storage_schema_version));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = old_tablet_meta.ls_id_;
    tablet_id_ = old_tablet_meta.tablet_id_;
    data_tablet_id_ = old_tablet_meta.data_tablet_id_;
    ref_tablet_id_ = old_tablet_meta.ref_tablet_id_;
    create_scn_ = old_tablet_meta.create_scn_;
    create_schema_version_ = old_tablet_meta.create_schema_version_;
    start_scn_ = old_tablet_meta.start_scn_;
    ddl_start_scn_ = SCN::max(ddl_info.ddl_start_scn_, old_tablet_meta.ddl_start_scn_);
    ddl_commit_scn_ = SCN::max(ddl_info.ddl_commit_scn_, old_tablet_meta.ddl_commit_scn_);
    clog_checkpoint_scn_ = SCN::max(clog_checkpoint_scn, old_tablet_meta.clog_checkpoint_scn_);
    compat_mode_ = old_tablet_meta.compat_mode_;
    ha_status_ = old_tablet_meta.ha_status_;
    report_status_ = old_tablet_meta.report_status_;

    snapshot_version_ = MAX(snapshot_version, old_tablet_meta.snapshot_version_);
    multi_version_start_ = MIN(MAX(multi_version_start, old_tablet_meta.multi_version_start_), snapshot_version_);
    table_store_flag_ = old_tablet_meta.table_store_flag_;
    max_sync_storage_schema_version_ = max_sync_storage_schema_version;
    max_serialized_medium_scn_ = 0; // abandoned
    ddl_checkpoint_scn_ = SCN::max(old_tablet_meta.ddl_checkpoint_scn_, ddl_info.ddl_checkpoint_scn_);
    ddl_snapshot_version_ = MAX(old_tablet_meta.ddl_snapshot_version_, ddl_info.ddl_snapshot_version_);
    ddl_execution_id_ = MAX(old_tablet_meta.ddl_execution_id_, ddl_info.ddl_execution_id_);
    ddl_data_format_version_ = MAX(old_tablet_meta.ddl_data_format_version_, ddl_info.data_format_version_);
    mds_checkpoint_scn_ = old_tablet_meta.mds_checkpoint_scn_;
    transfer_info_ = old_tablet_meta.transfer_info_;
    extra_medium_info_ = old_tablet_meta.extra_medium_info_;
    space_usage_ = old_tablet_meta.space_usage_;
    if (OB_FAIL(last_persisted_committed_tablet_status_.assign(old_tablet_meta.last_persisted_committed_tablet_status_))) {
      LOG_WARN("fail to init last_persisted_committed_tablet_status from old tablet meta", K(ret),
          "last_persisted_committed_tablet_status", old_tablet_meta.last_persisted_committed_tablet_status_);
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::init(
    const ObTabletMeta &old_tablet_meta,
    const share::SCN &flush_scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!old_tablet_meta.is_valid())
      || OB_UNLIKELY(!flush_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(old_tablet_meta), K(flush_scn));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = old_tablet_meta.ls_id_;
    tablet_id_ = old_tablet_meta.tablet_id_;
    data_tablet_id_ = old_tablet_meta.data_tablet_id_;
    ref_tablet_id_ = old_tablet_meta.ref_tablet_id_;
    create_scn_ = old_tablet_meta.create_scn_;
    create_schema_version_ = old_tablet_meta.create_schema_version_;
    start_scn_ = old_tablet_meta.start_scn_;
    clog_checkpoint_scn_ = old_tablet_meta.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = old_tablet_meta.ddl_checkpoint_scn_;
    snapshot_version_ = old_tablet_meta.snapshot_version_;
    multi_version_start_ = old_tablet_meta.multi_version_start_;
    compat_mode_ = old_tablet_meta.compat_mode_;
    ha_status_ = old_tablet_meta.ha_status_;
    report_status_ = old_tablet_meta.report_status_;
    table_store_flag_ = old_tablet_meta.table_store_flag_;
    ddl_start_scn_ = old_tablet_meta.ddl_start_scn_;
    ddl_commit_scn_ = old_tablet_meta.ddl_commit_scn_;
    ddl_execution_id_ = old_tablet_meta.ddl_execution_id_;
    ddl_data_format_version_ = old_tablet_meta.ddl_data_format_version_;
    ddl_snapshot_version_ = old_tablet_meta.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = old_tablet_meta.max_sync_storage_schema_version_;
    max_serialized_medium_scn_ = old_tablet_meta.max_serialized_medium_scn_;
    mds_checkpoint_scn_ = flush_scn;
    transfer_info_ = old_tablet_meta.transfer_info_;
    extra_medium_info_ = old_tablet_meta.extra_medium_info_;
    space_usage_ = old_tablet_meta.space_usage_;
    if (OB_FAIL(last_persisted_committed_tablet_status_.assign(old_tablet_meta.last_persisted_committed_tablet_status_))) {
      LOG_WARN("fail to init last_persisted_committed_tablet_status from old tablet meta", K(ret),
          "last_persisted_committed_tablet_status", old_tablet_meta.last_persisted_committed_tablet_status_);
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }

  return ret;
}

int ObTabletMeta::init(
    const ObMigrationTabletParam &param,
    const bool is_transfer)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    data_tablet_id_ = param.data_tablet_id_;
    ref_tablet_id_ = param.ref_tablet_id_;
    create_scn_ = param.create_scn_;
    create_schema_version_ = param.create_schema_version_;
    start_scn_ = param.start_scn_;
    clog_checkpoint_scn_ = param.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = param.ddl_checkpoint_scn_;
    snapshot_version_ = param.snapshot_version_;
    multi_version_start_ = param.multi_version_start_;
    compat_mode_ = param.compat_mode_;
    report_status_.reset();
    table_store_flag_ = param.table_store_flag_;
    ddl_start_scn_ = param.ddl_start_scn_;
    ddl_commit_scn_ = param.ddl_commit_scn_;
    ddl_snapshot_version_ = param.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = param.max_sync_storage_schema_version_;
    ha_status_ = param.ha_status_;
    max_serialized_medium_scn_ = param.max_serialized_medium_scn_;
    ddl_execution_id_ = param.ddl_execution_id_;
    ddl_data_format_version_ = param.ddl_data_format_version_;
    mds_checkpoint_scn_ = param.mds_checkpoint_scn_;
    transfer_info_ = param.transfer_info_;
    extra_medium_info_ = param.extra_medium_info_;
    if (param.version_ < ObMigrationTabletParam::PARAM_VERSION_V3) {
      int64_t tmp_pos = 0;
      const ObString &user_data = param.mds_data_.tablet_status_committed_kv_.v_.user_data_;
      if (user_data.empty()) {
        last_persisted_committed_tablet_status_.reset();
      } else if (OB_FAIL(last_persisted_committed_tablet_status_.deserialize(user_data.ptr(), user_data.length(), tmp_pos))) {
        LOG_WARN("failed to deserialize user data", K(ret), K(tmp_pos), K(user_data));
      }
    } else if (is_transfer) {
      last_persisted_committed_tablet_status_.reset();
    } else if (OB_FAIL(last_persisted_committed_tablet_status_.assign(param.last_persisted_committed_tablet_status_))) {
      LOG_WARN("fail to init last_persisted_committed_tablet_status from mig param", K(ret),
          "last_persisted_committed_tablet_status",
          param.last_persisted_committed_tablet_status_);
    }
    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::assign(const ObTabletMeta &other)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!other.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(other));
  } else {
    version_ = other.version_;
    length_ = other.length_;
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    data_tablet_id_ = other.data_tablet_id_;
    ref_tablet_id_ = other.ref_tablet_id_;
    create_scn_ = other.create_scn_;
    start_scn_ = other.start_scn_;
    clog_checkpoint_scn_ = other.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = other.ddl_checkpoint_scn_;
    snapshot_version_ = other.snapshot_version_;
    multi_version_start_ = other.multi_version_start_;
    ha_status_ = other.ha_status_;
    report_status_ = other.report_status_;
    table_store_flag_ = other.table_store_flag_;
    ddl_start_scn_ = other.ddl_start_scn_;
    ddl_snapshot_version_ = other.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = other.max_sync_storage_schema_version_;
    ddl_execution_id_ = other.ddl_execution_id_;
    ddl_data_format_version_ = other.ddl_data_format_version_;
    max_serialized_medium_scn_ = other.max_serialized_medium_scn_;
    ddl_commit_scn_ = other.ddl_commit_scn_;
    mds_checkpoint_scn_ = other.mds_checkpoint_scn_;
    transfer_info_ = other.transfer_info_;
    extra_medium_info_ = other.extra_medium_info_;
    space_usage_ = other.space_usage_;
    create_schema_version_ = other.create_schema_version_;
    compat_mode_ = other.compat_mode_;
    has_next_tablet_ = other.has_next_tablet_;
    if (OB_FAIL(last_persisted_committed_tablet_status_.assign(other.last_persisted_committed_tablet_status_))) {
      LOG_WARN("fail to init last_persisted_committed_tablet_status from old tablet meta", K(ret),
          "last_persisted_committed_tablet_status", other.last_persisted_committed_tablet_status_);
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::init(
    const ObTabletMeta &old_tablet_meta,
    const ObMigrationTabletParam *tablet_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!old_tablet_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(old_tablet_meta));
  } else if (OB_FAIL(inner_check_(old_tablet_meta, tablet_meta))) {
    LOG_WARN("failed to do inner check", K(ret), K(old_tablet_meta), KP(tablet_meta));
  } else if (OB_NOT_NULL(tablet_meta) && old_tablet_meta.clog_checkpoint_scn_ >= tablet_meta->clog_checkpoint_scn_) {
    if (OB_FAIL(assign(old_tablet_meta))) {
      LOG_WARN("failed to assign tablet meta", K(ret), K(old_tablet_meta));
    } else {
      mds_checkpoint_scn_ = MAX(old_tablet_meta.mds_checkpoint_scn_, tablet_meta->mds_checkpoint_scn_);
    }
  } else {
    const int64_t snapshot_version = OB_ISNULL(tablet_meta) ?
        old_tablet_meta.snapshot_version_ : MAX(old_tablet_meta.snapshot_version_, tablet_meta->snapshot_version_);
    const int64_t multi_version_start =  OB_ISNULL(tablet_meta) ?
        old_tablet_meta.multi_version_start_ : MAX(old_tablet_meta.multi_version_start_, tablet_meta->multi_version_start_);
    const int64_t max_sync_storage_schema_version = OB_ISNULL(tablet_meta) ?
        old_tablet_meta.max_sync_storage_schema_version_ : MIN(old_tablet_meta.max_sync_storage_schema_version_,
            tablet_meta->max_sync_storage_schema_version_);
    const SCN clog_checkpoint_scn = OB_ISNULL(tablet_meta) ?
        old_tablet_meta.clog_checkpoint_scn_ : MAX(old_tablet_meta.clog_checkpoint_scn_, tablet_meta->clog_checkpoint_scn_);
    ObTabletTransferInfo transfer_info =  old_tablet_meta.transfer_info_;
    if (transfer_info.has_transfer_table()) {
      transfer_info = OB_ISNULL(tablet_meta) ? old_tablet_meta.transfer_info_ : tablet_meta->transfer_info_;
    }

    ObTabletTableStoreFlag table_store_flag = old_tablet_meta.table_store_flag_;
    if (!table_store_flag.with_major_sstable()) {
      table_store_flag = OB_ISNULL(tablet_meta) ? table_store_flag : tablet_meta->table_store_flag_;
    }
    const SCN mds_checkpoint_scn = OB_ISNULL(tablet_meta) ?
        old_tablet_meta.mds_checkpoint_scn_ : MAX(old_tablet_meta.mds_checkpoint_scn_, tablet_meta->mds_checkpoint_scn_);
    // fuse restore status during migration, consider the following timeline
    // 1. SOURCE: tablet P0 was created with restore status FULL by replay start transfer in.
    // 2. TARGET: rebuild was triggered, then create P0 with restore status FULL, and data status INCOMPLETE.
    // 3. SOURCE: transfer handler modified the restore status of P0 to EMPTY.
    // 4. SOURCE: the minor of P0 was restored by restore handler, then set the restore status to MINOR_AND_MAJOR_META.
    // 5. TARGET: the minor of P0 was restored by migration, then set data status COMPLETE.
    // The result is P0 was FULL, but only exist minor sstables, with no major.
    ObTabletHAStatus new_ha_status = old_tablet_meta.ha_status_;
    if (!old_tablet_meta.ha_status_.is_data_status_complete() && OB_NOT_NULL(tablet_meta)) {
      ObTabletRestoreStatus::STATUS src_restore_status;
      if (OB_FAIL(tablet_meta->ha_status_.get_restore_status(src_restore_status))) {
        LOG_WARN("failed to get restore status", K(ret), KPC(tablet_meta));
      } else if (OB_FAIL(new_ha_status.set_restore_status(src_restore_status))) {
        LOG_WARN("failed to set new restore status", K(ret), K(new_ha_status), K(src_restore_status));
      }
    }

    if (OB_SUCC(ret)) {
      version_ = TABLET_META_VERSION;
      ls_id_ = old_tablet_meta.ls_id_;
      tablet_id_ = old_tablet_meta.tablet_id_;
      data_tablet_id_ = old_tablet_meta.data_tablet_id_;
      ref_tablet_id_ = old_tablet_meta.ref_tablet_id_;
      create_scn_ = old_tablet_meta.create_scn_;
      start_scn_ = old_tablet_meta.start_scn_;
      clog_checkpoint_scn_ = clog_checkpoint_scn;
      ddl_checkpoint_scn_ = old_tablet_meta.ddl_checkpoint_scn_;
      snapshot_version_ = snapshot_version;
      multi_version_start_ = multi_version_start;
      ha_status_ = new_ha_status;
      report_status_ = old_tablet_meta.report_status_; //old tablet meta report status already reset
      table_store_flag_ = table_store_flag;
      ddl_start_scn_ = old_tablet_meta.ddl_start_scn_;
      ddl_snapshot_version_ = old_tablet_meta.ddl_snapshot_version_;
      max_sync_storage_schema_version_ = max_sync_storage_schema_version;
      ddl_execution_id_ = old_tablet_meta.ddl_execution_id_;
      ddl_data_format_version_ = old_tablet_meta.ddl_data_format_version_;
      max_serialized_medium_scn_ = MAX(old_tablet_meta.max_serialized_medium_scn_,
          OB_ISNULL(tablet_meta) ? 0 : tablet_meta->max_serialized_medium_scn_);
      ddl_commit_scn_ = old_tablet_meta.ddl_commit_scn_;
      mds_checkpoint_scn_ = mds_checkpoint_scn;
      transfer_info_ = transfer_info;
      extra_medium_info_ = old_tablet_meta.extra_medium_info_;
      space_usage_ = old_tablet_meta.space_usage_;
      create_schema_version_ = old_tablet_meta.create_schema_version_;
      compat_mode_ = old_tablet_meta.compat_mode_;
      has_next_tablet_ = old_tablet_meta.has_next_tablet_;
      if (OB_FAIL(last_persisted_committed_tablet_status_.assign(old_tablet_meta.last_persisted_committed_tablet_status_))) {
        LOG_WARN("fail to init last_persisted_committed_tablet_status from old tablet meta", K(ret),
            "last_persisted_committed_tablet_status", old_tablet_meta.last_persisted_committed_tablet_status_);
      }
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

void ObTabletMeta::reset()
{
  version_ = 0;
  ls_id_.reset();
  tablet_id_.reset();
  data_tablet_id_.reset();
  ref_tablet_id_.reset();
  has_next_tablet_ = false;
  create_scn_ = ObTabletMeta::INVALID_CREATE_SCN;
  create_schema_version_ = 0;
  start_scn_.reset();
  clog_checkpoint_scn_.reset();
  ddl_checkpoint_scn_.set_min();
  snapshot_version_ = OB_INVALID_TIMESTAMP;
  multi_version_start_ = OB_INVALID_TIMESTAMP;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  ha_status_.reset();
  report_status_.reset();
  table_store_flag_.reset();
  ddl_start_scn_.set_min();
  ddl_commit_scn_.set_min();
  ddl_snapshot_version_ = OB_INVALID_TIMESTAMP;
  max_sync_storage_schema_version_ = 0;
  max_serialized_medium_scn_ = 0;
  ddl_execution_id_ = -1;
  ddl_data_format_version_ = 0;
  mds_checkpoint_scn_.reset();
  transfer_info_.reset();
  extra_medium_info_.reset();
  last_persisted_committed_tablet_status_.reset();
  space_usage_.reset();
  is_inited_ = false;
}

bool ObTabletMeta::is_valid() const
{
  // TODO: add more check
  return ls_id_.is_valid()
      && tablet_id_.is_valid()
      && data_tablet_id_.is_valid()
      && create_scn_ != INVALID_CREATE_SCN
      && multi_version_start_ >= 0
      && multi_version_start_ <= snapshot_version_
      && snapshot_version_ >= 0
      && snapshot_version_ != INT64_MAX
      && compat_mode_ != lib::Worker::CompatMode::INVALID
      && max_sync_storage_schema_version_ >= 0
      && max_serialized_medium_scn_ >= 0
      && ha_status_.is_valid()
      && transfer_info_.is_valid()
      && last_persisted_committed_tablet_status_.is_valid()
      && (ha_status_.is_restore_status_pending()
          || (!ha_status_.is_restore_status_pending()
              && clog_checkpoint_scn_ >= INIT_CLOG_CHECKPOINT_SCN
              && start_scn_ >= INIT_CLOG_CHECKPOINT_SCN
              && start_scn_ <= clog_checkpoint_scn_))
      && create_schema_version_ >= 0
      && space_usage_.is_valid();
}

int ObTabletMeta::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  const int64_t length = get_serialize_size();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tablet meta is invalid", K(ret), K(*this));
  } else if (TABLET_META_VERSION != version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid version", K(ret), K_(version));
  } else if (OB_UNLIKELY(length > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - new_pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, len, new_pos, version_))) {
    LOG_WARN("failed to serialize tablet meta's version", K(ret), K(len), K(new_pos), K_(version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i32(buf, len, new_pos, length))) {
    LOG_WARN("failed to serialize tablet meta's length", K(ret), K(len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(ls_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ls id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(data_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize data tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ref_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ref tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_bool(buf, len, new_pos, has_next_tablet_))) {
    LOG_WARN("failed to serialize has next tablet", K(ret), K(len), K(new_pos), K_(has_next_tablet));
  } else if (new_pos - pos < length && OB_FAIL(create_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize create scn", K(ret), K(len), K(new_pos), K_(create_scn));
  } else if (new_pos - pos < length && OB_FAIL(start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize start scn", K(ret), K(len), K(new_pos), K_(start_scn));
  } else if (new_pos - pos < length && OB_FAIL(clog_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(clog_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl checkpoint ts", K(ret), K(len), K(new_pos), K_(ddl_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, snapshot_version_))) {
    LOG_WARN("failed to serialize snapshot version", K(ret), K(len), K(new_pos), K_(snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, multi_version_start_))) {
    LOG_WARN("failed to serialize multi version start", K(ret), K(len), K(new_pos), K_(multi_version_start));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(compat_mode_)))) {
    LOG_WARN("failed to serialize compat mode", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ha_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ha status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(report_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize report status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(table_store_flag_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize table store flag", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl start log ts", K(ret), K(len), K(new_pos), K_(ddl_start_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_snapshot_version_))) {
    LOG_WARN("failed to serialize ddl snapshot version", K(ret), K(len), K(new_pos), K_(ddl_snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_sync_storage_schema_version_))) {
    LOG_WARN("failed to serialize max_sync_storage_schema_version", K(ret), K(len), K(new_pos), K_(max_sync_storage_schema_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_execution_id_))) {
    LOG_WARN("failed to serialize ddl execution id", K(ret), K(len), K(new_pos), K_(ddl_execution_id));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_data_format_version_))) {
    LOG_WARN("failed to serialize ddl data format version", K(ret), K(len), K(new_pos), K_(ddl_data_format_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_serialized_medium_scn_))) {
    LOG_WARN("failed to serialize max serialized medium scn", K(ret), K(len), K(new_pos), K_(max_serialized_medium_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_commit_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl commit scn", K(ret), K(len), K(new_pos), K_(ddl_commit_scn));
  } else if (new_pos - pos < length && OB_FAIL(mds_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize mds checkpoint ts", K(ret), K(len), K(new_pos), K_(mds_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(transfer_info_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize transfer info", K(ret), K(len), K(new_pos), K_(transfer_info));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, create_schema_version_))) {
    LOG_WARN("failed to serialize create schema version", K(ret), K(len), K(new_pos), K_(create_schema_version));
  } else if (new_pos - pos < length && OB_FAIL(space_usage_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet space usage", K(ret), K(len), K(new_pos), K_(space_usage));
  } else if (new_pos - pos < length && OB_FAIL(extra_medium_info_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize extra_medium_info", K(ret), K(len), K(new_pos), K_(extra_medium_info));
  } else if (new_pos - pos < length && OB_FAIL(last_persisted_committed_tablet_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize last_persisted_committed_tablet_status", K(ret), K(len), K(new_pos), K_(last_persisted_committed_tablet_status));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta's length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length), K(length));
  } else {
    pos = new_pos;
    LOG_DEBUG("succeed to serialize tablet meta", KPC(this));
  }

  return ret;
}

int ObTabletMeta::deserialize(
    const char *buf,
    const int64_t len,
    int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("cannot deserialize inited tablet meta", K(ret), K_(is_inited));
  } else if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, &version_))) {
    LOG_WARN("failed to deserialize tablet meta's version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, &length_))) {
    LOG_WARN("failed to deserialize tablet meta's length", K(ret), K(len), K(new_pos));
  } else if (TABLET_META_VERSION == version_) {
    int8_t compat_mode = -1;
    ddl_execution_id_ = 0;
    if (OB_UNLIKELY(length_ > len - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer's length is not enough", K(ret), K(length_), K(len - new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ls_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ls id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(tablet_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tablet id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(data_tablet_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize data tablet id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ref_tablet_id_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ref tablet id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_bool(buf, len, new_pos, &has_next_tablet_))) {
      LOG_WARN("failed to deserialize has_next_tablet_", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(create_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize create scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(start_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize start scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(clog_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ddl_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl checkpoint ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &snapshot_version_))) {
      LOG_WARN("failed to deserialize snapshot version", K(ret), K(len));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &multi_version_start_))) {
      LOG_WARN("failed to deserialize multi version start", K(ret), K(len));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i8(buf, len, new_pos, &compat_mode))) {
      LOG_WARN("failed to deserialize compat mode", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ha_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize restore status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(report_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize report status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(table_store_flag_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize table store flag", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ddl_start_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl start log ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_snapshot_version_))) {
      LOG_WARN("failed to deserialize ddl snapshot version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_sync_storage_schema_version_))) {
      LOG_WARN("failed to deserialize max_sync_storage_schema_version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_execution_id_))) {
      LOG_WARN("failed to deserialize ddl execution id", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_data_format_version_))) {
      LOG_WARN("failed to deserialize ddl data format version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_serialized_medium_scn_))) {
      LOG_WARN("failed to deserialize max serialized medium scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ddl_commit_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl commit scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(mds_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize mds checkpoint ts", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(transfer_info_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize transfer info", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &create_schema_version_))) {
      LOG_WARN("failed to deserialize create schema version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(space_usage_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize tablet space usage", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(extra_medium_info_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize extra_medium_info", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(last_persisted_committed_tablet_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize last_persisted_committed_tablet_status", K(ret), K(len), K(new_pos));
    } else if (OB_UNLIKELY(length_ != new_pos - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K_(length));
    } else {
      pos = new_pos;
      compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
      is_inited_ = true;
    }
    LOG_DEBUG("succeed to deserialize tablet meta", KPC(this));
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid version", K(ret), K_(version));
  }

  return ret;
}

int64_t ObTabletMeta::get_serialize_size() const
{
  int64_t size = 0;
  size += serialization::encoded_length_i32(version_);
  size += serialization::encoded_length_i32(length_);
  size += ls_id_.get_serialize_size();
  size += tablet_id_.get_serialize_size();
  size += data_tablet_id_.get_serialize_size();
  size += ref_tablet_id_.get_serialize_size();
  size += serialization::encoded_length_bool(has_next_tablet_);
  size += create_scn_.get_fixed_serialize_size();
  size += start_scn_.get_fixed_serialize_size();
  size += clog_checkpoint_scn_.get_fixed_serialize_size();
  size += ddl_checkpoint_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(snapshot_version_);
  size += serialization::encoded_length_i64(multi_version_start_);
  size += serialization::encoded_length_i8(static_cast<int8_t>(compat_mode_));
  size += ha_status_.get_serialize_size();
  size += report_status_.get_serialize_size();
  size += table_store_flag_.get_serialize_size();
  size += ddl_start_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(ddl_snapshot_version_);
  size += serialization::encoded_length_i64(max_sync_storage_schema_version_);
  size += serialization::encoded_length_i64(ddl_execution_id_);
  size += serialization::encoded_length_i64(ddl_data_format_version_);
  size += serialization::encoded_length_i64(max_serialized_medium_scn_);
  size += ddl_commit_scn_.get_fixed_serialize_size();
  size += mds_checkpoint_scn_.get_fixed_serialize_size();
  size += transfer_info_.get_serialize_size();
  size += serialization::encoded_length_i64(create_schema_version_);
  size += space_usage_.get_serialize_size();
  size += extra_medium_info_.get_serialize_size();
  size += last_persisted_committed_tablet_status_.get_serialize_size();
  return size;
}

int ObTabletMeta::deserialize_id(
      const char *buf,
      const int64_t len,
      int64_t &pos,
      share::ObLSID &ls_id,
      common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  int32_t version = 0;
  int32_t length = 0;
  if (OB_FAIL(serialization::decode_i32(buf, len, pos, &version))) {
    LOG_WARN("fail to deserialize tablet meta's version", K(ret), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, pos, &length))) {
    LOG_WARN("fail to deserialize tablet meta's length", K(ret), K(len), K(pos));
  } else if (TABLET_META_VERSION == version) {
    if (OB_FAIL(ls_id.deserialize(buf, len, pos))) {
      LOG_WARN("fail to deserialize ls id", K(ret), K(len), K(pos));
    } else if (OB_FAIL(tablet_id.deserialize(buf, len, pos))) {
      LOG_WARN("fail to deserialize tablet id", K(ret), K(len), K(pos));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("The version of tablet is not 1", K(ret), K(version));
  }

  return ret;
}

int ObTabletMeta::init_report_info(
    const blocksstable::ObSSTable *sstable,
    const int64_t report_version,
    ObTabletReportStatus &report_status)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(sstable) || !sstable->is_major_sstable() || report_version < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(sstable), K(report_version));
  } else if (sstable->get_snapshot_version() < report_status.merge_snapshot_version_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get unexpected merge snapshot version", K(ret), K(report_status), KPC(sstable));
  } else if (sstable->get_snapshot_version() == report_status.merge_snapshot_version_) {
  } else {
    report_status.reset();
    report_status.cur_report_version_ = report_version;
    report_status.merge_snapshot_version_ = sstable->get_snapshot_version();
    report_status.row_count_ = sstable->get_row_count();
    report_status.data_checksum_ = sstable->is_co_sstable()
                                 ? static_cast<const ObCOSSTableV2 *>(sstable)->get_cs_meta().data_checksum_
                                 : sstable->get_data_checksum();
  }
  return ret;
}

SCN ObTabletMeta::get_ddl_sstable_start_scn() const
{
  return ddl_start_scn_.is_valid_and_not_min () ? share::SCN::scn_dec(ddl_start_scn_) : ddl_start_scn_;
}

int ObTabletMeta::inner_check_(
    const ObTabletMeta &old_tablet_meta,
    const ObMigrationTabletParam *tablet_meta)
{
  int ret = OB_SUCCESS;
  if (!old_tablet_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("inner check get invalid argument", K(ret), K(old_tablet_meta));
  } else if (OB_ISNULL(tablet_meta)) {
    //do nothing
  } else if (old_tablet_meta.ls_id_ != tablet_meta->ls_id_
      || old_tablet_meta.tablet_id_ != tablet_meta->tablet_id_
      || old_tablet_meta.data_tablet_id_ != tablet_meta->data_tablet_id_
      || old_tablet_meta.ref_tablet_id_ != tablet_meta->ref_tablet_id_
      || old_tablet_meta.compat_mode_ != tablet_meta->compat_mode_
      || old_tablet_meta.transfer_info_.transfer_seq_ != tablet_meta->transfer_info_.transfer_seq_) {
    //TODO(muwei.ym) Fix it in 4.3
    //1.tablet meta transfer seq < old tablet meta transfer seq should failed.
    //2.tablet meta transfer seq > old tablet meta transfer seq should use tablet meta
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet meta part variable is not same with migration tablet param",
        K(ret), K(old_tablet_meta), KPC(tablet_meta));
  }
  return ret;
}

bool ObTabletMeta::has_transfer_table() const
{
  return transfer_info_.has_transfer_table();
}

int ObTabletMeta::reset_transfer_table()
{
  int ret = OB_SUCCESS;
  if (!transfer_info_.has_transfer_table()) {
    ret = OB_TRANSFER_SYS_ERROR;
    LOG_WARN("logical table should be exist", K(ret), K(transfer_info_));
  } else {
    transfer_info_.reset_transfer_table();
  }
  return ret;
}

SCN ObTabletMeta::get_max_replayed_scn() const
{
  return MAX3(clog_checkpoint_scn_, mds_checkpoint_scn_, ddl_checkpoint_scn_);
}

void ObTabletMeta::update_extra_medium_info(
    const compaction::ObMergeType merge_type,
    const int64_t finish_medium_scn)
{
  if (is_major_merge_type(merge_type)) {
    extra_medium_info_.last_compaction_type_ = is_major_merge(merge_type) ? compaction::ObMediumCompactionInfo::MAJOR_COMPACTION : compaction::ObMediumCompactionInfo::MEDIUM_COMPACTION;
    extra_medium_info_.last_medium_scn_ = finish_medium_scn;
    extra_medium_info_.wait_check_flag_ = true;
  }
}

void ObTabletMeta::update_extra_medium_info(
    const compaction::ObExtraMediumInfo &src_addr_extra_info,
    const compaction::ObExtraMediumInfo &src_data_extra_info,
    const int64_t finish_medium_scn)
{
  if (finish_medium_scn < src_addr_extra_info.last_medium_scn_
      || src_addr_extra_info.last_medium_scn_ < src_data_extra_info.last_medium_scn_) {
    extra_medium_info_.last_compaction_type_ = src_data_extra_info.last_compaction_type_;
    extra_medium_info_.last_medium_scn_ = src_data_extra_info.last_medium_scn_;
    if (0 == src_data_extra_info.last_medium_scn_) {
      extra_medium_info_.wait_check_flag_ = false;
    } else {
      extra_medium_info_.wait_check_flag_ = true;
    }
  } else {
    extra_medium_info_ = src_addr_extra_info;
  }
}

int ObTabletMeta::update_meta_last_persisted_committed_tablet_status(
    const ObTabletTxMultiSourceDataUnit &tx_data,
    const share::SCN &create_commit_scn,
    ObTabletCreateDeleteMdsUserData &last_persisted_committed_tablet_status)
{
  int ret = OB_SUCCESS;
  if (tx_data.is_in_tx()) {
    last_persisted_committed_tablet_status.on_init();
  } else {
    ObTabletCreateDeleteMdsUserData user_data;
    user_data.tablet_status_ = tx_data.tablet_status_;
    user_data.create_commit_scn_ = create_commit_scn;
    user_data.create_commit_version_ = tx_data.tx_scn_.get_val_for_tx();
    user_data.transfer_scn_ = tx_data.transfer_scn_;
    user_data.transfer_ls_id_ = tx_data.transfer_ls_id_;
    if (ObTabletStatus::DELETED == tx_data.tablet_status_) {
      //TODO(bizhu) check deleted trans scn
      user_data.delete_commit_scn_ = tx_data.tx_scn_;
      //user_data.delete_commit_version_ = tx_data.tx_scn_;
    }
    if (OB_FAIL(last_persisted_committed_tablet_status.assign(user_data))) {
      LOG_WARN("failed to set last_persisted_committed_tablet_status", K(ret), K(user_data));
    }
  }
  return ret;
}

ObMigrationTabletParam::ObMigrationTabletParam()
  : magic_number_(MAGIC_NUM),
    version_(PARAM_VERSION_V3), // TODO(@bowen.gbw): set version to v3
    is_empty_shell_(false),
    ls_id_(),
    tablet_id_(),
    data_tablet_id_(),
    ref_tablet_id_(),
    create_scn_(ObTabletMeta::INVALID_CREATE_SCN),
    start_scn_(),
    clog_checkpoint_scn_(),
    ddl_checkpoint_scn_(SCN::min_scn()),
    snapshot_version_(OB_INVALID_TIMESTAMP),
    multi_version_start_(OB_INVALID_TIMESTAMP),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    ha_status_(),
    report_status_(),
    storage_schema_(),
    medium_info_list_(),
    extra_medium_info_(),
    last_persisted_committed_tablet_status_(ObTabletStatus::MAX, ObTabletMdsUserDataType::MAX_TYPE),
    table_store_flag_(),
    ddl_start_scn_(SCN::min_scn()),
    ddl_snapshot_version_(OB_INVALID_TIMESTAMP),
    max_sync_storage_schema_version_(0),
    ddl_execution_id_(-1),
    ddl_data_format_version_(0),
    max_serialized_medium_scn_(0),
    ddl_commit_scn_(SCN::min_scn()),
    mds_checkpoint_scn_(),
    mds_data_(),
    transfer_info_(),
    create_schema_version_(0),
    allocator_("MigTblParam", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID)
{
}

bool ObMigrationTabletParam::is_valid() const
{
  bool bool_ret = true;

  if (bool_ret) {
    bool_ret = MAGIC_NUM == magic_number_
      && (PARAM_VERSION == version_  || PARAM_VERSION_V2 == version_ || PARAM_VERSION_V3 == version_)
      && ls_id_.is_valid()
      && tablet_id_.is_valid()
      && data_tablet_id_.is_valid()
      && create_scn_ != ObTabletMeta::INVALID_CREATE_SCN;
    if (!bool_ret) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid param", K_(ls_id), K_(tablet_id), K_(data_tablet_id), K_(create_scn),
               K_(magic_number), K_(version));
    }
  }

  if (bool_ret) {
    bool_ret = multi_version_start_ >= 0
        && multi_version_start_ <= snapshot_version_
        && compat_mode_ != lib::Worker::CompatMode::INVALID
        && max_sync_storage_schema_version_ >= 0
        && max_serialized_medium_scn_ >= 0
        && last_persisted_committed_tablet_status_.is_valid();
    if (!bool_ret) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid param", K_(multi_version_start), K_(snapshot_version),
          K_(compat_mode), K_(max_sync_storage_schema_version), K_(max_serialized_medium_scn),
          K_(clog_checkpoint_scn), K_(last_persisted_committed_tablet_status));
    }
  }

  if (bool_ret) {
    bool_ret = ha_status_.is_valid()
      && transfer_info_.is_valid()
      && (ha_status_.is_restore_status_pending()
          || (start_scn_ >= SCN::base_scn()
              && clog_checkpoint_scn_ >= SCN::base_scn()
              && start_scn_ <= clog_checkpoint_scn_
              && (is_empty_shell()
                  || (!is_empty_shell() && storage_schema_.is_valid()))));
    // no valid storage_schema_ for empty shell
    if (!bool_ret) {
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "invalid param", K_(ha_status), K_(transfer_info),
          K_(start_scn), K_(clog_checkpoint_scn), K_(start_scn), K(is_empty_shell()),
          "storage_schema", storage_schema_.is_valid());
    }
  }

  return bool_ret;
}

bool ObMigrationTabletParam::is_empty_shell() const
{
  return is_empty_shell_;
}

SCN ObMigrationTabletParam::get_max_tablet_checkpoint_scn() const
{
  return MAX3(clog_checkpoint_scn_, mds_checkpoint_scn_, ddl_checkpoint_scn_);
}

int ObMigrationTabletParam::get_tablet_status_for_transfer(ObTabletCreateDeleteMdsUserData &user_data) const
{
  int ret = OB_SUCCESS;
  user_data.reset();

  if (PARAM_VERSION == version_) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("not supported for this param version", K(ret), K_(ls_id), K_(tablet_id), K_(version));
  } else if (PARAM_VERSION_V2 == version_) {
    // use uncommitted tablet status mds dump kv
    const ObString &str = mds_data_.tablet_status_uncommitted_kv_.v_.user_data_;
    int64_t pos = 0;
    if (OB_UNLIKELY(str.empty())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("user data is empty", K(ret), K_(ls_id), K_(tablet_id), "tablet_status_uncommitted_kv", mds_data_.tablet_status_uncommitted_kv_);
    } else if (OB_FAIL(user_data.deserialize(str.ptr(), str.length(), pos))) {
      LOG_WARN("failed to deserialize user data", K(ret), K_(ls_id), K_(tablet_id));
    }
  } else if (version_ >= PARAM_VERSION_V3) {
    if (OB_FAIL(user_data.assign(last_persisted_committed_tablet_status_))) {
      LOG_WARN("fail to assign tablet status", K(ret), K_(ls_id), K_(tablet_id), K_(last_persisted_committed_tablet_status));
    }
  }

  return ret;
}

int ObMigrationTabletParam::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t length = 0;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("migration tablet param is invalid", K(ret), K(*this));
  } else if (FALSE_IT(length = get_serialize_size())) {
  } else if (OB_UNLIKELY(length > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, magic_number_))) {
    LOG_WARN("failed to serialize magic number", K(ret), K(len), K(new_pos), K_(magic_number));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, version_))) {
    LOG_WARN("failed to serialize version", K(ret), K(len), K(new_pos), K_(version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, length))) {
    LOG_WARN("failed to serialize length", K(ret), K(len), K(new_pos), K(length));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_bool(buf, len, new_pos, is_empty_shell_))) {
    LOG_WARN("failed to serialize is empty shell", K(ret), K(len), K(new_pos), K(is_empty_shell_));
  } else if (new_pos - pos < length && OB_FAIL(ls_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ls id", K(ret), K(len), K(new_pos), K_(ls_id));
  } else if (new_pos - pos < length && OB_FAIL(tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet id", K(ret), K(len), K(new_pos), K_(tablet_id));
  } else if (new_pos - pos < length && OB_FAIL(data_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize data tablet id", K(ret), K(len), K(new_pos), K_(data_tablet_id));
  } else if (new_pos - pos < length && OB_FAIL(ref_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ref tablet id", K(ret), K(len), K(new_pos), K_(ref_tablet_id));
  } else if (new_pos - pos < length && OB_FAIL(create_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(create_scn));
  } else if (new_pos - pos < length && OB_FAIL(start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize start scn", K(ret), K(len), K(new_pos), K_(start_scn));
  } else if (new_pos - pos < length && OB_FAIL(clog_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(clog_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl checkpoint ts", K(ret), K(len), K(new_pos), K_(ddl_checkpoint_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, snapshot_version_))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, multi_version_start_))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(multi_version_start));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(compat_mode_)))) {
    LOG_WARN("failed to serialize compat mode", K(ret), K(len), K(new_pos), K_(compat_mode));
  } else if (new_pos - pos < length && OB_FAIL(ha_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ha status", K(ret), K(len), K(new_pos), K_(ha_status));
  } else if (new_pos - pos < length && OB_FAIL(report_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize report status", K(ret), K(len), K(new_pos), K_(report_status));
  } else if (!is_empty_shell() && new_pos - pos < length && OB_FAIL(storage_schema_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize storage schema", K(ret), K(len), K(new_pos), K_(storage_schema));
  } else if (PARAM_VERSION_V2 <= version_ && !is_empty_shell() && new_pos - pos < length && OB_FAIL(medium_info_list_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize medium compaction list", K(ret), K(len), K(new_pos), K_(medium_info_list));
  } else if (new_pos - pos < length && OB_FAIL(table_store_flag_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize table store flag", K(ret), K(len), K(new_pos), K_(table_store_flag));
  } else if (new_pos - pos < length && OB_FAIL(ddl_start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl start log ts", K(ret), K(len), K(new_pos), K_(ddl_start_scn));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_snapshot_version_))) {
    LOG_WARN("failed to serialize ddl snapshot version", K(ret), K(len), K(new_pos), K_(ddl_snapshot_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_sync_storage_schema_version_))) {
    LOG_WARN("failed to serialize max_sync_storage_schema_version", K(ret), K(len), K(new_pos), K_(max_sync_storage_schema_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_execution_id_))) {
    LOG_WARN("failed to serialize ddl execution id", K(ret), K(len), K(new_pos), K_(ddl_execution_id));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_data_format_version_))) {
    LOG_WARN("failed to serialize ddl data format version", K(ret), K(len), K(new_pos), K_(ddl_data_format_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_serialized_medium_scn_))) {
    LOG_WARN("failed to serialize max serialized medium scn", K(ret), K(len), K(new_pos), K_(max_serialized_medium_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_commit_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl commit scn", K(ret), K(len), K(new_pos), K_(ddl_commit_scn));
  } else if (new_pos - pos < length && OB_FAIL(mds_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize mds checkpoint ts", K(ret), K(len), K(new_pos), K_(mds_checkpoint_scn));
  } else if (PARAM_VERSION_V2 == version_ && new_pos - pos < length && OB_FAIL(mds_data_.serialize(buf, len, new_pos))) {
    //TODO(xianzhi) src tablet status will used to dest create tablet and set tablet status cache. check it
    LOG_WARN("failed to serialize mds data", K(ret), K(len), K(new_pos), K_(mds_data));
  } else if (new_pos - pos < length && OB_FAIL(transfer_info_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize transfer info", K(ret), K(len), K(new_pos), K_(transfer_info));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, create_schema_version_))) {
    LOG_WARN("failed to serialize create schema version", K(ret), K(len), K(new_pos), K_(create_schema_version));
  } else if (PARAM_VERSION_V3 <= version_ && new_pos - pos < length && OB_FAIL(extra_medium_info_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize extra_medium_info", K(ret), K(len), K(new_pos), K_(extra_medium_info));
  } else if (PARAM_VERSION_V3 <= version_ && new_pos - pos < length && OB_FAIL(last_persisted_committed_tablet_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize last_persisted_committed_tablet_status", K(ret), K(len), K(new_pos), K_(last_persisted_committed_tablet_status));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length));
  } else {
    pos = new_pos;

    LOG_DEBUG("succeed to serialize migration tablet param", K(ret), KPC(this));
  }

  return ret;
}

int ObMigrationTabletParam::deserialize_v2_v3(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t new_pos = pos;
  int8_t compat_mode = -1;
  int64_t total_length = 0; // total serialize size
  int64_t length = 0; // total serialize size expect for the first 2 fields.

  if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &total_length))) {
    LOG_WARN("failed to deserialize length", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(total_length - 24 > len - new_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(total_length), K(len - new_pos));
  } else if (FALSE_IT(length = total_length - 16)) {
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_bool(buf, len, new_pos, &is_empty_shell_))) {
    LOG_WARN("failed to serialize is empty shell", K(ret), K(len), K(new_pos), K(is_empty_shell_));
  } else if (new_pos - pos < length && OB_FAIL(ls_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ls id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(data_tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize data tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ref_tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ref tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(create_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize create scn", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(start_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize start scn", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(clog_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl checkpoint ts", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &snapshot_version_))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &multi_version_start_))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&compat_mode)))) {
    LOG_WARN("failed to deserialize compat mode", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ha_status_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ha status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(report_status_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize report status", K(ret), K(len), K(new_pos));
  } else if (!is_empty_shell() && new_pos - pos < length && OB_FAIL(storage_schema_.deserialize(allocator_, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize storage schema", K(ret), K(len), K(new_pos));
  } else if (PARAM_VERSION_V2 <= version_ && !is_empty_shell() && new_pos - pos < length && OB_FAIL(medium_info_list_.deserialize(allocator_, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize medium compaction list", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(table_store_flag_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize table store flag", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_start_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl start log ts", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_snapshot_version_))) {
    LOG_WARN("failed to deserialize ddl snapshot version", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_sync_storage_schema_version_))) {
    LOG_WARN("failed to deserialize max sync storage schema version", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_execution_id_))) {
    LOG_WARN("failed to deserialize ddl execution id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_data_format_version_))) {
    LOG_WARN("failed to deserialize ddl data format version", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_serialized_medium_scn_))) {
    LOG_WARN("failed to deserialize max sync medium snapshot", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_commit_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl commit scn", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(mds_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize mds_checkpoint_scn", K(ret), K(len), K(new_pos));
  } else if (PARAM_VERSION_V2 == version_ && new_pos - pos < length && OB_FAIL(mds_data_.deserialize(allocator_, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize mds data", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(transfer_info_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize transfer info", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &create_schema_version_))) {
    LOG_WARN("failed to deserialize create schema version", K(ret), K(len));
  } else if (PARAM_VERSION_V3 <= version_ && new_pos - pos < length && OB_FAIL(extra_medium_info_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize extra_medium_info", K(ret), K(len), K(new_pos));
  } else if (PARAM_VERSION_V3 <= version_ && new_pos - pos < length && OB_FAIL(last_persisted_committed_tablet_status_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize last_persisted_committed_tablet_status", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length), KPC(this));
  } else {
    if (PARAM_VERSION_V2 == version_) {
      int64_t tmp_pos = 0;
      extra_medium_info_ = mds_data_.medium_info_list_.extra_medium_info_;
      const ObString &user_data = mds_data_.tablet_status_committed_kv_.v_.user_data_;
      if (user_data.empty()) {
        last_persisted_committed_tablet_status_.reset();
      } else if (OB_FAIL(last_persisted_committed_tablet_status_.deserialize(user_data.ptr(), user_data.length(), tmp_pos))) {
        LOG_WARN("failed to deserialize user data", K(ret), K(tmp_pos), K(user_data));
      }
    }
    compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
    pos = new_pos;
    LOG_DEBUG("succeed to deserialize migration tablet param v2_v3", K(ret), KPC(this));
  }

  return ret;
}

int ObMigrationTabletParam::deserialize_v1(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;

  int64_t new_pos = pos;
  int8_t compat_mode = -1;
  int64_t total_length = 0; // total serialize size
  int64_t length = 0; // total serialize size expect for the first 2 fields.
  ObArenaAllocator allocator("MigDeser");
  ObTabletAutoincSeq auto_inc_seq;
  ObTabletTxMultiSourceDataUnit tx_data;
  ObTabletBindingInfo ddl_data;
  ObTabletMdsData mds_data;

  if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &total_length))) {
    LOG_WARN("failed to deserialize length", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(total_length - 24 > len - new_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(total_length), K(len - new_pos));
  } else if (FALSE_IT(length = total_length - 16)) {
  } else if (new_pos - pos < length && OB_FAIL(ls_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ls id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(data_tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize data tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ref_tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ref tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(create_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize create scn", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(start_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize start scn", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(clog_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl checkpoint ts", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &snapshot_version_))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &multi_version_start_))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&compat_mode)))) {
    LOG_WARN("failed to deserialize compat mode", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(auto_inc_seq.deserialize(allocator, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize auto inc seq", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ha_status_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ha status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(report_status_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize report status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(tx_data.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize multi source data", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_data.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl data", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(storage_schema_.deserialize(allocator_, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize storage schema", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(medium_info_list_.deserialize(allocator_, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize medium compaction list", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(table_store_flag_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize table store flag", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_start_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl start log ts", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_snapshot_version_))) {
    LOG_WARN("failed to deserialize ddl snapshot version", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_sync_storage_schema_version_))) {
    LOG_WARN("failed to deserialize max sync storage schema version", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_execution_id_))) {
    LOG_WARN("failed to deserialize ddl execution id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_data_format_version_))) {
    LOG_WARN("failed to deserialize ddl data format version", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_serialized_medium_scn_))) {
    LOG_WARN("failed to deserialize max sync medium snapshot", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length && OB_FAIL(ddl_commit_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl commit scn", K(ret), K(len), K(new_pos));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length), KPC(this));
  } else if (OB_FAIL(ObTabletMdsData::build_mds_data(allocator, auto_inc_seq, tx_data, create_scn_, ddl_data,
                                                     clog_checkpoint_scn_, medium_info_list_, mds_data))) {
    LOG_WARN("failed to build mds data with version 1", K(ret), K(auto_inc_seq), K(tx_data), K_(create_scn),
             K(ddl_data), K_(clog_checkpoint_scn), K_(medium_info_list));
  } else if (OB_FAIL(mds_data_.init(allocator_, mds_data))) {
    LOG_WARN("failed to assign mds data", K(ret), K_(mds_data));
  } else if (OB_FAIL(ObTabletMeta::update_meta_last_persisted_committed_tablet_status(
        tx_data, create_scn_, last_persisted_committed_tablet_status_))) {
    LOG_WARN("fail to init last_persisted_committed_tablet_status from tx data", K(ret),
        K(tx_data), K(create_scn_));
  } else if (OB_FAIL(transfer_info_.init())) {
    LOG_WARN("failed to init transfer info", K(ret));
  } else {
    extra_medium_info_ = medium_info_list_.get_extra_medium_info();
    is_empty_shell_ = false;
    compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
    version_ = PARAM_VERSION_V2;
    pos = new_pos;
    LOG_DEBUG("succeed to deserialize migration tablet param v1", K(ret), KPC(this));
  }

  return ret;
}

int ObMigrationTabletParam::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t length;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(len < pos + 24)) { //at least 24 bytes for magic number, version and length
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &magic_number_))) {
    LOG_WARN("failed to deserialize magic_number", K(ret), K(len), K(new_pos));
  } else if (MAGIC_NUM == magic_number_) {
    if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &version_))) {
      LOG_WARN("failed to deserialize version", K(ret), K(len), K(new_pos));
    } else if (PARAM_VERSION != version_ && PARAM_VERSION_V2 != version_ && PARAM_VERSION_V3 != version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid param version", K(ret), K(version_));
    } else if ((PARAM_VERSION_V2 == version_ || PARAM_VERSION_V3 == version_)
        && OB_FAIL(deserialize_v2_v3(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize v2/v3", K(ret), K(len), K(new_pos));
    } else if (PARAM_VERSION == version_ && OB_FAIL(deserialize_v1(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize v1", K(ret), K(len), K(new_pos));
    } else {
      pos = new_pos;

      FLOG_INFO("succeed to deserialize migration tablet param", K(ret), KPC(this));
    }
  }

  return ret;
}

int64_t ObMigrationTabletParam::get_serialize_size() const
{
  int64_t size = 0;
  int64_t length = 0;
  size += serialization::encoded_length_i64(magic_number_);
  size += serialization::encoded_length_i64(version_);
  size += serialization::encoded_length_i64(length);
  size += serialization::encoded_length_bool(is_empty_shell_);
  size += ls_id_.get_serialize_size();
  size += tablet_id_.get_serialize_size();
  size += data_tablet_id_.get_serialize_size();
  size += ref_tablet_id_.get_serialize_size();
  size += create_scn_.get_fixed_serialize_size();
  size += start_scn_.get_fixed_serialize_size();
  size += clog_checkpoint_scn_.get_fixed_serialize_size();
  size += ddl_checkpoint_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(snapshot_version_);
  size += serialization::encoded_length_i64(multi_version_start_);
  size += serialization::encoded_length_i8(static_cast<int8_t>(compat_mode_));
  size += ha_status_.get_serialize_size();
  size += report_status_.get_serialize_size();
  size += is_empty_shell() ? 0 : storage_schema_.get_serialize_size();
  if (PARAM_VERSION_V2 <= version_) {
    size += is_empty_shell() ? 0 : medium_info_list_.get_serialize_size();
  }
  size += table_store_flag_.get_serialize_size();
  size += ddl_start_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(ddl_snapshot_version_);
  size += serialization::encoded_length_i64(max_sync_storage_schema_version_);
  size += serialization::encoded_length_i64(ddl_execution_id_);
  size += serialization::encoded_length_i64(ddl_data_format_version_);
  if (PARAM_VERSION_V2 == version_) {
    size += mds_data_.get_serialize_size();
  }
  size += serialization::encoded_length_i64(max_serialized_medium_scn_);
  size += ddl_commit_scn_.get_fixed_serialize_size();
  size += mds_checkpoint_scn_.get_fixed_serialize_size();
  size += transfer_info_.get_serialize_size();
  size += serialization::encoded_length_i64(create_schema_version_);
  if (PARAM_VERSION_V3 <= version_) {
    size += extra_medium_info_.get_serialize_size();
    size += last_persisted_committed_tablet_status_.get_serialize_size();
  }
  return size;
}

void ObMigrationTabletParam::reset()
{
  is_empty_shell_ = false;
  ls_id_.reset();
  tablet_id_.reset();
  data_tablet_id_.reset();
  ref_tablet_id_.reset();
  create_scn_ = ObTabletMeta::INVALID_CREATE_SCN;
  start_scn_.reset();
  clog_checkpoint_scn_.reset();
  ddl_checkpoint_scn_.set_min();
  snapshot_version_ = OB_INVALID_TIMESTAMP;
  multi_version_start_ = OB_INVALID_TIMESTAMP;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  ha_status_.reset();
  report_status_.reset();
  storage_schema_.reset();
  medium_info_list_.reset();
  extra_medium_info_.reset();
  // set max, which is invalid
  last_persisted_committed_tablet_status_.reset();
  last_persisted_committed_tablet_status_.tablet_status_ = ObTabletStatus::MAX;
  last_persisted_committed_tablet_status_.data_type_ = ObTabletMdsUserDataType::MAX_TYPE;
  table_store_flag_.reset();
  ddl_start_scn_.set_min();
  ddl_snapshot_version_ = OB_INVALID_TIMESTAMP;
  max_sync_storage_schema_version_ = 0;
  ddl_execution_id_ = -1;
  ddl_data_format_version_ = 0;
  max_serialized_medium_scn_ = 0;
  ddl_commit_scn_.set_min();
  mds_checkpoint_scn_.reset();
  mds_data_.reset();
  transfer_info_.reset();
  create_schema_version_ = 0;
  allocator_.reset();
}

int ObMigrationTabletParam::assign(const ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("migration tablet param is invalid", K(ret), K(param));
  } else {
    // allocator
    allocator_.set_attr(ObMemAttr(MTL_ID(), "MigTabletParam", ObCtxIds::DEFAULT_CTX_ID));
    version_ = param.version_;
    is_empty_shell_ = param.is_empty_shell_;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    data_tablet_id_ = param.data_tablet_id_;
    ref_tablet_id_ = param.ref_tablet_id_;
    create_scn_ = param.create_scn_;
    create_schema_version_ = param.create_schema_version_;
    start_scn_ = param.start_scn_;
    clog_checkpoint_scn_ = param.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = param.ddl_checkpoint_scn_;
    snapshot_version_ = param.snapshot_version_;
    multi_version_start_ = param.multi_version_start_;
    compat_mode_ = param.compat_mode_;
    ha_status_ = param.ha_status_;
    report_status_ = param.report_status_;
    storage_schema_.reset(); // TODO: refactor?
    medium_info_list_.reset();
    table_store_flag_ = param.table_store_flag_;
    ddl_start_scn_ = param.ddl_start_scn_;
    ddl_snapshot_version_ = param.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = param.max_sync_storage_schema_version_;
    max_serialized_medium_scn_ = param.max_serialized_medium_scn_;
    ddl_execution_id_ = param.ddl_execution_id_;
    ddl_data_format_version_ = param.ddl_data_format_version_;
    ddl_commit_scn_ = param.ddl_commit_scn_;
    mds_checkpoint_scn_ = param.mds_checkpoint_scn_;
    transfer_info_ = param.transfer_info_;
    extra_medium_info_ = param.extra_medium_info_;
    if (OB_FAIL(mds_data_.assign(param.mds_data_, allocator_))) {
      LOG_WARN("failed to assign mds data", K(ret), K(param));
    } else if (OB_FAIL(last_persisted_committed_tablet_status_.assign(
          param.last_persisted_committed_tablet_status_))) {
      LOG_WARN("fail to init last_persisted_committed_tablet_status from mig param", K(ret));
    }

    if (OB_FAIL(ret)) {
    } else if (is_empty_shell()) {
      // do nothing
    } else if (OB_FAIL(storage_schema_.init(allocator_, param.storage_schema_))) {
      LOG_WARN("failed to assign storage schema", K(ret), K(param));
    } else if (OB_FAIL(medium_info_list_.init(allocator_, &param.medium_info_list_))) {
      LOG_WARN("failed to assign medium info list", K(ret), K(param));
    }
  }
  return ret;
}

int ObMigrationTabletParam::build_deleted_tablet_info(const share::ObLSID &ls_id, const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (!ls_id.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid args", K(ret), K(ls_id), K(tablet_id));
  } else {
    const ObTabletRestoreStatus::STATUS restore_status = ObTabletRestoreStatus::FULL;
    const ObTabletDataStatus::STATUS data_status = ObTabletDataStatus::COMPLETE;
    const ObTabletExpectedStatus::STATUS expected_status = ObTabletExpectedStatus::DELETED;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    data_tablet_id_ = tablet_id;
    create_scn_ = ObTabletMeta::INIT_CREATE_SCN;
    create_schema_version_ = 0;
    start_scn_ = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
    clog_checkpoint_scn_ = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
    compat_mode_ = lib::Worker::get_compatibility_mode();
    multi_version_start_ = 0;
    snapshot_version_ = 0;
    transfer_info_.ls_id_ = ls_id;
    transfer_info_.transfer_start_scn_ = SCN::min_scn();
    transfer_info_.transfer_seq_ = 0;
    last_persisted_committed_tablet_status_.on_init();
    if (OB_FAIL(ha_status_.set_restore_status(restore_status))) {
      LOG_WARN("failed to set restore status", K(ret), K(restore_status));
    } else if (OB_FAIL(ha_status_.set_data_status(data_status))) {
      LOG_WARN("failed to set data status", K(ret), K(data_status));
    } else if (OB_FAIL(ha_status_.set_expected_status(expected_status))) {
      LOG_WARN("failed to set expected status", K(ret), K(expected_status));
    } else if (OB_FAIL(ObMigrationTabletParam::construct_placeholder_storage_schema_and_medium(
        allocator_,
        storage_schema_,
        medium_info_list_,
        mds_data_))) {
      LOG_WARN("failed to construct placeholder storage schema");
    }
  }
  return ret;
}


int ObMigrationTabletParam::construct_placeholder_storage_schema_and_medium(
    common::ObArenaAllocator &allocator,
    ObStorageSchema &storage_schema,
    compaction::ObMediumCompactionInfoList &medium_info_list,
    ObTabletFullMemoryMdsData &full_memory_mds_data)
{
  int ret = OB_SUCCESS;
  storage_schema.reset();

  storage_schema.allocator_ = &allocator;
  storage_schema.rowkey_array_.set_allocator(&allocator);
  storage_schema.column_array_.set_allocator(&allocator);

  storage_schema.storage_schema_version_ = ObStorageSchema::STORAGE_SCHEMA_VERSION_V3;
  storage_schema.is_use_bloomfilter_ = false;
  storage_schema.table_type_ = ObTableType::USER_TABLE;
  //storage_schema.table_mode_
  storage_schema.index_type_ = ObIndexType::INDEX_TYPE_PRIMARY;
  storage_schema.row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  storage_schema.schema_version_ = ObStorageSchema::STORAGE_SCHEMA_VERSION_V3;
  storage_schema.column_cnt_ = 1;
  storage_schema.store_column_cnt_ = 1;
  storage_schema.tablet_size_ = OB_DEFAULT_TABLET_SIZE;
  storage_schema.pctfree_ = OB_DEFAULT_PCTFREE;
  storage_schema.block_size_ = OB_DEFAULT_MACRO_BLOCK_SIZE;
  storage_schema.progressive_merge_round_ = 0;
  storage_schema.progressive_merge_num_ = 0;
  storage_schema.master_key_id_ = OB_INVALID_ID;
  storage_schema.compat_mode_ = static_cast<uint32_t>(lib::Worker::get_compatibility_mode());

  ObStorageRowkeyColumnSchema rowkey_schema;
  rowkey_schema.meta_type_.set_tinyint();
  rowkey_schema.column_idx_ = common::OB_APP_MIN_COLUMN_ID;
  rowkey_schema.order_ = ObOrderType::ASC;

  ObStorageColumnSchema col_schema;
  col_schema.is_rowkey_column_ = true;
  col_schema.is_column_stored_in_sstable_ = true;
  col_schema.meta_type_ = rowkey_schema.meta_type_;
  col_schema.default_checksum_ = 0;

  if (OB_FAIL(storage_schema.rowkey_array_.reserve(storage_schema.column_cnt_))) {
    STORAGE_LOG(WARN, "Fail to reserve rowkey column array", K(ret));
  } else if (OB_FAIL(storage_schema.column_array_.reserve(storage_schema.column_cnt_))) {
    STORAGE_LOG(WARN, "Fail to reserve column array", K(ret));
  } else if (OB_FAIL(storage_schema.rowkey_array_.push_back(rowkey_schema))) {
    STORAGE_LOG(WARN, "Fail to add rowkey column id to rowkey array", K(ret));
  } else if (OB_FAIL(storage_schema.column_array_.push_back(col_schema))) {
    STORAGE_LOG(WARN, "Fail to push into column array", K(ret), K(col_schema));
  } else {
    storage_schema.is_inited_ = true;
  }

  if (FAILEDx(medium_info_list.init(allocator))) {
    LOG_WARN("failed to init medium info list", K(ret));
  }

  if (FAILEDx(full_memory_mds_data.init(allocator))) {
    LOG_WARN("failed to init full memory mds data", K(ret));
  }

  if (OB_SUCC(ret) && OB_UNLIKELY(!storage_schema.is_valid() || !medium_info_list.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("placeholder storage schema or medium info list is not valid", K(ret), K(storage_schema), K(medium_info_list));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
