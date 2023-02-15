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
#include "share/schema/ob_table_schema.h"
#include "share/scn.h"

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
    has_next_tablet_(false),
    create_scn_(ObTabletMeta::INVALID_CREATE_SCN),
    start_scn_(),
    clog_checkpoint_scn_(),
    ddl_checkpoint_scn_(SCN::min_scn()),
    snapshot_version_(OB_INVALID_TIMESTAMP),
    multi_version_start_(OB_INVALID_TIMESTAMP),
    compat_mode_(lib::Worker::CompatMode::INVALID),
    autoinc_seq_(),
    ha_status_(),
    report_status_(),
    tx_data_(),
    ddl_data_(),
    table_store_flag_(),
    ddl_start_scn_(SCN::min_scn()),
    ddl_snapshot_version_(OB_INVALID_TIMESTAMP),
    max_sync_storage_schema_version_(0),
    ddl_execution_id_(-1),
    ddl_cluster_version_(0),
    max_serialized_medium_scn_(0),
    ddl_commit_scn_(SCN::min_scn()),
    is_inited_(false)
{
}

ObTabletMeta::~ObTabletMeta()
{
  reset();
}

int ObTabletMeta::init(
    ObIAllocator &allocator,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &data_tablet_id,
    const common::ObTabletID &lob_meta_tablet_id,
    const common::ObTabletID &lob_piece_tablet_id,
    const SCN create_scn,
    const int64_t snapshot_version,
    const lib::Worker::CompatMode compat_mode,
    const ObTabletTableStoreFlag &table_store_flag,
    const int64_t max_sync_storage_schema_version,
    const int64_t max_serialized_medium_scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!data_tablet_id.is_valid())
      // TODO: fix it after multi source data refactor
      // || OB_UNLIKELY(!create_scn.is_valid())
      || OB_UNLIKELY(OB_INVALID_TIMESTAMP == snapshot_version)
      || OB_UNLIKELY(lib::Worker::CompatMode::INVALID == compat_mode)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
        K(create_scn), K(snapshot_version), K(compat_mode));
  } else if (OB_FAIL(ha_status_.init_status())) {
    LOG_WARN("failed to init ha status", K(ret));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = ls_id;
    tablet_id_ = tablet_id;
    data_tablet_id_ = data_tablet_id;
    create_scn_ = create_scn;
    start_scn_ = INIT_CLOG_CHECKPOINT_SCN;
    clog_checkpoint_scn_ = INIT_CLOG_CHECKPOINT_SCN;
    ddl_checkpoint_scn_.set_min();
    compat_mode_ = compat_mode;
    snapshot_version_ = snapshot_version;
    multi_version_start_ = snapshot_version;
    table_store_flag_ = table_store_flag;
    ddl_start_scn_.set_min();
    ddl_commit_scn_.set_min();
    ddl_snapshot_version_ = 0;
    max_sync_storage_schema_version_ = max_sync_storage_schema_version;
    max_serialized_medium_scn_ = max_serialized_medium_scn;
    ddl_execution_id_ = -1;
    ddl_cluster_version_ = 0;

    report_status_.merge_snapshot_version_ = snapshot_version;
    report_status_.cur_report_version_ = snapshot_version;
    report_status_.data_checksum_ = 0;
    report_status_.row_count_ = 0;

    ddl_data_.snapshot_version_ = OB_INVALID_VERSION;
    ddl_data_.schema_version_ = OB_INVALID_VERSION;
    ddl_data_.lob_meta_tablet_id_ = lob_meta_tablet_id;
    ddl_data_.lob_piece_tablet_id_ = lob_piece_tablet_id;

    if (tablet_id_.is_ls_inner_tablet()) {
      tx_data_.tablet_status_ = ObTabletStatus::NORMAL;
    }

    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::init(
    common::ObIAllocator &allocator,
    const ObTabletMeta &old_tablet_meta,
    const int64_t snapshot_version,
    const int64_t multi_version_start,
    const ObTabletTxMultiSourceDataUnit &tx_data,
    const ObTabletBindingInfo &ddl_data,
    const ObTabletAutoincSeq &autoinc_seq,
    const int64_t max_sync_storage_schema_version,
    const int64_t max_serialized_medium_scn,
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
  } else if (OB_FAIL(ddl_data_.assign(ddl_data))) {
    LOG_WARN("failed to assign ddl data", K(ret));
  } else if (OB_FAIL(autoinc_seq_.assign(autoinc_seq))) {
    LOG_WARN("failed to assign autoinc seq", K(ret));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = old_tablet_meta.ls_id_;
    tablet_id_ = old_tablet_meta.tablet_id_;
    data_tablet_id_ = old_tablet_meta.data_tablet_id_;
    ref_tablet_id_ = old_tablet_meta.ref_tablet_id_;
    create_scn_ = old_tablet_meta.create_scn_;
    start_scn_ = old_tablet_meta.start_scn_;
    ddl_start_scn_ = SCN::max(ddl_info.ddl_start_scn_, old_tablet_meta.ddl_start_scn_);
    ddl_commit_scn_ = SCN::max(ddl_info.ddl_commit_scn_, old_tablet_meta.ddl_commit_scn_);
    clog_checkpoint_scn_ = SCN::max(clog_checkpoint_scn, old_tablet_meta.clog_checkpoint_scn_);
    compat_mode_ = old_tablet_meta.compat_mode_;
    ha_status_ = old_tablet_meta.ha_status_;
    report_status_ = old_tablet_meta.report_status_;

    snapshot_version_ = MAX(snapshot_version, old_tablet_meta.snapshot_version_);
    multi_version_start_ = MIN(MAX(multi_version_start, old_tablet_meta.multi_version_start_), snapshot_version_);
    tx_data_ = tx_data;
    table_store_flag_ = old_tablet_meta.table_store_flag_;
    max_sync_storage_schema_version_ = max_sync_storage_schema_version;
    max_serialized_medium_scn_ = max_serialized_medium_scn;
    ddl_checkpoint_scn_ = SCN::max(old_tablet_meta.ddl_checkpoint_scn_, ddl_info.ddl_checkpoint_scn_);
    ddl_snapshot_version_ = MAX(old_tablet_meta.ddl_snapshot_version_, ddl_info.ddl_snapshot_version_);
    ddl_execution_id_ = MAX(old_tablet_meta.ddl_execution_id_, ddl_info.ddl_execution_id_);
    ddl_cluster_version_ = MAX(old_tablet_meta.ddl_cluster_version_, ddl_info.ddl_cluster_version_);
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::init(
    common::ObIAllocator &allocator,
    const ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(ddl_data_.assign(param.ddl_data_))) {
    LOG_WARN("failed to assign ddl data", K(ret));
  } else if (OB_FAIL(autoinc_seq_.assign(param.autoinc_seq_))) {
    LOG_WARN("failed to assign autoinc seq", K(ret));
  } else if (OB_FAIL(ha_status_.init_status_for_ha(param.ha_status_))) {
    LOG_WARN("failed to init status for ha", K(ret));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    data_tablet_id_ = param.data_tablet_id_;
    ref_tablet_id_ = param.ref_tablet_id_;
    create_scn_ = param.create_scn_;
    start_scn_ = param.start_scn_;
    clog_checkpoint_scn_ = param.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = param.ddl_checkpoint_scn_;
    snapshot_version_ = param.snapshot_version_;
    multi_version_start_ = param.multi_version_start_;
    compat_mode_ = param.compat_mode_;
    report_status_ = param.report_status_;
    tx_data_ = param.tx_data_;
    table_store_flag_ = param.table_store_flag_;
    ddl_start_scn_ = param.ddl_start_scn_;
    ddl_commit_scn_ = param.ddl_commit_scn_;
    ddl_snapshot_version_ = param.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = param.max_sync_storage_schema_version_;
    max_serialized_medium_scn_ = param.max_serialized_medium_scn_;
    ddl_execution_id_ = param.ddl_execution_id_;
    ddl_cluster_version_ = param.ddl_cluster_version_;
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::init(
    common::ObIAllocator &allocator,
    const ObTabletMeta &old_tablet_meta)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!old_tablet_meta.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(old_tablet_meta));
  } else if (OB_FAIL(ddl_data_.assign(old_tablet_meta.ddl_data_))) {
    LOG_WARN("failed to assign ddl data", K(ret), K_(old_tablet_meta.ddl_data));
  } else if (OB_FAIL(autoinc_seq_.assign(old_tablet_meta.autoinc_seq_))) {
    LOG_WARN("failed to assign autoinc seq", K(ret));
  } else {
    version_ = TABLET_META_VERSION;
    ls_id_ = old_tablet_meta.ls_id_;
    tablet_id_ = old_tablet_meta.tablet_id_;
    data_tablet_id_ = old_tablet_meta.data_tablet_id_;
    ref_tablet_id_ = old_tablet_meta.ref_tablet_id_;
    create_scn_ = old_tablet_meta.create_scn_;
    start_scn_ = old_tablet_meta.start_scn_;
    clog_checkpoint_scn_ = old_tablet_meta.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = old_tablet_meta.ddl_checkpoint_scn_;
    compat_mode_ = old_tablet_meta.compat_mode_;
    report_status_ = old_tablet_meta.report_status_;
    ha_status_ = old_tablet_meta.ha_status_;
    snapshot_version_ = old_tablet_meta.snapshot_version_;
    multi_version_start_ = old_tablet_meta.multi_version_start_;
    tx_data_ = old_tablet_meta.tx_data_;
    table_store_flag_ = old_tablet_meta.table_store_flag_;
    ddl_start_scn_ = old_tablet_meta.ddl_start_scn_;
    ddl_commit_scn_ = old_tablet_meta.ddl_commit_scn_;
    ddl_snapshot_version_ = old_tablet_meta.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = old_tablet_meta.max_sync_storage_schema_version_;
    max_serialized_medium_scn_ = old_tablet_meta.max_serialized_medium_scn_;
    ddl_execution_id_ = old_tablet_meta.ddl_execution_id_;
    ddl_cluster_version_ = old_tablet_meta.ddl_cluster_version_;
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    reset();
  }
  return ret;
}

int ObTabletMeta::init(
    common::ObIAllocator &allocator,
    const ObTabletMeta &old_tablet_meta,
    const ObTabletTxMultiSourceDataUnit &tx_data,
    const ObTabletBindingInfo &ddl_data,
    const share::ObTabletAutoincSeq &autoinc_seq,
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

    ObTabletTableStoreFlag table_store_flag = old_tablet_meta.table_store_flag_;
    if (!table_store_flag.with_major_sstable()) {
      table_store_flag = OB_ISNULL(tablet_meta) ? table_store_flag : tablet_meta->table_store_flag_;
    }

    if (OB_NOT_NULL(tablet_meta) && tablet_meta->clog_checkpoint_scn_ > old_tablet_meta.clog_checkpoint_scn_) {
      if (OB_FAIL(autoinc_seq_.assign(tablet_meta->autoinc_seq_))) {
        LOG_WARN("failed to assign autoinc seq", K(ret));
      } else if (OB_FAIL(ddl_data_.assign(tablet_meta->ddl_data_))) {
        LOG_WARN("failed to assign ddl data", K(ret));
      } else {
        tx_data_ = tablet_meta->tx_data_;
      }
    } else {
      if (OB_FAIL(autoinc_seq_.assign(autoinc_seq))) {
        LOG_WARN("failed to assign autoinc seq", K(ret));
      } else if (OB_FAIL(ddl_data_.assign(ddl_data))) {
        LOG_WARN("failed to assign ddl data", K(ret));
      } else {
        tx_data_ = tx_data;
      }
    }

    version_ = TABLET_META_VERSION;
    ls_id_ = old_tablet_meta.ls_id_;
    tablet_id_ = old_tablet_meta.tablet_id_;
    data_tablet_id_ = old_tablet_meta.data_tablet_id_;
    ref_tablet_id_ = old_tablet_meta.ref_tablet_id_;
    create_scn_ = old_tablet_meta.create_scn_;
    start_scn_ = old_tablet_meta.start_scn_;
    clog_checkpoint_scn_ = clog_checkpoint_scn;
    snapshot_version_ = snapshot_version;
    multi_version_start_ = multi_version_start;
    compat_mode_ = old_tablet_meta.compat_mode_;
    ha_status_ = old_tablet_meta.ha_status_;
    report_status_ = old_tablet_meta.report_status_; //old tablet meta report status already reset
    table_store_flag_ = table_store_flag;
    ddl_checkpoint_scn_ = old_tablet_meta.ddl_checkpoint_scn_;
    ddl_start_scn_ = old_tablet_meta.ddl_start_scn_;
    ddl_commit_scn_ = old_tablet_meta.ddl_commit_scn_;
    ddl_snapshot_version_ = old_tablet_meta.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = max_sync_storage_schema_version;
    max_serialized_medium_scn_ = MAX(old_tablet_meta.max_serialized_medium_scn_,
        OB_ISNULL(tablet_meta) ? 0 : tablet_meta->max_serialized_medium_scn_);
    ddl_execution_id_ = old_tablet_meta.ddl_execution_id_;
    ddl_cluster_version_ = old_tablet_meta.ddl_cluster_version_;

    if (OB_SUCC(ret)) {
      is_inited_ = true;
    }
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
  start_scn_.reset();
  clog_checkpoint_scn_.reset();
  ddl_checkpoint_scn_.set_min();
  snapshot_version_ = OB_INVALID_TIMESTAMP;
  multi_version_start_ = OB_INVALID_TIMESTAMP;
  compat_mode_ = lib::Worker::CompatMode::INVALID;
  autoinc_seq_.reset();
  ha_status_.reset();
  report_status_.reset();
  tx_data_.reset();
  ddl_data_.reset();
  table_store_flag_.reset();
  ddl_start_scn_.set_min();
  ddl_commit_scn_.set_min();
  ddl_snapshot_version_ = OB_INVALID_TIMESTAMP;
  max_sync_storage_schema_version_ = 0;
  max_serialized_medium_scn_ = 0;
  ddl_execution_id_ = -1;
  ddl_cluster_version_ = 0;
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
      && compat_mode_ != lib::Worker::CompatMode::INVALID
      && max_sync_storage_schema_version_ >= 0
      && max_serialized_medium_scn_ >= 0
      && ha_status_.is_valid()
      && (ha_status_.is_restore_status_pending()
          || (!ha_status_.is_restore_status_pending()
              && clog_checkpoint_scn_ >= INIT_CLOG_CHECKPOINT_SCN
              && start_scn_ >= INIT_CLOG_CHECKPOINT_SCN
              && start_scn_ <= clog_checkpoint_scn_));
}

int ObTabletMeta::serialize(char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;

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
  } else if (FALSE_IT(length_ = get_serialize_size())) {
    // do nothing
  } else if (OB_UNLIKELY(length_ > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length_), K(len - new_pos));
  } else if (OB_FAIL(serialization::encode_i32(buf, len, new_pos, version_))) {
    LOG_WARN("failed to serialize tablet meta's version", K(ret), K(len), K(new_pos), K_(version));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i32(buf, len, new_pos, length_))) {
    LOG_WARN("failed to serialize tablet meta's length", K(ret), K(len), K(new_pos), K_(length));
  } else if (new_pos - pos < length_ && OB_FAIL(ls_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ls id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(data_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize data tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(ref_tablet_id_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ref tablet id", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_bool(buf, len, new_pos, has_next_tablet_))) {
    LOG_WARN("failed to serialize has next tablet", K(ret), K(len), K(new_pos), K_(has_next_tablet));
  } else if (new_pos - pos < length_ && OB_FAIL(create_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize create scn", K(ret), K(len), K(new_pos), K_(create_scn));
  } else if (new_pos - pos < length_ && OB_FAIL(start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize start scn", K(ret), K(len), K(new_pos), K_(start_scn));
  } else if (new_pos - pos < length_ && OB_FAIL(clog_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize clog checkpoint ts", K(ret), K(len), K(new_pos), K_(clog_checkpoint_scn));
  } else if (new_pos - pos < length_ && OB_FAIL(ddl_checkpoint_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl checkpoint ts", K(ret), K(len), K(new_pos), K_(ddl_checkpoint_scn));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i64(buf, len, new_pos, snapshot_version_))) {
    LOG_WARN("failed to serialize snapshot version", K(ret), K(len), K(new_pos), K_(snapshot_version));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i64(buf, len, new_pos, multi_version_start_))) {
    LOG_WARN("failed to serialize multi version start", K(ret), K(len), K(new_pos), K_(multi_version_start));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i8(buf, len, new_pos, static_cast<int8_t>(compat_mode_)))) {
    LOG_WARN("failed to serialize auto inc seq", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(autoinc_seq_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize auto inc seq", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(ha_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ha status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(report_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize report status", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(tx_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize multi source data", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(ddl_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl data", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(table_store_flag_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize table store flag", K(ret), K(len), K(new_pos));
  } else if (new_pos - pos < length_ && OB_FAIL(ddl_start_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl start log ts", K(ret), K(len), K(new_pos), K_(ddl_start_scn));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_snapshot_version_))) {
    LOG_WARN("failed to serialize ddl snapshot version", K(ret), K(len), K(new_pos), K_(ddl_snapshot_version));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_sync_storage_schema_version_))) {
    LOG_WARN("failed to serialize max_sync_storage_schema_version", K(ret), K(len), K(new_pos), K_(max_sync_storage_schema_version));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_execution_id_))) {
    LOG_WARN("failed to serialize ddl execution id", K(ret), K(len), K(new_pos), K_(ddl_execution_id));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_cluster_version_))) {
    LOG_WARN("failed to serialize ddl cluster version", K(ret), K(len), K(new_pos), K_(ddl_cluster_version));
  } else if (new_pos - pos < length_ && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_serialized_medium_scn_))) {
    LOG_WARN("failed to serialize max_serialized_medium_scn", K(ret), K(len), K(new_pos), K_(max_serialized_medium_scn));
  } else if (new_pos - pos < length_ && OB_FAIL(ddl_commit_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl commit scn", K(ret), K(len), K(new_pos), K_(ddl_commit_scn));
  } else if (OB_UNLIKELY(length_ != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet meta's length doesn't match standard length", K(ret), K(new_pos), K(pos), K_(length));
  } else {
    pos = new_pos;
  }

  return ret;
}

int ObTabletMeta::deserialize(
    common::ObIAllocator &allocator,
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
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&version_))) {
    LOG_WARN("failed to deserialize tablet meta's version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i32(buf, len, new_pos, (int32_t *)&length_))) {
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
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_bool(buf, len, new_pos, (bool *)&has_next_tablet_))) {
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
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&compat_mode)))) {
      LOG_WARN("failed to deserialize compat mode", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(autoinc_seq_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize auto inc seq", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ha_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize restore status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(report_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize report status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(tx_data_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize multi source data", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ddl_data_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl data", K(ret), K(len), K(new_pos));
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
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_cluster_version_))) {
      LOG_WARN("failed to deserialize ddl cluster version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_serialized_medium_scn_))) {
      LOG_WARN("failed to deserialize max_serialized_medium_scn", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length_ && OB_FAIL(ddl_commit_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl commit scn", K(ret), K(len), K(new_pos));
    } else if (OB_UNLIKELY(length_ != new_pos - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K_(length));
    } else {
      pos = new_pos;
      compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
      is_inited_ = true;
    }
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
  size += autoinc_seq_.get_serialize_size();
  size += ha_status_.get_serialize_size();
  size += report_status_.get_serialize_size();
  size += tx_data_.get_serialize_size();
  size += ddl_data_.get_serialize_size();
  size += table_store_flag_.get_serialize_size();
  size += ddl_start_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(ddl_snapshot_version_);
  size += serialization::encoded_length_i64(max_sync_storage_schema_version_);
  size += serialization::encoded_length_i64(ddl_execution_id_);
  size += serialization::encoded_length_i64(ddl_cluster_version_);
  size += serialization::encoded_length_i64(max_serialized_medium_scn_);
  size += ddl_commit_scn_.get_fixed_serialize_size();
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
    report_status.data_checksum_ = sstable->get_meta().get_basic_meta().data_checksum_;
    report_status.row_count_ = sstable->get_meta().get_basic_meta().row_count_;
  }
  return ret;
}

int ObTabletMeta::update(const ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  ObTabletRestoreStatus::STATUS change_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  ObTabletRestoreStatus::STATUS current_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  bool can_change = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(param));
  } else if (OB_FAIL(param.ha_status_.get_restore_status(change_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(param));
  } else if (OB_FAIL(ha_status_.get_restore_status(current_status))) {
    LOG_WARN("failed to get restore status", K(ret), K(ha_status_));
  } else if (OB_FAIL(ObTabletRestoreStatus::check_can_change_status(current_status, change_status, can_change))) {
    LOG_WARN("failed to check can change restore status", K(ret), K(ha_status_), K(change_status), K(param));
  } else if (!can_change) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("can not change restore status", K(ret), K(param), K(ha_status_), K(change_status));
  } else if (OB_FAIL(ha_status_.set_restore_status(change_status))) {
    LOG_WARN("failed to set restore status", K(ret), K(param));
  } else if (OB_FAIL(autoinc_seq_.assign(param.autoinc_seq_))) {
    LOG_WARN("failed to assign autoinc seq", K(ret), K(param));
  } else {
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    data_tablet_id_ = param.data_tablet_id_;
    ref_tablet_id_ = param.ref_tablet_id_;
    create_scn_ = param.create_scn_;
    start_scn_ = param.start_scn_;
    clog_checkpoint_scn_ = param.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = param.ddl_checkpoint_scn_;
    table_store_flag_ = param.table_store_flag_;
    ddl_start_scn_ = param.ddl_start_scn_;
    ddl_commit_scn_ = param.ddl_commit_scn_;
    ddl_snapshot_version_ = param.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = param.max_sync_storage_schema_version_;
    max_serialized_medium_scn_ = param.max_serialized_medium_scn_;
    ddl_execution_id_ = param.ddl_execution_id_;
    ddl_cluster_version_ = param.ddl_cluster_version_;
  }

  return ret;
}

SCN ObTabletMeta::get_ddl_sstable_start_scn() const
{
  return ddl_start_scn_.is_valid_and_not_min () ? share::SCN::scn_dec(ddl_start_scn_) : ddl_start_scn_;
}

int ObTabletMeta::update_create_scn(const SCN create_scn)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(create_scn));
  } else {
    create_scn_ = create_scn;
  }

  return ret;
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
    //do nohting
  } else if (OB_FAIL(old_tablet_meta.ls_id_ != tablet_meta->ls_id_
      || old_tablet_meta.tablet_id_ != tablet_meta->tablet_id_
      || old_tablet_meta.data_tablet_id_ != tablet_meta->data_tablet_id_
      || old_tablet_meta.ref_tablet_id_ != tablet_meta->ref_tablet_id_
      || old_tablet_meta.compat_mode_ != tablet_meta->compat_mode_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet meta part variable is not same with migration tablet param",
        K(ret), K(old_tablet_meta), KPC(tablet_meta));
  }
  return ret;
}


ObMigrationTabletParam::ObMigrationTabletParam()
  : magic_number_(MAGIC_NUM),
    version_(PARAM_VERSION),
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
    autoinc_seq_(),
    ha_status_(),
    report_status_(),
    tx_data_(),
    ddl_data_(),
    storage_schema_(),
    medium_info_list_(),
    table_store_flag_(),
    ddl_start_scn_(SCN::min_scn()),
    ddl_snapshot_version_(OB_INVALID_TIMESTAMP),
    max_sync_storage_schema_version_(0),
    ddl_execution_id_(-1),
    ddl_cluster_version_(0),
    max_serialized_medium_scn_(0),
    ddl_commit_scn_(SCN::min_scn()),
    allocator_()
{
}

bool ObMigrationTabletParam::is_valid() const
{
  return MAGIC_NUM == magic_number_
      && PARAM_VERSION == version_
      && ls_id_.is_valid()
      && tablet_id_.is_valid()
      && data_tablet_id_.is_valid()
      && create_scn_ != ObTabletMeta::INVALID_CREATE_SCN
      && multi_version_start_ >= 0
      && multi_version_start_ <= snapshot_version_
      && compat_mode_ != lib::Worker::CompatMode::INVALID
      && max_sync_storage_schema_version_ >= 0
      && max_serialized_medium_scn_ >= 0
      && ha_status_.is_valid()
      && (ha_status_.is_restore_status_pending()
          || (start_scn_ >= SCN::base_scn()
              && clog_checkpoint_scn_ >= SCN::base_scn()
              && start_scn_ <= clog_checkpoint_scn_
              && storage_schema_.is_valid()
              && medium_info_list_.is_valid()));
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
  } else if (new_pos - pos < length && OB_FAIL(autoinc_seq_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize auto inc seq", K(ret), K(len), K(new_pos), K_(autoinc_seq));
  } else if (new_pos - pos < length && OB_FAIL(ha_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ha status", K(ret), K(len), K(new_pos), K_(ha_status));
  } else if (new_pos - pos < length && OB_FAIL(report_status_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize report status", K(ret), K(len), K(new_pos), K_(report_status));
  } else if (new_pos - pos < length && OB_FAIL(tx_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize multi source data", K(ret), K(len), K(new_pos), K_(tx_data));
  } else if (new_pos - pos < length && OB_FAIL(ddl_data_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl data", K(ret), K(len), K(new_pos), K_(ddl_data));
  } else if (new_pos - pos < length && OB_FAIL(storage_schema_.serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize storage schema", K(ret), K(len), K(new_pos), K_(storage_schema));
  } else if (new_pos - pos < length && OB_FAIL(medium_info_list_.serialize(buf, len, new_pos))) {
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
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, ddl_cluster_version_))) {
    LOG_WARN("failed to serialize ddl cluster version", K(ret), K(len), K(new_pos), K_(ddl_cluster_version));
  } else if (new_pos - pos < length && OB_FAIL(serialization::encode_i64(buf, len, new_pos, max_serialized_medium_scn_))) {
    LOG_WARN("failed to serialize max_serialized_medium_scn", K(ret), K(len), K(new_pos), K_(max_serialized_medium_scn));
  } else if (new_pos - pos < length && OB_FAIL(ddl_commit_scn_.fixed_serialize(buf, len, new_pos))) {
    LOG_WARN("failed to serialize ddl commit scn", K(ret), K(len), K(new_pos), K_(ddl_commit_scn));
  } else if (OB_UNLIKELY(length != new_pos - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length));
  } else {
    pos = new_pos;
  }

  return ret;
}

// old format compatibility to 4.0, DO NOT add any new member deserialization!!!
int ObMigrationTabletParam::deserialize_old(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int8_t compat_mode = -1;
  ddl_execution_id_ = 0;

  if (OB_FAIL(ls_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ls id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize tablet id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(data_tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize data tablet id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(ref_tablet_id_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ref tablet id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(create_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize create scn", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(start_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize start scn", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(clog_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(ddl_checkpoint_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl checkpoint ts", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &snapshot_version_))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &multi_version_start_))) {
    LOG_WARN("failed to deserialize clog checkpoint ts", K(ret), K(len));
  } else if (OB_FAIL(serialization::decode_i8(buf, len, new_pos, (int8_t*)(&compat_mode)))) {
    LOG_WARN("failed to deserialize compat mode", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(autoinc_seq_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize auto inc seq", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(ha_status_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ha status", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(report_status_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize report status", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(tx_data_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize multi source data", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(ddl_data_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl data", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(storage_schema_.deserialize(allocator_, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize storage schema", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(medium_info_list_.deserialize(allocator_, buf, len, new_pos))) {
    LOG_WARN("failed to deserialize medium compaction list", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(table_store_flag_.deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize table store flag", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(ddl_start_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl start log ts", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_snapshot_version_))) {
    LOG_WARN("failed to deserialize ddl snapshot version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_sync_storage_schema_version_))) {
    LOG_WARN("failed to deserialize max sync storage schema version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_execution_id_))) {
    LOG_WARN("failed to deserialize ddl execution id", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_cluster_version_))) {
    LOG_WARN("failed to deserialize ddl cluster version", K(ret), K(len), K(new_pos));
  } else if (OB_FAIL(ddl_commit_scn_.fixed_deserialize(buf, len, new_pos))) {
    LOG_WARN("failed to deserialize ddl commit scn", K(ret), K(len), K(new_pos));
  } else {
    // old format compatibility to 4.0, DO NOT add any new member deserialization!!!
    compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
    pos = new_pos;
  }

  return ret;
}

int ObMigrationTabletParam::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int8_t compat_mode = -1;
  int64_t length = 0;
  ddl_execution_id_ = 0;

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
    } else if (OB_FAIL(serialization::decode_i64(buf, len, new_pos, &length))) {
      LOG_WARN("failed to deserialize length", K(ret), K(len), K(new_pos));
    } else if (OB_UNLIKELY(PARAM_VERSION != version_)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid version", K(ret), K_(version));
    } else if (OB_UNLIKELY(length > len - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - new_pos));
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
    } else if (new_pos - pos < length && OB_FAIL(autoinc_seq_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize auto inc seq", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length && OB_FAIL(ha_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ha status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length && OB_FAIL(report_status_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize report status", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length && OB_FAIL(tx_data_.deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize multi source data", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length && OB_FAIL(ddl_data_.deserialize(buf, len, new_pos))) {
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
    } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &ddl_cluster_version_))) {
      LOG_WARN("failed to deserialize ddl cluster version", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length && OB_FAIL(serialization::decode_i64(buf, len, new_pos, &max_serialized_medium_scn_))) {
      LOG_WARN("failed to deserialize max sync medium snapshot", K(ret), K(len), K(new_pos));
    } else if (new_pos - pos < length && OB_FAIL(ddl_commit_scn_.fixed_deserialize(buf, len, new_pos))) {
      LOG_WARN("failed to deserialize ddl commit scn", K(ret), K(len), K(new_pos));
    } else if (OB_UNLIKELY(length != new_pos - pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet's length doesn't match standard length", K(ret), K(new_pos), K(pos), K(length), KPC(this));
    } else {
      compat_mode_ = static_cast<lib::Worker::CompatMode>(compat_mode);
      pos = new_pos;
    }
  } else { // old format without length and version
    magic_number_ = MAGIC_NUM;
    version_ = PARAM_VERSION;
    if (OB_FAIL(deserialize_old(buf, len, pos))) {
      LOG_WARN("failed to deserialize old format", K(ret), K(len), K(pos));
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
  size += autoinc_seq_.get_serialize_size();
  size += ha_status_.get_serialize_size();
  size += report_status_.get_serialize_size();
  size += tx_data_.get_serialize_size();
  size += ddl_data_.get_serialize_size();
  size += storage_schema_.get_serialize_size();
  size += medium_info_list_.get_serialize_size();
  size += table_store_flag_.get_serialize_size();
  size += ddl_start_scn_.get_fixed_serialize_size();
  size += serialization::encoded_length_i64(ddl_snapshot_version_);
  size += serialization::encoded_length_i64(max_sync_storage_schema_version_);
  size += serialization::encoded_length_i64(ddl_execution_id_);
  size += serialization::encoded_length_i64(ddl_cluster_version_);
  size += serialization::encoded_length_i64(max_serialized_medium_scn_);
  size += ddl_commit_scn_.get_fixed_serialize_size();
  return size;
}

void ObMigrationTabletParam::reset()
{
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
  autoinc_seq_.reset();
  ha_status_.reset();
  report_status_.reset();
  tx_data_.reset();
  ddl_data_.reset();
  storage_schema_.reset();
  medium_info_list_.reset();
  table_store_flag_.reset();
  ddl_start_scn_.set_min();
  ddl_snapshot_version_ = OB_INVALID_TIMESTAMP;
  max_sync_storage_schema_version_ = 0;
  ddl_execution_id_ = -1;
  ddl_cluster_version_ = 0;
  max_serialized_medium_scn_ = 0;
  ddl_commit_scn_.set_min();
  allocator_.reset();
}

int ObMigrationTabletParam::assign(const ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("migration tablet param is invalid", K(ret), K(param));
  } else {
    ls_id_ = param.ls_id_;
    tablet_id_ = param.tablet_id_;
    data_tablet_id_ = param.data_tablet_id_;
    ref_tablet_id_ = param.ref_tablet_id_;
    create_scn_ = param.create_scn_;
    start_scn_ = param.start_scn_;
    clog_checkpoint_scn_ = param.clog_checkpoint_scn_;
    ddl_checkpoint_scn_ = param.ddl_checkpoint_scn_;
    snapshot_version_ = param.snapshot_version_;
    multi_version_start_ = param.multi_version_start_;
    compat_mode_ = param.compat_mode_;
    ha_status_ = param.ha_status_;
    report_status_ = param.report_status_;
    tx_data_ = param.tx_data_;
    storage_schema_.reset(); // TODO: refactor?
    medium_info_list_.reset();
    table_store_flag_ = param.table_store_flag_;
    ddl_start_scn_ = param.ddl_start_scn_;
    ddl_snapshot_version_ = param.ddl_snapshot_version_;
    max_sync_storage_schema_version_ = param.max_sync_storage_schema_version_;
    max_serialized_medium_scn_ = param.max_serialized_medium_scn_;
    ddl_execution_id_ = param.ddl_execution_id_;
    ddl_cluster_version_ = param.ddl_cluster_version_;
    ddl_commit_scn_ = param.ddl_commit_scn_;

    if (OB_FAIL(ddl_data_.assign(param.ddl_data_))) {
      LOG_WARN("failed to assign ddl data", K(ret), K(param), K(*this));
    } else if (OB_FAIL(autoinc_seq_.assign(param.autoinc_seq_))) {
      LOG_WARN("failed to assign autoinc seq", K(ret), K(param), K(*this));
    } else if (OB_FAIL(storage_schema_.init(allocator_, param.storage_schema_))) {
      LOG_WARN("failed to assign storage schema", K(ret), K(param));
    } else if (OB_FAIL(medium_info_list_.init(allocator_, &param.medium_info_list_))) {
      LOG_WARN("failed to assign medium info list", K(ret), K(param));
    }
  }
  return ret;
}

int ObMigrationTabletParam::construct_placeholder_storage_schema_and_medium(
    ObIAllocator &allocator,
    ObStorageSchema &storage_schema,
    compaction::ObMediumCompactionInfoList &medium_info_list)
{
  int ret = OB_SUCCESS;
  storage_schema.reset();

  storage_schema.allocator_ = &allocator;
  storage_schema.rowkey_array_.set_allocator(&allocator);
  storage_schema.column_array_.set_allocator(&allocator);

  storage_schema.storage_schema_version_ = ObStorageSchema::STORAGE_SCHEMA_VERSION;
  storage_schema.is_use_bloomfilter_ = false;
  storage_schema.table_type_ = ObTableType::USER_TABLE;
  //storage_schema.table_mode_
  storage_schema.index_type_ = ObIndexType::INDEX_TYPE_PRIMARY;
  storage_schema.index_status_ = ObIndexStatus::INDEX_STATUS_AVAILABLE;
  storage_schema.row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
  storage_schema.schema_version_ = ObStorageSchema::STORAGE_SCHEMA_VERSION;
  storage_schema.column_cnt_ = 1;
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

  if (OB_SUCC(ret) && OB_UNLIKELY(!storage_schema.is_valid() || !medium_info_list.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("placeholder storage schema or medium info list is not valid", K(ret), K(storage_schema), K(medium_info_list));
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
