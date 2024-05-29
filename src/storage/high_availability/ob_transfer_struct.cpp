/**
 * Copyright (c) 2022 OceanBase
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
#include "ob_transfer_struct.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "src/storage/tablet/ob_tablet_meta.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/tx/ob_ts_mgr.h"
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"

using namespace oceanbase;
using namespace share;
using namespace storage;


ObTXStartTransferOutInfo::ObTXStartTransferOutInfo()
  : src_ls_id_(),
    dest_ls_id_(),
    tablet_list_(),
    task_id_(),
    data_end_scn_(),
    transfer_epoch_(0),
    data_version_(DEFAULT_MIN_DATA_VERSION),
    filter_tx_need_transfer_(false),
    move_tx_ids_()
{
}

void ObTXStartTransferOutInfo::reset()
{
  src_ls_id_.reset();
  dest_ls_id_.reset();
  tablet_list_.reset();
  task_id_.reset();
  data_end_scn_.reset();
  transfer_epoch_ = 0;
  data_version_ = 0;
  filter_tx_need_transfer_ = false;
  move_tx_ids_.reset();
}

bool ObTXStartTransferOutInfo::is_valid() const
{
  return src_ls_id_.is_valid()
    && dest_ls_id_.is_valid()
    && !tablet_list_.empty()
    && data_version_ > 0;
}

int ObTXStartTransferOutInfo::assign(const ObTXStartTransferOutInfo &start_transfer_out_info)
{
  int ret = OB_SUCCESS;
  if (!start_transfer_out_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign start transfer out info get invalid argument", K(ret), K(start_transfer_out_info));
  } else if (OB_FAIL(tablet_list_.assign(start_transfer_out_info.tablet_list_))) {
    LOG_WARN("failed to assign start transfer out info", K(ret), K(start_transfer_out_info));
  } else if (OB_FAIL(move_tx_ids_.assign(start_transfer_out_info.move_tx_ids_))) {
    LOG_WARN("failed to assign move_tx_ids", K(ret), K(start_transfer_out_info));
  } else {
    src_ls_id_ = start_transfer_out_info.src_ls_id_;
    dest_ls_id_ = start_transfer_out_info.dest_ls_id_;
    task_id_ = start_transfer_out_info.task_id_;
    data_end_scn_ = start_transfer_out_info.data_end_scn_;
    transfer_epoch_ = start_transfer_out_info.transfer_epoch_;
    data_version_ = start_transfer_out_info.data_version_;
    filter_tx_need_transfer_ = start_transfer_out_info.filter_tx_need_transfer_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTXStartTransferOutInfo, src_ls_id_, dest_ls_id_, tablet_list_, task_id_,
    data_end_scn_, transfer_epoch_, data_version_, filter_tx_need_transfer_, move_tx_ids_);

ObTXStartTransferInInfo::ObTXStartTransferInInfo()
  : src_ls_id_(),
    dest_ls_id_(),
    start_scn_(),
    tablet_meta_list_(),
    task_id_(),
    data_version_(DEFAULT_MIN_DATA_VERSION)
{
}

void ObTXStartTransferInInfo::reset()
{
  src_ls_id_.reset();
  dest_ls_id_.reset();
  start_scn_.reset();
  tablet_meta_list_.reset();
  task_id_.reset();
  data_version_ = 0;
}

bool ObTXStartTransferInInfo::is_valid() const
{
  return src_ls_id_.is_valid()
      && dest_ls_id_.is_valid()
      && start_scn_.is_valid()
      && !tablet_meta_list_.empty()
      && data_version_ > 0;
}

int ObTXStartTransferInInfo::assign(const ObTXStartTransferInInfo &start_transfer_in_info)
{
  int ret = OB_SUCCESS;
  if (!start_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign start transfer in info get invalid argument", K(ret), K(start_transfer_in_info));
  } else if (OB_FAIL(tablet_meta_list_.assign(start_transfer_in_info.tablet_meta_list_))) {
    LOG_WARN("failed to assign start transfer out info", K(ret), K(start_transfer_in_info));
  } else {
    src_ls_id_ = start_transfer_in_info.src_ls_id_;
    dest_ls_id_ = start_transfer_in_info.dest_ls_id_;
    start_scn_ = start_transfer_in_info.start_scn_;
    task_id_ = start_transfer_in_info.task_id_;
    data_version_ = start_transfer_in_info.data_version_;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObTXStartTransferInInfo, src_ls_id_, dest_ls_id_, start_scn_, tablet_meta_list_, task_id_, data_version_);

/* ObTXFinishTransferInInfo */

OB_SERIALIZE_MEMBER(ObTXFinishTransferInInfo, src_ls_id_, dest_ls_id_, start_scn_, tablet_list_, task_id_, data_version_);
ObTXFinishTransferInInfo::ObTXFinishTransferInInfo()
  : src_ls_id_(),
    dest_ls_id_(),
    start_scn_(),
    tablet_list_(),
    task_id_(),
    data_version_(DEFAULT_MIN_DATA_VERSION)
{
}

void ObTXFinishTransferInInfo::reset()
{
  src_ls_id_.reset();
  dest_ls_id_.reset();
  start_scn_.reset();
  tablet_list_.reset();
  task_id_.reset();
  data_version_ = 0;
}

bool ObTXFinishTransferInInfo::is_valid() const
{
  return src_ls_id_.is_valid()
      && dest_ls_id_.is_valid()
      && start_scn_.is_valid()
      && !tablet_list_.empty()
      && data_version_ > 0;
}

int ObTXFinishTransferInInfo::assign(const ObTXFinishTransferInInfo &finish_transfer_in_info)
{
  int ret = OB_SUCCESS;
  if (!finish_transfer_in_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign finish transfer in info get invalid argument", K(ret), K(finish_transfer_in_info));
  } else if (OB_FAIL(tablet_list_.assign(finish_transfer_in_info.tablet_list_))) {
    LOG_WARN("failed to assign finish transfer in info", K(ret), K(finish_transfer_in_info));
  } else {
    src_ls_id_ = finish_transfer_in_info.src_ls_id_;
    dest_ls_id_ = finish_transfer_in_info.dest_ls_id_;
    start_scn_ = finish_transfer_in_info.start_scn_;
    task_id_ = finish_transfer_in_info.task_id_;
    data_version_ = finish_transfer_in_info.data_version_;
  }
  return ret;
}

/* ObTXFinishTransferOutInfo */
OB_SERIALIZE_MEMBER(ObTXFinishTransferOutInfo, src_ls_id_, dest_ls_id_, finish_scn_, tablet_list_, task_id_, data_version_);
ObTXFinishTransferOutInfo::ObTXFinishTransferOutInfo()
  : src_ls_id_(),
    dest_ls_id_(),
    finish_scn_(),
    tablet_list_(),
    task_id_(),
    data_version_(DEFAULT_MIN_DATA_VERSION)
{
}
void ObTXFinishTransferOutInfo::reset()
{
  src_ls_id_.reset();
  dest_ls_id_.reset();
  finish_scn_.reset();
  tablet_list_.reset();
  task_id_.reset();
  data_version_ = 0;
}
bool ObTXFinishTransferOutInfo::is_valid() const
{
  return src_ls_id_.is_valid()
      && dest_ls_id_.is_valid()
      && finish_scn_.is_valid()
      && !tablet_list_.empty()
      && data_version_ > 0;
}

int ObTXFinishTransferOutInfo::assign(const ObTXFinishTransferOutInfo &finish_transfer_out_info)
{
  int ret = OB_SUCCESS;
  if (!finish_transfer_out_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign finish transfer out info get invalid argument", K(ret), K(finish_transfer_out_info));
  } else if (OB_FAIL(tablet_list_.assign(finish_transfer_out_info.tablet_list_))) {
    LOG_WARN("failed to assign finish transfer out info", K(ret), K(finish_transfer_out_info));
  } else {
    src_ls_id_ = finish_transfer_out_info.src_ls_id_;
    dest_ls_id_ = finish_transfer_out_info.dest_ls_id_;
    finish_scn_ = finish_transfer_out_info.finish_scn_;
    task_id_ = finish_transfer_out_info.task_id_;
    data_version_ = finish_transfer_out_info.data_version_;
  }
  return ret;
}

void ObTransferEventRecorder::record_transfer_task_event(
    const share::ObTransferTaskID &task_id,
    const char *event_name,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id)
{
  SERVER_EVENT_ADD("TRANSFER", "REGISTER_MULTI_DATA_SOURCE",
      "event", event_name,
      "task_id", task_id,
      "src_ls_id", src_ls_id.id(),
      "dest_ls_id", dest_ls_id.id());
}

void ObTransferEventRecorder::record_ls_transfer_event(
    const char *event_name,
    const transaction::NotifyType &type,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const bool for_replay,
    const share::SCN &scn,
    const int64_t result)
{
  const char *type_str = NULL;
  if (transaction::NotifyType::REGISTER_SUCC == type) {
    type_str = "REGISTER_SUCC";
  } else if (transaction::NotifyType::ON_REDO == type) {
    type_str = "ON_REDO";
  } else if (transaction::NotifyType::ON_COMMIT == type) {
    type_str = "ON_COMMIT";
  } else if (transaction::NotifyType::ON_ABORT == type) {
    type_str = "ON_ABORT";
  } else if (transaction::NotifyType::TX_END == type) {
    type_str = "TX_END";
  } else if (transaction::NotifyType::ON_PREPARE == type) {
    type_str = "ON_PREPARE";
  } else {
    type_str = "UNKNOWN";
  }
  SERVER_EVENT_ADD("TRANSFER", event_name,
      "TYPE", type_str,
      "src_ls_id", src_ls_id.id(),
      "dest_ls_id", dest_ls_id.id(),
      "for_replay", for_replay,
      "scn", scn.get_val_for_inner_table_field(),
      "result", result);
}

void ObTransferEventRecorder::record_tablet_transfer_event(
    const char *event_name,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const int64_t transfer_seq,
    const ObTabletStatus &tablet_status,
    const int64_t result)
{
  SERVER_EVENT_ADD("TRANSFER", "TABLET_STATUS_CHANGE",
        "event_name", event_name,
        "tablet_status", ObTabletStatus::get_str(tablet_status),
        "ls_id", ls_id.id(),
        "tablet_id", tablet_id.id(),
        "transfer_seq", transfer_seq,
        "result", result);
}

void ObTransferEventRecorder::record_advance_transfer_status_event(
    const uint64_t tenant_id,
    const share::ObTransferTaskID &task_id,
    const share::ObLSID &src_ls_id,
    const share::ObLSID &dest_ls_id,
    const share::ObTransferStatus &next_status,
    const int64_t result)
{
  SERVER_EVENT_ADD("TRANSFER", "UPDATE_TRANSFER_STATUS",
        "tenant_id", tenant_id,
        "task_id", task_id.id(),
        "src_ls_id", src_ls_id.id(),
        "dest_ls_id", dest_ls_id.id(),
        "next_status", next_status.str(),
        "result", result);
}

int ObTXTransferUtils::get_tablet_status(
    const bool get_commit,
    ObTabletHandle &tablet_handle,
    ObTabletCreateDeleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  user_data.reset();
  ObTablet *tablet = nullptr;

  if (!tablet_handle.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablet status get invalid argument", K(ret), K(tablet_handle));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet), K(tablet_handle));
  } else if (OB_FAIL(get_tablet_status_(get_commit, tablet, user_data))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(tablet));
  }
  return ret;
}

int ObTXTransferUtils::get_tablet_status(
    const bool get_commit,
    const ObTablet *tablet,
    ObTabletCreateDeleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(tablet)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablet status get invalid argument", K(ret), KP(tablet));
  } else if (OB_FAIL(get_tablet_status_(get_commit, tablet, user_data))) {
    LOG_WARN("failed to get tablet status", K(ret), KPC(tablet));
  }
  return ret;
}

int ObTXTransferUtils::get_tablet_status_(
   const bool get_commit,
   const ObTablet *tablet,
   ObTabletCreateDeleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  bool unused_committed_flag = false;
  if (get_commit) {
    if (OB_FAIL(tablet->ObITabletMdsInterface::get_tablet_status(share::SCN::max_scn(), user_data, ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US))) {
      LOG_WARN("failed to get committed tablet status", K(ret), KPC(tablet), K(user_data));
    }
  } else {
    if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(user_data, unused_committed_flag))) {
      LOG_WARN("failed to get latest tablet status", K(ret), KPC(tablet), K(user_data));
    }
  }
  return ret;
}

// TODO(wenjinyu.wjy) (4.3)It needs to be added to trigger the tablet freezing operation
int ObTXTransferUtils::set_tablet_freeze_flag(storage::ObLS &ls, ObTablet *tablet)
{
  MDS_TG(10_ms);
  int ret = OB_SUCCESS;
  ObArray<ObTableHandleV2> memtables;
  ObTabletID tablet_id = tablet->get_tablet_meta().tablet_id_;
  SCN weak_read_scn;
  share::ObLSID ls_id = ls.get_ls_id();

  if (OB_ISNULL(tablet) ) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), KP(tablet));
  } else if (FALSE_IT(weak_read_scn = ls.get_ls_wrs_handler()->get_ls_weak_read_ts())) {
  } else if (!weak_read_scn.is_valid()
      || ObScnRange::MAX_SCN == weak_read_scn) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("weak read scn is invalid", K(ret), K(ls_id), K(weak_read_scn));
  } else if (ObScnRange::MIN_SCN == weak_read_scn) {
    ret = OB_EAGAIN;
    LOG_WARN("weak read service not inited, need to wait for weak read scn to advance", K(ret), K(ls_id), K(weak_read_scn));
  } else if (OB_FAIL(tablet->get_all_memtables(memtables))) {
    LOG_WARN("failed to get_memtable_mgr for get all memtable", K(ret), KPC(tablet));
  } else {
    CLICK();
    ObITabletMemtable *mt = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < memtables.count(); ++i) {
      if (OB_UNLIKELY(memtables.at(i).get_tablet_memtable(mt))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table in tables_handle is not memtable", K(ret), K(memtables.at(i)));
      } else if (!mt->is_active_memtable()) {
        // skip
      } else if (OB_UNLIKELY(!mt->is_data_memtable())) {
        // incremental direct load hold table lock will block transfer scheduling, so there will be no active direct load memtable
        ret = OB_TRANSFER_SYS_ERROR;
        LOG_WARN("memtable is not data memtable", K(ret), KPC(mt));
      } else {
        memtable::ObMemtable *memtable = static_cast<memtable::ObMemtable *>(mt);
        memtable->set_transfer_freeze(weak_read_scn);
      }
    }
    if (OB_SUCC(ret)) {
      LOG_INFO("succ set transfer freeze", K(tablet_id), K(ls_id));
    }
  }

  return ret;
}

int ObTXTransferUtils::create_empty_minor_sstable(
    const common::ObTabletID &tablet_id,
    const SCN start_scn,
    const SCN end_scn,
    const ObStorageSchema &table_schema,
    common::ObArenaAllocator &allocator,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  ObTabletCreateSSTableParam create_sstable_param;

  if (!tablet_id.is_valid() || !table_schema.is_valid() || !start_scn.is_valid() || !end_scn.is_valid() || start_scn == end_scn) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create empty minor sstable get invalid argument", K(ret), K(table_schema), K(tablet_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(build_empty_minor_sstable_param_(start_scn, end_scn, table_schema,
      tablet_id, create_sstable_param))) {
    LOG_WARN("failed to build empty minor sstable param", K(ret), K(tablet_id), K(start_scn), K(end_scn));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable<ObSSTable>(create_sstable_param, allocator, table_handle))) {
    LOG_WARN("failed to create minor sstable", K(ret), K(create_sstable_param), K(tablet_id));
  } else {
    LOG_INFO("succeed to create empty minor sstable", K(tablet_id), K(table_handle), K(start_scn), K(end_scn));
  }
  return ret;
}

int ObTXTransferUtils::build_empty_minor_sstable_param_(
    const SCN start_scn,
    const SCN end_scn,
    const ObStorageSchema &table_schema,
    const common::ObTabletID &tablet_id,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;

  if (!start_scn.is_valid() || !end_scn.is_valid() || start_scn == end_scn
      || !table_schema.is_valid() || !tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build empty minor sstable param get invalid argument", K(ret), K(table_schema), K(tablet_id), K(start_scn), K(end_scn));
  }else if (OB_FAIL(table_schema.get_encryption_id(param.encrypt_id_))) {
    LOG_WARN("fail to get encryption id", K(ret), K(table_schema));
  } else {
    param.master_key_id_ = table_schema.get_master_key_id();
    MEMCPY(param.encrypt_key_, table_schema.get_encrypt_key_str(), table_schema.get_encrypt_key_len());
    const int64_t multi_version_col_cnt = ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.table_key_.table_type_ = ObITable::TableType::MINOR_SSTABLE;
    param.table_key_.tablet_id_ = tablet_id;
    param.table_key_.scn_range_.start_scn_ = start_scn;
    param.table_key_.scn_range_.end_scn_ = end_scn;
    param.max_merged_trans_version_ = 0;

    param.schema_version_ = table_schema.get_schema_version();
    param.create_snapshot_version_ = 0;
    param.progressive_merge_round_ = table_schema.get_progressive_merge_round();
    param.progressive_merge_step_ = 0;

    param.table_mode_ = table_schema.get_table_mode_struct();
    param.index_type_ = table_schema.get_index_type();
    param.rowkey_column_cnt_ = table_schema.get_rowkey_column_num()
            + ObMultiVersionRowkeyHelpper::get_extra_rowkey_col_cnt();
    param.root_block_addr_.set_none_addr();
    param.data_block_macro_meta_addr_.set_none_addr();
    param.root_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    param.latest_row_store_type_ = ObRowStoreType::FLAT_ROW_STORE;
    param.data_index_tree_height_ = 0;
    param.index_blocks_cnt_ = 0;
    param.data_blocks_cnt_ = 0;
    param.micro_block_cnt_ = 0;
    param.use_old_macro_block_count_ = 0;
    param.column_cnt_ = table_schema.get_column_count() + multi_version_col_cnt;
    param.data_checksum_ = 0;
    param.occupy_size_ = 0;
    param.ddl_scn_.set_min();
    param.filled_tx_scn_ = end_scn;
    param.original_size_ = 0;
    param.compressor_type_ = ObCompressorType::NONE_COMPRESSOR;

    if (!param.is_valid()) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid param", K(ret), K(param));
    }
  }
  return ret;
}

/* ObTransferLockStatus */


const char *ObTransferLockStatus::str() const
{
  const char *str = "INVALID_STATUS";
  switch (status_) {
  case START: {
    str = "START";
    break;
  }
  case DOING: {
    str = "DOING";
    break;
  }
  default: {
    str = "INVALID_STATUS";
  }
  }
  return str;
}

int ObTransferLockStatus::parse_from_str(const ObString &str)
{
  int ret = OB_SUCCESS;
  if (0 == str.case_compare("START")) {
    status_ = START;
  } else if (0 == str.case_compare("DOING")) {
    status_ = DOING;
  } else {
    status_ = MAX_STATUS;
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid transfer status str", KR(ret), K(str));
  }
  return ret;
}

/* ObTransferLockInfoRowKey */

ObTransferLockInfoRowKey::ObTransferLockInfoRowKey() : tenant_id_(), ls_id_()
{}

bool ObTransferLockInfoRowKey::is_valid() const
{
  return ls_id_.is_valid();
}

/* ObTransferTaskLockInfo */

ObTransferTaskLockInfo::ObTransferTaskLockInfo()
    : tenant_id_(), ls_id_(), task_id_(), status_(), lock_owner_(), comment_()
{}

void ObTransferTaskLockInfo::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  task_id_ = 0;
  status_.reset();
  lock_owner_ = 0;
  comment_.reset();
}

bool ObTransferTaskLockInfo::is_valid() const
{
  return OB_INVALID_ID != tenant_id_ && ls_id_.is_valid() && task_id_ >= 0 && status_.is_valid()
      && lock_owner_ > 0;
}

int ObTransferTaskLockInfo::assign(const ObTransferTaskLockInfo &other)
{
  int ret = OB_SUCCESS;
  if (!other.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(other));
  } else {
    tenant_id_ = other.tenant_id_;
    ls_id_ = other.ls_id_;
    task_id_ = other.task_id_;
    status_ = other.status_;
    lock_owner_ = other.lock_owner_;
  }
  return ret;
}

int ObTransferTaskLockInfo::set(const uint64_t tenant_id, const share::ObLSID &ls_id, const int64_t task_id,
    const ObTransferLockStatus &status, const int64_t lock_owner, const common::ObString &comment)
{
  int ret = OB_SUCCESS;
  if (OB_INVALID_ID == tenant_id || task_id < 0 || !ls_id.is_valid() || !status.is_valid() || lock_owner <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tenant_id), K(ls_id), K(task_id), K(status), K(lock_owner));
  } else if (OB_FAIL(comment_.assign(comment))) {
    LOG_WARN("failed to assign comment", K(ret), K(comment));
  } else {
    tenant_id_ = tenant_id;
    ls_id_ = ls_id;
    task_id_ = task_id;
    status_ = status;
    lock_owner_ = lock_owner;
  }
  return ret;
}

ObTransferRelatedInfo::ObTransferRelatedInfo()
  : is_inited_(false),
    task_id_(),
    start_scn_(),
    start_out_log_replay_num_(0),
    start_in_log_replay_num_(0),
    finish_out_log_replay_num_(0),
    finish_in_log_replay_num_(0),
    map_(),
    lock_()
{
}

ObTransferRelatedInfo::~ObTransferRelatedInfo()
{
  destroy();
}

int ObTransferRelatedInfo::init()
{
  int ret = OB_SUCCESS;
  const int64_t BUCKET_NUM = 100;
  ObMemAttr attr(MTL_ID(), "TXBFRetryId");
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (OB_FAIL(map_.create(BUCKET_NUM, attr))) {
    LOG_WARN("fail to create hash map", K(ret), K(BUCKET_NUM));
  } else {
    start_out_log_replay_num_ = 0;
    start_in_log_replay_num_ = 0;
    finish_in_log_replay_num_ = 0;
    finish_out_log_replay_num_ = 0;
    is_inited_ = true;
  }
  return ret;
}

bool ObTransferRelatedInfo::is_valid() const
{
  return task_id_.is_valid()
         && start_scn_.is_valid()
         && start_out_log_replay_num_ >= 0
         && start_in_log_replay_num_ >= 0
         && finish_in_log_replay_num_ >= 0
         && finish_out_log_replay_num_ >= 0
         && is_inited_;
}

void ObTransferRelatedInfo::reset_()
{
  task_id_.reset();
  start_scn_.reset();
  start_out_log_replay_num_ = 0;
  start_in_log_replay_num_ = 0;
  finish_in_log_replay_num_ = 0;
  finish_out_log_replay_num_ = 0;
}

void ObTransferRelatedInfo::destroy()
{
  common::SpinWLockGuard guard(lock_);
  is_inited_ = false;
  reset_();
  map_.destroy();
}

void ObTransferRelatedInfo::reset()
{
  common::SpinWLockGuard guard(lock_);
  reset_();
  map_.reuse();
}

int ObTransferRelatedInfo::reset(const share::ObTransferTaskID &task_id)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (task_id != get_task_id_()) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("infos already are clean", K(ret), K(task_id), K(task_id_));
  } else {
    reset_();
    map_.reuse();
  }
  return ret;
}

int ObTransferRelatedInfo::inc_tx_backfill_retry_num_(const ObTabletID &id, int64_t &retry_num)
{
  int ret = OB_SUCCESS;
  retry_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument!", K(ret), K(id));
  } else {
    if (OB_FAIL(map_.get_refactored(id, retry_num))) {
      if (OB_HASH_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("fail to get retry id from map", K(ret), K(id));
      }
    }
    if (OB_FAIL(ret)) {
      // do nothing
    } else if (OB_FAIL(map_.set_refactored(id, ++retry_num, 1/*overwrite*/))) {
      LOG_WARN("fail to set retry_num", K(ret), K(id), K(retry_num));
    }
  }
  return ret;
}

int ObTransferRelatedInfo::set_info(
    const share::ObTransferTaskID &task_id,
    const share::SCN &start_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!task_id.is_valid() || !start_scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(start_scn));
  } else {
    common::SpinWLockGuard guard(lock_);
    reset_();
    map_.reuse();
    start_scn_ = start_scn;
    task_id_ = task_id;
  }
  return ret;
}

const share::ObTransferTaskID &ObTransferRelatedInfo::get_task_id_() const
{
  return task_id_;
}

const share::SCN &ObTransferRelatedInfo::get_start_scn_() const
{
  return start_scn_;
}

int ObTransferRelatedInfo::get_replay_retry_num_(
    const share::ObStorageHADiagTaskType type, const bool inc_retry_num, int64_t &retry_num)
{
  int ret = OB_SUCCESS;
  retry_num = 0;
  switch (type) {
    case ObStorageHADiagTaskType::TRANSFER_START_IN: {
      if (inc_retry_num) {
        start_out_log_replay_num_++;
      }
      retry_num = start_out_log_replay_num_;
      break;
    }
    case ObStorageHADiagTaskType::TRANSFER_START_OUT: {
      if (inc_retry_num) {
        start_in_log_replay_num_++;
      }
      retry_num = start_in_log_replay_num_;
      break;
    }
    case ObStorageHADiagTaskType::TRANSFER_FINISH_IN: {
      if (inc_retry_num) {
        finish_in_log_replay_num_++;
      }
      retry_num = finish_in_log_replay_num_;
      break;
    }
    case ObStorageHADiagTaskType::TRANSFER_FINISH_OUT: {
      if (inc_retry_num) {
        finish_out_log_replay_num_++;
      }
      retry_num = finish_out_log_replay_num_;
      break;
    }
    default: {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected type", K(ret), K(type));
    }
  }
  return ret;
}

int ObTransferRelatedInfo::record_error_diagnose_info_in_backfill(
    const share::SCN &log_sync_scn,
    const share::ObLSID &dest_ls_id,
    const int result_code,
    const ObTabletID &tablet_id,
    const ObMigrationStatus &migration_status,
    const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!log_sync_scn.is_valid()
      || !dest_ls_id.is_valid()
      || !tablet_id.is_valid()
      || OB_SUCCESS == result_code
      || migration_status < ObMigrationStatus::OB_MIGRATION_STATUS_NONE
      || migration_status >= ObMigrationStatus::OB_MIGRATION_STATUS_MAX) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_sync_scn), K(dest_ls_id),
        K(result_code), K(tablet_id), K(migration_status));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (log_sync_scn != get_start_scn_()) {
      if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("dest ls in migration", K(ret), K(dest_ls_id), K(migration_status));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("related_info already change", K(ret), K(start_scn_), K(log_sync_scn));
      }
    } else {
      int64_t retry_num = 0;
      if (OB_FAIL(inc_tx_backfill_retry_num_(tablet_id, retry_num))) {
        LOG_WARN("fail to get retry id", K(ret), K(tablet_id));
      } else if (OB_FAIL(ObStorageHADiagMgr::add_transfer_error_diagnose_info(get_task_id_(), dest_ls_id,
          share::ObStorageHADiagTaskType::TRANSFER_BACKFILLED, retry_num, result_code, tablet_id, result_msg))) {
        LOG_WARN("failed to add error diagnose info", K(ret), K(get_task_id_()), K(dest_ls_id),
            K(retry_num), K(result_code), K(tablet_id), K(result_msg));
      }
    }
  }
  return ret;
}

int ObTransferRelatedInfo::record_error_diagnose_info_in_replay(
    const share::ObTransferTaskID &task_id,
    const share::ObLSID &dest_ls_id,
    const int result_code,
    const bool clean_related_info,
    const share::ObStorageHADiagTaskType type,
    const share::ObStorageHACostItemName result_msg)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!task_id.is_valid()
      || !dest_ls_id.is_valid()
      || type < share::ObStorageHADiagTaskType::TRANSFER_START
      || type >= share::ObStorageHADiagTaskType::MAX_TYPE) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(task_id), K(dest_ls_id), K(type));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_SUCCESS == result_code) {
      // do nothing
    } else {
      int64_t log_replay_num = 0;
      if (OB_FAIL(get_replay_retry_num_(type, true/*inc_retry_num*/, log_replay_num))) {
        LOG_WARN("fail to inc retry num", K(ret), K(type), K(log_replay_num));
      } else if (OB_FAIL(ObStorageHADiagMgr::add_transfer_error_diagnose_info(task_id, dest_ls_id,
          type, log_replay_num, result_code, result_msg))) {
        LOG_WARN("failed to add error diagnose info", K(ret),
            K(task_id), K(dest_ls_id), K(type), K(log_replay_num), K(result_code), K(result_msg));
      }
    }
    if (OB_SUCCESS == result_code && clean_related_info) {
      reset_();
      map_.reuse();
    }
  }
  return ret;
}

int ObTransferRelatedInfo::construct_perf_diag_info_(
    const share::ObStorageHAPerfDiagParams &params,
    const uint64_t timestamp,
    const int64_t retry_num,
    const share::ObTransferTaskID &task_id,
    const int result,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator alloc;
  ObTransferPerfDiagInfo info;
  ObStorageHACostAccumItem accum_item;
  ObStorageHATimestampItem ts_item;
  ObIStorageHACostItem *item = nullptr;
  if (ObStorageHACostItemType::ACCUM_COST_TYPE == params.item_type_) {
    item = &accum_item;
    static_cast<ObStorageHACostAccumItem *>(item)->cost_time_ = timestamp;
  } else {
    item = &ts_item;
    static_cast<ObStorageHATimestampItem *>(item)->timestamp_ = timestamp;
  }
  item->name_ = params.name_;
  item->type_ = params.item_type_;
  item->retry_id_ = retry_num + 1;
  share::ObStorageHADiagTaskKey key;
  if (OB_FAIL(info.init(&alloc, MTL_ID()))) {
    LOG_WARN("fail to init info", K(ret));
  } else if (OB_FAIL(info.add_item(*item))) {
    LOG_WARN("fail to add item", K(ret), KPC(item));
  } else if (OB_FAIL(ObStorageHADiagMgr::construct_diagnose_info_key(
      task_id, ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE,
      params.task_type_, ObStorageHADiagType::PERF_DIAGNOSE, retry_num + 1, params.tablet_id_, key))) {
    LOG_WARN("failed to construct error diagnose info key",
        K(ret), K(task_id), K(retry_num), K(params.tablet_id_));
  } else if (OB_FAIL(ObStorageHADiagMgr::construct_diagnose_info(task_id, params.dest_ls_id_,
      params.task_type_, retry_num + 1, result, ObStorageHADiagModule::TRANSFER_PERF_DIAGNOSE, info))) {
    LOG_WARN("failed to construct diagnose info", K(ret), K(params), K(retry_num), K(result));
  } else if (OB_FAIL(ObStorageHADiagMgr::append_perf_diagnose_info(params.tablet_id_, info))) {
    LOG_WARN("fail to append diagnose info", K(ret), K(params.tablet_id_));
  } else if (OB_FAIL(ObStorageHADiagMgr::add_transfer_perf_diagnose_info(
        key, start_ts, params.tablet_count_, is_report, info))) {
    LOG_WARN("failed to add perf diagnose info", K(ret),
        K(key), K(info), K(timestamp), K(start_ts), K(is_report));
  }
  return ret;
}

int ObTransferRelatedInfo::record_perf_diagnose_info_in_replay(
    const share::ObStorageHAPerfDiagParams &params,
    const int result,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!params.is_valid()
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(params), K(start_ts));
  } else {
    common::SpinRLockGuard guard(lock_);
    int64_t log_replay_num = 0;
    if (OB_FAIL(get_replay_retry_num_(params.task_type_, false/*inc_retry_num*/, log_replay_num))) {
      LOG_WARN("fail to inc retry num", K(ret), K(params.task_type_), K(log_replay_num));
    } else if (OB_FAIL(construct_perf_diag_info_(params, timestamp,
        log_replay_num, params.task_id_, result, start_ts, is_report))) {
      LOG_WARN("fail to construct perf diag info", K(ret), K(params), K(timestamp),
          K(log_replay_num), K(result), K(start_ts), K(is_report));
    }
  }
  return ret;
}

int ObTransferRelatedInfo::record_perf_diagnose_info_in_backfill(
    const share::ObStorageHAPerfDiagParams &params,
    const share::SCN &log_sync_scn,
    const int result_code,
    const ObMigrationStatus &migration_status,
    const uint64_t timestamp,
    const int64_t start_ts,
    const bool is_report)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (!params.is_valid()
      ||!log_sync_scn.is_valid()
      || migration_status < ObMigrationStatus::OB_MIGRATION_STATUS_NONE
      || migration_status >= ObMigrationStatus::OB_MIGRATION_STATUS_MAX
      || start_ts < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(log_sync_scn), K(start_ts), K(params), K(migration_status));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (log_sync_scn != get_start_scn_()) {
      if (ObMigrationStatus::OB_MIGRATION_STATUS_NONE != migration_status) {
        ret = OB_STATE_NOT_MATCH;
        LOG_WARN("dest ls in migration", K(ret), K(migration_status));
      } else {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("related_info already change", K(ret), K(start_scn_), K(log_sync_scn));
      }
    } else {
      int64_t retry_num = 0;
      if (OB_FAIL(map_.get_refactored(params.tablet_id_, retry_num))) {
        if (OB_HASH_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get retry id from map", K(ret), K(params.tablet_id_));
        }
      }
      if (OB_FAIL(ret)) {
        //do nothing
      } else if (OB_FAIL(construct_perf_diag_info_(params, timestamp,
          retry_num, get_task_id_(), result_code, start_ts, is_report))) {
        LOG_WARN("fail to construct perf diag info", K(ret), K(params), K(timestamp),
            K(retry_num), K(result_code), K(start_ts), K(is_report));
      }
    }
  }
  return ret;
}

int ObTransferRelatedInfo::get_related_info_task_id(share::ObTransferTaskID &task_id) const
{
  int ret = OB_SUCCESS;
  task_id.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls transfer handler do not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    task_id = get_task_id_();
  }
  return ret;
}
