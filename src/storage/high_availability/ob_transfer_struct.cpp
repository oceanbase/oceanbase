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
#include "common/ob_version_def.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "src/storage/tx_storage/ob_ls_map.h"
#include "storage/high_availability/ob_storage_ha_diagnose_mgr.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/ob_storage_schema_util.h"
#include "share/schema/ob_tenant_schema_service.h"

using namespace oceanbase;
using namespace share;
using namespace storage;
using namespace transaction;

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

int64_t ObTXStartTransferOutInfo::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t save_pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    ObClusterVersion data_version;
    if (OB_FAIL(data_version.init(data_version_))) {
      LOG_WARN("failed to init data version", K(ret), K(data_version_));
    } else {
      J_OBJ_START();
      J_KV(K_(src_ls_id), K_(dest_ls_id), K_(tablet_list), K_(task_id),
           K_(data_end_scn), K_(transfer_epoch), K(data_version),
           K_(filter_tx_need_transfer), K_(move_tx_ids));
      J_OBJ_END();
    }
  }
  return pos;
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

int ObTXStartTransferInInfo::get_tablet_id_list(common::ObIArray<common::ObTabletID> &tablet_id_list) const
{
  int ret = OB_SUCCESS;
  tablet_id_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < tablet_meta_list_.count(); ++i) {
    const ObMigrationTabletParam &tablet_meta = tablet_meta_list_.at(i);
    if (OB_FAIL(tablet_id_list.push_back(tablet_meta.tablet_id_))) {
      LOG_WARN("failed to push tablet id into array", K(ret), K(tablet_meta));
    }
  }
  return ret;
}

int64_t ObTXStartTransferInInfo::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t save_pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    ObClusterVersion data_version;
    if (OB_FAIL(data_version.init(data_version_))) {
      LOG_WARN("failed to init data version", K(ret), K(data_version_));
    } else {
      J_OBJ_START();
      J_KV(K_(src_ls_id), K_(dest_ls_id), K_(start_scn), K_(tablet_meta_list), K_(task_id), K(data_version));
      J_OBJ_END();
    }
  }
  return pos;
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

int64_t ObTXFinishTransferInInfo::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t save_pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    ObClusterVersion data_version;
    if (OB_FAIL(data_version.init(data_version_))) {
      LOG_WARN("failed to init data version", K(ret), K(data_version_));
    } else {
      J_OBJ_START();
      J_KV(K_(src_ls_id), K_(dest_ls_id), K_(start_scn), K_(tablet_list), K_(task_id), K(data_version));
      J_OBJ_END();
    }
  }
  return pos;
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

int64_t ObTXFinishTransferOutInfo::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t save_pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    ObClusterVersion data_version;
    if (OB_FAIL(data_version.init(data_version_))) {
      LOG_WARN("failed to init data version", K(ret), K(data_version_));
    } else {
      J_OBJ_START();
      J_KV(K_(src_ls_id), K_(dest_ls_id), K_(finish_scn), K_(tablet_list), K_(task_id), K(data_version));
      J_OBJ_END();
    }
  }
  return pos;
}

ObTXTransferInAbortedInfo::ObTXTransferInAbortedInfo()
  : dest_ls_id_(),
    tablet_list_(),
    data_version_(MOCK_CLUSTER_VERSION_4_2_3_0)
{
}

void ObTXTransferInAbortedInfo::reset()
{
  dest_ls_id_.reset();
  tablet_list_.reset();
  data_version_ = 0;
}

bool ObTXTransferInAbortedInfo::is_valid() const
{
  return dest_ls_id_.is_valid()
      && !tablet_list_.empty()
      && data_version_ > 0;
}

int ObTXTransferInAbortedInfo::assign(const ObTXTransferInAbortedInfo &transfer_in_aborted_info)
{
  int ret = OB_SUCCESS;
  if (!transfer_in_aborted_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("assign transfer in aborted info get invalid argument", K(ret), K(transfer_in_aborted_info));
  } else if (OB_FAIL(tablet_list_.assign(transfer_in_aborted_info.tablet_list_))) {
    LOG_WARN("failed to assign transfer in aborted info", K(ret), K(transfer_in_aborted_info));
  } else {
    dest_ls_id_ = transfer_in_aborted_info.dest_ls_id_;
    data_version_ = transfer_in_aborted_info.data_version_;
  }
  return ret;
}

int64_t ObTXTransferInAbortedInfo::to_string(char *buf, const int64_t buf_len) const
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  int64_t save_pos = 0;
  if (OB_ISNULL(buf) || buf_len <= 0) {
      // do nothing
  } else {
    ObClusterVersion data_version;
    if (OB_FAIL(data_version.init(data_version_))) {
      LOG_WARN("failed to init data version", K(ret), K(data_version_));
    } else {
      J_OBJ_START();
      J_KV(K_(dest_ls_id), K_(tablet_list), K(data_version));
      J_OBJ_END();
    }
  }
  return pos;
}

OB_SERIALIZE_MEMBER(ObTXTransferInAbortedInfo, dest_ls_id_, tablet_list_, data_version_);


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
  if (get_commit) {
    if (OB_FAIL(tablet->get_latest_committed(user_data))) {
      LOG_WARN("failed to get committed tablet status", K(ret), KPC(tablet), K(user_data));
    }
  } else {
    mds::MdsWriter unused_writer;// will be removed later
    mds::TwoPhaseCommitState unused_trans_stat;// will be removed later
    share::SCN unused_trans_version;// will be removed later
    if (OB_FAIL(tablet->get_latest(user_data,
        unused_writer, unused_trans_stat, unused_trans_version))) {
      LOG_WARN("failed to get latest tablet status", K(ret), KPC(tablet), K(user_data));
    }
  }
  return ret;
}

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
  } else if (OB_FAIL(tablet->get_all_memtables_from_memtable_mgr(memtables))) {
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

int ObTXTransferUtils::traverse_trans_to_submit_redo_log_with_retry(
  storage::ObLS &ls,
  const int64_t timeout)
{
  int ret = OB_TX_NOLOGCB;
  ObTransID failed_tx_id;
  int64_t start_time = ObTimeUtil::current_time();

  while (OB_TX_NOLOGCB == ret
         && ObTimeUtil::current_time() - start_time < timeout) {
    ret = ls.get_tx_svr()->traverse_trans_to_submit_redo_log(failed_tx_id);
    if (OB_TX_NOLOGCB == ret) {
      usleep(10_ms);
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
  } else if (OB_FAIL(param.init_for_transfer_empty_minor_sstable(tablet_id, start_scn, end_scn, table_schema))) {
    LOG_WARN("fail to init sstable param", K(ret), K(tablet_id), K(start_scn), K(end_scn), K(table_schema));
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
  case ABORTED: {
    str = "ABORTED";
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
  } else if (0 == str.case_compare("ABORTED")) {
    status_ = ABORTED;
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

/******************ObTransferTabletInfoMgr*********************/
ObTransferBuildTabletInfoCtx::ObTransferTabletInfoMgr::ObTransferTabletInfoMgr()
  : lock_(),
    tablet_info_array_(),
    storage_schema_mgr_()

{
}

ObTransferBuildTabletInfoCtx::ObTransferTabletInfoMgr::~ObTransferTabletInfoMgr()
{
  common::SpinWLockGuard guard(lock_);
  tablet_info_array_.reset();
  storage_schema_mgr_.reset();
}

int ObTransferBuildTabletInfoCtx::ObTransferTabletInfoMgr::add_tablet_info(
    const ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add tablet info get invalid argument", K(ret), K(param));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(tablet_info_array_.push_back(param))) {
      LOG_WARN("failed to add tablet info", K(ret), K(param));
    }
  }
  return ret;
}

int64_t ObTransferBuildTabletInfoCtx::ObTransferTabletInfoMgr::get_tablet_info_num() const
{
  common::SpinRLockGuard guard(lock_);
  return tablet_info_array_.count();
}

int ObTransferBuildTabletInfoCtx::ObTransferTabletInfoMgr::get_tablet_info(
    const int64_t index, const ObMigrationTabletParam *&param)
{
  int ret = OB_SUCCESS;
  param = nullptr;
  common::SpinRLockGuard guard(lock_);
  if (index < 0 || index >= tablet_info_array_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablet info get invalid argument", K(ret), K(index));
  } else {
    ObMigrationTabletParam &tmp_param = tablet_info_array_.at(index);
    ObStorageSchema *storage_schema = nullptr;
    if (OB_FAIL(storage_schema_mgr_.get_storage_schema(tmp_param.tablet_id_, storage_schema))) {
      LOG_WARN("failed to get storage schema", K(tmp_param));
    } else if (OB_ISNULL(storage_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("storage schema should not be NULL", K(ret), K(tmp_param));
    } else if (storage_schema->column_cnt_ > tmp_param.storage_schema_.column_cnt_) {
      LOG_INFO("modified storage schema", "new_storage_schema", *storage_schema,
          "old_storage_schema", tmp_param.storage_schema_);
      if (OB_FAIL(tmp_param.storage_schema_.assign(tmp_param.allocator_, *storage_schema))) {
        LOG_WARN("failed to assign storage schema", K(ret), K(tmp_param), KPC(storage_schema));
      }
    }

    if (OB_SUCC(ret)) {
      param = &tablet_info_array_.at(index);
    }
  }
  return ret;
}

void ObTransferBuildTabletInfoCtx::ObTransferTabletInfoMgr::reuse()
{
  common::SpinWLockGuard guard(lock_);
  tablet_info_array_.reset();
  storage_schema_mgr_.reset();
}

int ObTransferBuildTabletInfoCtx::ObTransferTabletInfoMgr::build_storage_schema(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build storage schema info get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(storage_schema_mgr_.init(task_info.tablet_list_.count()))) {
    LOG_WARN("failed to init storage schema mgr", K(ret), K(task_info));
  } else if (OB_FAIL(storage_schema_mgr_.build_storage_schema(task_info, timeout_ctx))) {
    LOG_WARN("failed to build storage schema", K(ret), K(task_info));
  }
  return ret;
}

/******************ObTransferBuildTabletInfoCtx*********************/
ObTransferBuildTabletInfoCtx::ObTransferBuildTabletInfoCtx()
  : lock_(),
    dest_ls_id_(),
    index_(0),
    tablet_info_array_(),
    child_task_num_(0),
    total_tablet_count_(0),
    result_(OB_SUCCESS),
    data_version_(0),
    task_id_(),
    mgr_()
{
}

ObTransferBuildTabletInfoCtx::~ObTransferBuildTabletInfoCtx()
{
}

void ObTransferBuildTabletInfoCtx::reuse()
{
  common::SpinWLockGuard guard(lock_);
  total_tablet_count_ = 0;
  dest_ls_id_.reset();
  index_ = 0;
  tablet_info_array_.reset();
  child_task_num_ = 0;
  result_ = OB_SUCCESS;
  data_version_ = 0;
  task_id_.reset();
  mgr_.reuse();
}

int ObTransferBuildTabletInfoCtx::build_transfer_tablet_info(
    const share::ObLSID &dest_ls_id,
    const common::ObIArray<share::ObTransferTabletInfo> &tablet_info_array,
    const common::ObCurTraceId::TraceId &task_id,
    const uint64_t data_version)
{
  int ret = OB_SUCCESS;
  common::SpinWLockGuard guard(lock_);
  if (0 != index_ || !tablet_info_array_.empty()) {
    ret = OB_INIT_TWICE;
    LOG_WARN("build transfer tablet info init twice", K(ret), K(index_), K(tablet_info_array_));
  } else if (!dest_ls_id.is_valid() || !task_id.is_valid() || 0 == data_version) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build transfer tablet info get invalid argument", K(ret), K(task_id), K(data_version));
  } else if (OB_FAIL(tablet_info_array_.assign(tablet_info_array))) {
    LOG_WARN("failed to assign tablet info array", K(ret), K(tablet_info_array));
  } else {
    dest_ls_id_ = dest_ls_id;
    total_tablet_count_ = tablet_info_array_.count();
    task_id_ = task_id;
    data_version_ = data_version;
  }
  return ret;
}

bool ObTransferBuildTabletInfoCtx::is_valid() const
{
  common::SpinRLockGuard guard(lock_);
  return is_valid_();
}

bool ObTransferBuildTabletInfoCtx::is_valid_() const
{
  return index_ >= 0 && tablet_info_array_.count() >= 0 && index_ <= tablet_info_array_.count() && data_version_ > 0 && dest_ls_id_.is_valid();
}

int ObTransferBuildTabletInfoCtx::get_next_tablet_info(share::ObTransferTabletInfo &tablet_info)
{
  int ret = OB_SUCCESS;
  tablet_info.reset();
  common::SpinWLockGuard guard(lock_);
  if (!is_valid_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("transfer build tablet info ctx is invalid, unexpected", K(ret), KPC(this));
  } else if (index_ == tablet_info_array_.count()) {
    ret = OB_ITER_END;
  } else {
    tablet_info = tablet_info_array_.at(index_);
    ++index_;
  }
  return ret;
}

void ObTransferBuildTabletInfoCtx::inc_child_task_num()
{
  ATOMIC_INC(&child_task_num_);
}

void ObTransferBuildTabletInfoCtx::dec_child_task_num()
{
  ATOMIC_DEC(&child_task_num_);
}

int64_t ObTransferBuildTabletInfoCtx::get_child_task_num()
{
  int64_t child_task_num = ATOMIC_LOAD(&child_task_num_);
  return child_task_num;
}

int64_t ObTransferBuildTabletInfoCtx::get_total_tablet_count()
{
  common::SpinRLockGuard guard(lock_);
  return total_tablet_count_;
}

bool ObTransferBuildTabletInfoCtx::is_build_tablet_finish() const
{
  return total_tablet_count_ == mgr_.get_tablet_info_num();
}

bool ObTransferBuildTabletInfoCtx::is_failed() const
{
  common::SpinRLockGuard guard(lock_);
  return OB_SUCCESS != result_;
}

void ObTransferBuildTabletInfoCtx::set_result(const int32_t result)
{
  common::SpinWLockGuard guard(lock_);
  if (OB_SUCCESS == result_) {
    result_ = result;
  }
}

int32_t ObTransferBuildTabletInfoCtx::get_result()
{
  common::SpinRLockGuard guard(lock_);
  return result_;
}

int ObTransferBuildTabletInfoCtx::add_tablet_info(
    const ObMigrationTabletParam &param)
{
  int ret = OB_SUCCESS;
  if (!param.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("add tablet info get invalid argument", K(ret), K(param));
  } else if (OB_FAIL(mgr_.add_tablet_info(param))) {
    LOG_WARN("failed to add tablet info", K(ret), K(param));
  }
  return ret;
}

int ObTransferBuildTabletInfoCtx::get_tablet_info(
    const int64_t index, const ObMigrationTabletParam *&param)
{
  int ret = OB_SUCCESS;
  param = nullptr;
  if (index < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get tablet info get invalid argument", K(ret), K(index));
  } else if (OB_FAIL(mgr_.get_tablet_info(index, param))) {
    LOG_WARN("failed to get tablet info", K(ret), K(index));
  }
  return ret;
}

int64_t ObTransferBuildTabletInfoCtx::get_tablet_info_num() const
{
  return mgr_.get_tablet_info_num();
}

int ObTransferBuildTabletInfoCtx::build_storage_schema_info(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;

  if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build storage schema info get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(mgr_.build_storage_schema(task_info, timeout_ctx))) {
    LOG_WARN("failed to build storage schema", K(ret), K(task_info));
  }
  return ret;
}

/******************ObTransferStorageSchemaMgr*********************/
ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::ObTransferStorageSchemaMgr()
  : is_inited_(false),
    allocator_(),
    storage_schema_map_()
{
  ObMemAttr attr(MTL_ID(), "TransferSchema");
  allocator_.set_attr(attr);
}

ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::~ObTransferStorageSchemaMgr()
{
  reset();
}

void ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::reset()
{
  FOREACH(iter, storage_schema_map_) {
    ObStorageSchema *storage_schema = iter->second;
    storage_schema->~ObStorageSchema();
  }
  storage_schema_map_.destroy();
  allocator_.reset();
  is_inited_ = false;
}

int ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::init(const int64_t bucket_num)
{
  int ret = OB_SUCCESS;
  ObMemAttr attr(MTL_ID(), "TransferSchema");
  if (is_inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("transfer storage schema mgr is already init", K(ret));
  } else if (bucket_num <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("transfer storage schema mgr init get invalid argument", K(ret), K(bucket_num));
  } else if (OB_FAIL(storage_schema_map_.create(bucket_num, attr))) {
    LOG_WARN("failed to create storage schema map", K(ret), K(bucket_num));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::build_storage_schema(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer storage schema mgr do not init", K(ret));
  } else if (!task_info.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("build storage schema get invalid argument", K(ret), K(task_info));
  } else if (OB_FAIL(build_latest_storage_schema_(task_info, timeout_ctx))) {
    LOG_WARN("failed to build latest storage schema", K(ret));
  }
  return ret;
}

int ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::get_storage_schema(
    const ObTabletID &tablet_id,
    ObStorageSchema *&storage_schema)
{
  int ret = OB_SUCCESS;
  storage_schema = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer storage schema mgr do not init", K(ret));
  } else if (!tablet_id.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get storage schema get invalid argument", K(ret), K(tablet_id));
  } else if (OB_FAIL(storage_schema_map_.get_refactored(tablet_id, storage_schema))) {
    LOG_WARN("failed to get storage schema", K(ret), K(tablet_id));
  }
  return ret;
}

int ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::build_latest_storage_schema_(
    const share::ObTransferTaskInfo &task_info,
    ObTimeoutCtx &timeout_ctx)
{
  int ret = OB_SUCCESS;
  ObSchemaService *server_schema_service = nullptr;
  ObMultiVersionSchemaService *schema_service = nullptr;
  const int64_t start_ts = ObTimeUtil::current_time();
  ObLSService *ls_service = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer storage schema mgr do not init", K(ret));
  } else if (OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("schema_service is null", KR(ret));
  } else if (OB_ISNULL(server_schema_service = GCTX.schema_service_->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("server_schema_service is null", KR(ret));
  } else if (OB_ISNULL(schema_service = MTL(ObTenantSchemaService *)->get_schema_service())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get schema service from MTL", K(ret));
  } else if (OB_ISNULL(ls_service = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to get ObLSService from MTL", K(ret), KP(ls_service));
  } else if (OB_FAIL(ls_service->get_ls(task_info.src_ls_id_, ls_handle, ObLSGetMod::HA_MOD))) {
    LOG_WARN("failed to get ls", K(ret), K(task_info));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls should not be NULL", K(ret), KP(ls), K(task_info));
  } else {
    ObRefreshSchemaStatus status;
    status.tenant_id_ = task_info.tenant_id_;
    int64_t schema_version = 0;

    if (OB_FAIL(server_schema_service->fetch_schema_version(status, *GCTX.sql_proxy_, schema_version))) {
      LOG_WARN("fail to fetch schema version", KR(ret), K(status));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < task_info.tablet_list_.count(); ++i) {
        const ObTabletID &tablet_id = task_info.tablet_list_.at(i).tablet_id_;
        if (timeout_ctx.is_timeouted()) {
          ret = OB_TIMEOUT;
          LOG_WARN("transfer prepare storage schema timeout", K(ret));
        } else if (OB_FAIL(build_tablet_storage_schema_(task_info, tablet_id, schema_version, ls, *schema_service))) {
          LOG_WARN("failed to build tablet storage schema", K(ret), K(tablet_id));
        }
      }
    }
  }

  LOG_INFO("finish build storage schema", K(ret), "cost_ts", ObTimeUtil::current_time() - start_ts);
  return ret;
}

int ObTransferBuildTabletInfoCtx::ObTransferStorageSchemaMgr::build_tablet_storage_schema_(
    const share::ObTransferTaskInfo &task_info,
    const ObTabletID &tablet_id,
    const int64_t schema_version,
    ObLS *ls,
    ObMultiVersionSchemaService &schema_service)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObStorageSchema *storage_schema = nullptr;
  bool is_skip_merge_index = false;
  uint64_t compat_version = 0;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("transfer storage schema mgr do not init", K(ret));
  } else if (schema_version < 0 || !tablet_id.is_valid() || OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get table storage schema get invalid argument", K(ret), K(schema_version));
  } else if (OB_FAIL(ls->get_tablet(tablet_id, tablet_handle, 0,
      ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("failed to get tablet", K(ret), K(task_info));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), KP(tablet));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(task_info.tenant_id_, compat_version))) {
    LOG_WARN("fail to get data version", KR(ret), K(task_info));
  } else if (OB_FAIL(ObStorageSchemaUtil::alloc_storage_schema(allocator_, storage_schema))) {
    LOG_WARN("failed to alloc storage schema", K(ret));
  } else if (OB_FAIL(compaction::ObMediumCompactionScheduleFunc::get_table_schema_to_merge(
      schema_service, *tablet, schema_version, compat_version, allocator_, *storage_schema, is_skip_merge_index))) {
    LOG_WARN("failed to get table schema to merge", K(ret), KPC(tablet), K(task_info));
  } else if (OB_FAIL(storage_schema_map_.set_refactored(tablet_id, storage_schema))) {
    LOG_WARN("failed to push storage schema into map", K(ret), K(tablet_id), KPC(storage_schema));
  } else {
    storage_schema = nullptr;
  }

  if (OB_NOT_NULL(storage_schema)) {
    storage_schema->~ObStorageSchema();
  }
  return ret;
}
