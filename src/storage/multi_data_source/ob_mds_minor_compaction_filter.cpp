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

#include "storage/multi_data_source/ob_mds_minor_compaction_filter.h"

#include "src/storage/multi_data_source/mds_table_impl.h"
#include "src/storage/tx_storage/ob_ls_service.h"
#include "src/storage/ddl/ob_tablet_ddl_kv.h"

#define USING_LOG_PREFIX MDS

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::compaction;

namespace oceanbase
{
namespace storage
{
ERRSIM_POINT_DEF(INC_MAJOR_MDS_EXPIRED_TIME_SMALL);
ObMdsMinorFilter::ObMdsMinorFilter()
  : ObICompactionFilter(),
    is_inited_(false),
    last_major_snapshot_(0),
    ls_id_(ObLSID::INVALID_LS_ID),
    allocator_(ObMemAttr(MTL_ID(), "MdsMinorFilter"))
{
}

int ObMdsMinorFilter::init(
  const int64_t last_major_snapshot,
  const int64_t multi_version_start,
  const share::ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(last_major_snapshot < 0 || multi_version_start < 0 || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(last_major_snapshot), K(multi_version_start), K(ls_id));
  } else if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("is inited", K(ret), K(last_major_snapshot), K(multi_version_start));
  } else {
    last_major_snapshot_ = last_major_snapshot;
    truncate_filter_snapshot_ = MIN(multi_version_start, last_major_snapshot);
    ls_id_ = ls_id;
    LOG_INFO("truncate info filter snapshot", KR(ret), K(last_major_snapshot), K(multi_version_start), K_(truncate_filter_snapshot), K_(ls_id));
    is_inited_ = true;
  }
  return ret;
}

int ObMdsMinorFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_MAX;
  mds::MdsDumpKVStorageAdapter kv_adapter;
  constexpr uint8_t medium_info_mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<ObMediumCompactionInfoKey, ObMediumCompactionInfo>>::value;
  constexpr uint8_t truncateinfo_mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<ObTruncateInfoKey, ObTruncateInfo>>::value;
  constexpr uint8_t ddl_complete_unit_id =
    mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<ObTabletDDLCompleteMdsUserDataKey,
                                                        ObTabletDDLCompleteMdsUserData>>::value;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(kv_adapter.convert_from_mds_multi_version_row(row))) {
    LOG_WARN("fail to convert from mds multi version row", K(ret), K(row));
  } else if (medium_info_mds_unit_id == kv_adapter.get_type()) {
    ret = filter_medium_info(row, kv_adapter, filter_ret);
  } else if (truncateinfo_mds_unit_id == kv_adapter.get_type()) {
    ret = filter_truncate_info(row, kv_adapter, filter_ret);
  } else if (ddl_complete_unit_id == kv_adapter.get_type()) {
    ret = filter_ddl_complete_mds_info(row, kv_adapter, filter_ret);
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
    LOG_DEBUG("not medium info/truncate info", K(ret), K(row), K_(last_major_snapshot), K_(truncate_filter_snapshot), K(kv_adapter));
  }

  return ret;
}
int ObMdsMinorFilter::filter_medium_info(
  const blocksstable::ObDatumRow &row,
  const mds::MdsDumpKVStorageAdapter &kv_adapter,
  ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObMediumCompactionInfoKey medium_info_key;
  // filter medium compaction info
  if (OB_UNLIKELY(row.is_uncommitted_row()
      || !row.is_compacted_multi_version_row())) { // not filter uncommitted row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row or uncompacted row in mds table", K(ret), K(row));
  } else if (OB_FAIL(medium_info_key.mds_deserialize(kv_adapter.get_key().ptr(), kv_adapter.get_key().length(), pos))) {
    LOG_WARN("fail to deserialize medium_info_key", K(ret), K(kv_adapter));
  } else if (medium_info_key.get_medium_snapshot() <= last_major_snapshot_) {
    filter_ret = FILTER_RET_REMOVE;
    LOG_DEBUG("medium info is filtered", K(ret), K(row), K(last_major_snapshot_), K(medium_info_key), K(kv_adapter));
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
    LOG_DEBUG("medium info is not filtered", K(ret), K(row), K(last_major_snapshot_), K(medium_info_key), K(kv_adapter));
  }
  return ret;
}

int ObMdsMinorFilter::filter_truncate_info(
  const blocksstable::ObDatumRow &row,
  const mds::MdsDumpKVStorageAdapter &kv_adapter,
  ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  allocator_.reuse();
  ObTruncateInfo truncate_info;
  if (OB_UNLIKELY(row.is_uncommitted_row()
      || !row.is_compacted_multi_version_row())) { // not filter uncommitted row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row or uncompacted row in mds table", K(ret), K(row));
  } else if (OB_FAIL(truncate_info.deserialize(allocator_, kv_adapter.get_user_data().ptr(), kv_adapter.get_user_data().length(), pos))) {
    LOG_WARN("fail to deserialize truncate info", K(ret), K(kv_adapter));
  } else if (truncate_info.commit_version_ < truncate_filter_snapshot_) {
    filter_ret = FILTER_RET_REMOVE;
    // TODO change into debug log later
    LOG_INFO("truncate info is filtered", K(ret), K(row), K(truncate_filter_snapshot_), K(truncate_info), K(kv_adapter));
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
  }
  return ret;
}

int ObMdsMinorFilter::filter_ddl_complete_mds_info(
  const blocksstable::ObDatumRow &row,
  const mds::MdsDumpKVStorageAdapter &kv_adapter,
  ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  ObTabletDDLCompleteMdsUserData ddl_complete_info;
  bool need_filter = false;
  if (OB_UNLIKELY(row.is_uncommitted_row()
      || !row.is_compacted_multi_version_row())) { // not filter uncommitted row
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("uncommitted row or uncompacted row in mds table", K(ret), K(row));
  } else if (OB_FAIL(ddl_complete_info.deserialize(kv_adapter.get_user_data().ptr(),
                                                   kv_adapter.get_user_data().length(), pos))) {
    LOG_WARN("fail to deserialize ddl complete info", K(ret), K(kv_adapter));
  } else if  (!ddl_complete_info.trans_id_.is_valid()) {
    //full direct load
    need_filter = false;
  } else if (OB_FAIL(should_filter_ddl_inc_major_info(ddl_complete_info, need_filter))) {
    LOG_WARN("fail to filter inc major info", K(ret), K(row), K(ddl_complete_info), K(kv_adapter));
  }

  if (OB_SUCC(ret)) {
    if (need_filter) {
      filter_ret = FILTER_RET_REMOVE;
      LOG_TRACE("ddl complete info is filtered", K(ret), K(row), K(ddl_complete_info), K(kv_adapter));
    } else {
      filter_ret = FILTER_RET_NOT_CHANGE;
    }
  }
  if (OB_UNLIKELY(INC_MAJOR_MDS_EXPIRED_TIME_SMALL)) {
    FLOG_INFO("filter inc major info", K(ret), K(row), K(ddl_complete_info), K(kv_adapter), K(need_filter));
  }
  return ret;
}

int ObMdsMinorFilter::should_filter_ddl_inc_major_info(
  const ObTabletDDLCompleteMdsUserData &ddl_complete_info,
  bool &need_filter)
{
  int ret = OB_SUCCESS;
  need_filter = false;
  transaction::ObTransID tx_id = ddl_complete_info.trans_id_;
  common::ObTabletID tablet_id = ddl_complete_info.table_key_.tablet_id_;
  share::SCN start_scn = ddl_complete_info.start_scn_;

  bool tx_exist = false;
  bool inc_major_sstables_exist = false;
  bool expired = false;

  ObLSHandle ls_handle;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id_, ls_handle, ObLSGetMod::TRANS_MOD))) {
    LOG_WARN("get ls handle fail", K(ret), KPC(this));
  } else if (OB_FAIL(is_transaction_exist(ls_handle, tx_id, tx_exist))) {
    LOG_WARN("fail to check trans exist", K(ret), K(tx_id));
  } else if (tx_exist) {
  } else if (OB_FAIL(
               has_ddl_inc_major_sstables(ls_handle, tx_id, tablet_id, inc_major_sstables_exist))) {
    LOG_WARN("fail to check inc major sstables exist", K(ret));
  } else if (inc_major_sstables_exist) {
  } else if (OB_FAIL(is_ddl_inc_major_mds_expired(start_scn, expired))) {
    LOG_WARN("fail to check inc major reach time limit", K(ret), K(start_scn));
  } else if (expired) {
    need_filter = true;
  }
  return ret;
}

int ObMdsMinorFilter::is_transaction_exist(ObLSHandle &ls_handle, const transaction::ObTransID &tx_id,
                                           bool &exist)
{
  int ret = OB_SUCCESS;
  ObTxTableGuard tx_table_guard;
  int64_t state;
  share::SCN commit_version;
  share::SCN recycled_scn;
  exist = false;
  if (OB_FAIL(ls_handle.get_ls()->get_tx_table()->get_tx_table_guard(tx_table_guard))) {
    LOG_WARN("get tx table guard failed", KR(ret), KPC(this));
  } else if (OB_FAIL(tx_table_guard.try_get_tx_state(tx_id, state, commit_version, recycled_scn))) {
    if (OB_TRANS_CTX_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      exist = false;
    } else {
      LOG_WARN("get tx state failed", KR(ret), K(tx_id), KPC(this));
    }
  } else {
    exist = true;
  }
  return ret;
}

int ObMdsMinorFilter::has_ddl_inc_major_sstables(ObLSHandle &ls_handle,
                                                  const transaction::ObTransID &tx_id,
                                                  ObTabletID &tablet_id, bool &exist)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;

  ObDDLKvMgrHandle ddl_kv_mgr_handle;
  ObArray<ObDDLKVHandle> ddl_kv_handles;
  bool is_data_in_transfer = false;
  bool exist_ddl_kv = false;
  bool exist_ddl_dump = false;
  bool exist_inc_major = false;
  if (OB_FAIL(ls_handle.get_ls()->get_tablet_svr()->get_tablet(
        tablet_id, tablet_handle, 0, ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
  // check data in transfer
  } else if (!tablet_handle.get_obj()->get_tablet_meta().ha_status_.is_data_status_complete()) {
    is_data_in_transfer = true;
    //check ddlkv exist
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ddl_kv_mgr(ddl_kv_mgr_handle)))   {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_TRACE("kv mgr not exist", K(ret), K(tablet_handle.get_obj()->get_tablet_id()));
      ret = OB_SUCCESS; /* for empty table, ddl kv may not exist*/
    }
  } else if (OB_FAIL(ddl_kv_mgr_handle.get_obj()->get_ddl_kvs(false, ddl_kv_handles))) {
    LOG_WARN("failed to get ddl kv", K(ret));
  } else {
    FOREACH(iter, ddl_kv_handles) {
      if (iter->get_obj()->get_trans_id() == tx_id) {
        exist_ddl_kv = true;
      }
    }
  }
  //check ddl_dump exist
  ObTableStoreIterator table_store_iter;
  if (OB_FAIL(ret) || is_data_in_transfer || exist_ddl_kv) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_ddl_sstables(table_store_iter, tx_id))){
    LOG_WARN("failed to get inc major ddl sstable iter", KR(ret), K(tx_id));
  } else if (0 != table_store_iter.count()) {
    exist_ddl_dump = true;
  }

  //check inc_major exist
  ObTableStoreIterator inc_major_table_store_iter;
  if (OB_FAIL(ret) || is_data_in_transfer || exist_ddl_kv || exist_ddl_dump) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_inc_major_sstables(inc_major_table_store_iter, tx_id))) {
    LOG_WARN("failed to get inc major sstable iter", KR(ret), K(tx_id));
  } else if (0 != inc_major_table_store_iter.count()) {
    exist_inc_major = true;
  }

  if (OB_SUCC(ret)) {
    exist = is_data_in_transfer || exist_ddl_kv || exist_ddl_dump || exist_inc_major;
  }
  return ret;
}

int ObMdsMinorFilter::is_ddl_inc_major_mds_expired(share::SCN start_scn, bool &expired)
{
  int ret = OB_SUCCESS;
  expired = false;
  int64_t time_limit = 7LL * 24 * 60 * 60 * 1000 * 1000; // 7 days in microseconds
  if (OB_UNLIKELY(INC_MAJOR_MDS_EXPIRED_TIME_SMALL)) {
    time_limit = 1LL * 1000 * 1000; // 1s in microseconds
  }
  int64_t start_ts = start_scn.convert_to_ts();
  int64_t current_time = ObTimeUtility::current_time();
  if (current_time - start_ts >= time_limit) {
    expired = true;
  }
  return ret;
}

ObCrossLSMdsMinorFilter::ObCrossLSMdsMinorFilter()
  : ObICompactionFilter()
{
}

int ObCrossLSMdsMinorFilter::filter(
    const blocksstable::ObDatumRow &row,
    ObFilterRet &filter_ret)
{
  int ret = OB_SUCCESS;
  filter_ret = FILTER_RET_MAX;
  mds::MdsDumpKVStorageAdapter kv_adapter;
  constexpr uint8_t tablet_status_mds_unit_id = mds::TupleTypeIdx<mds::NormalMdsTable, mds::MdsUnit<mds::DummyKey, ObTabletCreateDeleteMdsUserData>>::value;

  if (OB_FAIL(kv_adapter.convert_from_mds_multi_version_row(row))) {
    LOG_WARN("fail to convert from mds multi version row", K(ret), K(row));
  } else if (tablet_status_mds_unit_id == kv_adapter.get_type()) {
    if (OB_UNLIKELY(row.is_uncommitted_row()
        || !row.is_compacted_multi_version_row())) { // not filter uncommitted row
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("uncommitted row or uncompacted row in mds table", K(ret), K(row));
    } else {
      filter_ret = FILTER_RET_REMOVE;
      LOG_DEBUG("filter tablet status for cross ls mds minor merge", K(ret));
    }
  } else {
    filter_ret = FILTER_RET_NOT_CHANGE;
  }

  return ret;
}
} // namespace storage
} // namespace oceanbase