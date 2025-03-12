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

#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx/ob_trans_service.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_store/ob_storage_meta_io_util.h"

#define USING_LOG_PREFIX STORAGE

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::transaction;
using namespace oceanbase::palf;
using namespace oceanbase::memtable;

namespace oceanbase
{
namespace storage
{
ObTabletCreateDeleteHelper::ReadMdsFunctor::ReadMdsFunctor(ObTabletCreateDeleteMdsUserData &user_data)
  : user_data_(user_data)
{
}

int ObTabletCreateDeleteHelper::ReadMdsFunctor::operator()(const ObTabletCreateDeleteMdsUserData &data)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(user_data_.assign(data))) {
    LOG_WARN("failed to copy", K(ret));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::replay_mds_get_tablet(
    const ObTabletMapKey &key, ObLS *ls, ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls is null", K(ret));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
#ifdef OB_BUILD_SHARED_STORAGE
      if (GCTX.is_shared_storage_mode() && OB_FAIL(try_get_current_version_tablet_(key, ls, handle))) {
        if (OB_TABLET_NOT_EXIST != ret) {
          LOG_WARN("fail to get current version tablet", K(ret), K(key));
        }
      }
#endif
    } else {
      LOG_WARN("fail to get tablet", K(ret), K(key));
    }
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObTabletCreateDeleteHelper::try_get_current_version_tablet_(
    const ObTabletMapKey &key, ObLS *ls, ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator;
  ObPrivateTabletCurrentVersion current_version;
  ObStorageObjectOpt opt;
  bool is_exist = false;
  opt.set_ss_private_tablet_meta_current_verison_object_opt(key.ls_id_.id(), key.tablet_id_.id());
  ObLSTabletService *ls_tablet_svr;

  if (OB_FAIL(ObStorageMetaIOUtil::check_meta_existence(opt, ls->get_ls_epoch(), is_exist))) {
    LOG_WARN("fail to check existence", K(ret), K(opt));
  } else if (!is_exist) {
    ret = OB_TABLET_NOT_EXIST;
  } else if (OB_FAIL(ObStorageMetaIOUtil::read_storage_meta_object(
      opt, allocator, MTL_ID(), ls->get_ls_epoch(), current_version))) {
    LOG_WARN("fail to read current version tablet addr", K(ret), K(opt));
  } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet service is null", K(ret), K(key));
  } else if (OB_FAIL(ls_tablet_svr->ss_replay_create_tablet(current_version.tablet_addr_, key.tablet_id_))) {
    LOG_WARN("fail to replay create tablet", K(ret), K(current_version));
  } else if (OB_FAIL(get_tablet(key, handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get tablet", K(ret), K(key));
    }
  }
  LOG_INFO("try get current version tablet", K(ret), K(key), K(is_exist));
  return ret;
}
#endif

int ObTabletCreateDeleteHelper::get_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle,
    const int64_t timeout_us)
{
#ifdef ENABLE_DEBUG_LOG
  ObTimeGuard tg("ObTabletCreateDeleteHelper::get_tablet", 10000);
#endif
  int ret = OB_SUCCESS;
  static const int64_t SLEEP_TIME_US = 10;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const int64_t begin_time = ObClockGenerator::getClock();
  int64_t current_time = 0;

  while (OB_SUCC(ret)) {
    ret = t3m->get_tablet(WashTabletPriority::WTP_HIGH, key, handle);
    if (OB_SUCC(ret)) {
      break;
    } else if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_DEBUG("tablet does not exist", K(ret), K(key));
    } else if (OB_ITEM_NOT_SETTED == ret) {
      current_time = ObClockGenerator::getClock();
      if (current_time - begin_time > timeout_us) {
        ret = OB_TABLET_NOT_EXIST;
        LOG_WARN("continuously meet item not set error", K(ret), K(key),
            K(begin_time), K(current_time), K(timeout_us));
      } else {
        ret = OB_SUCCESS;
        ob_usleep(SLEEP_TIME_US);
      }
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(key));
    }
  }
  return ret;
}

int ObTabletCreateDeleteHelper::check_and_get_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle,
    const int64_t timeout_us,
    const ObMDSGetTabletMode mode,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;

  if (OB_FAIL(get_tablet(key, handle, timeout_us))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_DEBUG("tablet does not exist", K(ret), K(key), K(mode));
    } else {
      LOG_WARN("failed to get tablet", K(ret), K(key), K(mode));
    }
  } else if (OB_ISNULL(tablet = handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(handle));
  } else if (tablet->is_ls_inner_tablet()) {
    // no need to check ls inner tablet, do nothing
  } else if (ObMDSGetTabletMode::READ_WITHOUT_CHECK == mode) {
    // no checking
  } else if (ObMDSGetTabletMode::READ_ALL_COMMITED == mode) {
    if (OB_UNLIKELY(snapshot_version != ObTransVersion::MAX_TRANS_VERSION)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("read all committed mode should only pass max scn", K(ret), K(key), K(mode), K(snapshot_version));
    } else if (OB_FAIL(tablet->check_tablet_status_for_read_all_committed())) {
      LOG_WARN("failed to check tablet status", K(ret), K(key));
    }
  } else if (ObMDSGetTabletMode::READ_READABLE_COMMITED == mode) {
    if (OB_FAIL(tablet->check_new_mds_with_cache(snapshot_version))) {
      LOG_WARN("failed to check status for new mds", K(ret), K(mode), K(snapshot_version));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected mode", K(ret), K(key), K(mode));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::check_status_for_new_mds(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    ObTabletStatusCache &tablet_status_cache)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter writer;// will be removed later
  mds::TwoPhaseCommitState trans_state;// will be removed later
  share::SCN trans_version;// will be removed later

  if (OB_UNLIKELY(tablet.is_empty_shell())) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("tablet is empty shell", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else if (OB_FAIL(tablet.get_latest(user_data, writer, trans_state, trans_version))) {
    if (OB_EMPTY_RESULT == ret) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("tablet creation has not been committed, or has been roll backed", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("failed to get snapshot", KR(ret), K(ls_id), K(tablet_id));
    }
  } else {
    const ObTabletStatus::Status &status = user_data.tablet_status_.get_status();
    switch (status) {
      case ObTabletStatus::NORMAL:
      case ObTabletStatus::SPLIT_DST:
        ret = check_read_snapshot_for_normal_or_split_dst(tablet, snapshot_version, user_data, writer, trans_state, trans_version);
        break;
      case ObTabletStatus::TRANSFER_IN:
        ret = check_read_snapshot_for_transfer_in(tablet, snapshot_version, user_data, writer, trans_state, trans_version);
        break;
      case ObTabletStatus::DELETED:
      case ObTabletStatus::TRANSFER_OUT:
        ret = check_read_snapshot_for_deleted_or_transfer_out(tablet, snapshot_version, user_data, writer, trans_state, trans_version);
        break;
      case ObTabletStatus::TRANSFER_OUT_DELETED:
        ret = check_read_snapshot_for_transfer_out_deleted(tablet, snapshot_version, user_data);
        break;
      case ObTabletStatus::SPLIT_SRC:
        ret = check_read_snapshot_for_split_src(tablet, snapshot_version, user_data, trans_state);
        break;
      case ObTabletStatus::SPLIT_SRC_DELETED:
        ret = check_read_snapshot_for_split_src_deleted(tablet, user_data, trans_state);
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected tablet status", K(ret), K(ls_id), K(tablet_id), K(user_data));
    }

    if (OB_FAIL(ret)) {
    } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_state && ObTabletStatus::NORMAL == user_data.tablet_status_) {
      tablet_status_cache.set_value(user_data);
      LOG_INFO("refresh tablet status cache", K(ret), K(ls_id), K(tablet_id), K(tablet_status_cache), K(snapshot_version));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_by_commit_version(
    const ObTablet &tablet,
    const int64_t create_commit_version,
    const int64_t delete_commit_version,
    const int64_t snapshot_version,
    const ObTabletStatus &tablet_status)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  if (snapshot_version == ObTransVersion::MAX_TRANS_VERSION) {
    // do nothing
  } else if (OB_UNLIKELY(create_commit_version == ObTransVersion::INVALID_TRANS_VERSION)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create tablet trans version is invalid",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(create_commit_version));
  } else if (snapshot_version < create_commit_version) {
    // read snapshot is smaller than create tablet trans version,
    // no previous committed transaction
    ret= OB_SNAPSHOT_DISCARDED;
    LOG_INFO("tablet status is set to MAX because read snapshot is smaller than create trans version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(create_commit_version));
  } else if (delete_commit_version == ObTransVersion::INVALID_TRANS_VERSION) {
    // delete commit version is not valid, no delete transaction committed
  } else if (snapshot_version < delete_commit_version) {
    // read snapshot is smaller than delete tablet trans version,
    // previous transaction is create tablet/transfer in create tablet, so tablet status is NORMAL
    LOG_INFO("tablet status is set to NORMAL because read snapshot is smaller than delete trans version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(delete_commit_version));
  } else {
    // snapshot_version >= user_data.delete_commit_version_
    ret = ObTabletStatus::TRANSFER_OUT_DELETED == tablet_status ? OB_TABLET_NOT_EXIST : OB_TABLE_NOT_EXIST;
    LOG_INFO("tablet is deleted or transfer out deleted",
        K(ret), K(ls_id), K(tablet_id), K(tablet_status), K(snapshot_version), K(delete_commit_version));
  }

  if (OB_FAIL(ret)) {
  } else if (ObTabletStatus::NORMAL == tablet_status || ObTabletStatus::TRANSFER_IN == tablet_status || ObTabletStatus::SPLIT_DST == tablet_status) {
    if (OB_UNLIKELY(tablet.is_empty_shell())) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("tablet is empty shell", K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(create_commit_version));
    }
  } else {
    ret = OB_TABLET_NOT_EXIST;
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_normal_or_split_dst(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const mds::MdsWriter &writer,
    const mds::TwoPhaseCommitState &trans_state,
    const share::SCN &trans_version)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletStatus &tablet_status = user_data.tablet_status_;
  share::SCN read_snapshot;

  if (OB_UNLIKELY(ObTabletStatus::NORMAL != tablet_status && ObTabletStatus::SPLIT_DST != tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else if (user_data.create_commit_version_ == ObTransVersion::MAX_TRANS_VERSION) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("create commit version is max trans version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(trans_state), K(user_data));
  } else if (user_data.create_commit_version_ != ObTransVersion::INVALID_TRANS_VERSION) {
    LOG_INFO("tablet create transaction is committed, currently in finish transfer in transacation",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(trans_state), K(user_data));
    if (OB_FAIL(check_read_snapshot_for_finish_transfer_in_tx(tablet, snapshot_version, user_data))) {
      LOG_WARN("fail to check readsnapshot for finish transfer in tx",
          K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(trans_state), K(user_data));
    }
  } else if (OB_FAIL(check_read_snapshot_for_create_tx(
      tablet, snapshot_version, user_data, writer, trans_state, trans_version))) {
    LOG_WARN("fail to check read snapshot for create tx",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(trans_state), K(user_data));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_finish_transfer_in_tx(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObTabletCreateDeleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;

  if (snapshot_version < user_data.create_commit_version_) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("read snapshot smaller than create commit version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_create_tx(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const mds::MdsWriter &writer,
    const mds::TwoPhaseCommitState &trans_state,
    const share::SCN &trans_version)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletStatus &tablet_status = user_data.tablet_status_;
  share::SCN read_snapshot;

  if (trans_state < mds::TwoPhaseCommitState::ON_PREPARE) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("tablet creation transaction has not entered 2pc procedure",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(trans_state), K(user_data));
  } else if (OB_FAIL(read_snapshot.convert_for_tx(snapshot_version))) {
    LOG_WARN("failed to convert from int64_t to SCN", K(ret), K(snapshot_version));
  } else if (trans_state >= mds::TwoPhaseCommitState::ON_PREPARE && trans_state < mds::TwoPhaseCommitState::ON_COMMIT) {
    if (read_snapshot < trans_version) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("read snapshot is smaller than prepare version",
          K(ret), K(ls_id), K(tablet_id), K(trans_state), K(read_snapshot), K(trans_version));
    } else if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
      // primary tenant
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("tablet creation transaction has not committed",
          K(ret), K(ls_id), K(tablet_id), K(trans_state), K(read_snapshot), K(trans_version));
    } else {
      // standby tenant(including restore/invalid role): call interface from @xuwang.txw, get "potential" commit version, then decide
      // whether allow to read
      const ObTransID tx_id(writer.writer_id_);
      ObTxCommitData::TxDataState tx_data_state;
      share::SCN commit_version;
      if (OB_FAIL(check_for_standby(ls_id, tx_id, read_snapshot, tx_data_state, commit_version))) {
        LOG_WARN("failed to check for standby", K(ret), K(ls_id), K(tablet_id), K(tx_id), K(read_snapshot));
      } else {
        switch (tx_data_state) {
          case ObTxCommitData::TxDataState::COMMIT:
            {
              if (read_snapshot < commit_version) {
                ret = OB_SNAPSHOT_DISCARDED;
                LOG_WARN("read snapshot is smaller than trans version",
                    K(ret), K(ls_id), K(tablet_id), K(tx_id), K(trans_state), K(read_snapshot), K(trans_version));
              }
            }
            break;
          case ObTxCommitData::TxDataState::RUNNING:
          case ObTxCommitData::TxDataState::ABORT:
            ret = OB_SNAPSHOT_DISCARDED;
            LOG_WARN("transaction has not been committed", K(ret), K(ls_id), K(tx_id), K(tx_data_state), K(read_snapshot));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected tx data state", K(ret), K(ls_id), K(tablet_id), K(tx_id), K(tx_data_state), K(read_snapshot));
        }
      }
    }
  } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_state) {
    if (snapshot_version < user_data.create_commit_version_) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("read snapshot smaller than create commit version",
          K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_transfer_in(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const mds::MdsWriter &writer,
    const mds::TwoPhaseCommitState &trans_state,
    const share::SCN &trans_version)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletStatus &tablet_status = user_data.tablet_status_;
  share::SCN read_snapshot;

  if (OB_UNLIKELY(ObTabletStatus::TRANSFER_IN != tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else if (snapshot_version < user_data.create_commit_version_) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("read snapshot smaller than create commit version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
  } else if (trans_state < mds::TwoPhaseCommitState::ON_PREPARE) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("start transfer in transaction has not entered 2pc procedure, should retry",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(trans_state));
  } else if (OB_FAIL(read_snapshot.convert_for_tx(snapshot_version))) {
    LOG_WARN("failed to convert from int64_t to SCN", K(ret), K(snapshot_version));
  } else if (trans_state >= mds::TwoPhaseCommitState::ON_PREPARE && trans_state < mds::TwoPhaseCommitState::ON_COMMIT) {
    if (read_snapshot < trans_version) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("read snapshot is smaller than prepare version, should retry",
          K(ret), K(ls_id), K(tablet_id), K(trans_state), K(read_snapshot), K(trans_version));
    } else if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
      // primary tenant: not allowed to read, retry
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("read snapshot is no smaller than prepare version, primary tenant should retry",
          K(ret), K(ls_id), K(tablet_id), K(trans_state), K(read_snapshot), K(trans_version));
    } else {
      // standby tenant(including restore/invalid role): call interface from @xuwang.txw, get "potential" commit version, then decide
      // whether allow to read
      const ObTransID tx_id(writer.writer_id_);
      ObTxCommitData::TxDataState tx_data_state;
      share::SCN commit_version;
      if (OB_FAIL(check_for_standby(ls_id, tx_id, read_snapshot, tx_data_state, commit_version))) {
        LOG_WARN("failed to check for standby", K(ret), K(ls_id), K(tablet_id), K(tx_id), K(read_snapshot));
      } else {
        switch (tx_data_state) {
          case ObTxCommitData::TxDataState::COMMIT:
            {
              if (read_snapshot < commit_version) {
                ret = OB_TABLET_NOT_EXIST;
                LOG_WARN("read snapshot is smaller than commit version, should retry",
                    K(ret), K(ls_id), K(tablet_id), K(tx_id), K(trans_state), K(read_snapshot), K(trans_version));
              }
            }
            break;
          case ObTxCommitData::TxDataState::RUNNING:
          case ObTxCommitData::TxDataState::ABORT:
            ret = OB_TABLET_NOT_EXIST;
            LOG_WARN("transaction has not been committed", K(ret), K(ls_id), K(tx_id), K(tx_data_state), K(read_snapshot));
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected tx data state", K(ret), K(ls_id), K(tablet_id), K(tx_id), K(tx_data_state), K(read_snapshot));
            break;
        }
      }
    }
  } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_state) {
    // check start transfer commit version
    // trans version is regarded as transfer in commit version
    if (read_snapshot < trans_version) {
      // not allow to read
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("read snapshot is smaller than start transfer in transaction commit version, should retry",
          K(ret), K(ls_id), K(tablet_id), K(trans_state), K(read_snapshot), K(trans_version));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_deleted_or_transfer_out(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const mds::MdsWriter &writer,
    const mds::TwoPhaseCommitState &trans_state,
    const share::SCN &trans_version)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletStatus &tablet_status = user_data.tablet_status_;
  share::SCN read_snapshot;

  if (OB_UNLIKELY(ObTabletStatus::TRANSFER_OUT != tablet_status && ObTabletStatus::DELETED != tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else if (snapshot_version < user_data.create_commit_version_) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("read snapshot smaller than create commit version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
  } else if (OB_FAIL(read_snapshot.convert_for_tx(snapshot_version))) {
    LOG_WARN("failed to convert from int64_t to SCN", K(ret), K(snapshot_version));
  } else if (trans_state < mds::TwoPhaseCommitState::ON_PREPARE) {
    if (read_snapshot.is_max()) {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("read snapshot is MAX, maybe this is a write request, should retry on dst ls",
          K(ret), K(ls_id), K(tablet_id), K(read_snapshot), K(user_data));
    }
  } else if (trans_state >= mds::TwoPhaseCommitState::ON_PREPARE && trans_state < mds::TwoPhaseCommitState::ON_COMMIT) {
    if (read_snapshot < trans_version) {
      // allow to read
    } else if (MTL_TENANT_ROLE_CACHE_IS_PRIMARY_OR_INVALID()) {
      // primary tenant: retry
      ret = OB_TABLET_NOT_EXIST;
      LOG_INFO("read snapshot is no smaller than prepare version on primary tenant, should retry on target ls",
          K(ret), K(ls_id), K(tablet_id), K(trans_state), K(read_snapshot), K(trans_version));
    } else {
      // standby tenant(including restore/invalid role): call interface from @xuwang.txw, get "potential" commit version, then decide
      // whether allow to read
      const ObTransID tx_id(writer.writer_id_);
      ObTxCommitData::TxDataState tx_data_state;
      share::SCN commit_version;
      if (OB_FAIL(check_for_standby(ls_id, tx_id, read_snapshot, tx_data_state, commit_version))) {
        LOG_WARN("failed to check for standby", K(ret), K(ls_id), K(tablet_id), K(tx_id), K(read_snapshot));
      } else {
        switch (tx_data_state) {
          case ObTxCommitData::TxDataState::COMMIT:
            {
              if (read_snapshot < commit_version) {
                // allow to read
              } else {
                ret = OB_TABLET_NOT_EXIST;
                LOG_WARN("read snapshot is no smaller than trans version, should retry on target ls",
                    K(ret), K(ls_id), K(tablet_id), K(tx_id), K(read_snapshot), K(trans_version));
              }
            }
            break;
          case ObTxCommitData::TxDataState::RUNNING:
          case ObTxCommitData::TxDataState::ABORT:
            // allow to read
            break;
          default:
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected tx data state", K(ret), K(ls_id), K(tablet_id), K(tx_id), K(tx_data_state), K(read_snapshot));
            break;
        }
      }
    }
  } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_state) {
    // check start transfer commit version
    // trans version is regarded as transfer out commit version
    if (read_snapshot < trans_version) {
      // allow to read
    } else {
      ret = OB_TABLET_NOT_EXIST;
      LOG_WARN("read snapshot is no smaller than trans version, should retry on target ls",
          K(ret), K(ls_id), K(tablet_id), K(read_snapshot), K(trans_version));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_transfer_out_deleted(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObTabletCreateDeleteMdsUserData &user_data)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletStatus &tablet_status = user_data.tablet_status_;

  if (OB_UNLIKELY(ObTabletStatus::TRANSFER_OUT_DELETED != tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else if (snapshot_version < user_data.create_commit_version_) {
    ret = OB_SNAPSHOT_DISCARDED;
    LOG_WARN("read snapshot smaller than create commit version",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
  } else if (snapshot_version >= user_data.start_transfer_commit_version_) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("read snapshot is no smaller than start transfer commit version, should retry on dst ls",
        K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_for_standby(
    const share::ObLSID &ls_id,
    const transaction::ObTransID &tx_id,
    const share::SCN &snapshot,
    ObTxCommitData::TxDataState &tx_data_state,
    share::SCN &commit_version)
{
  int ret = OB_SUCCESS;
  ObTransService *trans_service = MTL(ObTransService*);
  ObPartTransCtx *tx_ctx = nullptr;

  if (OB_UNLIKELY(snapshot.is_max())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, snapshot is MAX", K(ret), K(snapshot));
  } else if (OB_FAIL(trans_service->mds_infer_standby_trx_state(nullptr/*ls_ptr*/,
      ls_id, tx_id, snapshot, tx_data_state, commit_version))) {
    LOG_WARN("failed to do mds infer standby trx state", K(ret), K(ls_id), K(tx_id), K(snapshot));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_split_src(
    const ObTablet &tablet,
    const int64_t snapshot_version,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const mds::TwoPhaseCommitState &trans_state)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletStatus &tablet_status = user_data.tablet_status_;

  if (OB_UNLIKELY(ObTabletStatus::SPLIT_SRC != tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_state) {
    if (snapshot_version < user_data.create_commit_version_) {
      ret = OB_SNAPSHOT_DISCARDED;
      LOG_WARN("read snapshot smaller than create commit version",
          K(ret), K(ls_id), K(tablet_id), K(snapshot_version), K(user_data));
    } else {
      ret = OB_TABLET_IS_SPLIT_SRC;
      LOG_WARN("tablet is split src", K(ret), K(ls_id), K(tablet_id), K(common::lbt()));
    }
  }

  return ret;
}

int ObTabletCreateDeleteHelper::check_read_snapshot_for_split_src_deleted(
    const ObTablet &tablet,
    const ObTabletCreateDeleteMdsUserData &user_data,
    const mds::TwoPhaseCommitState &trans_state)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = tablet.get_tablet_meta().ls_id_;
  const common::ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObTabletStatus &tablet_status = user_data.tablet_status_;

  if (OB_UNLIKELY(ObTabletStatus::SPLIT_SRC_DELETED != tablet_status)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(user_data));
  } else if (mds::TwoPhaseCommitState::ON_COMMIT == trans_state) {
    ret = OB_TABLET_NOT_EXIST;
    LOG_WARN("split src deleted", K(ret), K(ls_id), K(tablet_id), K(common::lbt()));
  }

  return ret;
}

int ObTabletCreateDeleteHelper::create_tmp_tablet(
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(ls_service->get_ls(key.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("fail to get ls", K(ret), "ls_id", key.ls_id_);
  } else if (OB_FAIL(t3m->create_tmp_tablet(WashTabletPriority::WTP_HIGH, key, allocator, ls_handle, handle))) {
    LOG_WARN("fail to create temporary tablet", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::prepare_create_msd_tablet()
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_FAIL(t3m->get_mstx_tablet_creator().throttle_tablet_creation())) {
    LOG_WARN("fail to prepare full tablet", K(ret));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::create_msd_tablet(
    const ObTabletMapKey &key,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObLSHandle ls_handle;
  ObLSService *ls_service = MTL(ObLSService*);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(ls_service->get_ls(key.ls_id_, ls_handle, ObLSGetMod::TABLET_MOD))) {
    LOG_WARN("fail to get ls", K(ret), "ls_id", key.ls_id_);
  } else if (OB_FAIL(t3m->create_msd_tablet(WashTabletPriority::WTP_HIGH, key, ls_handle, handle))) {
    LOG_WARN("fail to create multi source data tablet", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::acquire_tmp_tablet(
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &handle)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (CLICK_FAIL(t3m->acquire_tmp_tablet(WashTabletPriority::WTP_HIGH, key, allocator, handle))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::acquire_tablet_from_pool(
    const ObTabletPoolType &type,
    const ObTabletMapKey &key,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(key));
  } else if (OB_FAIL(t3m->acquire_tablet_from_pool(type, WashTabletPriority::WTP_HIGH, key, handle))) {
    LOG_WARN("fail to acquire tablet from pool", K(ret), K(key), K(type));
  } else if (OB_ISNULL(handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), K(handle));
  }
  return ret;
}

int ObTabletCreateDeleteHelper::create_empty_sstable(
    common::ObArenaAllocator &allocator,
    const ObStorageSchema &storage_schema,
    const common::ObTabletID &tablet_id,
    const int64_t snapshot_version,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  table_handle.reset();
  ObTabletCreateSSTableParam param;

  if (OB_UNLIKELY(!storage_schema.is_valid() || snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("get invalid arguments", K(ret), K(snapshot_version), K(storage_schema));
  } else if (storage_schema.get_column_group_count() > 1) { // column store mode
    if (OB_FAIL(create_empty_co_sstable(allocator, storage_schema, tablet_id, snapshot_version, table_handle))) {
      LOG_WARN("failed to create co sstable", K(ret), K(storage_schema));
    }
  } else if (OB_FAIL(param.init_for_empty_major_sstable(tablet_id, storage_schema, snapshot_version, -1/*cg idx*/, false/*has all cg*/))) {
    LOG_WARN("failed to build sstable param", K(ret), K(tablet_id), K(storage_schema), K(snapshot_version));
  } else if (OB_FAIL(create_sstable(param, allocator, table_handle))) {
    LOG_WARN("failed to create sstable", K(ret), K(param));
  }

  if (OB_FAIL(ret)) {
    table_handle.reset();
  }
  return ret;
}

int ObTabletCreateDeleteHelper::create_empty_co_sstable(
    common::ObArenaAllocator &allocator,
    const ObStorageSchema &storage_schema,
    const common::ObTabletID &tablet_id,
    const int64_t snapshot_version,
    ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;
  table_handle.reset();

  // build main cg sstables
  bool has_all_cg = false;
  const ObIArray<ObStorageColumnGroupSchema> &cg_schemas = storage_schema.get_column_groups();
  for (int64_t idx = cg_schemas.count() - 1; OB_SUCC(ret) && idx >= 0; --idx) {
    const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(idx);
    if (cg_schema.is_all_column_group()) {
      has_all_cg = true;
      break;
    }
  }

  ObTableHandleV2 co_handle;
  for (int64_t idx = 0; OB_SUCC(ret) && idx < cg_schemas.count(); ++idx) {
    const ObStorageColumnGroupSchema &cg_schema = cg_schemas.at(idx);
    if (cg_schema.is_all_column_group() || (!has_all_cg && cg_schema.is_rowkey_column_group())) {
      ObTabletCreateSSTableParam cs_param;
      ObCOSSTableV2 *co_sstable = nullptr;
      ObSSTable *sstable = nullptr;

      if (OB_FAIL(cs_param.init_for_empty_major_sstable(tablet_id, storage_schema, snapshot_version, idx, has_all_cg))) {
        LOG_WARN("failed to build table cs param for column store", K(ret), K(tablet_id), K(cg_schema));
      } else if (OB_FAIL(create_sstable<ObCOSSTableV2>(cs_param, allocator, co_handle))) {
        LOG_WARN("failed to create all cg sstable", K(ret), K(cs_param));
      } else if (OB_FAIL(co_handle.get_sstable(sstable))) {
        LOG_WARN("failed to get sstable from handle", K(ret), K(co_handle));
      } else if (OB_ISNULL(co_sstable = static_cast<ObCOSSTableV2 *>(sstable))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null co sstable", K(ret), KPC(sstable));
      } else if (OB_UNLIKELY(!co_sstable->is_empty())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("co sstable should be empty", K(ret), KPC(co_sstable));
      } else {
        table_handle = co_handle;
      }
      break;
    } else {
      // for optimization, we don't need to create empty cg sstable here.
    }
  }
  return ret;
}

bool ObTabletCreateDeleteHelper::is_pure_data_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() == 1 && is_contain(tablet_ids, data_tablet_id);
}

bool ObTabletCreateDeleteHelper::is_mixed_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() > 1 && is_contain(tablet_ids, data_tablet_id);
}

bool ObTabletCreateDeleteHelper::is_pure_aux_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && !info.is_create_bind_hidden_tablets_;
}

bool ObTabletCreateDeleteHelper::is_pure_hidden_tablets(const ObCreateTabletInfo &info)
{
  const ObTabletID &data_tablet_id = info.data_tablet_id_;
  const ObSArray<ObTabletID> &tablet_ids = info.tablet_ids_;
  return tablet_ids.count() >= 1 && !is_contain(tablet_ids, data_tablet_id) && info.is_create_bind_hidden_tablets_;
}
} // namespace storage
} // namespace oceanbase
