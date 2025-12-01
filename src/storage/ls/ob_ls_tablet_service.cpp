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

#include "storage/ls/ob_ls_tablet_service.h"

#include "common/row/ob_row_store.h"
#include "lib/objectpool/ob_server_object_pool.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/utility/utility.h"
#include "logservice/ob_log_base_header.h"
#include "logservice/ob_log_base_type.h"
#include "logservice/ob_log_service.h"
#include "share/scn.h"
#include "observer/report/ob_i_meta_report.h"
#include "share/ob_disk_usage_table_operator.h"
#include "share/rc/ob_tenant_base.h"
#include "share/schema/ob_table_param.h"
#include "share/schema/ob_table_dml_param.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "share/ob_ddl_common.h"
#include "storage/blocksstable/ob_index_block_builder.h"
#include "storage/blocksstable/ob_sstable_meta.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/ob_partition_range_spliter.h"
#include "storage/ob_query_iterator_factory.h"
#include "storage/ob_relative_table.h"
#include "storage/ob_row_reshape.h"
#include "storage/ob_storage_struct.h"
#include "storage/ob_storage_table_guard.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/access/ob_table_scan_range.h"
#include "storage/access/ob_rows_info.h"
#include "storage/access/ob_table_scan_range.h"
#include "storage/access/ob_rows_info.h"
#include "storage/access/ob_table_estimator.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet_binding_helper.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/tablet/ob_tablet_create_sstable_param.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/tablet/ob_tablet_service_clog_replay_executor.h"
#include "storage/tablet/ob_tablet_status.h"
#include "storage/tablet/ob_tablet_slog_helper.h"
#include "storage/tablet/ob_tablet_persister.h"
#include "storage/tx/ob_trans_define.h"
#include "storage/tx/ob_trans_part_ctx.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tablet_map_key.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/slog/ob_storage_log_replayer.h"
#include "storage/slog/ob_storage_log_struct.h"
#include "storage/slog/ob_storage_logger.h"
#include "share/ob_lob_access_utils.h"
#include "observer/table_load/ob_table_load_service.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_store_trans_px_writer.h"
#include "observer/ob_server_event_history_table_operator.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "storage/concurrency_control/ob_data_validation_service.h"
#include "storage/lob/ob_lob_tablet_dml.h"

using namespace oceanbase::share;
using namespace oceanbase::common;
using namespace oceanbase::transaction;
using namespace oceanbase::blocksstable;
using namespace oceanbase::observer;

namespace oceanbase
{
namespace storage
{
using namespace mds;

ERRSIM_POINT_DEF(EN_CREATE_EMPTY_SHELL_TABLET_ERROR);

ObLSTabletService::ObLSTabletService()
  : ls_(nullptr),
    tx_data_memtable_mgr_(),
    tx_ctx_memtable_mgr_(),
    lock_memtable_mgr_(),
    mds_table_mgr_(),
    tablet_id_set_(),
    bucket_lock_(),
    rs_reporter_(nullptr),
    allow_to_read_mgr_(),
    is_inited_(false),
    is_stopped_(false)
{
}

ObLSTabletService::~ObLSTabletService()
{
}

int ObLSTabletService::init(
    ObLS *ls,
    observer::ObIMetaReport *rs_reporter)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_ISNULL(ls) || OB_ISNULL(rs_reporter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls), KP(rs_reporter));
  } else if (OB_FAIL(tablet_id_set_.init(ObTabletCommon::BUCKET_LOCK_BUCKET_CNT, MTL_ID()))) {
    LOG_WARN("fail to init tablet id set", K(ret));
  } else if (OB_FAIL(bucket_lock_.init(ObTabletCommon::BUCKET_LOCK_BUCKET_CNT,
      ObLatchIds::TABLET_BUCKET_LOCK, "TabletSvrBucket", MTL_ID()))) {
    LOG_WARN("failed to init bucket lock", K(ret));
  } else if (OB_FAIL(set_allow_to_read_(ls))) {
    LOG_WARN("failed to set allow to read", K(ret));
  } else if (OB_FAIL(mds_table_mgr_.init(ls))) {
    LOG_WARN("fail to init mds table mgr", KR(ret));
  } else {
    ls_ = ls;
    rs_reporter_ = rs_reporter;
    is_stopped_ = false;
    is_inited_ = true;
  }

  if (OB_UNLIKELY(!is_inited_)) {
    destroy();
  }

  return ret;
}

void ObLSTabletService::destroy()
{
  delete_all_tablets();
  tablet_id_set_.destroy();
  tx_data_memtable_mgr_.destroy();
  tx_ctx_memtable_mgr_.destroy();
  lock_memtable_mgr_.destroy();
  mds_table_mgr_.destroy();
  bucket_lock_.destroy();
  rs_reporter_ = nullptr;
  ls_= nullptr;
  is_stopped_ = false;
  is_inited_ = false;
}

int ObLSTabletService::stop()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    is_stopped_ = true;
  }
  return ret;
}

int ObLSTabletService::offline()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(offline_gc_tablet_for_create_or_transfer_in_abort_())) {
    LOG_WARN("failed to offline_gc_tablet_for_create_or_transfer_in_abort", K(ret));
  } else {
    DestroyMemtableAndMemberAndMdsTableOperator clean_mem_op(this);
    if (OB_FAIL(tablet_id_set_.foreach(clean_mem_op))) {
      LOG_WARN("fail to clean memtables", K(ret), "cur_tablet_id", clean_mem_op.cur_tablet_id_);
    }
    mds_table_mgr_.offline();
  }
  return ret;
}

int ObLSTabletService::online()
{
  return OB_SUCCESS;
}

int ObLSTabletService::replay(
    const void *buffer,
    const int64_t nbytes,
    const palf::LSN &lsn,
    const SCN &scn)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;
  logservice::ObLogBaseHeader base_header;
  common::ObTabletID tablet_id;
  const char *log_buf = static_cast<const char *>(buffer);
  ObTabletServiceClogReplayExecutor replayer_executor;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(base_header.deserialize(log_buf, nbytes, pos))) {
    LOG_WARN("log base header deserialize error", K(ret));
  } else if (logservice::ObLogBaseType::STORAGE_SCHEMA_LOG_BASE_TYPE != base_header.get_log_type()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("log type not supported", K(ret), "log_type", base_header.get_log_type());
  } else if (OB_FAIL(tablet_id.deserialize(log_buf, nbytes, pos))) {
    LOG_WARN("fail to deserialize tablet id", K(ret), KP(log_buf), K(nbytes), K(pos));
  } else if (OB_FAIL(replayer_executor.init(log_buf, nbytes, pos, scn))) {
    LOG_WARN("failed to init replayer", K(ret), KP(log_buf), K(nbytes), K(pos), K(lsn), K(scn));
  } else if (OB_FAIL(replayer_executor.execute(scn, ls_->get_ls_id(), tablet_id))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // TODO (gaishun.gs): unify multi data replay logic
      LOG_INFO("tablet does not exist, skip", K(ret), K(replayer_executor));
    } else if (OB_TIMEOUT == ret) {
      LOG_INFO("replace timeout errno", KR(ret), K(replayer_executor));
      ret = OB_EAGAIN;
    } else {
      LOG_WARN("failed to replay", K(ret), K(replayer_executor));
    }
  }

  return ret;
}

void ObLSTabletService::switch_to_follower_forcedly()
{
  // TODO
}

int ObLSTabletService::switch_to_leader()
{
  int ret = OB_SUCCESS;
  //TODO
  return ret;
}

int ObLSTabletService::switch_to_follower_gracefully()
{
  int ret = OB_SUCCESS;
  //TODO
  return ret;
}

int ObLSTabletService::resume_leader()
{
  int ret = OB_SUCCESS;
  //TODO
  return ret;
}

int ObLSTabletService::flush(SCN &recycle_scn)
{
  UNUSED(recycle_scn);
  return OB_SUCCESS;
}

SCN ObLSTabletService::get_rec_scn()
{
  return SCN::max_scn();
}

int ObLSTabletService::prepare_for_safe_destroy()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(delete_all_tablets())) {
    LOG_WARN("fail to delete all tablets", K(ret));
  }
#ifdef ERRSIM
  if (!ls_->get_ls_id().is_sys_ls()) {
    SERVER_EVENT_SYNC_ADD("ls_tablet_service", "after_delete_all_tablets",
                          "tenant_id", MTL_ID(),
                          "ls_id", ls_->get_ls_id().id());
    DEBUG_SYNC(AFTER_LS_GC_DELETE_ALL_TABLETS);
  }
#endif
  return ret;
}

int ObLSTabletService::safe_to_destroy(bool &is_safe)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  is_safe = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    int64_t tx_data_memtable_mgr_ref = tx_data_memtable_mgr_.get_ref();
    int64_t tx_ctx_memtable_mgr_ref = tx_ctx_memtable_mgr_.get_ref();
    int64_t lock_memtable_mgr_ref = lock_memtable_mgr_.get_ref();
    int64_t mds_table_mgr_ref = mds_table_mgr_.get_ref();
    if (0 != tx_data_memtable_mgr_ref || 0 != tx_ctx_memtable_mgr_ref
        || 0 != lock_memtable_mgr_ref || 0 != mds_table_mgr_ref) {
      if (REACH_TIME_INTERVAL(60L * 1000000)) {  // 60s
        LOG_WARN("inner tablet memtable mgr can't destroy", K(tx_data_memtable_mgr_ref),
          K(tx_ctx_memtable_mgr_ref), K(lock_memtable_mgr_ref), K(mds_table_mgr_ref));
      }
      is_safe = false;
    } else {
      tx_data_memtable_mgr_.destroy();
      tx_ctx_memtable_mgr_.destroy();
      lock_memtable_mgr_.destroy();
      mds_table_mgr_.destroy();
    }
    if (is_safe) {
       bool is_wait_gc = false;
      if (OB_FAIL(t3m->has_meta_wait_gc(is_wait_gc))) {
        LOG_WARN("failed to check has_meta_wait_gc", K(ret));
        is_safe = false;
      } else {
        is_safe = !is_wait_gc;
      }
    }
  }

  return ret;
}

int ObLSTabletService::delete_all_tablets()
{
  int ret = OB_SUCCESS;
  if (OB_NOT_NULL(ls_)) {
    const ObLSID &ls_id = ls_->get_ls_id();
    ObSArray<ObTabletID> tablet_id_array;
    GetAllTabletIDOperator op(tablet_id_array);

    ObTimeGuard time_guard("ObLSTabletService::delete_all_tablets", 1_s);
    common::ObBucketWLockAllGuard lock_guard(bucket_lock_);
    time_guard.click("Lock");
    if (OB_FAIL(tablet_id_set_.foreach(op))) {
      LOG_WARN("failed to traverse tablet id set", K(ret), K(ls_id));
    } else if (tablet_id_array.empty()) {
      // tablet id array is empty, do nothing
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
        const ObTabletID &tablet_id = tablet_id_array.at(i);
        if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
          LOG_ERROR("failed to do remove tablet", K(ret), K(ls_id), K(tablet_id));
          ob_usleep(1000 * 1000);
          ob_abort();
        }
      }
      time_guard.click("RemoveTablet");

      if (OB_SUCC(ret)) {
        report_tablet_to_rs(tablet_id_array);
        time_guard.click("ReportToRS");
      }
    }
  }
  return ret;
}

int ObLSTabletService::remove_tablet(const ObTabletHandle& tablet_handle)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet", K(ret), K(tablet_handle));
  } else {
    const ObTablet &target_tablet = *(tablet_handle.get_obj());
    const ObLSID ls_id = ls_->get_ls_id();
    const ObTabletID tablet_id = target_tablet.get_tablet_meta().tablet_id_;
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTabletHandle cur_tablet_handle;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());

    if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, cur_tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        LOG_INFO("tablet does not exist, maybe already deleted", K(ret), K(key));
      } else {
        LOG_WARN("failed to get tablet", K(ret), K(key));
      }
    } else if (&target_tablet != cur_tablet_handle.get_obj()) {
      ret = OB_EAGAIN;
      LOG_INFO("tablet object has been changed, need retry", K(ret), K(key), K(target_tablet), KPC(cur_tablet_handle.get_obj()));
    } else {
      if (OB_FAIL(ObTabletSlogHelper::write_remove_tablet_slog(ls_id, tablet_id))) {
        LOG_WARN("failed to write remove tablet slog", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(tablet_handle.get_obj()->wait_release_memtables())) {
        LOG_ERROR("failed to release memtables", K(ret), K(tablet_id));
      } else if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
        LOG_ERROR("failed to do remove tablet", K(ret), K(ls_id), K(tablet_id));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else {
        report_tablet_to_rs(tablet_id);
      }
    }
  }
  return ret;
}

int ObLSTabletService::remove_tablets(const common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  const int64_t tablet_cnt = tablet_id_array.count();
  ObSArray<uint64_t> all_tablet_id_hash_array;
  ObSArray<ObTabletID> tablet_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(0 == tablet_cnt)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args, tablet id array is empty", K(ret), K(tablet_id_array));
  } else if (OB_FAIL(all_tablet_id_hash_array.reserve(tablet_cnt))) {
    LOG_WARN("failed to reserve memory for array", K(ret), K(tablet_cnt));
  } else if (OB_FAIL(tablet_ids.reserve(tablet_cnt))) {
    LOG_WARN("failed to reserve memory for array", K(ret), K(tablet_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
      const ObTabletID &tablet_id = tablet_id_array.at(i);
      if (OB_FAIL(all_tablet_id_hash_array.push_back(tablet_id.hash()))) {
        LOG_WARN("failed to push back tablet id hash value", K(ret), K(tablet_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    ObMetaDiskAddr tablet_addr;
    ObTimeGuard time_guard("ObLSTabletService::remove_tablets", 1_s);
    ObMultiBucketLockGuard lock_guard(bucket_lock_, true/*is_write_lock*/);
    if (OB_FAIL(lock_guard.lock_multi_buckets(all_tablet_id_hash_array))) {
      LOG_WARN("failed to lock multi buckets", K(ret));
    } else {
      time_guard.click("Lock");

      const share::ObLSID &ls_id = ls_->get_ls_id();
      ObTabletHandle tablet_handle;
      ObTabletMapKey key;
      key.ls_id_ = ls_id;

      // check tablet existence
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_cnt; ++i) {
        const ObTabletID &tablet_id = tablet_id_array.at(i);
        key.tablet_id_ = tablet_id;
        tablet_addr.reset();
        if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
          if (OB_TABLET_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
            LOG_INFO("tablet does not exist, maybe already deleted", K(ret), K(key));
          } else {
            LOG_WARN("failed to get tablet", K(ret), K(key));
          }
        } else if (OB_FAIL(tablet_handle.get_obj()->wait_release_memtables())) {
          LOG_ERROR("failed to release memtables", K(ret), K(tablet_id));
        } else if (OB_FAIL(tablet_handle.get_obj()->get_meta_disk_addr(tablet_addr))) {
          LOG_WARN("failed to get tablet addr", K(ret), K(key));
        } else if (!tablet_addr.is_disked()) {
          if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
            LOG_WARN("failed to remove non disked tablet from memory", K(ret), K(key));
          } else {
            FLOG_INFO("succeeded to remove non disked tablet from memory", K(ret), K(key));
          }
        } else if (OB_FAIL(tablet_ids.push_back(tablet_id))) {
          LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
        }
      }

      // write slog and do remove tablet
      if (OB_FAIL(ret)) {
      } else if (tablet_ids.empty()) {
        LOG_INFO("all tablets already deleted, do nothing", K(ret), K(ls_id), K(tablet_id_array));
      } else if (OB_FAIL(ObTabletSlogHelper::write_remove_tablet_slog(ls_id, tablet_ids))) {
        LOG_WARN("failed to write remove tablet slog", K(ret), K(ls_id), K(tablet_ids));
      } else {
        time_guard.click("WrSlog");
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
          const ObTabletID &tablet_id = tablet_ids.at(i);
          if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
            LOG_ERROR("failed to do remove tablet", K(ret), K(ls_id), K(tablet_id));
            ob_usleep(1000 * 1000);
            ob_abort();
          }
        }

        if (OB_SUCC(ret)) {
          report_tablet_to_rs(tablet_ids);
          time_guard.click("ReportToRS");
        }
      }
    }
  }

  return ret;
}

int ObLSTabletService::do_remove_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("RmTabletLock", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");
  if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
    LOG_WARN("fail to remove tablet with lock", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

// TODO(yunshan.tys) cope with failure of deleting tablet (tablet hasn't been loaded from disk)
int ObLSTabletService::inner_remove_tablet(
    const share::ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);

  if (OB_FAIL(tablet_id_set_.erase(tablet_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // tablet id is already erased
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to erase tablet id from set", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (OB_FAIL(tx_svr->remove_tablet(tablet_id, ls_id))) {
    LOG_ERROR("fail to remove tablet to ls cache", K(ret), K(tablet_id), K(ls_id));
  }

  if (OB_SUCC(ret)) {
    // loop retry to delete tablet from t3m
    while (OB_FAIL(t3m->del_tablet(key))) {
      if (REACH_TIME_INTERVAL(10 * 1000 * 1000)) {
        LOG_ERROR("failed to delete tablet from t3m", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    FLOG_INFO("succeeded to remove tablet", K(ret), K(ls_id), K(tablet_id));
  }

  return ret;
}

int ObLSTabletService::get_tablet(
    const ObTabletID &tablet_id,
    ObTabletHandle &handle,
    const int64_t timeout_us,
    const ObMDSGetTabletMode mode)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()
      || mode < ObMDSGetTabletMode::READ_ALL_COMMITED)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(mode));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle, timeout_us, mode,
      ObTransVersion::MAX_TRANS_VERSION))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_DEBUG("failed to check and get tablet", K(ret), K(key), K(timeout_us), K(mode));
    } else {
      LOG_WARN("failed to check and get tablet", K(ret), K(key), K(timeout_us), K(mode));
    }
  }

  return ret;
}

int ObLSTabletService::get_tablet_addr(const ObTabletMapKey &key, ObMetaDiskAddr &addr)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    // 加读锁保证ckpt与对tablet的操作串行, 防止ckpt时tablet操作处在
    // slog已经写入而tablet addr未更新的状态，导致slog丢失
    ObBucketHashRLockGuard lock_guard(bucket_lock_, key.tablet_id_.hash());
    if (OB_FAIL(t3m->get_tablet_addr(key, addr))) {
      LOG_WARN("fail to get tablet addr", K(ret), K(key));
    }
  }

  return ret;
}

void ObLSTabletService::report_tablet_to_rs(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = ls_->get_ls_id();

  if (tablet_id.is_ls_inner_tablet()) {
    // no need to report for ls inner tablet
  } else if (OB_FAIL(rs_reporter_->submit_tablet_update_task(tenant_id, ls_id, tablet_id))) {
    LOG_WARN("failed to report tablet info", KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
  }
}

void ObLSTabletService::report_tablet_to_rs(
    const common::ObIArray<common::ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = ls_->get_ls_id();

  // ignore ret on purpose
  for (int64_t i = 0; i < tablet_id_array.count(); ++i) {
    const common::ObTabletID &tablet_id = tablet_id_array.at(i);
    if (tablet_id.is_ls_inner_tablet()) {
      // no need to report for ls inner tablet
      continue;
    } else if (OB_FAIL(rs_reporter_->submit_tablet_update_task(tenant_id, ls_id, tablet_id))) {
      LOG_WARN("failed to report tablet info", KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
    }
  }
}

int ObLSTabletService::table_scan(ObTabletHandle &tablet_handle, ObTableScanIterator &iter, ObTableScanParam &param)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_table_scan_begin);
  bool allow_to_read = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (FALSE_IT(allow_to_read_mgr_.load_allow_to_read_info(allow_to_read))) {
  } else if (!allow_to_read) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("ls is not allow to read", K(ret), KPC(ls_));
  } else if (OB_FAIL(prepare_scan_table_param(param, *(MTL(ObTenantSchemaService*)->get_schema_service())))) {
    LOG_WARN("failed to prepare scan table param", K(ret), K(param));
  } else if (OB_FAIL(inner_table_scan(tablet_handle, iter, param))) {
    LOG_WARN("failed to do table scan", K(ret), KP(&iter), K(param));
  }
  NG_TRACE(S_table_scan_end);

  return ret;
}

int ObLSTabletService::table_rescan(ObTabletHandle &tablet_handle, ObTableScanParam &param, ObNewRowIterator *result)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_table_rescan_begin);
  bool allow_to_read = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K(result), K_(is_inited));
  } else if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret));
  } else if (FALSE_IT(allow_to_read_mgr_.load_allow_to_read_info(allow_to_read))) {
  } else if (!allow_to_read) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("ls is not allow to read", K(ret), KPC(ls_));
  } else if (OB_FAIL(prepare_scan_table_param(param, *(MTL(ObTenantSchemaService*)->get_schema_service())))) {
    LOG_WARN("failed to prepare scan table param", K(ret), K(result), K(param));
  } else {
    ObTableScanIterator *iter = static_cast<ObTableScanIterator*>(result);
    if (OB_FAIL(inner_table_scan(tablet_handle, *iter, param))) {
      LOG_WARN("failed to do table scan", K(ret), K(result), K(param));
    }
  }
  NG_TRACE(S_table_rescan_end);
  return ret;
}

int ObLSTabletService::refresh_tablet_addr(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  while (OB_SUCC(ret)) {
    ret = tablet_id_set_.set(tablet_id);
    if (OB_SUCC(ret)) {
      break;
    } else if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ob_usleep(100 * 1000);
      if (REACH_COUNT_INTERVAL(100)) {
        LOG_ERROR("no memory for tablet id set, retry", K(ret), K(tablet_id));
      }
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to set tablet id set", K(ret), K(tablet_id));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle))) {
    LOG_WARN("failed to add tablet to meta mem mgr", K(ret), K(key), K(tablet_handle));
  }

  return ret;
}

int ObLSTabletService::trim_old_tablets(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle_head;
  ObTimeGuard time_guard("ObLSTabletService::trim_old_tablets", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");

  if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle_head))) {
    LOG_WARN("failed to check and get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle_head.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle_head.get_obj());
  } else if (OB_UNLIKELY(!tablet_handle_head.get_obj()->get_tablet_meta().has_next_tablet_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("doesn't have old tablet", K(ret), "tablet_meta", tablet_handle_head.get_obj()->get_tablet_meta());
  } else {
    time_guard.click("GetTablet");
    ObTablet *tablet_head = tablet_handle_head.get_obj();
    ObTabletHandle new_tablet_handle;
    ObLSID ls_id = tablet_head->get_tablet_meta().ls_id_;

    tablet_head->trim_tablet_list();
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);

    if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tablet_head, new_tablet_handle))) {
      LOG_WARN("fail to persist and transfor tablet", K(ret), KPC(tablet_head), K(new_tablet_handle));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->get_tablet_addr())) {
    } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
      LOG_WARN("failed to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
    } else if (FALSE_IT(time_guard.click("WrSlog"))) {
    } else if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(
        key, tablet_handle_head, new_tablet_handle))) {
      LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle_head), K(new_tablet_handle));
      ob_usleep(1000 * 1000);
      ob_abort();
    } else {
      time_guard.click("CASwap");
      FLOG_INFO("succeeded to trim tablets list", K(key), K(disk_addr));
    }
  }

  return ret;
}

int ObLSTabletService::rollback_rebuild_tablet(const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle_head;
  ObTimeGuard time_guard("ObLSTabletService::rollback_rebuild_tablet", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");

  if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle_head))) {
    LOG_WARN("failed to check and get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle_head.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle_head.get_obj());
  } else {
    time_guard.click("GetTablet");

    ObTablet *tablet_head = tablet_handle_head.get_obj();
    ObTabletHandle next_tablet_handle;
    ObMetaObj<ObTablet> meta_obj;
    tablet_head->get_next_tablet_guard().get_obj(meta_obj);
    next_tablet_handle.set_obj(meta_obj);
    next_tablet_handle.set_wash_priority(WashTabletPriority::WTP_LOW);
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObMetaDiskAddr disk_addr;
    ObTabletHandle new_tablet_handle;

    if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(
        *next_tablet_handle.get_obj(), new_tablet_handle))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), K(next_tablet_handle), K(new_tablet_handle));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->get_tablet_addr())) {
    } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(
        key.ls_id_, tablet_id, disk_addr))) {
      LOG_WARN("failed to write update tablet slog", K(ret), K(key), K(disk_addr));
    } else if (FALSE_IT(time_guard.click("WrSlog"))) {
    } else if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(
        key, tablet_handle_head, new_tablet_handle))) {
      LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle_head), K(new_tablet_handle));
      ob_usleep(1000 * 1000);
      ob_abort();
    } else {
      time_guard.click("CASwap");
      FLOG_INFO("succeeded to rollback rebuild", K(key), K(disk_addr));
    }
  }

  return ret;
}

int ObLSTabletService::rebuild_tablet_with_old(
    const ObMigrationTabletParam &mig_tablet_param,
    ObTabletHandle &tablet_guard)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObLSTabletService::rebuild_tablet_with_old", 1_s);
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "RebuildTablet"));
  ObTabletHandle old_tablet_hdl;
  ObTabletHandle tmp_tablet_hdl;
  ObTabletHandle new_tablet_hdl;
  ObTablet *tmp_tablet = nullptr;
  ObFreezer *freezer = ls_->get_freezer();
  ObMetaDiskAddr disk_addr;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  const common::ObTabletID &tablet_id = mig_tablet_param.tablet_id_;
  const share::ObLSID &ls_id = mig_tablet_param.ls_id_;
  const ObTabletMapKey key(ls_id, tablet_id);

  if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (FALSE_IT(time_guard.click("GetTablet"))) {
  } else if (old_tablet_hdl.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_hdl.get_obj());
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else if (FALSE_IT(tmp_tablet = tmp_tablet_hdl.get_obj())) {
  } else if (OB_FAIL(tmp_tablet->init_with_migrate_param(allocator, mig_tablet_param, true/*is_update*/, freezer))) {
    LOG_WARN("failed to init tablet", K(ret), K(mig_tablet_param));
  } else if (FALSE_IT(tmp_tablet->set_next_tablet_guard(tablet_guard))) {
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet, new_tablet_hdl))) {
    LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
  } else if (FALSE_IT(time_guard.click("Persist"))) {
  } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
  } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
    LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
  } else if (FALSE_IT(time_guard.click("WrSlog"))) {
  } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_hdl, new_tablet_hdl))) {
    LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(old_tablet_hdl), K(new_tablet_hdl));
    ob_usleep(1000 * 1000);
    ob_abort();
  } else if (OB_FAIL(new_tablet_hdl.get_obj()->start_ddl_if_need())) {
    LOG_WARN("start ddl if need failed", K(ret), K(key));
  } else {
    time_guard.click("CASwap");
    LOG_INFO("rebuild tablet with old succeed", K(ret), K(key), K(disk_addr));
  }
  return ret;
}

int ObLSTabletService::migrate_update_tablet(
    const ObMigrationTabletParam &mig_tablet_param)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObLSTabletService::migrate_update_tablet", 1_s);
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "MigUpTab"));
  const common::ObTabletID &tablet_id = mig_tablet_param.tablet_id_;
  const share::ObLSID &ls_id = mig_tablet_param.ls_id_;
  const ObTabletMapKey key(mig_tablet_param.ls_id_, mig_tablet_param.tablet_id_);
  ObTabletHandle old_tablet_hdl;
  ObTabletHandle tmp_tablet_hdl;
  ObTabletHandle new_tablet_hdl;
  ObTablet *new_tablet = nullptr;
  ObMetaDiskAddr disk_addr;
  ObFreezer *freezer = ls_->get_freezer();
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (FALSE_IT(time_guard.click("GetTablet"))) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else if (FALSE_IT(new_tablet = tmp_tablet_hdl.get_obj())) {
  } else if (OB_FAIL(new_tablet->init_with_migrate_param(allocator, mig_tablet_param, true/*is_update*/, freezer))) {
    LOG_WARN("failed to init tablet", K(ret), K(mig_tablet_param));
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*new_tablet, new_tablet_hdl))) {
    LOG_WARN("fail to persist and transform tablet", K(ret), KPC(new_tablet), K(new_tablet_hdl));
  } else if (FALSE_IT(time_guard.click("Persist"))) {
  } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
  } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
    LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
  } else if (FALSE_IT(time_guard.click("WrSlog"))) {
  } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_hdl, new_tablet_hdl))) {
    LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(old_tablet_hdl), K(new_tablet_hdl));
    ob_usleep(1000 * 1000);
    ob_abort();
  } else if (OB_FAIL(new_tablet_hdl.get_obj()->start_ddl_if_need())) {
    LOG_WARN("start ddl if need failed", K(ret));
  } else {
    time_guard.click("CASwap");
    LOG_INFO("migrate update tablet succeed", K(ret), K(key), K(disk_addr));
  }

  return ret;
}

int ObLSTabletService::migrate_create_tablet(
    const ObMigrationTabletParam &mig_tablet_param,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObLSTabletService::migrate_create_tablet", 1_s);
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "MigCreateTab"));
  const share::ObLSID &ls_id = mig_tablet_param.ls_id_;
  const common::ObTabletID &tablet_id = mig_tablet_param.tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObFreezer *freezer = ls_->get_freezer();
  ObTabletHandle tmp_tablet_hdl;
  ObTabletHandle tablet_handle;
  ObMetaDiskAddr disk_addr;
  ObTransService *tx_svr = MTL(ObTransService*);

  if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to create temporary tablet", K(ret), K(key));
  } else if (OB_FAIL(tmp_tablet_hdl.get_obj()->init_with_migrate_param(allocator, mig_tablet_param, false/*is_update*/, freezer))) {
    LOG_WARN("fail to init tablet", K(ret), K(mig_tablet_param));
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet_hdl.get_obj(), tablet_handle))) {
    LOG_WARN("fail to persist and transform tablet", K(ret), K(tmp_tablet_hdl), K(tablet_handle));
  } else if (FALSE_IT(time_guard.click("Persist"))) {
  } else if (FALSE_IT(disk_addr = tablet_handle.get_obj()->tablet_addr_)) {
  } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
    LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
  } else if (FALSE_IT(time_guard.click("WrSlog"))) {
  } else if (OB_FAIL(refresh_tablet_addr(ls_id, tablet_id, tablet_handle))) {
    LOG_WARN("failed to refresh tablet addr", K(ret), K(ls_id), K(tablet_id), K(lbt()));
    ob_usleep(1000 * 1000);
    ob_abort();
  } else if (OB_FAIL(tablet_handle.get_obj()->start_ddl_if_need())) {
    LOG_WARN("start ddl if need failed", K(ret));
  } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
    LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_handle));
  } else {
    LOG_INFO("migrate create tablet succeed", K(ret), K(key), K(disk_addr));
  }

  if (OB_SUCC(ret)) {
    handle = tablet_handle;
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
      LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
    }
  }

  return ret;
}

int ObLSTabletService::refresh_memtable_for_ckpt(
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &cur_addr,
    ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!old_addr.is_equal_for_persistence(cur_addr))) {
    ret = OB_NOT_THE_OBJECT;
    LOG_WARN("the old tablet has been replaced", K(ret), K(old_addr), K(cur_addr));
  } else if (OB_UNLIKELY(old_addr != cur_addr)) {
    // memtables were updated
    if (OB_FAIL(new_tablet_handle.get_obj()->refresh_memtable_and_update_seq(cur_addr.seq()))) {
      LOG_WARN("fail to pull newest memtables", K(ret), K(old_addr), K(cur_addr), K(new_tablet_handle));
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_checkpoint(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    const ObMetaDiskAddr &new_addr,
    const bool is_replay_old,
    ObTabletHandle &new_handle)
{
  int ret = OB_SUCCESS;

  ObTimeGuard time_guard("UpdateTabletCKPT", 3_s);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet svr hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid()
                      || !old_addr.is_valid()
                      || !new_addr.is_valid()
                      || !new_addr.is_block()
                      || (!is_replay_old && !new_handle.is_valid()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(new_addr), K(new_handle));
  } else {
    common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "CKPTUpdate"));
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObTabletHandle tablet_handle;
    ObMetaDiskAddr addr;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.tablet_id_.hash());
    time_guard.click("Lock");
    if (OB_FAIL(t3m->get_tablet_addr(key, addr))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      }
      LOG_WARN("fail to get old tablet addr", K(ret), K(key));
    } else if (!is_replay_old) {
      if (OB_FAIL(t3m->get_tablet(WashTabletPriority::WTP_LOW, key, tablet_handle))) {
        LOG_WARN("fail to get tablet", K(ret), K(key));
      } else if (FALSE_IT(time_guard.click("GetOld"))) {
      } else if (OB_FAIL(refresh_memtable_for_ckpt(old_addr, addr, new_handle))) {
        LOG_WARN("fail to update tablet", K(ret), K(old_addr), K(addr), K(new_handle));
      } else if (FALSE_IT(time_guard.click("UpdateTablet"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, new_handle))) {
        LOG_WARN("fail to compare and swap tablet", K(ret), K(tablet_handle), K(new_handle));
      }
    } else {
      time_guard.click("GetOld");
      if (OB_UNLIKELY(addr != old_addr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("the old tablet has been replaced, which is not allowed during upgrade",
            K(ret), K(addr), K(old_addr));
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_addr, new_addr))) {
        LOG_WARN("fail to compare and swap tablet", K(ret), K(old_addr), K(new_addr));
      }
    }

    if (OB_SUCC(ret)) {
      time_guard.click("CASwap");
      FLOG_INFO("succeeded to update tablet ckpt", K(key), K(old_addr), K(new_addr),
          K(is_replay_old));
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_table_store(
    const ObTabletHandle &old_tablet_handle,
    const ObIArray<storage::ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "UpTabStore"));
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet svr hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!old_tablet_handle.is_valid() || 0 == tables.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old tablet handle is invalid", K(ret), K(old_tablet_handle), K(tables.count()));
  } else {
    ObTablet *old_tablet = old_tablet_handle.get_obj();
    const common::ObTabletID &tablet_id = old_tablet->get_tablet_meta().tablet_id_;

    ObTimeGuard time_guard("ObLSTabletService::ReplaceSSTable", 1_s);
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");

    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObTabletHandle tablet_handle;
    if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("this tablet has been deleted, skip it", K(ret), K(tablet_id));
      } else {
        LOG_WARN("fail to get tablet", K(ret));
      }
    } else if (tablet_handle.get_obj() != old_tablet) {
      ret = OB_EAGAIN;
      LOG_WARN("tablet has changed, skip it", K(ret), K(tablet_handle), K(old_tablet_handle));
    } else if (old_tablet->is_empty_shell()) {
      LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet);
    } else {
      time_guard.click("GetTablet");
      ObTabletHandle tmp_tablet_hdl;
      ObTabletHandle new_tablet_hdl;
      ObTablet *tmp_tablet = nullptr;
      const share::ObLSID &ls_id = ls_->get_ls_id();
      const ObTabletMapKey key(ls_id, tablet_id);
      ObMetaDiskAddr disk_addr;

      if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
        LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
      } else if (FALSE_IT(tmp_tablet = tmp_tablet_hdl.get_obj())) {
      } else if (OB_FAIL(tmp_tablet->init_for_defragment(allocator, tables, *old_tablet))) {
        LOG_WARN("fail to init new tablet", K(ret), KPC(old_tablet));
      } else if (FALSE_IT(time_guard.click("InitTablet"))) {
      } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet, new_tablet_hdl))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
      } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
        LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
      } else if (FALSE_IT(time_guard.click("WrSlog"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, new_tablet_hdl))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(old_tablet_handle), K(new_tablet_hdl));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else {
        time_guard.click("CASwap");
        LOG_INFO("succeeded to build new tablet", K(ret), K(disk_addr),
            K(new_tablet_hdl), KPC(new_tablet_hdl.get_obj()));
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_table_store(
    const common::ObTabletID &tablet_id,
    const ObUpdateTableStoreParam &param,
    ObTabletHandle &handle)
{
  static const int64_t MAX_RETRY_CNT = 1000;
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "UpTabStore"));
  const share::ObLSID &ls_id = ls_->get_ls_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTimeGuard time_guard("ObLSTabletService::UpdateTableStore", 1_s);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(param));
  } else {
    ObTabletHandle old_tablet_hdl;
    ObTabletHandle tmp_tablet_hdl;
    ObMetaDiskAddr old_tablet_addr;
    int64_t retry_count = 0;
    bool first_time = true;
    do{
      ret = OB_SUCCESS;
      old_tablet_hdl.reset();
      tmp_tablet_hdl.reset();
      old_tablet_addr.reset();
      if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_TABLET_NOT_EXIST;
        } else {
          LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
        }
      } else if (FALSE_IT(time_guard.click("AcqTmpTab"))) {
      } else if (OB_FAIL(get_tablet_and_address(key, old_tablet_hdl, old_tablet_addr, time_guard))) {
        LOG_WARN("fail to get old tablet and acquire new tablet buffer", K(ret), K(key));
      } else if (old_tablet_hdl.get_obj()->is_empty_shell()) {
        handle = old_tablet_hdl;
      } else {
        ObTabletHandle new_tablet_hdl;
        ObTablet *tmp_tablet = tmp_tablet_hdl.get_obj();
        ObTablet *old_tablet = old_tablet_hdl.get_obj();
        if (OB_FAIL(tmp_tablet->init_for_merge(allocator, param, *old_tablet))) {
          LOG_WARN("failed to init tablet", K(ret), K(param), KPC(old_tablet));
        } else if (FALSE_IT(time_guard.click("InitNew"))) {
        } else if (OB_FAIL(acquire_new_tablet(key, tmp_tablet_hdl, new_tablet_hdl, time_guard))) {
          LOG_WARN("fail to acquire new tablet", K(ret), K(key), K(old_tablet_hdl));
        } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet, new_tablet_hdl))) {
          LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
        } else {
          time_guard.click("PersistNew");
          ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
          ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
          time_guard.click("WLock");
          ObMetaDiskAddr &new_addr = new_tablet_hdl.get_obj()->tablet_addr_;
          ObMetaDiskAddr cur_addr;
          if (OB_FAIL(MTL(ObTenantMetaMemMgr *)->get_tablet_addr(key, cur_addr))) {
            LOG_WARN("fail to get tablet addr from t3m", K(ret), K(key));
          } else if (old_tablet_addr != cur_addr) {
            ret = OB_EAGAIN;
            LOG_INFO("The tablet address in tablet pointer has changed, please try again", K(ret), K(cur_addr),
                K(old_tablet_addr), KPC(old_tablet));
          } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, new_addr))) {
            LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(new_addr));
          } else if (FALSE_IT(time_guard.click("WrSlog"))) {
          } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_hdl, new_tablet_hdl))) {
            LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(old_tablet_hdl), K(new_tablet_hdl));
            ob_usleep(1000 * 1000);
            ob_abort();
          } else {
            handle = new_tablet_hdl;
            time_guard.click("SLOGAndCAS");
            LOG_INFO("succeeded to build new tablet", K(ret), K(key), K(old_tablet_addr), K(cur_addr), K(new_addr), K(param), K(handle));
          }
        }
      }

      if (OB_EAGAIN == ret) {
        ++retry_count;
        if (retry_count >= MAX_RETRY_CNT) {
          LOG_WARN("Reached the maximum retry count but still failed to update tablet table store",
            K(ret), K(key), K(retry_count), K(MAX_RETRY_CNT));
          break;
        } else {
          if (first_time || REACH_TIME_INTERVAL(10_s)) {
            LOG_INFO("Failed due to CAS, retry to update table store", K(ret), K(key), K(retry_count),
              K(MAX_RETRY_CNT));
            first_time = false;
          }
          ob_usleep(100_ms);
        }
      }
    } while (OB_EAGAIN == ret);
  }
  return ret;
}

int ObLSTabletService::update_tablet_mstx(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &old_addr,
    const ObTabletHandle &old_tablet_handle,
    ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTablet *new_tablet = nullptr;
  ObMetaDiskAddr new_addr;

  ObTimeGuard time_guard("ObLSTabletService::update_tablet_mstx", 1_s);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!key.is_valid()
      || !old_addr.is_valid()
      || !old_tablet_handle.is_valid()
      || !new_tablet_handle.is_valid()
      || !(new_tablet = new_tablet_handle.get_obj())->is_valid()
      || !(new_addr = new_tablet->get_tablet_addr()).is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(key), K(old_addr), K(old_tablet_handle),
        K(new_tablet_handle), KPC(new_tablet), K(new_addr));
  } else {
    ObMetaDiskAddr addr;
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);


    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.tablet_id_.hash());
    time_guard.click("Lock");
    ObTabletHandle curr_tablet_handle;
    if (OB_FAIL(direct_get_tablet(key.tablet_id_, curr_tablet_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // deleted, skip
      } else {
        LOG_WARN("fail to get tablet", K(ret), K(key));
      }
    } else if (curr_tablet_handle.get_obj() != old_tablet_handle.get_obj()) {
      // we could never persist this tablet, skip this
      LOG_INFO("tablet has been changed", K(ret), K(key), K(old_addr), K(old_tablet_handle), K(curr_tablet_handle));
    } else if (FALSE_IT(addr = curr_tablet_handle.get_obj()->get_tablet_addr())) {
    } else if (old_addr != addr) { /* the same full tablet obj with different memtables will hit this */
      // Let this updating fail and throw tablet back to the persist queue.
      // It will be tackled correctly in the next round because its address will be compared
      // again in ObFullTabletCreator::persist_tablet()
      ret = common::OB_NOT_THE_OBJECT;
      LOG_WARN("old address has changed, skip update", K(ret), K(key), K(addr), K(old_addr));
    } else {
      time_guard.click("GetOld");
      if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(key.ls_id_, key.tablet_id_, new_addr))) {
        LOG_WARN("fail to write update tablet slog", K(ret), K(key), K(new_addr));
      } else if (FALSE_IT(time_guard.click("WrSlog"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, new_tablet_handle))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(old_tablet_handle), K(new_tablet_handle));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else if (FALSE_IT(time_guard.click("CASwap"))) {
      } else {
        FLOG_INFO("succeeded to update tablet mstx", K(ret),
            K(key), K(old_addr), K(old_tablet_handle), K(new_addr), K(new_tablet_handle));
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_to_empty_shell(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls_->get_ls_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "UpdEmptySh"));
  ObTabletHandle new_tablet_handle;
  ObTabletHandle tmp_tablet_handle;
  ObTabletHandle old_tablet_handle;
  ObTimeGuard time_guard("UpdateTabletToEmptyShell", 3_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());

  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet svr hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (old_tablet_handle.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_handle.get_obj());
  } else if (FALSE_IT(time_guard.click("GetOld"))) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_handle))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else {
    time_guard.click("Acquire");
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObTablet *old_tablet = old_tablet_handle.get_obj();
    ObTablet *tmp_tablet = tmp_tablet_handle.get_obj();
    ObTablet *new_tablet = nullptr;
    ObMetaDiskAddr disk_addr;
    if (OB_FAIL(tmp_tablet->init_empty_shell(*tmp_tablet_handle.get_allocator(), *old_tablet))) {
      LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
    } else if (FALSE_IT(time_guard.click("InitNew"))) {
    } else if (OB_FAIL(ObTabletPersister::transform_empty_shell(*tmp_tablet, new_tablet_handle))) {
      LOG_WARN("fail to transform emtpy shell", K(ret), K(tablet_id));
    } else if (FALSE_IT(time_guard.click("Trans"))) {
    } else if (FALSE_IT(new_tablet = new_tablet_handle.get_obj())) {
    } else if (OB_FAIL(ObTabletSlogHelper::write_empty_shell_tablet_slog(new_tablet, disk_addr))) {
      LOG_WARN("fail to write emtpy shell tablet slog", K(ret), K(tablet_id), K(disk_addr));
    } else if (FALSE_IT(new_tablet->tablet_addr_ = disk_addr)) {
    } else if (FALSE_IT(time_guard.click("WrSlog"))) {
    } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, new_tablet_handle))) {
      LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(old_tablet_handle), K(new_tablet_handle));
      ob_usleep(1000 * 1000);
      ob_abort();
    } else {
      ls_->get_tablet_gc_handler()->set_tablet_gc_trigger();
      time_guard.click("CASwap");
      LOG_INFO("succeeded to build empty shell tablet", K(ret), K(key), K(disk_addr));
    }
  }
  return ret;
}

int ObLSTabletService::update_medium_compaction_info(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "UpMeidumCom"));
  ObTabletHandle old_tablet_handle;
  ObTimeGuard time_guard("ObLSTabletService::update_medium_compaction_info", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_handle))) {
    LOG_WARN("failed to check and get tablet", K(ret), K(tablet_id));
  } else if (old_tablet_handle.get_obj()->is_empty_shell()) {
    handle = old_tablet_handle;
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_handle.get_obj());
  } else {
    time_guard.click("GetTablet");
    ObTabletHandle tmp_tablet_hdl;
    ObTabletHandle new_tablet_hdl;
    ObTablet *tmp_tablet = nullptr;
    ObTablet *old_tablet = old_tablet_handle.get_obj();
    const share::ObLSID &ls_id = ls_->get_ls_id();
    const ObTabletMapKey key(ls_id, tablet_id);
    ObMetaDiskAddr disk_addr;
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

    if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      } else {
        LOG_WARN("failed to acquire tablet", K(ret), K(key));
      }
    } else if (FALSE_IT(tmp_tablet = tmp_tablet_hdl.get_obj())) {
    } else if (OB_FAIL(tmp_tablet->init_with_update_medium_info(allocator, *old_tablet))) {
      LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
    } else if (FALSE_IT(time_guard.click("InitNew"))) {
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet, new_tablet_hdl))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
    } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
      LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
    } else if (FALSE_IT(time_guard.click("WrSlog"))) {
    } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, new_tablet_hdl))) {
      LOG_WARN("failed to compare and swap tablet", K(ret), K(key), K(old_tablet_handle), K(new_tablet_hdl));
    } else if (FALSE_IT(time_guard.click("CASwap"))) {
    } else {
      handle = new_tablet_hdl;
    }
  }
  return ret;
}

int ObLSTabletService::build_new_tablet_from_mds_table(
    const common::ObTabletID &tablet_id,
    const int64_t mds_construct_sequence,
    const share::SCN &flush_scn)
{
  TIMEGUARD_INIT(STORAGE, 10_ms);
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "BuildMSD"));
  const share::ObLSID &ls_id = ls_->get_ls_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle old_tablet_handle;
  ObTabletHandle tmp_tablet_hdl;
  ObTabletHandle new_tablet_handle;
  ObTimeGuard time_guard("MdsDump", 3_s);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id));
  } else if (CLICK_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
    } else {
      LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
    }
  } else {
    ObTablet *tmp_tablet = tmp_tablet_hdl.get_obj();
    time_guard.click("Acquire");
    CLICK();
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");
    if (CLICK_FAIL(direct_get_tablet(tablet_id, old_tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    } else if (old_tablet_handle.get_obj()->is_empty_shell()) {
      LOG_INFO("old tablet is empty shell tablet, should skip mds table dump operation", K(ret), K(ls_id), K(tablet_id),
          "old_tablet", *old_tablet_handle.get_obj());
    } else {
      time_guard.click("GetOld");
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
      ObTablet *old_tablet = old_tablet_handle.get_obj();
      ObMetaDiskAddr disk_addr;
      ObTabletMdsData mds_data;

      if (OB_FAIL(ret)) {
      } else if (CLICK_FAIL(old_tablet->read_mds_table(allocator, mds_data, true, mds_construct_sequence))) {
        if (OB_EMPTY_RESULT == ret) {
          const share::SCN &old_mds_checkpoint_scn = old_tablet->get_tablet_meta().mds_checkpoint_scn_;
          if (ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN != old_mds_checkpoint_scn) {
            // mds checkpoint scn is NOT initial scn, it means mds table dump has happened,
            // use empty mds data
            ret = OB_SUCCESS;
            LOG_INFO("read empty mds data from mds table, just advance mds checkpoint scn",
                K(ret), K(key), K(old_mds_checkpoint_scn));
          } else if (OB_FAIL(ObTablet::build_user_data_for_aborted_tx_tablet(allocator, flush_scn, mds_data))) {
            LOG_WARN("failed to build user data for aborted tx tablet", K(ret), K(key), K(flush_scn));
          } else {
            LOG_INFO("read empty mds data from mds table, mock mds data for first time mds table dump",
                K(ret), K(key), K(old_mds_checkpoint_scn), K(flush_scn), K(mds_data));
          }
        } else {
          LOG_WARN("failed to read mds table", K(ret), K(key));
        }
      }

      if (OB_FAIL(ret)) {
      } else if (CLICK_FAIL(tmp_tablet->init_for_mds_table_dump(allocator, *old_tablet, flush_scn, mds_data, old_tablet->mds_data_))) {
        LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet), K(flush_scn));
      } else if (CLICK_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet, new_tablet_handle))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_handle));
      } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
      } else if (CLICK_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
      } else if (FALSE_IT(time_guard.click("WrSlog"))) {
        LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
      } else if (CLICK_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, new_tablet_handle))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(disk_addr), K(old_tablet_handle));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else {
        CLICK();
        time_guard.click("CASwap");
        LOG_INFO("succeeded to build new tablet", K(ret), K(key), K(disk_addr), K(new_tablet_handle), K(mds_data));
      }
    }
  }

  return ret;
}

int ObLSTabletService::update_tablet_report_status(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTimeGuard time_guard("ObLSTabletService::update_tablet_report_status", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle.get_obj());
  } else {
    time_guard.click("GetTablet");

    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletHandle new_tablet_handle;

    if (tablet->tablet_meta_.report_status_.need_report()) {
      tablet->tablet_meta_.report_status_.cur_report_version_ = tablet->tablet_meta_.report_status_.merge_snapshot_version_;
      if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tablet), K(new_tablet_handle));
      } else if (FALSE_IT(time_guard.click("Persist"))) {
      } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(key.ls_id_, tablet_id, disk_addr))) {
        LOG_WARN("failed to write update tablet slog", K(ret), K(key), K(disk_addr));
      } else if (FALSE_IT(time_guard.click("WrSlog"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, new_tablet_handle))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(new_tablet_handle));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else {
        time_guard.click("CASwap");
        LOG_INFO("succeeded to build new tablet", K(ret), K(key), K(disk_addr), K(tablet_handle));
      }
    } else {
      FLOG_INFO("tablet doesn't need to report", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_restore_status(
    const common::ObTabletID &tablet_id,
    const ObTabletRestoreStatus::STATUS &restore_status,
    const bool need_reset_transfer_flag)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletRestoreStatus::STATUS current_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  bool can_change = false;

  ObTimeGuard time_guard("ObLSTabletService::update_tablet_report_status", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!ObTabletRestoreStatus::is_valid(restore_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(restore_status));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle.get_obj());
  } else {
    time_guard.click("GetTablet");

    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletHandle new_tablet_handle;

    if (OB_FAIL(tablet->tablet_meta_.ha_status_.get_restore_status(current_status))) {
      LOG_WARN("failed to get restore status", K(ret), KPC(tablet));
    } else if (OB_FAIL(ObTabletRestoreStatus::check_can_change_status(current_status, restore_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(current_status), K(restore_status), KPC(tablet));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change restore status", K(ret), K(current_status), K(restore_status), KPC(tablet));
    } else if (OB_FAIL(tablet->tablet_meta_.ha_status_.set_restore_status(restore_status))) {
      LOG_WARN("failed to set restore status", K(ret), K(restore_status), KPC(tablet));
    } else if (need_reset_transfer_flag
               && OB_FALSE_IT((void)tablet->tablet_meta_.reset_transfer_table())) {
    } else {
      // TODO(jiahua.cjh) move check valid to tablet init after generate new version tablet.
      if (OB_FAIL(tablet->check_valid())) {
        LOG_WARN("failed to check tablet valid", K(ret), K(restore_status), KPC(tablet));
      } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tablet), K(new_tablet_handle));
      } else if (FALSE_IT(time_guard.click("Persist"))) {
      } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(key.ls_id_, tablet_id, disk_addr))) {
        LOG_WARN("failed to write update tablet slog", K(ret), K(key), K(disk_addr));
      } else if (FALSE_IT(time_guard.click("WrSlog"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, new_tablet_handle))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(new_tablet_handle));
        ob_abort();
      } else {
        time_guard.click("CASwap");
        LOG_INFO("succeeded to build new tablet", K(ret), K(key), K(disk_addr), K(restore_status), K(need_reset_transfer_flag), K(tablet_handle));
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = tablet->tablet_meta_.ha_status_.set_restore_status(current_status))) {
          LOG_WARN("failed to set restore status", K(tmp_ret), K(current_status), KPC(tablet));
          ob_abort();
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_ha_data_status(
    const common::ObTabletID &tablet_id,
    const ObTabletDataStatus::STATUS &data_status)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletDataStatus::STATUS current_status = ObTabletDataStatus::DATA_STATUS_MAX;
  bool can_change = false;

  ObTimeGuard time_guard("ObLSTabletService::update_tablet_ha_data_status", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!ObTabletDataStatus::is_valid(data_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(data_status));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle.get_obj());
  } else {
    time_guard.click("GetTablet");

    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletHandle new_tablet_handle;

    if (OB_FAIL(tablet->tablet_meta_.ha_status_.get_data_status(current_status))) {
      LOG_WARN("failed to get data status", K(ret), KPC(tablet));
    } else if (OB_FAIL(ObTabletDataStatus::check_can_change_status(current_status, data_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(current_status), K(data_status), KPC(tablet));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change data status", K(ret), K(current_status), K(data_status), KPC(tablet));
    } else if (current_status == data_status) {
      LOG_INFO("data status is same, skip update", K(tablet_id), K(current_status), K(data_status));
    } else if (OB_FAIL(tablet->tablet_meta_.ha_status_.set_data_status(data_status))) {
      LOG_WARN("failed to set data status", K(ret), KPC(tablet), K(data_status));
    } else {
      if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tablet), K(new_tablet_handle));
      } else if (FALSE_IT(time_guard.click("Persist"))) {
      } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(key.ls_id_, tablet_id, disk_addr))) {
        LOG_WARN("failed to write update tablet slog", K(ret), K(tablet_handle), K(disk_addr));
      } else if (FALSE_IT(time_guard.click("WrSlog"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, new_tablet_handle))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(new_tablet_handle));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else {
        time_guard.click("CASwap");
        LOG_INFO("succeeded to update tablet ha data status", K(ret), K(key), K(disk_addr), K(data_status), K(tablet_handle));
      }
      if (OB_FAIL(ret)) {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = tablet->tablet_meta_.ha_status_.set_data_status(current_status))) {
          LOG_WARN("failed to set data status", K(tmp_ret), K(current_status), KPC(tablet));
          ob_abort();
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_ha_expected_status(
    const common::ObTabletID &tablet_id,
    const ObTabletExpectedStatus::STATUS &expected_status)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletExpectedStatus::STATUS current_status = ObTabletExpectedStatus::EXPECTED_STATUS_MAX;
  bool can_change = false;

  ObTimeGuard time_guard("ObLSTabletService::update_tablet_ha_data_status", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!ObTabletExpectedStatus::is_valid(expected_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(expected_status));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle.get_obj());
  } else {
    time_guard.click("GetTablet");

    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletHandle new_tablet_handle;

    if (OB_FAIL(tablet->tablet_meta_.ha_status_.get_expected_status(current_status))) {
      LOG_WARN("failed to get data status", K(ret), KPC(tablet));
    } else if (expected_status == current_status) {
      LOG_INFO("tablet ha expected status is same, no need update", K(tablet_id),
          K(current_status), K(expected_status));
    } else if (OB_FAIL(ObTabletExpectedStatus::check_can_change_status(current_status, expected_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(current_status), K(expected_status), KPC(tablet));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change meta status", K(ret), K(current_status), K(expected_status), KPC(tablet));
    } else {
      if (OB_FAIL(tablet->tablet_meta_.ha_status_.set_expected_status(expected_status))) {
        LOG_WARN("failed to set ha meta status", K(ret), KPC(tablet), K(expected_status));
      } else {
        if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tablet, new_tablet_handle))) {
          LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tablet), K(new_tablet_handle));
        } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
        } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(key.ls_id_, tablet_id, disk_addr))) {
          LOG_WARN("failed to write update tablet slog", K(ret), K(tablet_handle), K(disk_addr));
        } else if (FALSE_IT(time_guard.click("WrSlog"))) {
        } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, new_tablet_handle))) {
          LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(new_tablet_handle));
          ob_usleep(1000 * 1000);
          ob_abort();
        } else {
          time_guard.click("CASwap");
          LOG_INFO("succeeded to update tablet meta status", K(ret), K(key), K(disk_addr), K(expected_status), KPC(tablet));
        }

        if (OB_FAIL(ret)) {
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = tablet->tablet_meta_.ha_status_.set_expected_status(current_status))) {
            LOG_WARN("failed to set expected status", K(tmp_ret), K(current_status), KPC(tablet));
            ob_abort();
          }
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::replay_create_tablet(
    const ObMetaDiskAddr &disk_addr,
    const char *buf,
    const int64_t buf_len,
    const ObTabletID &tablet_id,
    ObTabletTransferInfo &tablet_transfer_info)
{
  int ret = OB_SUCCESS;
  bool b_exist = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);
  ObFreezer *freezer = ls_->get_freezer();
  const ObLSID &ls_id = ls_->get_ls_id();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
    LOG_WARN("fail to check tablet existence", K(ret), K(ls_id), K(tablet_id));
  } else if (b_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("restart replay tablet should not exist", K(ret), K(ls_id), K(tablet_id));
  } else {
    ObTimeGuard time_guard("ObLSTabletService::replay_create_tablet", 1_s);
    common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "ReplayCreate"));
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTabletHandle tablet_hdl;
    ObTablet *tablet = nullptr;
    int64_t pos = 0;
    ObMetaDiskAddr old_addr;
    ObTabletPoolType pool_type(ObTabletPoolType::TP_MAX);
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tablet_hdl))) {
      LOG_WARN("fail to create temporary tablet", K(ret), K(key));
    } else if (FALSE_IT(tablet = tablet_hdl.get_obj())) {
    } else if (FALSE_IT(tablet->tablet_addr_ = disk_addr)) {
    } else if (OB_FAIL(t3m->get_tablet_addr(key, old_addr))) {
      LOG_WARN("fail to get tablet addr", K(ret), K(key));
    } else if (OB_FAIL(tablet->deserialize(allocator, buf, buf_len, pos))) {
      LOG_WARN("fail to deserialize tablet", K(ret), K(buf), K(buf_len), K(pos));
    } else if (FALSE_IT(time_guard.click("Deserialize"))) {
    } else if (OB_FAIL(tablet->init_shared_params(ls_id,
                                                  tablet_id,
                                                  tablet->get_tablet_meta().max_sync_storage_schema_version_,
                                                  tablet->get_tablet_meta().max_serialized_medium_scn_,
                                                  tablet->get_tablet_meta().compat_mode_,
                                                  freezer))) {
      LOG_WARN("failed to init shared params", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
      LOG_WARN("fail to set tablet id set", K(ret), K(tablet_id));
    } else {
      if (tablet->is_empty_shell()) {
        pool_type = ObTabletPoolType::TP_NORMAL;
      } else {
        const int64_t try_cache_size = tablet->get_try_cache_size();
        if (try_cache_size > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE) {
          pool_type = ObTabletPoolType::TP_LARGE;
        } else {
          pool_type = ObTabletPoolType::TP_NORMAL;
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_addr, disk_addr, pool_type, true /* whether to set tablet pool */))) {
      LOG_WARN("fail to compare and swap tablat in t3m", K(ret), K(key), K(old_addr), K(disk_addr));
    } else if (FALSE_IT(time_guard.click("CASwap"))) {
    } else if (OB_FAIL(tablet->check_and_set_initial_state())) {
      LOG_WARN("fail to check and set initial state", K(ret), K(key));
    } else if (OB_FAIL(tablet->start_ddl_if_need())) {
      LOG_WARN("start ddl if need failed", K(ret));
    } else if (OB_FAIL(tablet->inc_macro_ref_cnt())) {
      LOG_WARN("fail to increase macro blocks' ref cnt for meta and data", K(ret));
    } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
      LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_hdl));
    }

    if (OB_SUCC(ret)) {
      tablet_transfer_info = tablet->get_tablet_meta().transfer_info_;
      FLOG_INFO("succeeded to replay create one tablet", K(ret), K(ls_id), K(tablet_id), KPC(tablet));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
        LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObLSTabletService::get_tablet_with_timeout(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &handle,
    const int64_t retry_timeout_us,
    const ObMDSGetTabletMode mode,
    const share::SCN &snapshot)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
  const int64_t timeout_step_us = 10 * 1000 * 1000; // 10s
  const int64_t snapshot_version = snapshot.get_val_for_tx();

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid()
      || mode < ObMDSGetTabletMode::READ_ALL_COMMITED)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(mode));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle, timeout_step_us, mode, snapshot_version))) {
    while (OB_ALLOCATE_MEMORY_FAILED == ret && ObClockGenerator::getClock() < retry_timeout_us) {
      ret = ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle, timeout_step_us, mode, snapshot_version);
    }
    if (OB_ALLOCATE_MEMORY_FAILED == ret) {
      ret = OB_TIMEOUT;
      LOG_WARN("get tablet timeout", K(ret), K(retry_timeout_us), K(ObTimeUtil::current_time()), K(mode));
    }
  }
  return ret;
}

int ObLSTabletService::get_ls_migration_required_size(int64_t &required_size)
{
  int ret = OB_SUCCESS;
  int64_t tmp_size = 0;
  required_size = 0;
  const ObLSID &ls_id = ls_->get_ls_id();
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletMapKey key;
  ObSArray<ObTabletID> tablet_ids;
  GetAllTabletIDOperator op(tablet_ids);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("t3m should not be nullptr", K(ret), KP(t3m));
  } else if (OB_FAIL(tablet_id_set_.foreach(op))) {
    LOG_WARN("fail to get all tablet ids from set", K(ret));
  } else {
    key.ls_id_ = ls_id;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      key.tablet_id_ = tablet_ids.at(i);
      if (OB_FAIL(t3m->get_tablet_migration_size(key, tmp_size))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // do nothing (expected: add no lock when fetching tablets from tablet_id_set_)
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get tablet required_size", K(ret), K(key), K(tmp_size));
        }
      } else if (-1 == tmp_size) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in_valid tablet_size", K(ret), K(key), K(tmp_size));
      } else {
        required_size += tmp_size;
      }
    } // end for
  }

  if (OB_FAIL(ret)) {
    required_size = 0;
  }
  return ret;
}

int ObLSTabletService::direct_get_tablet(const common::ObTabletID &tablet_id, ObTabletHandle &handle)
{
#ifdef ENABLE_DEBUG_LOG
  ObTimeGuard tg("direct_get_tablet", 10_ms);
#endif
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);

  if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, handle))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("failed to get tablet from t3m", K(ret), K(key));
    }
  }

  return ret;
}

int ObLSTabletService::inner_table_scan(
    ObTabletHandle &tablet_handle,
    ObTableScanIterator &iter,
    ObTableScanParam &param)
{
  // NOTICE: ObTableScanParam for_update_ param is ignored here,
  // upper layer will handle it, so here for_update_ is always false
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = iter.get_ctx_guard().get_store_ctx();
  int64_t data_max_schema_version = 0;
  bool is_bounded_staleness_read = (NULL == param.trans_desc_)
                                   ? false
                                   : param.snapshot_.is_weak_read();
  if (OB_UNLIKELY(!tablet_handle.is_valid()) || OB_UNLIKELY(!param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_handle), K(param));
  } else if (is_bounded_staleness_read
      && OB_FAIL(tablet_handle.get_obj()->get_max_schema_version(data_max_schema_version))) {
    LOG_WARN("failed to get max schema version", K(ret), K(param));
  } else if (is_bounded_staleness_read
      && OB_FAIL(tablet_handle.get_obj()->check_schema_version_for_bounded_staleness_read(
          param.schema_version_, data_max_schema_version, param.index_id_))) {
    //check schema_version with ref_table_id, because schema_version of scan_param is from ref table
    LOG_WARN("check schema version for bounded staleness read fail", K(ret), K(param));
    //need to get store ctx of PG, cur_key_ saves the real partition
  } else if (param.fb_snapshot_.is_min()) {
    ret = OB_SNAPSHOT_DISCARDED;
  } else {
    const int64_t snapshot_version = store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    if (OB_FAIL(tablet_handle.get_obj()->check_snapshot_readable_with_cache(snapshot_version, param.schema_version_))) {
      LOG_WARN("failed to check snapshot readable", K(ret), K(snapshot_version), K(param.schema_version_));
    } else if (param.need_switch_param_) {
      if (OB_FAIL(iter.switch_param(param, tablet_handle))) {
        LOG_WARN("failed to init table scan iterator, ", K(ret));
      }
    } else if (OB_FAIL(iter.init(param, tablet_handle))) {
      LOG_WARN("failed to init table scan iterator, ", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    LOG_WARN("failed to do table scan", K(ret), K(param), K(*this),
        K(data_max_schema_version));
  }

  return ret;
}

int ObLSTabletService::has_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    bool &b_exist)
{
  int ret = OB_SUCCESS;
  b_exist = false;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);

  if (OB_FAIL(t3m->has_tablet(key, b_exist))) {
    LOG_WARN("failed to check tablet exist", K(ret), K(key));
  }

  return ret;
}

int ObLSTabletService::create_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &data_tablet_id,
    const share::SCN &create_scn,
    const int64_t snapshot_version,
    const ObCreateTabletSchema &create_tablet_schema,
    ObTabletHandle &tablet_handle)
{
  TIMEGUARD_INIT(CreateTablet, 5_ms);
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "CreateTab"));
  common::ObArenaAllocator *allocator = nullptr;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tablet = nullptr;
  blocksstable::ObSSTable new_sstable;
  blocksstable::ObSSTable *sstable = nullptr;
  ObTabletCreateSSTableParam param;
  ObTablet *tmp_tablet = nullptr;
  ObFreezer *freezer = ls_->get_freezer();
  bool need_create_empty_major_sstable = false;
  ObTabletTableStoreFlag table_store_flag;
  table_store_flag.set_with_major_sstable();
  tablet_handle.reset();

  if (CLICK_FAIL(ObTabletCreateDeleteHelper::prepare_create_msd_tablet())) {
    LOG_WARN("fail to prepare create msd tablet", K(ret));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.tablet_id_.hash());
    if (CLICK_FAIL(ObTabletCreateDeleteHelper::create_msd_tablet(key, tablet_handle))) {
      LOG_WARN("failed to create msd tablet", K(ret), K(key));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())
        || OB_ISNULL(allocator = tablet_handle.get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new tablet is null", K(ret), KP(tablet), KP(allocator), K(tablet_handle));
    } else if (CLICK_FAIL(ObTabletCreateDeleteHelper::check_need_create_empty_major_sstable(
        create_tablet_schema, need_create_empty_major_sstable))) {
      LOG_WARN("failed to check need create sstable", K(ret));
    } else if (!need_create_empty_major_sstable) {
      table_store_flag.set_without_major_sstable();
      LOG_INFO("no need to create sstable", K(ls_id), K(tablet_id), K(create_tablet_schema));
    } else if (CLICK_FAIL(ObTabletCreateDeleteHelper::build_create_sstable_param(create_tablet_schema, tablet_id, snapshot_version, param))) {
      LOG_WARN("failed to build create sstable param", K(ret), K(tablet_id),
          K(create_tablet_schema), K(snapshot_version), K(param));
    } else if (CLICK_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, tmp_allocator, new_sstable))) {
      LOG_WARN("failed to create sstable", K(ret), K(param));
    } else {
      sstable = &new_sstable;
    }
    if (OB_FAIL(ret)) {
    } else if (CLICK_FAIL(tablet->init_for_first_time_creation(*allocator, ls_id, tablet_id, data_tablet_id,
        create_scn, snapshot_version, create_tablet_schema, table_store_flag, sstable, freezer))) {
      LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
          K(create_scn), K(snapshot_version), K(create_tablet_schema), K(table_store_flag));
    } else if (CLICK_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle))) {
      LOG_WARN("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle));
    } else if (CLICK_FAIL(tablet_id_set_.set(tablet_id))) {
      LOG_WARN("fail to insert tablet id", K(ret), K(ls_id), K(tablet_id));
    } else if (CLICK_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
      LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_handle));
    } else {
      report_tablet_to_rs(tablet_id);
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("succeed to create tablet", K(ret), K(ls_id), K(tablet_id));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
        LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObLSTabletService::create_inner_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &data_tablet_id,
    const share::SCN &create_scn,
    const int64_t snapshot_version,
    const ObCreateTabletSchema &create_tablet_schema,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "LSCreateTab"));
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);
  const ObTabletMapKey key(ls_id, tablet_id);
  ObSSTable new_sstable;
  ObSSTable *sstable = nullptr;
  ObTabletCreateSSTableParam param;
  ObTablet *tmp_tablet = nullptr;
  ObFreezer *freezer = ls_->get_freezer();
  bool need_create_empty_major_sstable = false;
  ObTabletTableStoreFlag table_store_flag;
  table_store_flag.set_with_major_sstable();
  ObTabletHandle tmp_tablet_hdl;
  ObMetaDiskAddr disk_addr;

  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("failed to create temporary tablet", K(ret), K(key));
  } else if (OB_ISNULL(tmp_tablet = tmp_tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), KPC(tmp_tablet), K(tmp_tablet_hdl));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::check_need_create_empty_major_sstable(
      create_tablet_schema, need_create_empty_major_sstable))) {
    LOG_WARN("failed to check need create sstable", K(ret));
  } else if (!need_create_empty_major_sstable) {
    table_store_flag.set_without_major_sstable();
    LOG_INFO("no need to create sstable", K(ls_id), K(tablet_id), K(create_tablet_schema));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::build_create_sstable_param(
      create_tablet_schema, tablet_id, snapshot_version, param))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(tablet_id),
        K(create_tablet_schema), K(snapshot_version), K(param));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, new_sstable))) {
    LOG_WARN("failed to create sstable", K(ret), K(param));
  } else {
    sstable = &new_sstable;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tmp_tablet->init_for_first_time_creation(allocator, ls_id, tablet_id, data_tablet_id,
      create_scn, snapshot_version, create_tablet_schema, table_store_flag, sstable, freezer))) {
    LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
        K(create_scn), K(snapshot_version), K(create_tablet_schema), K(table_store_flag));
  } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet, tablet_handle))) {
    LOG_WARN("fail to persist and transform tablet", K(ret), K(tmp_tablet_hdl), K(tablet_handle));
  } else if (FALSE_IT(disk_addr = tablet_handle.get_obj()->get_tablet_addr())) {
  } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
    LOG_WARN("failed to write update tablet slog", K(ret), K(tablet_handle));
  } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle))) {
    LOG_WARN("failed to add tablet to meta mem mgr", K(ret), K(key), K(tablet_handle));
  } else if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
    LOG_ERROR("fail to set tablet id set", K(ret), K(tablet_id));
    ob_usleep(1000 * 1000);
    ob_abort();
  } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
    LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_handle));
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("create inner tablet success", K(key), K(ls_id), K(tablet_id), K(disk_addr));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
      LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObLSTabletService::create_transfer_in_tablet(
    const share::ObLSID &ls_id,
    const ObMigrationTabletParam &tablet_meta,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tablet = nullptr;
  ObFreezer *freezer = ls_->get_freezer();
  common::ObArenaAllocator *allocator = nullptr;
  ObTabletHandle old_tablet_hdl;

  LOG_INFO("prepare to init transfer in tablet", K(ret), K(ls_id), K(tablet_meta));
  ObTimeGuard time_guard("CreateTransferIn", 3_s);

  bool cover_empty_shell = false;
  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet service do not init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create transfer in tablet get invalid argument", K(ret), K(ls_id), K(tablet_meta));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("tablet not exist", K(ret), K(tablet_id));
    } else {
      LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
    }
  } else if (!old_tablet_hdl.get_obj()->is_empty_shell()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old tablet is not empty shell", K(ret), K(old_tablet_hdl));
  } else if (FALSE_IT(cover_empty_shell = true)) {
  // Called in mds trans callback, slog won't be written.
  } else if (OB_FAIL(do_remove_tablet(ls_id, tablet_id))) {
    LOG_WARN("failed to remove tablet", K(ret), K(ls_id), K(tablet_id));
  }

  time_guard.click("Prepare");
  if (FAILEDx(ObTabletCreateDeleteHelper::prepare_create_msd_tablet())) {
    LOG_WARN("failed to prepare create msd tablet", K(ret));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");
    if (FAILEDx(ObTabletCreateDeleteHelper::create_msd_tablet(key, tablet_handle))) {
      LOG_WARN("failed to create msd tablet", K(ret), K(key));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())
        || OB_ISNULL(allocator = tablet_handle.get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new tablet is null", K(ret), KP(tablet), KP(allocator), K(tablet_handle));
    } else if (OB_FAIL(tablet->init_with_migrate_param(*allocator, tablet_meta, false/*is_update*/, freezer))) {
      LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_meta));
    } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle))) {
      LOG_WARN("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle));
    } else if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
      LOG_WARN("fail to insert tablet id", K(ret), K(ls_id), K(tablet_id));
    } else {
      time_guard.click("Swap");
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("create transfer in tablet", K(ret), K(key), K(tablet_meta), K(cover_empty_shell));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
        LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObLSTabletService::create_empty_shell_tablet(
    const ObMigrationTabletParam &param,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const share::ObLSID &ls_id = param.ls_id_;
  const common::ObTabletID &tablet_id = param.tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle old_tablet_hdl;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet svr hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(key));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_DEBUG("tablet not exist", K(ret), K(tablet_id));
    } else {
      LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(remove_tablet(old_tablet_hdl))) {
    LOG_WARN("failed to remove tablet", K(ret), K(key));
  }

  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash()); // must lock after prepare
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "MigEmptyT"));
  ObTabletHandle tmp_tablet_hdl;
  if (FAILEDx(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to create temporary tablet", K(ret), K(key));
  } else {
    ObFreezer *freezer = ls_->get_freezer();
    ObTablet *tmp_tablet = tmp_tablet_hdl.get_obj();
    ObTabletHandle tablet_handle;
    ObTablet *new_tablet = nullptr;
    ObMetaDiskAddr disk_addr;
    if (OB_FAIL(tmp_tablet->init_with_migrate_param(allocator, param, false/*is_update*/, freezer))) {
      LOG_WARN("failed to init tablet", K(ret), K(param));
  #ifdef ERRSIM
    } else if (OB_SUCCESS != EN_CREATE_EMPTY_SHELL_TABLET_ERROR) {
      ret = EN_CREATE_EMPTY_SHELL_TABLET_ERROR;
      LOG_WARN("[ERRSIM] fake create empty shell tablet error", K(ret), K(param));
  #endif
    } else if (OB_FAIL(ObTabletPersister::transform_empty_shell(*tmp_tablet, tablet_handle))) {
      LOG_WARN("failed to transform empty shell", K(ret), KPC(tmp_tablet));
    } else if (FALSE_IT(new_tablet = tablet_handle.get_obj())) {
    } else if (OB_FAIL(ObTabletSlogHelper::write_empty_shell_tablet_slog(new_tablet, disk_addr))) {
      LOG_WARN("fail to write emtpy shell tablet slog", K(ret), K(tablet_id), K(disk_addr));
    } else if (FALSE_IT(new_tablet->tablet_addr_ = disk_addr)) {
    } else if (OB_FAIL(refresh_tablet_addr(ls_id, tablet_id, tablet_handle))) {
      LOG_WARN("failed to refresh tablet addr", K(ret), K(ls_id), K(tablet_id), K(lbt()));
      ob_usleep(1000 * 1000);
      ob_abort();
    }

    if (OB_SUCC(ret)) {
      ls_->get_tablet_gc_handler()->set_tablet_gc_trigger();
      LOG_INFO("succeeded to create empty shell tablet", K(ret), K(key), K(param));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
        LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObLSTabletService::rollback_remove_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_start_scn)
{
  int ret = OB_SUCCESS;
  bool is_same = true;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet service do not init", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid() || !transfer_start_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(transfer_start_scn));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(check_rollback_tablet_is_same_(ls_id, tablet_id, transfer_start_scn, is_same))) {
      LOG_WARN("failed to check rollback tablet is same", K(ret), K(ls_id), K(tablet_id), K(transfer_start_scn));
    } else if (!is_same) {
      //do nothing
    } else if (OB_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
      LOG_WARN("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObLSTabletService::rollback_remove_tablet_without_lock(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);
  const ObTabletMapKey key(ls_id, tablet_id);

  if (OB_FAIL(tablet_id_set_.erase(tablet_id))) {
    if (OB_HASH_NOT_EXIST == ret) {
      // tablet id is already erased
      ret = OB_SUCCESS;
      LOG_DEBUG("tablet id does not exist, maybe has not been inserted yet", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_WARN("fail to erase tablet id from set", K(ret), K(ls_id), K(tablet_id));
    }
  }

  if (OB_SUCC(ret)) {
    // loop retry to delete tablet from t3m
    while (OB_FAIL(t3m->del_tablet(key))) {
      if (REACH_TIME_INTERVAL(10_s)) {
        LOG_ERROR("failed to delete tablet from t3m", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tx_svr->remove_tablet(tablet_id, ls_id))) {
    LOG_WARN("fail to remove tablet cache", K(ret), K(tablet_id), K(ls_id));
  }

  return ret;
}

int ObLSTabletService::create_memtable(
    const common::ObTabletID &tablet_id,
    const int64_t schema_version,
    const bool for_replay,
    const share::SCN clog_checkpoint_scn)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle old_tablet_handle;
  const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
  ObTabletHandle new_tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store;
  ObTabletMemberWrapper<share::ObTabletAutoincSeq> autoinc_seq;

  ObTimeGuard time_guard("ObLSTabletService::create_memtable", 10_ms);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || schema_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(schema_version));
  } else {
    // we need bucket lock here to protect multi version tablet creation
    // during tablet creating new memtable and put it into table store.
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");
    if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_handle))) {
      LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
    } else if (old_tablet_handle.get_obj()->is_empty_shell()) {
      LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_handle.get_obj());
    } else {
      time_guard.click("get tablet");
      ObTabletCreateDeleteMdsUserData user_data;
      bool is_committed = false;
      ObTablet &old_tablet = *(old_tablet_handle.get_obj());
      // forbid create new memtable when transfer
      if (for_replay) {
      } else if (OB_FAIL(old_tablet.ObITabletMdsInterface::get_latest_tablet_status(user_data, is_committed))) {
      } else if (!is_committed || (user_data.tablet_status_ != ObTabletStatus::NORMAL
            && user_data.tablet_status_ != ObTabletStatus::TRANSFER_IN)) {
        ret = OB_EAGAIN;
        LOG_WARN("tablet status not allow create new memtable", K(ret), K(is_committed), K(user_data));
      }
      if (FAILEDx(old_tablet.create_memtable(schema_version, clog_checkpoint_scn, for_replay))) {
        if (OB_MINOR_FREEZE_NOT_ALLOW != ret) {
          LOG_WARN("fail to create memtable", K(ret), K(new_tablet_handle), K(schema_version), K(tablet_id));
        }
      } else if (FALSE_IT(time_guard.click("create memtable"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, old_tablet_handle))) {
        LOG_WARN("fail to compare and swap tablet", K(ret), K(key), K(old_tablet_handle));
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_allow_to_read()
{
  int ret = OB_SUCCESS;
  bool allow_to_read = false;
  allow_to_read_mgr_.load_allow_to_read_info(allow_to_read);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (!allow_to_read) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("ls is not allow to read", K(ret), KPC(ls_));
  }
  return ret;
}

// ATTENTION!
// here we pass VALUE rather than REF for tablet id,
// because tablet id may be from iter, which will be reset in function,
// thus tablet id will be invalid
int ObLSTabletService::get_read_tables(
    const common::ObTabletID tablet_id,
    const int64_t timeout_us,
    // snapshot used for get tablet for mds
    const int64_t snapshot_version_for_tablet,
    // snapshot used for filter tables in table_store
    const int64_t snapshot_version_for_tables,
    ObTabletTableIterator &iter,
    const bool allow_no_ready_read)
{
  int ret = OB_SUCCESS;
  ObTabletHandle &handle = iter.tablet_handle_;
  iter.reset();
  bool allow_to_read = false;
  ObTabletMapKey key;
  key.tablet_id_ = tablet_id;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() ||
                         snapshot_version_for_tables < 0 ||
                         snapshot_version_for_tablet < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(snapshot_version_for_tablet),
             K(snapshot_version_for_tables));
  } else if (FALSE_IT(allow_to_read_mgr_.load_allow_to_read_info(allow_to_read))) {
  } else if (!allow_to_read) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("ls is not allow to read", K(ret), KPC(ls_));
  } else if (FALSE_IT(key.ls_id_ = ls_->get_ls_id())) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle,
      timeout_us,
      ObMDSGetTabletMode::READ_READABLE_COMMITED,
      snapshot_version_for_tablet))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("fail to check and get tablet", K(ret), K(key), K(timeout_us),
               K(snapshot_version_for_tablet), K(snapshot_version_for_tables));
    }
  } else if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, invalid tablet handle", K(ret), K(handle));
  } else if (OB_FAIL(handle.get_obj()->get_read_tables(snapshot_version_for_tables, iter, allow_no_ready_read))) {
    LOG_WARN("fail to get read tables", K(ret), K(handle), K(tablet_id),
             K(snapshot_version_for_tablet), K(snapshot_version_for_tables),
             K(iter), K(allow_no_ready_read));
  }
  return ret;
}

int ObLSTabletService::set_tablet_status(
    const common::ObTabletID &tablet_id,
    const ObTabletCreateDeleteMdsUserData &tablet_status,
    mds::MdsCtx &ctx)
{
  TIMEGUARD_INIT(SetTabletStatus, 5_ms);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !tablet_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(tablet_status));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    ObTabletHandle tablet_handle;
    if (CLICK_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("this tablet has been deleted, skip it", K(ret), K(tablet_id));
      } else {
        LOG_WARN("fail to get tablet", K(ret));
      }
    } else if (CLICK_FAIL(tablet_handle.get_obj()->set_tablet_status(tablet_status, ctx))) {
      LOG_WARN("fail to set tablet status", K(ret), K(tablet_id), K(tablet_status));
    } else {
      LOG_INFO("succeeded to set tablet status", K(ret), K(tablet_id), K(tablet_status));
    }
  }
  return ret;
}

int ObLSTabletService::replay_set_tablet_status(
    const common::ObTabletID &tablet_id,
    const share::SCN &scn,
    const ObTabletCreateDeleteMdsUserData &tablet_status,
    mds::MdsCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !tablet_status.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(tablet_status));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    ObTabletHandle tablet_handle;
    if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("this tablet has been deleted, skip it", K(ret), K(tablet_id));
      } else {
        LOG_WARN("fail to get tablet", K(ret));
      }
    } else if (OB_FAIL(tablet_handle.get_obj()->replay_set_tablet_status(scn, tablet_status, ctx))) {
      LOG_WARN("fail to replay set tablet status", K(ret), K(tablet_id), K(scn), K(tablet_status));
    } else {
      LOG_INFO("succeeded to replay set tablet status", K(ret), K(tablet_id), K(scn), K(tablet_status));
    }
  }
  return ret;
}

int ObLSTabletService::set_ddl_info(
    const common::ObTabletID &tablet_id,
    const ObTabletBindingMdsUserData &ddl_data,
    mds::MdsCtx &ctx,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(ddl_data));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    ObTabletHandle tablet_handle;
    if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("this tablet has been deleted, skip it", K(ret), K(tablet_id));
      } else {
        LOG_WARN("fail to get tablet", K(ret));
      }
    } else if (OB_FAIL(tablet_handle.get_obj()->set_ddl_info(ddl_data, ctx, timeout_us))) {
      LOG_WARN("fail to set ddl info", K(ret), K(tablet_id), K(ddl_data), K(timeout_us));
    } else {
      LOG_INFO("succeeded to set ddl info", K(ret), K(tablet_id), K(ddl_data), K(timeout_us));
    }
  }
  return ret;
}

int ObLSTabletService::replay_set_ddl_info(
    const common::ObTabletID &tablet_id,
    const share::SCN &scn,
    const ObTabletBindingMdsUserData &ddl_data,
    mds::MdsCtx &ctx)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(ddl_data));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    ObTabletHandle tablet_handle;
    if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        ret = OB_EAGAIN;
        LOG_WARN("this tablet has been deleted, skip it", K(ret), K(tablet_id));
      } else {
        LOG_WARN("fail to get tablet", K(ret));
      }
    } else if (OB_FAIL(tablet_handle.get_obj()->replay_set_ddl_info(scn, ddl_data, ctx))) {
      LOG_WARN("fail to set ddl info", K(ret), K(tablet_id), K(ddl_data), K(scn));
    } else {
      LOG_INFO("succeeded to set ddl info", K(ret), K(tablet_id), K(ddl_data), K(scn));
    }
  }
  return ret;
}

int ObLSTabletService::build_create_sstable_param_for_migration(
    const blocksstable::ObMigrationSSTableParam &mig_param,
    ObTabletCreateSSTableParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!mig_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(mig_param));
  } else {
    param.table_key_                     = mig_param.table_key_;
    param.schema_version_                = mig_param.basic_meta_.schema_version_;
    param.create_snapshot_version_       = mig_param.basic_meta_.create_snapshot_version_;
    param.progressive_merge_round_       = mig_param.basic_meta_.progressive_merge_round_;
    param.progressive_merge_step_        = mig_param.basic_meta_.progressive_merge_step_;
    param.is_ready_for_read_             = false;
    param.table_mode_                    = mig_param.basic_meta_.table_mode_;
    param.index_type_                    = static_cast<share::schema::ObIndexType>(mig_param.basic_meta_.index_type_);
    param.rowkey_column_cnt_             = mig_param.basic_meta_.rowkey_column_count_;
    param.root_row_store_type_           = mig_param.basic_meta_.root_row_store_type_;
    param.latest_row_store_type_         = mig_param.basic_meta_.latest_row_store_type_;
    param.index_blocks_cnt_              = mig_param.basic_meta_.index_macro_block_count_;
    param.data_blocks_cnt_               = mig_param.basic_meta_.data_macro_block_count_;
    param.micro_block_cnt_               = mig_param.basic_meta_.data_micro_block_count_;
    param.use_old_macro_block_count_     = mig_param.basic_meta_.use_old_macro_block_count_;
    param.row_count_                     = mig_param.basic_meta_.row_count_;
    param.column_cnt_                    = mig_param.basic_meta_.column_cnt_;
    param.data_checksum_                 = mig_param.basic_meta_.data_checksum_;
    param.occupy_size_                   = mig_param.basic_meta_.occupy_size_;
    param.original_size_                 = mig_param.basic_meta_.original_size_;
    param.max_merged_trans_version_      = mig_param.basic_meta_.max_merged_trans_version_;
    param.ddl_scn_                       = mig_param.basic_meta_.ddl_scn_;
    param.filled_tx_scn_                 = mig_param.basic_meta_.filled_tx_scn_;
    param.contain_uncommitted_row_       = mig_param.basic_meta_.contain_uncommitted_row_;
    param.compressor_type_               = mig_param.basic_meta_.compressor_type_;
    param.encrypt_id_                    = mig_param.basic_meta_.encrypt_id_;
    param.master_key_id_                 = mig_param.basic_meta_.master_key_id_;
    MEMCPY(param.encrypt_key_, mig_param.basic_meta_.encrypt_key_, share::OB_MAX_TABLESPACE_ENCRYPT_KEY_LENGTH);
    param.root_block_addr_.set_none_addr();
    param.data_block_macro_meta_addr_.set_none_addr();;
    if (OB_FAIL(param.column_checksums_.assign(mig_param.column_checksums_))) {
      LOG_WARN("fail to assign column checksums", K(ret), K(mig_param));
    }
  }
  return ret;
}

int ObLSTabletService::insert_rows(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    common::ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_insert_rows_begin);
  int64_t afct_num = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid())
      || !ctx.is_write()
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ctx), K(dml_param), K(column_ids), KP(row_iter));
  } else if (dml_param.is_direct_insert()) { // direct-insert mode
    if (OB_FAIL(direct_insert_rows(dml_param.table_param_->get_data_table().get_table_id(),
                                   dml_param.direct_insert_task_id_,
                                   dml_param.ddl_task_id_,
                                   ctx.tablet_id_,
                                   column_ids,
                                   row_iter,
                                   afct_num))) {
      LOG_WARN("failed to insert rows direct", KR(ret),
          K(dml_param.table_param_->get_data_table().get_table_id()),
          K(dml_param.direct_insert_task_id_),
          K(dml_param.ddl_task_id_),
          K(ctx.tablet_id_),
          K(column_ids));
    }
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_INSERT);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    ObStoreRow *tbl_rows = nullptr;
    int64_t row_count = 0;
    int64_t batch_idx = 0;
    ObNewRow *rows = nullptr;
    void *allocated_tbl_rows_buf = nullptr;
    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      tablet_handle.reset();
      ObTabletHandle tmp_handle;
      SMART_VAR(ObRowsInfo, rows_info) {
        ObRelativeTable &relative_table = run_ctx.relative_table_;
        ObStoreRow reserved_row;
        while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
          ObStoreRow reserved_row;
          // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
          dml_param.lob_allocator_.reuse();
          // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
          // outside the while loop.
          if (tmp_handle.get_obj() != relative_table.tablet_iter_.get_tablet_handle().get_obj()) {
            tmp_handle = run_ctx.relative_table_.tablet_iter_.get_tablet_handle();
            rows_info.reset();
            if (OB_FAIL(rows_info.init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (row_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row_count should be greater than 0", K(ret));
          } else if (1 == row_count) {
            tbl_rows = &reserved_row;
          } else if (0 == batch_idx) { // first batch
            if (OB_ISNULL(allocated_tbl_rows_buf = work_allocator.alloc(row_count * sizeof(ObStoreRow)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret), K(row_count));
            } else {
              tbl_rows = new (allocated_tbl_rows_buf) ObStoreRow[row_count];
            }
          }

          if (OB_SUCC(ret)) {
            for (int64_t i = 0; i < row_count; i++) {
              tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_INSERT);
              tbl_rows[i].row_val_ = rows[i];
            }
            if (OB_FAIL(rows_info.assign_rows(row_count, tbl_rows))) {
              LOG_WARN("fail to assign rows", K(ret), K(batch_idx), K(row_count));
            } else if (OB_FAIL(insert_rows_to_tablet(tmp_handle, run_ctx, rows_info, afct_num))) {
              LOG_WARN("insert to each tablets fail", K(ret), K(batch_idx), K(rows_info));
            }
          }
          batch_idx++;
        }  // end of while
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("Failed to allocate ObRowsInfo", K(ret));
      }
    }
    if (nullptr != allocated_tbl_rows_buf) {
      work_allocator.free(allocated_tbl_rows_buf);
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeeded to insert rows", K(ret));
    affected_rows = afct_num;
    EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
  }
  NG_TRACE(S_insert_rows_end);

  return ret;
}

int ObLSTabletService::direct_insert_rows(
    const uint64_t table_id,
    const int64_t px_task_id,
    const int64_t ddl_task_id,
    const ObTabletID &tablet_id,
    const ObIArray<uint64_t> &column_ids,
    ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  ObTableLoadTableCtx *table_ctx = nullptr;
  ObTableLoadUniqueKey key(table_id, ddl_task_id);
  if (OB_FAIL(ObTableLoadService::get_ctx(key, table_ctx))) {
    LOG_WARN("fail to get table ctx", KR(ret), K(key));
  } else {
    int64_t row_count = 0;
    ObNewRow *rows = nullptr;
    table::ObTableLoadTransId trans_id;
    trans_id.segment_id_ = px_task_id;
    trans_id.trans_gid_ = 1;
    ObTableLoadStore store(table_ctx);
    ObTableLoadStoreTransPXWriter writer;
    if (OB_FAIL(store.init())) {
      LOG_WARN("fail to init store", KR(ret));
    } else if (OB_FAIL(store.px_get_trans_writer(trans_id, writer))) {
      LOG_WARN("fail to get trans writer", KR(ret), K(trans_id));
    } else if (OB_FAIL(writer.prepare_write(tablet_id, column_ids))) {
      LOG_WARN("fail to prepare write", KR(ret), K(tablet_id), K(column_ids));
    }

    while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
      if (OB_UNLIKELY(row_count <= 0)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("row_count should be greater than 0", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && (i < row_count); ++i) {
          const ObNewRow &row = rows[i];
          if (OB_FAIL(writer.write(row))) {
            LOG_WARN("fail to write", KR(ret), K(i), K(row));
          } else {
            ++affected_rows;
          }
        }
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  if (OB_NOT_NULL(table_ctx)) {
    ObTableLoadService::put_ctx(table_ctx);
    table_ctx = nullptr;
  }
  return ret;
}

int ObLSTabletService::mock_duplicated_rows_(common::ObNewRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObValueRowIterator *dup_iter = NULL;

  if (OB_ISNULL(dup_iter = ObQueryIteratorFactory::get_insert_dup_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory to alloc ObValueRowIterator", K(ret));
  } else {
    duplicated_rows = dup_iter;
    if (OB_FAIL(dup_iter->init(true))) {
      LOG_WARN("failed to initialize ObValueRowIterator", K(ret));
      ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
      duplicated_rows = nullptr;
    }
  }

  return ret;
}

int ObLSTabletService::insert_rows_with_fetch_dup(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<uint64_t> &duplicated_column_ids,
    common::ObNewRowIterator *row_iter,
    const ObInsertFlag flag,
    int64_t &affected_rows,
    common::ObNewRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  int64_t afct_num = 0;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid()
             || !ctx.is_write()
             || !dml_param.is_valid()
             || column_ids.count() <= 0
             || duplicated_column_ids.count() <= 0
             || nullptr == row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ctx), K(dml_param),
        K(column_ids), K(duplicated_column_ids), KP(row_iter), K(flag));
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_INSERT);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    ObStoreRow *tbl_rows = nullptr;
    int64_t row_count = 0;
    int64_t batch_idx = 0;
    ObNewRow *rows = nullptr;
    void *allocated_tbl_rows_buf = nullptr;
    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      ObTabletHandle tmp_handle;
      SMART_VAR(ObRowsInfo, rows_info) {
        bool has_duplicate = false;
        int64_t dup_row_count = 0;
        ObRelativeTable &relative_table = run_ctx.relative_table_;
        while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
          ObStoreRow reserved_row;
          // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
          dml_param.lob_allocator_.reuse();
          // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
          // outside the while loop.
          if (tmp_handle.get_obj() != relative_table.tablet_iter_.get_tablet_handle().get_obj()) {
            tmp_handle = relative_table.tablet_iter_.get_tablet_handle();
            rows_info.reset();
            if (OB_FAIL(rows_info.init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), true/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
          if (row_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row_count should be greater than 0", K(ret));
          } else if (1 == row_count) {
            tbl_rows = &reserved_row;
          } else if (0 == batch_idx) { // first batch
            if (OB_ISNULL(allocated_tbl_rows_buf = work_allocator.alloc(row_count * sizeof(ObStoreRow)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret), K(row_count));
            } else {
              tbl_rows = new (allocated_tbl_rows_buf) ObStoreRow[row_count];
            }
          }

          if (OB_SUCC(ret)) {
            for (int64_t i = 0; i < row_count; i++) {
              tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_INSERT);
              tbl_rows[i].row_val_ = rows[i];
            }
            if (OB_FAIL(rows_info.assign_rows(row_count, tbl_rows))) {
              LOG_WARN("fail to assign rows", K(ret), K(row_count));
            } else if (OB_FAIL(insert_rows_to_tablet(tablet_handle, run_ctx, rows_info, afct_num))) {
              if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
                for (int64_t i = 0; i < row_count; i++) {
                  if (rows_info.is_row_duplicate(i)) {
                    dup_row_count++;
                  }
                }
                has_duplicate = true;
                ret = OB_SUCCESS;
                int tmp_ret = OB_SUCCESS;
                // For primary key conflicts caused by concurrent insertions within
                // a statement, we need to return the corresponding duplicated_rows.
                // However, under circumstances where an exception may unexpectedly
                // prevent us from reading the conflicting rows within statements,
                // at such times, it becomes necessary for us to mock the rows.
                LOG_DEBUG("get conflict rows", K(batch_idx), K(ctx.tablet_id_), K(rows_info));
                if (OB_TMP_FAIL(get_conflict_rows(tablet_handle,
                                                  run_ctx,
                                                  duplicated_column_ids,
                                                  rows_info,
                                                  duplicated_rows))) {
                  LOG_WARN("failed to get conflict row(s)", K(ret), K(batch_idx), K(rows_info));
                  ret = tmp_ret;
                } else if (nullptr == duplicated_rows) {
                  if (OB_TMP_FAIL(mock_duplicated_rows_(duplicated_rows))) {
                    LOG_WARN("failed to mock duplicated row(s)", K(ret), K(batch_idx), K(rows_info));
                    ret = tmp_ret;
                  }
                }
              } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
                LOG_WARN("failed to write row", K(ret));
              }
            }
          }
          batch_idx++;
        } // end of while

        if (OB_ITER_END == ret) {
          ret = has_duplicate ? OB_ERR_PRIMARY_KEY_DUPLICATE : OB_SUCCESS; // recover the duplicate key error
          EVENT_ADD(ObStatEventIds::SQL_INSERT_DUPLICATE_COUNT, dup_row_count);
        }
      } else {
        LOG_WARN("Failed to allocate ObRowsInfo", K(ret));
      }
    }
    if (nullptr != allocated_tbl_rows_buf) {
      work_allocator.free(allocated_tbl_rows_buf);
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeeded to insert rows with fetch dup", K(ret));
    affected_rows = afct_num;
    EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
  }
  return ret;
}

int ObLSTabletService::insert_row(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<uint64_t> &duplicated_column_ids,
    const common::ObNewRow &row,
    const ObInsertFlag flag,
    int64_t &affected_rows,
    common::ObNewRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid()
             || !ctx.is_write()
             || !dml_param.is_valid()
             || column_ids.count() <= 0
             || duplicated_column_ids.count() <= 0
             || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ctx), K(dml_param),
        K(column_ids), K(duplicated_column_ids), K(row), K(flag));
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_INSERT);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    ObStoreRow &tbl_row = run_ctx.tbl_row_;
    const ObRelativeTable &data_table = run_ctx.relative_table_;
    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      tbl_row.flag_.set_flag(ObDmlFlag::DF_INSERT);
      tbl_row.row_val_ = row;
      const bool check_exist = !data_table.is_storage_index_table() ||
        data_table.is_unique_index() ||
        ctx.mvcc_acc_ctx_.write_flag_.is_update_pk_dop();
      if (OB_FAIL(insert_row_to_tablet(check_exist,
                                       tablet_handle,
                                       run_ctx,
                                       tbl_row))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          int tmp_ret = OB_SUCCESS;
          // For primary key conflicts caused by concurrent insertions within
          // a statement, we need to return the corresponding duplicated_rows.
          // However, under circumstances where an exception may unexpectedly
          // prevent us from reading the conflicting rows within statements,
          // at such times, it becomes necessary for us to mock the rows.
          if (OB_TMP_FAIL(get_conflict_row(tablet_handle,
                                           run_ctx,
                                           duplicated_column_ids,
                                           tbl_row.row_val_,
                                           duplicated_rows))) {
            LOG_WARN("failed to get conflict row(s)", K(ret), K(duplicated_column_ids), K(row));
            ret = tmp_ret;
          } else if (nullptr == duplicated_rows) {
            if (OB_TMP_FAIL(mock_duplicated_rows_(duplicated_rows))) {
              LOG_WARN("failed to mock duplicated row(s)", K(ret), K(duplicated_column_ids), K(row));
              ret = tmp_ret;
            }
          }
        }
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to write row", K(ret));
        }
      } else {
        LOG_DEBUG("succeeded to insert row", K(ret), K(row));
        affected_rows = 1;
        EVENT_INC(STORAGE_INSERT_ROW_COUNT);
      }
    }
  }
  return ret;
}

static inline
bool is_lob_update(ObDMLRunningCtx &run_ctx, const ObIArray<int64_t> &update_idx)
{
  bool bool_ret = false;
  for (int64_t i = 0; i < update_idx.count() && !bool_ret; ++i) {
    int64_t idx = update_idx.at(i);
    if (run_ctx.col_descs_->at(idx).col_type_.is_lob_storage()) {
      bool_ret = true;
    }
  }
  return bool_ret;
}

int ObLSTabletService::update_rows(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const ObIArray<uint64_t> &column_ids,
    const ObIArray< uint64_t> &updated_column_ids,
    ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_update_rows_begin);
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  int64_t afct_num = 0;
  int64_t dup_num = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid()
             || !ctx.is_write()
             || !dml_param.is_valid()
             || column_ids.count() <= 0
             || updated_column_ids.count() <= 0
             || nullptr == row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ctx), K(dml_param),
        K(column_ids), K(updated_column_ids), KP(row_iter));
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_UPDATE);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    bool rowkey_change = false;
    UpdateIndexArray update_idx;
    ObRowStore row_store;
    bool lob_update = false;
    ObRelativeTable &relative_table = run_ctx.relative_table_;
    char *buf = nullptr;

    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, &updated_column_ids, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else if (FALSE_IT(tablet_handle.reset())) {
    } else if (OB_UNLIKELY(!relative_table.is_valid())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("data table is not prepared", K(ret));
    } else if (OB_FAIL(construct_update_idx(relative_table.get_rowkey_column_num(),
        run_ctx.col_map_, updated_column_ids, update_idx))) {
      LOG_WARN("failed to construct update_idx", K(ret), K(updated_column_ids));
    } else if (OB_FAIL(check_rowkey_change(updated_column_ids, relative_table, rowkey_change))) {
      LOG_WARN("failed to check rowkey changes", K(ret));
    } else {
      int64_t cur_time = 0;
      lob_update = is_lob_update(run_ctx, update_idx);
      ObStoreRow old_tbl_row;
      ObStoreRow &new_tbl_row = run_ctx.tbl_row_;
      ObNewRow *old_rows = nullptr;
      ObNewRow *new_rows = nullptr;
      int64_t old_rows_count = 0;
      int64_t new_rows_count = 0;
      old_tbl_row.flag_.set_flag(ObDmlFlag::DF_UPDATE);
      new_tbl_row.flag_.set_flag(ObDmlFlag::DF_UPDATE);
      ObStoreRow *old_tbl_rows = nullptr;
      ObStoreRow *new_tbl_rows = nullptr;
      ObStoreRow *tmp_tbl_rows = nullptr;
      ObTabletHandle tmp_handle;
      ObRowsInfo *rows_infos = nullptr;
      /**
      * When _ob_immediate_row_conflict_check is true, indicates MySQL compatibility mode requiring:
      * - Row-by-row UPDATE execution
      * - Immediate conflict row checking
      *
      * Normally conflict row checking not needed for non-unique indexes, it is required in these special cases:
      * 1. Partitioned table PK updates causing row movement:
      *    - DAS layer splits into DELETE+INSERT
      *    - Global indexes may use UPDATE directly
      *    - Different execution paths may cause inconsistent conflict handling
      *    between main table and index table
      *
      * 2. PDML (Parallel DML) PK updates:
      *    - Different threads updating different rows
      *    - Update order mismatch between main table and index table
      *    - May lead to inconsistent conflict resolution
      *
      * For these cases, non-unique indexes MUST still perform conflict checking
      * (through duplicate key error reporting) to prevent data inconsistency, but this checking
      * can keep using batch interfaces without row-by-row updates.
      */
      const bool use_row_by_row_update = ctx.mvcc_acc_ctx_.write_flag_.is_immediate_row_check() &&
        rowkey_change && (!relative_table.is_storage_index_table() || relative_table.is_unique_index());
      // delay_new is for oracle compatible, refer to
      //const bool check_exist = rowkey_change && (!relative_table.is_storage_index_table() || relative_table.is_unique_index());
      //const bool delay_new = check_exist && lib::is_oracle_mode();
      bool delay_new = false;

      while (OB_SUCC(ret)
          && OB_SUCC(row_iter->get_next_rows(old_rows, old_rows_count))
          && OB_SUCC(row_iter->get_next_rows(new_rows, new_rows_count))) {
        // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
        dml_param.lob_allocator_.reuse();
        // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
        // outside the while loop.
        if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
          tmp_handle = run_ctx.relative_table_.tablet_iter_.get_tablet_handle();
          if (nullptr != rows_infos) {
            rows_infos[0].reset();
            rows_infos[1].reset();
            if (OB_FAIL(rows_infos[0].init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            } else if (OB_FAIL(rows_infos[1].init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
        }
        cur_time = ObClockGenerator::getClock();
        if (OB_UNLIKELY(cur_time > dml_param.timeout_)) {
          ret = OB_TIMEOUT;
          LOG_WARN("query timeout", K(cur_time), K(dml_param), K(ret));
        } else if (OB_UNLIKELY(old_rows_count != new_rows_count)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row count is not equal", K(ret), K(old_rows_count), K(new_rows_count));
        } else if (1 == new_rows_count) {
          old_tbl_row.row_val_ = old_rows[0];
          new_tbl_row.row_val_ = new_rows[0];
        } else if (nullptr == rows_infos) { // is first batch
          int64_t total_tbl_rows_cnt = old_rows_count + new_rows_count;
          // tmp_tbl_rows is need to delete old rows when rowkey changed and need total quantity log
          const bool need_tmp_tbl_rows = rowkey_change && run_ctx.dml_param_.is_total_quantity_log_;
          total_tbl_rows_cnt += need_tmp_tbl_rows ? old_rows_count : 0;
          // alloc memory for old_rows_info + new_rows_info + total_tbl_row in one time
          const int64_t buf_size = 2 * sizeof(ObRowsInfo) + total_tbl_rows_cnt * sizeof(ObStoreRow);

          if (OB_ISNULL(buf = static_cast<char*>(work_allocator.alloc(buf_size)))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret), K(buf_size));
          } else {
            rows_infos = new (buf) ObRowsInfo[2];
            if (OB_FAIL(rows_infos[0].init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            } else if (OB_FAIL(rows_infos[1].init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            } else {
              old_tbl_rows =  new (buf +  2 * sizeof(ObRowsInfo)) ObStoreRow[total_tbl_rows_cnt];
              new_tbl_rows = old_tbl_rows + old_rows_count;
              // the tmp_tbl_rows is used to delete the old row if the rowkey change
              tmp_tbl_rows = need_tmp_tbl_rows ? new_tbl_rows + new_rows_count : nullptr;
            }
          }
        }

        if (OB_SUCC(ret) && nullptr != rows_infos) {
          for (int64_t i = 0; i < new_rows_count; i++) {
            old_tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_UPDATE);
            old_tbl_rows[i].row_val_ = old_rows[i];
            new_tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_UPDATE);
            new_tbl_rows[i].row_val_ = new_rows[i];
          }
          if (OB_FAIL(rows_infos[0].assign_rows(old_rows_count, old_tbl_rows))) {
            LOG_WARN("fail to assign old rows", K(ret), K(old_rows_count));
          } else if (OB_FAIL(rows_infos[1].assign_rows(new_rows_count, new_tbl_rows))) {
            LOG_WARN("fail to assign old rows", K(ret), K(new_rows_count));
          }
        }

        if (OB_SUCC(ret)) {
          if (1 == new_rows_count) {
            if (OB_FAIL(update_row_to_tablet(tmp_handle,
                                             run_ctx,
                                             rowkey_change,
                                             update_idx,
                                             delay_new,
                                             lob_update,
                                             old_tbl_row,
                                             new_tbl_row,
                                             row_store))) {
              LOG_WARN("failed to update row to tablet", K(ret), K(old_tbl_row), K(new_tbl_row));
            }
          } else {
            if (use_row_by_row_update) {
              for (int64_t i = 0; OB_SUCC(ret) && i < new_rows_count; i++) {
                if (OB_FAIL(update_row_to_tablet(tmp_handle,
                                                 run_ctx,
                                                 rowkey_change,
                                                 update_idx,
                                                 delay_new,
                                                 lob_update,
                                                 old_tbl_rows[i],
                                                 new_tbl_rows[i],
                                                 row_store))) {
                  LOG_WARN("failed to update row to tablet", K(ret), K(i), K(old_tbl_rows[i]), K(new_tbl_rows[i]));
                }
              }
            } else {
              if (OB_FAIL(update_rows_to_tablet(tmp_handle,
                                                run_ctx,
                                                rowkey_change,
                                                update_idx,
                                                delay_new,
                                                lob_update,
                                                tmp_tbl_rows,
                                                rows_infos[0],
                                                rows_infos[1],
                                                row_store))) {
                LOG_WARN("failed to update row to tablets", K(ret), K(new_rows_count));
              }
            }
          }
          if (OB_SUCC(ret)) {
            afct_num += new_rows_count;
          }
        }
      } // end of while

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (OB_SUCC(ret) && row_store.get_row_count() > 0) {
        ObStoreRow *old_tbl_row_for_delay = old_tbl_rows ? &old_tbl_rows[0] : &old_tbl_row;
        ObStoreRow *new_tbl_row_for_delay = new_tbl_rows ? &new_tbl_rows[0] : &new_tbl_row;
        if (OB_FAIL(delay_process_new_rows(run_ctx,
                                           update_idx,
                                           rowkey_change,
                                           *old_tbl_row_for_delay,
                                           *new_tbl_row_for_delay,
                                           row_store))) {
          LOG_WARN("fail to delay process new rows", K(ret));
        }
      }
      if (nullptr != rows_infos) {
        rows_infos[0].~ObRowsInfo();
        rows_infos[1].~ObRowsInfo();
        work_allocator.free(buf);
      }
    }

    if (OB_SUCC(ret)) {
      affected_rows = afct_num;
      EVENT_ADD(STORAGE_UPDATE_ROW_COUNT, afct_num);
    }
  }
  NG_TRACE(S_update_rows_end);
  return ret;
}

// delay_new is for oracle compatible, refer to
int ObLSTabletService::delay_process_new_rows(
    ObDMLRunningCtx &run_ctx,
    const common::ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    ObStoreRow &old_tbl_row,
    ObStoreRow &new_tbl_row,
    ObRowStore &row_store)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tmp_handle;
  ObRowStore::Iterator row_iter = row_store.begin();
  while (OB_SUCC(ret) && OB_SUCC(row_iter.get_next_row(new_tbl_row.row_val_))) {
    // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
    // outside the while loop.
    if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
      tmp_handle = run_ctx.relative_table_.tablet_iter_.get_tablet_handle();
    }
    if (OB_FAIL(row_iter.get_next_row(old_tbl_row.row_val_))) {
      LOG_WARN("fail to get row from row stores", K(ret));
    } else if (OB_FAIL(process_new_row(tmp_handle,
                                       run_ctx,
                                       update_idx,
                                       rowkey_change,
                                       old_tbl_row,
                                       new_tbl_row))) {
      LOG_WARN("fail to process new row", K(ret), K(old_tbl_row), K(new_tbl_row));
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  }
  return ret;
}

int ObLSTabletService::put_rows(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const ObIArray<uint64_t> &column_ids,
    ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_update_rows_begin);
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  int64_t afct_num = 0;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ctx.is_valid())
      || OB_UNLIKELY(!ctx.is_write())
      || OB_UNLIKELY(!dml_param.is_valid())
      || OB_UNLIKELY(column_ids.count() <= 0)
      || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ctx), K(dml_param), K(column_ids), KP(row_iter));
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_UPDATE);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    void *ptr = nullptr;
    ObStoreRow* tbl_rows = nullptr;
    int64_t row_count = 0;
    int64_t batch_idx = 0;
    ObNewRow *rows = nullptr;
    void *allocated_tbl_rows_buf = nullptr;

    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      ObTabletHandle tmp_handle;
      SMART_VAR(ObRowsInfo, rows_info) {
        const ObRelativeTable &relative_table = run_ctx.relative_table_;
        while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
          ObStoreRow reserved_row;
          // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
          // outside the while loop.
          if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
            tmp_handle = run_ctx.relative_table_.tablet_iter_.get_tablet_handle();
            rows_info.reset();
            if (OB_FAIL(rows_info.init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (row_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row_count should be greater than 0", K(ret));
          } else if (1 == row_count) {
            tbl_rows = &reserved_row;
          } else if (0 == batch_idx) { // first batch
            if (OB_ISNULL(allocated_tbl_rows_buf = work_allocator.alloc(row_count * sizeof(ObStoreRow)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret), K(row_count));
            } else {
              tbl_rows = new (allocated_tbl_rows_buf) ObStoreRow[row_count];
            }
          }

          if (OB_SUCC(ret)) {
            for (int64_t i = 0; i < row_count; i++) {
              tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_UPDATE);
              tbl_rows[i].row_val_ = rows[i];
            }
            if (OB_FAIL(rows_info.assign_rows(row_count, tbl_rows))) {
              LOG_WARN("fail to assign rows", K(ret), K(batch_idx), K(row_count));
            } else if (OB_FAIL(put_rows_to_tablet(tmp_handle, run_ctx, rows_info, afct_num))) {
              LOG_WARN("insert to each tablets fail", K(ret), K(batch_idx), K(rows_info));
            }
          }
          batch_idx++;
        }  // end of while

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("Failed to allocate ObRowsInfo", K(ret));
      }
    }
    if (nullptr != allocated_tbl_rows_buf) {
      work_allocator.free(allocated_tbl_rows_buf);
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeeded to put rows", K(ret));
    affected_rows = afct_num;
    EVENT_ADD(STORAGE_UPDATE_ROW_COUNT, afct_num);
  }
  NG_TRACE(S_update_row_end);

  return ret;
}

int ObLSTabletService::delete_rows(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const ObIArray<uint64_t> &column_ids,
    ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_delete_rows_begin);
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  ObRowReshape *row_reshape = nullptr;
  int64_t afct_num = 0;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(row_iter) || !ctx.is_valid() || !ctx.is_write()
             || column_ids.count() <= 0 || OB_ISNULL(row_iter)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(dml_param), K(column_ids),
        KP(row_iter), K(ctx));
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_DELETE);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    ObStoreRow *tbl_rows = nullptr;
    ObStoreRow *tmp_tbl_rows = nullptr;
    int64_t row_count = 0;
    ObNewRow *rows = nullptr;

    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      tablet_handle.reset();
      ObRowsInfo *rows_info = nullptr;
      ObRelativeTable &relative_table = run_ctx.relative_table_;
      ObStoreRow &tbl_row = run_ctx.tbl_row_;
      int64_t cur_time = 0;
      ObTabletHandle tmp_handle;
      char *buf = nullptr;

      while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
        // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
        dml_param.lob_allocator_.reuse();
        // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
        // outside the while loop.
        if (tmp_handle.get_obj() != relative_table.tablet_iter_.get_tablet_handle().get_obj()) {
          tmp_handle = run_ctx.relative_table_.tablet_iter_.get_tablet_handle();
          if (nullptr != rows_info) {
            rows_info->reset();
            if (OB_FAIL(rows_info->init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
        }
        cur_time = ObClockGenerator::getClock();
        if (OB_FAIL(ret)) {
        } else if (cur_time > run_ctx.dml_param_.timeout_) {
          ret = OB_TIMEOUT;
          LOG_WARN("query timeout", K(cur_time), K(run_ctx.dml_param_), K(ret));
        } else if (row_count <= 0) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("row_count should be greater than 0", K(ret));
        } else if (1 == row_count) {
          tbl_rows = &tbl_row;
        } else if (nullptr == rows_info) {
          // tmp_tbl_rows is use to write the old rows into clog
          const bool need_tmp_tbl_rows = run_ctx.dml_param_.is_total_quantity_log_;
          int64_t total_tbl_rows_cnt = row_count;
          total_tbl_rows_cnt += need_tmp_tbl_rows ? row_count : 0;
          if (OB_ISNULL(buf = static_cast<char*>(work_allocator.alloc(sizeof(ObRowsInfo) + total_tbl_rows_cnt * sizeof(ObStoreRow))))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret), K(row_count));
          } else {
            rows_info = new (buf) ObRowsInfo();
            if (OB_FAIL(rows_info->init(relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info(), false/*need_find_all_duplicate_key*/))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            } else {
              tbl_rows = new (buf + sizeof(ObRowsInfo)) ObStoreRow[total_tbl_rows_cnt];
              tmp_tbl_rows = need_tmp_tbl_rows ? tbl_rows + row_count : nullptr;
            }
          }
        }

        if (OB_SUCC(ret)) {
          for (int64_t i = 0; i < row_count; i++) {
            tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_DELETE);
            tbl_rows[i].row_val_ = rows[i];
          }
          if (1 == row_count) {
            if (OB_FAIL(delete_row_in_tablet(tmp_handle, run_ctx, tbl_row))) {
              LOG_WARN("fail to delete row in tablet", K(ret), K(tbl_row));
            } else {
              afct_num += 1;
            }
          } else {
            if (OB_FAIL(rows_info->assign_rows(row_count, tbl_rows))) {
              LOG_WARN("fail to assign rows", K(ret), K(row_count));
            } else if (OB_FAIL(delete_rows_in_tablet(tmp_handle, run_ctx, tmp_tbl_rows, *rows_info))) {
              LOG_WARN("fail to delete rows in tablet", K(ret), K(*rows_info));
            } else {
              afct_num += row_count;
            }
          }
        }
      } // end of while

      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      }
      if (rows_info != nullptr) {
        rows_info->~ObRowsInfo();
        work_allocator.free(buf);
      }
    }
    if (OB_SUCC(ret)) {
      affected_rows = afct_num;
      EVENT_ADD(STORAGE_DELETE_ROW_COUNT, afct_num);
    }
  }
  NG_TRACE(S_delete_rows_end);
  return ret;
}

int ObLSTabletService::lock_rows(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const ObLockFlag lock_flag,
    const bool is_sfu,
    ObNewRowIterator *row_iter,
    int64_t &affected_rows)
{
  UNUSEDx(lock_flag, is_sfu);
  NG_TRACE(S_lock_rows_begin);
  int ret = OB_SUCCESS;
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  int64_t afct_num = 0;
  ObColDescArray col_desc;
  common::ObSEArray<uint64_t, 1> column_ids;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid()
             || !ctx.is_write()
             || !dml_param.is_valid()
             || OB_ISNULL(row_iter))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(dml_param), KPC(row_iter));
  } else {
    timeguard.click("Get");
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_LOCK);
    ObNewRow *row = nullptr;
    if (OB_FAIL(prepare_dml_running_ctx(nullptr, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else if (FALSE_IT(tablet_handle.reset())) {
    } else if (FALSE_IT(timeguard.click("Prepare"))) {
    } else if (OB_FAIL(run_ctx.relative_table_.get_rowkey_column_ids(col_desc))) {
      LOG_WARN("Fail to get column desc", K(ret));
    } else if (OB_FAIL(run_ctx.relative_table_.get_rowkey_column_ids(column_ids))) {
      LOG_WARN("Fail to get column ids", K(ret));
    } else {
      timeguard.click("GetIds");
      run_ctx.column_ids_ = &column_ids;
      ObTabletHandle tmp_handle;
      int64_t error_row_idx = 0;
      ObStoreRow tbl_row;
      while (OB_SUCCESS == ret && OB_SUCC(row_iter->get_next_row(row))) {
        // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
        // outside the while loop.
        if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
          tmp_handle = run_ctx.relative_table_.tablet_iter_.get_tablet_handle();
        }
        ObRelativeTable &relative_table = run_ctx.relative_table_;
        bool is_exists = true;
        tbl_row.row_val_ = *row;
        if (ObTimeUtility::current_time() > dml_param.timeout_) {
          ret = OB_TIMEOUT;
          int64_t cur_time = ObClockGenerator::getClock();
          LOG_WARN("query timeout", K(cur_time), K(dml_param), K(ret));
        } else if (GCONF.enable_defensive_check()
            && OB_FAIL(check_old_row_legitimacy(tmp_handle, run_ctx, 1, &tbl_row, error_row_idx))) {
          LOG_WARN("check row legitimacy failed", K(ret), KPC(row));
        } else if (GCONF.enable_defensive_check()
            && OB_FAIL(check_new_row_nullable_value(col_desc, relative_table, *row))) {
          LOG_WARN("check lock row nullable failed", K(ret));
        } else if (FALSE_IT(timeguard.click("Check"))) {
        } else if (OB_FAIL(tmp_handle.get_obj()->lock_row(run_ctx.relative_table_, ctx, *row))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
            LOG_WARN("failed to lock row", K(*row), K(ret));
          }
        } else {
          ++afct_num;
        }
        timeguard.click("Lock");
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        affected_rows = afct_num;
      }
    }
  }
  NG_TRACE(S_lock_rows_end);
  return ret;
}

int ObLSTabletService::lock_row(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const ObNewRow &row,
    const ObLockFlag lock_flag,
    const bool is_sfu)
{
  UNUSEDx(lock_flag, is_sfu);
  int ret = OB_SUCCESS;
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  ObTimeGuard timeguard(__func__, 3 * 1000 * 1000);
  int64_t afct_num = 0;
  ObColDescArray col_desc;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("tablet service is not initialized", K(ret));
  } else if (OB_UNLIKELY(!ctx.is_valid() || !dml_param.is_valid() || !row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ctx), K(dml_param), K(row));
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_LOCK);
    if (OB_FAIL(prepare_dml_running_ctx(nullptr, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else if (OB_FAIL(run_ctx.relative_table_.get_rowkey_column_ids(col_desc))) {
      LOG_WARN("Fail to get column desc", K(ret));
    } else {
      if (ObTimeUtility::current_time() > dml_param.timeout_) {
        ret = OB_TIMEOUT;
        int64_t cur_time = ObClockGenerator::getClock();
        LOG_WARN("query timeout", K(cur_time), K(dml_param), K(ret));
      } else if (OB_FAIL(tablet_handle.get_obj()->lock_row(run_ctx.relative_table_, ctx, row))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
          LOG_WARN("failed to lock row", K(row), K(ret));
        }
      } else {
        ++afct_num;
      }
    }
  }

  return ret;
}

int ObLSTabletService::trim_rebuild_tablet(
    const ObTabletID &tablet_id,
    const bool is_rollback)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_id));
  } else if (is_rollback && OB_FAIL(rollback_rebuild_tablet(tablet_id))) {
    LOG_WARN("failed to rollback tablet rebuilt", K(ret), K(is_rollback), K(tablet_id));
  } else if (!is_rollback && OB_FAIL(trim_old_tablets(tablet_id))) {
    LOG_WARN("failed to trim old tablets", K(ret), K(is_rollback), K(tablet_id));
  }

  return ret;
}

int ObLSTabletService::create_or_update_migration_tablet(
    const ObMigrationTabletParam &mig_tablet_param,
    const bool is_transfer)
{
  // TODO: is_transfer may be redundant, temporarily unused
  UNUSEDx(is_transfer);
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = mig_tablet_param.ls_id_;
  const common::ObTabletID &tablet_id = mig_tablet_param.tablet_id_;
  ObTabletHandle tablet_handle;
  bool b_exist = false;
  const bool need_create_msd_tablet = mig_tablet_param.is_empty_shell();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (!mig_tablet_param.is_empty_shell()
      && (OB_UNLIKELY(!mig_tablet_param.is_valid())
      || OB_UNLIKELY(ls_id != ls_->get_ls_id()))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(mig_tablet_param), K_(ls));
  } else if (need_create_msd_tablet) {
    if (OB_FAIL(create_empty_shell_tablet(mig_tablet_param, tablet_handle))) {
      LOG_WARN("failed to create empty shell tablet", K(ret), K(tablet_id), K(mig_tablet_param));
    }
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
      LOG_WARN("failed to check tablet existence", K(ls_id), K(tablet_id));
    } else if (b_exist
        && OB_FAIL(migrate_update_tablet(mig_tablet_param))) {
      LOG_WARN("failed to update tablet meta", K(ret), K(tablet_id), K(mig_tablet_param));
    } else if (!b_exist
        && OB_FAIL(migrate_create_tablet(mig_tablet_param, tablet_handle))) {
      LOG_WARN("failed to migrate create tablet", K(ret), K(mig_tablet_param));
    }
  }

  return ret;
}

int ObLSTabletService::rebuild_create_tablet(
    const ObMigrationTabletParam &mig_tablet_param,
    const bool keep_old)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = mig_tablet_param.ls_id_;
  const common::ObTabletID &tablet_id = mig_tablet_param.tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  bool b_exist = false;
  const bool need_create_msd_tablet = mig_tablet_param.is_empty_shell() && !keep_old;
  ObTabletHandle new_tablet_handle;
  ObTabletHandle old_tablet_handle;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!mig_tablet_param.is_valid())
      || OB_UNLIKELY(ls_id != ls_->get_ls_id())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(mig_tablet_param), K_(ls));
  } else if (OB_UNLIKELY(mig_tablet_param.is_empty_shell() && keep_old)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, rebuild create an empty shell tablet, but still need the old tablet", K(ret),
        K(mig_tablet_param), K(keep_old));
  } else if (need_create_msd_tablet) {
    if (OB_FAIL(create_empty_shell_tablet(mig_tablet_param, new_tablet_handle))) {
      LOG_WARN("failed to create empty shell tablet", K(ret), K(tablet_id), K(mig_tablet_param));
    }
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
      LOG_WARN("fail to check tablet existence", K(ls_id), K(tablet_id));
    } else if (!b_exist &&
        OB_FAIL(migrate_create_tablet(mig_tablet_param, new_tablet_handle))) {
      LOG_WARN("failed to rebuild create tablet", K(ret), K(tablet_id), K(mig_tablet_param));
    } else if (b_exist && !keep_old &&
        OB_FAIL(migrate_update_tablet(mig_tablet_param))) {
      LOG_WARN("failed to rebuild create tablet", K(ret), K(tablet_id), K(mig_tablet_param));
    } else if (b_exist && keep_old) {
      if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, old_tablet_handle,
          ObTabletCommon::DEFAULT_GET_TABLET_DURATION_US,
          ObMDSGetTabletMode::READ_WITHOUT_CHECK,
          ObTransVersion::MAX_TRANS_VERSION))) {
        LOG_WARN("failed to check and get tablet", K(ret), K(key));
      } else if (OB_UNLIKELY(old_tablet_handle.get_obj()->get_tablet_meta().has_next_tablet_)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("number of nodes on list exceeds 2", K(ret));
      } else if (OB_FAIL(rebuild_tablet_with_old(mig_tablet_param, old_tablet_handle))) {
        LOG_WARN("failed to rebuild tablet and maintain linked list", K(ret), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObLSTabletService::build_ha_tablet_new_table_store(
    const ObTabletID &tablet_id,
    const ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "BuildHaTab"));
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObMetaDiskAddr disk_addr;
  ObTimeGuard time_guard("ObLSTabletService::build_ha_tablet_new_table_store", 1_s);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(param));
  } else {
    ObTabletHandle old_tablet_handle;
    ObTabletHandle tmp_tablet_handle;
    ObTabletHandle new_tablet_handle;

    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");

    if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    } else if (old_tablet_handle.get_obj()->is_empty_shell()) {
      LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_handle.get_obj());
    } else {
      time_guard.click("GetTablet");

      ObTablet *old_tablet = old_tablet_handle.get_obj();
      ObTablet *tmp_tablet = nullptr;
      const share::ObLSID &ls_id = ls_->get_ls_id();
      const ObTabletMapKey key(ls_id, tablet_id);

      if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_handle))) {
        LOG_WARN("failed to acquire tablet", K(ret), K(key));
      } else if (FALSE_IT(tmp_tablet = tmp_tablet_handle.get_obj())) {
      } else if (OB_FAIL(tmp_tablet->init_for_sstable_replace(allocator, param, *old_tablet))) {
        LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
      } else if (FALSE_IT(time_guard.click("InitTablet"))) {
      } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(*tmp_tablet, new_tablet_handle))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_handle));
      } else if (FALSE_IT(time_guard.click("Persist"))) {
      } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(ObTabletSlogHelper::write_update_tablet_slog(ls_id, tablet_id, disk_addr))) {
        LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
      } else if (FALSE_IT(time_guard.click("WrSlog"))) {
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, new_tablet_handle))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(disk_addr));
        ob_usleep(1000 * 1000);
        ob_abort();
      } else {
        time_guard.click("CASwap");
        LOG_INFO("succeed to build ha tablet new table store", K(ret), K(key), K(disk_addr), K(param));
      }
    }
  }
  return ret;
}

int ObLSTabletService::need_check_old_row_legitimacy(ObDMLRunningCtx &run_ctx,
                                                     bool &need_check,
                                                     bool &is_udf)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &data_table = run_ctx.relative_table_;
  need_check = false;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  is_udf = false;
  // TODO(jingxing): setting this to true
  if (OB_FAIL(data_table.has_udf_column(need_check))) {
    LOG_WARN("check has udf column failed", K(ret));
  } else if (need_check) {
    is_udf = true;
    ObTableStoreIterator &table_iter = *data_table.tablet_iter_.table_iter();
    while (OB_SUCC(ret) && !need_check) {
      ObITable *table_ptr = nullptr;
      if (OB_FAIL(table_iter.get_next(table_ptr))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("get next table failed", K(ret));
        }
      } else if (OB_ISNULL(table_ptr)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("error unexpected, table ptr must not be nullptr", K(ret));
      } else {
        need_check = table_ptr->is_major_sstable();
      }
    }
  } else if (GCONF.enable_defensive_check()) {
    need_check = true;
    if (data_table.is_index_table() && !data_table.can_read_index()) {
      //index can not be read during building index, so does not check old index row
      need_check = false;
    }
    if (ObDmlFlag::DF_LOCK == run_ctx.dml_flag_) {
      need_check = false;
    }
  }
  return ret;
}

int ObLSTabletService::check_old_row_legitimacy(
    ObTabletHandle &data_tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const int64_t row_count,
    const ObStoreRow *old_tbl_rows,
    int64_t &error_row_idx)
{
  int ret = OB_SUCCESS;
  // usage:
  //   alter system set_tp tp_no=9,match=3221487629,error_code=4377,frequency=1
  // where session_id is 3221487629
  error_row_idx = 0;
  const int inject_err = OB_E(EventTable::EN_9, run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_->get_session_id()) OB_SUCCESS;
  if (OB_ERR_DEFENSIVE_CHECK == inject_err) {
    ret = OB_ERR_DEFENSIVE_CHECK;
  }
  ObRelativeTable &data_table = run_ctx.relative_table_;
  bool need_check = false;
  bool is_udf = false;
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(run_ctx.column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old row is invalid", K(ret));
  } else if (OB_FAIL(need_check_old_row_legitimacy(run_ctx, need_check, is_udf))) {
    LOG_WARN("identify need check old row legitimacy", K(ret));
  } else if (need_check) {
    //the vertical partition is no longer maintained,
    //and the defense check skips the vertical partition function
    const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
    ObArenaAllocator scan_allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_TABLE_SCAN_ITER));
    ObIAllocator *allocator = &scan_allocator;
    ObSingleRowGetter old_row_getter(*allocator, *data_tablet_handle.get_obj());
    ObNewRow *storage_old_row = nullptr;
    const ObIArray<uint64_t> &column_ids = *run_ctx.column_ids_;
    if (OB_FAIL(init_single_row_getter(old_row_getter, run_ctx, column_ids, data_table, true))) {
      LOG_WARN("failed to init single row getter", K(ret));
    }
    ObStoreRowkey rowkey;
    ObDatumRowkey datum_rowkey;
    ObDatumRowkeyHelper rowkey_helper;

    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      const common::ObNewRow &old_row = old_tbl_rows[i].row_val_;
      rowkey.assign(old_row.cells_, data_table.get_rowkey_column_num());
      //check old row whether different with SQL.old_row,
      uint64_t err_col_id = OB_INVALID_ID;
      if (OB_FAIL(rowkey_helper.convert_datum_rowkey(rowkey.get_rowkey(), datum_rowkey))) {
        STORAGE_LOG(WARN, "Failed to transfer datum rowkey", K(ret), K(rowkey));
      } else if (OB_FAIL(old_row_getter.open(datum_rowkey, true))) {
        LOG_WARN("open old row getter failed", K(ret), K(rowkey));
      } else if (OB_FAIL(old_row_getter.get_next_row(storage_old_row))) {
        if (OB_ITER_END == ret) {
          ret = OB_ERR_DEFENSIVE_CHECK;
          FLOG_WARN("old row in storage is not exists", K(ret));
        } else {
          LOG_WARN("get next row from old_row_iter failed", K(ret), KPC(run_ctx.column_ids_), K(i), K(old_row));
        }
      } else if (OB_ISNULL(storage_old_row)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected error, storage old row is NULL", K(ret));
      } else if (storage_old_row->get_count() != old_row.get_count()) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        FLOG_WARN("storage old row is not matched with sql old row", K(ret));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < old_row.get_count(); ++i) {
          const ObObj &storage_val = storage_old_row->get_cell(i);
          const ObObj &sql_val = old_row.get_cell(i);
          int cmp = 0;
          if (sql_val.is_lob_storage()) {
            // skip all text and lob
          } else if (OB_UNLIKELY(ObLongTextType == storage_val.get_type() && sql_val.is_lob_locator())) {
            //skip check lob column type when do the normal sql write check
          } else if (OB_UNLIKELY(storage_val.is_nop_value())) {
            bool is_nop = false;
            if (OB_FAIL(data_table.is_nop_default_value(column_ids.at(i), is_nop))) {
              LOG_WARN("check column whether has nop default value failed", K(ret), K(column_ids.at(i)));
            } else if (!is_nop) {
              err_col_id = column_ids.at(i);
              ret = OB_ERR_DEFENSIVE_CHECK;
              FLOG_WARN("storage old row is not matched with sql old row", K(ret),
                      K(i), K(column_ids.at(i)), K(storage_val), K(sql_val));
            }
          } else if (sql_val.is_nop_value()) {
            //this column is nop val, means that this column does not be touched by DML
            //just ignore it
          } else if (OB_FAIL(storage_val.compare(sql_val, cmp)) || 0 != cmp) {
            ret = OB_ERR_DEFENSIVE_CHECK;
            err_col_id = column_ids.at(i);
            FLOG_WARN("storage_val is not equal with sql_val, maybe catch a bug", K(ret),
                    K(storage_val), K(sql_val), K(cmp), K(column_ids.at(i)));
          }
        }
      }

      if (OB_ERR_DEFENSIVE_CHECK == ret && dml_param.is_batch_stmt_) {
        // 批量删除的时候可能索引表的删除在主表前边，所以所有的表在batch删除的时候出现4377，都可能是重复删导致的
        ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
        LOG_TRACE("batch stmt execution has a correctness error, needs rollback", K(ret),
                  "column_id", column_ids,
                  KPC(storage_old_row), K(i),
                  "sql_old_row", old_row,
                  "dml_param", run_ctx.dml_param_,
                  "dml_type", run_ctx.dml_flag_);
      }
      if (OB_ERR_DEFENSIVE_CHECK == ret) {
        int tmp_ret = OB_SUCCESS;
        bool is_virtual_gen_col = false;
        if (OB_TMP_FAIL(check_real_leader_for_4377_(run_ctx.store_ctx_.ls_id_))) {
          ret = tmp_ret;
          LOG_WARN("check real leader for 4377 found exception", K(ret), K(i), K(old_row), K(data_table));
        } else if (nullptr != run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_
            && OB_TMP_FAIL(check_need_rollback_in_transfer_for_4377_(run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_,
                                                                         data_tablet_handle))) {
          ret = tmp_ret;
          LOG_WARN("check need rollback in transfer for 4377 found exception", K(ret), K(old_row), K(data_table));
        } else if (OB_TMP_FAIL(check_parts_tx_state_in_transfer_for_4377_(run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_))) {
          ret = tmp_ret;
          LOG_WARN("check need rollback in transfer for 4377 found exception", K(ret), K(old_row), K(data_table));
        } else if (is_udf) {
          ret = OB_ERR_INDEX_KEY_NOT_FOUND;
          LOG_WARN("index key not found on udf column", K(ret), K(old_row));
        } else if (data_table.is_index_table() && OB_TMP_FAIL(check_is_gencol_check_failed(data_table, err_col_id, is_virtual_gen_col))) {
          //don't change ret if gencol check failed
          LOG_WARN("check is functional index failed", K(ret), K(tmp_ret), K(data_table));
        } else if (is_virtual_gen_col) {
          ret = OB_ERR_GENCOL_LEGIT_CHECK_FAILED;
          LOG_WARN("Legitimacy check failed for functional index.", K(ret), K(old_row), KPC(storage_old_row));
        }
        if (OB_ERR_DEFENSIVE_CHECK == ret) {
          ObString func_name = ObString::make_string("check_old_row_legitimacy");
          LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
          LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                        "Fatal Error!!! Catch a defensive error!",
                        K(ret),
                        "column_id", column_ids,
                        KPC(storage_old_row),
                        "sql_old_row", old_row,
                        "dml_param", run_ctx.dml_param_,
                        "dml_flag", run_ctx.dml_flag_,
                        "store_ctx", run_ctx.store_ctx_,
                        "relative_table", run_ctx.relative_table_);
          LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                           OB_ERR_DEFENSIVE_CHECK,
                           "Fatal Error!!! Catch a defensive error!");
          concurrency_control::ObDataValidationService::set_delay_resource_recycle(run_ctx.store_ctx_.ls_id_);
          LOG_ERROR("Dump data table info", K(ret), K(data_table));
          run_ctx.store_ctx_.force_print_trace_log();
        }
      }

      if (OB_FAIL(ret)) {
        error_row_idx = i;
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_is_gencol_check_failed(const ObRelativeTable &data_table, uint64_t error_col_id, bool &is_virtual_gen_col)
{
  int ret = OB_SUCCESS;
  is_virtual_gen_col = false;
  if (data_table.is_index_table()) {
    const ObColumnParam *param = nullptr;
    const uint64_t tenant_id = MTL_ID();
    uint64_t index_table_id = data_table.get_table_id();
    const ObTableSchema *index_table_schema = NULL;
    const ObTableSchema *data_table_schema = NULL;
    ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService*)->get_schema_service();
    ObSchemaGetterGuard schema_guard;
    if (OB_ISNULL(schema_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null", K(ret), KP(schema_service));
    } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema manager", K(ret), K(tenant_id));
    }  else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_id, index_table_schema))) {
      LOG_WARN("get index table schema failed", K(ret));
    } else if (OB_ISNULL(index_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index table schema is unexpected null", K(ret));
    } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, index_table_schema->get_data_table_id(), data_table_schema))) {
      LOG_WARN("get data table schema failed", K(ret));
    } else if (OB_ISNULL(data_table_schema)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("data table schema is unexpected null", K(ret));
    } else if (OB_INVALID_ID != error_col_id) {
      //check specified column
      const ObColumnSchemaV2 *column = NULL;
      if (is_shadow_column(error_col_id)) {
        //shadow column does not exists in basic table, do nothing
      } else if (OB_ISNULL(column = data_table_schema->get_column_schema(error_col_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null", K(ret), KP(column));
      } else if (column->is_virtual_generated_column()) {
        is_virtual_gen_col = true;
      }
    } else {
      //check all columns
      for (ObTableSchema::const_column_iterator iter = index_table_schema->column_begin();
          OB_SUCC(ret) && iter != index_table_schema->column_end() && !is_virtual_gen_col; iter++) {
        const ObColumnSchemaV2 *column = *iter;
        //the column id in the data table is the same with that in the index table
        if (OB_ISNULL(column)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), KP(column));
        } else if (is_shadow_column(column->get_column_id())) {
          //shadow column does not exists in basic table, do nothing
        } else if (OB_ISNULL(column = data_table_schema->get_column_schema(column->get_column_id()))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null", K(ret), KP(column));
        } else if (column->is_virtual_generated_column()) {
          is_virtual_gen_col = true;
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_new_row_legitimacy(
    ObDMLRunningCtx &run_ctx,
    const int64_t row_count,
    const ObStoreRow *new_tbl_rows)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &data_table = run_ctx.relative_table_;
  int64_t data_table_cnt = data_table.get_column_count();
  if (OB_ISNULL(run_ctx.column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column ids is nullptr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    const ObNewRow &new_row = new_tbl_rows[i].row_val_;
    if (OB_FAIL(check_new_row_nullable_value(*run_ctx.column_ids_, data_table, new_row))) {
      LOG_WARN("check new row nullable value failed", K(ret),
               "dml_param", run_ctx.dml_param_,
               "dml_type", run_ctx.dml_flag_,
               K(new_row));
    } else if (OB_FAIL(check_new_row_shadow_pk(*run_ctx.column_ids_, data_table, new_row))) {
      LOG_WARN("check new row nullable value failed", K(ret),
               "dml_param", run_ctx.dml_param_,
               "dml_type", run_ctx.dml_flag_,
               K(new_row));
    }
  }
  return ret;
}

int ObLSTabletService::insert_rows_to_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObRowsInfo &rows_info,
    int64_t &afct_num)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = run_ctx.store_ctx_;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  ObRelativeTable &data_table = run_ctx.relative_table_;
  const int64_t row_count = rows_info.get_rowkey_cnt();
  if (OB_FAIL(ret)) {
  } else if (ObClockGenerator::getClock() > dml_param.timeout_) {
    ret = OB_TIMEOUT;
    int64_t cur_time = ObClockGenerator::getClock();
    LOG_WARN("query timeout", K(cur_time), K(dml_param), K(ret));
  } else if (OB_FAIL(insert_lob_tablet_rows(tablet_handle, run_ctx, rows_info.rows_, row_count))) {
    LOG_WARN("failed to insert rows to lob tablet", K(ret));
  } else if (OB_FAIL(insert_tablet_rows(tablet_handle, run_ctx, rows_info))) {
    LOG_WARN("failed to insert rows to data tablet", K(ret));
  } else {
    afct_num = afct_num + row_count;
  }
  return ret;
}

int ObLSTabletService::insert_tablet_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &table = run_ctx.relative_table_;
  const bool check_exists = !table.is_storage_index_table() ||
          table.is_unique_index() ||
          run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_update_pk_dop();
  const int64_t row_count = rows_info.get_rowkey_cnt();

  // 1. Defensive checking of new rows.
  if (GCONF.enable_defensive_check()) {
    if (OB_FAIL(check_new_row_legitimacy(run_ctx, row_count, rows_info.rows_))) {
      LOG_WARN("Failed to check new row legitimacy", K(ret), K(rows_info));
    }
  }

  // 2. Insert rows with uniqueness constraint and write conflict checking.
  // Check write conflict in memtable + sstable.
  // Check uniqueness constraint in sstable only.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_handle.get_obj()->insert_rows(table,
                                                     run_ctx.store_ctx_,
                                                     check_exists,
                                                     *run_ctx.col_descs_,
                                                     run_ctx.dml_param_.encrypt_meta_,
                                                     rows_info))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        blocksstable::ObDatumRowkey &duplicate_rowkey = rows_info.get_conflict_rowkey();
        LOG_WARN("Rowkey already exist", K(ret), K(duplicate_rowkey), K(row_count),
            K(rows_info.get_conflict_idx()), "need_find_all_duplicate_key", rows_info.need_find_all_duplicate_key());
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("Failed to insert rows to tablet", K(ret), K(rows_info));
      }
    }
  }

  // 4. Log user error message if rowkey is duplicate.
  if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret && !run_ctx.dml_param_.is_ignore_ && !rows_info.need_find_all_duplicate_key()) {
    int tmp_ret = OB_SUCCESS;
    char rowkey_buffer[OB_TMP_BUF_SIZE_256];
    ObString index_name = "PRIMARY";
    if (OB_TMP_FAIL(extract_rowkey(table, rows_info.get_conflict_rowkey(),
         rowkey_buffer, OB_TMP_BUF_SIZE_256, run_ctx.dml_param_.tz_info_))) {
      LOG_WARN("Failed to extract rowkey", K(ret), K(tmp_ret));
    }
    if (table.is_index_table()) {
      if (OB_TMP_FAIL(table.get_index_name(index_name))) {
        LOG_WARN("Failed to get index name", K(ret), K(tmp_ret));
      }
    } else if (lib::is_oracle_mode() && OB_TMP_FAIL(table.get_primary_key_name(index_name))) {
      LOG_WARN("Failed to get pk name", K(ret), K(tmp_ret));
    }
    LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, index_name.length(), index_name.ptr());
  }
  return ret;
}

int ObLSTabletService::put_rows_to_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObRowsInfo &rows_info,
    int64_t &afct_num)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = run_ctx.store_ctx_;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  ObRelativeTable &data_table = run_ctx.relative_table_;
  const int64_t row_count = rows_info.get_rowkey_cnt();

  for (int64_t i = 0; i < run_ctx.col_descs_->count() && OB_SUCC(ret); ++i) {
    const ObColDesc &column = run_ctx.col_descs_->at(i);
    if (column.col_type_.is_lob_storage()) {
      ret = OB_NOT_SUPPORTED;
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "Lob column uses put_rows interface");
      LOG_WARN("put_rows not support lob", K(ret), K(column));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (ObClockGenerator::getClock() > dml_param.timeout_) {
    ret = OB_TIMEOUT;
    int64_t cur_time = ObClockGenerator::getClock();
    LOG_WARN("query timeout", K(cur_time), K(dml_param), K(ret));
  } else if (OB_FAIL(put_tablet_rows(tablet_handle, run_ctx, rows_info))) {
    LOG_WARN("failed to put rows to data tablet", K(ret));
  } else {
    afct_num = afct_num + row_count;
  }
  return ret;
}

int ObLSTabletService::put_tablet_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &table = run_ctx.relative_table_;
  const int64_t row_count = rows_info.get_rowkey_cnt();
  // 1. Defensive checking of new rows.
  if (GCONF.enable_defensive_check()) {
    if (OB_FAIL(check_new_row_legitimacy(run_ctx, row_count, rows_info.rows_))) {
      LOG_WARN("Failed to check new row legitimacy", K(ret), K(rows_info));
    }
  }
  // 2. Insert rows with write conflict checking.
  // Check write conflict in memtable + sstable.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(tablet_handle.get_obj()->insert_rows(table,
                                                     run_ctx.store_ctx_,
                                                     false /* check_exists */,
                                                     *run_ctx.col_descs_,
                                                     run_ctx.dml_param_.encrypt_meta_,
                                                     rows_info))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("Failed to insert rows to tablet", K(ret), K(rows_info));
      }
    }
  }
  return ret;
}

int ObLSTabletService::insert_lob_col(
    ObDMLRunningCtx &run_ctx,
    const ObColDesc &column,
    ObObj &obj,
    ObLobAccessParam *del_param,
    ObLobCommon *lob_common)
{
  int ret = OB_SUCCESS;
  uint64_t lob_id;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]failed to get lob manager handle.", K(ret));
  } else if (!column.col_type_.is_lob_storage() || obj.is_nop_value() || obj.is_null()) {
    // do nothing
  } else {
    // init lob access param
    ObLobAccessParam lob_param;
    lob_param.tx_desc_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_;
    lob_param.parent_seq_no_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_;
    lob_param.snapshot_ = run_ctx.dml_param_.snapshot_;
    lob_param.is_total_quantity_log_ = run_ctx.dml_param_.is_total_quantity_log_;
    if (lob_param.snapshot_.is_none_read()) {
      // NOTE:
      // lob_insert need table_scan, the snapshot already generated in
      // run_ctx.store_ctx, use it as an LS ReadSnapshot
      lob_param.snapshot_.init_ls_read(run_ctx.store_ctx_.ls_id_,
                                       run_ctx.store_ctx_.mvcc_acc_ctx_.snapshot_);
    }
    lob_param.tx_id_ = lob_param.tx_desc_->get_tx_id();
    lob_param.sql_mode_ = run_ctx.dml_param_.sql_mode_;
    lob_param.ls_id_ = run_ctx.store_ctx_.ls_id_;
    lob_param.tablet_id_ = run_ctx.relative_table_.get_tablet_id();
    lob_param.coll_type_ = ObLobCharsetUtil::get_collation_type(column.col_type_.get_type(), column.col_type_.get_collation_type());
    lob_param.allocator_ = &run_ctx.dml_param_.lob_allocator_;
    lob_param.lob_common_ = lob_common;
    if (OB_NOT_NULL(del_param)) {
      lob_param.handle_size_ = del_param->handle_size_;
      lob_param.checksum_ = del_param->checksum_;
      lob_param.total_seq_cnt_ = del_param->total_seq_cnt_;
      lob_param.used_seq_cnt_ = del_param->used_seq_cnt_;
      lob_param.seq_no_st_ = del_param->seq_no_st_;
    }
    lob_param.timeout_ = run_ctx.dml_param_.timeout_;
    lob_param.scan_backward_ = false;
    lob_param.offset_ = 0;
    // Notice: currently only inrow data
    ObString raw_data = obj.get_string();
    ObString data;
    // for not strict sql mode, will insert empty string without lob header
    bool has_lob_header = obj.has_lob_header() && raw_data.length() > 0;
    ObLobLocatorV2 loc(raw_data, has_lob_header);
    if (OB_FAIL(set_lob_storage_params(run_ctx, column, lob_param))) {
      LOG_WARN("set_lob_storage_params fail", K(ret), K(column));
    } else if (OB_FAIL(lob_mngr->append(lob_param, loc))) {
      LOG_WARN("[STORAGE_LOB]lob append failed.", K(ret));
    } else {
      ObLobCommon *res_lob_common = lob_param.lob_common_;
      obj.set_lob_value(obj.get_type(), res_lob_common, lob_param.handle_size_);
      LOG_DEBUG("[STORAGE_LOB]write ob lob data.", K(lob_param), KPC(res_lob_common),
                K(lob_param.handle_size_), K(column.col_type_.get_collation_type()));
    }
  }
  return ret;
}

int ObLSTabletService::insert_lob_tablet_row(
    ObTabletHandle &data_tablet,
    ObDMLRunningCtx &run_ctx,
    ObStoreRow &row)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = run_ctx.col_descs_->count();
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]failed to get lob manager handle.", K(ret));
  } else if (row.row_val_.count_ != col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]column count invalid", K(ret), K(col_cnt), K(row.row_val_.count_));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObColDesc &column = run_ctx.col_descs_->at(i);
      ObObj &obj = row.row_val_.get_cell(i);
      if (obj.is_null() || obj.is_nop_value()) {
        // do nothing
      } else if (column.col_type_.is_lob_storage()) {
        if(OB_FAIL(insert_lob_col(run_ctx, column, obj, nullptr, nullptr))) {
          LOG_WARN("[STORAGE_LOB]failed to insert lob col.", K(ret), K(row), K(i));
        }
      }
    }
  }
  return ret;
}

int update_lob_meta_table_seq_no(ObDMLRunningCtx &run_ctx, int64_t row_count)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  const ObTableDMLParam *table_param = dml_param.table_param_;
  if (OB_ISNULL(table_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_param is null", K(ret));
  } else if (! table_param->get_data_table().is_lob_meta_table()) {
    // skip if not lob meta table
  } else if (row_count != 1) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("lob meta table row_count incorrect", K(ret), K(row_count));
  } else if (! dml_param.spec_seq_no_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("spec_seq_no_ is invalid", K(ret), K(row_count), K(dml_param));
  } else if (! run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_.is_valid()
      || run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_ > dml_param.spec_seq_no_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("seq_no unexpected", K(run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_), K(dml_param.spec_seq_no_));
  } else {
    LOG_DEBUG("set seq_no", K(run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_), K(dml_param.spec_seq_no_));
    run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_ = dml_param.spec_seq_no_;
  }
  return ret;
}

int ObLSTabletService::insert_lob_tablet_rows(
    ObTabletHandle &data_tablet,
    ObDMLRunningCtx &run_ctx,
    ObStoreRow *rows,
    int64_t row_count)
{
  int ret = OB_SUCCESS;
  // DEBUG_SYNC(DELAY_INDEX_WRITE);
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]failed to get lob manager handle.", K(ret));
  } else if (OB_FAIL(update_lob_meta_table_seq_no(run_ctx, row_count))) {
    LOG_WARN("update_lob_meta_table_seq_no fail", K(ret), K(run_ctx.dml_param_));
  } else {
    int64_t col_cnt = run_ctx.col_descs_->count();
    for (int64_t k = 0; OB_SUCC(ret) && k < row_count; k++) {
      if (OB_FAIL(insert_lob_tablet_row(data_tablet, run_ctx, rows[k]))) {
        LOG_WARN("[STORAGE_LOB]failed to insert lob row.", K(ret));
      }
    }
  }
  return ret;
}

int ObLSTabletService::extract_rowkey(
    const ObRelativeTable &table,
    const blocksstable::ObDatumRowkey &rowkey,
    char *buffer,
    const int64_t buffer_len,
    const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<share::schema::ObColDesc, common::OB_MAX_ROWKEY_COLUMN_NUMBER> rowkey_cols;
  ObStoreRowkey store_rowkey;
  ObDatumRowkeyHelper rowkey_helper;
  if (OB_FAIL(table.get_rowkey_column_ids(rowkey_cols))) {
    STORAGE_LOG(WARN, "Failed to get rowkey cols", K(ret), K(table));
  } else if (OB_FAIL(rowkey_helper.convert_store_rowkey(rowkey, rowkey_cols, store_rowkey))) {
    STORAGE_LOG(WARN, "Failed to convert store rowkey", K(ret), K(rowkey));
  } else {
    ret = extract_rowkey(table, store_rowkey, buffer, buffer_len, tz_info);
  }
  return ret;
}

int ObLSTabletService::extract_rowkey(
    const ObRelativeTable &table,
    const common::ObStoreRowkey &rowkey,
    char *buffer,
    const int64_t buffer_len,
    const ObTimeZoneInfo *tz_info)
{
  int ret = OB_SUCCESS;

  if (!table.is_valid() || !rowkey.is_valid() || OB_ISNULL(buffer) || buffer_len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(table), K(rowkey), K(buffer), K(buffer_len), K(tz_info));
  } else {
    const int64_t rowkey_size = table.get_rowkey_column_num();
    int64_t pos = 0;
    int64_t valid_rowkey_size = 0;
    uint64_t column_id = OB_INVALID_ID;

    for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_size; i++) {
      if (OB_FAIL(table.get_rowkey_col_id_by_idx(i, column_id))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Failed to get rowkey column description", K(i), K(ret));
      } else if (column_id <= OB_MIN_SHADOW_COLUMN_ID) {
        valid_rowkey_size ++;
      }
    }
    for (int64_t i = 0; OB_SUCC(ret) && i < valid_rowkey_size; ++i) {
      const ObObj &obj  = rowkey.get_obj_ptr()[i];
      if (OB_FAIL(obj.print_plain_str_literal(buffer, buffer_len - 1, pos, tz_info))) {
        LOG_WARN("fail to print_plain_str_literal", K(ret));
      } else if (i < valid_rowkey_size - 1) {
        if (OB_FAIL(databuff_printf(buffer,  buffer_len - 1, pos, "-"))) {
          LOG_WARN("databuff print failed", K(ret));
        }
      }
    }
    if (buffer != nullptr) {
      buffer[pos++] = '\0';
    }
  }

  return ret;
}

int ObLSTabletService::get_next_rows(
    ObNewRowIterator *row_iter,
    ObNewRow *&rows,
    int64_t &row_count)
{
  return row_iter->get_next_rows(rows, row_count);
}

int ObLSTabletService::construct_update_idx(
    const int64_t schema_rowkey_cnt,
    const share::schema::ColumnMap *col_map,
    const common::ObIArray<uint64_t> &upd_col_ids,
    UpdateIndexArray &update_idx)
{
  int ret = OB_SUCCESS;
  int err = OB_SUCCESS;

  if (OB_ISNULL(col_map) || upd_col_ids.count() <= 0 || update_idx.count() > 0 || schema_rowkey_cnt <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(col_map), K(upd_col_ids), K(upd_col_ids.count()), K(schema_rowkey_cnt));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < upd_col_ids.count(); ++i) {
      int32_t idx = -1;
      const uint64_t &col_id = upd_col_ids.at(i);
      if (OB_SUCCESS != (err = col_map->get(col_id, idx)) || idx < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("column id doesn't exist", K(ret), K(col_id), K(err));
      } else if (idx < schema_rowkey_cnt) {
        // update_idx should not contain rowkey
      } else if (OB_FAIL(update_idx.push_back(idx))) {
        LOG_WARN("fail to push idx into update_idx", K(ret), K(idx));
      }
    }
    if (OB_SUCC(ret) && update_idx.count() > 1) {
      lib::ob_sort(update_idx.begin(), update_idx.end());
    }
  }

  return ret;
}

int ObLSTabletService::check_rowkey_change(
    const ObIArray<uint64_t> &update_ids,
    const ObRelativeTable &relative_table,
    bool &rowkey_change)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(update_ids.count() <= 0 || !relative_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(update_ids), K(ret));
  } else {
    const int64_t count = update_ids.count();
    bool is_rowkey = false;
    rowkey_change = false;
    for (int64_t i = 0; OB_SUCC(ret) && i < count && !rowkey_change; ++i) {
      if (OB_FAIL(relative_table.is_rowkey_column_id(update_ids.at(i), is_rowkey))) {
        LOG_WARN("check is_rowkey fail", K(ret), K(update_ids.at(i)));
      } else {
        rowkey_change = is_rowkey;
      }
    }

    if (OB_FAIL(ret)) {
      // do nothing
    } else if (relative_table.is_unique_index() && !rowkey_change) {
      uint64_t cid = OB_INVALID_ID;
      bool innullable = true;
      for (int64_t j = 0; OB_SUCC(ret) && j < relative_table.get_rowkey_column_num() && !rowkey_change; ++j) {
        if (OB_FAIL(relative_table.get_rowkey_col_id_by_idx(j, cid))) {
          LOG_WARN("get rowkey column id fail", K(ret), K(j));
        } else if (cid > OB_MIN_SHADOW_COLUMN_ID) {
          if (innullable) {
            break; // other_change
          } else {
            cid -= OB_MIN_SHADOW_COLUMN_ID;
            for (int64_t k = 0; OB_SUCC(ret) && k < count; ++k) {
              if (cid == update_ids.at(k)) {
                rowkey_change = true;
                break;
              }
            }
          }
        } else {
          bool is_nullable = false;
          if (OB_FAIL(relative_table.is_column_nullable_for_write(cid, is_nullable))) {
            LOG_WARN("check nullable fail", K(ret), K(cid), K(relative_table));
          } else if (is_nullable) {
            innullable = false;
          }
        }
      }
    }

    LOG_DEBUG("check rowkey change for update", K(rowkey_change), K(update_ids), K(relative_table));
  }

  return ret;
}

int ObLSTabletService::process_delta_lob(
    ObDMLRunningCtx &run_ctx,
    const ObColDesc &column,
    ObObj &old_obj,
    ObLobLocatorV2 &delta_lob,
    ObObj &obj)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  bool has_diff = false;
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]failed to get lob manager handle.", K(ret));
  } else if (!delta_lob.is_delta_temp_lob()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB] invalid lob type", K(ret), K(delta_lob));
  } else if (OB_FAIL(ObDeltaLob::has_diff(delta_lob, has_diff))) {
    LOG_WARN("delata lob has_diff fail", K(ret), K(delta_lob), K(column));
  } else if (! has_diff) {
    LOG_DEBUG("no diff means no update, so set nop", K(delta_lob), K(column));
    obj.set_nop_value();
  } else {
    ObLobAccessParam lob_param;
    // init lob param
    lob_param.tx_desc_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_;
    lob_param.parent_seq_no_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_;
    lob_param.snapshot_ = run_ctx.dml_param_.snapshot_;
    if (lob_param.snapshot_.is_none_read()) {
      // NOTE:
      // lob_insert need table_scan, the snapshot already generated in
      // run_ctx.store_ctx, use it as an LS ReadSnapshot
      lob_param.snapshot_.init_ls_read(run_ctx.store_ctx_.ls_id_,
                                       run_ctx.store_ctx_.mvcc_acc_ctx_.snapshot_);
    }
    lob_param.tx_id_ = lob_param.tx_desc_->get_tx_id();
    lob_param.sql_mode_ = run_ctx.dml_param_.sql_mode_;
    lob_param.ls_id_ = run_ctx.store_ctx_.ls_id_;
    lob_param.tablet_id_ = run_ctx.relative_table_.get_tablet_id();
    lob_param.coll_type_ = ObLobCharsetUtil::get_collation_type(column.col_type_.get_type(), column.col_type_.get_collation_type());
    lob_param.allocator_ = &run_ctx.dml_param_.lob_allocator_;
    // should use old obj lob
    ObLobLocatorV2 old_lob;
    ObString old_disk_lob;
    if (OB_FAIL(set_lob_storage_params(run_ctx, column, lob_param))) {
      LOG_WARN("set_lob_storage_params fail", K(ret), K(column));
    } else if (OB_FAIL(old_obj.get_lob_locatorv2(old_lob))) {
      LOG_WARN("get old lob locator failed.", K(ret), K(old_obj));
    } else if (!old_lob.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("old lob locator is invalid.", K(ret));
    } else if (OB_FAIL(old_lob.get_disk_locator(old_disk_lob))) {
      LOG_WARN("fail to get old lob disk locator.", K(ret));
    } else {
      lob_param.lob_locator_ = nullptr;
      lob_param.lob_common_ = reinterpret_cast<ObLobCommon*>(old_disk_lob.ptr());
      lob_param.handle_size_ = old_disk_lob.length();
      lob_param.byte_size_ = lob_param.lob_common_->get_byte_size(lob_param.handle_size_);

      lob_param.timeout_ = run_ctx.dml_param_.timeout_;
      lob_param.scan_backward_ = false;
      if (OB_FAIL(lob_mngr->process_delta(lob_param, delta_lob))) {
        LOG_WARN("failed to process delta lob.", K(ret), K(lob_param), K(delta_lob));
      } else {
        // update obj with new disk locator
        obj.set_lob_value(obj.get_type(), lob_param.lob_common_, lob_param.handle_size_);
        ObExtInfoLogHeader header;
        header.init(obj, true);
        if (! lob_param.ext_info_log_.is_null()
          && OB_FAIL(register_ext_info_commit_cb(run_ctx, obj, lob_param.ext_info_log_, header))) {
          LOG_WARN("register_ext_info_commit_cb fail", K(ret), K(lob_param));
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::register_ext_info_commit_cb(
    ObDMLRunningCtx &run_ctx,
    ObObj &col_data,
    ObObj &ext_info_data,
    const ObExtInfoLogHeader &header)
{
  int ret = OB_SUCCESS;
  if (ext_info_data.is_null()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ext_info_log is null", K(ret), K(ext_info_data));
  } else if (OB_FAIL(run_ctx.store_ctx_.mvcc_acc_ctx_.mem_ctx_->register_ext_info_commit_cb(
      run_ctx.store_ctx_,
      run_ctx.dml_param_.timeout_,
      run_ctx.dml_flag_,
      col_data,
      ext_info_data,
      run_ctx.dml_param_.snapshot_,
      header,
      run_ctx.relative_table_.get_tablet_id()))) {
    LOG_WARN("register_ext_info_commit_cb fail", K(ret), K(run_ctx.store_ctx_), K(col_data), K(ext_info_data));
  }
  return ret;
}

int ObLSTabletService::set_lob_storage_params(
    ObDMLRunningCtx &run_ctx,
    const ObColDesc &column,
    ObLobAccessParam &lob_param)
{
  int ret = OB_SUCCESS;
  const ObTableDMLParam *table_param = run_ctx.dml_param_.table_param_;
  const ObColumnParam *column_param = nullptr;
  if (OB_ISNULL(table_param)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("table_param is null", K(ret));
  } else if (OB_ISNULL(column_param = table_param->get_data_table().get_column(column.col_id_))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column_param is null", K(ret), K(table_param));
  } else {
    lob_param.schema_chunk_size_ = column_param->get_lob_chunk_size();
    lob_param.inrow_threshold_ = table_param->get_data_table().get_lob_inrow_threshold();
  }
  return ret;
}

int ObLSTabletService::process_lob_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    const int64_t row_count,
    ObStoreRow *old_rows,
    ObStoreRow *new_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i ++) {
    if (OB_FAIL(process_lob_row(tablet_handle, run_ctx, update_idx, rowkey_change, old_rows[i], new_rows[i]))) {
      LOG_WARN("fail to process_lob_row", K(ret), K(i), K(old_rows[i]), K(new_rows[i]));
    }
  }
  return ret;
}

int ObLSTabletService::process_lob_row(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    ObStoreRow &old_row,
    ObStoreRow &new_row)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(old_row.row_val_.get_count() != new_row.row_val_.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[STORAGE_LOB]invalid args", K(old_row), K(new_row), K(ret));
  } else if (OB_UNLIKELY(old_row.row_val_.get_count() != run_ctx.col_descs_->count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("[STORAGE_LOB]invalid args", K(old_row), K(new_row), KPC(run_ctx.col_descs_));
  } else if (OB_FAIL(update_lob_meta_table_seq_no(run_ctx, 1/*row_count*/))) {
    LOG_WARN("update_lob_meta_table_seq_no fail", K(ret), K(run_ctx.dml_param_));
  } else {
    ObStoreRow old_sql_row;
    old_sql_row.row_val_ = old_row.row_val_;

    for (int64_t i = 0; OB_SUCC(ret) && i < old_row.row_val_.get_count(); ++i) {
      if (run_ctx.col_descs_->at(i).col_type_.is_lob_storage()) {
        ObObj &old_obj = old_row.row_val_.get_cell(i);
        ObObj &new_obj = new_row.row_val_.get_cell(i);
        bool is_update = false;
        for (int64_t j = 0; !is_update && j < update_idx.count(); ++j) {
          if (update_idx.at(j) == i) {
            is_update = true;
          }
        }
        if (is_update) {
          // get new lob locator
          ObString new_lob_str = (new_obj.is_null() || new_obj.is_nop_value())
                                 ? ObString(0, nullptr) : new_obj.get_string();
          // for not strict sql mode, will insert empty string without lob header
          bool has_lob_header = new_obj.has_lob_header() && new_lob_str.length() > 0;
          ObLobLocatorV2 new_lob(new_lob_str, has_lob_header);
          if (OB_FAIL(ret)) {
          } else if (new_obj.is_null() ||
                     new_obj.is_nop_value() ||
                     new_lob.is_full_temp_lob() ||
                     new_lob.is_persist_lob() ||
                     (new_lob.is_lob_disk_locator() && new_lob.has_inrow_data())) {
            ObLobCommon *lob_common = nullptr;
            ObLobAccessParam lob_param;
            if (OB_FAIL(new_lob.get_lob_data_byte_len(lob_param.update_len_))) {
              LOG_WARN("fail to get new lob byte len", K(ret), K(new_lob), K(i));
            } else if (OB_FAIL(delete_lob_col(run_ctx, run_ctx.col_descs_->at(i), old_obj, lob_common, lob_param))) {
              LOG_WARN("[STORAGE_LOB]failed to erase old lob col", K(ret), K(old_row), K(i));
            } else if (OB_FAIL(insert_lob_col(run_ctx, run_ctx.col_descs_->at(i), new_obj, &lob_param, lob_common))) {
              LOG_WARN("[STORAGE_LOB]failed to insert new lob col.", K(ret), K(new_row), K(i));
            }
          } else if (new_lob.is_delta_temp_lob()) {
            if (OB_FAIL(process_delta_lob(run_ctx, run_ctx.col_descs_->at(i), old_obj, new_lob, new_obj))) {
              LOG_WARN("failed to process delta lob.", K(ret), K(i));
            }
          } else {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected obj for new lob", K(ret), K(i), K(new_obj), K(new_lob));
          }
        } else {
          if (old_obj.is_null()) {
            new_obj.set_null();
          } else if (old_obj.is_nop_value()) {
            new_obj.set_nop_value();
          } else if (new_obj.is_nop_value() || new_obj.is_null()) {
            // do nothing
          } else {
            ObString val_str = old_obj.get_string();
            ObLobCommon *lob_common = reinterpret_cast<ObLobCommon*>(val_str.ptr());
            if (!lob_common->in_row_ && rowkey_change) {
              ObLobAccessParam lob_param;
              if (val_str.length() < ObLobConstants::LOB_WITH_OUTROW_CTX_SIZE) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("not enough space for lob header", K(ret), K(val_str), K(i));
              } else if (OB_FAIL(delete_lob_col(run_ctx, run_ctx.col_descs_->at(i), old_obj, lob_common, lob_param))) {
                LOG_WARN("[STORAGE_LOB]failed to erase old lob col", K(ret), K(old_row), K(i));
              } else if (OB_FAIL(insert_lob_col(run_ctx, run_ctx.col_descs_->at(i), new_obj, nullptr, nullptr))) { // no need del_param
                LOG_WARN("[STORAGE_LOB]failed to insert new lob col.", K(ret), K(new_row), K(i));
              }
            } else if (!lob_common->in_row_) {
              bool is_support_ext_info_log = false;
              if (OB_FAIL(ObLobTabletDmlHelper::is_support_ext_info_log(run_ctx, is_support_ext_info_log))) {
                LOG_WARN("failed to check is support ext info log", K(ret));
              } else if (is_support_ext_info_log) {
                if (OB_FAIL(ObLobTabletDmlHelper::set_lob_data_outrow_ctx_op(lob_common, ObLobDataOutRowCtx::OpType::VALID_OLD_VALUE_EXT_INFO_LOG))) {
                  LOG_WARN("[STORAGE_LOB]failed to set_lob_data_outrow_ctx_op.", K(ret), KP(lob_common), K(i));
                } else {
                  ObExtInfoLogHeader header;
                  header.init(old_obj, false);
                  if (OB_FAIL(register_ext_info_commit_cb(run_ctx, old_obj, old_obj, header))) {
                    LOG_WARN("register ext info commit cb fail", K(ret), K(old_row));
                  }
                }
              }
            } else {
              new_obj.set_lob_value(new_obj.get_type(), val_str.ptr(), val_str.length()); // remove has lob header flag
            }
          }
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_rows_to_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const bool rowkey_change,
    const ObIArray<int64_t> &update_idx,
    const bool delay_new,
    const bool lob_update,
    ObStoreRow *tmp_tbl_rows,
    ObRowsInfo &old_rows_info,
    ObRowsInfo &new_rows_info,
    ObRowStore &row_store)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  const ObColDescIArray &col_descs = *run_ctx.col_descs_;
  const int64_t row_count = new_rows_info.get_rowkey_cnt();
  ObStoreRow *old_tbl_rows = old_rows_info.rows_;
  ObStoreRow *new_tbl_rows = new_rows_info.rows_;
  if (OB_UNLIKELY(!old_rows_info.is_valid() || !new_rows_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rows info", K(ret), K(old_rows_info), K(new_rows_info));
  } else if (OB_FAIL(process_old_rows(tablet_handle,
                                      run_ctx,
                                      rowkey_change,
                                      lob_update,
                                      tmp_tbl_rows,
                                      old_rows_info))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process old row", K(ret), K(old_rows_info),
        K(col_descs), K(rowkey_change), K(lob_update), K(row_count));
    }
  } else if (OB_FAIL(process_lob_rows(tablet_handle,
                                      run_ctx,
                                      update_idx,
                                      rowkey_change,
                                      row_count,
                                      old_tbl_rows,
                                      new_tbl_rows))) {
    LOG_WARN("failed to process lob col change", K(ret));
  } else if (delay_new) {
    if (OB_FAIL(cache_rows_to_row_store(row_count,
                                        old_tbl_rows,
                                        new_tbl_rows,
                                        row_store))) {
      LOG_WARN("fail to cache_rows_to_row_store", K(ret), K(row_count));
    }
  } else if (OB_FAIL(process_new_rows(tablet_handle,
                                      run_ctx,
                                      update_idx,
                                      rowkey_change,
                                      old_rows_info,
                                      new_rows_info))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process new row", K(ret), K(old_rows_info), K(new_rows_info));
    }
  }
  return ret;
}

int ObLSTabletService::cache_rows_to_row_store(const int64_t row_count,
                                               ObStoreRow *old_tbl_rows,
                                               ObStoreRow *new_tbl_rows,
                                               ObRowStore &row_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(row_store.add_row(new_tbl_rows[i].row_val_))) {
      LOG_WARN("failed to store new row", K(i), K(new_tbl_rows[i]), K(ret));
    } else if (OB_FAIL(row_store.add_row(old_tbl_rows[i].row_val_))) {
      LOG_WARN("failed to store old row", K(i), K(old_tbl_rows[i]), K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::update_row_to_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const bool rowkey_change,
    const ObIArray<int64_t> &update_idx,
    const bool delay_new,
    const bool lob_update,
    ObStoreRow &old_tbl_row,
    ObStoreRow &new_tbl_row,
    ObRowStore &row_store)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  const ObColDescIArray &col_descs = *run_ctx.col_descs_;

  if (OB_UNLIKELY(col_descs.count() != old_tbl_row.row_val_.get_count()
      || col_descs.count() != new_tbl_row.row_val_.get_count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(col_descs.count()),
        K(old_tbl_row.row_val_), K(new_tbl_row.row_val_));
  } else if (OB_FAIL(process_old_row(tablet_handle,
                                     run_ctx,
                                     rowkey_change,
                                     lob_update,
                                     old_tbl_row))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process old row", K(ret),
          K(*run_ctx.col_descs_), K(old_tbl_row), K(rowkey_change));
    }
  } else if (OB_FAIL(process_lob_row(tablet_handle,
                                     run_ctx,
                                     update_idx,
                                     rowkey_change,
                                     old_tbl_row,
                                     new_tbl_row))) {
    LOG_WARN("failed to process lob col change", K(ret), K(old_tbl_row), K(new_tbl_row));
  } else if (delay_new) {
    if (OB_FAIL(cache_rows_to_row_store(1, &old_tbl_row, &new_tbl_row, row_store))) {
      LOG_WARN("fail to cache_rows_to_row_store", K(ret));
    }
  } else if (OB_FAIL(process_new_row(tablet_handle,
                                     run_ctx,
                                     update_idx,
                                     rowkey_change,
                                     old_tbl_row,
                                     new_tbl_row))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process new row", K(new_tbl_row), K(ret));
    }
  }

  return ret;
}

int ObLSTabletService::process_old_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const bool rowkey_change,
    const bool lob_update,
    ObStoreRow *tmp_tbl_rows,
    ObRowsInfo &old_rows_info)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  bool is_delete_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
  int64_t error_row_idx = 0;
  ObStoreRow *old_tbl_rows = old_rows_info.rows_;
  const int64_t row_count = old_rows_info.get_rowkey_cnt();

  if (OB_UNLIKELY(!relative_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relative tables", K(ret), K(relative_table));
  } else if (OB_UNLIKELY(!store_ctx.is_valid()
      || nullptr == run_ctx.col_descs_
      || run_ctx.col_descs_->count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(store_ctx), KP(run_ctx.col_descs_), K(is_delete_total_quantity_log));
  } else if (OB_FAIL(check_old_row_legitimacy(tablet_handle, run_ctx, row_count, old_tbl_rows, error_row_idx))) {
    if (OB_ERR_DEFENSIVE_CHECK == ret) {
      dump_diag_info_for_old_row_loss(relative_table, store_ctx, old_tbl_rows[error_row_idx]);
    }
    LOG_WARN("check old row legitimacy failed", K(error_row_idx), K(old_tbl_rows[error_row_idx].row_val_));
  } else if (OB_FAIL(process_old_rows_lob_col(tablet_handle, run_ctx, row_count, old_tbl_rows))){
    LOG_WARN("failed to process old row lob col", K(ret));
  } else {
    ObColDescIArray &col_descs = const_cast<ObColDescIArray&>(*run_ctx.col_descs_);
    const uint64_t &table_id = relative_table.get_table_id();
    if (OB_UNLIKELY(run_ctx.dml_param_.prelock_)) {
      bool locked = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        const ObDatumRowkey &datum_rowkey = old_rows_info.get_rowkey(i);
        if (OB_FAIL(check_row_locked_by_myself(tablet_handle, relative_table, store_ctx, datum_rowkey, locked))) {
          LOG_WARN("fail to check row locked", K(ret), K(old_tbl_rows[i]));
        } else if (!locked) {
          ret = OB_ERR_ROW_NOT_LOCKED;
          LOG_DEBUG("row has not been locked", K(ret), K(old_tbl_rows[i]));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (rowkey_change) {
      for (int64_t i = 0; i < row_count; i++) {
        old_tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_DELETE);
      }
      if (!is_delete_total_quantity_log) {
        if (OB_FAIL(tablet_handle.get_obj()->insert_rows(relative_table,
                                                         run_ctx.store_ctx_,
                                                         false /*check_exist*/,
                                                         col_descs,
                                                         run_ctx.dml_param_.encrypt_meta_,
                                                         old_rows_info))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to write data tablet row", K(ret), K(old_rows_info));
          }
        }
      } else if (OB_ISNULL(tmp_tbl_rows)) { // tmp_tbl_rows must be not null if rowkey changed and need total quantity log
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tmp tbl rows", K(ret));
      } else {
        for (int64_t i = 0; i < row_count; i++) {
          tmp_tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_UPDATE);
          tmp_tbl_rows[i].row_val_ = old_tbl_rows[i].row_val_;
        }
        ObSEArray<int64_t, 8> update_idx;
        if (OB_FAIL(tablet_handle.get_obj()->update_rows(relative_table,
                                                         run_ctx.store_ctx_,
                                                         col_descs,
                                                         update_idx,
                                                         tmp_tbl_rows,
                                                         run_ctx.dml_param_.encrypt_meta_,
                                                         old_rows_info))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to write data tablet row", K(ret), K(old_rows_info));
          }
        }
      }
    } else if (lob_update) {
      // need to lock main table rows that don't need to be deleted
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        const ObDatumRowkey &datum_rowkey = old_rows_info.get_rowkey(i);
        if (OB_FAIL(tablet_handle.get_obj()->lock_row(relative_table, store_ctx, datum_rowkey))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("lock row failed", K(ret), K(table_id), K(i), K(datum_rowkey));
          }
        }
      }
      LOG_DEBUG("generate lock node before update lob columns", K(ret), K(table_id));
    }
  }
  return ret;
}

int ObLSTabletService::process_old_row(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const bool rowkey_change,
    const bool lob_update,
    ObStoreRow &tbl_row)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  int64_t error_row_idx = 0;
  bool is_delete_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
  if (OB_UNLIKELY(!relative_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relative tables", K(ret), K(relative_table));
  } else if (OB_UNLIKELY(!store_ctx.is_valid()
      || nullptr == run_ctx.col_descs_
      || run_ctx.col_descs_->count() <= 0
      || !tbl_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(store_ctx), KP(run_ctx.col_descs_), K(tbl_row), K(is_delete_total_quantity_log));
  } else if (OB_FAIL(check_old_row_legitimacy(tablet_handle, run_ctx, 1, &tbl_row, error_row_idx))) {
    if (OB_ERR_DEFENSIVE_CHECK == ret) {
      dump_diag_info_for_old_row_loss(relative_table, store_ctx, tbl_row);
    }
    LOG_WARN("check old row legitimacy failed", K(tbl_row.row_val_));
  } else if (OB_FAIL(process_old_row_lob_col(tablet_handle, run_ctx, tbl_row))){
    LOG_WARN("failed to process old row lob col", K(ret), K(tbl_row));
  } else {
    ObColDescIArray &col_descs = const_cast<ObColDescIArray&>(*run_ctx.col_descs_);
    const uint64_t &table_id = relative_table.get_table_id();
    int64_t rowkey_size = relative_table.get_rowkey_column_num();
    ObStoreRowkey store_rowkey;
    ObDatumRowkey datum_rowkey;
    ObDatumRowkeyHelper rowkey_helper(run_ctx.allocator_);

    if (OB_UNLIKELY(run_ctx.dml_param_.prelock_)) {
      bool locked = false;
      if (OB_FAIL(store_rowkey.assign(tbl_row.row_val_.cells_, rowkey_size))) {
        LOG_WARN("Failed to assign rowkey", K(ret), K(tbl_row), K(rowkey_size));
      } else if (OB_FAIL(rowkey_helper.convert_datum_rowkey(store_rowkey.get_rowkey(), datum_rowkey))) {
        STORAGE_LOG(WARN, "Failed to transfer datum rowkey", K(ret), K(store_rowkey), K(rowkey_size), K(tbl_row));
      } else if (OB_FAIL(check_row_locked_by_myself(tablet_handle, relative_table, store_ctx, datum_rowkey, locked))) {
        LOG_WARN("fail to check row locked", K(ret), K(tbl_row));
      } else if (!locked) {
        ret = OB_ERR_ROW_NOT_LOCKED;
        LOG_DEBUG("row has not been locked", K(ret), K(tbl_row));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rowkey_change) {
      ObStoreRow del_row(tbl_row);
      del_row.flag_.set_flag(ObDmlFlag::DF_DELETE);
      if (!is_delete_total_quantity_log) {
        if (OB_FAIL(tablet_handle.get_obj()->insert_row(relative_table,
                                                        run_ctx.store_ctx_,
                                                        false /*check_exist*/,
                                                        col_descs,
                                                        del_row,
                                                        run_ctx.dml_param_.encrypt_meta_))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to write data tablet row", K(ret), K(del_row));
          }
        }
      } else if (OB_FAIL(delete_lob_tablet_rows(run_ctx, tablet_handle, tbl_row))){
        LOG_WARN("failed to delete lob rows.", K(ret), K(tbl_row));
      } else {
        ObStoreRow new_tbl_row;
        new_tbl_row.flag_.set_flag(ObDmlFlag::DF_DELETE);
        new_tbl_row.row_val_ = tbl_row.row_val_;
        del_row.flag_.set_flag(ObDmlFlag::DF_UPDATE);
        ObSEArray<int64_t, 8> update_idx;
        if (OB_FAIL(tablet_handle.get_obj()->update_row(relative_table,
                                                        run_ctx.store_ctx_,
                                                        col_descs,
                                                        update_idx,
                                                        del_row,
                                                        new_tbl_row,
                                                        run_ctx.dml_param_.encrypt_meta_))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to write data tablet row", K(ret), K(del_row), K(new_tbl_row));
          }
        }
      }
    } else if (lob_update) {
      // need to lock main table rows that don't need to be deleted
      if (OB_FAIL(store_rowkey.assign(tbl_row.row_val_.cells_, rowkey_size))) {
        LOG_WARN("Failed to assign rowkey", K(ret), K(tbl_row), K(rowkey_size));
      } else if (OB_FAIL(rowkey_helper.convert_datum_rowkey(store_rowkey.get_rowkey(), datum_rowkey))) {
        STORAGE_LOG(WARN, "Failed to transfer datum rowkey", K(ret), K(store_rowkey));
      } else if (OB_FAIL(tablet_handle.get_obj()->lock_row(relative_table, store_ctx, datum_rowkey))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("lock row failed", K(ret), K(table_id), K(tbl_row), K(rowkey_size));
        }
      }
      LOG_DEBUG("generate lock node before update lob columns", K(ret), K(table_id), K(tbl_row));
    }
  }
  return ret;
}
int ObLSTabletService::process_new_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const common::ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    ObRowsInfo &old_rows_info,
    ObRowsInfo &new_rows_info)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = new_rows_info.get_rowkey_cnt();

  if (OB_UNLIKELY(update_idx.count() < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(update_idx));
  } else if (GCONF.enable_defensive_check()
      && OB_FAIL(check_new_row_legitimacy(run_ctx, row_count, new_rows_info.rows_))) {
    LOG_WARN("check new row legitimacy failed", K(ret), K(new_rows_info));
  } else {
    const ObColDescIArray &col_descs = *run_ctx.col_descs_;
    ObRelativeTable &relative_table = run_ctx.relative_table_;
    bool is_update_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
    const common::ObTimeZoneInfo *tz_info = run_ctx.dml_param_.tz_info_;
    const int64_t row_count = new_rows_info.get_rowkey_cnt();

    if (is_update_total_quantity_log && !rowkey_change) {
      if (OB_FAIL(tablet_handle.get_obj()->update_rows(relative_table,
                                                       run_ctx.store_ctx_,
                                                       col_descs,
                                                       update_idx,
                                                       old_rows_info.rows_,
                                                       run_ctx.dml_param_.encrypt_meta_,
                                                       new_rows_info))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to update to row", K(ret), K(new_rows_info));
        }
      }
    } else {
      if (rowkey_change) {
        for (int64_t i = 0; i < row_count; i++) {
          new_rows_info.rows_[i].flag_.set_flag(ObDmlFlag::DF_INSERT);
        }
      }
      const bool check_exist = rowkey_change &&
        (!relative_table.is_storage_index_table() ||
         relative_table.is_unique_index() ||
         run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_update_pk_dop() ||
         run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_immediate_row_check());
      if (OB_FAIL(tablet_handle.get_obj()->insert_rows(relative_table,
                                                       run_ctx.store_ctx_,
                                                       check_exist,
                                                       col_descs,
                                                       run_ctx.dml_param_.encrypt_meta_,
                                                       new_rows_info))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          int tmp_ret = OB_SUCCESS;
          char rowkey_buffer[OB_TMP_BUF_SIZE_256];
          ObString index_name = "PRIMARY";
          if (OB_TMP_FAIL(extract_rowkey(relative_table, new_rows_info.get_conflict_rowkey(),
               rowkey_buffer, OB_TMP_BUF_SIZE_256, run_ctx.dml_param_.tz_info_))) {
            LOG_WARN("Failed to extract rowkey", K(ret), K(tmp_ret));
          }
          if (relative_table.is_index_table()) {
            if (OB_TMP_FAIL(relative_table.get_index_name(index_name))) {
              LOG_WARN("Failed to get index name", K(ret), K(tmp_ret));
            }
          } else if (lib::is_oracle_mode() && OB_TMP_FAIL(relative_table.get_primary_key_name(index_name))) {
            LOG_WARN("Failed to get pk name", K(ret), K(tmp_ret));
          }
          LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, rowkey_buffer, index_name.length(), index_name.ptr());
        } else if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to update to row", K(ret), K(new_rows_info));
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::process_new_row(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const common::ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    const ObStoreRow &old_tbl_row,
    const ObStoreRow &new_tbl_row)

{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(update_idx.count() < 0 || !new_tbl_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(update_idx), K(new_tbl_row), K(rowkey_change));
  } else if (GCONF.enable_defensive_check()
      && OB_FAIL(check_new_row_legitimacy(run_ctx, 1, &new_tbl_row))) {
    LOG_WARN("check new row legitimacy failed", K(ret), K(new_tbl_row));
  } else {
    const ObColDescIArray &col_descs = *run_ctx.col_descs_;
    ObRelativeTable &relative_table = run_ctx.relative_table_;
    bool is_update_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
    const common::ObTimeZoneInfo *tz_info = run_ctx.dml_param_.tz_info_;

    ObStoreRow new_row;
    new_row.flag_.set_flag(rowkey_change ? ObDmlFlag::DF_INSERT : ObDmlFlag::DF_UPDATE);
    new_row.row_val_ = new_tbl_row.row_val_;

    if (is_update_total_quantity_log && !rowkey_change) {
      ObStoreRow old_row;
      old_row.flag_.set_flag(ObDmlFlag::DF_UPDATE);
      old_row.row_val_ = old_tbl_row.row_val_;
      if (OB_FAIL(tablet_handle.get_obj()->update_row(relative_table,
                                                      run_ctx.store_ctx_,
                                                      col_descs,
                                                      update_idx,
                                                      old_row,
                                                      new_row,
                                                      run_ctx.dml_param_.encrypt_meta_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to update to row", K(ret), K(old_row), K(new_row));
        }
      }
    } else {

      const bool check_exist = rowkey_change && (!relative_table.is_storage_index_table() ||
        relative_table.is_unique_index() ||
        run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_update_pk_dop() ||
        run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_immediate_row_check());
      if (OB_FAIL(tablet_handle.get_obj()->insert_row(relative_table,
                                                      run_ctx.store_ctx_,
                                                      check_exist,
                                                      col_descs,
                                                      new_row,
                                                      run_ctx.dml_param_.encrypt_meta_))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          char buffer[OB_TMP_BUF_SIZE_256];
          ObStoreRowkey rowkey;
          if (OB_SUCCESS != rowkey.assign(new_tbl_row.row_val_.cells_, relative_table.get_rowkey_column_num())) {
            LOG_WARN("Failed to assign rowkey", K(new_tbl_row));
          } else if (OB_SUCCESS != extract_rowkey(relative_table, rowkey, buffer, OB_TMP_BUF_SIZE_256, tz_info)) {
            LOG_WARN("extract rowkey failed", K(rowkey));
          } else {
            ObString index_name = "PRIMARY";
            if (relative_table.is_index_table()) {
              relative_table.get_index_name(index_name);
            }
            LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, buffer, index_name.length(), index_name.ptr());
          }
          LOG_WARN("rowkey already exists", K(ret), K(new_tbl_row));
        } else if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to update to row", K(ret), K(new_row));
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_new_row_nullable_value(
    const ObIArray<uint64_t> &column_ids,
    ObRelativeTable &data_table,
    const ObNewRow &new_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(column_ids.count() != new_row.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row is invalid", K(ret), K(new_row.get_count()), K(column_ids.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < column_ids.count(); ++i) {
    uint64_t column_id = column_ids.at(i);
    bool is_nullable = false;
    if (OB_UNLIKELY(is_shadow_column(column_id))) {
      //the shadow pk is generated internally,
      //and the nullable attribute check for it is skipped
    } else if (OB_FAIL(data_table.is_column_nullable_for_write(column_id, is_nullable))) {
      LOG_WARN("check is_column_nullable_for_write failed", K(ret), K(column_id));
    } else if (new_row.get_cell(i).is_null() && !is_nullable) {
      bool is_hidden = false;
      bool is_gen_col = false;
      bool is_nullable_for_read = false;
      if (OB_FAIL(data_table.is_column_nullable_for_read(column_id, is_nullable_for_read))) {
        LOG_WARN("check is nullable for read failed", K(ret));
      } else if (is_nullable_for_read) {
        LOG_TRACE("Catch a defensive nullable error, but this column is not null novalidate",
                  K(column_id), K(column_ids), K(new_row), K(data_table));
      } else if (OB_FAIL(data_table.is_hidden_column(column_id, is_hidden))) {
        LOG_WARN("get is hidden column failed", K(ret), K(column_id));
      } else if (OB_FAIL(data_table.is_gen_column(column_id, is_gen_col))) {
        LOG_WARN("get is gen column failed", K(ret), K(column_id));
      } else if (is_hidden && !is_gen_col) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("Catch a defensive nullable error, "
                 "maybe cause by add column not null default null ONLINE", K(ret),
                 K(column_id), K(column_ids), K(new_row), K(data_table));
      } else {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_new_row_nullable_value");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      K(column_id), K(column_ids), K(new_row), K(data_table));
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
      }
    } else if (new_row.get_cell(i).is_number()) {
      number::ObNumber num;
      if (OB_FAIL(new_row.get_cell(i).get_number(num))) {
        LOG_WARN("get number value from object fail", K(ret), K(new_row.get_cell(i)));
      } else if (OB_FAIL(num.sanity_check())) {
        LOG_WARN("sanity check number failed", K(ret), K(new_row.get_cell(i)));
      }
      if (OB_SUCCESS != ret) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_new_row_nullable_value");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      K(column_id), K(column_ids), K(new_row), K(data_table));
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_new_row_nullable_value(const ObIArray<ObColDesc> &col_descs,
                                                    ObRelativeTable &relative_table,
                                                    const ObNewRow &new_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_descs.count() > new_row.get_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row is invalid", K(ret), K(new_row.get_count()), K(col_descs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    uint64_t column_id = col_descs.at(i).col_id_;
    bool is_nullable = false;
    if (OB_UNLIKELY(is_shadow_column(column_id))) {
      //the shadow pk is generated internally,
      //and the nullable attribute check for it is skipped
    } else if (OB_FAIL(relative_table.is_column_nullable_for_write(column_id, is_nullable))) {
      LOG_WARN("check is_column_nullable_for_write failed", K(ret), K(column_id));
    } else if (new_row.get_cell(i).is_null() && !is_nullable) {
      bool is_hidden = false;
      bool is_gen_col = false;
      bool is_nullable_for_read = false;
      if (OB_FAIL(relative_table.is_column_nullable_for_read(column_id, is_nullable_for_read))) {
        LOG_WARN("check is nullable for read failed", K(ret));
      } else if (is_nullable_for_read) {
        //this column is not null novalidate, maybe the null column come from the old data
        //so output trace log and ignore it
        LOG_TRACE("Catch a defensive nullable error, but this column is not null novalidate",
                  K(column_id), K(col_descs), K(new_row), K(relative_table));
      } else if (OB_FAIL(relative_table.is_hidden_column(column_id, is_hidden))) {
        LOG_WARN("get is hidden column failed", K(ret), K(column_id));
      } else if (OB_FAIL(relative_table.is_gen_column(column_id, is_gen_col))) {
        LOG_WARN("get is gen column failed", K(ret), K(column_id));
      } else if (is_hidden && !is_gen_col) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("Catch a defensive nullable error, "
                 "maybe cause by add column not null default null ONLINE", K(ret),
                 K(column_id), K(col_descs), K(new_row), K(relative_table));
      } else {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_new_row_nullable_value");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      K(column_id), K(col_descs), K(new_row), K(relative_table));
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
      }
    } else if (new_row.get_cell(i).is_number()) {
      number::ObNumber num;
      if (OB_FAIL(new_row.get_cell(i).get_number(num))) {
        LOG_WARN("get number value from object fail", K(ret), K(new_row.get_cell(i)));
      } else if (OB_FAIL(num.sanity_check())) {
        LOG_WARN("sanity check number failed", K(ret), K(new_row.get_cell(i)));
      }
      if (OB_SUCCESS != ret) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_new_row_nullable_value");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      K(column_id), K(col_descs), K(new_row), K(relative_table));
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_new_row_shadow_pk(
    const ObIArray<uint64_t> &column_ids,
    ObRelativeTable &data_table,
    const ObNewRow &new_row)
{
  int ret = OB_SUCCESS;
  if (data_table.get_shadow_rowkey_column_num() > 0) {
    //check shadow pk
    int64_t rowkey_cnt = data_table.get_rowkey_column_num();
    int64_t spk_cnt = data_table.get_shadow_rowkey_column_num();
    int64_t index_col_cnt = rowkey_cnt - spk_cnt;
    bool need_spk = false;
    if (OB_UNLIKELY(index_col_cnt <= 0) || OB_UNLIKELY(column_ids.count() < rowkey_cnt)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("index column count is invalid", K(ret),
               K(index_col_cnt), K(rowkey_cnt), K(spk_cnt), K(column_ids.count()));
    } else if (lib::is_mysql_mode()) {
      // mysql兼容：只要unique index key中有null列，则需要填充shadow列
      bool rowkey_has_null = false;
      for (int64_t i = 0; !rowkey_has_null && i < index_col_cnt; i++) {
        rowkey_has_null = new_row.get_cell(i).is_null();
      }
      need_spk = rowkey_has_null;
    } else {
      // oracle兼容：只有unique index key全为null列时，才需要填充shadow列
      bool is_rowkey_all_null = true;
      for (int64_t i = 0; is_rowkey_all_null && i < index_col_cnt; i++) {
        is_rowkey_all_null = new_row.get_cell(i).is_null();
      }
      need_spk = is_rowkey_all_null;
    }
    for (int64_t i = index_col_cnt; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      uint64_t spk_column_id = column_ids.at(i);
      uint64_t real_pk_id = spk_column_id - OB_MIN_SHADOW_COLUMN_ID;
      const ObObj &spk_value = new_row.get_cell(i);
      int64_t pk_idx = OB_INVALID_INDEX;
      int cmp = 0;
      if (OB_LIKELY(!need_spk)) {
        if (!spk_value.is_null()) {
          ret = OB_ERR_DEFENSIVE_CHECK;
          ObString func_name = ObString::make_string("check_new_row_shadow_pk");
          LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
          LOG_ERROR("Fatal Error!!! Catch a defensive error!", K(ret),
                    "column_id", column_ids, K(new_row), K(data_table),
                    K(spk_value), K(i), K(spk_column_id), K(real_pk_id));
        }
      } else if (OB_UNLIKELY(!has_exist_in_array(column_ids, real_pk_id, &pk_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("real pk column not exists in column_ids", K(ret), K(column_ids), K(real_pk_id));
      } else if (OB_FAIL(new_row.get_cell(pk_idx).compare(spk_value, cmp)) || 0 != cmp) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_new_row_shadow_pk");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      "column_id", column_ids, K(new_row), K(data_table),
                      K(spk_value), "pk_value", new_row.get_cell(pk_idx),
                      K(pk_idx), K(i), K(spk_column_id), K(real_pk_id));
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_row_locked_by_myself(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDatumRowkey &rowkey,
    bool &locked)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();

  if (OB_UNLIKELY(nullptr == tablet
      || !relative_table.is_valid()
      || !store_ctx.is_valid()
      || !rowkey.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_handle),
        K(relative_table), K(store_ctx), K(rowkey));
  } else {
    ObStorageTableGuard guard(tablet, store_ctx, true);
    if (OB_FAIL(guard.refresh_and_protect_memtable_for_write(relative_table))) {
      LOG_WARN("fail to protect table", K(ret), K(tablet_handle));
    } else if (OB_FAIL(tablet->check_row_locked_by_myself(relative_table, store_ctx, rowkey, locked))) {
      LOG_WARN("fail to check row locked, ", K(ret), K(rowkey));
    }
  }

  return ret;
}
int ObLSTabletService::get_conflict_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const common::ObIArray<uint64_t> &out_col_ids,
    const ObRowsInfo &rows_info,
    common::ObNewRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &data_table = run_ctx.relative_table_;
  ObArenaAllocator scan_allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_TABLE_SCAN_ITER));
  ObIAllocator *allocator = &scan_allocator;
  ObTablet *data_tablet = tablet_handle.get_obj();
  if (OB_UNLIKELY(!rows_info.has_set_error() || !rows_info.have_conflict())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no confict rows in rows_info", K(ret), K(rows_info));
  } else if (OB_ISNULL(data_tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else {
    ObSingleRowGetter single_row_getter(*allocator, *data_tablet);
    if (OB_FAIL(init_single_row_getter(single_row_getter, run_ctx, out_col_ids, data_table, true))) {
      LOG_WARN("failed to init single row getter", K(ret));
    }
    const int64_t row_count = rows_info.get_rowkey_cnt();
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      const ObDatumRowkey &datum_rowkey = rows_info.get_rowkey(i);
      if (!rows_info.is_row_duplicate(i)) {
        LOG_DEBUG("not conflict row", K(row_count), K(i), K(datum_rowkey));
      } else {
        LOG_DEBUG("get conflict row", K(row_count), K(i), K(datum_rowkey));
        if (OB_FAIL(single_get_row(single_row_getter, datum_rowkey, duplicated_rows))) {
          LOG_WARN("failed to single get row", K(ret), K(datum_rowkey));
        }
      }
    }
  }
  if (OB_FAIL(ret)) {
    if (nullptr != duplicated_rows) {
      ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
      duplicated_rows = nullptr;
    }
  }

  return ret;
}

int ObLSTabletService::get_conflict_row(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const common::ObIArray<uint64_t> &out_col_ids,
    const common::ObNewRow &row,
    common::ObNewRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &data_table = run_ctx.relative_table_;
  ObArenaAllocator scan_allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_TABLE_SCAN_ITER));
  ObIAllocator *allocator = &scan_allocator;
  ObTablet *data_tablet = tablet_handle.get_obj();
  ObDatumRowkeyHelper rowkey_helper(scan_allocator);
  ObDatumRowkey datum_rowkey;
  ObStoreRowkey rowkey;
  rowkey.assign(row.cells_, data_table.get_rowkey_column_num());

  if (OB_ISNULL(data_tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else {
    ObSingleRowGetter single_row_getter(*allocator, *data_tablet);
    if (OB_FAIL(init_single_row_getter(single_row_getter, run_ctx, out_col_ids, data_table, true))) {
      LOG_WARN("failed to init single row getter", K(ret));
    } else if (OB_FAIL(rowkey_helper.convert_datum_rowkey(rowkey.get_rowkey(), datum_rowkey))) {
      STORAGE_LOG(WARN, "Failed to transfer datum rowkey", K(ret), K(rowkey));
    } else if (OB_FAIL(single_get_row(single_row_getter, datum_rowkey, duplicated_rows))) {
      LOG_WARN("failed to single get row", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
    if (nullptr != duplicated_rows) {
      ObQueryIteratorFactory::free_insert_dup_iter(duplicated_rows);
      duplicated_rows = nullptr;
    }
  }

  return ret;
}

int ObLSTabletService::init_single_row_getter(
    ObSingleRowGetter &row_getter,
    ObDMLRunningCtx &run_ctx,
    const ObIArray<uint64_t> &out_col_ids,
    ObRelativeTable &relative_table,
    bool skip_read_lob)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(row_getter.init_dml_access_ctx(run_ctx.store_ctx_, run_ctx.dml_param_, skip_read_lob))) {
    LOG_WARN("init dml access ctx failed", K(ret));
  } else if (OB_FAIL(row_getter.init_dml_access_param(relative_table,
      run_ctx.dml_param_, out_col_ids, skip_read_lob))) {
    LOG_WARN("init dml access param failed", K(ret));
  }

  return ret;
}

int ObLSTabletService::single_get_row(
    ObSingleRowGetter &row_getter,
    const ObDatumRowkey &rowkey,
    ObNewRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObNewRow *row = nullptr;

  if (OB_FAIL(row_getter.open(rowkey))) {
    LOG_WARN("init single row getter failed", K(ret));
  } else if (OB_FAIL(row_getter.get_next_row(row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("get next row from single row getter failed", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  } else if (NULL == duplicated_rows) {
    ObValueRowIterator *dup_iter = NULL;
    if (OB_ISNULL(dup_iter = ObQueryIteratorFactory::get_insert_dup_iter())) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("no memory to alloc ObValueRowIterator", K(ret));
    } else {
      duplicated_rows = dup_iter;
      if (OB_FAIL(dup_iter->init(false))) {
        LOG_WARN("failed to initialize ObValueRowIterator", K(ret));
      }
    }
  }
  if (OB_SUCC(ret) && row != nullptr) {
    ObValueRowIterator *dup_iter = static_cast<ObValueRowIterator*>(duplicated_rows);
    if (OB_FAIL(dup_iter->add_row(*row))) {
      LOG_WARN("failed to store conflict row", K(ret), K(*row));
    } else {
      LOG_DEBUG("get conflict row", KPC(row));
    }
  }

  return ret;
}

int ObLSTabletService::insert_row_to_tablet(
  const bool check_exist,
  ObTabletHandle &tablet_handle,
  ObDMLRunningCtx &run_ctx,
  ObStoreRow &tbl_row)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  const ObColDescIArray &idx_col_descs = run_ctx.idx_col_descs_;

  if (OB_UNLIKELY(!store_ctx.is_valid()
      || !relative_table.is_valid()
      || nullptr == run_ctx.col_descs_
      || run_ctx.col_descs_->count() <= 0
      || !tbl_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(store_ctx), KP(run_ctx.col_descs_), K(tbl_row), K(ret));
  } else if (GCONF.enable_defensive_check()
      && OB_FAIL(check_new_row_legitimacy(run_ctx, 1, &tbl_row))) {
    LOG_WARN("check new row legitimacy failed", K(ret), K(tbl_row.row_val_));
  } else if (OB_FAIL(insert_lob_tablet_row(tablet_handle, run_ctx, tbl_row))) {
    LOG_WARN("failed to write lob tablets rows", K(ret));
  } else {
    const ObColDescIArray &col_descs = *run_ctx.col_descs_;
    if (OB_FAIL(tablet_handle.get_obj()->insert_row(relative_table,
                                                    store_ctx,
                                                    check_exist,
                                                    col_descs,
                                                    tbl_row,
                                                    run_ctx.dml_param_.encrypt_meta_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to insert tablet row", K(ret),
            "table id", relative_table.get_table_id(), K(col_descs), K(tbl_row));
      }
    }
  }

  return ret;
}

int ObLSTabletService::process_old_rows_lob_col(
    ObTabletHandle &data_tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const int64_t row_count,
    ObStoreRow *old_tbl_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(process_old_row_lob_col(data_tablet_handle, run_ctx, old_tbl_rows[i]))) {
      LOG_WARN("fail to process_old_row_lob_col", K(ret), K(i));
    }
  }
  return ret;
}

int ObLSTabletService::process_old_row_lob_col(
    ObTabletHandle &data_tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObStoreRow &tbl_row)
{
  int ret = OB_SUCCESS;
  bool has_lob_col = false;
  bool need_reread = is_sys_table(run_ctx.relative_table_.get_table_id());
  int64_t col_cnt = run_ctx.col_descs_->count();
  if (tbl_row.row_val_.count_ != col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]Invliad row col cnt", K(ret), K(col_cnt), K(tbl_row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObColDesc &column = run_ctx.col_descs_->at(i);
      if (is_lob_storage(column.col_type_.get_type())) {
        has_lob_col = true;
        need_reread = need_reread || !(tbl_row.row_val_.cells_[i].has_lob_header());
        break;
      }
    }
  }
  if (OB_SUCC(ret) && has_lob_col) {
    if (!need_reread) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        const ObColDesc &column = run_ctx.col_descs_->at(i);
        if (is_lob_storage(column.col_type_.get_type())) {
          ObObj &obj = tbl_row.row_val_.cells_[i];
          bool has_lob_header = obj.has_lob_header();
          if (obj.is_null() || obj.is_nop_value()) {
            // do nothing
          } else if (!has_lob_header) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("lob should have lob locator here.", K(ret), K(i), K(tbl_row.row_val_.cells_[i]));
          } else {
            ObLobLocatorV2 lob(obj.get_string(), has_lob_header);
            ObString disk_loc;
            if (!lob.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid lob locator.", K(ret), K(lob));
            } else if (lob.is_simple()) {
              // do nothing
            } else if (OB_FAIL(lob.get_disk_locator(disk_loc))) {
              LOG_WARN("failed to get disk lob locator.", K(ret), K(lob));
            } else {
              obj.set_lob_value(obj.get_type(), disk_loc.ptr(), disk_loc.length());
              if (has_lob_header) {
                obj.set_has_lob_header();
              }
            }
          }
        }
      }
    } else {
      if (OB_FAIL(table_refresh_row(data_tablet_handle, run_ctx, tbl_row.row_val_))) {
        LOG_WARN("[STORAGE_LOB]re-read lob col failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLSTabletService::table_refresh_row(
    ObTabletHandle &data_tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObNewRow &row)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator scan_allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_LOB_ACCESS_BUFFER));
  ObTablet *data_tablet = data_tablet_handle.get_obj();
  ObRelativeTable &data_table = run_ctx.relative_table_;
  ObStoreRowkey rowkey;
  if (OB_FAIL(rowkey.assign(row.cells_, data_table.get_rowkey_column_num()))) {
    LOG_WARN("get rowkey col num failed", K(ret));
  }

  int64_t col_cnt = run_ctx.col_descs_->count();
  ObSEArray<uint64_t, 8> out_col_ids;
  for (int i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
    if (OB_FAIL(out_col_ids.push_back(run_ctx.col_descs_->at(i).col_id_))) {
      LOG_WARN("push col id failed.", K(ret), K(i));
    }
  }
  ObDatumRowkey datum_rowkey;
  ObDatumRowkeyHelper rowkey_helper(scan_allocator);
  ObSingleRowGetter single_row_getter(scan_allocator, *data_tablet);
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(init_single_row_getter(single_row_getter,
      run_ctx, out_col_ids, data_table, true))) {
    LOG_WARN("failed to init single row getter", K(ret));
  } else if (OB_FAIL(rowkey_helper.convert_datum_rowkey(rowkey.get_rowkey(), datum_rowkey))) {
    LOG_WARN("Failed to transfer datum rowkey", K(ret), K(rowkey));
  } else {
    ObNewRow *new_row = nullptr;
    if (OB_FAIL(single_row_getter.open(datum_rowkey))) {
      LOG_WARN("init single row getter failed", K(ret));
    } else if (OB_FAIL(single_row_getter.get_next_row(new_row))) {
      if (ret == OB_ITER_END) {
        LOG_DEBUG("re-read old row not exist", K(ret), K(row));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row from single row getter failed", K(ret));
      }
    } else if (OB_ISNULL(new_row)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get next row from single row null", K(ret));
    } else if (new_row->get_count() != row.get_count()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get row from single row col count not equal.", K(ret), K(row.get_count()), K(new_row->get_count()));
    } else {
      LOG_DEBUG("get new row success.", K(row), KPC(new_row));
      // only write cells, not write row
      // passing fixed double scale from row to new_row
      for (int64_t i = 0; OB_SUCC(ret) && i < new_row->get_count(); ++i) {
        if (row.cells_[i].is_fixed_double()) {
          new_row->cells_[i].set_scale(row.cells_[i].get_scale());
        } else if (OB_FAIL(ob_write_obj(run_ctx.dml_param_.lob_allocator_, new_row->cells_[i], row.cells_[i]))) {
          LOG_WARN("copy ObObj error", K(ret), K(i), K(new_row->cells_[i]));
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::delete_rows_in_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObStoreRow *tmp_tbl_rows,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  ObStoreCtx &ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  const int64_t row_count = rows_info.get_rowkey_cnt();
  ObStoreRow *tbl_rows = rows_info.rows_;
  int64_t error_row_idx = 0;

  if (OB_FAIL(check_old_row_legitimacy(tablet_handle, run_ctx, row_count, tbl_rows, error_row_idx))) {
    if (OB_ERR_DEFENSIVE_CHECK == ret) {
      dump_diag_info_for_old_row_loss(relative_table, ctx, tbl_rows[error_row_idx]);
    }
    LOG_WARN("check old row legitimacy failed", K(rows_info));
  } else if (OB_FAIL(process_old_rows_lob_col(tablet_handle, run_ctx, row_count, tbl_rows))){
    LOG_WARN("failed to process old rows lob col", K(ret));
    LOG_WARN("failed to process old row lob col", K(ret), K(rows_info));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_FAIL(delete_lob_tablet_rows(run_ctx, tablet_handle, tbl_rows[i]))) {
        LOG_WARN("failed to delete lob rows.", K(ret), K(i), K(tbl_rows[i]));
      }
    }
  }

  if (OB_FAIL(ret)) {
  } else if (!dml_param.is_total_quantity_log_) {
    if (OB_FAIL(tablet_handle.get_obj()->insert_rows(relative_table,
                                                     ctx,
                                                     false /*check_exist*/,
                                                     *run_ctx.col_descs_,
                                                     dml_param.encrypt_meta_,
                                                     rows_info))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to set row", K(ret), K(*run_ctx.col_descs_), K(rows_info));
      }
    }
  } else if (OB_ISNULL(tmp_tbl_rows)) { // tmp_tbl_rows must be not null if need write total quantity log
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tmp tbl rows", K(ret));
  } else {
    for (int64_t i = 0; i < row_count; i++) {
      tmp_tbl_rows[i].flag_.set_flag(ObDmlFlag::DF_UPDATE);
      tmp_tbl_rows[i].row_val_ = tbl_rows[i].row_val_;
    }
    ObSEArray<int64_t, 8> update_idx; // update_idx is a dummy param here
    if (OB_FAIL(tablet_handle.get_obj()->update_rows(relative_table,
                                                     ctx,
                                                     *run_ctx.col_descs_,
                                                     update_idx,
                                                     tmp_tbl_rows,
                                                     dml_param.encrypt_meta_,
                                                     rows_info))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to set row", K(ret), K(*run_ctx.col_descs_), K(rows_info));
      }
    }
  }
  return ret;
}

int ObLSTabletService::delete_row_in_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObStoreRow &tbl_row)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  ObStoreCtx &ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  ObSEArray<int64_t, 8> update_idx; // update_idx is a dummy param here
  int64_t error_row_idx = 0;

  if (OB_FAIL(check_old_row_legitimacy(tablet_handle, run_ctx, 1, &tbl_row, error_row_idx))) {
    if (OB_ERR_DEFENSIVE_CHECK == ret) {
      dump_diag_info_for_old_row_loss(relative_table, ctx, tbl_row);
    }
    LOG_WARN("check old row legitimacy failed", K(tbl_row));
  } else if (OB_FAIL(process_old_row_lob_col(tablet_handle, run_ctx, tbl_row))) {
    LOG_WARN("failed to process old row lob col", K(ret), K(tbl_row));
  } else if (OB_FAIL(delete_lob_tablet_rows(run_ctx, tablet_handle, tbl_row))) {
    LOG_WARN("failed to delete lob rows.", K(ret), K(tbl_row));
  } else if (!dml_param.is_total_quantity_log_) {
    if (OB_FAIL(tablet_handle.get_obj()->insert_row(relative_table,
                                                    ctx,
                                                    false /*check_exist*/,
                                                    *run_ctx.col_descs_,
                                                    tbl_row,
                                                    dml_param.encrypt_meta_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to set row", K(ret), K(*run_ctx.col_descs_), K(tbl_row));
      }
    }
  } else {
    ObStoreRow old_tbl_row;
    update_idx.reset(); // update_idx is a dummy param here
    old_tbl_row.flag_.set_flag(ObDmlFlag::DF_UPDATE);
    old_tbl_row.row_val_ = tbl_row.row_val_;
    if (OB_FAIL(tablet_handle.get_obj()->update_row(relative_table,
                                                    ctx,
                                                    *run_ctx.col_descs_,
                                                    update_idx,
                                                    old_tbl_row,
                                                    tbl_row,
                                                    dml_param.encrypt_meta_))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to set row", K(ret), K(*run_ctx.col_descs_), K(tbl_row), K(old_tbl_row));
      }
    } else {
      LOG_DEBUG("succeeded to del main table row", K(tbl_row), K(old_tbl_row));
    }
  }

  return ret;
}

int ObLSTabletService::delete_lob_col(
    ObDMLRunningCtx &run_ctx,
    const ObColDesc &column,
    ObObj &obj,
    ObLobCommon *&lob_common,
    ObLobAccessParam &lob_param)
{
  int ret = OB_SUCCESS;
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]get lob manager instance failed.", K(ret));
  } else if (!column.col_type_.is_lob_storage() || obj.is_nop_value() || obj.is_null()) {
    // do nothing
  } else {
    ObString data = obj.get_string();
    ObLobCommon* old_lob_common = reinterpret_cast<ObLobCommon*>(data.ptr());
    // Notice: Only disk locator here!
    ObLobLocatorV2 locator(data, obj.has_lob_header());
    if (data.length() < sizeof(ObLobCommon)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("[STORAGE_LOB]Invalid Lob data.", K(ret), K(obj), K(data));
    } else if (locator.is_inrow()) {
      // delete inrow lob no need to use the lob manager
    } else if (OB_FAIL(set_lob_storage_params(run_ctx, column, lob_param))) {
      LOG_WARN("set_lob_storage_params fail", K(ret), K(column));
    } else {
      ObLobCommon* old_lob_common = reinterpret_cast<ObLobCommon*>(data.ptr());
      ObLobData* old_lob_data = reinterpret_cast<ObLobData*>(old_lob_common->buffer_);
      ObLobDataOutRowCtx *old_lob_outrow_ctx = reinterpret_cast<ObLobDataOutRowCtx*>(old_lob_data->buffer_);
      if (old_lob_outrow_ctx->is_valid_old_value()) {
      } else {
        void *buf = run_ctx.dml_param_.lob_allocator_.alloc(data.length());
        if (OB_ISNULL(buf)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to deep copy lob data.", K(ret), K(data));
        } else {
          MEMCPY(buf, data.ptr(), data.length());
          lob_common = reinterpret_cast<ObLobCommon*>(buf);
          lob_param.tx_desc_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_desc_;
          lob_param.parent_seq_no_ = run_ctx.store_ctx_.mvcc_acc_ctx_.tx_scn_;
          lob_param.snapshot_ = run_ctx.dml_param_.snapshot_;
          lob_param.tx_id_ = lob_param.tx_desc_->get_tx_id();
          lob_param.sql_mode_ = run_ctx.dml_param_.sql_mode_;
          lob_param.is_total_quantity_log_ = run_ctx.dml_param_.is_total_quantity_log_;
          lob_param.ls_id_ = run_ctx.store_ctx_.ls_id_;
          lob_param.tablet_id_ = run_ctx.relative_table_.get_tablet_id();
          lob_param.coll_type_ = ObLobCharsetUtil::get_collation_type(column.col_type_.get_type(), column.col_type_.get_collation_type());
          lob_param.allocator_ = &run_ctx.dml_param_.lob_allocator_;
          lob_param.lob_common_ = lob_common;
          lob_param.handle_size_ = data.length();
          lob_param.byte_size_ = lob_param.lob_common_->get_byte_size(data.length());
          lob_param.timeout_ = run_ctx.dml_param_.timeout_;
          lob_param.scan_backward_ = false;
          lob_param.offset_ = 0;
          // delete may not have latest tx read snapshot, so need read_latest lob aux table
          lob_param.need_read_latest_ = true;
          // use byte size to delete all
          lob_param.len_ = lob_param.byte_size_; //ObCharset::strlen_char(lob_param.coll_type_, sql_data.ptr(), sql_data.length());
          if (lob_param.byte_size_ < 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("calc byte size is negative.", K(ret), K(data), K(lob_param));
          } else if (OB_FAIL(lob_mngr->erase(lob_param))) {
            LOG_WARN("[STORAGE_LOB]lob erase failed.", K(ret), K(lob_param));
          } else if (OB_FAIL(ObLobTabletDmlHelper::handle_valid_old_outrow_lob_value(run_ctx.dml_param_.is_total_quantity_log_,
                                                                                     old_lob_common,
                                                                                     lob_common))) {
            LOG_WARN("handle_valid_old_outrow_lob_value fail", K(ret), K(lob_param));
          }
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::delete_lob_tablet_rows(
    ObDMLRunningCtx &run_ctx,
    ObTabletHandle &data_tablet,
    ObStoreRow &tbl_row)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = run_ctx.col_descs_->count();
  if (tbl_row.row_val_.count_ != col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]Invliad row col cnt", K(col_cnt), K(tbl_row));
  } else if (OB_FAIL(update_lob_meta_table_seq_no(run_ctx, 1/*row_count*/))) {
    LOG_WARN("update_lob_meta_table_seq_no fail", K(ret), K(run_ctx.dml_param_));
  } else {
    ObLobCommon *lob_common = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObColDesc &column = run_ctx.col_descs_->at(i);
      if (column.col_type_.is_lob_storage()) {
        ObObj &obj = tbl_row.row_val_.get_cell(i);
        ObLobAccessParam lob_param;
        if (OB_FAIL(delete_lob_col(run_ctx, column, obj, lob_common, lob_param))) {
          LOG_WARN("[STORAGE_LOB]failed to erase lob col.", K(ret), K(i), K(tbl_row));
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::prepare_scan_table_param(
    ObTableScanParam &param,
    share::schema::ObMultiVersionSchemaService &schema_service)
{
  int ret =  OB_SUCCESS;
  if (NULL == param.table_param_ || OB_INVALID_ID == param.table_param_->get_table_id()) {
    void *buf = NULL;
    ObTableParam *table_param = NULL;
    ObSchemaGetterGuard schema_guard;
    const ObTableSchema *table_schema = NULL;
    const uint64_t tenant_id = MTL_ID();
    const bool check_formal = param.index_id_ > OB_MAX_CORE_TABLE_ID;
    if (OB_FAIL(schema_service.get_tenant_schema_guard(tenant_id, schema_guard))) {
      LOG_WARN("failed to get schema manager", K(ret), K(tenant_id));
    } else if (check_formal && OB_FAIL(schema_guard.check_formal_guard())) {
      LOG_WARN("Fail to check formal schema, ", K(param.index_id_), K(ret));
    } else  if (OB_FAIL(schema_guard.get_table_schema(tenant_id,
                param.index_id_, table_schema))) {
      LOG_WARN("Fail to get table schema", K(param.index_id_), K(ret));
    } else if (NULL == table_schema) {
      ret = OB_TABLE_NOT_EXIST;
      LOG_WARN("table not exist", K(param.index_id_), K(ret));
    } else {
       if (NULL == (buf = param.allocator_->alloc(sizeof(ObTableParam)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Fail to allocate memory, ", K(ret));
       } else {
         //TODO table param should not generate twice!!!!
         table_param = new (buf) ObTableParam(*param.allocator_);
         table_param->get_enable_lob_locator_v2() = (!IS_CLUSTER_VERSION_BEFORE_4_1_0_0);
         if (OB_FAIL(table_param->convert(*table_schema, param.column_ids_))) {
           LOG_WARN("Fail to convert table param, ", K(ret));
         } else {
           param.table_param_ = table_param;
         }
       }
    }
  }
  return ret;
}

void ObLSTabletService::dump_diag_info_for_old_row_loss(
    ObRelativeTable &data_table,
    ObStoreCtx &store_ctx,
    const ObStoreRow &tbl_row)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "DumpDIAGInfo"));
  ObTableAccessParam access_param;
  ObTableAccessContext access_ctx;
  ObSEArray<int32_t, 16> out_col_pros;
  ObStoreRowkey rowkey;
  ObDatumRowkey datum_rowkey;
  ObDatumRowkeyHelper rowkey_helper(allocator);
  const int64_t schema_rowkey_cnt = data_table.get_rowkey_column_num();
  ObTableStoreIterator &table_iter = *data_table.tablet_iter_.table_iter();
  const ObITableReadInfo &rowkey_read_info = data_table.tablet_iter_.get_tablet()->get_rowkey_read_info();
  ObQueryFlag query_flag(ObQueryFlag::Forward,
      false, /*is daily merge scan*/
      false, /*is read multiple macro block*/
      false, /*sys task scan, read one macro block in single io*/
      false /*is full row scan?*/,
      false,
      false);
  query_flag.read_latest_ = ObQueryFlag::OBSF_MASK_READ_LATEST;
  common::ObVersionRange trans_version_rang;
  trans_version_rang.base_version_ = 0;
  trans_version_rang.multi_version_start_ = 0;
  trans_version_rang.snapshot_version_ = store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();

  for (int64_t i = 0; OB_SUCC(ret) && i < rowkey_read_info.get_schema_rowkey_count(); i++) {
    if (OB_FAIL(out_col_pros.push_back(i))) {
      STORAGE_LOG(WARN, "Failed to push back col project", K(ret), K(i));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(rowkey.assign(tbl_row.row_val_.cells_, schema_rowkey_cnt))) {
    LOG_WARN("Failed to assign rowkey", K(ret));
  } else if (OB_FAIL(rowkey_helper.convert_datum_rowkey(rowkey.get_rowkey(), datum_rowkey))) {
    STORAGE_LOG(WARN, "Failed to transfer datum rowkey", K(ret), K(rowkey));
  } else if (OB_FAIL(access_ctx.init(query_flag, store_ctx, allocator, trans_version_rang))) {
    LOG_WARN("Fail to init access ctx", K(ret));
  } else {
    access_param.is_inited_ = true;
    access_param.iter_param_.table_id_ = data_table.get_table_id();
    access_param.iter_param_.tablet_id_ = data_table.tablet_iter_.get_tablet()->get_tablet_meta().tablet_id_;
    access_param.iter_param_.read_info_ = &rowkey_read_info;
    access_param.iter_param_.rowkey_read_info_ = &rowkey_read_info;
    access_param.iter_param_.out_cols_project_ = &out_col_pros;
    access_param.iter_param_.need_trans_info_ = true;

    ObStoreRowIterator *getter = nullptr;
    ObITable *table = nullptr;
    const ObDatumRow *row = nullptr;

    FLOG_INFO("Try to find the specified rowkey within all the sstable", K(tbl_row), K(table_iter));
    FLOG_INFO("Prepare the diag env to dump the rows", K(store_ctx), K(rowkey), K(datum_rowkey),
        K(access_ctx.trans_version_range_));

    table_iter.resume();
    while (OB_SUCC(ret)) {
      if (OB_FAIL(table_iter.get_next(table))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next tables", K(ret));
        }
      } else if (OB_ISNULL(table)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("table must not be null", K(ret), K(table_iter));
      } else if (OB_FAIL(table->get(access_param.iter_param_, access_ctx, datum_rowkey, getter))) {
        LOG_WARN("Failed to get param", K(ret), KPC(table));
      } else if (OB_FAIL(getter->get_next_row(row))) {
        LOG_WARN("Failed to get next row", K(ret), KPC(table));
      } else if (row->row_flag_.is_not_exist() || row->row_flag_.is_delete()){
        FLOG_INFO("Cannot found rowkey in the table", KPC(row), KPC(table));
      } else if (table->is_sstable()) {
        FLOG_INFO("Found rowkey in the sstable",
            KPC(row), KPC(reinterpret_cast<ObSSTable*>(table)));
      } else {
        FLOG_INFO("Found rowkey in the memtable",
            KPC(row), KPC(reinterpret_cast<memtable::ObMemtable*>(table)));
      }

      // ignore error in the loop
      if (OB_FAIL(ret) && OB_ITER_END != ret) {
        ret = OB_SUCCESS;
      }
      if (OB_NOT_NULL(getter)) {
        getter->~ObStoreRowIterator();
        getter = nullptr;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }

    if (OB_SUCC(ret)) {
      FLOG_INFO("prepare to use single merge to find row", K(rowkey), K(datum_rowkey), K(access_param));
      ObSingleMerge *get_merge = nullptr;
      ObGetTableParam get_table_param;
      ObDatumRow *row = nullptr;
      void *buf = nullptr;
      if (OB_FAIL(get_table_param.tablet_iter_.assign(data_table.tablet_iter_))) {
        LOG_WARN("Failed to assign tablet iterator", K(ret));
      } else if (OB_ISNULL(buf = allocator.alloc(sizeof(ObSingleMerge)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("Failed to alloc memory for single merge", K(ret));
      } else if (FALSE_IT(get_merge = new(buf)ObSingleMerge())) {
      } else if (OB_FAIL(get_merge->init(access_param, access_ctx, get_table_param))) {
        LOG_WARN("Failed to init single get merge", K(ret));
      } else if (OB_FAIL(get_merge->open(datum_rowkey))) {
        LOG_WARN("Failed to open single merge", K(ret));
      } else if (FALSE_IT(get_merge->disable_fill_default())) {
      } else {
        while (OB_SUCC(get_merge->get_next_row(row))) {
          FLOG_INFO("Found one row for the rowkey", KPC(row));
        }
        FLOG_INFO("Finish to find rowkey with single merge", K(ret), K(rowkey), K(datum_rowkey));
      }
      if (OB_NOT_NULL(get_merge)) {
        get_merge->~ObSingleMerge();
        get_merge = nullptr;
      }
    }
#ifdef ENABLE_DEBUG_LOG
    // print single row check info
    if (store_ctx.mvcc_acc_ctx_.tx_id_.is_valid()) {
      transaction::ObTransService *trx = MTL(transaction::ObTransService *);
      if (OB_NOT_NULL(trx)
          && NULL != trx->get_defensive_check_mgr()) {
        (void)trx->get_defensive_check_mgr()->dump(store_ctx.mvcc_acc_ctx_.tx_id_);
      }
    }
#endif
  }
}

int ObLSTabletService::prepare_dml_running_ctx(
    const common::ObIArray<uint64_t> *column_ids,
    const common::ObIArray<uint64_t> *upd_col_ids,
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(run_ctx.init(
      column_ids,
      upd_col_ids,
      MTL(ObTenantSchemaService*)->get_schema_service(),
      tablet_handle))) {
    LOG_WARN("failed to init run ctx", K(ret));
  }

  return ret;
}


int ObLSTabletService::get_ls_min_end_scn(
    SCN &min_end_scn_from_latest_tablets, SCN &min_end_scn_from_old_tablets)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_->get_ls_id();
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObSArray<ObTabletID> tablet_ids;
  GetAllTabletIDOperator op(tablet_ids, true/*except_ls_inner_tablet*/);
  min_end_scn_from_latest_tablets.set_max();
  min_end_scn_from_old_tablets.set_max();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_id_set_.foreach(op))) {
    STORAGE_LOG(WARN, "fail to get all tablet ids from set", K(ret));
  } else {
    SCN ls_checkpoint = ls_->get_clog_checkpoint_scn();
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      ObTabletMapKey key(ls_id, tablet_ids.at(i));
      SCN min_end_scn_from_latest = SCN::max_scn();
      SCN min_end_scn_from_old = SCN::max_scn();
      if (OB_FAIL(t3m->get_min_end_scn_for_ls(key,
                                              ls_checkpoint,
                                              min_end_scn_from_latest,
                                              min_end_scn_from_old))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("fail to get min end scn", K(ret), K(key));
        } else {
          ret = OB_SUCCESS;
        }
      } else {
        if (min_end_scn_from_latest < min_end_scn_from_latest_tablets) {
          LOG_DEBUG("update", K(key), K(min_end_scn_from_latest_tablets), K(min_end_scn_from_latest));
          min_end_scn_from_latest_tablets = min_end_scn_from_latest;
        }

        if (min_end_scn_from_old < min_end_scn_from_old_tablets) {
          LOG_DEBUG("update", K(key), K(min_end_scn_from_old_tablets), K(min_end_scn_from_old));
          min_end_scn_from_old_tablets = min_end_scn_from_old;
        }
      }
    }
    // now tx_data contains mds tx_op to remove retain_ctx
    // so we need wait ls_checkpoint advance to recycle tx_data
    if (ls_checkpoint < min_end_scn_from_latest_tablets) {
      min_end_scn_from_latest_tablets = ls_checkpoint;
    }
    LOG_INFO("get ls min end scn finish", K(ls_checkpoint));
  }
  return ret;
}

int ObLSTabletService::get_multi_ranges_cost(
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    const common::ObIArray<common::ObStoreRange> &ranges,
    int64_t &total_size)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator iter;
  const int64_t max_snapshot_version = INT64_MAX;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_read_tables(tablet_id, timeout_us, max_snapshot_version, max_snapshot_version, iter))) {
    LOG_WARN("fail to get all read tables", K(ret), K(tablet_id), K(max_snapshot_version));
  } else {
    ObPartitionMultiRangeSpliter spliter;
    if (OB_FAIL(spliter.get_multi_range_size(
        ranges,
        iter.get_tablet()->get_rowkey_read_info(),
        *iter.table_iter(),
        total_size))) {
      LOG_WARN("fail to get multi ranges cost", K(ret), K(ranges));
    }
  }
  return ret;
}

int ObLSTabletService::split_multi_ranges(
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    const ObIArray<ObStoreRange> &ranges,
    const int64_t expected_task_count,
    common::ObIAllocator &allocator,
    ObArrayArray<ObStoreRange> &multi_range_split_array)
{
  int ret = OB_SUCCESS;
  ObTabletTableIterator iter;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_read_tables(tablet_id, timeout_us, INT64_MAX, INT64_MAX, iter))) {
    LOG_WARN("fail to get all read tables", K(ret), K(tablet_id), K(timeout_us));
  } else {
    ObPartitionMultiRangeSpliter spliter;
    if (OB_FAIL(spliter.get_split_multi_ranges(
        ranges,
        expected_task_count,
        iter.get_tablet()->get_rowkey_read_info(),
        *iter.table_iter(),
        allocator,
        multi_range_split_array))) {
      LOG_WARN("fail to get splitted ranges", K(ret), K(ranges), K(expected_task_count));
    }
  }

  return ret;
}

int ObLSTabletService::estimate_row_count(
    const ObTableScanParam &param,
    const ObTableScanRange &scan_range,
    const int64_t timeout_us,
    common::ObIArray<ObEstRowCountRecord> &est_records,
    int64_t &logical_row_count,
    int64_t &physical_row_count)
{
  int ret = OB_SUCCESS;
  ObPartitionEst batch_est;
  ObTabletTableIterator tablet_iter;
  common::ObSEArray<ObITable*, 4> tables;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!param.is_estimate_valid() ||
                         !scan_range.is_valid() ||
                         param.frozen_version_ == -1)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(param), K(scan_range), K(param.frozen_version_));
  } else if (scan_range.is_empty()) {
  } else {
    if (OB_FAIL(get_read_tables(param.tablet_id_, timeout_us, param.frozen_version_,
                                param.frozen_version_, tablet_iter, false))) {
      LOG_WARN("failed to get tablet_iter", K(ret), K(param.frozen_version_), K(param), K(timeout_us));
    } else {
      int64_t major_version = -1;
      while(OB_SUCC(ret)) {
        ObITable *table = nullptr;
        if (OB_FAIL(tablet_iter.table_iter()->get_next(table))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get next table", K(ret), K(tablet_iter.table_iter()));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else if (OB_ISNULL(table)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("table shoud not be null", K(ret), K(tablet_iter.table_iter()));
        } else if (table->is_sstable()) {
          const ObSSTable *sstable = static_cast<const ObSSTable*>(table);
          if (sstable->is_major_sstable()) {
            major_version = sstable->get_data_version();
          } else if (table->get_upper_trans_version() <= major_version) {
            LOG_DEBUG("cur sstable is corvered by major", K(ret), K(major_version), K(*table));
            continue;
          }
        }
        if (OB_FAIL(ret)) {
        } else if (table->is_empty()) {
          LOG_DEBUG("cur table is empty", K(ret), K(*table));
          continue;
        } else if (OB_FAIL(tables.push_back(table))) {
          LOG_WARN("failed to push back table", K(ret), K(tables));
        }
      }
    }
    if (OB_SUCC(ret) && tables.count() > 0) {
      ObTableEstimateBaseInput base_input(param.scan_flag_, param.index_id_, param.tx_id_, tables, tablet_iter.get_tablet_handle());
      if (scan_range.is_get()) {
        if (OB_FAIL(ObTableEstimator::estimate_row_count_for_get(base_input, scan_range.get_rowkeys(), batch_est))) {
          LOG_WARN("failed to estimate row count", K(ret), K(param), K(scan_range));
        }
      } else if (OB_FAIL(ObTableEstimator::estimate_row_count_for_scan(base_input, scan_range.get_ranges(), batch_est, est_records))) {
        LOG_WARN("failed to estimate row count", K(ret), K(param), K(scan_range));
      }
    }
  }
  if (OB_SUCC(ret)) {
    logical_row_count = batch_est.logical_row_count_;
    physical_row_count = batch_est.physical_row_count_;
  }
  LOG_DEBUG("estimate result", K(ret), K(batch_est), K(est_records));
  return ret;
}

int ObLSTabletService::estimate_block_count_and_row_count(
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    int64_t &macro_block_count,
    int64_t &micro_block_count,
    int64_t &sstable_row_count,
    int64_t &memtable_row_count)
{
  int ret = OB_SUCCESS;
  macro_block_count = 0;
  micro_block_count = 0;
  sstable_row_count = 0;
  memtable_row_count = 0;
  ObTabletTableIterator tablet_iter;
  share::SCN max_readable_scn;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), nullptr, max_readable_scn))) {
    LOG_WARN("failed to get gts", K(ret));
  } else if (OB_FAIL(get_read_tables(tablet_id, timeout_us,
                                     static_cast<int64_t>(max_readable_scn.get_val_for_sql()),
                                     static_cast<int64_t>(max_readable_scn.get_val_for_sql()),
                                     tablet_iter, false/*allow_no_ready_read*/))) {
    LOG_WARN("failed to get read tables", K(ret));
  }

  ObITable *table = nullptr;
  ObSSTable *sstable = nullptr;
  while (OB_SUCC(ret)) {
    ObSSTableMetaHandle sst_meta_hdl;
    if (OB_FAIL(tablet_iter.table_iter()->get_next(table))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get next tables", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else if (OB_ISNULL(table)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get unexpected null table", K(ret), K(tablet_iter.table_iter()));
    } else if (table->is_memtable()) {
      memtable_row_count += static_cast<memtable::ObMemtable *>(table)->get_physical_row_cnt();
    } else if (table->is_sstable()) {
      sstable = static_cast<ObSSTable *>(table);
      if (OB_FAIL(sstable->get_meta(sst_meta_hdl))) {
        LOG_WARN("fail to get sstable meta handle", K(ret));
      } else {
        sstable = static_cast<ObSSTable *>(table);
        macro_block_count += sstable->get_data_macro_block_count();
        micro_block_count += sst_meta_hdl.get_sstable_meta().get_data_micro_block_count();
        sstable_row_count += sst_meta_hdl.get_sstable_meta().get_row_count();
      }
    }
  }
  return ret;
}

int ObLSTabletService::get_tx_data_memtable_mgr(ObMemtableMgrHandle &mgr_handle)
{
  mgr_handle.reset();
  return mgr_handle.set_memtable_mgr(&tx_data_memtable_mgr_);
}

int ObLSTabletService::get_tx_ctx_memtable_mgr(ObMemtableMgrHandle &mgr_handle)
{
  mgr_handle.reset();
  return mgr_handle.set_memtable_mgr(&tx_ctx_memtable_mgr_);
}

int ObLSTabletService::get_lock_memtable_mgr(ObMemtableMgrHandle &mgr_handle)
{
  mgr_handle.reset();
  return mgr_handle.set_memtable_mgr(&lock_memtable_mgr_);
}

int ObLSTabletService::get_mds_table_mgr(mds::MdsTableMgrHandle &mgr_handle)
{
  mgr_handle.reset();
  return mgr_handle.set_mds_table_mgr(&mds_table_mgr_);
}

int ObLSTabletService::get_bf_optimal_prefix(int64_t &prefix)
{
  int ret = OB_SUCCESS;
  prefix = 0;
  return ret;
}

int ObLSTabletService::create_ls_inner_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const SCN &major_frozen_scn,
    const ObCreateTabletSchema &create_tablet_schema,
    const SCN &create_scn)
{
  int ret = OB_SUCCESS;
  bool b_exist = false;
  ObTabletHandle tablet_handle;
  common::ObTabletID empty_tablet_id;
  ObMetaDiskAddr disk_addr;
  const int64_t snapshot_version = major_frozen_scn.get_val_for_tx();

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!major_frozen_scn.is_valid())
      || OB_UNLIKELY(!create_tablet_schema.is_valid())
      || OB_UNLIKELY(lib::Worker::CompatMode::INVALID == create_tablet_schema.get_compat_mode())
      || OB_UNLIKELY(!create_scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id), K(major_frozen_scn),
        K(create_tablet_schema), K(create_scn));
  } else if (OB_UNLIKELY(ls_id != ls_->get_ls_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls id is unexpected", K(ret), "arg_ls_id", ls_id, "ls_id", ls_->get_ls_id());
  } else if (OB_UNLIKELY(!tablet_id.is_ls_inner_tablet())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet id is not ls inner tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
    LOG_WARN("failed to check tablet existence", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(b_exist)) {
    ret = OB_TABLET_EXIST;
    LOG_WARN("tablet already exists", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_FAIL(create_inner_tablet(ls_id, tablet_id, tablet_id/*data_tablet_id*/,
        create_scn, snapshot_version, create_tablet_schema, tablet_handle))) {
    LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(tablet_id),
        K(create_scn), K(major_frozen_scn), K(create_tablet_schema));
  }

  return ret;
}

int ObLSTabletService::remove_ls_inner_tablet(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<common::ObTabletID, 1> tablet_id_array;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!ls_id.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(ls_id != ls_->get_ls_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls id is unexpected", K(ret), "arg_ls_id", ls_id, "ls_id", ls_->get_ls_id());
  } else if (OB_FAIL(do_remove_tablet(ls_id, tablet_id))) {
    LOG_WARN("failed to remove tablet", K(ret), K(ls_id), K(tablet_id));
  }

  return ret;
}

int ObLSTabletService::build_tablet_iter(ObLSTabletIterator &iter)
{
  int ret = common::OB_SUCCESS;
  GetAllTabletIDOperator op(iter.tablet_ids_);
  iter.ls_tablet_service_ = this;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_id_set_.foreach(op))) {
    STORAGE_LOG(WARN, "fail to get all tablet ids from set", K(ret));
  } else if (OB_UNLIKELY(!iter.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid", K(ret), K(iter));
  }
  if (OB_FAIL(ret)) {
    iter.reset();
  }
  return ret;
}

int ObLSTabletService::build_tablet_iter(ObHALSTabletIDIterator &iter)
{
  int ret = common::OB_SUCCESS;
  GetAllTabletIDOperator op(iter.tablet_ids_);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(tablet_id_set_.foreach(op))) {
    STORAGE_LOG(WARN, "fail to get all tablet ids from set", K(ret));
  } else if (OB_UNLIKELY(!iter.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("iter is invalid", K(ret), K(iter));
  }

  if (OB_FAIL(ret)) {
    iter.reset();
  }
  return ret;
}

int ObLSTabletService::build_tablet_iter(ObHALSTabletIterator &iter)
{
  int ret = common::OB_SUCCESS;
  iter.ls_tablet_service_ = this;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(build_tablet_iter(iter.tablet_id_iter_))) {
    STORAGE_LOG(WARN, "fail to build ha tablet id iterator", K(ret));
  }

  if (OB_FAIL(ret)) {
    iter.reset();
  }
  return ret;
}

int ObLSTabletService::set_allow_to_read_(ObLS *ls)
{
  int ret = OB_SUCCESS;
  bool allow_read = false;

  if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("set allow to read get invalid argument", K(ret), KP(ls));
  } else {
    if (OB_FAIL(ls->check_allow_read(allow_read))) {
      LOG_WARN("failed to check allow read", K(ret), KPC(ls));
    } else if (!allow_read) {
      allow_to_read_mgr_.disable_to_read();
      FLOG_INFO("set ls do not allow to read", KPC(ls));
    } else {
      allow_to_read_mgr_.enable_to_read();
    }
  }
  return ret;
}

void ObLSTabletService::enable_to_read()
{
  allow_to_read_mgr_.enable_to_read();
}

void ObLSTabletService::disable_to_read()
{
  allow_to_read_mgr_.disable_to_read();
}

int ObLSTabletService::GetAllTabletIDOperator::operator()(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id));
  } else if (except_ls_inner_tablet_ && tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(tablet_ids_.push_back(tablet_id))) {
    LOG_WARN("failed to push back tablet id", K(ret), K(tablet_id));
  }
  return ret;
}

int ObLSTabletService::DestroyMemtableAndMemberAndMdsTableOperator::operator()(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  cur_tablet_id_ = tablet_id;
  if (OB_UNLIKELY(!tablet_id.is_valid()) || OB_ISNULL(tablet_svr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(tablet_svr_));
  } else if (OB_ISNULL(tablet_svr_->ls_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret));
  } else {
    const ObTabletMapKey key(tablet_svr_->ls_->get_ls_id(), tablet_id);
    if (OB_FAIL(t3m->release_memtable_and_mds_table_for_ls_offline(key))) {
      LOG_WARN("failed to release memtables and mds table", K(ret), K(key));
    }
  }
  return ret;
}

int ObLSTabletService::SetMemtableFrozenOperator::operator()(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObTabletHandle handle;
  cur_tablet_id_ = tablet_id;
  if (OB_UNLIKELY(!tablet_id.is_valid()) || OB_ISNULL(tablet_svr_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(tablet_svr_));
  } else if (OB_FAIL(tablet_svr_->get_tablet(tablet_id,
                                             handle,
                                             ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT,
                                             ObMDSGetTabletMode::READ_WITHOUT_CHECK))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_WARN("failed to get tablet, skip set memtable frozen", K(ret), K(tablet_id));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("failed to get tablet", K(ret), K(tablet_id));
    }
  } else if (OB_FAIL(handle.get_obj()->set_frozen_for_all_memtables())) {
    LOG_WARN("failed to set frozen for all memtables", K(ret), K(tablet_id));
  }
  return ret;
}

void ObLSTabletService::AllowToReadMgr::disable_to_read()
{
  bool old_v = false;
  bool new_v = false;
  do {
    old_v = ATOMIC_LOAD(&allow_to_read_);
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&allow_to_read_, old_v, new_v) != old_v);
}

void ObLSTabletService::AllowToReadMgr::enable_to_read()
{
  bool old_v = false;
  bool new_v = true;
  do {
    old_v = ATOMIC_LOAD(&allow_to_read_);
    if (old_v == new_v) {
      break;
    }
  } while (ATOMIC_CAS(&allow_to_read_, old_v, new_v) != old_v);
}

void ObLSTabletService::AllowToReadMgr::load_allow_to_read_info(
    bool &allow_to_read)
{
  allow_to_read = ATOMIC_LOAD(&allow_to_read_);
}

int ObLSTabletService::get_all_tablet_ids(
    const bool except_ls_inner_tablet,
    common::ObIArray<ObTabletID> &tablet_id_array)
{
  int ret = OB_SUCCESS;
  GetAllTabletIDOperator op(tablet_id_array, except_ls_inner_tablet);
  if (OB_FAIL(tablet_id_set_.foreach(op))) {
    LOG_WARN("failed to traverse tablet id set", K(ret));
  }
  return ret;
}

int ObLSTabletService::flush_mds_table(int64_t recycle_scn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet service is not init", KR(ret), KPC(this));
  } else if (OB_FAIL(mds_table_mgr_.flush(SCN::max_scn(), checkpoint::INVALID_TRACE_ID, true))) {
    LOG_WARN("flush mds table failed", KR(ret), KPC(this));
  }
  LOG_INFO("finish flush mds table", KR(ret), K(recycle_scn));
  return ret;
}

int ObLSTabletService::set_frozen_for_all_memtables()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    SetMemtableFrozenOperator set_mem_frozen_op(this);
    if (OB_FAIL(tablet_id_set_.foreach(set_mem_frozen_op))) {
      LOG_WARN("fail to set memtables frozen", K(ret), K(set_mem_frozen_op.cur_tablet_id_));
    }
  }
  return ret;
}

int ObLSTabletService::ha_scan_all_tablets(const HandleTabletMetaFunc &handle_tablet_meta_f)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ObHALSTabletIterator iterator(ls_->get_ls_id(), false/* need_initial_state */);
    if (OB_FAIL(build_tablet_iter(iterator))) {
      LOG_WARN("fail to build tablet iterator", K(ret), KPC(this));
    } else {
      ObTabletHandle tablet_handle;
      ObTablet *tablet = nullptr;
      obrpc::ObCopyTabletInfo tablet_info;
      ObTabletCreateDeleteMdsUserData user_data;
      mds::MdsWriter writer;// will be removed later
      mds::TwoPhaseCommitState trans_stat;// will be removed later
      share::SCN trans_version;// will be removed later

      while (OB_SUCC(ret)) {
        tablet_info.reset();
        user_data.reset();
        trans_stat = mds::TwoPhaseCommitState::STATE_END;
        if (OB_FAIL(iterator.get_next_tablet(tablet_handle))) {
          if (OB_ITER_END == ret) {
            ret = OB_SUCCESS;
            break;
          } else {
            LOG_WARN("failed to get tablet", K(ret));
          }
        } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("tablet is nullptr", K(ret), K(tablet_handle));
        } else if (OB_FAIL(tablet->get_latest(user_data, writer, trans_stat, trans_version))) {
          if (OB_EMPTY_RESULT == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get latest tablet status", K(ret), KPC(tablet));
          }
        } else if (trans_stat != mds::TwoPhaseCommitState::ON_COMMIT
            && ObTabletStatus::TRANSFER_IN == user_data.tablet_status_) {
          //TODO(muwei.ym) CAN NOT USE this condition when MDS supports uncommitted transaction in 4.3

          // why we should skip uncommited transfer in tablet, because if we backup the uncommited transfer in tablet
          // but the final result of the uncommited transfer in tablet is aborted, which means the tablet has no MDS Table,
          // and the restore process will create this tablet nontheless, however this aborted tablet should be GC-ed at the end,
          // because the aborted tablet has no MDS Table, the tablet can not be GC-ed correctly, resulting in the tablet dangling
          // for details, see issue-51990749

          // we can skip backup uncommited transfer in tablets because we backup ls meta first
          // if the start transfer in transaction is not commited, the clog_checkpoint_scn recorded
          // in ls meta will not be advanced, so that even though we skip backup uncommited
          // transfer in tablet, we can still restore transfer in tablet by replaying clog
          LOG_WARN("tablet is transfer in but not commited, skip", K(tablet_handle), K(user_data));
          continue;
        } else if (OB_FAIL(tablet->build_migration_tablet_param(tablet_info.param_))) {
          LOG_WARN("failed to build migration tablet param", K(ret));
        } else if (OB_FAIL(tablet->get_ha_sstable_size(tablet_info.data_size_))) {
          LOG_WARN("failed to get sstable size", K(ret), KPC(tablet));
        } else if (OB_FAIL(ObStorageHAUtils::get_server_version(tablet_info.version_))) {
          LOG_WARN("failed to get server version", K(ret), K(tablet_info));
        } else {
          tablet_info.tablet_id_ = tablet->get_tablet_meta().tablet_id_;
          tablet_info.status_ = ObCopyTabletStatus::TABLET_EXIST;
          if (OB_FAIL(handle_tablet_meta_f(tablet_info, tablet_handle))) {
           LOG_WARN("fail to handle tablet meta", K(ret), K(tablet_info));
          }
        }
      }
    }
  }

  return ret;
}

int ObLSTabletService::ha_get_tablet(
    const common::ObTabletID &tablet_id,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  bool is_empty_shell = false;
  ObTablet *tablet = nullptr;
  if (OB_FAIL(direct_get_tablet(tablet_id, handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (OB_ISNULL(tablet = handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(tablet_id));
  } else if (tablet->is_empty_shell()) {
    // treat empty shell as tablet not exist.
    ret = OB_TABLET_NOT_EXIST;
  }
  return ret;
}

int ObLSTabletService::get_tablet_without_memtables(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const bool force_alloc_new = true;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_ISNULL(t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant meta mem mgr should not be null", K(ret), KP(t3m));
  } else if (OB_FAIL(t3m->get_tablet_with_allocator(
      priority, key, allocator, handle, force_alloc_new))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
    } else {
      LOG_WARN("failed to get tablet with allocator", K(ret), K(priority), K(key));
    }
  } else if (OB_FAIL(handle.get_obj()->clear_memtables_on_table_store())) {
    LOG_WARN("failed to clear memtables on table store", K(ret), K(key));
  }
  return ret;
}

int ObLSTabletService::ha_get_tablet_without_memtables(
    const WashTabletPriority &priority,
    const ObTabletMapKey &key,
    common::ObArenaAllocator &allocator,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(get_tablet_without_memtables(priority, key, allocator, handle))) {
    LOG_WARN("failed to get tablet without memtables", K(ret), K(priority), K(key));
  } else if (OB_ISNULL(tablet = handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(key));
  } else if (tablet->is_empty_shell()) {
    // treat empty shell as tablet not exist.
    ret = OB_TABLET_NOT_EXIST;
  }
  return ret;
}

int ObLSTabletService::check_real_leader_for_4377_(const ObLSID ls_id)
{
  int ret = OB_SUCCESS;
  ObLSService* ls_srv = nullptr;
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  int64_t epoch = 0;
  bool is_real_leader = false;

  if (OB_ISNULL(ls_srv = MTL(ObLSService*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("MTL(ObLSService*) fail, MTL not init?", K(ret));
  } else if (OB_FAIL(ls_srv->get_ls(ls_id,
                                    ls_handle,
                                    ObLSGetMod::TRANS_MOD))) {
    LOG_ERROR("ls_srv->get_ls() fail", KR(ret));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid ls", KR(ret));
  } else if (ls->is_logonly_replica()) {
    ret = OB_NOT_MASTER;
    LOG_WARN("get follower status with logonly replica", KPC(ls));
  } else if (OB_FAIL(ls->get_tx_svr()->get_tx_ls_log_adapter()->get_role(is_real_leader, epoch))) {
    LOG_WARN("get replica status fail", K(ret), KPC(ls));
  } else if (!is_real_leader) {
    ret = OB_NOT_MASTER;
    LOG_WARN("get follower status during 4377", K(ret), KPC(ls));
  }

  return ret;
}

int ObLSTabletService::check_need_rollback_in_transfer_for_4377_(const transaction::ObTxDesc *tx_desc,
                                                                 ObTabletHandle &data_tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTabletCreateDeleteMdsUserData user_data;
  mds::MdsWriter unused_writer;// will be removed later
  mds::TwoPhaseCommitState unused_trans_stat;// will be removed later
  share::SCN unused_trans_version;// will be removed later

  if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tx_desc is null when check 4377", K(ret));
  } else if (OB_FAIL(data_tablet_handle.get_obj()->get_latest(
        user_data, unused_writer, unused_trans_stat, unused_trans_version))) {
    LOG_WARN("can not get the tablet status from ObITabletMdsInterface", K(ret));
  } else {
    // ObTabletStatus tablet_status = user_data.tablet_status_;
    // CASE 1: tablet_status_ == NORMAL, means there's no transfer in progress right now.
    // If there's valid transfer_scn_, which means transfer has finished, we should check
    // whether the transaction started before transfer to avoid throwing incorrect 4377
    // error. Otherwise, it's a normal case of 4377 check because no transfer has happened,
    // and we should still throw 4377 error.
    //
    // CASE 2: tablet_status_ == TRANSFER_OUT, means it's in transfer out progress.
    // CASE 2.1: transfer_scn_ is invalid, the transfer_scn_ is not backfilled, we still
    // read from origin tablet, if there's inconsistent, it should throw 4377 error
    // CASE 2.2: transfer_scn_ is valid, the transfer_scn_ is backfilled, it means that
    // tablet has started to transfer out. If it's the first transfer, we should
    // throw 4377 error, otherwise we should compare the transfer_scn_ with the tx_scn,
    // and decide to rollback the transaction or throw 4377 error.
    // However, we can not be sure about whether this transfer out is the first transfer or not.
    // So we consider all the transfer out status as not the first transfer here, to avoid
    // throwing the incorrect 4377 error.
    //
    // CASE 3: tablet_status_ == TRANSFER_IN, means it's in transfer in progress, and
    // the transfer out progress must have been finished. We can compare the transfer_scn_
    // with the tx_scn directly because there should be valid transfer_scn_.
    //
    // Above all, we do not need to consider the status of tablet.
    if (user_data.transfer_scn_.is_valid()) {
      if (user_data.transfer_scn_.convert_to_ts() > tx_desc->get_active_ts()) {
        // TODO(yangyifei.yyf): active_ts is set by ObClockGenerator::get_clock, it's not a GTS.
        // And transfer_scn is set by redo_scn, it's incorrect to compare it with active_ts.
        // We should adjust this judgment later.
        ret = OB_TRANS_NEED_ROLLBACK;
        LOG_WARN("maybe meet transfer during kill tx, which can cause 4377 error, so we will rollback it",
                K(tx_desc),
                K(user_data));
      }
    } else {
      // There's no transfer after trans begin, still throw 4377 error
    }
  }
  return ret;
}

int ObLSTabletService::check_parts_tx_state_in_transfer_for_4377_(transaction::ObTxDesc *tx_desc)
{
  int ret = OB_SUCCESS;
  transaction::ObTransService *txs = MTL(transaction::ObTransService *);
  transaction::ObTxPartList copy_parts;
  bool is_alive = false;

  if (OB_ISNULL(tx_desc)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("tx_desc is null", K(ret));
  } else if (OB_FAIL(tx_desc->get_parts_copy(copy_parts))) {
    TRANS_LOG(WARN, "get participants copy error", K(ret));
  } else {
    // Although we can verify whether transaction data is incomplete due to the
    // transfer by checking the local tablet. While in the scenario where we
    // validate the index table through the main table, we cannot discover
    // whether the main table's txn data is incomplete due to the transfer by
    // using the local index table. Furthermore, the local index table and the
    // main table may not even locate on the same machine.
    //
    // Therefore, we choose to traverse all participant lists through RPC to
    // confirm whether the txn context that could potentially participate in the
    // transfer and has been terminated due to the transfer. In this scenario,
    // we consider the occurrence of 4377 as misreporting, and we convert it
    // into OB_TRANS_NEED_ROLLBACK to indicate the need to rollback the txn.
    for (int64_t i = 0; OB_SUCC(ret) && i < copy_parts.count(); i++) {
      ObLSID ls_id = copy_parts[i].id_;
      is_alive = false;
      if (OB_FAIL(txs->ask_tx_state_for_4377(ls_id,
                                             tx_desc->get_tx_id(),
                                             is_alive))) {
        TRANS_LOG(WARN, "fail to ask tx state for 4377", K(tx_desc));
      } else if (!is_alive) {
        ret = OB_TRANS_NEED_ROLLBACK;
        LOG_WARN("maybe meet transfer during kill tx, which can cause 4377 error, so we will rollback it",
                 K(tx_desc));
      }
    }
  }

  return ret;
}

int ObLSTabletService::offline_gc_tablet_for_create_or_transfer_in_abort_()
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls_->get_ls_id();
  LOG_INFO("start offline_gc_tablet_for_create_or_transfer_in_abort", K(ret), K(ls_id));
  ObTabletIDArray deleted_tablets;
  ObLSTabletIterator tablet_iter(ObMDSGetTabletMode::READ_WITHOUT_CHECK);
  bool tablet_status_is_written = false;
  ObTabletCreateDeleteMdsUserData data;
  bool is_finish = false;
  // get deleted_tablets
  if (OB_FAIL(build_tablet_iter(tablet_iter))) {
    LOG_WARN("failed to build ls tablet iter", KR(ret), KPC(this));
  } else {
    ObTabletHandle tablet_handle;
    ObTablet *tablet = NULL;
    mds::MdsWriter writer;// will be removed later
    mds::TwoPhaseCommitState trans_stat;// will be removed later
    share::SCN trans_version;// will be removed later
    while (OB_SUCC(ret)) {
      if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          break;
        } else {
          LOG_WARN("failed to get tablet", KR(ret), KPC(this), K(tablet_handle));
        }
      } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("invalid tablet handle", KR(ret), KPC(this), K(tablet_handle));
      } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet is NULL", KR(ret));
      } else if (tablet->is_ls_inner_tablet()) {
        // skip ls inner tablet
      } else if (tablet->is_empty_shell()) {
        // skip empty shell
      } else if (OB_FAIL(tablet->check_tablet_status_written(tablet_status_is_written))) {
        LOG_WARN("failed to check mds written", KR(ret), KPC(tablet));
      } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest(data, writer, trans_stat, trans_version))) {
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
          if (tablet_status_is_written) {
            if (OB_FAIL(deleted_tablets.push_back(tablet->get_tablet_meta().tablet_id_))) {
              LOG_WARN("failed to push_back", KR(ret));
            } else {
              LOG_INFO("tablet need be gc", KPC(tablet));
            }
          }
        }
      }
    }
  }

  // gc deleted_tablets
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < deleted_tablets.count(); ++i) {
      const common::ObTabletID &tablet_id = deleted_tablets.at(i);
      DestroyMemtableAndMemberAndMdsTableOperator clean_mem_op(this);
      if (OB_FAIL(clean_mem_op(tablet_id))) {
        LOG_WARN("failed to clean mem", KR(ret));
      } else if (OB_FAIL(do_remove_tablet(ls_id, tablet_id))) {
        LOG_WARN("failed to remove tablet", K(ret), K(ls_id), K(tablet_id));
      } else {
        LOG_INFO("gc tablet finish", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_rollback_tablet_is_same_(
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const share::SCN &transfer_start_scn,
    bool &is_same)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  is_same = true;

  if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      is_same = true;
      ret = OB_SUCCESS;
    }
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(ls_id), K(tablet_id));
  } else if (transfer_start_scn != tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
    is_same = false;
    LOG_ERROR("rollback tablet is not same, cannot rollback", K(ls_id), K(tablet_id), K(transfer_start_scn), KPC(tablet));
  } else {
    is_same = true;
  }
  return ret;
}

int ObLSTabletService::check_tablet_no_active_memtable(const ObIArray<ObTabletID> &tablet_list, bool &has)
{
  int ret = OB_SUCCESS;
  has = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    for (int64_t idx = 0; !has && OB_SUCC(ret) && idx < tablet_list.count(); idx++) {
      ObTabletID tablet_id = tablet_list.at(idx);
      ObTabletHandle handle;
      ObTablet *tablet = NULL;
      ObTableHandleV2 table_handle;
      if (OB_FAIL(direct_get_tablet(tablet_id, handle))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (FALSE_IT(tablet = handle.get_obj())) {
      } else if (OB_FAIL(tablet->get_active_memtable(table_handle))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("failed to get active memtable", K(ret), K(tablet_id));
        }
      } else if (OB_ISNULL(table_handle.get_table())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get unexpected null table", K(ret), K(tablet_id));
      } else if (table_handle.get_table()->is_active_memtable()) {
        LOG_WARN("tablet has active memtable", K(tablet_id), K(table_handle));
        has = true;
      }
    }
  }
  return ret;
}

int ObLSTabletService::get_tablet_and_address(
    const ObTabletMapKey &key,
    ObTabletHandle &old_tablet,
    ObMetaDiskAddr &old_tablet_addr,
    ObTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  ObBucketHashRLockGuard lock_guard(bucket_lock_, key.tablet_id_.hash());
  time_guard.click("RLock");
  if (OB_FAIL(direct_get_tablet(key.tablet_id_, old_tablet))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (FALSE_IT(old_tablet_addr = old_tablet.get_obj()->tablet_addr_)) {
  } else if (FALSE_IT(time_guard.click("GetTablet"))) {
  } else if (old_tablet.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet.get_obj());
  }
  return ret;
}

int ObLSTabletService::acquire_new_tablet(
    const ObTabletMapKey &key,
    const ObTabletHandle &old_tablet,
    ObTabletHandle &new_tablet,
    ObTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  if (OB_UNLIKELY(!key.is_valid() || !old_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(key), K(old_tablet));
  } else if (OB_FAIL(old_tablet.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret), K(old_tablet));
  } else {
    time_guard.click("FetchTabStor");
    const int64_t tablet_cache_size = old_tablet.get_obj()->get_try_cache_size();
    const int64_t try_cache_size = tablet_cache_size + table_store_wrapper.get_member()->get_deep_copy_size();
    const ObTabletPoolType type = try_cache_size > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE
                                ? ObTabletPoolType::TP_LARGE : ObTabletPoolType::TP_NORMAL;
    const bool try_smaller_pool = tablet_cache_size > ObTenantMetaMemMgr::NORMAL_TABLET_POOL_SIZE ? false : true;
    if (OB_FAIL(ObTabletPersister::acquire_tablet(type, key, try_smaller_pool, new_tablet))) {
      LOG_WARN("fail to acquire tablet", K(ret), K(key), K(type));
    }
    time_guard.click("AcqNewTab");
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase
