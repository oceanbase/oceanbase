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


#include "ob_ls_tablet_service.h"
#include "share/schema/ob_tenant_schema_service.h"
#include "storage/blocksstable/ob_datum_row_store.h"
#include "storage/blocksstable/ob_datum_row_utils.h"
#include "storage/ob_dml_running_ctx.h"
#include "storage/ob_partition_range_spliter.h"
#include "storage/ob_query_iterator_factory.h"
#include "storage/ob_value_row_iterator.h"
#include "storage/access/ob_table_scan_iterator.h"
#include "storage/access/ob_rows_info.h"
#include "storage/access/ob_rows_info.h"
#include "storage/access/ob_table_estimator.h"
#include "storage/access/ob_index_sstable_estimator.h"
#include "storage/access/ob_skip_index_sortedness.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/blocksstable/ob_sstable.h"
#include "storage/ddl/ob_direct_insert_sstable_ctx_new.h"
#include "storage/retrieval/ob_block_stat_iter.h"
#include "storage/tablet/ob_mds_schema_helper.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tablet/ob_tablet_service_clog_replay_executor.h"
#include "observer/table_load/ob_table_load_table_ctx.h"
#include "observer/table_load/ob_table_load_coordinator.h"
#include "observer/table_load/ob_table_load_store.h"
#include "observer/table_load/ob_table_load_store_trans_px_writer.h"
#include "storage/high_availability/ob_storage_ha_utils.h"
#include "share/ob_partition_split_query.h"
#include "storage/concurrency_control/ob_data_validation_service.h"
#include "share/scheduler/ob_partition_auto_split_helper.h"
#include "storage/concurrency_control/ob_data_validation_service.h"
#include "storage/slog_ckpt/ob_tablet_replay_create_handler.h"
#include "storage/tablet/ob_tablet_mds_table_mini_merger.h"
#include "storage/ddl/ob_tablet_ddl_kv.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "storage/meta_mem/ob_tablet_pointer.h"
#include "storage/truncate_info/ob_truncate_partition_filter.h"
#include "storage/access/ob_old_row_check_dumper.h"
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
    allow_to_read_mgr_(),
    is_inited_(false),
    is_stopped_(false)
{
}

ObLSTabletService::~ObLSTabletService()
{
}

int ObLSTabletService::init(
    ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K_(is_inited));
  } else if (OB_ISNULL(ls)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ls));
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
  } else if (OB_FAIL(offline_build_tablet_without_memtable_())) {
    LOG_WARN("failed to offline build tablet without memtable", K(ret));
  } else if (OB_FAIL(offline_gc_tablet_for_create_or_transfer_in_abort_())) {
    LOG_WARN("failed to offline_gc_tablet_for_create_or_transfer_in_abort", K(ret));
  } else if (OB_FAIL(offline_destroy_memtable_and_mds_table_())) {
    LOG_WARN("failed to offline destroy memtable and mds table", K(ret));
  } else {
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
  if (OB_NOT_NULL(ls_) && !ls_->get_ls_id().is_sys_ls()) {
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
          ob_usleep(1_s);
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
    const int64_t ls_epoch = ls_->get_ls_epoch();
    const ObTabletID tablet_id = target_tablet.get_tablet_meta().tablet_id_;
    const ObTabletMapKey key(ls_id, tablet_id);
    ObTabletHandle cur_tablet_handle;
    int64_t last_gc_version = -1;
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
    } else if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_last_gc_version_for_tablet(ls_id, tablet_id, last_gc_version))) {
      LOG_WARN("failed to get last gc version for tablet", K(ret), K(key));
    } else {
      const ObMetaDiskAddr &tablet_addr = tablet_handle.get_obj()->get_tablet_addr();
      if (OB_FAIL(TENANT_STORAGE_META_SERVICE.remove_tablet(ls_id, ls_epoch, tablet_handle, last_gc_version))) {
        LOG_WARN("failed to write remove tablet slog", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(tablet_handle.get_obj()->wait_release_memtables())) {
        LOG_ERROR("failed to release memtables", K(ret), K(tablet_id));
      } else if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
        LOG_ERROR("failed to do remove tablet", K(ret), K(ls_id), K(tablet_id));
        ob_usleep(1_s);
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
  ObSArray<ObMetaDiskAddr> tablet_addrs;
  ObSArray<int64_t> last_gc_versions;

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
        } else if (OB_FAIL(tablet_addrs.push_back(tablet_addr))) {
          LOG_WARN("failed to push back tablet addr", K(ret), K(tablet_addr));
        }
      }

      // write slog and do remove tablet
      if (OB_FAIL(ret)) {
      } else if (tablet_ids.empty()) {
        LOG_INFO("all tablets already deleted, do nothing", K(ret), K(ls_id), K(tablet_id_array));
      } else if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->get_last_gc_versions_for_tablets(ls_id, tablet_ids, last_gc_versions))) {
        LOG_WARN("failed to get last gc versions for tablets", K(ret), K(ls_id), K(tablet_ids));
      } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.batch_remove_tablet(ls_id, ls_->get_ls_epoch(), tablet_ids, tablet_addrs, last_gc_versions))) {
        LOG_WARN("failed to remove tablets", K(ret), K(ls_id), K(tablet_ids));
      } else {
        time_guard.click("WrSlog");
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
          const ObTabletID &tablet_id = tablet_ids.at(i);
          if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
            LOG_ERROR("failed to do remove tablet", K(ret), K(ls_id), K(tablet_id));
            ob_usleep(1_s);
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
  ObTenantDirectLoadMgr *tenant_direct_load_mgr = MTL(ObTenantDirectLoadMgr *);
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
      if (REACH_TIME_INTERVAL(10_s)) {
        LOG_ERROR("failed to delete tablet from t3m", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  if (OB_SUCC(ret)) {
    if (OB_FAIL(tenant_direct_load_mgr->remove_tablet_direct_load(
        ObTabletDirectLoadMgrKey(tablet_id, ObDirectLoadType::DIRECT_LOAD_DDL)))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("remove tablet direct load failed", K(ret), K(ls_id), K(tablet_id));
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
  } else if (OB_FAIL(t3m->get_tablet_addr(key, addr))) {
    LOG_WARN("fail to get tablet addr", K(ret), K(key));
  }

  return ret;
}

void ObLSTabletService::report_tablet_to_rs(const common::ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls_->get_ls_id();

  if (tablet_id.is_ls_inner_tablet()) {
    // no need to report for ls inner tablet
  } else if (OB_FAIL(MTL(ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id))) {
    LOG_WARN("failed to report tablet info", KR(ret), K(ls_id), K(tablet_id));
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
    } else if (OB_FAIL(MTL(ObTabletTableUpdater*)->submit_tablet_update_task(ls_id, tablet_id))) {
      LOG_WARN("failed to report tablet info", KR(ret), K(tenant_id), K(ls_id), K(tablet_id));
    }
  }
}

int ObLSTabletService::table_scan(ObTabletHandle &tablet_handle, ObTableScanIterator &iter, ObTableScanParam &param)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_table_scan_begin);
  const bool is_ls_inner_tablet = tablet_handle.get_obj()->get_tablet_id().is_ls_inner_tablet();
  bool allow_to_read = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (FALSE_IT(allow_to_read_mgr_.load_allow_to_read_info(allow_to_read))) {
  } else if (!allow_to_read) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("ls is not allow to read", K(ret), KPC(ls_));
  } else if (!is_ls_inner_tablet
      && OB_FAIL(prepare_scan_table_param(param, *(MTL(ObTenantSchemaService*)->get_schema_service())))) {
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
    const ObUpdateTabletPointerParam &param,
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
  } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle, param))) {
    LOG_WARN("failed to add tablet to meta mem mgr", K(ret), K(key), K(tablet_handle), K(param));
  }

  return ret;
}

int ObLSTabletService::migrate_update_tablet(
    const uint64_t data_version,
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
  int32_t private_transfer_epoch = -1;
  int64_t tablet_meta_version = 0;
  if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (FALSE_IT(time_guard.click("GetTablet"))) {
  } else if (OB_ISNULL(old_tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect null old_tablet_hdl", K(ret), K(old_tablet_hdl));
  } else if (OB_FAIL(old_tablet_hdl.get_obj()->get_private_transfer_epoch(private_transfer_epoch))) {
    LOG_WARN("failed to get transfer epoch", K(ret), K(old_tablet_hdl));
  } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
    LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
  }
  const ObTabletPersisterParam param(data_version,
                                     ls_id,
                                     ls_->get_ls_epoch(),
                                     tablet_id, private_transfer_epoch,
                                     tablet_meta_version);
  const bool is_transfer = false;

  if (FAILEDx(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else if (FALSE_IT(new_tablet = tmp_tablet_hdl.get_obj())) {
  } else if (OB_FAIL(new_tablet->init_with_migrate_param(allocator, mig_tablet_param, true/*is_update*/, freezer, is_transfer, private_transfer_epoch))) {
    LOG_WARN("failed to init tablet", K(ret), K(mig_tablet_param));
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *new_tablet, new_tablet_hdl))) {
    LOG_WARN("fail to persist and transform tablet", K(ret), KPC(new_tablet), K(new_tablet_hdl));
  } else if (FALSE_IT(time_guard.click("Persist"))) {
  } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
  } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_hdl, new_tablet_hdl, time_guard))) {
    LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
  } else if (OB_FAIL(new_tablet_hdl.get_obj()->start_direct_load_task_if_need())) {
    LOG_WARN("start ddl if need failed", K(ret));
  } else {
    LOG_INFO("migrate update tablet succeed", K(ret), K(key), K(disk_addr));
  }

  return ret;
}

int ObLSTabletService::migrate_create_tablet(
    const uint64_t data_version,
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
  ObMetaDiskAddr disk_addr;
  int32_t private_transfer_epoch = -1;
  int64_t tablet_meta_version = 0;
  ObTransService *tx_svr = MTL(ObTransService*);
  const bool is_transfer = false;
  handle.reset();
  /// NOTE: for SS mode, just use mig_tablet_param's transfer epoch here
  if (OB_FAIL(mig_tablet_param.transfer_info_.get_private_transfer_epoch(private_transfer_epoch))) {
    LOG_WARN("failed to get transfer epoch", K(ret), K(key), K(mig_tablet_param.transfer_info_));
  } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
    LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to create temporary tablet", K(ret), K(key));
  } else if (OB_FAIL(tmp_tablet_hdl.get_obj()->init_with_migrate_param(allocator, mig_tablet_param, false/*is_update*/, freezer, is_transfer, private_transfer_epoch))) {
    LOG_WARN("fail to init tablet", K(ret), K(mig_tablet_param));
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else {
    const ObTabletPersisterParam param(data_version,
                                    ls_id,
                                    ls_->get_ls_epoch(),
                                    tablet_id,
                                    private_transfer_epoch,
                                    tablet_meta_version);
    if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet_hdl.get_obj(), handle))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), K(tmp_tablet_hdl), K(handle));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = handle.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(safe_create_cas_tablet(ls_id, tablet_id, disk_addr, handle, time_guard))) {
      LOG_WARN("fail to create tablet and cas", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
    } else if (OB_FAIL(handle.get_obj()->start_direct_load_task_if_need())) {
      LOG_WARN("start ddl if need failed", K(ret));
    } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
      LOG_WARN("fail to create tablet cache", K(ret), K(key), K(handle));
    } else {
      LOG_INFO("migrate create tablet succeed", K(ret), K(key), K(disk_addr));
    }
  }

  if (OB_FAIL(ret)) {
    int tmp_ret = OB_SUCCESS;
    handle.reset();
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

int ObLSTabletService::update_tablet_table_store(
    const ObTabletHandle &old_tablet_handle,
    const ObIArray<storage::ObITable *> &tables)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "UpTabStore"));
  ObTabletHandle new_tablet_hdl;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet svr hasn't been inited", K(ret));
  } else if (OB_UNLIKELY(!old_tablet_handle.is_valid() || 0 == tables.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("old tablet handle is invalid", K(ret), K(old_tablet_handle), K(tables.count()));
  } else {
    ObTablet *old_tablet = old_tablet_handle.get_obj();
    const common::ObTabletID &tablet_id = old_tablet->get_tablet_meta().tablet_id_;
    uint64_t data_version = 0;
    ObTimeGuard time_guard("ObLSTabletService::ReplaceSSTable", 1_s);
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");

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
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
      LOG_WARN("fail to get min data version", K(ret));
    } else {
      time_guard.click("GetTablet");
      ObTabletHandle tmp_tablet_hdl;
      ObTablet *tmp_tablet = nullptr;
      const share::ObLSID &ls_id = ls_->get_ls_id();
      const ObTabletMapKey key(ls_id, tablet_id);
      ObMetaDiskAddr disk_addr;
      int32_t private_transfer_epoch = -1;
      int64_t tablet_meta_version = 0;
      if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
        LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
      } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
        LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
      }
      const ObTabletPersisterParam param(data_version,
                                         ls_id,
                                         ls_->get_ls_epoch(),
                                         tablet_id,
                                         private_transfer_epoch,
                                         tablet_meta_version);
      if (FAILEDx(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
        LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
      } else if (FALSE_IT(tmp_tablet = tmp_tablet_hdl.get_obj())) {
      } else if (OB_FAIL(tmp_tablet->init_for_defragment(allocator, tables, *old_tablet))) {
        LOG_WARN("fail to init new tablet", K(ret), KPC(old_tablet));
      } else if (FALSE_IT(time_guard.click("InitTablet"))) {
      } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet, new_tablet_hdl))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
      } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_handle, new_tablet_hdl, time_guard))) {
        LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
      } else {
        LOG_INFO("succeeded to build new tablet", K(ret), K(disk_addr),
            K(new_tablet_hdl), KPC(new_tablet_hdl.get_obj()));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObServerAutoSplitScheduler &auto_split_scheduler = ObServerAutoSplitScheduler::get_instance();
    if (OB_TMP_FAIL(auto_split_scheduler.push_task(new_tablet_hdl, *ls_))) {
      LOG_WARN("fail to push auto split task", K(tmp_ret));
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_table_store(
    const common::ObTabletID &tablet_id,
    const ObUpdateTableStoreParam &param,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("UpdateTmpTablet", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  if (share::is_reserve_mode()) {
    // TODO(@DanLing) use LocalArena later
    allocator.set_ctx_id(ObCtxIds::MERGE_RESERVE_CTX_ID);
  }

  const share::ObLSID &ls_id = ls_->get_ls_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  uint64_t data_version = 0;
  ObTabletHandle old_tablet_hdl;
  ObTabletHandle tmp_tablet_hdl;
  ObTabletHandle new_tablet_hdl;
  ObTimeGuard time_guard("ObLSTabletService::UpdateTableStore", 1_s);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(param));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
    } else {
      LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
    }
  } else {
    ObTablet *tmp_tablet = tmp_tablet_hdl.get_obj();
    time_guard.click("Acquire");
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");
    if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    } else if (old_tablet_hdl.get_obj()->is_empty_shell()) {
      if (OB_FAIL(handle.assign(old_tablet_hdl))) {
        LOG_WARN("failed to assign tablet_handle", K(ret), K(old_tablet_hdl));
      }
      LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_hdl.get_obj());
    } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
      LOG_WARN("fail to get min data version", K(ret));
    } else {
      time_guard.click("GetTablet");
      ObTablet *old_tablet = old_tablet_hdl.get_obj();
      ObMetaDiskAddr disk_addr;
      int32_t private_transfer_epoch = -1;
      int64_t tablet_meta_version = 0;
      if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
        LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
      } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
        LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
      }
      const ObTabletPersisterParam persist_param(data_version,
                                                 ls_id,
                                                 ls_->get_ls_epoch(),
                                                 tablet_id,
                                                 private_transfer_epoch,
                                                 tablet_meta_version);
      const share::SCN &tablet_pointer_ss_change_version =
        param.tablet_ss_change_fully_applied_ ? param.update_tablet_ss_change_version_ : share::SCN::invalid_scn();
      // ss attached mds minor end scn may excceed tablet's mds_checkpoint_scn
      const SCN &mds_flush_scn = GCTX.is_shared_storage_mode() ? param.get_clog_checkpoint_scn() : old_tablet->tablet_meta_.mds_checkpoint_scn_;
      if (OB_FAIL(ret)) {
      } else if (!is_mds_merge(param.compaction_info_.merge_type_) && OB_FAIL(tmp_tablet->init_for_merge(allocator, param, *old_tablet))) {
        LOG_WARN("failed to init tablet", K(ret), K(param), KPC(old_tablet));
      } else if (is_mds_merge(param.compaction_info_.merge_type_) && OB_FAIL(tmp_tablet->init_with_mds_sstable(allocator, *old_tablet, mds_flush_scn, param))) {
        LOG_WARN("failed to init tablet with mds", K(ret), K(param), KPC(old_tablet));
      } else if (FALSE_IT(time_guard.click("InitNew"))) {
      } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(persist_param, *tmp_tablet, new_tablet_hdl))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
      } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_hdl, new_tablet_hdl, time_guard, tablet_pointer_ss_change_version))) {
        LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
      } else if (OB_FAIL(handle.assign(new_tablet_hdl))) {
        LOG_WARN("failed to assign tablet_handle", K(ret), K(old_tablet_hdl));
      } else {
        LOG_INFO("succeeded to build new tablet", K(ret), K(key), K(disk_addr), K(param), K(handle));
      }
    }
  }
  if (OB_SUCC(ret)) {
    int tmp_ret = OB_SUCCESS;
    ObServerAutoSplitScheduler &auto_split_scheduler= ObServerAutoSplitScheduler::get_instance();
    if (OB_TMP_FAIL(auto_split_scheduler.push_task(new_tablet_hdl, *ls_))) {
      LOG_WARN("fail to push auto split task", K(tmp_ret));
    }
  }

  return ret;
}

int ObLSTabletService::update_tablet_to_empty_shell(
    const uint64_t data_version,
    const common::ObTabletID &tablet_id)
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
    int32_t private_transfer_epoch = -1;
    int64_t tablet_meta_version = 0;
    if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
    } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    }
    const ObTabletPersisterParam param(data_version,
                                       ls_id,
                                       ls_->get_ls_epoch(),
                                       tablet_id,
                                       private_transfer_epoch,
                                       tablet_meta_version);
    if (FAILEDx(tmp_tablet->init_empty_shell(allocator, *old_tablet))) {
      LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
    } else if (FALSE_IT(time_guard.click("InitNew"))) {
    } else if (OB_FAIL(ObTabletPersister::transform_empty_shell(param, *tmp_tablet, new_tablet_handle))) {
      LOG_WARN("fail to transform emtpy shell", K(ret), K(tablet_id));
    } else if (FALSE_IT(time_guard.click("Transform"))) {
    } else {
      if (GCTX.is_shared_storage_mode()) {
        disk_addr = new_tablet_handle.get_obj()->tablet_addr_;
        if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_handle, new_tablet_handle, time_guard))) {
          LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr), K(data_version));
        }
      } else {
        if (OB_FAIL(safe_update_cas_empty_shell(data_version, key, old_tablet_handle, new_tablet_handle, time_guard))) {
          LOG_WARN("fail to cas empty shell", K(ret), K(key), K(old_tablet_handle), K(new_tablet_handle), K(data_version));
        }
      }
    }
    if (OB_SUCC(ret)) {
      ls_->get_tablet_gc_handler()->set_tablet_gc_trigger();
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
  uint64_t data_version = 0;
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
    if (OB_FAIL(handle.assign(old_tablet_handle))) {
      LOG_WARN("failed to assign tablet_handle", K(ret), K(old_tablet_handle));
    }
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_handle.get_obj());
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("GetTablet");
    ObTabletHandle tmp_tablet_hdl;
    ObTabletHandle new_tablet_hdl;
    ObTablet *tmp_tablet = nullptr;
    ObTablet *old_tablet = old_tablet_handle.get_obj();
    const share::ObLSID &ls_id = ls_->get_ls_id();
    const ObTabletMapKey key(ls_id, tablet_id);
    ObMetaDiskAddr disk_addr;
    int32_t private_transfer_epoch = -1;
    int64_t tablet_meta_version = 0;
    if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
    } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    }
    const ObTabletPersisterParam param(data_version,
                                       ls_id,
                                       ls_->get_ls_epoch(),
                                       tablet_id,
                                       private_transfer_epoch,
                                       tablet_meta_version);
    if (FAILEDx(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      } else {
        LOG_WARN("failed to acquire tablet", K(ret), K(key));
      }
    } else if (FALSE_IT(tmp_tablet = tmp_tablet_hdl.get_obj())) {
    } else if (OB_FAIL(tmp_tablet->init_with_update_medium_info(allocator, *old_tablet, true/*clear_wait_check_flag*/))) {
      LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
    } else if (FALSE_IT(time_guard.click("InitNew"))) {
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet, new_tablet_hdl))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
    } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_handle, new_tablet_hdl, time_guard))) {
      LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
    } else if (OB_FAIL(handle.assign(new_tablet_hdl))) {
      LOG_WARN("failed to assign tablet_handle", K(ret), K(new_tablet_hdl));
    }
  }
  return ret;
}

int ObLSTabletService::build_new_tablet_from_mds_table(
    compaction::ObTabletMergeCtx &ctx,
    const common::ObTabletID &tablet_id,
    const ObTableHandleV2 &mds_mini_sstable_handle,
    const share::SCN &flush_scn,
    ObTabletHandle &handle)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "BuildMSD"));
  const share::ObLSID &ls_id = ls_->get_ls_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle old_tablet_hdl;
  ObTabletHandle tablet_for_mds_dump_handle;
  ObTabletHandle tmp_tablet_hdl;
  ObTabletHandle new_tablet_handle;
  const blocksstable::ObSSTable *mds_sstable = nullptr;
  ObTimeGuard time_guard("ObLSTabletService::build_new_tablet_from_mds_table_with_mini", 30_ms);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
    } else {
      LOG_WARN("failed to acquire tablet", K(ret), K(key));
    }
  } else {
    uint64_t data_version = 0;
    time_guard.click("Acquire");
    if (OB_FAIL(direct_get_tablet(tablet_id, tablet_for_mds_dump_handle))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    } else if (OB_ISNULL(tablet_for_mds_dump_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    } else if (tablet_for_mds_dump_handle.get_obj()->is_empty_shell()) {
      if (OB_FAIL(handle.assign(tablet_for_mds_dump_handle))) {
        LOG_WARN("failed to assign tablet_handle", K(ret), K(tablet_for_mds_dump_handle));
      }
      LOG_INFO("mds tablet is empty shell tablet, should skip mds table dump operation", K(ret),
          "mds tablet", *tablet_for_mds_dump_handle.get_obj());
    } else if (OB_FAIL(mds_mini_sstable_handle.get_sstable(mds_sstable))) {
      LOG_WARN("fail to get sstable from mds mini handle", K(ret), K(mds_mini_sstable_handle));
    } else if (OB_UNLIKELY(!mds_sstable->is_mds_sstable())) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid sstable type", K(ret), K(*mds_sstable));
    } else if (mds_sstable->get_filled_tx_scn().is_min() || mds_sstable->get_filled_tx_scn().is_max()) {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid mds filled tx scn", K(ret), K(*mds_sstable));
    } else {
      ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
      time_guard.click("Lock");
      if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (OB_ISNULL(old_tablet_hdl.get_obj())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
      } else if (old_tablet_hdl.get_obj()->is_empty_shell()) {
        if (OB_FAIL(handle.assign(old_tablet_hdl))) {
          LOG_WARN("failed to assign tablet_handle", K(ret), K(tablet_for_mds_dump_handle));
        }
        LOG_INFO("old tablet is empty shell tablet, should skip mds table dump operation", K(ret),
            "old tablet", *old_tablet_hdl.get_obj());
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("fail to get min data version", K(ret));
      } else {
        time_guard.click("GetOldTablet");
        ObTablet *old_tablet = old_tablet_hdl.get_obj();
        ObTablet *tmp_tablet = tmp_tablet_hdl.get_obj();
        ObMetaDiskAddr disk_addr;
        int32_t private_transfer_epoch = -1;
        int64_t tablet_meta_version = 0;
        if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
          LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
        } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
          LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
        }
        const ObTabletPersisterParam param(data_version,
                                           ls_id,
                                           ls_->get_ls_epoch(),
                                           tablet_id,
                                           private_transfer_epoch,
                                           tablet_meta_version);
        ObUpdateTableStoreParam mds_param(ctx.static_param_.version_range_.snapshot_version_,
                                          1/*multi_version_start*/,
                                          ObMdsSchemaHelper::get_instance().get_storage_schema(),
                                          ctx.get_ls_rebuild_seq(),
                                          mds_sstable,
                                          false/*allow_duplicate_sstable*/);
        if (FAILEDx(mds_param.init_with_compaction_info(
          ObCompactionTableStoreParam(ctx.get_merge_type(), mds_sstable->get_end_scn()/*clog_checkpoint_scn*/, false/*need_report*/, false/*has_truncate_info*/)))) {
          LOG_WARN("failed to init with compaction info", KR(ret));
        } else if (OB_FAIL(tmp_tablet->init_with_mds_sstable(allocator, *old_tablet, flush_scn, mds_param))) {
          LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet), K(flush_scn), KPC(mds_sstable));
        } else if (FALSE_IT(time_guard.click("InitTablet"))) {
        } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet, new_tablet_handle))) {
          LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_handle));
        } else if (FALSE_IT(time_guard.click("Persist"))) {
        } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
        } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_hdl, new_tablet_handle, time_guard))) {
          LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
        } else if (OB_FAIL(handle.assign(new_tablet_handle))) {
          LOG_WARN("failed to assign tablet_handle", K(ret), K(new_tablet_handle));
        } else {
          time_guard.click("SafeCAS");
          LOG_INFO("succeeded to build new tablet with mds mini sstable",
              K(ret), K(key), K(disk_addr), K(new_tablet_handle), K(flush_scn), KP(mds_sstable));
        }
      }
    }
  }

  return ret;
}

int ObLSTabletService::update_tablet_release_memtable_for_offline(
    const common::ObTabletID &tablet_id,
    const SCN scn)
{
  int ret = OB_SUCCESS;
  const ObLSID ls_id(ls_->get_ls_id());
  const ObTabletMapKey key(ls_id, tablet_id);
  uint64_t data_version = 0;
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTimeGuard time_guard("ObLSTabletService::update_tablet_release_memtable", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || !scn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(scn));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, tablet_handle))) {
    LOG_WARN("fail to direct get tablet", K(ret), K(key));
  } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet should not be NULL", K(ret), K(key));
  } else if (tablet->is_empty_shell()) {
    //do nothing
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("get_tablet");
    ObITable *table = nullptr;
    ObTableStoreIterator iter;
    const bool is_from_buf_pool = nullptr == tablet_handle.get_obj()->get_allocator();
    if (is_from_buf_pool) {
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
      ObTabletHandle new_tablet_handle;
      ObUpdateTabletPointerParam param;
      int32_t private_transfer_epoch = -1;
      int64_t tablet_meta_version = 0;
      if (OB_FAIL(tablet->get_private_transfer_epoch(private_transfer_epoch))) {
        LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", tablet->get_tablet_meta());
      } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
        LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
      }
      const ObTabletPersisterParam persist_param(data_version,
                                                 ls_id,
                                                 ls_->get_ls_epoch(),
                                                 tablet_id,
                                                 private_transfer_epoch,
                                                 tablet_meta_version);
      if (FAILEDx(ObTabletPersister::copy_from_old_tablet(persist_param, *tablet, new_tablet_handle))) {
        LOG_WARN("fail to copy from old tablet", K(ret), KPC(tablet));
      } else if (FALSE_IT(time_guard.click("CpTablet"))) {
      } else if (OB_FAIL(new_tablet_handle.get_obj()->rebuild_memtables(scn))) {
        LOG_WARN("fail to rebuild memtables", K(ret), K(scn), K(new_tablet_handle));
      } else if (OB_FAIL(new_tablet_handle.get_obj()->get_updating_tablet_pointer_param(param,
              false/*update tablet attr*/))) {
        LOG_WARN("fail to get updating tablet pointer parameters", K(ret));
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, new_tablet_handle, param))) {
        LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(new_tablet_handle), K(param));
      } else {
        time_guard.click("CASwap");
        LOG_INFO("succeeded to copy tablet to release memtable", K(ret), K(key), K(tablet_handle), K(new_tablet_handle));
      }
    } else if (OB_UNLIKELY(!tablet->get_tablet_addr().is_memory())) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("This tablet is full tablet, but its addr isn't memory", K(ret), KPC(tablet));
    } else if (OB_FAIL(tablet->get_all_sstables(iter))) {
      LOG_WARN("fail to get all sstable", K(ret), K(iter));
    } else if (1 == iter.count() && OB_FAIL(iter.get_next(table))) {
      LOG_WARN("fail to get next table", K(ret), K(iter));
    } else if (OB_UNLIKELY(iter.count() > 1)
               || (OB_NOT_NULL(table) && (!table->is_sstable()
                                          || static_cast<ObSSTable *>(table)->get_data_macro_block_count() != 0))) {
      ret = OB_NOT_SUPPORTED;
      LOG_ERROR("This tablet is full tablet, but all of its sstables isn't only one empty major",
          K(ret), K(iter), KPC(table));
    } else if (OB_FAIL(tablet_handle.get_obj()->wait_release_memtables())) {
      LOG_ERROR("failed to release memtables", K(ret), K(tablet_id));
    } else if (OB_FAIL(inner_remove_tablet(ls_id, tablet_id))) {
      LOG_ERROR("failed to do remove tablet", K(ret), K(ls_id), K(tablet_id));
    } else {
      time_guard.click("RmTablet");
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_report_status(
    const common::ObTabletID &tablet_id,
    const bool found_column_group_checksum_error)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  uint64_t data_version = 0;
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
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("GetTablet");
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletHandle new_tablet_handle;
    bool need_report = true;

    if (OB_UNLIKELY(found_column_group_checksum_error)) {
      tablet->tablet_meta_.report_status_.found_cg_checksum_error_ = true;
    } else if (tablet->tablet_meta_.report_status_.need_report()) {
      tablet->tablet_meta_.report_status_.cur_report_version_ = tablet->tablet_meta_.report_status_.merge_snapshot_version_;
    } else {
      need_report = false;
      FLOG_INFO("tablet doesn't need to report", K(ret), K(tablet_id));
    }

    if (need_report) {
      int32_t private_transfer_epoch = -1;
      int64_t tablet_meta_version = 0;
      if (OB_FAIL(tablet->get_private_transfer_epoch(private_transfer_epoch))) {
        LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", tablet->get_tablet_meta());
      } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
        LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
      }
      const ObTabletPersisterParam param(data_version,
                                         ls_->get_ls_id(),
                                         ls_->get_ls_epoch(),
                                         tablet_id,
                                         private_transfer_epoch,
                                         tablet_meta_version);
      if (FAILEDx(ObTabletPersister::persist_and_transform_tablet(param, *tablet, new_tablet_handle))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tablet), K(new_tablet_handle));
      } else if (FALSE_IT(time_guard.click("Persist"))) {
      } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, tablet_handle, new_tablet_handle, time_guard))) {
        LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
      } else {
        LOG_INFO("succeeded to build new tablet", K(ret), K(key), K(disk_addr), K(tablet_handle));
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_snapshot_version(
    const common::ObTabletID &tablet_id,
    const int64_t snapshot_version)
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "UTabletSnapVer"));
  ObTabletHandle old_tablet_handle;
  uint64_t data_version = 0;
  ObTimeGuard time_guard("ObLSTabletService::update_tablet_snapshot_version", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || 0 >= snapshot_version)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(snapshot_version));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_handle))) {
    LOG_WARN("failed to check and get tablet", K(ret), K(tablet_id));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("GetTablet");

    ObTabletHandle tmp_tablet_hdl;
    ObTabletHandle new_tablet_hdl;
    ObTablet *tmp_tablet = nullptr;
    ObTablet *old_tablet = old_tablet_handle.get_obj();
    const share::ObLSID &ls_id = ls_->get_ls_id();
    const ObTabletMapKey key(ls_id, tablet_id);
    ObMetaDiskAddr disk_addr;
    int32_t private_transfer_epoch = -1;
    int64_t tablet_meta_version = 0;
    if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
    } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    }
    const ObTabletPersisterParam param(data_version,
                                       ls_id,
                                       ls_->get_ls_epoch(),
                                       tablet_id,
                                       private_transfer_epoch,
                                       tablet_meta_version);
    ObTabletDataStatus::STATUS current_status = ObTabletDataStatus::DATA_STATUS_MAX;

    if (FAILEDx(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      } else {
        LOG_WARN("failed to acquire tablet", K(ret), K(key));
      }
    } else if (FALSE_IT(tmp_tablet = tmp_tablet_hdl.get_obj())) {
    } else if (OB_FAIL(old_tablet->tablet_meta_.ha_status_.get_data_status(current_status))) {
      LOG_WARN("failed to get data status", K(ret), KPC(old_tablet));
    } else if (OB_FAIL(tmp_tablet->init_with_replace_members(allocator, *old_tablet, snapshot_version, current_status))) {
      LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
    } else if (FALSE_IT(time_guard.click("InitNew"))) {
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet, new_tablet_hdl))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_hdl));
    } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_handle, new_tablet_hdl, time_guard))) {
      LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
    }
  }
  return ret;
}

int ObLSTabletService::ObUpdateRestoreStatus::modify_tablet_meta(ObTabletMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta.ha_status_.set_restore_status(restore_status_))) {
    LOG_WARN("failed to set restore status", K(ret), K(restore_status_), K(meta));
  }  else if (need_reset_transfer_flag_
      && OB_FALSE_IT((void)meta.reset_transfer_table())) {
  } else if (need_to_set_split_data_complete_
      && OB_FALSE_IT((void)meta.split_info_.set_data_incomplete(false))) {
  }
  return ret;
}

int ObLSTabletService::update_tablet_restore_status(
    const share::SCN &reorg_scn,
    const common::ObTabletID &tablet_id,
    const ObTabletRestoreStatus::STATUS &restore_status,
    const bool need_reset_transfer_flag,
    const bool need_to_set_split_data_complete)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletRestoreStatus::STATUS current_status = ObTabletRestoreStatus::RESTORE_STATUS_MAX;
  bool can_change = false;
  uint64_t data_version = 0;
  ObTimeGuard time_guard("ObLSTabletService::update_tablet_restore_status", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!reorg_scn.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!ObTabletRestoreStatus::is_valid(restore_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(reorg_scn), K(tablet_id), K(restore_status));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle.get_obj());
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("GetTablet");
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletHandle new_tablet_handle;
    const bool current_has_transfer_table = tablet->tablet_meta_.has_transfer_table();
    const bool current_split_data_incomplete_status = tablet->tablet_meta_.split_info_.is_data_incomplete();
    ObUpdateRestoreStatus modifier(restore_status, need_reset_transfer_flag, need_to_set_split_data_complete);

    if (OB_FAIL(tablet->tablet_meta_.ha_status_.get_restore_status(current_status))) {
      LOG_WARN("failed to get restore status", K(ret), KPC(tablet));
    } else if (OB_FAIL(ObTabletRestoreStatus::check_can_change_status(current_status, restore_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(current_status), K(restore_status), KPC(tablet));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change restore status", K(ret), K(current_status), K(restore_status), KPC(tablet));
    } else {
      // TODO(jiahua.cjh) move check valid to tablet init after generate new version tablet.
      int32_t private_transfer_epoch = -1;
      int64_t tablet_meta_version = 0;
      if (OB_FAIL(tablet->get_private_transfer_epoch(private_transfer_epoch))) {
        LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", tablet->get_tablet_meta());
      } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
        LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
      }
      const ObTabletPersisterParam param(data_version,
                                         ls_->get_ls_id(),
                                         ls_->get_ls_epoch(),
                                         tablet_id,
                                         private_transfer_epoch,
                                         tablet_meta_version);
      if (FAILEDx(tablet->check_valid())) {
        LOG_WARN("failed to check tablet valid", K(ret), K(restore_status), KPC(tablet));
      } else if (OB_FAIL(ObTabletPersister::persist_and_transform_only_tablet_meta(reorg_scn, param, *tablet, modifier, new_tablet_handle))) {
        LOG_WARN("fail to persist and transform tablet", K(ret), K(tablet), K(new_tablet_handle), K(restore_status));
      } else if (FALSE_IT(time_guard.click("Persist"))) {
      } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
      } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, tablet_handle, new_tablet_handle, time_guard))) {
        LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
      } else {
        LOG_INFO("succeeded to build new tablet", K(ret), K(key), K(disk_addr), K(restore_status), K(need_reset_transfer_flag), K(tablet_handle));
#ifdef ERRSIM
        SERVER_EVENT_SYNC_ADD("storage_ha", "update_tablet_restore_status",
                              "tablet_id", tablet_id.id(),
                              "old_restore_status", current_status,
                              "new_restore_status", restore_status);
#endif
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_tablet_ha_data_status(
    const share::SCN &reorg_scn,
    const common::ObTabletID &tablet_id,
    const ObTabletDataStatus::STATUS &data_status)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletDataStatus::STATUS current_status = ObTabletDataStatus::DATA_STATUS_MAX;
  bool can_change = false;
  uint64_t data_version = 0;
  ObTimeGuard time_guard("ObLSTabletService::update_tablet_ha_data_status", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!reorg_scn.is_valid())
      || OB_UNLIKELY(!tablet_id.is_valid())
      || OB_UNLIKELY(!ObTabletDataStatus::is_valid(data_status))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(reorg_scn), K(tablet_id), K(data_status));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
  } else if (tablet_handle.get_obj()->is_empty_shell()) {
    LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", tablet_handle.get_obj());
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("GetTablet");
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *old_tablet = tablet_handle.get_obj();
    ObTablet *tmp_tablet = nullptr;
    common::ObArenaAllocator allocator("UpdateSchema", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
    ObTabletHandle new_tablet_handle;
    ObTabletHandle tmp_tablet_handle;
    int32_t private_transfer_epoch = -1;
    int64_t tablet_meta_version = 0;
    if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
    } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    }
    const ObTabletPersisterParam param(data_version,
                                       ls_->get_ls_id(),
                                       ls_->get_ls_epoch(),
                                       tablet_id,
                                       private_transfer_epoch,
                                       tablet_meta_version);
    bool is_row_store_with_co_major = false;

    if (OB_FAIL(ret)) {
    } else if (old_tablet->get_reorganization_scn() != reorg_scn) {
      ret = OB_TABLET_REORG_SCN_NOT_MATCH;
      LOG_WARN("tablet reorg scn is not same, cannot update", K(ret), K(reorg_scn), KPC(old_tablet));
    } else if (OB_FAIL(old_tablet->tablet_meta_.ha_status_.get_data_status(current_status))) {
      LOG_WARN("failed to get data status", K(ret), KPC(old_tablet));
    } else if (OB_FAIL(ObTabletDataStatus::check_can_change_status(current_status, data_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(current_status), K(data_status), KPC(old_tablet));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change data status", K(ret), K(current_status), K(data_status), KPC(old_tablet));
    } else if (current_status == data_status) {
      LOG_INFO("data status is same, skip update", K(tablet_id), K(current_status), K(data_status));
    } else if (ObTabletDataStatus::is_complete(data_status) // may reuse exist co major in cs replica when rebuild, but tablet is row store like src
               && OB_FAIL(old_tablet->check_row_store_with_co_major(is_row_store_with_co_major))) {
      LOG_WARN("failed to check row store with co major", K(ret), KPC(old_tablet));
    } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_handle))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      } else {
        LOG_WARN("failed to acquire tablet", K(ret), K(key));
      }
    } else if (FALSE_IT(tmp_tablet = tmp_tablet_handle.get_obj())) {
    // need update tablet to column store with column store storage schema when rebuild reuse old co major in cs replica
    } else if (OB_FAIL(tmp_tablet->init_with_replace_members(allocator, *old_tablet, old_tablet->tablet_meta_.snapshot_version_, data_status, is_row_store_with_co_major))) {
      LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
    } else if (FALSE_IT(time_guard.click("InitNew"))) {
    } else if (OB_FAIL(tmp_tablet->check_valid())) {
      LOG_WARN("failed to check tablet valid", K(ret), K(data_status), KPC(tmp_tablet));
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet, new_tablet_handle))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_handle));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, tablet_handle, new_tablet_handle, time_guard))) {
      LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
    } else {
      LOG_INFO("succeeded to update tablet ha data status", K(ret), K(key), K(disk_addr), K(data_status), K(tablet_handle), K(tmp_tablet_handle), K(is_row_store_with_co_major), K(time_guard));
    }
  }
  return ret;
}

int ObLSTabletService::ObUpdateHAExpectedStatus::modify_tablet_meta(ObTabletMeta &meta)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(meta.ha_status_.set_expected_status(expected_status_))) {
    LOG_WARN("failed to set ha expected status", K(ret), K(expected_status_));
  }
  return ret;
}

int ObLSTabletService::update_tablet_ha_expected_status(
    const share::SCN &reorg_scn,
    const common::ObTabletID &tablet_id,
    const ObTabletExpectedStatus::STATUS &expected_status)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletExpectedStatus::STATUS current_status = ObTabletExpectedStatus::EXPECTED_STATUS_MAX;
  bool can_change = false;
  uint64_t data_version = 0;
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
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("GetTablet");
    ObMetaDiskAddr disk_addr;
    const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
    ObTablet *tablet = tablet_handle.get_obj();
    ObTabletHandle new_tablet_handle;
    int32_t private_transfer_epoch = -1;
    int64_t tablet_meta_version = 0;
    if (OB_FAIL(tablet->get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", tablet->get_tablet_meta());
    } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    }
    const ObTabletPersisterParam param(data_version,
                                       ls_->get_ls_id(),
                                       ls_->get_ls_epoch(),
                                       tablet_id,
                                       private_transfer_epoch,
                                       tablet_meta_version);
    ObUpdateHAExpectedStatus modifier(expected_status);
    if (FAILEDx(tablet->tablet_meta_.ha_status_.get_expected_status(current_status))) {
      LOG_WARN("failed to get data status", K(ret), KPC(tablet));
    } else if (expected_status == current_status) {
      LOG_INFO("tablet ha expected status is same, no need update", K(tablet_id),
          K(current_status), K(expected_status));
    } else if (OB_FAIL(ObTabletExpectedStatus::check_can_change_status(current_status, expected_status, can_change))) {
      LOG_WARN("failed to check can change status", K(ret), K(current_status), K(expected_status), KPC(tablet));
    } else if (!can_change) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("can not change meta status", K(ret), K(current_status), K(expected_status), KPC(tablet));
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_only_tablet_meta(reorg_scn, param, *tablet, modifier, new_tablet_handle))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tablet), K(new_tablet_handle), K(expected_status));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, tablet_handle, new_tablet_handle, time_guard))) {
      LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
    } else {
      LOG_INFO("succeeded to update tablet meta status", K(ret), K(key), K(disk_addr), K(expected_status), KPC(tablet));
    }
  }
  return ret;
}

int ObLSTabletService::update_private_tablet_last_match_meta_version_without_lock(
    const common::ObTabletID &tablet_id,
    ObTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  const ObLSID &ls_id = ls_->get_ls_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  int64_t ls_epoch = 0;
  ObTabletHandle tablet_handle;
  ObUpdateTabletPointerParam update_param;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  if (OB_UNLIKELY(!tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet id", K(ret), K(tablet_id));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (FALSE_IT(time_guard.click("GetTablet"))) {
  } else if (OB_FAIL(tablet_handle.get_obj()->get_updating_tablet_pointer_param_for_meta_version(update_param))) {
    LOG_WARN("failed to get tablet updating param for meta version", K(ret), K(key));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_ls_epoch(ls_epoch))) {
    LOG_WARN("failed to get ls epoch", K(ret), K(key));
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.update_tablet(key.ls_id_, ls_epoch, key.tablet_id_, update_param))) {
    LOG_WARN("failed to write update tablet slog", K(ret), K(key), K(update_param));
  } else if (FALSE_IT(time_guard.click("WriteSlog"))) {
  } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle, update_param))) {
    LOG_WARN("fail to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(update_param));
    ob_usleep(1_s);
    ob_abort();
  } else {
    time_guard.click("CASwap");
  }
  FLOG_INFO("finish update private tablet last match meta version", K(ret), K(key), K(update_param));
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObLSTabletService::get_pending_upload_tablet_id_arr(
    const SCN &ls_ss_checkpoint_scn,
    ObIArray<ObTabletID> &tablet_id_arr)
{
  int ret = OB_SUCCESS;
  tablet_id_arr.reuse();

  bool locked = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    const ObLSID &ls_id = ls_->get_ls_id();
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    ObTabletMapKey key;
    ObSArray<ObTabletID> tablet_ids;
    GetAllTabletIDOperator op(tablet_ids);
    if (OB_FAIL(tablet_id_set_.foreach(op))) {
      STORAGE_LOG(WARN, "fail to get all tablet ids from set", K(ret));
    } else {
      key.ls_id_ = ls_id;
      bool need_upload;
      for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
        key.tablet_id_ = tablet_ids.at(i);
        need_upload = false;
        if (OB_FAIL(t3m->check_tablet_has_sstable_need_upload(ls_ss_checkpoint_scn, key, need_upload))) {
          LOG_WARN("fail to check tablet need upload or not", K(ret), K(ls_ss_checkpoint_scn), K(key));
        } else if (!need_upload) {
          // do nothing
        } else if (OB_FAIL(tablet_id_arr.push_back(key.tablet_id_))) {
          LOG_WARN("fail to push back", K(ret), K(tablet_id_arr), K(key));
        }
      } // end for
    }
  }

  return ret;
}

int ObLSTabletService::write_tablet_id_set_to_pending_free()
{
  int ret = OB_SUCCESS;

  bool locked = false;
  while (OB_SUCC(ret) && !locked) {
    common::ObBucketTryRLockAllGuard lock_guard(bucket_lock_);
    if (OB_FAIL(lock_guard.get_ret()) && OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "fail to lock all tablet id set", K(ret));
    } else if (OB_EAGAIN == ret) {
      // try again after 1ms sleep.
      ob_usleep(1000);
      ret = OB_SUCCESS;
    } else {
      // get lock successfully
      locked = true;

      const ObLSID &ls_id = ls_->get_ls_id();
      ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
      ObTabletMapKey key;
      ObSArray<ObTabletID> tablet_ids;
      ObSArray<ObMetaDiskAddr> tablet_addrs;
      ObSArray<int64_t> last_gc_versions;
      GetAllTabletIDOperator op(tablet_ids);
      if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        LOG_WARN("not inited", K(ret), K_(is_inited));
      } else if (OB_FAIL(tablet_id_set_.foreach(op))) {
        STORAGE_LOG(WARN, "fail to get all tablet ids from set", K(ret));
      } else {
        key.ls_id_ = ls_id;
        if (OB_FAIL(tablet_addrs.reserve(tablet_ids.count()))) {
          LOG_WARN("failed to reserve tablet_addrs", K(ret));
        }
        ObMetaDiskAddr addr;
        for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
          key.tablet_id_ = tablet_ids.at(i);
          if (OB_FAIL(t3m->get_tablet_addr(key, addr))) {
            LOG_WARN("fail to get tablet addr", K(ret), K(key));
          } else if (OB_FAIL(tablet_addrs.push_back(addr))) {
            LOG_WARN("fail to push back", K(ret), K(tablet_addrs), K(addr));
          }
        } // end for
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(t3m->get_last_gc_versions_for_tablets(ls_id, tablet_ids, last_gc_versions)))  {
          LOG_WARN("failed to get last gc versions for tablets", K(ret), K(ls_id), K(tablet_ids));
        } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.batch_remove_tablet(ls_id,
                                                                      ls_->get_ls_epoch(),
                                                                      tablet_ids,
                                                                      tablet_addrs,
                                                                      last_gc_versions))) {
          LOG_WARN("fail to write batch tablets to pending arr", K(ret), K(ls_id), K(ls_->get_ls_epoch()), K(tablet_ids), K(tablet_addrs));
        } else {
          LOG_INFO("succeed to write_tablet_id_set_to_pending_free", K(ret), K(ls_id), K(tablet_ids.count()), K(tablet_ids));
        }
      }
    }
  } // end while
  return ret;
}

int ObLSTabletService::update_with_ss_tablet(
    const uint64_t data_version,
    const ObTablet &ss_tablet,
    const SCN &meta_version,
    const SCN &tx_data_table_filled_tx_scn)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObLSTabletService::update_with_ss_tablet", 1_s);
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "SSUpdateTab"));
  const share::ObLSID &ls_id = ls_->get_ls_id();
  const common::ObTabletID &tablet_id = ss_tablet.get_tablet_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle old_tablet_hdl;
  ObTabletHandle tmp_tablet_hdl;
  ObTabletHandle new_tablet_hdl;
  ObTablet *new_tablet = nullptr;
  ObTablet *old_tablet = nullptr;
  ObMetaDiskAddr disk_addr;
  int32_t private_transfer_epoch = -1;
  int64_t tablet_meta_version = 0;

  if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
    LOG_WARN("failed to get tablet", K(ret), K(key));
  } else if (OB_ISNULL(old_tablet = old_tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is nullptr", K(ret), K(old_tablet_hdl));
  } else if (OB_FAIL(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
  } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
    LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
  }

  const ObTabletPersisterParam param(data_version, ls_id, ls_->get_ls_epoch(), tablet_id, private_transfer_epoch, tablet_meta_version);
  share::SCN old_meta_version;

  if (OB_FAIL(ret)) {
  } else if (FALSE_IT(old_meta_version = old_tablet->get_pointer_handle().get_resource_ptr()->get_ss_change_version())) {
  } else if (old_meta_version >= meta_version) {
    LOG_INFO("ignore smaller meta version tablet", K(ret), K(key),
      K(old_meta_version), K(meta_version),
      "old_tablet_meta", old_tablet->get_tablet_meta(),
      "new_tablet_meta", ss_tablet.get_tablet_meta());
  } else if (FALSE_IT(time_guard.click("GetTablet"))) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else if (FALSE_IT(new_tablet = tmp_tablet_hdl.get_obj())) {
  } else if (OB_FAIL(new_tablet->init_with_ss_tablet(allocator, ss_tablet, meta_version, true/*is_update*/, private_transfer_epoch))) {
    LOG_WARN("failed to init tablet", K(ret), K(ss_tablet));
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else if (OB_FAIL(tmp_tablet_hdl.get_obj()->check_tx_data_can_explain_user_data(tx_data_table_filled_tx_scn))) {
    LOG_WARN("fail to check tx data can explain user data", K(ret), K(tx_data_table_filled_tx_scn), K(tmp_tablet_hdl));
  } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *new_tablet, new_tablet_hdl))) {
    LOG_WARN("fail to persist and transform tablet", K(ret), KPC(new_tablet), K(new_tablet_hdl));
  } else if (FALSE_IT(time_guard.click("Persist"))) {
  } else if (FALSE_IT(disk_addr = new_tablet_hdl.get_obj()->tablet_addr_)) {
  } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_hdl, new_tablet_hdl, time_guard, meta_version))) {
    LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
  } else if (OB_FAIL(new_tablet_hdl.get_obj()->start_direct_load_task_if_need())) {
    LOG_WARN("start ddl if need failed", K(ret));
  } else {
    LOG_INFO("update ss tablet succeed", K(ret), K(key), K(disk_addr), K(meta_version),
      K(private_transfer_epoch), "ss_tablet_meta", ss_tablet.get_tablet_meta());
  }

  return ret;
}

int ObLSTabletService::create_with_ss_tablet(
    const uint64_t data_version,
    const ObTablet &ss_tablet,
    const SCN &meta_version,
    const SCN &tx_data_table_filled_tx_scn,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("ObLSTabletService::create_with_ss_tablet", 1_s);
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "SSCreateTab"));
  const share::ObLSID &ls_id = ls_->get_ls_id();
  const common::ObTabletID &tablet_id = ss_tablet.get_tablet_id();
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle tmp_tablet_hdl;
  ObMetaDiskAddr disk_addr;
  int32_t old_private_transfer_epoch = -1;
  int32_t private_transfer_epoch = -1;
  int64_t tablet_meta_version = 0;

  ObTransService *tx_svr = MTL(ObTransService*);

  if (OB_FAIL(ss_tablet.get_private_transfer_epoch(old_private_transfer_epoch))) {
    LOG_WARN("failed to get private transfer epoch", K(ret), K(key), "ss_tablet_meta", ss_tablet.get_tablet_meta());
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.pick_private_transfer_epoch(ls_id,
                                                                             ls_->get_ls_epoch(),
                                                                             tablet_id,
                                                                             old_private_transfer_epoch,
                                                                             private_transfer_epoch))) {
    LOG_WARN("failed to pick private transfer epoch", K(ret), K(ls_id), "ls_epoch", ls_->get_ls_epoch(),
      K(tablet_id), K(old_private_transfer_epoch));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to create temporary tablet", K(ret), K(key));
  } else if (OB_FAIL(tmp_tablet_hdl.get_obj()->init_with_ss_tablet(allocator, ss_tablet, meta_version, false/*is_update*/, private_transfer_epoch))) {
    LOG_WARN("fail to init tablet", K(ret), K(ss_tablet));
  } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
    LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else {
    const ObTabletPersisterParam param(data_version, ls_id, ls_->get_ls_epoch(), tablet_id, private_transfer_epoch, tablet_meta_version);
    if (OB_FAIL(tmp_tablet_hdl.get_obj()->check_tx_data_can_explain_user_data(tx_data_table_filled_tx_scn))) {
      LOG_WARN("fail to check tx data can explain user data", K(ret), K(tx_data_table_filled_tx_scn), K(tmp_tablet_hdl));
    } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet_hdl.get_obj(), tablet_handle))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), K(tmp_tablet_hdl), K(tablet_handle));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = tablet_handle.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(safe_create_cas_tablet(ls_id, tablet_id, disk_addr, tablet_handle, time_guard, meta_version))) {
      LOG_WARN("fail to create tablet and cas", K(ret), K(ls_id), K(tablet_id), K(disk_addr));
    } else if (OB_FAIL(tablet_handle.get_obj()->start_direct_load_task_if_need())) {
      LOG_WARN("start ddl if need failed", K(ret));
    } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
      LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_handle));
    } else {
      LOG_INFO("create ss tablet succeed", K(ret), K(key), K(disk_addr), K(meta_version),
        K(private_transfer_epoch), "ss_tablet_meta", ss_tablet.get_tablet_meta());
    }
  }

  if (OB_FAIL(ret)) {
    tablet_handle.reset();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
      LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
    }
  }

  return ret;
}

int ObLSTabletService::create_or_update_with_ss_tablet(
    const ObTablet &ss_tablet,
    const SCN &meta_version,
    const SCN &tx_data_table_filled_tx_scn)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls_->get_ls_id();
  const common::ObTabletID &tablet_id = ss_tablet.get_tablet_id();
  uint64_t data_version = 0;
  ObTabletHandle tablet_handle;
  bool b_exist = false;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!ss_tablet.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ss_tablet));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
      LOG_WARN("failed to check tablet existence", K(ls_id), K(tablet_id));
    } else if (b_exist && OB_FAIL(update_with_ss_tablet(data_version, ss_tablet, meta_version, tx_data_table_filled_tx_scn))) {
      LOG_WARN("failed to update ss tablet", K(ret), K(tablet_id), K(data_version), K(ss_tablet), K(meta_version));
    } else if (!b_exist && OB_FAIL(create_with_ss_tablet(data_version, ss_tablet, meta_version, tx_data_table_filled_tx_scn, tablet_handle))) {
      LOG_WARN("failed to create ss tablet", K(ret), K(data_version), K(ss_tablet), K(meta_version));
    }
  }

  return ret;
}
int ObLSTabletService::advance_notify_ss_change_version(
    const ObTabletID &tablet_id,
    const share::SCN &transfer_scn,
    const share::SCN &change_version)
{
  int ret = OB_SUCCESS;
  ObTimeGuard time_guard("advance_notify_ss_change_version", 10_ms);
  const share::ObLSID ls_id = ls_->get_ls_id();
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(is_stopped_)) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("tablet service stopped", K(ret));
  } else if (OB_UNLIKELY(!ls_id.is_valid() || !tablet_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(ls_id), K(tablet_id));
  } else if (OB_UNLIKELY(nullptr == t3m)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected t3m", K(ret), KP(t3m));
  } else {
    time_guard.click();
    const ObTabletMapKey key(ls_id, tablet_id);
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click();
    if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, tablet_handle, 0,
            ObMDSGetTabletMode::READ_WITHOUT_CHECK,
            ObTransVersion::MAX_TRANS_VERSION))) {
      if (OB_TABLET_NOT_EXIST == ret) {
        LOG_INFO("tablet not exist", K(ret), K(key));
      } else {
        LOG_WARN("check and get tablet failed", K(ret), K(key));
      }
    } else if (FALSE_IT(time_guard.click())) {
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected tablet", K(ret), K(key), KP(tablet));
    } else if (tablet->get_reorganization_scn() != transfer_scn) {
      ret = OB_VERSION_NOT_MATCH;
      LOG_WARN("transfer scn not match", K(ret), K(key), K(tablet->get_reorganization_scn()),
          K(transfer_scn));
    } else if (OB_FAIL(t3m->advance_notify_ss_change_version(key, change_version))) {
      if (OB_NO_NEED_UPDATE != ret) {
        LOG_WARN("advance notify ss change version failed", K(ret), K(key), K(change_version));
      }
    } else {
      time_guard.click();
    }
  }
  return ret;
}
#endif

int ObLSTabletService::replay_deserialize_tablet(
    const ObTabletMapKey &key,
    const char *buf,
    const int64_t buf_len,
    const ObTabletHandle &tablet_handle,
    common::ObArenaAllocator &allocator,
    ObTabletPoolType &pool_type,
    ObUpdateTabletPointerParam &param)
{
  int ret = OB_SUCCESS;
  const ObMetaDiskAddr &disk_addr = param.resident_info_.addr_;
  ObTablet *tablet = tablet_handle.get_obj();
  int64_t pos = 0;
  if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("upexpected error, tablet is nullptr", K(ret), K(key), K(tablet_handle));
  } else if (FALSE_IT(tablet->tablet_addr_ = disk_addr)) {
  } else if (OB_FAIL(tablet->deserialize_for_replay(allocator, buf, buf_len, pos))) {
    LOG_WARN("fail to deserialize tablet", K(ret), KP(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(tablet->init_shared_params(key.ls_id_,
                                                key.tablet_id_,
                                                tablet->get_tablet_meta().compat_mode_))) {
    LOG_WARN("failed to init shared params", K(ret), K(key));
  // The tablet attr isn't valid, which is empty shell or upgrade from old version observer in shared nothing mode.
  } else if (!param.resident_info_.attr_.is_valid() && OB_FAIL(tablet->calc_tablet_attr(param.resident_info_.attr_))) {
    LOG_WARN("fail to calc tablet attr by tablet", K(ret));
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
  return ret;
}

int ObLSTabletService::replay_create_tablet(
    const ObUpdateTabletPointerParam &param,
    const char *buf,
    const int64_t buf_len,
    const ObTabletID &tablet_id,
    ObTabletTransferInfo &tablet_transfer_info)
{
  int ret = OB_SUCCESS;
  const ObMetaDiskAddr &disk_addr = param.tablet_addr();
  bool b_exist = false;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);
  const ObLSID &ls_id = ls_->get_ls_id();
  // In shared storage mode, don't need to load tablet to accelerate restart.
  const bool load_tablet = !GCTX.is_shared_storage_mode();
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "ReplayCreate"));
  ObTabletHandle tablet_hdl;
  ObUpdateTabletPointerParam tablet_ptr_param = param;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
    LOG_WARN("fail to check tablet existence", K(ret), K(ls_id), K(tablet_id));
  } else if (b_exist) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("restart replay tablet should not exist", K(ret), K(ls_id), K(tablet_id));
  } else {
    tablet_ptr_param.refresh_tablet_cache();
    ObTimeGuard time_guard("ObLSTabletService::replay_create_tablet", 1_s);
    const ObTabletMapKey key(ls_id, tablet_id);
    ObMetaDiskAddr old_addr;
    ObTabletPoolType pool_type(ObTabletPoolType::TP_MAX);
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tablet_hdl))) {
      LOG_WARN("fail to create temporary tablet", K(ret), K(key));
    } else if (OB_FAIL(t3m->get_tablet_addr(key, old_addr))) {
      LOG_WARN("fail to get tablet addr", K(ret), K(key));
    } else if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
      LOG_WARN("fail to set tablet id set", K(ret), K(tablet_id));
    } else if (!load_tablet) {
      pool_type = tablet_ptr_param.is_empty_shell() ? ObTabletPoolType::TP_NORMAL : ObTabletPoolType::TP_LARGE;
      tablet_hdl.reset();
      if (tablet_id.is_ls_inner_tablet() && tablet_id.is_special_memtable_tablet()
          && OB_FAIL(t3m->init_memtablet_mgr_for_inner_tablet(key, tablet_ptr_param.compat_mode()))) {
        LOG_WARN("fail to init memtable mgr for inner tablet", K(ret), K(tablet_ptr_param));
      }
    } else if (OB_FAIL(replay_deserialize_tablet(key, buf, buf_len, tablet_hdl ,allocator, pool_type, tablet_ptr_param))) {
      LOG_WARN("fail to replay deserialize tablet", K(ret));
    } else {
      time_guard.click("Deserialize");
    }
    if (OB_FAIL(ret)) { // do nothing
    } else if (OB_FAIL(t3m->compare_and_swap_tablet(
        key,
        old_addr,
        disk_addr,
        tablet_ptr_param,
        pool_type,
        true /* whether to set tablet pool */))) {
      LOG_WARN("fail to compare and swap tablat in t3m", K(ret), K(key), K(old_addr), K(disk_addr));
    } else if (FALSE_IT(time_guard.click("CASwap"))) {
    } else if (load_tablet && OB_FAIL(tablet_hdl.get_obj()->start_direct_load_task_if_need())) {
      LOG_WARN("start ddl if need failed", K(ret));
    } else if (load_tablet && OB_FAIL(tablet_hdl.get_obj()->inc_macro_ref_cnt())) {
      LOG_WARN("fail to increase macro blocks' ref cnt for meta and data", K(ret));
    } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
      LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_hdl));
    } else if (load_tablet) {
      tablet_transfer_info = tablet_hdl.get_obj()->get_tablet_meta().transfer_info_;
    } else {
      tablet_transfer_info = param.transfer_info();
    }

    if (OB_SUCC(ret)) {
      FLOG_INFO("succeeded to replay create one tablet", K(ret), K(ls_id), K(tablet_id), K(pool_type), K(param),
          KPC(tablet_hdl.get_obj()));
    } else {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
        LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  if (OB_SUCC(ret) && load_tablet) {
    int tmp_ret = OB_SUCCESS;
    ObServerAutoSplitScheduler &auto_split_scheduler= ObServerAutoSplitScheduler::get_instance();
    if (OB_TMP_FAIL(auto_split_scheduler.push_task(tablet_hdl, *ls_))) {
      LOG_WARN("fail to push auto split task", K(tmp_ret));
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
  const int64_t timeout_step_us = 10_s;
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
    ObTabletResidentInfo info;
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_ids.count(); ++i) {
      key.tablet_id_ = tablet_ids.at(i);
      if (OB_FAIL(t3m->get_tablet_resident_info(key, info))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // do nothing (expected: add no lock when fetching tablets from tablet_id_set_)
          ret = OB_SUCCESS;
        } else {
          LOG_WARN("fail to get tablet required_size", K(ret), K(key), K(tmp_size));
        }
      } else if (!info.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("in_valid resident_info", K(ret), K(key), K(info));
      } else {
        required_size += info.get_tablet_meta_size() +  // meta_size
                         info.get_required_size() -     // data_size
                         info.get_ss_public_sstable_occupy_size(); // shared_data size
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
    const int64_t current_time = ObClockGenerator::getClock();
    const int64_t timeout = param.timeout_ - current_time;
    bool need_split_dst_table = true;
    if (OB_UNLIKELY(timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("table scan timeout", K(ret), K(current_time), "table_scan_param_timeout", param.timeout_, K(lbt()));
    } else if (OB_FAIL(tablet_handle.get_obj()->check_snapshot_readable_with_cache(snapshot_version, param.schema_version_, timeout, need_split_dst_table))) {
      LOG_WARN("failed to check snapshot readable", K(ret), K(snapshot_version), K(param.schema_version_), K(timeout));
    } else if (param.need_switch_param_) {
      if (OB_FAIL(iter.switch_param(param, tablet_handle, need_split_dst_table))) {
        LOG_WARN("failed to init table scan iterator, ", K(ret));
      }
    } else if (OB_FAIL(iter.init(param, tablet_handle, need_split_dst_table))) {
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
    const lib::Worker::CompatMode &compat_mode,
    const bool need_create_empty_major_sstable,
    const share::SCN &clog_checkpoint_scn,
    const share::SCN &mds_checkpoint_scn,
    const storage::ObTabletMdsUserDataType &create_type,
    const bool micro_index_clustered,
    const bool has_cs_replica,
    const ObTabletID &split_src_tablet_id,
    ObTabletHandle &tablet_handle)
{
  TIMEGUARD_INIT(CreateTablet, 5_ms);
  int ret = OB_SUCCESS;
  common::ObArenaAllocator tmp_allocator(common::ObMemAttr(MTL_ID(), "CreateTab"));
  common::ObArenaAllocator *allocator = nullptr;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);
  const ObTabletMapKey key(ls_id, tablet_id);
  const bool need_generate_cs_replica_cg_array = ls_->is_cs_replica()
                                          && create_tablet_schema.is_row_store()
                                          && create_tablet_schema.is_user_data_table();
  ObTablet *tablet = nullptr;
  ObFreezer *freezer = ls_->get_freezer();
  tablet_handle.reset();

  const bool is_split_dest_tablet = storage::ObTabletMdsUserDataType::START_SPLIT_DST == create_type;
#ifdef ERRSIM
  if (is_split_dest_tablet) {
    LOG_INFO("stuck the process fo creating the split dest tablet", K(tablet_id), K(data_tablet_id), K(clog_checkpoint_scn));
    DEBUG_SYNC(PARTITION_SPLIT_REPLAY_CREATE_TABLET);
  }
#endif
  if (CLICK_FAIL(ObTabletCreateDeleteHelper::prepare_create_msd_tablet())) {
    LOG_WARN("fail to prepare create msd tablet", K(ret));
  } else {
    ObUpdateTabletPointerParam param;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.tablet_id_.hash());
    if (CLICK_FAIL(ObTabletCreateDeleteHelper::create_msd_tablet(key, tablet_handle))) {
      LOG_WARN("failed to create msd tablet", K(ret), K(key));
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())
        || OB_ISNULL(allocator = tablet_handle.get_obj()->get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new tablet is null", K(ret), KP(tablet), KP(allocator), K(tablet_handle));
    } else if (CLICK_FAIL(tablet->init_for_first_time_creation(*allocator, ls_id, tablet_id, data_tablet_id,
        create_scn, snapshot_version, create_tablet_schema, need_create_empty_major_sstable, clog_checkpoint_scn, mds_checkpoint_scn,
        is_split_dest_tablet, split_src_tablet_id, micro_index_clustered, need_generate_cs_replica_cg_array, has_cs_replica, freezer))) {
      LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
          K(create_scn), K(snapshot_version), K(create_tablet_schema));
    } else if (CLICK_FAIL(tablet->get_updating_tablet_pointer_param(param))) {
      LOG_WARN("fail to get updating tablet pointer parameters", K(ret), KPC(tablet));
    } else if (CLICK_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle, param))) {
      LOG_WARN("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(param));
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
    const uint64_t data_version,
    const share::ObLSID &ls_id,
    const common::ObTabletID &tablet_id,
    const common::ObTabletID &data_tablet_id,
    const share::SCN &create_scn,
    const int64_t snapshot_version,
    const ObCreateTabletSchema &create_tablet_schema,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  uint64_t compat_version = 0;
  const uint64_t tenant_id = MTL_ID();
  bool need_create_empty_major_old_version = true;
  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "LSCreateTab"));
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTransService *tx_svr = MTL(ObTransService*);
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tmp_tablet = nullptr;
  ObFreezer *freezer = ls_->get_freezer();
  ObTabletHandle tmp_tablet_hdl;
  ObMetaDiskAddr disk_addr;
  int64_t tablet_meta_version = 0;
  ObTimeGuard time_guard("ObLSTabletService::create_inner_tablet", 10_ms);
  const share::SCN clog_checkpoint_scn = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;
  const share::SCN mds_checkpoint_scn = ObTabletMeta::INIT_CLOG_CHECKPOINT_SCN;

  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());

  if (FAILEDx(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("failed to create temporary tablet", K(ret), K(key));
  } else if (OB_ISNULL(tmp_tablet = tmp_tablet_hdl.get_obj())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("new tablet is null", K(ret), KPC(tmp_tablet), K(tmp_tablet_hdl));
  } else if (FALSE_IT(time_guard.click("CreateTablet"))) {
  } else if (OB_FAIL(tmp_tablet->init_for_first_time_creation(allocator, ls_id, tablet_id, data_tablet_id,
      create_scn, snapshot_version, create_tablet_schema, true/*need_create_empty_major_sstable*/, clog_checkpoint_scn, mds_checkpoint_scn,
      false/*is_split_dest_tablet*/, ObTabletID()/*split_src_tablet_id*/, false/*micro_index_clustered*/, false/*need_generate_cs_replica_cg_array*/, false/*has_cs_replica*/, freezer))) {
    LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_id), K(data_tablet_id),
        K(create_scn), K(snapshot_version), K(create_tablet_schema));
  } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
    LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
  } else {
    const ObTabletPersisterParam param(data_version,
                                       ls_id,
                                       ls_->get_ls_epoch(),
                                       tablet_id,
                                       ObTabletTransferInfo::TRANSFER_INIT_SEQ,
                                       tablet_meta_version);
    if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(param, *tmp_tablet, tablet_handle))) {
      LOG_WARN("fail to persist and transform tablet", K(ret), K(tmp_tablet_hdl), K(tablet_handle));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = tablet_handle.get_obj()->get_tablet_addr())) {
    } else if (OB_FAIL(safe_create_cas_tablet(ls_id, tablet_id, disk_addr, tablet_handle, time_guard))) {
      LOG_WARN("fail to refresh tablet", K(ret), K(ls_id), K(tablet_id), K(disk_addr), K(tablet_handle));
    } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
      LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_handle));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_INFO("create ls inner tablet success", K(ret), K(key), K(disk_addr));
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
  ObTransService *tx_svr = MTL(ObTransService*);
  const common::ObTabletID &tablet_id = tablet_meta.tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTablet *tablet = nullptr;
  ObFreezer *freezer = ls_->get_freezer();
  common::ObArenaAllocator *allocator = nullptr;
  ObTabletHandle old_tablet_hdl;
  const bool is_transfer = true;

  LOG_INFO("prepare to init transfer in tablet", K(ret), K(ls_id), K(tablet_meta));
  ObTimeGuard time_guard("CreateTransferIn", 3_s);

  bool cover_empty_shell = false;
  bool rollback_if_failed = false;
  int32_t private_transfer_epoch = -1;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet service do not init", K(ret));
  } else if (!ls_id.is_valid() || !tablet_meta.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("create transfer in tablet get invalid argument", K(ret), K(ls_id), K(tablet_meta));
  } else if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_hdl))) {
    if (OB_TABLET_NOT_EXIST == ret) {
      LOG_DEBUG("tablet not exist", K(ret), K(tablet_id));
      ret = OB_SUCCESS;
      if (OB_FAIL(pick_private_transfer_epoch_by_mig_param(tablet_meta, *ls_, private_transfer_epoch))) {
        LOG_WARN("failed to pick private transfer epoch", K(ret), K(key), K(tablet_meta));
      } else {
        time_guard.click("PickPriTransEpoch");
      }
    } else {
      LOG_WARN("fail to get tablet", K(ret), K(tablet_id));
    }
  } else if (GCTX.is_shared_storage_mode()) {
    // to avoid empty shell leaking when ss mode
    ret = OB_EAGAIN;
    LOG_WARN("empty shell still exists, abort transfer", K(ret), K(key));
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
    ObUpdateTabletPointerParam param;
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    time_guard.click("Lock");
    if (FAILEDx(ObTabletCreateDeleteHelper::create_msd_tablet(key, tablet_handle))) {
      LOG_WARN("failed to create msd tablet", K(ret), K(key));
    } else if (FALSE_IT(rollback_if_failed = true))  {
    } else if (OB_ISNULL(tablet = tablet_handle.get_obj())
        || OB_ISNULL(allocator = tablet_handle.get_obj()->get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("new tablet is null", K(ret), KP(tablet), KP(allocator), K(tablet_handle));
    } else if (OB_FAIL(tablet->init_with_migrate_param(*allocator, tablet_meta, false/*is_update*/,
        freezer, is_transfer, private_transfer_epoch))) {
      LOG_WARN("failed to init tablet", K(ret), K(ls_id), K(tablet_meta));
    } else if (OB_FAIL(tablet->get_updating_tablet_pointer_param(param))) {
      LOG_WARN("fail to get updating tablet pointer parameters", K(ret), KPC(tablet));
    } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, tablet_handle, tablet_handle, param))) {
      LOG_WARN("failed to compare and swap tablet", K(ret), K(key), K(tablet_handle), K(param));
    } else if (OB_FAIL(tablet_id_set_.set(tablet_id))) {
      LOG_WARN("fail to insert tablet id", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(tx_svr->create_tablet(key.tablet_id_, key.ls_id_))) {
      LOG_WARN("fail to create tablet cache", K(ret), K(key), K(tablet_meta));
    } else {
      time_guard.click("Swap");
    }

    if (OB_SUCC(ret)) {
      LOG_INFO("create transfer in tablet", K(ret), K(key), K(tablet_meta), K(cover_empty_shell), K(private_transfer_epoch));
    } else if (rollback_if_failed) {
      int tmp_ret = OB_SUCCESS;
      if (OB_TMP_FAIL(rollback_remove_tablet_without_lock(ls_id, tablet_id))) {
        LOG_ERROR("fail to rollback remove tablet", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }

  return ret;
}

int ObLSTabletService::create_empty_shell_tablet(
    const uint64_t data_version,
    const ObMigrationTabletParam &param,
    ObTabletHandle &tablet_handle)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  const share::ObLSID &ls_id = param.ls_id_;
  const common::ObTabletID &tablet_id = param.tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  ObTabletHandle old_tablet_hdl;
  ObTimeGuard time_guard("ObLSTabletService::create_empty_shell_tablet", 10_ms);
  const bool is_transfer = false;
  bool is_update = false;

  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());

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
  } else {
    // tablet already exists, just do update
    is_update = true;
    time_guard.click("RemoveOld");
  }

  common::ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "MigEmptyT"));
  ObTabletHandle tmp_tablet_hdl;
  if (OB_FAIL(ret)) {
  } else if (is_update && OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to acquire temporary tablet", K(ret), K(key));
  } else if (!is_update && OB_FAIL(ObTabletCreateDeleteHelper::create_tmp_tablet(key, allocator, tmp_tablet_hdl))) {
    LOG_WARN("fail to create temporary tablet", K(ret), K(key));
  } else {
    time_guard.click("CreateTablet");
    ObFreezer *freezer = ls_->get_freezer();
    ObTablet *tmp_tablet = tmp_tablet_hdl.get_obj();
    ObTabletHandle tablet_handle;
    ObTablet *new_tablet = nullptr;
    ObMetaDiskAddr disk_addr;
    int32_t private_transfer_epoch = -1;
    int64_t tablet_meta_version = 0;
    if (OB_FAIL(param.transfer_info_.get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get transfer epoch", K(ret), "transfer_info", param.transfer_info_);
    } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    }
    const ObTabletPersisterParam persist_param(data_version,
                                               ls_id,
                                               ls_->get_ls_epoch(),
                                               tablet_id,
                                               private_transfer_epoch,
                                               tablet_meta_version);
    if (FAILEDx(tmp_tablet->init_with_migrate_param(allocator, param, false/*is_update*/, freezer, is_transfer, private_transfer_epoch))) {
      LOG_WARN("failed to init tablet", K(ret), K(param));
    } else if (FALSE_IT(time_guard.click("InitTablet"))) {
  #ifdef ERRSIM
    } else if (OB_SUCCESS != EN_CREATE_EMPTY_SHELL_TABLET_ERROR) {
      ret = EN_CREATE_EMPTY_SHELL_TABLET_ERROR;
      LOG_WARN("[ERRSIM] fake create empty shell tablet error", K(ret), K(param));
  #endif
    } else if (OB_FAIL(ObTabletPersister::transform_empty_shell(persist_param, *tmp_tablet, tablet_handle))) {
      LOG_WARN("failed to transform empty shell", K(ret), KPC(tmp_tablet));
    } else if (FALSE_IT(time_guard.click("Transform"))) {
    } else if (is_update) {
      if (OB_UNLIKELY(!old_tablet_hdl.is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid old tablet handle", K(ret), K(old_tablet_hdl));
      } else if (GCTX.is_shared_storage_mode()) {
        disk_addr = tablet_handle.get_obj()->tablet_addr_;
        if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_hdl, tablet_handle, time_guard))) {
          LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr), K(data_version));
        }
      } else {
        if (OB_FAIL(safe_update_cas_empty_shell(data_version, key, old_tablet_hdl, tablet_handle, time_guard))) {
          LOG_WARN("fail to cas empty shell", K(ret), K(key), K(old_tablet_hdl), K(tablet_handle), K(data_version));
        }
      }
    } else {
      if (GCTX.is_shared_storage_mode()) {
        disk_addr = tablet_handle.get_obj()->tablet_addr_;
        if (OB_FAIL(safe_create_cas_tablet(ls_id, tablet_id, disk_addr, tablet_handle, time_guard))) {
          LOG_WARN("fail to safe create cas tablet", K(ret), K(ls_id), K(tablet_id), K(disk_addr), K(tablet_handle));
        }
      } else {
        if (OB_FAIL(safe_create_cas_empty_shell(data_version, ls_id, tablet_id, tablet_handle, time_guard))) {
          LOG_WARN("fail to refresh empty shell", K(ret), K(ls_id), K(tablet_id), K(data_version), K(tablet_handle));
        }
      }
    }

    if (OB_SUCC(ret)) {
      ls_->get_tablet_gc_handler()->set_tablet_gc_trigger();
      LOG_INFO("succeeded to create empty shell tablet", K(ret), K(key), K(param), K(is_update));
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

  return ret;
}

int ObLSTabletService::create_memtable(const common::ObTabletID &tablet_id, CreateMemtableArg &arg)
{
  int ret = OB_SUCCESS;
  ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
  ObTabletHandle old_tablet_handle;
  const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
  ObTabletHandle new_tablet_handle;

  ObTimeGuard time_guard("ObLSTabletService::create_memtable", 10_ms);
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || arg.schema_version_ < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_id), K(arg));
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
      ObUpdateTabletPointerParam param;
      mds::MdsWriter writer;
      mds::TwoPhaseCommitState trans_stat;
      share::SCN trans_version;
      ObTablet &old_tablet = *(old_tablet_handle.get_obj());
      bool is_committed = false;
      // forbid create new memtable when transfer
      if (arg.for_replay_) {
      } else if (OB_FAIL(old_tablet.ObITabletMdsInterface::get_latest_tablet_status(user_data, writer, trans_stat, trans_version))) {
        LOG_WARN("fail to get latest tablet status", K(ret));
      } else if (FALSE_IT(is_committed = mds::TwoPhaseCommitState::ON_COMMIT == trans_stat)) {
      } else if (is_committed && ObTabletStatus::SPLIT_SRC == user_data.tablet_status_) {
        ret = OB_TABLET_IS_SPLIT_SRC;
        LOG_WARN("tablet is split src, not allow to create new memtable", K(ret), K(user_data));
      } else if (!is_committed || !user_data.tablet_status_.is_writable_for_dml()) {
        ret = OB_EAGAIN;
        if (REACH_TIME_INTERVAL(10000)) {
          LOG_WARN("tablet status not allow create new memtable", K(ret), K(is_committed), K(user_data));
        }
      }
      if (FAILEDx(old_tablet.create_memtable(arg))) {
        if (OB_MINOR_FREEZE_NOT_ALLOW != ret) {
          LOG_WARN("fail to create memtable", K(ret), K(new_tablet_handle), K(tablet_id), K(arg));
        }
      } else if (FALSE_IT(time_guard.click("create memtable"))) {
      } else if (OB_FAIL(old_tablet.get_updating_tablet_pointer_param(param, false /*update tablet attr*/))) {
        LOG_WARN("fail to get updating tablet pointer parameters", K(ret), K(old_tablet));
      } else if (OB_FAIL(t3m->compare_and_swap_tablet(key, old_tablet_handle, old_tablet_handle, param))) {
        LOG_WARN("fail to compare and swap tablet", K(ret), K(key), K(old_tablet_handle), K(param));
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
    const bool allow_no_ready_read,
    const bool need_split_src_table,
    const bool need_split_dst_table)
{
  int ret = inner_get_read_tables(tablet_id, timeout_us, snapshot_version_for_tablet, snapshot_version_for_tables, iter, allow_no_ready_read, need_split_src_table, false/*need_split_dst_table*/, ObMDSGetTabletMode::READ_READABLE_COMMITED);
  if (OB_TABLET_IS_SPLIT_SRC == ret && need_split_dst_table) {
    if (OB_FAIL(inner_get_read_tables_for_split_src(tablet_id, timeout_us, snapshot_version_for_tables, iter, allow_no_ready_read))) {
      LOG_WARN("failed to inner get read tables", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::inner_get_read_tables(
    const common::ObTabletID tablet_id,
    const int64_t timeout_us,
    const int64_t snapshot_version_for_tablet,
    const int64_t snapshot_version_for_tables,
    ObTabletTableIterator &iter,
    const bool allow_no_ready_read,
    const bool need_split_src_table,
    const bool need_split_dst_table,
    const ObMDSGetTabletMode mode)
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
    LOG_WARN("ls is not allow to read", K(ret), KPC(ls_), K(lbt()));
  } else if (FALSE_IT(key.ls_id_ = ls_->get_ls_id())) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle,
      timeout_us,
      mode,
      snapshot_version_for_tablet))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("fail to check and get tablet", K(ret), K(key), K(timeout_us),
               K(snapshot_version_for_tablet), K(snapshot_version_for_tables));
    }
  } else if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, invalid tablet handle", K(ret), K(handle));
  } else if (OB_FAIL(handle.get_obj()->get_read_tables(snapshot_version_for_tables, iter, allow_no_ready_read, need_split_src_table, need_split_dst_table))) {
    LOG_WARN("fail to get read tables", K(ret), K(handle), K(tablet_id),
             K(snapshot_version_for_tablet), K(snapshot_version_for_tables),
             K(iter), K(allow_no_ready_read));
  }
  return ret;
}

// Caller guarantees snapshot version checks has been passed, e.g., snapshot_version >= tablet's create_commit_version_.
// Because split src tablets can only be accessed using READ_ALL_COMMITED with INT64_MAX snapshot_version.
int ObLSTabletService::inner_get_read_tables_for_split_src(
    const common::ObTabletID tablet_id,
    const int64_t timeout_us,
    const int64_t snapshot_version,
    ObTabletTableIterator &iter,
    const bool allow_no_ready_read)
{
  int ret = OB_SUCCESS;
  ObTabletHandle &handle = iter.tablet_handle_;
  iter.reset();
  ObTabletMapKey key;
  key.tablet_id_ = tablet_id;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_id.is_valid() || snapshot_version < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_id), K(snapshot_version));
  } else if (FALSE_IT(key.ls_id_ = ls_->get_ls_id())) {
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::check_and_get_tablet(key, handle,
      timeout_us,
      ObMDSGetTabletMode::READ_ALL_COMMITED,
      INT64_MAX))) {
    if (OB_TABLET_NOT_EXIST != ret) {
      LOG_WARN("fail to check and get tablet", K(ret), K(key), K(timeout_us), K(snapshot_version));
    }
  } else if (OB_UNLIKELY(!handle.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected error, invalid tablet handle", K(ret), K(handle));
  } else if (OB_FAIL(handle.get_obj()->get_read_tables(
      snapshot_version,
      iter,
      allow_no_ready_read,
      false/*need_split_src_table*/,
      true/*need_split_dst_table*/))) {
    LOG_WARN("fail to get read tables", K(ret), K(handle), K(tablet_id), K(snapshot_version),
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
      LOG_WARN("fail to set tablet status", K(ret), "ls_id", ls_->get_ls_id(), K(tablet_id), K(tablet_status));
    } else {
      LOG_INFO("succeeded to set tablet status", K(ret), "ls_id", ls_->get_ls_id(), K(tablet_id), K(tablet_status));
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
      LOG_INFO("succeeded to replay set tablet status", K(ret), "ls_id", ls_->get_ls_id(), K(tablet_id), K(scn), K(tablet_status));
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
      LOG_INFO("succeeded to set ddl info", K(ret), "ls_id", ls_->get_ls_id(), K(tablet_id), K(ddl_data), K(timeout_us));
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
      LOG_INFO("succeeded to set ddl info", K(ret), "ls_id", ls_->get_ls_id(), K(tablet_id), K(ddl_data), K(scn));
    }
  }
  return ret;
}

int ObLSTabletService::insert_rows(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &column_ids,
    blocksstable::ObDatumRowIterator *row_iter,
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
  } else {
    ObDMLRunningCtx run_ctx(ctx,
                            dml_param,
                            ctx.mvcc_acc_ctx_.mem_ctx_->get_query_allocator(),
                            ObDmlFlag::DF_INSERT);
    int64_t row_count = 0;
    int64_t batch_idx = 0;
    ObDatumRow *rows = nullptr;
    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      tablet_handle.reset();
      ObTabletHandle tmp_handle;
      SMART_VAR(ObRowsInfo, rows_info) {
        ObRelativeTable &relative_table = run_ctx.relative_table_;
        const ObColDescIArray &col_descs = *(run_ctx.col_descs_);
        blocksstable::ObDatumRowIterator *unused_dup_row_iter = nullptr;
        while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
          // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
          dml_param.lob_allocator_.reuse();
          // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
          // outside the while loop.
          if (tmp_handle.get_obj() != relative_table.tablet_iter_.get_tablet_handle().get_obj()) {
            rows_info.reset();
            if (OB_FAIL(tmp_handle.assign(relative_table.tablet_iter_.get_tablet_handle()))) {
              LOG_WARN("failed to assign tablet_handle", K(ret), K(relative_table.tablet_iter_.get_tablet_handle()));
            } else if (OB_FAIL(rows_info.init(
                col_descs, relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info()))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (row_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row_count should be greater than 0", K(ret));
          } else {
            for (int64_t i = 0; i < row_count; i++) {
              rows[i].row_flag_.set_flag(ObDmlFlag::DF_INSERT);
            }
          }
          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(rows_info.assign_rows(row_count, rows))) {
            LOG_WARN("fail to assign rows", K(ret), K(batch_idx), K(row_count));
          } else if (OB_FAIL(insert_rows_to_tablet(tmp_handle, run_ctx, rows_info))) {
            LOG_WARN("insert to each tablets fail", K(ret));
          } else {
            afct_num += row_count;
          }
        } // end of while
      } else {
        LOG_WARN("Failed to allocate ObRowsInfo", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && !run_ctx.lob_dml_ctx_.is_all_task_done()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob data may not be insert", K(ret), K(run_ctx.lob_dml_ctx_));
    }

  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeeded to insert rows", K(ret), K(afct_num));
    affected_rows = afct_num;
    EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
  }
  NG_TRACE(S_insert_rows_end);

  return ret;
}

int ObLSTabletService::get_storage_row(
    const ObDatumRow &sql_row,
    const ObIArray<uint64_t> &column_ids,
    const ObColDescIArray &column_descs,
    ObRowGetter &row_getter,
    ObRelativeTable &data_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    ObDatumRow *&out_row,
    bool use_fuse_row_cache)
{
  int ret = OB_SUCCESS;
  ObDatumRowkey datum_rowkey;
  ObDatumRowkeyHelper rowkey_helper;
  if (OB_FAIL(rowkey_helper.prepare_datum_rowkey(sql_row, data_table.get_rowkey_column_num(), column_descs, datum_rowkey))) {
    LOG_WARN("failed to prepare rowkey", K(ret), K(sql_row), K(column_descs));
  } else if (OB_FAIL(init_row_getter(row_getter, store_ctx, dml_param, column_ids, data_table, false/*is_multi_get*/, true))) {
    LOG_WARN("failed to init single row getter", K(ret), K(column_ids));
  } else if (OB_FAIL(row_getter.open(datum_rowkey, use_fuse_row_cache))) {
    LOG_WARN("failed to open storage row", K(ret), K(datum_rowkey));
  } else if (OB_FAIL(row_getter.get_next_row(out_row))) {
    if (OB_ITER_END != ret) {
      LOG_WARN("failed to get single storage row", K(ret), K(sql_row));
    }
  }
  return ret;
}

int ObLSTabletService::mock_duplicated_rows_(blocksstable::ObDatumRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  ObValueRowIterator *dup_iter = NULL;

  if (OB_ISNULL(dup_iter = ObQueryIteratorFactory::get_insert_dup_iter())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory to alloc ObValueRowIterator", K(ret));
  } else {
    duplicated_rows = dup_iter;
    if (OB_FAIL(dup_iter->init())) {
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
    blocksstable::ObDatumRowIterator *row_iter,
    const ObInsertFlag flag,
    int64_t &affected_rows,
    blocksstable::ObDatumRowIterator *&duplicated_rows)
{
  int ret = OB_SUCCESS;
  int64_t afct_num = 0;
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
    int64_t row_count = 0;
    ObDatumRow *rows = nullptr;
    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      tablet_handle.reset();
      ObTabletHandle tmp_handle;
      SMART_VAR(ObRowsInfo, rows_info) {
        int64_t dup_row_count = 0;
        bool has_ignore_dup_error = false;
        ObRelativeTable &relative_table = run_ctx.relative_table_;
        const ObColDescIArray &col_descs = *(run_ctx.col_descs_);
        while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
          // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
          dml_param.lob_allocator_.reuse();
          // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
          // outside the while loop.
          if (tmp_handle.get_obj() != relative_table.tablet_iter_.get_tablet_handle().get_obj()) {
            tmp_handle.assign(relative_table.tablet_iter_.get_tablet_handle());
            rows_info.reset();
            if (OB_FAIL(rows_info.init(col_descs,
                                       relative_table,
                                       ctx,
                                       tmp_handle.get_obj()->get_rowkey_read_info()))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
          if (OB_FAIL(ret)) {
          } else if (row_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row_count should be greater than 0", K(ret));
          } else {
            for (int64_t i = 0; i < row_count; i++) {
              rows[i].row_flag_.set_flag(ObDmlFlag::DF_INSERT);
            }
            if (OB_FAIL(rows_info.assign_rows(row_count, rows))) {
              LOG_WARN("fail to assign rows", K(ret), K(row_count));
            } else if (OB_FAIL(rows_info.set_need_find_all_duplicate_rows(true, /*need_find_all_duplicate_key*/
                                                                          &duplicated_column_ids,
                                                                          &duplicated_rows))) {
              LOG_WARN("fail to set_need_find_all_duplicate_rows", K(ret), K(row_count));
            } else if (OB_FAIL(insert_rows_to_tablet(tmp_handle, run_ctx, rows_info))) {
              // the dup error is ignored here, so need clean up lob tasks that were successfully created but not executed
              run_ctx.lob_dml_ctx_.reuse();
              if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
                for (int64_t i = 0; i < row_count; i++) {
                  if (rows_info.is_row_duplicate(i)) {
                    dup_row_count++;
                  }
                }
                has_ignore_dup_error = true;
                ret = OB_SUCCESS; // ignore this error to continue to find all duplicate rows
              } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
                LOG_WARN("failed to write row", K(ret));
              }
            } else {
              afct_num += row_count;
            }
          }
        } // end of while

        if (OB_ITER_END == ret) {
          if (has_ignore_dup_error) {
            ret = OB_ERR_PRIMARY_KEY_DUPLICATE; // recover the duplicate key error
            EVENT_ADD(ObStatEventIds::SQL_INSERT_DUPLICATE_COUNT, dup_row_count);
            if (nullptr == duplicated_rows) {
              // For primary key conflicts caused by concurrent insertions within
              // a statement, we need to return the corresponding duplicated_rows.
              // However, under circumstances where an exception may unexpectedly
              // prevent us from reading the conflicting rows within statements,
              // at such times, it becomes necessary for us to mock the rows.
              int tmp_ret = OB_SUCCESS;
              if (OB_TMP_FAIL(mock_duplicated_rows_(duplicated_rows))) {
                LOG_WARN("failed to mock duplicated rows", K(tmp_ret));
                ret = tmp_ret;
              }
            }
          } else {
            ret = OB_SUCCESS;
          }
        }

        if (OB_SUCC(ret) && !run_ctx.lob_dml_ctx_.is_all_task_done()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("lob data may not be insert", K(ret), K(run_ctx.lob_dml_ctx_));
        }
      } else {
        LOG_WARN("Failed to allocate ObRowsInfo", K(ret));
      }
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeeded to insert rows with fetch dup", K(ret));
    affected_rows = afct_num;
    EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
  }
  return ret;
}

static inline
bool is_lob_update(ObDMLRunningCtx &run_ctx, const ObIArray<int64_t> &update_idx)
{
  bool bool_ret = false;
  if (run_ctx.relative_table_.is_storage_index_table() &&
      run_ctx.relative_table_.is_index_local_storage() &&
      run_ctx.relative_table_.is_vector_index()) {
    // bool_ret = false
  } else {
    for (int64_t i = 0; i < update_idx.count() && !bool_ret; ++i) {
      int64_t idx = update_idx.at(i);
      if (run_ctx.col_descs_->at(idx).col_type_.is_lob_storage()) {
        bool_ret = true;
      }
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
    blocksstable::ObDatumRowIterator *row_iter,
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
                            ObDmlFlag::DF_UPDATE,
                            true /* is_need_check_old_row_ */);
    ObIAllocator &work_allocator = run_ctx.allocator_;
    bool rowkey_change = false;
    UpdateIndexArray update_idx;
    ObDatumRowStore row_store;
    bool lob_update = false;
    ObRelativeTable &relative_table = run_ctx.relative_table_;

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
      ObDatumRow *old_rows = nullptr;
      ObDatumRow *new_rows = nullptr;
      ObDatumRow *tmp_rows = nullptr;
      int64_t old_rows_count = 0;
      int64_t new_rows_count = 0;
      ObTabletHandle tmp_handle;
      ObRowsInfo *rows_infos = nullptr;
      int64_t max_tmp_row_cnt = 0;
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
      //const bool delay_new = check_exist && lib::is_oracle_mode();
      // batch interface can be compatible with the oracle behavior in a batch, so no need delay_new for performance
      const bool delay_new = false;
      const ObColDescIArray &col_descs = *(run_ctx.col_descs_);

      // for major mv base table need update full column
      if (dml_param.table_param_->get_data_table().get_mv_mode().table_referenced_by_fast_lsm_mv_flag_ ||
          run_ctx.is_delete_insert_table_) {
        ctx.update_full_column_ =  true;
      }

      while (OB_SUCC(ret)
          && OB_SUCC(row_iter->get_next_rows(old_rows, old_rows_count))
          && OB_SUCC(row_iter->get_next_rows(new_rows, new_rows_count))) {
        // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
        dml_param.lob_allocator_.reuse();
        // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
        // outside the while loop.
        if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
          if (OB_FAIL(tmp_handle.assign(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()))) {
            LOG_WARN("failed to assign tablet_handle", K(ret), K(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()));
          } else if (nullptr != rows_infos) {
            rows_infos[0].reset();
            rows_infos[1].reset();
            if (OB_FAIL(rows_infos[0].init(col_descs, relative_table, ctx,
                tmp_handle.get_obj()->get_rowkey_read_info()))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            } else if (OB_FAIL(rows_infos[1].init(col_descs, relative_table, ctx,
                tmp_handle.get_obj()->get_rowkey_read_info()))) {
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
          old_rows[0].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
          new_rows[0].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
        } else if (nullptr == rows_infos) { // is first batch
          if (OB_ISNULL(rows_infos = static_cast<ObRowsInfo*>(work_allocator.alloc(2 * sizeof(ObRowsInfo) )))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("fail to allocate memory", K(ret));
          } else {
            new (rows_infos) ObRowsInfo[2];
            if (OB_FAIL(rows_infos[0].init(
                col_descs, relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info()))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            } else if (OB_FAIL(rows_infos[1].init(
                col_descs, relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info()))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
            }
          }
        }

        if (OB_SUCC(ret) && nullptr != rows_infos && 1 != new_rows_count) {
          for (int64_t i = 0; i < new_rows_count; i++) {
            old_rows[i].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
            new_rows[i].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
          }
          if (OB_FAIL(rows_infos[0].assign_rows(old_rows_count, old_rows))) {
            LOG_WARN("fail to assign old rows", K(ret), K(old_rows_count));
          } else if (OB_FAIL(rows_infos[1].assign_rows(new_rows_count, new_rows))) {
            LOG_WARN("fail to assign old rows", K(ret), K(new_rows_count));
          }
          // the tmp_tbl_rows is used to delete the old row if the rowkey change
          if (OB_SUCC(ret) && (rowkey_change || run_ctx.is_delete_insert_table_) && (tmp_rows == nullptr || new_rows_count > max_tmp_row_cnt)) {
            max_tmp_row_cnt = new_rows_count;
            if (tmp_rows != nullptr) {
              work_allocator.free(tmp_rows);
              tmp_rows = nullptr;
            }
            if (OB_ISNULL(tmp_rows = static_cast<ObDatumRow*>(work_allocator.alloc(new_rows_count * sizeof(ObDatumRow))))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret));
            } else {
              new (tmp_rows) ObDatumRow[new_rows_count];
            }
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
                                             old_rows[0],
                                             new_rows[0],
                                             row_store))) {
              LOG_WARN("failed to update row to tablet", K(ret), K(old_rows[0]), K(new_rows[0]));
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
                                                 old_rows[i],
                                                 new_rows[i],
                                                 row_store))) {
                  LOG_WARN("failed to update row to tablet", K(ret), K(i), K(old_rows[i]), K(new_rows[i]));
                }
              }
            } else {
              if (OB_FAIL(update_rows_to_tablet(tmp_handle,
                                                run_ctx,
                                                rowkey_change,
                                                update_idx,
                                                delay_new,
                                                lob_update,
                                                tmp_rows,
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
      if (OB_SUCC(ret) && !run_ctx.lob_dml_ctx_.is_all_task_done()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("lob data may not be insert", K(ret), K(run_ctx.lob_dml_ctx_));
      }
      if (OB_SUCC(ret) && row_store.get_row_count() > 0) {
        ObDatumRow &old_row_for_delay = old_rows[0];
        ObDatumRow &new_row_for_delay = new_rows[0];
        if (OB_FAIL(delay_process_new_rows(run_ctx,
                                           update_idx,
                                           rowkey_change,
                                           old_row_for_delay,
                                           new_row_for_delay,
                                           row_store))) {
          LOG_WARN("fail to delay process new rows", K(ret));
        }
      }
      if (nullptr != rows_infos) {
        rows_infos[0].~ObRowsInfo();
        rows_infos[1].~ObRowsInfo();
        work_allocator.free(rows_infos);
      }
      if (nullptr != tmp_rows) {
        work_allocator.free(tmp_rows);
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
    ObDatumRow &old_row,
    ObDatumRow &new_row,
    ObDatumRowStore &row_store)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tmp_handle;
  ObDatumRowStore::Iterator row_iter = row_store.begin();
  while (OB_SUCC(ret) && OB_SUCC(row_iter.get_next_row(new_row))) {
    // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
    // outside the while loop.
    if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
      tmp_handle.assign(run_ctx.relative_table_.tablet_iter_.get_tablet_handle());
    }
    if (OB_FAIL(row_iter.get_next_row(old_row))) {
      LOG_WARN("fail to get row from row stores", K(ret));
    } else if (OB_FAIL(process_lob_before_update(tmp_handle,
                                                 run_ctx,
                                                 update_idx,
                                                 rowkey_change,
                                                 1,
                                                 &old_row,
                                                 &new_row))) {
      LOG_WARN("process_lob_before_update fail", K(ret), K(old_row), K(new_row));
    } else if (OB_FAIL(process_new_row(tmp_handle,
                                       run_ctx,
                                       update_idx,
                                       rowkey_change,
                                       old_row,
                                       new_row))) {
      LOG_WARN("fail to process new row", K(ret), K(old_row), K(new_row));
    } else if (OB_FAIL(process_lob_after_update(tmp_handle,
                                                run_ctx,
                                                update_idx,
                                                rowkey_change,
                                                1,
                                                &old_row,
                                                &new_row))) {
      LOG_WARN("process_lob_after_update fail", K(ret), K(old_row), K(new_row));
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
    ObDatumRowIterator *row_iter,
    int64_t &affected_rows)
{
  int ret = OB_SUCCESS;
  NG_TRACE(S_update_rows_begin);
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  int64_t afct_num = 0;
  ObTimeGuard timeguard(__func__, 3_s);

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
    ObDatumRow *rows = nullptr;
    int64_t row_count = 0;
    const ObRelativeTable &data_table = run_ctx.relative_table_;

    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      ObTabletHandle tmp_handle;
      SMART_VAR(ObRowsInfo, rows_info) {
      const ObRelativeTable &data_table = run_ctx.relative_table_;
      const ObColDescIArray &col_descs = *(run_ctx.col_descs_);
        while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
          ObStoreRow reserved_row;
          // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
          // outside the while loop.
          if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
            rows_info.reset();
            if (OB_FAIL(tmp_handle.assign(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()))) {
              LOG_WARN("failed to assign tablet_handle", K(ret), K(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()));
            } else if (OB_FAIL(rows_info.init(
                col_descs, data_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info()))) {
              LOG_WARN("Failed to init rows info", K(ret), K(data_table));
            }
          }
          if (OB_FAIL(ret)) {
            // do nothing
          } else if (row_count <= 0) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("row_count should be greater than 0", K(ret));
          } else {
            for (int64_t i = 0; i < row_count; i++) {
              rows[i].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
            }
            if (OB_FAIL(rows_info.assign_rows(row_count, rows))) {
              LOG_WARN("fail to assign rows", K(ret), K(row_count));
            }
          }

          if (OB_FAIL(ret)) {
          } else if (OB_FAIL(put_rows_to_tablet(tmp_handle, run_ctx, rows_info, afct_num))) {
            LOG_WARN("put to each tablets fail", K(ret));
          }
        }  // end of while

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
      } else {
        LOG_WARN("Failed to allocate ObRowsInfo", K(ret));
      }
    }

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret) && !run_ctx.lob_dml_ctx_.is_all_task_done()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("lob data may not be insert", K(ret), K(run_ctx.lob_dml_ctx_));
    }
  }

  if (OB_SUCC(ret)) {
    LOG_DEBUG("succeeded to put rows", K(ret));
    affected_rows = afct_num;
    EVENT_ADD(STORAGE_INSERT_ROW_COUNT, afct_num);
  }
  NG_TRACE(S_update_row_end);

  return ret;
}

int ObLSTabletService::delete_rows(
    ObTabletHandle &tablet_handle,
    ObStoreCtx &ctx,
    const ObDMLBaseParam &dml_param,
    const ObIArray<uint64_t> &column_ids,
    blocksstable::ObDatumRowIterator *row_iter,
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
                            ObDmlFlag::DF_DELETE,
                            true /* is_need_check_old_row_ */);
    int64_t row_count = 0;
    ObDatumRow *rows = nullptr;
    ObDatumRow *tmp_rows = nullptr;
    char *tmp_rows_buf = nullptr;
    int64_t max_tmp_row_cnt = 0;

    if (OB_FAIL(prepare_dml_running_ctx(&column_ids, nullptr, tablet_handle, run_ctx))) {
      LOG_WARN("failed to prepare dml running ctx", K(ret));
    } else {
      tablet_handle.reset();
      // for delete_insert table need update full column
      if (run_ctx.is_delete_insert_table_) {
        ctx.update_full_column_ =  true;
      }

      SMART_VAR(ObRowsInfo, rows_info) {
        ObRelativeTable &relative_table = run_ctx.relative_table_;
        const ObColDescIArray &col_descs = *(run_ctx.col_descs_);
         ObIAllocator &work_allocator = run_ctx.allocator_;
        int64_t cur_time = 0;
        ObTabletHandle tmp_handle;
        int64_t max_row_cnt = 0;

        while (OB_SUCC(ret) && OB_SUCC(get_next_rows(row_iter, rows, row_count))) {
          // need to be called just after get_next_row to ensure that previous row's LOB memoroy is valid if get_next_row accesses it
          dml_param.lob_allocator_.reuse();
          // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
          // outside the while loop.
          if (tmp_handle.get_obj() != relative_table.tablet_iter_.get_tablet_handle().get_obj()) {
            rows_info.reset();
            if (OB_FAIL(tmp_handle.assign(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()))) {
              LOG_WARN("failed to assign tablet_handle", K(ret), K(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()));
            } else if (OB_FAIL(rows_info.init(
                col_descs, relative_table, ctx, tmp_handle.get_obj()->get_rowkey_read_info()))) {
              LOG_WARN("Failed to init rows info", K(ret), K(relative_table));
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
          } else if (row_count == 1) {
            tmp_rows = &run_ctx.datum_row_;
          } else if (tmp_rows_buf == nullptr || row_count > max_tmp_row_cnt) {
            max_tmp_row_cnt = row_count;
            if (tmp_rows_buf != nullptr) {
              work_allocator.free(tmp_rows_buf);
              tmp_rows_buf = nullptr;
            }
            if (OB_ISNULL(tmp_rows_buf = static_cast<char*>(work_allocator.alloc(sizeof(ObDatumRow) * row_count)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("fail to allocate memory", K(ret), K(row_count));
            } else {
              tmp_rows = new (tmp_rows_buf) ObDatumRow[row_count];
            }
          } else {
            tmp_rows = reinterpret_cast<ObDatumRow*>(tmp_rows_buf);
          }

          if (OB_SUCC(ret)) {
            for (int64_t i = 0; i < row_count; i++) {
              rows[i].row_flag_.set_flag(ObDmlFlag::DF_DELETE);
            }
            if (OB_FAIL(rows_info.assign_rows(row_count, rows))) {
              LOG_WARN("fail to assign rows", K(ret), K(row_count));
            } else if (OB_FAIL(delete_rows_in_tablet(tmp_handle, run_ctx, tmp_rows, rows_info))) {
              LOG_WARN("fail to delete rows in tablet", K(ret), K(rows_info));
            } else {
              afct_num += row_count;
            }
          }
        } // end of while

        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
        }
        if (tmp_rows_buf != nullptr) {
          work_allocator.free(tmp_rows_buf);
        }
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
    blocksstable::ObDatumRowIterator *row_iter,
    int64_t &affected_rows)
{
  UNUSEDx(lock_flag, is_sfu);
  NG_TRACE(S_lock_rows_begin);
  int ret = OB_SUCCESS;
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  ObTimeGuard timeguard(__func__, 3_s);
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
    ObDatumRow *row = nullptr;
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
      run_ctx.col_descs_ = &col_desc;
      ObTabletHandle tmp_handle;
      int64_t error_row_idx = 0;
      while (OB_SUCCESS == ret && OB_SUCC(row_iter->get_next_row(row))) {
        // Let ObStorageTableGuard refresh retired memtable, should not hold origin tablet handle
        // outside the while loop.
        if (tmp_handle.get_obj() != run_ctx.relative_table_.tablet_iter_.get_tablet_handle().get_obj()) {
          if (OB_FAIL(tmp_handle.assign(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()))) {
            LOG_WARN("failed to assign tablet_handle", K(ret), K(run_ctx.relative_table_.tablet_iter_.get_tablet_handle()));
          }
        }
        ObRelativeTable &relative_table = run_ctx.relative_table_;
        const ObStorageDatumUtils &datum_utils = dml_param.table_param_->get_data_table().get_read_info().get_datum_utils();
        bool is_exists = true;
        if (ObTimeUtility::current_time() > dml_param.timeout_) {
          ret = OB_TIMEOUT;
          int64_t cur_time = ObClockGenerator::getClock();
          LOG_WARN("query timeout", K(cur_time), K(dml_param), K(ret));
        } else if (GCONF.enable_defensive_check()
            && OB_FAIL(check_old_row_legitimacy_wrap(datum_utils.get_cmp_funcs(), tmp_handle, run_ctx, 1, row, error_row_idx))) {
          LOG_WARN("check row legitimacy failed", K(ret), KPC(row));
        } else if (GCONF.enable_defensive_check()
            && OB_FAIL(check_datum_row_nullable_value(col_desc, relative_table, *row))) {
          LOG_WARN("check lock row nullable failed", K(ret));
        } else if (FALSE_IT(timeguard.click("Check"))) {
        } else if (OB_FAIL(lock_row_wrap(tmp_handle, dml_param.data_row_for_lob_, run_ctx.relative_table_, ctx, col_desc, *row))) {
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
    blocksstable::ObDatumRow &row,
    const ObLockFlag lock_flag,
    const bool is_sfu)
{
  UNUSEDx(lock_flag, is_sfu);
  int ret = OB_SUCCESS;
  const ObTabletID &data_tablet_id = ctx.tablet_id_;
  ObTimeGuard timeguard(__func__, 3_s);
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
      } else if (OB_FAIL(lock_row_wrap(tablet_handle, dml_param.data_row_for_lob_, run_ctx.relative_table_, ctx, col_desc, row))) {
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
  uint64_t data_version = 0;

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
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else if (need_create_msd_tablet) {
    if (OB_FAIL(create_empty_shell_tablet(data_version, mig_tablet_param, tablet_handle))) {
      LOG_WARN("failed to create empty shell tablet", K(ret), K(tablet_id), K(mig_tablet_param));
    }
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
      LOG_WARN("failed to check tablet existence", K(ls_id), K(tablet_id));
    } else if (b_exist
        && OB_FAIL(migrate_update_tablet(data_version, mig_tablet_param))) {
      LOG_WARN("failed to update tablet meta", K(ret), K(tablet_id), K(mig_tablet_param));
    } else if (!b_exist
        && OB_FAIL(migrate_create_tablet(data_version, mig_tablet_param, tablet_handle))) {
      LOG_WARN("failed to migrate create tablet", K(ret), K(mig_tablet_param));
    }
  }

  return ret;
}

int ObLSTabletService::rebuild_create_tablet(
    const ObMigrationTabletParam &mig_tablet_param)
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = mig_tablet_param.ls_id_;
  const common::ObTabletID &tablet_id = mig_tablet_param.tablet_id_;
  const ObTabletMapKey key(ls_id, tablet_id);
  bool b_exist = false;
  const bool need_create_msd_tablet = mig_tablet_param.is_empty_shell();
  ObTabletHandle new_tablet_handle;
  uint64_t data_version = 0;
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
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else if (need_create_msd_tablet) {
    if (OB_FAIL(create_empty_shell_tablet(data_version, mig_tablet_param, new_tablet_handle))) {
      LOG_WARN("failed to create empty shell tablet", K(ret), K(tablet_id), K(mig_tablet_param));
    }
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
    if (OB_FAIL(has_tablet(ls_id, tablet_id, b_exist))) {
      LOG_WARN("fail to check tablet existence", K(ls_id), K(tablet_id));
    } else if (!b_exist &&
        OB_FAIL(migrate_create_tablet(data_version, mig_tablet_param, new_tablet_handle))) {
      LOG_WARN("failed to rebuild create tablet", K(ret), K(tablet_id), K(data_version), K(mig_tablet_param));
    } else if (b_exist &&
        OB_FAIL(migrate_update_tablet(data_version, mig_tablet_param))) {
      LOG_WARN("failed to rebuild create tablet", K(ret), K(tablet_id), K(data_version), K(mig_tablet_param));
    }
  }

  return ret;
}

int ObLSTabletService::build_tablet_with_batch_tables(
    const ObTabletID &tablet_id,
    const ObBatchUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "BuildHaTab"));
  ObMetaDiskAddr disk_addr;
  ObTabletPointer *pointer = nullptr;
  ObTimeGuard time_guard("ObLSTabletService::build_ha_tablet_new_table_store", 1_s);
  ObTabletHandle new_tablet_handle;
  bool no_need_to_upload = false;

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

    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());

    time_guard.click("Lock");

    if (OB_FAIL(direct_get_tablet(tablet_id, old_tablet_handle))) {
      LOG_WARN("failed to get tablet", K(ret), K(tablet_id));
    } else if (old_tablet_handle.get_obj()->is_empty_shell()) {
      no_need_to_upload = true;
      LOG_INFO("old tablet is empty shell tablet, should skip this operation", K(ret), "old_tablet", old_tablet_handle.get_obj());
    } else if (OB_ISNULL(pointer = old_tablet_handle.get_obj()->get_pointer_handle().get_tablet_pointer())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet pointer should not be NULL", K(ret), K(old_tablet_handle));
    } else {
      uint64_t data_version = 0;
      ObTabletMdsExclusiveLockGuard mds_truncate_log_guard(pointer->get_mds_truncate_lock(), true);
      if (OB_FAIL(mds_truncate_log_guard.get_ret())) {
        LOG_WARN("try lock mds truncate failed", K(ret), K(tablet_id), "release mds scn", param.release_mds_scn_);
        ret = OB_EAGAIN;
      } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
        LOG_WARN("fail to get min data version", K(ret));
      } else {
        if (param.release_mds_scn_.is_min()) {
          //do nothing
        } else {
          // pointer->get_mds_truncate_lock().exclusive_lock(); // need use try lock, and expand critical area
          if (OB_FAIL(pointer->release_mds_nodes_redo_scn_below(tablet_id, param.release_mds_scn_))) {
            //overwrite ret
            LOG_WARN("failed to relase mds node redo scn below", K(ret), K(tablet_id), "release mds scn", param.release_mds_scn_);
            ret = OB_RELEASE_MDS_NODE_ERROR;
          }
          // pointer->get_mds_truncate_lock().exclusive_unlock();
        }

        time_guard.click("ReleaseMDS");

        ObTablet *old_tablet = old_tablet_handle.get_obj();
        ObTablet *tmp_tablet = nullptr;
        const share::ObLSID &ls_id = ls_->get_ls_id();
        const ObTabletMapKey key(ls_id, tablet_id);
        int32_t private_transfer_epoch = -1;
        int64_t tablet_meta_version = 0;
        if (FAILEDx(old_tablet->get_private_transfer_epoch(private_transfer_epoch))) {
          LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet->get_tablet_meta());
        } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
          LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
        }
        const ObTabletPersisterParam persist_param(data_version,
                                                   ls_id,
                                                   ls_->get_ls_epoch(),
                                                   tablet_id,
                                                   private_transfer_epoch,
                                                   tablet_meta_version);
        if (OB_FAIL(ret)) {
        } else if (OB_FAIL(ObTabletCreateDeleteHelper::acquire_tmp_tablet(key, allocator, tmp_tablet_handle))) {
          LOG_WARN("failed to acquire tablet", K(ret), K(key));
        } else if (FALSE_IT(tmp_tablet = tmp_tablet_handle.get_obj())) {
        } else if (OB_FAIL(tmp_tablet->init_for_sstable_replace(allocator, param, *old_tablet))) {
          LOG_WARN("failed to init tablet", K(ret), KPC(old_tablet));
        } else if (FALSE_IT(time_guard.click("InitTablet"))) {
        } else if (OB_FAIL(ObTabletPersister::persist_and_transform_tablet(persist_param, *tmp_tablet, new_tablet_handle))) {
          LOG_WARN("fail to persist and transform tablet", K(ret), KPC(tmp_tablet), K(new_tablet_handle));
        } else if (FALSE_IT(time_guard.click("Persist"))) {
        } else if (FALSE_IT(disk_addr = new_tablet_handle.get_obj()->tablet_addr_)) {
        } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_tablet_handle, new_tablet_handle, time_guard))) {
          LOG_WARN("fail to update tablet", K(ret), K(key), K(disk_addr));
        } else {
          if (OB_SUCC(ret)) {
            LOG_INFO("succeed to build ha tablet new table store", KR(ret), K(key), K(disk_addr), K(param));
          }
        }
      }
    }
  }
#ifdef OB_BUILD_SHARED_STORAGE
  // register upload task after HA tablet is built
  if (OB_SUCC(ret) && GCTX.is_shared_storage_mode()) {
    if (no_need_to_upload) {
      // do nothing
    } else if (OB_FAIL(register_all_sstables_upload_(new_tablet_handle))) {
      LOG_WARN("register sstables upload failed", KR(ret), K(tablet_id), K(param));
    }
  }
#endif
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObLSTabletService::register_all_sstables_upload_(ObTabletHandle &new_tablet_handle)
{
  int ret = OB_SUCCESS;
    const int64_t start_register_upload_ts = ObClockGenerator::getClock();
    do {
      if (!new_tablet_handle.is_valid()) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("unexpected invalid tablet", KR(ret));
      } else if (OB_FAIL(ls_->register_all_sstables_upload(new_tablet_handle))) {
        const int64_t WARN_THREASHOLD = 60LL * 1000LL * 1000LL /* 60 seconds */;
        if (ret == OB_ALLOCATE_MEMORY_FAILED) {
          if (ObClockGenerator::getClock() - start_register_upload_ts > WARN_THREASHOLD &&
              REACH_TIME_INTERVAL(WARN_THREASHOLD)) {
            LOG_ERROR("register sstable upload cost too much time",
                       KTIME(start_register_upload_ts),
                       K(new_tablet_handle));
          }
        } else {
          LOG_ERROR("fail to register new tablet for upload");
        }
      }
    } while (ret == OB_ALLOCATE_MEMORY_FAILED);
    return ret;
}
#endif

int ObLSTabletService::safe_update_cas_tablet(
    const ObTabletMapKey &key,
    const ObMetaDiskAddr &addr,
    const ObTabletHandle &old_handle,
    ObTabletHandle &new_handle,
    ObTimeGuard &time_guard,
    const share::SCN &ss_change_version)
{
  int ret = OB_SUCCESS;
  ObUpdateTabletPointerParam param;
  int64_t ls_epoch = 0;
  if (OB_FAIL(new_handle.get_obj()->get_ls_epoch(ls_epoch))) {
    LOG_WARN("fail to get ls epoch", K(ret), K(key), K(new_handle));
  } else if (OB_FAIL(new_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
    LOG_WARN("fail to get updating tablet pointer parameters", K(ret), K(new_handle));
  } else if (ss_change_version.is_valid() &&
             FALSE_IT(param.resident_info_.attr_.ss_change_version_ = ss_change_version)) {
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.update_tablet(key.ls_id_, ls_epoch, key.tablet_id_, param))) {
    LOG_WARN("fail to write update tablet slog", K(ret), K(key), K(param));
  } else if (FALSE_IT(time_guard.click("WrSlog"))) {
  } else if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(key, old_handle, new_handle, param))) {
    LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(addr), K(param));
    ob_usleep(1_s);
    ob_abort();
  } else {
    time_guard.click("CASwap");
  }
  return ret;
}

/*static*/int ObLSTabletService::pick_private_transfer_epoch_by_mig_param(
    const ObMigrationTabletParam &mig_param,
    const ObLS &dest_ls,
    int32_t &private_transfer_epoch)
{
  int ret = OB_SUCCESS;
  private_transfer_epoch = -1;

  const ObTabletID &tablet_id = mig_param.tablet_id_;
  const ObLSID &dest_ls_id = dest_ls.get_ls_id();
  int32_t old_private_transfer_epoch = -1;
  ObLSService *ls_svr = nullptr;
  ObLSID src_ls_id;
  ObLSHandle src_ls_handle;
  ObLS *src_ls = nullptr;
  ObTabletHandle src_tablet_handle;

  ObTimeGuard time_guard("PickPriTransEpoch", 100_ms);
  // lock free
  if (OB_ISNULL(ls_svr = MTL(ObLSService *))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null ls service", K(ret), KP(ls_svr));
  } else if (OB_UNLIKELY(!mig_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid mig_param", K(ret), K(mig_param));
  } else if (OB_UNLIKELY(mig_param.get_transfer_dest_ls_id() != dest_ls_id)) { // check dest_ls
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("dest_ls mismatch with mig_param", K(ret), K(dest_ls), K(mig_param));
  } else if (FALSE_IT(src_ls_id = mig_param.get_transfer_src_ls_id())) {
  } else if (OB_FAIL(ls_svr->get_ls(src_ls_id, src_ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get src ls", K(ret), K(src_ls_id));
  } else if (OB_ISNULL(src_ls = src_ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null src_ls", K(ret), K(src_ls_handle));
  } else if (OB_FAIL(src_ls->ha_get_tablet(tablet_id, src_tablet_handle))) {
    LOG_WARN("failed to ha get tablet", K(ret), K(tablet_id), K(src_ls_handle), K(mig_param));
  } else if (FALSE_IT(time_guard.click("GetSrcTablet"))) {
  } else if (OB_FAIL(src_tablet_handle.get_obj()->get_private_transfer_epoch(old_private_transfer_epoch))) {
    LOG_WARN("failed to get private transfer epoch from src_tablet", K(ret), "src_tablet_meta", src_tablet_handle.get_obj()->get_tablet_meta(),
      K(mig_param));
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.pick_private_transfer_epoch(dest_ls_id,
                                                                             dest_ls.get_ls_epoch(),
                                                                             tablet_id,
                                                                             old_private_transfer_epoch,
                                                                             private_transfer_epoch))) {
    LOG_WARN("failed to pick private transfer epoch", K(dest_ls_id), K(tablet_id),
      K(old_private_transfer_epoch), K(private_transfer_epoch));
  } else {
    time_guard.click("PickPriTransEpoch");
    LOG_INFO("succeed to pick private transfer epoch by mig_param", K(ret), K(dest_ls_id),
      K(src_ls_id), K(tablet_id), "transfer_info", mig_param.transfer_info_, K(old_private_transfer_epoch),
      K(private_transfer_epoch), K(time_guard));
  }
  return ret;
}

int ObLSTabletService::safe_update_cas_empty_shell(
    const uint64_t data_version,
    const ObTabletMapKey &key,
    const ObTabletHandle &old_handle,
    ObTabletHandle &new_handle,
    ObTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  ObMetaDiskAddr addr;
  ObUpdateTabletPointerParam param;
  ObTablet *tablet = new_handle.get_obj();
  if (OB_FAIL(new_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
    LOG_WARN("fail to get updating tablet pointer parameters", K(ret), K(new_handle));
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.write_empty_shell_tablet(data_version, ls_->get_ls_epoch(), tablet, addr))) {
    LOG_WARN("fail to write emtpy shell tablet slog", K(ret), K(key), K(new_handle));
  } else if (FALSE_IT(tablet->tablet_addr_ = addr)) {
  } else if (FALSE_IT(param.resident_info_.addr_ = addr)) {
  } else if (FALSE_IT(time_guard.click("WrSlog"))) {
  } else if (OB_FAIL(MTL(ObTenantMetaMemMgr*)->compare_and_swap_tablet(key, old_handle, new_handle, param))) {
    LOG_ERROR("failed to compare and swap tablet", K(ret), K(key), K(old_handle), K(new_handle), K(param));
    ob_usleep(1_s);
    ob_abort();
  } else {
    time_guard.click("CASwap");
  }
  return ret;
}

int ObLSTabletService::safe_create_cas_tablet(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const ObMetaDiskAddr &addr,
    ObTabletHandle &tablet_handle,
    ObTimeGuard &time_guard,
    const share::SCN &ss_change_version)
{
  int ret = OB_SUCCESS;
  ObUpdateTabletPointerParam param;
  int64_t ls_epoch = 0;
  if (OB_FAIL(tablet_handle.get_obj()->get_ls_epoch(ls_epoch))) {
    LOG_WARN("fail to get ls epoch", K(ret), K(ls_id), K(tablet_id), K(addr));
  } else if (OB_FAIL(tablet_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
    LOG_WARN("fail to get updating tablet pointer parameters", K(ret), K(tablet_handle));
  } else if (ss_change_version.is_valid() &&
             FALSE_IT(param.resident_info_.attr_.ss_change_version_ = ss_change_version)) {
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.update_tablet(ls_id, ls_epoch, tablet_id, param))) {
    LOG_WARN("fail to write update tablet slog", K(ret), K(ls_id), K(tablet_id), K(addr));
  } else if (FALSE_IT(time_guard.click("WrSlog"))) {
  } else if (OB_FAIL(refresh_tablet_addr(ls_id, tablet_id, param, tablet_handle))) {
    LOG_WARN("failed to refresh tablet addr", K(ret), K(ls_id), K(tablet_id), K(param), K(lbt()));
    ob_usleep(1_s);
    ob_abort();
  } else {
    time_guard.click("RefreshAddr");
  }
  return ret;
}

int ObLSTabletService::safe_create_cas_empty_shell(
    const uint64_t data_version,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObTabletHandle &tablet_handle,
    ObTimeGuard &time_guard)
{
  int ret = OB_SUCCESS;
  ObTablet *tablet = tablet_handle.get_obj();
  ObUpdateTabletPointerParam param;
  ObMetaDiskAddr addr;
  if (OB_FAIL(tablet_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
    LOG_WARN("fail to get updating tablet pointer parameters", K(ret), K(tablet_handle));
  } else if (OB_FAIL(TENANT_STORAGE_META_SERVICE.write_empty_shell_tablet(data_version, ls_->get_ls_epoch(), tablet, addr))) {
    LOG_WARN("fail to write emtpy shell tablet", K(ret), K(tablet_id), K(addr));
  } else if (FALSE_IT(tablet->tablet_addr_ = addr)) {
  } else if (FALSE_IT(param.resident_info_.addr_ = addr)) {
  } else if (FALSE_IT(time_guard.click("WrSlog"))) {
  } else if (OB_FAIL(refresh_tablet_addr(ls_id, tablet_id, param, tablet_handle))) {
    LOG_WARN("failed to refresh tablet addr", K(ret), K(ls_id), K(tablet_id), K(param), K(lbt()));
    ob_usleep(1_s);
    ob_abort();
  } else {
    time_guard.click("Refresh");
  }
  return ret;
}

int ObLSTabletService::check_old_row_legitimacy(
    const ObStoreCmpFuncs &cmp_funcs,
    ObTabletHandle &data_tablet_handle,
    ObRelativeTable &data_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObIArray<uint64_t> *column_ids_ptr,
    const ObColDescIArray *col_descs_ptr,
    const bool is_need_check_old_row,
    const bool is_udf,
    const blocksstable::ObDmlFlag &dml_flag,
    const blocksstable::ObDatumRow &old_row)
{
  int ret = OB_SUCCESS;
  // usage:
  //   alter system set_tp tp_no=9,match=3221487629,error_code=4377,frequency=1
  // where session_id is 3221487629
  const int inject_err = OB_E(EventTable::EN_9, store_ctx.mvcc_acc_ctx_.tx_desc_->get_session_id()) OB_SUCCESS;
  if (OB_ERR_DEFENSIVE_CHECK == inject_err) {
    ret = OB_ERR_DEFENSIVE_CHECK;
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(data_table.get_rowkey_column_num() > old_row.count_) || OB_ISNULL(column_ids_ptr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("old row is invalid", K(ret), K(old_row), K(data_table.get_rowkey_column_num()), KP(column_ids_ptr));
  } else if (is_need_check_old_row) {
    //the vertical partition is no longer maintained,
    //and the defense check skips the vertical partition function
    ObArenaAllocator scan_allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_TABLE_SCAN_ITER));
    ObRowGetter storage_row_getter(scan_allocator, *data_tablet_handle.get_obj());
    ObDatumRow *storage_old_row = nullptr;
    const ObIArray<uint64_t> &column_ids = *column_ids_ptr;
    const ObColDescIArray &column_descs =  *col_descs_ptr;
    uint64_t err_col_id = OB_INVALID_ID;
    if (OB_FAIL(get_storage_row(old_row, column_ids, column_descs, storage_row_getter,
                                data_table, store_ctx, dml_param, storage_old_row, true))) {
      if (OB_ITER_END == ret) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        FLOG_WARN("old row in storage is not exists", K(ret), K(old_row));
      } else {
        LOG_WARN("get next row from old_row_getter failed", K(ret), K(column_ids), K(old_row));
      }
    } else if (OB_ISNULL(storage_old_row)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected error, storage old row is NULL", K(ret));
    } else if (storage_old_row->count_ != old_row.count_) {
      ret = OB_ERR_DEFENSIVE_CHECK;
      FLOG_WARN("storage old row is not matched with sql old row", K(ret));
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < old_row.count_; ++i) {
        const ObStorageDatum &storage_val = storage_old_row->storage_datums_[i];
        const ObStorageDatum &sql_val = old_row.storage_datums_[i];
        const ObObjMeta &sql_meta = column_descs.at(i).col_type_;
        int cmp_ret = 0;
        if (sql_meta.is_lob_storage() || sql_meta.is_user_defined_sql_type()) {
          // skip all text and lob
        } else if (sql_meta.is_lob_locator()) {
          //skip check lob column type when do the normal sql write check
        } else if (OB_UNLIKELY(storage_val.is_nop_value())) {
          bool is_nop = false;
          if (OB_FAIL(data_table.is_nop_default_value(column_ids.at(i), is_nop))) {
            LOG_WARN("check column whether has nop default value failed", K(ret), K(column_ids.at(i)));
          } else if (!is_nop) {
            err_col_id = column_ids.at(i);
            ret = OB_ERR_DEFENSIVE_CHECK;
            err_col_id = column_ids.at(i);
            FLOG_WARN("storage_val is not equal with sql_val, maybe catch a bug", K(ret),
                 K(i), K(column_ids.at(i)), K(storage_val), K(sql_val));
          }
        } else if (sql_val.is_nop_value()) {
          //this column is nop val, means that this column does not be touched by DML
          //just ignore it
        } else if (OB_FAIL(cmp_funcs.at(i).compare(storage_val, sql_val, cmp_ret)) || 0 != cmp_ret) {
          ret = OB_ERR_DEFENSIVE_CHECK;
          err_col_id = column_ids.at(i);
          FLOG_WARN("storage_val is not equal with sql_val, maybe catch a bug", K(ret),
                  K(storage_val), K(sql_val), K(column_ids.at(i)), K(cmp_ret));
        }
      }
    }

    if (OB_ERR_DEFENSIVE_CHECK == ret && dml_param.is_batch_stmt_) {
      // 批量删除的时候可能索引表的删除在主表前边，所以所有的表在batch删除的时候出现4377，都可能是重复删导致的
      ret = OB_BATCHED_MULTI_STMT_ROLLBACK;
      LOG_TRACE("batch stmt execution has a correctness error, needs rollback", K(ret),
                "column_id", column_ids,
                KPC(storage_old_row),
                "sql_old_row", old_row,
                K(dml_param),
                "dml_type", dml_flag);
    }
    if (OB_ERR_DEFENSIVE_CHECK == ret) {
      int tmp_ret = OB_SUCCESS;
      bool is_virtual_gen_col = false;
      if (OB_TMP_FAIL(check_real_leader_for_4377_(store_ctx.ls_id_))) {
        ret = tmp_ret;
        LOG_WARN("check real leader for 4377 found exception", K(ret), K(old_row), K(data_table));
      } else if (nullptr != store_ctx.mvcc_acc_ctx_.tx_desc_
          && OB_TMP_FAIL(check_need_rollback_in_transfer_for_4377_(store_ctx.mvcc_acc_ctx_.tx_desc_,
                                                                       data_tablet_handle))) {
        ret = tmp_ret;
        LOG_WARN("check need rollback in transfer for 4377 found exception", K(ret), K(old_row), K(data_table));
      } else if (OB_TMP_FAIL(check_parts_tx_state_in_transfer_for_4377_(store_ctx.mvcc_acc_ctx_.tx_desc_))) {
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
                      K(dml_param),
                      K(dml_flag),
                      K(store_ctx),
                      "relative_table", data_table);
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
        concurrency_control::ObDataValidationService::set_delay_resource_recycle(store_ctx.ls_id_);
        LOG_ERROR("Dump data table info", K(ret), K(data_table));
        store_ctx.force_print_trace_log();
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
    const ObDatumRow *datum_rows)
{
  int ret = OB_SUCCESS;
  ObRelativeTable &data_table = run_ctx.relative_table_;
  int64_t data_table_cnt = data_table.get_column_count();
  if (OB_ISNULL(run_ctx.column_ids_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("column ids is nullptr", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(check_datum_row_nullable_value(*run_ctx.col_descs_, data_table, datum_rows[i]))) {
      LOG_WARN("check datum row nullable value failed", K(ret),
               "dml_param", run_ctx.dml_param_,
               "dml_type", run_ctx.dml_flag_);
    } else if (OB_FAIL(check_datum_row_shadow_pk(*run_ctx.column_ids_, data_table, datum_rows[i],
        run_ctx.dml_param_.table_param_->get_data_table().get_read_info().get_datum_utils()))) {
      LOG_WARN("check datum row nullable value failed", K(ret),
               "dml_param", run_ctx.dml_param_,
               "dml_type", run_ctx.dml_flag_);
    }
  }
  return ret;
}

int ObLSTabletService::insert_rows_to_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObRowsInfo &rows_info)
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
  } else if (OB_FAIL(insert_vector_index_rows(tablet_handle, run_ctx, rows_info.rows_, row_count))) {
    LOG_WARN("failed to process vector index rows", K(ret));
  } else if (OB_FAIL(process_lob_before_insert(tablet_handle, run_ctx, rows_info.rows_, row_count))) {
    LOG_WARN("process_lob_before_insert fail", K(ret), K(row_count));
  } else if (OB_FAIL(insert_tablet_rows(tablet_handle, run_ctx, rows_info))) {
    LOG_WARN("failed to insert rows to data tablet", K(ret));
  } else if (OB_FAIL(process_lob_after_insert(tablet_handle, run_ctx, rows_info.rows_, row_count))) {
    LOG_WARN("process_lob_after_insert fail", K(ret), K(row_count));
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
  const bool check_exists = !table.is_storage_index_table() || table.is_unique_index() ||
      run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_update_pk_dop();

  const int64_t row_count = rows_info.get_rowkey_cnt();
  // 1. Defensive checking of new rows.
  if (GCONF.enable_defensive_check()) {
    if (OB_FAIL(check_new_row_legitimacy(run_ctx, row_count, rows_info.rows_))) {
      LOG_WARN("Failed to check new row legitimacy", K(ret), K(rows_info));
    }
  }
  // 2. Insert rows with uniqueness constraint and write conflict checking.
  if (OB_SUCC(ret)) {
    if (OB_FAIL(insert_rows_wrap(tablet_handle,
                                 table,
                                 run_ctx.store_ctx_,
                                 run_ctx.dml_param_,
                                 check_exists,
                                 *run_ctx.col_descs_,
                                 rows_info))) {
      if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
        blocksstable::ObDatumRowkey &duplicate_rowkey = rows_info.get_conflict_rowkey();
        LOG_WARN("Rowkey already exist", K(ret), K(duplicate_rowkey), K(row_count),
            K(rows_info.get_conflict_idx()), "need_find_all_duplicate_key", rows_info.need_find_all_duplicate_key());
#ifndef OB_BUILD_PACKAGE
        if (table.is_fts_index()) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected error, duplicated row", K(ret), K(table));
        }
#endif
      } else if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("Failed to insert rows to tablet", K(ret), K(rows_info));
      }
    }
  }
  // 3. Log user error message if rowkey is duplicate.
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
    if (OB_FAIL(insert_rows_wrap(tablet_handle,
                                 table,
                                 run_ctx.store_ctx_,
                                 run_ctx.dml_param_,
                                 false /* check_exists */,
                                 *run_ctx.col_descs_,
                                 rows_info))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret) {
        LOG_WARN("Failed to insert rows to tablet", K(ret), K(rows_info));
      }
    }
  }
  return ret;
}

OB_INLINE int ObLSTabletService::check_rowkey_length(const ObDMLRunningCtx &run_ctx, const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  int64_t rowkey_length = 0;
  const int64_t rowkey_column_num = run_ctx.relative_table_.get_rowkey_column_num();
  if (run_ctx.has_lob_rowkey_) {
    for (int64_t i = 0; i < rowkey_column_num; ++i) {
      rowkey_length += datum_row.storage_datums_[i].len_;
    }
    if (rowkey_length > OB_MAX_VARCHAR_LENGTH_KEY) {
      ret = OB_ERR_TOO_LONG_KEY_LENGTH;
      LOG_USER_ERROR(OB_ERR_TOO_LONG_KEY_LENGTH, OB_MAX_VARCHAR_LENGTH_KEY);
      STORAGE_LOG(WARN, "rowkey is too long", K(ret), K(rowkey_length), K(rowkey_column_num), K(datum_row));
    }
  }
  return ret;
}

int ObLSTabletService::process_lob_before_insert(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &datum_row,
    const int16_t row_idx)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = run_ctx.col_descs_->count();
  ObLobManager *lob_mngr = MTL(ObLobManager*);
  const ObTableSchemaParam &table_param = run_ctx.dml_param_.table_param_->get_data_table();
  if (OB_ISNULL(lob_mngr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]failed to get lob manager handle.", K(ret));
  } else if (datum_row.count_ != col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]column count invalid", K(ret), K(col_cnt), K(datum_row.count_), KPC(run_ctx.col_descs_));
  } else if (table_param.is_vector_index_snapshot()) {
    // dml insert to 5 table skip insert lob locator;
    LOG_DEBUG("dml insert to 5 table skip insert lob locator");
  } else {
    const int64_t cur_time = ObClockGenerator::getClock();
    const int64_t relative_timeout = run_ctx.dml_param_.timeout_ - cur_time;
    if (OB_UNLIKELY(relative_timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout has reached", K(ret), "timeout", run_ctx.dml_param_.timeout_, K(cur_time));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObColDesc &column = run_ctx.col_descs_->at(i);
      ObStorageDatum &datum = datum_row.storage_datums_[i];
      if (datum.is_null() || datum.is_nop_value()) {
        // do nothing
      } else if (column.col_type_.is_lob_storage()) {
        if (OB_FAIL(ObLobTabletDmlHelper::process_lob_column_before_insert(tablet_handle, run_ctx, datum_row, row_idx, i, datum))) {
          LOG_WARN("process_lob_column_before_insert fail", K(ret), K(column), K(i), K(datum), K(datum_row));
        }
      }
    }
    if (OB_SUCC(ret) && OB_FAIL(check_rowkey_length(run_ctx, datum_row))) {
      LOG_WARN("failed to check rowkey length", K(ret), K(datum_row));
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

int ObLSTabletService::process_lob_before_insert(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow *rows,
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
      if (OB_FAIL(process_lob_before_insert(tablet_handle, run_ctx, rows[k], k))) {
        LOG_WARN("[STORAGE_LOB]failed to insert lob row.", K(ret), K(k));
      }
    }
  }
  return ret;
}

int ObLSTabletService::insert_vector_index_rows(
      ObTabletHandle &data_tablet,
      ObDMLRunningCtx &run_ctx,
      blocksstable::ObDatumRow *rows,
      int64_t row_count)
{
  int ret = OB_SUCCESS;
  const ObTableSchemaParam &table_param = run_ctx.dml_param_.table_param_->get_data_table();
  if (table_param.is_vector_delta_buffer()) {
    ObString vec_idx_param = run_ctx.dml_param_.table_param_->get_data_table().get_vec_index_param();
    int64_t vec_dim = run_ctx.dml_param_.table_param_->get_data_table().get_vec_dim();
    const uint64_t vec_id_col_id = run_ctx.dml_param_.table_param_->get_data_table().get_vec_id_col_id();
    const uint64_t vec_vector_col_id = run_ctx.dml_param_.table_param_->get_data_table().get_vec_vector_col_id();
    const uint64_t vec_type_col_id = vec_vector_col_id - 1;
    LOG_DEBUG("[vec index debug] show vector index params", K(vec_idx_param), K(vec_dim),
              K(vec_id_col_id), K(vec_type_col_id), K(vec_vector_col_id));
    // get vector col idx
    int64_t vec_id_idx = OB_INVALID_INDEX;
    int64_t type_idx = OB_INVALID_INDEX;
    int64_t vector_idx = OB_INVALID_INDEX;
    int64_t extra_info_actual_size = 0;
    // get extra info col idx
    // delta_buffer table columns def is: <vid, type, vector, extra_infos>
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    if (OB_FAIL(vec_index_service->acquire_adapter_guard(run_ctx.store_ctx_.ls_id_,
                                                        run_ctx.relative_table_.get_tablet_id(),
                                                        ObIndexType::INDEX_TYPE_VEC_DELTA_BUFFER_LOCAL,
                                                        adaptor_guard,
                                                        &vec_idx_param,
                                                        vec_dim))) {
      LOG_WARN("fail to get ObPluginVectorIndexAdapter", K(ret), K(run_ctx.store_ctx_), K(run_ctx.relative_table_));
    } else if (OB_FAIL(adaptor_guard.get_adatper()->get_extra_info_actual_size(extra_info_actual_size))) {
      LOG_WARN("fail to get extra info actual size", K(ret), K(extra_info_actual_size));
    }
    ObArray<share::ObExtraIdxType> extra_info_id_types;
    for (int64_t i = 0; OB_SUCC(ret) && i < run_ctx.dml_param_.table_param_->get_col_descs().count(); i++) {
      uint64_t col_id = run_ctx.dml_param_.table_param_->get_col_descs().at(i).col_id_;
      if (col_id == vec_id_col_id) {
        vec_id_idx = i;
      } else if (col_id == vec_type_col_id) {
        type_idx = i;
      } else if (col_id == vec_vector_col_id) {
        vector_idx = i;
      } else if (extra_info_actual_size > 0){
        // has extra_info
        ObExtraIdxType extra_idx_type;
        extra_idx_type.idx_ = i;
        extra_idx_type.type_= run_ctx.dml_param_.table_param_->get_col_descs().at(i).col_type_;
        if (OB_FAIL(extra_info_id_types.push_back(extra_idx_type))) {
          LOG_WARN("fail to push back extra info idx", K(ret), K(extra_info_id_types), K(col_id));
        }
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_UNLIKELY(vec_id_idx == OB_INVALID_INDEX || type_idx == OB_INVALID_INDEX || vector_idx == OB_INVALID_INDEX)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to get vec index column idxs", K(ret), K(vec_id_col_id), K(vec_type_col_id), K(vec_vector_col_id),
          K(vec_id_idx), K(type_idx), K(vector_idx));
    } else {
      if (OB_FAIL(adaptor_guard.get_adatper()->insert_rows(rows, vec_id_idx, type_idx, vector_idx, extra_info_id_types, row_count))) {
        LOG_WARN("fail to insert vector to adaptor", K(ret), KP(rows), K(row_count));
      } else {
        for (int64_t k = 0; OB_SUCC(ret) && k < row_count; k++) {
          // process for each row or call batch
          LOG_DEBUG("show all vector del buffer row for insert", K(rows[k].storage_datums_));
          // set vector null for not to storage
          rows[k].storage_datums_[vector_idx].set_null();
        }
        adaptor_guard.get_adatper()->update_can_skip(NOT_SKIP);
      }
    }
  } else if (table_param.is_ivf_vector_index()) { // check outrow
    ObLobManager *lob_mngr = MTL(ObLobManager*);
    for (int64_t k = 0; OB_SUCC(ret) && k < row_count; k++) {
      blocksstable::ObDatumRow &datum_row = rows[k];
      int64_t col_cnt = run_ctx.col_descs_->count();
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        const ObColDesc &column = run_ctx.col_descs_->at(i);
        ObStorageDatum &datum = datum_row.storage_datums_[i];
        if (datum.is_null() || datum.is_nop_value()) {
          // do nothing
        } else if (column.col_type_.is_lob_storage()) {
          ObString raw_data = datum.get_string();
          bool has_lob_header = datum.has_lob_header() && raw_data.length() > 0;
          ObLobLocatorV2 src_data_locator(raw_data, has_lob_header);
          int64_t new_byte_len = 0;
          if (OB_FAIL(src_data_locator.get_lob_data_byte_len(new_byte_len))) {
            LOG_WARN("fail to get lob byte len", K(ret));
          } else if (new_byte_len > table_param.get_lob_inrow_threshold()) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected outrow datum in ivf vector index", K(ret), K(new_byte_len),
                K(table_param.get_lob_inrow_threshold()));
          }
        }
      }
    }
  } else if (OB_UNLIKELY(run_ctx.dml_param_.table_param_->get_data_table().is_vector_index_id())) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    share::SCN current_scn;
    ObString vec_idx_param = run_ctx.dml_param_.table_param_->get_data_table().get_vec_index_param();
    if (OB_FAIL(vec_index_service->acquire_adapter_guard(run_ctx.store_ctx_.ls_id_,
                                                        run_ctx.relative_table_.get_tablet_id(),
                                                        ObIndexType::INDEX_TYPE_VEC_INDEX_ID_LOCAL,
                                                        adaptor_guard,
                                                        &vec_idx_param))) {
      LOG_WARN("fail to get ObPluginVectorIndexAdapter", K(ret), K(run_ctx.store_ctx_), K(run_ctx.relative_table_));
    } else {
      adaptor_guard.get_adatper()->update_index_id_dml_scn(run_ctx.store_ctx_.mvcc_acc_ctx_.snapshot_.version_);
      adaptor_guard.get_adatper()->update_can_skip(NOT_SKIP);
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
      } else if (!is_shadow_column(column_id)) {
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
    blocksstable::ObDatumRowIterator *row_iter,
    blocksstable::ObDatumRow *&rows,
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
        } else if (is_shadow_column(cid)) {
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


int ObLSTabletService::process_lob_before_update(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    const int64_t row_count,
    blocksstable::ObDatumRow *old_rows,
    blocksstable::ObDatumRow *new_rows)
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = run_ctx.col_descs_->count();
  const ObTableSchemaParam &table_param = run_ctx.dml_param_.table_param_->get_data_table();

  if (table_param.is_vector_index_snapshot()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected process vec table update in process_lob_before_update", K(ret));
  } else if (OB_FAIL(update_lob_meta_table_seq_no(run_ctx, 1/*row_count*/))) {
    LOG_WARN("update_lob_meta_table_seq_no fail", K(ret), K(run_ctx.dml_param_));
  } else {
    const int64_t cur_time = ObClockGenerator::getClock();
    const int64_t relative_timeout = run_ctx.dml_param_.timeout_ - cur_time;
    if (OB_UNLIKELY(relative_timeout <= 0)) {
      ret = OB_TIMEOUT;
      LOG_WARN("timeout has reached", K(ret), "timeout", run_ctx.dml_param_.timeout_, K(cur_time));
    }

    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObColDesc &column = run_ctx.col_descs_->at(i);
      if (column.col_type_.is_lob_storage()) {
        bool is_col_update = false;
        const bool is_rowkey_col = i < run_ctx.relative_table_.get_rowkey_column_num();
        for (int64_t j = 0; !is_rowkey_col && !is_col_update && j < update_idx.count(); ++j) {
          if (update_idx.at(j) == i) {
            is_col_update = true;
          }
        }

        for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; row_idx++) {
          ObStorageDatum &old_datum = old_rows[row_idx].storage_datums_[i];
          ObStorageDatum &new_datum = new_rows[row_idx].storage_datums_[i];
          if (is_rowkey_col || is_col_update) {
            // get new lob locator
            ObString new_lob_str = (new_datum.is_null() || new_datum.is_nop_value())
                                   ? ObString(0, nullptr) : new_datum.get_string();
            // for not strict sql mode, will insert empty string without lob header
            bool has_lob_header = new_datum.has_lob_header() && new_lob_str.length() > 0;
            ObLobLocatorV2 new_lob(new_lob_str, has_lob_header);
            if (OB_FAIL(ret)) {
            } else if (new_datum.is_null() ||
                       new_datum.is_nop_value() ||
                       new_lob.is_full_temp_lob() ||
                       new_lob.is_persist_lob() ||
                       (new_lob.is_lob_disk_locator() && new_lob.has_inrow_data())) {
              if (OB_FAIL(ObLobTabletDmlHelper::process_lob_column_before_update(
                  run_ctx, old_rows[row_idx], new_rows[row_idx], rowkey_change, row_idx, i, old_datum, new_datum))) {
                LOG_WARN("process_lob_column_before_update fail", K(ret), K(column), K(i), K(row_idx), K(new_datum), K(new_lob));
              }
            } else if (new_lob.is_delta_temp_lob()) {
              if (OB_FAIL(ObLobTabletDmlHelper::process_delta_lob(run_ctx, old_rows[row_idx], i, old_datum, new_lob, new_datum))) {
                LOG_WARN("failed to process delta lob.", K(ret), K(i), K(row_idx));
              }
            } else {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("unexpected obj for new lob", K(ret), K(i), K(row_idx), K(new_datum), K(new_lob));
            }
          } else {
            if (old_datum.is_null()) {
              new_datum.set_null();
            } else if (old_datum.is_nop_value()) {
              new_datum.set_nop();
            } else if (new_datum.is_nop_value() || new_datum.is_null()) {
              // do nothing
            } else {
              ObString val_str = old_datum.get_string();
              ObLobCommon *lob_common = reinterpret_cast<ObLobCommon*>(val_str.ptr());
              if (!lob_common->in_row_ && (rowkey_change || run_ctx.is_delete_insert_table_)) {
                if (val_str.length() < ObLobManager::LOB_WITH_OUTROW_CTX_SIZE) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("not enough space for lob header", K(ret), K(val_str), K(i));
                } else if (OB_FAIL(ObLobTabletDmlHelper::process_lob_column_before_update(
                      run_ctx, old_rows[row_idx], new_rows[row_idx], rowkey_change, row_idx, i, old_datum, new_datum))) {
                  LOG_WARN("process_lob_column_before_update fail", K(ret), K(column), K(i), K(row_idx), K(new_datum));
                }
              } else if (!lob_common->in_row_) {
                if (OB_FAIL(ObLobTabletDmlHelper::register_ext_info_commit_cb(run_ctx, column, old_rows[row_idx].storage_datums_[i]))) {
                  LOG_WARN("register ext info commit cb fail", K(ret), K(row_idx), K(i), K(old_rows[row_idx]));
                }
              } else {
                new_datum.reuse();
                new_datum.set_string(val_str.ptr(), val_str.length());
                if (old_datum.has_lob_header()) {
                  new_datum.set_has_lob_header();
                }
              }
            }
          }
        } // end of for row
      }
    } // end of for column
    for (int64_t row_idx = 0; OB_SUCC(ret) && row_idx < row_count; row_idx++) {
      if (OB_FAIL(check_rowkey_length(run_ctx, new_rows[row_idx]))) {
        LOG_WARN("failed to check rowkey length", K(ret), K(row_idx), K(new_rows[row_idx]));
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
    ObDatumRow *tmp_rows,
    ObRowsInfo &old_rows_info,
    ObRowsInfo &new_rows_info,
    ObDatumRowStore &row_store)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  const ObColDescIArray &col_descs = *run_ctx.col_descs_;
  const int64_t row_count = new_rows_info.get_rowkey_cnt();

  if (OB_UNLIKELY(!old_rows_info.is_valid() || !new_rows_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid rows info", K(ret), K(old_rows_info), K(new_rows_info));
  } else if (OB_FAIL(process_old_rows(tablet_handle,
                                      run_ctx,
                                      rowkey_change,
                                      lob_update,
                                      tmp_rows,
                                      old_rows_info))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process old rows", K(ret), K(old_rows_info),
        K(col_descs), K(rowkey_change), K(lob_update), K(row_count));
    }
  } else if (OB_FAIL(insert_vector_index_rows(tablet_handle, run_ctx, new_rows_info.rows_, row_count))) {
    LOG_WARN("failed to process vector index insert", K(ret), K(new_rows_info));
  } else if (delay_new) {
    if (OB_FAIL(cache_rows_to_row_store(row_count,
                                        old_rows_info.rows_,
                                        new_rows_info.rows_,
                                        row_store))) {
      LOG_WARN("fail to cache_rows_to_row_store", K(ret), K(row_count));
    }
  } else if (OB_FAIL(process_lob_before_update(tablet_handle,
                                               run_ctx,
                                               update_idx,
                                               rowkey_change,
                                               row_count,
                                               old_rows_info.rows_,
                                               new_rows_info.rows_))) {
    LOG_WARN("process_lob_before_update fail", K(ret), K(row_count));
  } else if (OB_FAIL(process_new_rows(tablet_handle,
                                      run_ctx,
                                      update_idx,
                                      rowkey_change,
                                      old_rows_info,
                                      new_rows_info))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process new row", K(ret), K(old_rows_info), K(new_rows_info));
    }
  } else if (OB_FAIL(process_lob_after_update(tablet_handle,
                                              run_ctx,
                                              update_idx,
                                              rowkey_change,
                                              row_count,
                                              old_rows_info.rows_,
                                              new_rows_info.rows_))) {
    LOG_WARN("process_lob_after_update fail", K(ret), K(row_count));
  }
  return ret;
}

int ObLSTabletService::cache_rows_to_row_store(const int64_t row_count,
                                               ObDatumRow *old_rows,
                                               ObDatumRow *new_rows,
                                               ObDatumRowStore &row_store)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(row_store.add_row(new_rows[i]))) {
      LOG_WARN("failed to store new row", K(i), K(new_rows[i]), K(ret));
    } else if (OB_FAIL(row_store.add_row(old_rows[i]))) {
      LOG_WARN("failed to store old row", K(i), K(old_rows[i]), K(ret));
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
    ObDatumRow &old_datum_row,
    ObDatumRow &new_datum_row,
    ObDatumRowStore &row_store)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  const ObColDescIArray &col_descs = *run_ctx.col_descs_;

  if (OB_UNLIKELY(col_descs.count() != old_datum_row.count_ || col_descs.count() != new_datum_row.count_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(col_descs.count()), K(old_datum_row.count_), K(new_datum_row.count_));
  } else if (OB_FAIL(process_old_row(tablet_handle,
                                     run_ctx,
                                     rowkey_change,
                                     lob_update,
                                     old_datum_row))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process old row", K(ret), K(*run_ctx.col_descs_), K(old_datum_row), K(rowkey_change));
    }
  } else if (OB_FAIL(insert_vector_index_rows(tablet_handle, run_ctx, &new_datum_row, 1))) {
    LOG_WARN("failed to process vector index insert", K(ret), K(new_datum_row));
  } else if (delay_new) {
    if (OB_FAIL(cache_rows_to_row_store(1, &old_datum_row, &new_datum_row, row_store))) {
      LOG_WARN("fail to cache_rows_to_row_store", K(ret));
    }
  } else if (OB_FAIL(process_lob_before_update(tablet_handle,
                                               run_ctx,
                                               update_idx,
                                               rowkey_change,
                                               1,
                                               &old_datum_row,
                                               &new_datum_row))) {
    LOG_WARN("process_lob_before_update fail", K(ret), K(old_datum_row), K(new_datum_row));
  } else if (OB_FAIL(process_new_row(tablet_handle,
                                     run_ctx,
                                     update_idx,
                                     rowkey_change,
                                     old_datum_row,
                                     new_datum_row))) {
    if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
      LOG_WARN("fail to process new row", K(new_datum_row), K(ret));
    }
  } else if (OB_FAIL(process_lob_after_update(tablet_handle,
                                              run_ctx,
                                              update_idx,
                                              rowkey_change,
                                              1,
                                              &old_datum_row,
                                              &new_datum_row))) {
    LOG_WARN("process_lob_after_update fail", K(ret), K(old_datum_row), K(new_datum_row));
  }

  return ret;
}

int ObLSTabletService::process_old_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const bool rowkey_change,
    const bool lob_update,
    ObDatumRow *tmp_rows,
    ObRowsInfo &old_rows_info)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &store_ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  bool is_delete_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
  int64_t error_row_idx = 0;
  ObDatumRow *old_rows = old_rows_info.rows_;
  const int64_t row_count = old_rows_info.get_rowkey_cnt();

  if (OB_UNLIKELY(!relative_table.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid relative tables", K(ret), K(relative_table));
  } else if (OB_UNLIKELY(!store_ctx.is_valid()
      || nullptr == run_ctx.col_descs_
      || run_ctx.col_descs_->count() <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(store_ctx), KP(run_ctx.col_descs_), K(is_delete_total_quantity_log));
  } else if (OB_UNLIKELY(!run_ctx.dml_param_.table_param_->get_data_table().is_index_table() &&
                         run_ctx.is_delete_insert_table_ != store_ctx.mvcc_acc_ctx_.write_flag_.is_delete_insert())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected delete insert status", K(ret), K(store_ctx.mvcc_acc_ctx_.write_flag_),
              KPC(tablet_handle.get_obj()), K(run_ctx.dml_param_.table_param_->get_data_table()));
  } else if (OB_FAIL(check_old_row_legitimacy_wrap(run_ctx.cmp_funcs_,
      tablet_handle, run_ctx, row_count, old_rows, error_row_idx))) {
    LOG_WARN("check old row legitimacy failed", K(error_row_idx), K(old_rows[error_row_idx]));
  } else if (OB_FAIL(process_old_rows_lob_col(tablet_handle, run_ctx, row_count, old_rows))){
    LOG_WARN("failed to process old rows lob col", K(ret));
  } else {
    ObColDescIArray &col_descs = const_cast<ObColDescIArray&>(*run_ctx.col_descs_);
    const uint64_t &table_id = relative_table.get_table_id();
    if (OB_UNLIKELY(run_ctx.dml_param_.prelock_)) {
      bool locked = false;
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        const ObDatumRowkey &datum_rowkey = old_rows_info.get_rowkey(i);
        if (OB_FAIL(check_row_locked_by_myself_wrap(tablet_handle,
            run_ctx.dml_param_.data_row_for_lob_, relative_table, store_ctx, datum_rowkey, locked))) {
          LOG_WARN("fail to check row locked", K(ret), K(datum_rowkey));
        } else if (!locked) {
          ret = OB_ERR_ROW_NOT_LOCKED;
          LOG_DEBUG("row has not been locked", K(ret), K(datum_rowkey));
        }
      }
    }

    if (OB_FAIL(ret)) {
    } else if (rowkey_change || run_ctx.is_delete_insert_table_) {
      if (OB_ISNULL(tmp_rows)) { // tmp_rows must be not null if rowkey changed
        ret = OB_INVALID_ARGUMENT;
        LOG_WARN("invalid tmp rows", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        old_rows[i].row_flag_.set_flag(ObDmlFlag::DF_DELETE);
        if (OB_FAIL(tmp_rows[i].shallow_copy(old_rows[i]))) {
          LOG_WARN("fail to shallow copy", K(i));
        } else {
          tmp_rows[i].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
        }
      }
      if (OB_SUCC(ret)) {
        ObSEArray<int64_t, 8> update_idx;
        if (OB_FAIL(delete_lob_tablet_rows(tablet_handle, run_ctx, old_rows, row_count))){
          LOG_WARN("failed to delete lob rows.", K(ret), K(row_count));
        } else if (OB_FAIL(update_rows_wrap(tablet_handle,
                                     run_ctx.dml_param_.data_row_for_lob_,
                                     relative_table,
                                     run_ctx.store_ctx_,
                                     col_descs,
                                     update_idx,
                                     tmp_rows,
                                     run_ctx.dml_param_.encrypt_meta_,
                                     old_rows_info))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to update rows", K(ret), K(old_rows_info));
          }
        }
      }
    } else if (lob_update) {
      // need to lock main table rows that don't need to be deleted
      for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
        const ObDatumRowkey &datum_rowkey = old_rows_info.get_rowkey(i);
        if (OB_FAIL(lock_row_wrap(tablet_handle, run_ctx.dml_param_.data_row_for_lob_, relative_table, store_ctx, datum_rowkey))) {
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
    ObDatumRow &datum_row)
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
      || !datum_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(store_ctx), KP(run_ctx.col_descs_), K(datum_row), K(is_delete_total_quantity_log));
  } else if (OB_UNLIKELY(!run_ctx.dml_param_.table_param_->get_data_table().is_index_table() &&
                         run_ctx.is_delete_insert_table_ != store_ctx.mvcc_acc_ctx_.write_flag_.is_delete_insert())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected delete insert status", K(ret), K(store_ctx.mvcc_acc_ctx_.write_flag_),
              KPC(tablet_handle.get_obj()), K(run_ctx.dml_param_.table_param_->get_data_table()));
  } else if (OB_FAIL(check_old_row_legitimacy_wrap(run_ctx.cmp_funcs_, tablet_handle, run_ctx, 1, &datum_row, error_row_idx))) {
    LOG_WARN("check old row legitimacy failed", K(ret), K(datum_row));
  } else if (OB_FAIL(process_old_row_lob_col(tablet_handle, run_ctx, datum_row))){
    LOG_WARN("failed to process old row lob col", K(ret), K(datum_row));
  } else {
    ObColDescIArray &col_descs = const_cast<ObColDescIArray&>(*run_ctx.col_descs_);
    const uint64_t &table_id = relative_table.get_table_id();
    int64_t rowkey_size = relative_table.get_rowkey_column_num();
    ObDatumRowkey datum_rowkey;
    ObDatumRowkeyHelper rowkey_helper(run_ctx.allocator_);
    if (OB_UNLIKELY(run_ctx.dml_param_.prelock_)) {
      bool locked = false;
      if (OB_FAIL(rowkey_helper.prepare_datum_rowkey(datum_row, rowkey_size, col_descs, datum_rowkey))) {
        LOG_WARN("Failed to prepare rowkey", K(ret), K(datum_row), K(rowkey_size), K(datum_rowkey));
      } else if (OB_FAIL(check_row_locked_by_myself_wrap(tablet_handle, run_ctx.dml_param_.data_row_for_lob_, relative_table, store_ctx, datum_rowkey, locked))) {
        LOG_WARN("fail to check row locked", K(ret), K(datum_row), K(datum_rowkey));
      } else if (!locked) {
        ret = OB_ERR_ROW_NOT_LOCKED;
        LOG_DEBUG("row has not been locked", K(ret), K(datum_row), K(datum_rowkey));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (rowkey_change || run_ctx.is_delete_insert_table_) {
      ObDatumRow del_row;
      ObDatumRow new_row;

      ObSEArray<int64_t, 8> update_idx;
      if (OB_FAIL(del_row.shallow_copy(datum_row))) {
        LOG_WARN("failed to shallow copy datum row", K(ret), K(datum_row), K(del_row));
      } else if (FALSE_IT(del_row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE))) {
      } else if (OB_FAIL(new_row.shallow_copy(datum_row))) {
        LOG_WARN("failed to shallow copy datum row", K(ret), K(datum_row), K(new_row));
      } else if (FALSE_IT(new_row.row_flag_.set_flag(ObDmlFlag::DF_DELETE))) {
      } else if (OB_FAIL(delete_lob_tablet_rows(tablet_handle, run_ctx, datum_row))){
        LOG_WARN("failed to delete lob rows.", K(ret), K(datum_row));
      } else if (OB_FAIL(update_row_wrap(tablet_handle,
                                         run_ctx.dml_param_.data_row_for_lob_,
                                         relative_table,
                                         run_ctx.store_ctx_,
                                         col_descs,
                                         update_idx,
                                         del_row,
                                         new_row,
                                         run_ctx.dml_param_.encrypt_meta_))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to write data tablet row", K(ret), K(del_row), K(new_row));
        }
      }
    } else if (lob_update) {
      // need to lock main table rows that don't need to be deleted
      if (OB_FAIL(rowkey_helper.prepare_datum_rowkey(datum_row, rowkey_size, col_descs, datum_rowkey))) {
        LOG_WARN("Failed to prepare rowkey", K(ret), K(datum_row), K(rowkey_size));
      } else if (OB_FAIL(lock_row_wrap(tablet_handle, run_ctx.dml_param_.data_row_for_lob_, relative_table, store_ctx, datum_rowkey))) {
        if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("lock row failed", K(ret), K(table_id), K(datum_row), K(rowkey_size), K(datum_rowkey));
        }
      }
      LOG_DEBUG("generate lock node before update lob columns", K(ret), K(table_id), K(datum_row), K(datum_rowkey));
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
    LOG_WARN("fail to check is delete insert table", K(ret));
  } else if (OB_UNLIKELY(!run_ctx.dml_param_.table_param_->get_data_table().is_index_table() &&
                         run_ctx.is_delete_insert_table_ != run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_delete_insert())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected delete insert status", K(ret), K(run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_),
              KPC(tablet_handle.get_obj()), K(run_ctx.dml_param_.table_param_->get_data_table()));
  } else if (GCONF.enable_defensive_check()
      && OB_FAIL(check_new_row_legitimacy(run_ctx, row_count, new_rows_info.rows_))) {
    LOG_WARN("check new row legitimacy failed", K(ret), K(new_rows_info));
  } else {
    const ObColDescIArray &col_descs = *run_ctx.col_descs_;
    ObRelativeTable &relative_table = run_ctx.relative_table_;
    bool is_update_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
    const common::ObTimeZoneInfo *tz_info = run_ctx.dml_param_.tz_info_;
    const int64_t row_count = new_rows_info.get_rowkey_cnt();

    if (!rowkey_change) {
      if (run_ctx.is_delete_insert_table_) {
        for (int64_t i = 0; i < row_count; i++) {
          new_rows_info.rows_[i].row_flag_.set_flag(ObDmlFlag::DF_INSERT);
        }
      }
      if (!is_update_total_quantity_log) {
        // For minimal mode, set pk columns of old_row to nop value, because
        // they are already stored in new_row.
        const int64_t rowkey_col_cnt = relative_table.get_rowkey_column_num();
        for (int64_t i = 0; i < row_count; i++) {
          for (int64_t j = 0; j < rowkey_col_cnt; ++j) {
            (old_rows_info.rows_[i].storage_datums_[j]).set_nop();
          }
        }
      }
      if (OB_FAIL(update_rows_wrap(tablet_handle,
                                   run_ctx.dml_param_.data_row_for_lob_,
                                   relative_table,
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
      for (int64_t i = 0; i < row_count; i++) {
        new_rows_info.rows_[i].row_flag_.set_flag(ObDmlFlag::DF_INSERT);
      }
      const bool check_exist = !relative_table.is_storage_index_table() || relative_table.is_unique_index() ||
                               run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_update_pk_dop() ||
                               run_ctx.store_ctx_.mvcc_acc_ctx_.write_flag_.is_immediate_row_check();

      if (OB_FAIL(insert_rows_wrap(tablet_handle,
                                   relative_table,
                                   run_ctx.store_ctx_,
                                   run_ctx.dml_param_,
                                   check_exist,
                                   col_descs,
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
    const ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    const ObDatumRow &old_datum_row,
    ObDatumRow &new_datum_row)
{
  int ret = OB_SUCCESS;
  ObStoreCtx &ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  const bool is_update_total_quantity_log = run_ctx.dml_param_.is_total_quantity_log_;
  const common::ObTimeZoneInfo *tz_info = run_ctx.dml_param_.tz_info_;
  if (OB_UNLIKELY(!ctx.is_valid()
      || !relative_table.is_valid()
      || nullptr == run_ctx.col_descs_
      || run_ctx.col_descs_->count() <= 0
      || update_idx.count() < 0
      || (is_update_total_quantity_log && !old_datum_row.is_valid())
      || !new_datum_row.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(ctx),
        KP(run_ctx.col_descs_), K(update_idx), K(old_datum_row), K(new_datum_row),
        K(is_update_total_quantity_log), K(rowkey_change));
  } else if (OB_UNLIKELY(!run_ctx.dml_param_.table_param_->get_data_table().is_index_table() &&
                         run_ctx.is_delete_insert_table_ != ctx.mvcc_acc_ctx_.write_flag_.is_delete_insert())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("Unexpected delete insert status", K(ret), K(ctx.mvcc_acc_ctx_.write_flag_),
              KPC(tablet_handle.get_obj()), K(run_ctx.dml_param_.table_param_->get_data_table()));
  } else {
    const ObColDescIArray &col_descs = *run_ctx.col_descs_;
    if (run_ctx.is_delete_insert_table_ && !rowkey_change) {
      new_datum_row.row_flag_.set_flag(ObDmlFlag::DF_INSERT);
    } else {
      new_datum_row.row_flag_.set_flag(rowkey_change ? ObDmlFlag::DF_INSERT : ObDmlFlag::DF_UPDATE);
    }
    if (!rowkey_change) {
      ObDatumRow old_row;
      if (OB_FAIL(old_row.shallow_copy(old_datum_row))) {
        LOG_WARN("failed to shallow copy datum row", K(ret), K(old_datum_row), K(old_row));
      } else {
        old_row.row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
        if (!is_update_total_quantity_log) {
          // For minimal mode, set pk columns of old_row to nop value, because
          // they are already stored in new_row.
          const int64_t rowkey_col_cnt = relative_table.get_rowkey_column_num();
          for (int64_t i = 0; i < rowkey_col_cnt; ++i) {
            (old_row.storage_datums_[i]).set_nop();
          }
        }
        if (OB_FAIL(update_row_wrap(tablet_handle, run_ctx.dml_param_.data_row_for_lob_, relative_table,
            ctx, col_descs, update_idx, old_row, new_datum_row, run_ctx.dml_param_.encrypt_meta_))) {
          if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
            LOG_WARN("failed to update to row", K(ret), K(old_row), K(new_datum_row));
          }
        }
      }
    } else {
      const bool check_exist = !relative_table.is_storage_index_table() ||
                               relative_table.is_unique_index() ||
                               ctx.mvcc_acc_ctx_.write_flag_.is_update_pk_dop() ||
                               ctx.mvcc_acc_ctx_.write_flag_.is_immediate_row_check();
      if (OB_FAIL(insert_row_wrap(tablet_handle,
                                  run_ctx.dml_param_.data_row_for_lob_,
                                  relative_table,
                                  ctx,
                                  check_exist,
                                  col_descs,
                                  new_datum_row,
                                  run_ctx.dml_param_.encrypt_meta_))) {
        if (OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
          char buffer[OB_TMP_BUF_SIZE_256];
          ObDatumRowkey rowkey;
          if (OB_SUCCESS != rowkey.assign(new_datum_row.storage_datums_, relative_table.get_rowkey_column_num())) {
            LOG_WARN("Failed to assign rowkey", K(new_datum_row));
          } else if (OB_SUCCESS != extract_rowkey(relative_table, rowkey, buffer, OB_TMP_BUF_SIZE_256, tz_info)) {
            LOG_WARN("extract rowkey failed", K(rowkey));
          } else {
            ObString index_name = "PRIMARY";
            if (relative_table.is_index_table()) {
              relative_table.get_index_name(index_name);
            }
            LOG_USER_ERROR(OB_ERR_PRIMARY_KEY_DUPLICATE, buffer, index_name.length(), index_name.ptr());
          }
          LOG_WARN("rowkey already exists", K(ret), K(new_datum_row));
        } else if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
          LOG_WARN("failed to update to row", K(ret), K(new_datum_row));
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_datum_row_nullable_value(const ObIArray<ObColDesc> &col_descs,
                                                    ObRelativeTable &relative_table,
                                                    const blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(col_descs.count() > datum_row.get_column_count())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("new row is invalid", K(ret), K(datum_row.get_column_count()), K(col_descs.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < col_descs.count(); ++i) {
    uint64_t column_id = col_descs.at(i).col_id_;
    bool is_nullable = false;
    if (datum_row.storage_datums_[i].is_nop()) {
      //nothing
    } else if (OB_UNLIKELY(is_shadow_column(column_id))) {
      //the shadow pk is generated internally,
      //and the nullable attribute check for it is skipped
    } else if (OB_FAIL(relative_table.is_column_nullable_for_write(column_id, is_nullable))) {
      LOG_WARN("check is_column_nullable_for_write failed", K(ret), K(column_id));
    } else if (datum_row.storage_datums_[i].is_null() && !is_nullable) {
      bool is_hidden = false;
      bool is_gen_col = false;
      bool is_nullable_for_read = false;
      if (OB_FAIL(relative_table.is_column_nullable_for_read(column_id, is_nullable_for_read))) {
        LOG_WARN("check is nullable for read failed", K(ret));
      } else if (is_nullable_for_read) {
        //this column is not null novalidate, maybe the null column come from the old data
        //so output trace log and ignore it
        LOG_TRACE("Catch a defensive nullable error, but this column is not null novalidate",
                  K(column_id), K(col_descs), K(datum_row), K(relative_table));
      } else if (OB_FAIL(relative_table.is_hidden_column(column_id, is_hidden))) {
        LOG_WARN("get is hidden column failed", K(ret), K(column_id));
      } else if (OB_FAIL(relative_table.is_gen_column(column_id, is_gen_col))) {
        LOG_WARN("get is gen column failed", K(ret), K(column_id));
      } else if (is_hidden && !is_gen_col) {
        ret = OB_BAD_NULL_ERROR;
        LOG_WARN("Catch a defensive nullable error, "
                 "maybe cause by add column not null default null ONLINE", K(ret),
                 K(column_id), K(col_descs), K(datum_row), K(relative_table));
      } else {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_datum_row_nullable_value");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      K(column_id), K(col_descs), K(datum_row), K(relative_table));
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
      }
    } else if (!datum_row.storage_datums_[i].is_null() && col_descs.at(i).col_type_.is_number()) {
      number::ObNumber num(datum_row.storage_datums_[i].get_number());
      if (OB_FAIL(num.sanity_check())) {
        LOG_WARN("sanity check number failed", K(ret), K(i), K(datum_row.storage_datums_[i]));
      }
      if (OB_SUCCESS != ret) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_datum_row_nullable_value");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      K(column_id), K(col_descs), K(datum_row), K(relative_table));
        LOG_DBA_ERROR_V2(OB_STORAGE_DEFENSIVE_CHECK_FAIL,
                         OB_ERR_DEFENSIVE_CHECK,
                         "Fatal Error!!! Catch a defensive error!");
      }
    }
  }
  return ret;
}

int ObLSTabletService::check_datum_row_shadow_pk(
    const ObIArray<uint64_t> &column_ids,
    ObRelativeTable &data_table,
    const blocksstable::ObDatumRow &datum_row,
    const blocksstable::ObStorageDatumUtils &rowkey_datum_utils)
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
        rowkey_has_null = datum_row.storage_datums_[i].is_null();
      }
      need_spk = rowkey_has_null;
    } else {
      // oracle兼容：只有unique index key全为null列时，才需要填充shadow列
      bool is_rowkey_all_null = true;
      for (int64_t i = 0; is_rowkey_all_null && i < index_col_cnt; i++) {
        is_rowkey_all_null = datum_row.storage_datums_[i].is_null();
      }
      need_spk = is_rowkey_all_null;
    }
    for (int64_t i = index_col_cnt; OB_SUCC(ret) && i < rowkey_cnt; ++i) {
      uint64_t spk_column_id = column_ids.at(i);
      uint64_t real_pk_id = spk_column_id - OB_MIN_SHADOW_COLUMN_ID;
      const ObStorageDatum &spk_value = datum_row.storage_datums_[i];
      int64_t pk_idx = OB_INVALID_INDEX;
      int cmp = 0;
      if (OB_LIKELY(!need_spk)) {
        if (!spk_value.is_null()) {
          ret = OB_ERR_DEFENSIVE_CHECK;
          ObString func_name = ObString::make_string("check_datum_row_shadow_pk");
          LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
          LOG_ERROR("Fatal Error!!! Catch a defensive error!", K(ret),
                    "column_id", column_ids, K(datum_row), K(data_table),
                    K(spk_value), K(i), K(spk_column_id), K(real_pk_id));
        }
      } else if (OB_UNLIKELY(!has_exist_in_array(column_ids, real_pk_id, &pk_idx))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("real pk column not exists in column_ids", K(ret), K(column_ids), K(real_pk_id));
      } else if (OB_FAIL(rowkey_datum_utils.get_cmp_funcs().at(i).compare(datum_row.storage_datums_[pk_idx], spk_value, cmp)) || 0 != cmp) {
        ret = OB_ERR_DEFENSIVE_CHECK;
        ObString func_name = ObString::make_string("check_datum_row_shadow_pk");
        LOG_USER_ERROR(OB_ERR_DEFENSIVE_CHECK, func_name.length(), func_name.ptr());
        LOG_ERROR_RET(OB_ERR_DEFENSIVE_CHECK,
                      "Fatal Error!!! Catch a defensive error!", K(ret),
                      "column_id", column_ids, K(datum_row), K(data_table),
                      K(spk_value), "pk_value", datum_row.storage_datums_[pk_idx],
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

int ObLSTabletService::process_old_rows_lob_col(
    ObTabletHandle &data_tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const int64_t row_count,
    blocksstable::ObDatumRow *old_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(process_old_row_lob_col(data_tablet_handle, run_ctx, old_rows[i]))) {
      LOG_WARN("fail to process_old_row_lob_col", K(ret), K(i));
    }
  }
  return ret;
}

int ObLSTabletService::process_old_row_lob_col(
    ObTabletHandle &data_tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  bool has_lob_col = false;
  bool need_reread = is_sys_table(run_ctx.relative_table_.get_table_id());
  int64_t col_cnt = run_ctx.col_descs_->count();
  if (datum_row.count_ != col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]Invliad row col cnt", K(ret), K(col_cnt), K(datum_row));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObColDesc &column = run_ctx.col_descs_->at(i);
      if (is_lob_storage(column.col_type_.get_type())) {
        has_lob_col = true;
        ObStorageDatum &datum = datum_row.storage_datums_[i];
        need_reread = need_reread || (!datum.is_null() && !datum.is_nop_value() && !datum.has_lob_header());
        break;
      }
    }
  }
  if (OB_SUCC(ret) && has_lob_col) {
    if (!need_reread) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        const ObColDesc &column = run_ctx.col_descs_->at(i);
        if (is_lob_storage(column.col_type_.get_type())) {
          ObStorageDatum &datum = datum_row.storage_datums_[i];
          bool has_lob_header = datum.has_lob_header();
          if (datum.is_null() || datum.is_nop_value()) {
            // do nothing
          } else if (!has_lob_header) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("lob should have lob locator here.", K(ret), K(i), K(datum));
          } else {
            ObLobLocatorV2 lob(datum.get_string(), has_lob_header);
            ObString disk_loc;
            if (!lob.is_valid()) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("invalid lob locator.", K(ret), K(lob));
            } else if (lob.is_simple()) {
              // do nothing
            } else if (OB_FAIL(lob.get_disk_locator(disk_loc))) {
              LOG_WARN("failed to get disk lob locator.", K(ret), K(lob));
            } else {
              datum.set_string(disk_loc.ptr(), disk_loc.length());
              if (has_lob_header) {
                datum.set_has_lob_header();
              }
            }
          }
        }
      }
    } else {
      if (OB_FAIL(table_refresh_row_wrap(data_tablet_handle, run_ctx, datum_row))) {
        LOG_WARN("[STORAGE_LOB]re-read lob col failed", K(ret));
      }
    }
  }
  return ret;
}

int ObLSTabletService::table_refresh_row(
    ObTabletHandle &data_tablet_handle,
    ObRelativeTable &data_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObColDescIArray &col_descs,
    ObIAllocator &lob_allocator,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  ObArenaAllocator scan_allocator(common::ObMemAttr(MTL_ID(), ObModIds::OB_LOB_ACCESS_BUFFER));
  ObRowGetter storage_row_getter(scan_allocator, *data_tablet_handle.get_obj());

  int64_t col_cnt = col_descs.count();
  ObSEArray<uint64_t, 8> out_col_ids;
  for (int i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
    if (OB_FAIL(out_col_ids.push_back(col_descs.at(i).col_id_))) {
      LOG_WARN("push col id failed.", K(ret), K(i));
    }
  }
  if (OB_FAIL(ret)) {
  } else {
    ObDatumRow *new_row = nullptr;
    if (OB_FAIL(get_storage_row(datum_row, out_col_ids, col_descs, storage_row_getter,
                                data_table, store_ctx, dml_param, new_row))) {
      if (ret == OB_ITER_END) {
        LOG_DEBUG("re-read old row not exist", K(ret), K(datum_row));
        ret = OB_SUCCESS;
      } else {
        LOG_WARN("get next row from single row getter failed", K(ret));
      }
    } else if (OB_ISNULL(new_row)) {
      ret = OB_ERR_NULL_VALUE;
      LOG_WARN("get next row from single row null", K(ret));
    } else if (new_row->count_ != datum_row.count_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("get row from single row col count not equal.", K(ret), K(datum_row.count_), K(new_row->count_));
    } else {
      LOG_DEBUG("get new row success.", K(datum_row), KPC(new_row));
      // only write cells, not write row
      for (int64_t i = 0; OB_SUCC(ret) && i < new_row->count_; ++i) {
        if (OB_FAIL(datum_row.storage_datums_[i].deep_copy(new_row->storage_datums_[i], lob_allocator))) {
          LOG_WARN("copy storage datum error", K(ret), K(i), K(new_row->storage_datums_[i]));
        } else if (col_descs.at(i).col_type_.is_lob_storage()) {
          datum_row.storage_datums_[i].set_has_lob_header();
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::delete_rows_in_tablet(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    ObDatumRow *tmp_rows,
    ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  const ObDMLBaseParam &dml_param = run_ctx.dml_param_;
  ObStoreCtx &ctx = run_ctx.store_ctx_;
  ObRelativeTable &relative_table = run_ctx.relative_table_;
  const ObStorageDatumUtils &datum_utils = dml_param.table_param_->get_data_table().get_read_info().get_datum_utils();
  const int64_t row_count = rows_info.get_rowkey_cnt();
  ObDatumRow *rows = rows_info.rows_;
  int64_t error_row_idx = 0;

  if (OB_FAIL(check_old_row_legitimacy_wrap(
      run_ctx.cmp_funcs_, tablet_handle, run_ctx, row_count, rows, error_row_idx))) {
    LOG_WARN("check old row legitimacy failed", K(rows_info));
  } else if (OB_FAIL(process_old_rows_lob_col(tablet_handle, run_ctx, row_count, rows))){
    LOG_WARN("failed to process old rows lob col", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
      if (OB_FAIL(delete_lob_tablet_rows(tablet_handle, run_ctx, rows[i]))) {
        LOG_WARN("failed to delete lob rows.", K(ret), K(i), K(rows[i]));
      }
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(tmp_rows[i].shallow_copy(rows[i]))) {
      LOG_WARN("failed to shallow copy datum row", K(ret), K(i));
    } else {
      tmp_rows[i].row_flag_.set_flag(ObDmlFlag::DF_UPDATE);
    }
  }
  if (OB_SUCC(ret)) {
    ObSEArray<int64_t, 8> update_idx; // update_idx is a dummy param here
    if (OB_FAIL(update_rows_wrap(tablet_handle,
                                 dml_param.data_row_for_lob_,
                                 relative_table,
                                 ctx,
                                 *run_ctx.col_descs_,
                                 update_idx,
                                 tmp_rows,
                                 dml_param.encrypt_meta_,
                                 rows_info))) {
      if (OB_TRY_LOCK_ROW_CONFLICT != ret && OB_TRANSACTION_SET_VIOLATION != ret) {
        LOG_WARN("failed to set row", K(ret), K(*run_ctx.col_descs_), K(rows_info));
      }
    }
  }
  return ret;
}

int ObLSTabletService::delete_lob_tablet_rows(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow *rows,
    int64_t row_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (OB_FAIL(delete_lob_tablet_rows(tablet_handle, run_ctx, rows[i]))) {
      LOG_WARN("failed to delete lob rows.", K(ret), K(i), K(rows[i]));
    }
  }
  return ret;
}

int ObLSTabletService::delete_lob_tablet_rows(
    ObTabletHandle &data_tablet,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &datum_row)
{
  int ret = OB_SUCCESS;
  int64_t col_cnt = run_ctx.col_descs_->count();
  const ObTableSchemaParam &table_param = run_ctx.dml_param_.table_param_->get_data_table();
  if (table_param.is_vector_index_snapshot()) {
    LOG_INFO("vector index skip dml delete lob tablet", K(ret));
  } else if (datum_row.count_ != col_cnt) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("[STORAGE_LOB]Invliad row col cnt", K(col_cnt), K(datum_row));
  } else if (OB_FAIL(update_lob_meta_table_seq_no(run_ctx, 1/*row_count*/))) {
    LOG_WARN("update_lob_meta_table_seq_no fail", K(ret), K(run_ctx.dml_param_));
  } else {
    ObLobCommon *lob_common = nullptr;
    for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
      const ObColDesc &column = run_ctx.col_descs_->at(i);
      if (column.col_type_.is_lob_storage()) {
        blocksstable::ObStorageDatum &datum = datum_row.storage_datums_[i];
        ObLobAccessParam lob_param;
        if (OB_FAIL(ObLobTabletDmlHelper::delete_lob_col(run_ctx, datum_row, i, datum, lob_common, lob_param))) {
          LOG_WARN("[STORAGE_LOB]failed to erase lob col.", K(ret), K(i), K(datum_row));
        }
      }
    }
  }
  return ret;
}
// revert end

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
         if (OB_FAIL(table_param->convert(*table_schema, param.column_ids_, param.pd_storage_flag_))) {
           LOG_WARN("Fail to convert table param, ", K(ret));
         } else {
           param.table_param_ = table_param;
         }
       }
    }
  }
  return ret;
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
  } else if (OB_FAIL(get_read_tables(tablet_id, timeout_us, max_snapshot_version, max_snapshot_version, iter, false/*allow_no_ready_read*/, true/*need_split_src_table*/, true/*need_split_dst_table*/))) {
    LOG_WARN("fail to get all read tables", K(ret), K(tablet_id), K(max_snapshot_version));
  } else {
    ObPartitionSplitQuery split_query;
    ObSEArray<ObStoreRange, 16> new_ranges;
    ObArenaAllocator allocator(common::ObMemAttr(MTL_ID(), "GetMulRangeCost"));
    const ObTabletHandle &tablet_handle = iter.get_tablet_handle();
    bool is_splited_range = false;
    if (OB_FAIL(split_query.split_multi_ranges_if_need(ranges, new_ranges,
        allocator,
        tablet_handle,
        is_splited_range))) {
      LOG_WARN("fail to split ranges", K(ret), K(ranges));
    } else {
      ObPartitionMultiRangeSpliter spliter;
      if (OB_FAIL(spliter.get_multi_range_size(is_splited_range ? new_ranges : ranges,
                                               iter.get_tablet()->get_rowkey_read_info(),
                                               *iter.table_iter(),
                                               total_size))) {
        LOG_WARN("fail to get multi ranges cost", K(ret), K(ranges));
      }
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
  const int64_t max_snapshot_version = INT64_MAX;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret));
  } else if (OB_FAIL(get_read_tables(tablet_id,
                                     timeout_us,
                                     max_snapshot_version,
                                     max_snapshot_version,
                                     iter,
                                     false /*allow_no_ready_read*/,
                                     true /*need_split_src_table*/,
                                     true /*need_split_dst_table*/))) {
    LOG_WARN("fail to get all read tables", K(ret), K(tablet_id), K(max_snapshot_version));
  } else {
    ObPartitionSplitQuery split_query;
    ObSEArray<ObStoreRange, 16> new_ranges;
    const ObTabletHandle &tablet_handle = iter.get_tablet_handle();
    bool is_splited_range = false;
    if (OB_FAIL(split_query.split_multi_ranges_if_need(
            ranges, new_ranges, allocator, tablet_handle, is_splited_range))) {
      LOG_WARN("fail to split ranges", K(ret), K(ranges));
    } else {
      ObPartitionMultiRangeSpliter spliter;
      if (OB_FAIL(spliter.get_split_multi_ranges(is_splited_range ? new_ranges : ranges,
                                                 expected_task_count,
                                                 iter.get_tablet()->get_rowkey_read_info(),
                                                 *iter.table_iter(),
                                                 allocator,
                                                 multi_range_split_array,
                                                 /* for compaction */ false))) {
        LOG_WARN("fail to get splitted ranges", K(ret), K(ranges), K(expected_task_count));
      }
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
    const int64_t snapshot_version = -1 == param.frozen_version_ ?
        GET_BATCH_ROWS_READ_SNAPSHOT_VERSION : param.frozen_version_;
    if (OB_FAIL(get_read_tables(param.tablet_id_, timeout_us, snapshot_version, snapshot_version, tablet_iter, false/*allow_no_ready_read*/, true/*need_split_src_table*/, true/*need_split_dst_table*/))) {
      if (OB_TABLET_NOT_EXIST != ret) {
        LOG_WARN("failed to get tablet_iter", K(ret), K(snapshot_version), K(param));
      }
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
        } else if (table->no_data_to_read()) {
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

int ObLSTabletService::inner_estimate_block_count_and_row_count(
    ObTabletTableIterator &tablet_iter,
    int64_t &macro_block_count,
    int64_t &micro_block_count,
    int64_t &sstable_row_count,
    int64_t &memtable_row_count,
    common::ObIArray<int64_t> &cg_macro_cnt_arr,
    common::ObIArray<int64_t> &cg_micro_cnt_arr)
{
  int ret = OB_SUCCESS;
  ObITable *table = nullptr;
  ObSSTable *sstable = nullptr;
  macro_block_count = 0;
  micro_block_count = 0;
  sstable_row_count = 0;
  memtable_row_count = 0;
  cg_macro_cnt_arr.reset();
  cg_micro_cnt_arr.reset();
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  }

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
    } else if (table->is_direct_load_memtable()) {
      ObDDLKV *ddl_kv = static_cast<ObDDLKV *>(table);
      int64_t macro_block_count_in_ddl_kv = 0;
      int64_t micro_block_count_in_ddl_kv = 0;
      int64_t row_count_in_ddl_kv = 0;
      if (OB_FAIL(ddl_kv->get_block_count_and_row_count(macro_block_count_in_ddl_kv,
                                                        micro_block_count_in_ddl_kv,
                                                        row_count_in_ddl_kv))) {
        LOG_WARN("fail to get block count and row count", K(ret));
      } else {
        macro_block_count += macro_block_count_in_ddl_kv;
        micro_block_count += micro_block_count_in_ddl_kv;
        sstable_row_count += row_count_in_ddl_kv;
      }
    } else if (table->is_data_memtable()) {
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
    if (OB_SUCC(ret) && table->is_co_sstable()) {
      ObCOSSTableV2 *co_sstable = static_cast<ObCOSSTableV2 *>(table);
      common::ObArray<ObSSTableWrapper> table_wrappers;
      if (OB_FAIL(co_sstable->get_all_tables(table_wrappers))) {
        LOG_WARN("fail to get all tables", K(ret), KPC(co_sstable));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < table_wrappers.count(); i++) {
          ObITable *cg_table = table_wrappers.at(i).get_sstable();
          ObSSTableMetaHandle co_sst_meta_hdl;
          if (OB_UNLIKELY(cg_table == nullptr || !cg_table->is_sstable())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected cg table", K(ret), KPC(cg_table));
          } else if (FALSE_IT(sstable = static_cast<ObSSTable *>(cg_table))) {
          } else if (OB_FAIL(sstable->get_meta(co_sst_meta_hdl))) {
            LOG_WARN("fail to get sstable meta handle", K(ret));
          } else if (OB_FAIL(cg_macro_cnt_arr.push_back(sstable->get_data_macro_block_count()))) {
            LOG_WARN("fail to push macro count", K(ret));
          } else if (OB_FAIL(cg_micro_cnt_arr.push_back(co_sst_meta_hdl.get_sstable_meta().get_data_micro_block_count()))) {
            LOG_WARN("fail to push micro count", K(ret));
          }
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::estimate_block_count_and_row_count_for_split_extra(
    const ObTabletID &tablet_id,
    const int64_t split_cnt,
    const ObMDSGetTabletMode mode,
    const int64_t timeout_us,
    const int64_t snapshot_version_for_tables,
    int64_t &macro_block_count,
    int64_t &micro_block_count,
    int64_t &sstable_row_count,
    int64_t &memtable_row_count,
    common::ObIArray<int64_t> &cg_macro_cnt_arr,
    common::ObIArray<int64_t> &cg_micro_cnt_arr)
{
  int ret = OB_SUCCESS;
  /* To calculate tables block count for sample in tablet spliting, we should multiple a split ratio
  * for src tablet tables block count, because the src tablet is only part of new tablet. So here
  * we should calculate block count of src and new tablet respectively */
  ObTabletTableIterator tablet_iter;
  int64_t tmp_tablet_macro_block_cnt = 0;
  int64_t tmp_tablet_micro_block_cnt = 0;
  int64_t tmp_tablet_sstable_row_cnt = 0;
  int64_t tmp_tablet_memtable_row_cnt = 0;
  ObArray<int64_t> tmp_tablet_cg_macro_cnt_arr;
  ObArray<int64_t> tmp_tablet_cg_micro_cnt_arr;
  if (OB_FAIL(inner_get_read_tables(
      tablet_id,
      timeout_us,
      INT64_MAX,
      snapshot_version_for_tables,
      tablet_iter,
      true/*allow_no_ready_read*/,
      false/*need_split_src_table*/,
      false/*need_split_dst_table*/,
      mode))) {
    LOG_WARN("fail to get read tables", K(ret), K(tablet_id));
  } else if (OB_FAIL(inner_estimate_block_count_and_row_count( // origin tablet
      tablet_iter,
      tmp_tablet_macro_block_cnt,
      tmp_tablet_micro_block_cnt,
      tmp_tablet_sstable_row_cnt,
      tmp_tablet_memtable_row_cnt,
      tmp_tablet_cg_macro_cnt_arr,
      tmp_tablet_cg_micro_cnt_arr))) {
    LOG_WARN("fail to inner estimate block count", K(ret));
  } else {
    /* estimate block count is using total range of tablet, so here no need to cut range.
    Specifilly, if local index table, we should estimate whole range of origin tablet.
    but for main table, we should multiple split_ratio to estimate origin block cnt */
    tmp_tablet_macro_block_cnt = tmp_tablet_macro_block_cnt / split_cnt;
    tmp_tablet_micro_block_cnt = tmp_tablet_micro_block_cnt / split_cnt;
    tmp_tablet_sstable_row_cnt = tmp_tablet_sstable_row_cnt / split_cnt;
    tmp_tablet_memtable_row_cnt = tmp_tablet_memtable_row_cnt / split_cnt;
    for (int64_t i = 0; i < tmp_tablet_cg_macro_cnt_arr.count(); i++) {
      tmp_tablet_cg_macro_cnt_arr.at(i) = tmp_tablet_cg_macro_cnt_arr.at(i) / split_cnt;
    }
    for (int64_t i = 0; i < tmp_tablet_cg_micro_cnt_arr.count(); i++) {
      tmp_tablet_cg_micro_cnt_arr.at(i) = tmp_tablet_cg_micro_cnt_arr.at(i) / split_cnt;
    }
    macro_block_count += tmp_tablet_macro_block_cnt;
    micro_block_count += tmp_tablet_micro_block_cnt;
    sstable_row_count += tmp_tablet_sstable_row_cnt;
    memtable_row_count += tmp_tablet_memtable_row_cnt;
    if (OB_SUCC(ret)) {
      ObIArray<int64_t> &arr = cg_macro_cnt_arr;
      ObIArray<int64_t> &inc_arr = tmp_tablet_cg_macro_cnt_arr;
      if (!inc_arr.empty()) {
        if (arr.empty()) {
          if (OB_FAIL(append(arr, inc_arr))) {
            LOG_WARN("failed to append", K(ret));
          }
        } else {
          int64_t cnt = std::min(arr.count(), inc_arr.count());
          for (int64_t i = 0; i < cnt; i++) {
            arr.at(i) += inc_arr.at(i);
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      ObIArray<int64_t> &arr = cg_micro_cnt_arr;
      ObIArray<int64_t> &inc_arr = tmp_tablet_cg_micro_cnt_arr;
      if (!inc_arr.empty()) {
        if (arr.empty()) {
          if (OB_FAIL(append(arr, inc_arr))) {
            LOG_WARN("failed to append", K(ret));
          }
        } else {
          int64_t cnt = std::min(arr.count(), inc_arr.count());
          for (int64_t i = 0; i < cnt; i++) {
            arr.at(i) += inc_arr.at(i);
          }
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::estimate_block_count_and_row_count(
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    int64_t &macro_block_count,
    int64_t &micro_block_count,
    int64_t &sstable_row_count,
    int64_t &memtable_row_count,
    common::ObIArray<int64_t> &cg_macro_cnt_arr,
    common::ObIArray<int64_t> &cg_micro_cnt_arr)
{
  int ret = OB_SUCCESS;
  ObTabletHandle tablet_handle;
  ObTabletTableIterator tablet_iter;
  share::SCN max_readable_scn;
  int64_t snapshot_version_for_tablet = 0;
  int64_t snapshot_version_for_tables = 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_FAIL(OB_TS_MGR.get_gts(MTL_ID(), nullptr, max_readable_scn))) {
    LOG_WARN("failed to get gts", K(ret));
  } else if (FALSE_IT(snapshot_version_for_tablet = static_cast<int64_t>(max_readable_scn.get_val_for_sql()))) {
  } else if (FALSE_IT(snapshot_version_for_tables = static_cast<int64_t>(max_readable_scn.get_val_for_sql()))) {
  } else if (OB_FAIL(inner_get_read_tables(
          tablet_id,
          timeout_us,
          snapshot_version_for_tablet,
          snapshot_version_for_tables,
          tablet_iter,
          false/*allow_no_ready_read*/,
          false/*need_split_src_table*/,
          false/*need_split_dst_table*/,
          ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
    LOG_WARN("failed to get read tables", K(ret));
  } else if (OB_FAIL(inner_estimate_block_count_and_row_count(
          tablet_iter,
          macro_block_count,
          micro_block_count,
          sstable_row_count,
          memtable_row_count,
          cg_macro_cnt_arr,
          cg_micro_cnt_arr))) {
    LOG_WARN("fail to inner estimate block count", K(ret));
  }

  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    tablet_iter.reset();
    if (OB_FAIL(inner_get_read_tables(
              tablet_id,
              timeout_us,
              ObTransVersion::MAX_TRANS_VERSION,
              snapshot_version_for_tables,
              tablet_iter,
              false/*allow_no_ready_read*/,
              false/*need_split_src_table*/,
              true/*need_split_dst_table*/,
              ObMDSGetTabletMode::READ_ALL_COMMITED))) {
      LOG_WARN("failed to get read tables for split src", K(ret));
    } else if (OB_FAIL(inner_estimate_block_count_and_row_count(
            tablet_iter,
            macro_block_count,
            micro_block_count,
            sstable_row_count,
            memtable_row_count,
            cg_macro_cnt_arr,
            cg_micro_cnt_arr))) {
      LOG_WARN("fail to inner estimate block count", K(ret));
    }
  } else if (OB_REPLICA_NOT_READABLE == ret) { // ls migrating or tablet without major
    const int orig_ret = ret;
    ObTabletID src_tablet_id;
    ObTabletSplitMdsUserData split_data;
    ObTabletSplitMdsUserData src_split_data;
    ObTabletHandle tablet_handle;
    ObTabletHandle src_tablet_handle;
    int64_t split_cnt = 0;
    tablet_iter.reset();
    bool allow_to_read = false;
    allow_to_read_mgr_.load_allow_to_read_info(allow_to_read);
    if (!allow_to_read) {
      ret = OB_REPLICA_NOT_READABLE;
      LOG_WARN("ls is not allow to read", K(ret), KPC(ls_));
    } else if (OB_FAIL(ls_->get_tablet(tablet_id, tablet_handle, timeout_us, ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
      LOG_WARN("failed to get tablet", K(ret), K(src_tablet_id));
    } else if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(split_data, timeout_us))) {
      LOG_WARN("failed to get split data", K(ret));
    } else if (OB_UNLIKELY(!split_data.is_split_dst())) {
      ret = orig_ret;
      LOG_WARN("maybe not split dst or split task finish", K(ret));
    } else if (OB_FAIL(inner_get_read_tables(
            tablet_id,
            timeout_us,
            snapshot_version_for_tablet,
            snapshot_version_for_tables,
            tablet_iter,
            true/*allow_no_ready_read*/,
            false/*need_split_src_table*/,
            false/*need_split_dst_table*/,
            ObMDSGetTabletMode::READ_READABLE_COMMITED))) {
      LOG_WARN("failed to get read tables", K(ret));
    } else if (OB_FAIL(inner_estimate_block_count_and_row_count(
              tablet_iter,
              macro_block_count,
              micro_block_count,
              sstable_row_count,
              memtable_row_count,
              cg_macro_cnt_arr,
              cg_micro_cnt_arr))) {
      LOG_WARN("fail to inner estimate block count", K(ret));
    } else if (OB_UNLIKELY(0 == tablet_iter.tablet_handle_.get_obj()->get_major_table_count())) {
      if (OB_FAIL(split_data.get_split_src_tablet_id(src_tablet_id))) {
        LOG_WARN("failed to get split src tablet id", K(ret));
      } else if (OB_FAIL(ls_->get_tablet(src_tablet_id, src_tablet_handle, timeout_us, ObMDSGetTabletMode::READ_ALL_COMMITED))) {
        LOG_WARN("failed to get tablet", K(ret), K(src_tablet_id));
      } else if (OB_FAIL(src_tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(src_split_data, timeout_us))) {
        LOG_WARN("failed to get split data", K(ret), K(src_tablet_id));
      } else if (OB_FAIL(src_split_data.get_split_dst_tablet_cnt(split_cnt))) {
        LOG_WARN("failed to get split cnt", K(ret), K(src_tablet_id), K(src_split_data));
      } else if (OB_FAIL(estimate_block_count_and_row_count_for_split_extra(
                src_tablet_id,
                split_cnt,
                ObMDSGetTabletMode::READ_ALL_COMMITED,
                timeout_us,
                snapshot_version_for_tables,
                macro_block_count,
                micro_block_count,
                sstable_row_count,
                memtable_row_count,
                cg_macro_cnt_arr,
                cg_micro_cnt_arr))) {
        LOG_WARN("failed to estimate block count and row count for split extra tablets", K(ret));
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
  uint64_t data_version = 0;
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
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else if (OB_FAIL(create_inner_tablet(data_version, ls_id, tablet_id, tablet_id/*data_tablet_id*/,
        create_scn, snapshot_version, create_tablet_schema, tablet_handle))) {
    LOG_WARN("failed to do create tablet", K(ret), K(ls_id), K(tablet_id), K(data_version),
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

int ObLSTabletService::build_tablet_iter(ObLSTabletAddrIterator &iter)
{
  int ret = common::OB_SUCCESS;
  GetAllTabletIDOperator op(iter.tablet_ids_, false /*except_ls_inner_tablet*/);
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

int ObLSTabletService::build_tablet_iter(ObLSTabletIterator &iter, const bool except_ls_inner_tablet)
{
  int ret = common::OB_SUCCESS;
  GetAllTabletIDOperator op(iter.tablet_ids_, except_ls_inner_tablet);
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
  } else if (OB_FAIL(iter.sort_tablet_ids_if_need())) {
    LOG_WARN("failed to sort tablet ids if need", K(ret), K(iter));
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

int ObLSTabletService::build_tablet_iter(ObLSTabletFastIter &iter, const bool except_ls_inner_tablet)
{
  int ret = common::OB_SUCCESS;
  GetAllTabletIDOperator op(iter.tablet_ids_, except_ls_inner_tablet);
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

/**
 * @brief: hold all bucket rlock when building tablet iter(only used for tenant slog checkpoint)
 * see
*/
int ObLSTabletService::build_tablet_iter_with_lock_hold(ObLSTabletFastIter &iter)
{
  int ret = common::OB_SUCCESS;
  const int64_t max_retry_time = 1000;
  int64_t retry_time = 0;
  GetAllTabletIDOperator op(iter.tablet_ids_, /*except_ls_inner_tablet*/false);
  iter.ls_tablet_service_ = this;
  bool locked = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", K(ret), K_(is_inited));
  }

  while (OB_SUCC(ret) && !locked) {
    // hold all bucket rlock
    common::ObBucketTryRLockAllGuard rlock_guard(bucket_lock_);

    if (OB_FAIL(rlock_guard.get_ret()) && OB_EAGAIN != ret) {
      STORAGE_LOG(WARN, "failed to hold all ls bucket rlock", K(ret));
    } else if (OB_EAGAIN == ret) {
      if (retry_time >= max_retry_time) {
        STORAGE_LOG(WARN, "already meet max_retry_time but still lock failed", K(ret), K(retry_time), K(max_retry_time));
        break;
      }
      ++retry_time;
      ret = OB_SUCCESS;
      // sleep for 1ms and retry again...
      ob_usleep(1000);
    } else if (FALSE_IT(locked = true)) {
    } else if (OB_FAIL(tablet_id_set_.foreach(op))) {
      STORAGE_LOG(WARN, "fail to get all tablet ids from set", K(ret));
    } else if (OB_UNLIKELY(!iter.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("iter is invalid", K(ret), K(iter));
    }
  }

  if (OB_FAIL(ret)) {
    iter.reset();
  }
  return ret;
}

int ObLSTabletService::is_tablet_exist(const common::ObTabletID &tablet_id, bool &is_exist)
{
  int ret = OB_SUCCESS;
  is_exist = false;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "not inited", KR(ret));
  } else if (OB_FAIL(tablet_id_set_.exist(tablet_id))) {
    if (OB_HASH_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
      is_exist = true;
    } else if (OB_HASH_NOT_EXIST == ret) {
      ret = OB_SUCCESS; // ignore ret
    } else {
      STORAGE_LOG(WARN, "fail to check is tablet exist", KR(ret), K(tablet_id));
    }
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
      FLOG_INFO("set ls allow to read", KPC(ls));
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

int ObLSTabletService::ha_scan_all_tablets(
    const HandleTabletMetaFunc &handle_tablet_meta_f,
    const bool need_sorted_tablet_id)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else {
    ObHALSTabletIterator iterator(ls_->get_ls_id(), false/* need_initial_state */, need_sorted_tablet_id);
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
        } else if (OB_FAIL(tablet->get_latest_tablet_status(user_data, writer, trans_stat, trans_version))) {
          if (OB_EMPTY_RESULT == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get latest tablet status", K(ret), KPC(tablet));
          }
        } else if (trans_stat != mds::TwoPhaseCommitState::ON_COMMIT
            && ObTabletStatus::TRANSFER_IN == user_data.tablet_status_) {
          //TODO(muwei.ym) CAN NOT USE this condition when MDS supports uncommitted transaction

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
  TIMEGUARD_INIT(GetStaticTablet, 1_s);
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
  } else if (CLICK_FAIL(t3m->get_tablet_with_allocator(
      priority, key, allocator, handle, force_alloc_new))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_TABLET_NOT_EXIST;
    } else {
      LOG_WARN("failed to get tablet with allocator", K(ret), K(priority), K(key));
    }
  } else if (CLICK_FAIL(handle.get_obj()->clear_memtables_on_table_store())) {
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
  } else if (OB_FAIL(data_tablet_handle.get_obj()->get_latest_tablet_status(
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

int ObLSTabletService::offline_build_tablet_without_memtable_()
{
  int ret = OB_SUCCESS;
  ObArray<ObTabletID> tablet_id_array;
  const bool except_ls_inner_tablet = false;
  const SCN scn(SCN::max_scn());

  if (OB_FAIL(get_all_tablet_ids(except_ls_inner_tablet, tablet_id_array))) {
    LOG_WARN("failed to get all tablet ids", K(ret));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < tablet_id_array.count(); ++i) {
      const ObTabletID &tablet_id = tablet_id_array.at(i);
      if (OB_FAIL(update_tablet_release_memtable_for_offline(tablet_id, scn))) {
        LOG_WARN("failed to update tablet release memtable", K(ret), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObLSTabletService::offline_destroy_memtable_and_mds_table_()
{
  int ret = OB_SUCCESS;
  DestroyMemtableAndMemberAndMdsTableOperator clean_mem_op(this);
  if (OB_FAIL(tablet_id_set_.foreach(clean_mem_op))) {
    LOG_WARN("fail to clean memtables", K(ret), "cur_tablet_id", clean_mem_op.cur_tablet_id_);
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
int ObLSTabletService::alloc_private_tablet_meta_version_with_lock(
  const ObTabletMapKey &key,
  int64_t &tablet_meta_version)
{
  int ret = OB_SUCCESS;
  tablet_meta_version = 0;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (!GCTX.is_shared_storage_mode())  {
    // do nothing if is SN mode.
  } else if (OB_UNLIKELY(!key.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid tablet key", K(ret), K(key));
  } else {
    ObBucketHashWLockGuard lock_guard(bucket_lock_, key.tablet_id_.hash());
    if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc private tablet meta version", K(ret), K(key));
    }
  }
  return ret;
}

int ObLSTabletService::alloc_private_tablet_meta_version_without_lock(
  const ObTabletMapKey &key,
  int64_t &tablet_meta_version)
{
  int ret = OB_SUCCESS;
  tablet_meta_version = 0;
  if (!GCTX.is_shared_storage_mode())  {
    // do nothing if is SN mode.
  } else {
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    bool retried = false;
    ObTimeGuard time_guard(__func__, 12_ms);
    OB_ASSERT(nullptr != t3m);
    if (OB_UNLIKELY(!key.is_valid())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid tablet key", K(ret), K(key));
    } else if (OB_FAIL(t3m->alloc_tablet_meta_version(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    } else {
      time_guard.click("AllocVer1");
    }

    if (OB_EAGAIN == ret) {
      // log and retry on more time
      retried = true;
      if (OB_FAIL(update_private_tablet_last_match_meta_version_without_lock(key.tablet_id_, time_guard))) {
        LOG_WARN("failed to update private tablet last match meta version", K(ret), K(key));
      } else if (OB_FAIL(t3m->alloc_tablet_meta_version(key, tablet_meta_version))) {
        LOG_WARN("failed to alloc tablet meta version again", K(ret), K(key));
      } else {
        time_guard.click("AllocVer2");
      }
    }
    LOG_INFO("alloc private tablet meta version finished", KR(ret), K(retried), K(key), K(tablet_meta_version));
  }
  return ret;
}


int ObLSTabletService::apply_defragment_tablet(
  ObTenantMetaMemMgr &t3m,
  const ObTabletMapKey &tablet_key,
  const ObMetaDiskAddr &old_addr,
  ObTabletHandle &new_handle,
  ObTenantStorageMetaService &tsms)
{
  int ret = OB_SUCCESS;

  ObTimeGuard time_guard("ApplyTabletDfgt", 2_s);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet service hasn't been initalized yet", K(ret));
  } else if (OB_UNLIKELY(!tablet_key.is_valid()
                      || !old_addr.is_valid()
                      || !new_handle.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tablet_key), K(old_addr), K(new_handle));
  } else {
    OB_ASSERT(new_handle.get_obj() != nullptr);
    ObTabletHandle tablet_handle_from_t3m;
    ObMetaDiskAddr tablet_addr_from_t3m;
    ObUpdateTabletPointerParam param;
    int64_t ls_epoch = 0;

    // HOLD BUCKET LOCK
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_key.tablet_id_.hash());
    time_guard.click("HeldBucketLock");
    if (OB_FAIL(lock_guard.get_ret())) {
      LOG_WARN("failed to hold bucket_lock_", K(ret), K(tablet_key.tablet_id_.hash()));
    } else if (OB_FAIL(t3m.get_tablet_addr(tablet_key, tablet_addr_from_t3m))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      }
      LOG_WARN("failed to get tablet addr from t3m", K(ret), K(tablet_key));
    } else if (!old_addr.is_equal_for_persistence(tablet_addr_from_t3m)) {
      // nothing to do
      LOG_INFO("tablet has been updated, skip applying tablet", K(tablet_key),
        K(old_addr), K(tablet_addr_from_t3m));
    } else if (OB_FAIL(new_handle.get_obj()->get_ls_epoch(ls_epoch))) {
      LOG_WARN("fail to get ls epoch", K(ret), K(tablet_key), K(new_handle));
    } else if (OB_FAIL(new_handle.get_obj()->get_updating_tablet_pointer_param(param))) {
      LOG_WARN("failed to get updating tablet pointer parameters", K(ret), K(tablet_key), K(new_handle));
    } else if (OB_FAIL(tsms.update_tablet(tablet_key.ls_id_,
          ls_epoch, tablet_key.tablet_id_, param))) { // write slog if tsms is provided
      LOG_WARN("failed to write update tablet slog", K(ret), K(tablet_key), K(param));
    } else if (OB_FAIL(t3m.get_tablet(WashTabletPriority::WTP_LOW, tablet_key, tablet_handle_from_t3m))) {
      // tablet must exists at t3m!!!
      OB_ASSERT(ret != OB_ENTRY_NOT_EXIST && ret != OB_ITEM_NOT_SETTED);
      LOG_WARN("failed to get tablet from t3m", K(ret), K(tablet_key));
    } else if (OB_FAIL(refresh_memtable_for_ckpt(old_addr, tablet_addr_from_t3m, new_handle))) {
      /// @c old_addr and @c tablet_addr_from_t3m must be equal for persister
      OB_ASSERT(ret != OB_NOT_THE_OBJECT);
      LOG_WARN("failed to update tablet", K(ret),
        K(old_addr), K(tablet_addr_from_t3m), K(new_handle));
    } else if (FALSE_IT(time_guard.click("UpdateTablet"))) {
    } else if (OB_FAIL(t3m.compare_and_swap_tablet(tablet_key, tablet_handle_from_t3m,
        new_handle, param))) {
      LOG_WARN("fail to compare and swap tablet", K(ret), K(tablet_handle_from_t3m),
          K(new_handle), K(param));

      ob_usleep(1000);
      ob_abort();
    }
  }
  return ret;
}

int ObLSTabletService::refresh_empty_shell_for_slog_ckpt(
  ObTenantMetaMemMgr &t3m,
  const ObTabletMapKey &tablet_key,
  const ObMetaDiskAddr &old_addr)
{
  int ret = OB_SUCCESS;
  uint64_t data_version = 0;

  ObTablet *empty_shell = nullptr;
  ObUpdateTabletPointerParam param;
  ObTimeGuard time_guard("refreshEmptyShell", 3_s);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ls tablet service hasn't been initalized yet", K(ret));
  } else if (OB_UNLIKELY(GCTX.is_shared_storage_mode())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("refresh_empty_shell_for_slog_ckpt is only supported for SN mode", K(ret));
  } else if (OB_UNLIKELY(!tablet_key.is_valid() ||
                          !old_addr.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(tablet_key), K(old_addr));
  } else if (!old_addr.is_file()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("original addr is not a file, which means it's not a empty shell",
      K(ret), K(old_addr));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    ObTabletHandle tablet_handle_from_t3m;
    ObTabletHandle new_tablet_handle;
    ObTablet *tablet_from_t3m = nullptr;
    ObMetaDiskAddr tablet_addr_from_t3m;
    ObUpdateTabletPointerParam param;
    int64_t ls_epoch = 0;

    // HOLD BUCKET LOCK
    ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_key.tablet_id_.hash());
    time_guard.click("HeldBucketLock");

    if (OB_FAIL(lock_guard.get_ret())) {
      LOG_WARN("failed to hold bucket_lock_", K(ret), K(tablet_key.tablet_id_.hash()));
    } else if (OB_FAIL(t3m.get_tablet_addr(tablet_key, tablet_addr_from_t3m))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_TABLET_NOT_EXIST;
      }
      LOG_WARN("failed to get tablet addr from t3m", K(ret), K(tablet_key));
    } else if (!old_addr.is_equal_for_persistence(tablet_addr_from_t3m)) {
      // nothing to do
      LOG_INFO("tablet has been updated, skip applying tablet", K(tablet_key),
        K(old_addr), K(tablet_addr_from_t3m));
    } else if (OB_FAIL(t3m.get_tablet(WashTabletPriority::WTP_LOW, tablet_key, tablet_handle_from_t3m))) {
      // tablet must exists at t3m!!!
      OB_ASSERT(ret != OB_ENTRY_NOT_EXIST && ret != OB_ITEM_NOT_SETTED);
      LOG_WARN("failed to get tablet from t3m", K(ret), K(tablet_key));
    } else if (FALSE_IT(time_guard.click("GetOld"))) {
    } else if (OB_ISNULL(tablet_from_t3m = tablet_handle_from_t3m.get_obj())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null tablet_from_t3m", K(ret));
    } else if (OB_UNLIKELY(!tablet_from_t3m->is_empty_shell())) {
      /// IMPOSSIBLE!!!
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected non empty shell tablet", K(ret));
    } else if (OB_FAIL(tablet_from_t3m->get_ls_epoch(ls_epoch))) {
      LOG_WARN("failed to get ls epoch", K(ret));
    } else {
      time_guard.click("Acquire");
      const ObTabletPersisterParam param(data_version, tablet_key.ls_id_, ls_epoch, tablet_key.tablet_id_, tablet_from_t3m->get_transfer_seq(), 0);
      if (OB_FAIL(ObTabletPersister::transform_empty_shell(param, *tablet_from_t3m, new_tablet_handle))) {
        LOG_WARN("failed to transform empty shell", K(ret), K(tablet_key), K(data_version));
      } else if (FALSE_IT(time_guard.click("Transform"))) {
      } else if (OB_FAIL(safe_update_cas_empty_shell(data_version, tablet_key, tablet_handle_from_t3m, new_tablet_handle, time_guard))) {
        LOG_WARN("fail to cas empty shell", K(ret), K(tablet_key), K(tablet_handle_from_t3m), K(new_tablet_handle), K(data_version));
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
      } else if (OB_FAIL(tablet->ObITabletMdsInterface::get_latest_tablet_status(data, writer, trans_stat, trans_version))) {
        if (OB_EMPTY_RESULT == ret) {
          ret = OB_SUCCESS;
          if (tablet_status_is_written) {
            if (OB_FAIL(deleted_tablets.push_back(tablet->get_tablet_id()))) {
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
      if (OB_FAIL(do_remove_tablet(ls_id, tablet_id))) {
        LOG_WARN("failed to remove tablet", K(ret), K(ls_id), K(tablet_id));
      } else {
        LOG_INFO("gc tablet finish", K(ret), K(ls_id), K(tablet_id));
      }
    }
  }
  return ret;
}

int ObLSTabletService::ObDmlSplitCtx::prepare_write_dst(
    const ObTabletID &src_tablet_id,
    const ObTabletID &dst_tablet_id,
    ObStoreCtx &store_ctx,
    const ObRelativeTable &relative_table)
{
  int ret = OB_SUCCESS;
  const share::schema::ObTableSchemaParam *schema_param = relative_table.get_schema_param();
  if (OB_UNLIKELY(!src_tablet_id.is_valid() || !dst_tablet_id.is_valid()) || OB_ISNULL(store_ctx.ls_) || OB_ISNULL(schema_param)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arg", K(ret), K(src_tablet_id), K(dst_tablet_id), KP(schema_param));
  } else if (OB_FAIL(store_ctx.ls_->get_tablet_svr()->get_tablet_with_timeout(
      dst_tablet_id, dst_tablet_handle_, store_ctx.timeout_, ObMDSGetTabletMode::READ_READABLE_COMMITED, share::SCN::max_scn()))) {
    LOG_WARN("failed to get tablet", K(ret));
  } else if (OB_FAIL(dst_relative_table_.init(schema_param,
                                              dst_tablet_handle_.get_obj()->get_tablet_meta().tablet_id_,
                                              relative_table.allow_not_ready()))) {
    LOG_WARN("fail to init dst_relative_table_", K(ret), K(relative_table), K(dst_tablet_handle_.get_obj()->get_tablet_meta()));
  } else if (OB_FAIL(dst_relative_table_.tablet_iter_.set_tablet_handle(dst_tablet_handle_))) {
    LOG_WARN("fail to set tablet handle to iter", K(ret), K(dst_relative_table_.tablet_iter_));
  } else if (OB_FAIL(dst_relative_table_.tablet_iter_.refresh_read_tables_from_tablet(
      store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx(),
      dst_relative_table_.allow_not_ready(),
      false/*major_sstable_only*/,
      true/*need_split_src_table*/,
      false/*need_split_dst_table*/))) {
    LOG_WARN("failed to get relative table read tables", K(ret));
  } else if (schema_param->get_read_info().need_truncate_filter()
      && OB_FAIL(dst_relative_table_.prepare_truncate_part_filter(
        allocator_,
        store_ctx.mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx()))) {
    LOG_WARN("failed to prepare truncate part filter", K(ret));
  }
  LOG_INFO("prepare write dst", K(ret), K(src_tablet_id), K(dst_tablet_id));
  return ret;
}

int ObLSTabletService::ObDmlSplitCtx::prepare_write_dst(
    ObTabletHandle &tablet_handle,
    const blocksstable::ObDatumRow *data_row_for_lob,
    ObStoreCtx &store_ctx,
    const ObRelativeTable &relative_table,
    const ObDatumRowkey &rowkey)
{
  int ret = OB_SUCCESS;
  ObTabletID dst_tablet_id;
  const bool is_lob = nullptr != data_row_for_lob;
  const int64_t abs_timeout_us = store_ctx.timeout_;
  if (OB_ISNULL(store_ctx.ls_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid ls", K(ret));
  } else if (is_lob) {
    if (OB_FAIL(ObTabletSplitMdsHelper::calc_split_dst_lob(
        *store_ctx.ls_, *tablet_handle.get_obj(), *data_row_for_lob, abs_timeout_us, dst_tablet_id))) {
      LOG_WARN("failed to calc split dst lob", K(ret));
    }
  } else {
    if (OB_FAIL(ObTabletSplitMdsHelper::calc_split_dst(
        *store_ctx.ls_, *tablet_handle.get_obj(), rowkey, abs_timeout_us, dst_tablet_id))) {
      LOG_WARN("failed to calc split dst tablet", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(prepare_write_dst(
      tablet_handle.get_obj()->get_tablet_meta().tablet_id_, dst_tablet_id, store_ctx, relative_table))) {
    LOG_WARN("failed to prepare write dst", K(ret));
  }
  return ret;
}

int ObLSTabletService::ObDmlSplitCtx::prepare_write_dst(
    ObTabletHandle &tablet_handle,
    const ObDatumRow *data_row_for_lob,
    ObStoreCtx &store_ctx,
    const ObRelativeTable &relative_table,
    const ObDatumRow &new_row)
{
  int ret = OB_SUCCESS;
  const int64_t rowkey_column_cnt = relative_table.get_rowkey_column_num();
  ObDatumRowkey rowkey;
  if (OB_FAIL(rowkey.assign(new_row.storage_datums_, rowkey_column_cnt))) {
    LOG_WARN("failed to from rowkey", K(ret));
  } else if (OB_FAIL(prepare_write_dst(tablet_handle, data_row_for_lob, store_ctx, relative_table, rowkey))) {
    LOG_WARN("failed to prepare write dst", K(ret));
  }
  return ret;
}

void ObLSTabletService::ObDmlSplitCtx::reuse()
{
  dst_tablet_handle_.reset();
  dst_relative_table_.destroy();
  allocator_.reuse();
}

int ObLSTabletService::lock_row_wrap(
    ObTabletHandle &tablet_handle,
    const blocksstable::ObDatumRow *data_row_for_lob,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDatumRowkey &rowkey)
{
  int ret = tablet_handle.get_obj()->lock_row(relative_table, store_ctx, rowkey);
  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ObDmlSplitCtx dml_split_ctx;
    if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle, data_row_for_lob, store_ctx, relative_table, rowkey))) {
      LOG_WARN("failed to prepare split dml ctx", K(ret));
    } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->lock_row(dml_split_ctx.dst_relative_table_, store_ctx, rowkey))) {
      LOG_WARN("failed to lock row", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::lock_row_wrap(
    ObTabletHandle &tablet_handle,
    const blocksstable::ObDatumRow *data_row_for_lob,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    ObColDescArray &col_descs,
    blocksstable::ObDatumRow &row)
{
  int ret = tablet_handle.get_obj()->lock_row(relative_table, store_ctx, col_descs, row);
  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ObDmlSplitCtx dml_split_ctx;
    if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle, data_row_for_lob, store_ctx, relative_table, row))) {
      LOG_WARN("failed to prepare split dml ctx", K(ret));
    } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->lock_row(dml_split_ctx.dst_relative_table_, store_ctx, col_descs, row))) {
      LOG_WARN("failed to lock row", K(ret));
    }
  }
  return ret;
}

// split by new_row, because in minimal mode old row's pk are nop
int ObLSTabletService::update_row_wrap(
    ObTabletHandle &tablet_handle,
    const blocksstable::ObDatumRow *data_row_for_lob,
    ObRelativeTable &relative_table,
    storage::ObStoreCtx &store_ctx,
    const ObIArray<share::schema::ObColDesc> &col_descs,
    const ObIArray<int64_t> &update_idx,
    const blocksstable::ObDatumRow &old_row,
    blocksstable::ObDatumRow &new_row,
    const ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr)
{
  int ret = tablet_handle.get_obj()->update_row(
      relative_table, store_ctx, col_descs, update_idx, old_row, new_row, encrypt_meta_arr);
  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ObDmlSplitCtx dml_split_ctx;
    if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle, data_row_for_lob, store_ctx, relative_table, new_row))) {
      LOG_WARN("failed to prepare split dml ctx", K(ret));
    } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->update_row(
        dml_split_ctx.dst_relative_table_, store_ctx, col_descs, update_idx, old_row, new_row, encrypt_meta_arr))) {
      LOG_WARN("failed to update row", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::batch_calc_split_dst_rows(
    ObLS &ls,
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObDatumRow *rows,
    const int64_t row_count,
    const int64_t abs_timeout_us,
    ObIArray<ObTabletID> &dst_tablet_ids,
    ObIArray<ObArray<int64_t>> &dst_row_ids)
{
  int ret = OB_SUCCESS;
  ObTabletSplitMdsUserData src_split_data;
  ObSEArray<ObTabletSplitMdsUserData, 2> dst_split_datas;
  ObTabletHandle dst_tablet_handle;
  const ObITableReadInfo &rowkey_read_info = tablet_handle.get_obj()->get_rowkey_read_info();
  dst_row_ids.reset();
  if (OB_FAIL(tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(src_split_data, abs_timeout_us - ObTimeUtility::current_time()))) {
    LOG_WARN("failed to get split data", K(ret));
  } else if (OB_FAIL(src_split_data.get_split_dst_tablet_ids(dst_tablet_ids))) {
    LOG_WARN("failed to get split dst tablet ids", K(ret));
  } else if (OB_FAIL(dst_split_datas.prepare_allocate(dst_tablet_ids.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < dst_tablet_ids.count(); i++) {
    dst_tablet_handle.reset();
    if (OB_FAIL(ls.get_tablet_with_timeout(dst_tablet_ids.at(i), dst_tablet_handle, abs_timeout_us))) {
      LOG_WARN("failed to get split dst tablet", K(ret));
    } else if (OB_FAIL(dst_tablet_handle.get_obj()->ObITabletMdsInterface::get_split_data(dst_split_datas.at(i), abs_timeout_us - ObTimeUtility::current_time()))) {
      LOG_WARN("failed to part key compare", K(ret));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(dst_row_ids.prepare_allocate(dst_tablet_ids.count()))) {
    LOG_WARN("failed to prepare allocate", K(ret));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    const int64_t rowkey_column_cnt = relative_table.get_rowkey_column_num();
    ObDatumRowkey rowkey;
    ObTabletID dst_tablet_id;
    int64_t dst_idx = 0;
    if (OB_FAIL(rowkey.assign(rows[i].storage_datums_, rowkey_column_cnt))) {
      LOG_WARN("failed to from rowkey", K(ret));
    } else if (OB_FAIL(src_split_data.calc_split_dst(rowkey_read_info, dst_split_datas, rowkey, dst_tablet_id, dst_idx))) {
      LOG_WARN("failed to calc split dst tablet", K(ret));
    } else if (OB_UNLIKELY(dst_idx >= dst_row_ids.count() || dst_idx >= dst_tablet_ids.count() || dst_tablet_ids.at(dst_idx) != dst_tablet_id)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("dst idx out of range", K(ret), K(dst_idx), K(dst_row_ids.count()), K(dst_tablet_id), K(dst_tablet_ids));
    } else if (OB_FAIL(dst_row_ids.at(dst_idx).push_back(i))) {
      LOG_WARN("failed to push back", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::insert_rows_wrap(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const bool check_exist,
    const ObColDescIArray &col_descs,
    ObRowsInfo &rows_info)
{
  int tmp_ret = OB_SUCCESS;
  const int64_t row_count = rows_info.get_rowkey_cnt();
  int ret = tablet_handle.get_obj()->insert_rows(
      relative_table, store_ctx, check_exist, col_descs, dml_param.encrypt_meta_, rows_info);
  if (rows_info.need_find_all_duplicate_key() && OB_ERR_PRIMARY_KEY_DUPLICATE == ret) {
    if (OB_TMP_FAIL(get_conflict_rows(tablet_handle,
                                      relative_table,
                                      store_ctx,
                                      dml_param,
                                      rows_info))) {
      LOG_WARN("failed to get conflict row(s)", K(ret), K(rows_info));
      ret = tmp_ret;
    }
  } else if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ret = OB_SUCCESS;
    ObDmlSplitCtx dml_split_ctx;
    if (nullptr != dml_param.data_row_for_lob_) {
      if (OB_FAIL(dml_split_ctx.prepare_write_dst(
          tablet_handle, dml_param.data_row_for_lob_, store_ctx, relative_table, *dml_param.data_row_for_lob_))) {
        LOG_WARN("failed to prepare split dml ctx", K(ret));
      } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->insert_rows(
          dml_split_ctx.dst_relative_table_, store_ctx, check_exist, col_descs,
          dml_param.encrypt_meta_, rows_info))) {
        LOG_WARN("failed to insert rows", K(ret));
      }
    } else {
      ObSEArray<ObTabletID, 2> dst_tablet_ids;
      ObSEArray<ObArray<int64_t>, 2> dst_row_ids;
      const ObITableReadInfo &rowkey_read_info = tablet_handle.get_obj()->get_rowkey_read_info();
      bool has_ignore_dup_error = false;
      bool is_first_dup_tmp_rows_info = true;
      if (OB_FAIL(batch_calc_split_dst_rows(*store_ctx.ls_, tablet_handle, relative_table,
          rows_info.rows_, row_count, store_ctx.timeout_, dst_tablet_ids, dst_row_ids))) {
        LOG_WARN("failed to batch calc split dst rows", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < dst_row_ids.count(); i++) {
        if (dst_row_ids[i].empty()) {
          // do nothing
        } else {
          const int64_t dst_row_count = dst_row_ids[i].count();
          SMART_VAR(ObRowsInfo, tmp_rows_info) {
            ObDatumRow *dst_tbl_rows = nullptr;
            dml_split_ctx.reuse();
            if (OB_FAIL(dml_split_ctx.prepare_write_dst(
                tablet_handle.get_obj()->get_tablet_meta().tablet_id_, dst_tablet_ids.at(i), store_ctx, relative_table))) {
              LOG_WARN("failed to prepare split dml ctx", K(ret));
            } else if (OB_FAIL(ObDatumRowUtils::ob_create_rows_shallow_copy(
                dml_split_ctx.allocator_, rows_info.rows_, dst_row_ids[i], dst_tbl_rows))) {
              LOG_WARN("failed to create rows", K(ret));
            } else if (OB_FAIL(tmp_rows_info.init(col_descs, dml_split_ctx.dst_relative_table_, store_ctx, rowkey_read_info))) {
              LOG_WARN("Failed to init rows info", K(ret), K(dml_split_ctx.dst_relative_table_));
            } else if (OB_FAIL(tmp_rows_info.assign_rows(dst_row_count, dst_tbl_rows))) {
              LOG_WARN("failed to assign rows", K(ret));
            } else if (OB_FAIL(tmp_rows_info.set_need_find_all_duplicate_rows(rows_info.need_find_all_duplicate_key(),
                                                                              rows_info.dup_row_column_ids_,
                                                                              rows_info.dup_row_iter_))) {
              LOG_WARN("failed to set_need_find_all_duplicate_rows", K(ret));
            } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->insert_rows(
                  dml_split_ctx.dst_relative_table_, store_ctx, check_exist, col_descs,
                  dml_param.encrypt_meta_, tmp_rows_info))) {
              LOG_WARN("fail to insert splitted tablet", K(ret), K(i), K(dst_row_count), K(tmp_rows_info.need_find_all_duplicate_key()));
              if (OB_ERR_PRIMARY_KEY_DUPLICATE != ret) {
                // do nothing
              } else if (tmp_rows_info.need_find_all_duplicate_key()) {
                has_ignore_dup_error = true;
                ret = OB_SUCCESS; // ignore this error to continue to find all duplicate rows
                int tmp_ret = OB_SUCCESS;
                if (OB_TMP_FAIL(get_conflict_rows(dml_split_ctx.dst_tablet_handle_,
                                                  dml_split_ctx.dst_relative_table_,
                                                  store_ctx,
                                                  dml_param,
                                                  tmp_rows_info))) {
                  LOG_WARN("failed to get conflict row(s)", K(ret), K(tmp_rows_info));
                  ret = tmp_ret;
                }
                if (OB_SUCC(ret)) {
                  if (OB_FAIL(rows_info.assign_duplicate_splitted_rows_info(is_first_dup_tmp_rows_info,  dst_row_ids[i], tmp_rows_info))) {
                    LOG_WARN("fail to assign_duplicate_splitted_rows_info", K(ret), K(i), K(tmp_rows_info), K(rows_info));
                  }
                  if (is_first_dup_tmp_rows_info) {
                    is_first_dup_tmp_rows_info = false;
                  }
                }
              } else { // no need to get duplicate key
                OB_ASSERT(is_first_dup_tmp_rows_info == true);
                int tmp_ret = OB_SUCCESS;
                if (OB_TMP_FAIL(rows_info.assign_duplicate_splitted_rows_info(is_first_dup_tmp_rows_info,  dst_row_ids[i], tmp_rows_info))) {
                  LOG_WARN("fail to assign_duplicate_splitted_rows_info", K(ret), K(i), K(tmp_rows_info), K(rows_info));
                  ret = tmp_ret;
                }
              }
            } else {
              // success
            }
          }
        }
      } // end of for
      if (OB_SUCC(ret)) {
        ret = has_ignore_dup_error ? OB_ERR_PRIMARY_KEY_DUPLICATE : OB_SUCCESS; // recover the error
      }
    }
  }
  return ret;
}

int ObLSTabletService::update_rows_wrap(
    ObTabletHandle &tablet_handle,
    const blocksstable::ObDatumRow *data_row_for_lob,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObColDescIArray &col_descs,
    const ObIArray<int64_t> &update_idx,
    const blocksstable::ObDatumRow *old_rows,
    const common::ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr,
    ObRowsInfo &rows_info)
{
  const int64_t row_count = rows_info.get_rowkey_cnt();
  int ret = tablet_handle.get_obj()->update_rows(relative_table, store_ctx,
      col_descs, update_idx, old_rows, encrypt_meta_arr, rows_info);
  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ret = OB_SUCCESS;
    ObDmlSplitCtx dml_split_ctx;
    if (nullptr != data_row_for_lob) {
      if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle, data_row_for_lob,
          store_ctx, relative_table, *data_row_for_lob))) {
        LOG_WARN("failed to prepare split dml ctx", K(ret));
      } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->update_rows(
          dml_split_ctx.dst_relative_table_, store_ctx, col_descs, update_idx,
          old_rows, encrypt_meta_arr, rows_info))) {
        LOG_WARN("failed to update row", K(ret));
      }
    } else {
      ObSEArray<ObTabletID, 2> dst_tablet_ids;
      ObSEArray<ObArray<int64_t>, 2> dst_row_ids;
      const ObITableReadInfo &rowkey_read_info = tablet_handle.get_obj()->get_rowkey_read_info();
      if (OB_FAIL(batch_calc_split_dst_rows(*store_ctx.ls_, tablet_handle, relative_table,
          rows_info.rows_, row_count, store_ctx.timeout_, dst_tablet_ids, dst_row_ids))) {
        LOG_WARN("failed to batch calc split dst rows", K(ret));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < dst_row_ids.count(); i++) {
        if (!dst_row_ids[i].empty()) {
          SMART_VAR(ObRowsInfo, tmp_rows_info) {
            ObDatumRow *dst_old_rows = nullptr;
            ObDatumRow *dst_new_rows = nullptr;
            const int64_t dst_row_count = dst_row_ids[i].count();
            dml_split_ctx.reuse();
            if (OB_FAIL(dml_split_ctx.prepare_write_dst(
                tablet_handle.get_obj()->get_tablet_meta().tablet_id_,
                dst_tablet_ids.at(i), store_ctx, relative_table))) {
              LOG_WARN("failed to prepare split dml ctx", K(ret));
            } else if (OB_FAIL(ObDatumRowUtils::ob_create_rows_shallow_copy(
                  dml_split_ctx.allocator_, old_rows, dst_row_ids[i], dst_old_rows))) {
              LOG_WARN("failed to create old rows", K(ret));
            } else if (OB_FAIL(ObDatumRowUtils::ob_create_rows_shallow_copy(
                  dml_split_ctx.allocator_, rows_info.rows_, dst_row_ids[i], dst_new_rows))) {
              LOG_WARN("failed to create new rows", K(ret));
            } else if (OB_FAIL(tmp_rows_info.init(
                col_descs, dml_split_ctx.dst_relative_table_, store_ctx, rowkey_read_info))) {
              LOG_WARN("Failed to init rows info", K(ret), K(dml_split_ctx.dst_relative_table_));
            } else if (OB_FAIL(tmp_rows_info.assign_rows(dst_row_count, dst_new_rows))) {
              LOG_WARN("failed to assign rows", K(ret));
            } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->update_rows(
                dml_split_ctx.dst_relative_table_, store_ctx, col_descs, update_idx,
                dst_old_rows, encrypt_meta_arr, tmp_rows_info))) {
              LOG_WARN("failed to update tablet rows", K(ret));
            }
          }
        }
      }
    }
  }
  return ret;
}
int ObLSTabletService::insert_row_wrap(
    ObTabletHandle &tablet_handle,
    const blocksstable::ObDatumRow *data_row_for_lob,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const bool check_exists,
    const ObIArray<share::schema::ObColDesc> &col_descs,
    ObDatumRow &row,
    const ObIArray<transaction::ObEncryptMetaCache> *encrypt_meta_arr)
{
  int ret = tablet_handle.get_obj()->insert_row(
      relative_table, store_ctx, check_exists, col_descs, row, encrypt_meta_arr);
  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ObDmlSplitCtx dml_split_ctx;
    if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle, data_row_for_lob, store_ctx, relative_table, row))) {
      LOG_WARN("failed to prepare split dml ctx", K(ret));
    } else if (OB_FAIL(dml_split_ctx.dst_tablet_handle_.get_obj()->insert_row(
        dml_split_ctx.dst_relative_table_, store_ctx, check_exists, col_descs, row, encrypt_meta_arr))) {
      LOG_WARN("failed to do insert to in dst tablet", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::check_row_locked_by_myself_wrap(
    ObTabletHandle &tablet_handle,
    const blocksstable::ObDatumRow *data_row_for_lob,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDatumRowkey &rowkey,
    bool &locked)
{
  int ret = check_row_locked_by_myself(tablet_handle, relative_table, store_ctx, rowkey, locked);
  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ObDmlSplitCtx dml_split_ctx;
    if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle, data_row_for_lob, store_ctx, relative_table, rowkey))) {
      LOG_WARN("failed to prepare split dml ctx", K(ret));
    } else if (OB_FAIL(check_row_locked_by_myself(dml_split_ctx.dst_tablet_handle_, dml_split_ctx.dst_relative_table_, store_ctx, rowkey, locked))) {
      LOG_WARN("failed to check row locked", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::get_conflict_rows(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = rows_info.get_rowkey_cnt();

  if (OB_UNLIKELY(!rows_info.has_set_error() || !rows_info.have_conflict())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("no confict rows in rows_info", K(ret), K(rows_info));
  // just project the dup row from input row for primary table
  } else if (!relative_table.is_storage_index_table()) {
    if (OB_FAIL(get_conflict_rows_by_project(relative_table, rows_info))) {
      LOG_WARN("fail to project conflict rows", K(ret), K(rows_info));
    }
  } else if (OB_FAIL(get_conflict_rows_by_multi_get(tablet_handle,
                                                    relative_table,
                                                    store_ctx,
                                                    dml_param,
                                                    rows_info))) {
    if (OB_TABLET_IS_SPLIT_SRC == ret) {
      if (OB_FAIL(get_conflict_rows_by_single_get(tablet_handle,
                                                    relative_table,
                                                    store_ctx,
                                                    dml_param,
                                                    rows_info))) {

      }
    }
  }
  return ret;
}

int ObLSTabletService::get_conflict_rows_by_project(
    ObRelativeTable &relative_table,
    const ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  common::ObSEArray<int32_t, 16> projector;
  const share::schema::ObTableSchemaParam *schema_param = relative_table.get_schema_param();
  const int64_t row_count = rows_info.get_rowkey_cnt();
  const common::ObIArray<uint64_t> &out_col_ids = *rows_info.dup_row_column_ids_;
  blocksstable::ObDatumRowIterator *&dup_row_iter = *rows_info.dup_row_iter_;
  ObValueRowIterator *dup_value_iter = nullptr;

  for (int32_t i = 0; OB_SUCC(ret) && i < out_col_ids.count(); ++i) {
    int idx = OB_INVALID_INDEX;
    if (OB_FAIL(schema_param->get_col_map().get(out_col_ids.at(i), idx))) {
      LOG_WARN("get column index from column map failed", K(ret), K(out_col_ids.at(i)));
    } else if (OB_FAIL(projector.push_back(idx))) {
      LOG_WARN("store output projector failed", K(ret));
    }
  }

  if (OB_SUCC(ret)) {
    if (nullptr == dup_row_iter) {
      if (OB_ISNULL(dup_value_iter = ObQueryIteratorFactory::get_insert_dup_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory to alloc ObValueRowIterator", K(ret));
      } else if (OB_FAIL(dup_value_iter->init())) {
        LOG_WARN("failed to initialize ObValueRowIterator", K(ret));
      } else {
        dup_row_iter = dup_value_iter;
      }
    } else {
      dup_value_iter = static_cast<ObValueRowIterator*>(dup_row_iter);
    }
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    if (rows_info.rowkeys_[i].marked_rowkey_.is_row_duplicate()) {
      if (OB_FAIL(dup_value_iter->add_row(rows_info.rows_[rows_info.rowkeys_[i].row_idx_], projector))) {
        LOG_WARN("failed to add row", K(ret), K(i), K(rows_info.rowkeys_[i]));
      }
    }
  }
  return ret;
}

int ObLSTabletService::get_conflict_rows_by_multi_get(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = rows_info.get_rowkey_cnt();
  ObMemAttr mem_attr(MTL_ID(), "GetConflictRow");
  ObSEArray<ObDatumRowkey, 2> rowkeys;
  rowkeys.set_attr(mem_attr);
  ObArenaAllocator get_allocator(mem_attr);

  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    const ObDatumRowkey &datum_rowkey = rows_info.get_rowkey(i);
    if (!rows_info.is_row_duplicate(i)) {
      LOG_DEBUG("not conflict row", K(row_count), K(i), K(datum_rowkey));
    } else if (OB_FAIL(rowkeys.push_back(datum_rowkey))) {
      LOG_WARN("Failed to push back datum_rowkey", K(ret), K(row_count), K(i), K(datum_rowkey));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(rowkeys.count() == 0)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpect rowkwys count", K(ret));
  } else if (rowkeys.count() > 1 && !rows_info.is_sorted()) { // need sort
    ObDatumComparor<ObDatumRowkey> comparor(*rows_info.get_datum_utils(), ret, false/*reverse*/);
    lib::ob_sort(rowkeys.begin(), rowkeys.end(), comparor);
  }

  if (OB_SUCC(ret)) {
    ObTablet *data_tablet = tablet_handle.get_obj();
    ObDatumRow *out_row = nullptr;
    ObValueRowIterator *dup_value_iter = nullptr;
    blocksstable::ObDatumRowIterator *&dup_row_iter = *rows_info.dup_row_iter_;

    if (OB_ISNULL(data_tablet)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("tablet is null", K(ret), K(tablet_handle));
    } else if (nullptr == dup_row_iter) {
      if (OB_ISNULL(dup_value_iter = ObQueryIteratorFactory::get_insert_dup_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory to alloc ObValueRowIterator", K(ret));
      } else if (OB_FAIL(dup_value_iter->init())) {
        LOG_WARN("failed to initialize ObValueRowIterator", K(ret));
      } else {
        dup_row_iter = dup_value_iter;
      }
    } else {
      dup_value_iter = static_cast<ObValueRowIterator*>(dup_row_iter);
    }

    if (OB_SUCC(ret)) {
      ObRowGetter row_getter(get_allocator, *data_tablet);
      if (OB_FAIL(init_row_getter(row_getter,
                                  store_ctx,
                                  dml_param,
                                  *rows_info.dup_row_column_ids_,
                                  relative_table,
                                  rowkeys.count() > 1/*is_multi_get*/,
                                  true/*skip_read_lob*/))) {
        LOG_WARN("failed to init single row getter", K(ret), K(rows_info));
      } else if (OB_FAIL(row_getter.open(rowkeys, false/*use_fuse_row_cache*/))) {
        LOG_WARN("failed to open storage row", K(ret), K(rowkeys));
      }
      while (OB_SUCC(ret)) {
        if (OB_FAIL(row_getter.get_next_row(out_row))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get single storage row", K(ret));
          }
        } else if (OB_FAIL(dup_value_iter->add_row(*out_row))) {
          LOG_WARN("failed to add row", K(ret));
        }
      }
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
      } else if (OB_TABLET_IS_SPLIT_SRC != ret) {
        if (nullptr != dup_row_iter) {
          ObQueryIteratorFactory::free_insert_dup_iter(dup_row_iter);
          dup_row_iter = nullptr;
        }
      }
    }
  }
  return ret;
}

int ObLSTabletService::get_conflict_rows_by_single_get(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &relative_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObRowsInfo &rows_info)
{
  int ret = OB_SUCCESS;
  const int64_t row_count = rows_info.get_rowkey_cnt();

  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    const ObDatumRowkey &datum_rowkey = rows_info.get_rowkey(i);
    if (!rows_info.is_row_duplicate(i)) {
      LOG_DEBUG("not conflict row", K(row_count), K(i), K(datum_rowkey));
    } else if (OB_FAIL(single_get_conflict_row(tablet_handle, relative_table,
        store_ctx, dml_param, *rows_info.dup_row_column_ids_, datum_rowkey, *rows_info.dup_row_iter_))) {
      if (OB_TABLET_IS_SPLIT_SRC == ret) {
        ObDmlSplitCtx dml_split_ctx;
        if (OB_FAIL(dml_split_ctx.prepare_write_dst(
            tablet_handle, dml_param.data_row_for_lob_, store_ctx, relative_table, datum_rowkey))) {
          LOG_WARN("failed to prepare split dml ctx", K(ret));
        } else if (OB_FAIL(single_get_conflict_row(dml_split_ctx.dst_tablet_handle_,
            dml_split_ctx.dst_relative_table_, store_ctx, dml_param,
            *rows_info.dup_row_column_ids_, datum_rowkey, *rows_info.dup_row_iter_))) {
          LOG_WARN("failed to get conflict rows", K(ret));
        }
      } else {
        LOG_WARN("fail to get conflict row", K(ret), K(datum_rowkey));
      }
    }
  }

  return ret;
}

int ObLSTabletService::single_get_conflict_row(
    ObTabletHandle &tablet_handle,
    ObRelativeTable &data_table,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const common::ObIArray<uint64_t> &out_col_ids,
    const ObDatumRowkey &datum_rowkey,
    blocksstable::ObDatumRowIterator *&dup_row_iter)
{
  int ret = OB_SUCCESS;
  ObMemAttr mem_attr(MTL_ID(), "GetConflictRow");
  ObArenaAllocator get_allocator(mem_attr);
  ObTablet *data_tablet = tablet_handle.get_obj();
  ObDatumRow *out_row = nullptr;
  ObValueRowIterator *dup_value_iter = nullptr;

  if (OB_ISNULL(data_tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tablet is null", K(ret), K(tablet_handle));
  } else {
    ObRowGetter row_getter(get_allocator, *data_tablet);
    if (OB_FAIL(init_row_getter(row_getter, store_ctx, dml_param, out_col_ids,
        data_table, false/*is_multi_get*/, true/*skip_read_lob*/))) {
      LOG_WARN("failed to init single row getter", K(ret), K(out_col_ids));
    } else if (OB_FAIL(row_getter.open(datum_rowkey, false/*use_fuse_row_cache*/))) {
      LOG_WARN("failed to open storage row", K(ret), K(datum_rowkey));
    } else if (OB_FAIL(row_getter.get_next_row(out_row))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("failed to get single storage row", K(ret));
      } else {
        ret = OB_SUCCESS;
      }
    } else if (nullptr == dup_row_iter) {
      if (OB_ISNULL(dup_value_iter = ObQueryIteratorFactory::get_insert_dup_iter())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory to alloc ObValueRowIterator", K(ret));
      } else if (OB_FAIL(dup_value_iter->init())) {
        LOG_WARN("failed to initialize ObValueRowIterator", K(ret));
      } else {
        dup_row_iter = dup_value_iter;
      }
    } else {
      dup_value_iter = static_cast<ObValueRowIterator*>(dup_row_iter);
    }
  }

  if (OB_SUCC(ret)) {
    if (out_row != nullptr && OB_FAIL(dup_value_iter->add_row(*out_row))) {
      LOG_WARN("failed to add row", K(ret));
    }
  } else if (OB_TABLET_IS_SPLIT_SRC != ret) {
    if (nullptr != dup_row_iter) {
      ObQueryIteratorFactory::free_insert_dup_iter(dup_row_iter);
      dup_row_iter = nullptr;
    }
  }
  return ret;
}

int ObLSTabletService::init_row_getter(
    ObRowGetter &row_getter,
    ObStoreCtx &store_ctx,
    const ObDMLBaseParam &dml_param,
    const ObIArray<uint64_t> &out_col_ids,
    ObRelativeTable &relative_table,
    const bool is_multi_get,
    const bool skip_read_lob)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(row_getter.init_dml_access_param(relative_table, out_col_ids, skip_read_lob))) {
    LOG_WARN("init dml access param failed", K(ret));
  } else if (OB_FAIL(row_getter.prepare_cached_iter_node(dml_param, is_multi_get))) {
    LOG_WARN("prepare cached iter node failed", K(ret));
  } else if (OB_FAIL(row_getter.init_dml_access_ctx(store_ctx, skip_read_lob))) {
    LOG_WARN("init dml access ctx failed", K(ret));
  }

  return ret;
}

int ObLSTabletService::table_refresh_row_wrap(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow &row)
{
  int ret = table_refresh_row(tablet_handle, run_ctx.relative_table_,
      run_ctx.store_ctx_, run_ctx.dml_param_, *run_ctx.col_descs_,
      run_ctx.dml_param_.lob_allocator_, row);
  if (OB_TABLET_IS_SPLIT_SRC == ret) {
    ObDmlSplitCtx dml_split_ctx;
    if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle,
        run_ctx.dml_param_.data_row_for_lob_,
        run_ctx.store_ctx_, run_ctx.relative_table_, row))) {
      LOG_WARN("failed to prepare split dml ctx", K(ret));
    } else if (OB_FAIL(table_refresh_row(dml_split_ctx.dst_tablet_handle_,
        dml_split_ctx.dst_relative_table_, run_ctx.store_ctx_, run_ctx.dml_param_,
        *run_ctx.col_descs_, run_ctx.dml_param_.lob_allocator_, row))) {
      LOG_WARN("failed to table refresh row", K(ret));
    }
  }
  return ret;
}

int ObLSTabletService::check_old_row_legitimacy_wrap(
    const ObStoreCmpFuncs &cmp_funcs,
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const int64_t row_count,
    const blocksstable::ObDatumRow *old_rows,
    int64_t &error_row_idx)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < row_count; i++) {
    ret = check_old_row_legitimacy(cmp_funcs, tablet_handle, run_ctx.relative_table_,
        run_ctx.store_ctx_, run_ctx.dml_param_, run_ctx.column_ids_, run_ctx.col_descs_,
        run_ctx.is_need_check_old_row_, run_ctx.is_udf_, run_ctx.dml_flag_, old_rows[i]);

    if (OB_TABLET_IS_SPLIT_SRC == ret) {
      ObDmlSplitCtx dml_split_ctx;
      if (OB_FAIL(dml_split_ctx.prepare_write_dst(tablet_handle, run_ctx.dml_param_.data_row_for_lob_,
          run_ctx.store_ctx_, run_ctx.relative_table_, old_rows[i]))) {
        LOG_WARN("failed to prepare split dml ctx", K(ret));
      } else if (OB_FAIL(check_old_row_legitimacy(cmp_funcs, dml_split_ctx.dst_tablet_handle_,
          dml_split_ctx.dst_relative_table_, run_ctx.store_ctx_, run_ctx.dml_param_, run_ctx.column_ids_,
          run_ctx.col_descs_, run_ctx.is_need_check_old_row_, run_ctx.is_udf_, run_ctx.dml_flag_, old_rows[i]))) {
        LOG_WARN("failed to check old row legitimacy", K(ret));
        error_row_idx = i;
      }
    } else {
      error_row_idx = i;
    }
  }
  if (OB_ERR_DEFENSIVE_CHECK == ret) {
    int tmp_ret = OB_SUCCESS;
    ObOldRowCheckDumper dumper(run_ctx, old_rows[error_row_idx]);
    if (OB_TMP_FAIL(dumper.dump_diag_log())) {
      LOG_WARN("Failed to dump diag log for defensive check", KR(tmp_ret));
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
  } else if (GCTX.is_shared_storage_mode() && tablet->is_empty_shell()) {
    // just skip if tablet is empty shell
    is_same = false;
  } else if (transfer_start_scn != tablet->get_tablet_meta().transfer_info_.transfer_start_scn_) {
    is_same = false;
    LOG_ERROR("rollback tablet is not same, cannot rollback", K(ls_id), K(tablet_id), K(transfer_start_scn), KPC(tablet));
  } else {
    is_same = true;
  }
  return ret;
}

int ObLSTabletService::process_lob_after_insert(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    blocksstable::ObDatumRow *rows,
    int64_t row_count)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < run_ctx.lob_dml_ctx_.task_count(); ++i) {
    ObLobDataInsertTask &task = run_ctx.lob_dml_ctx_.task(i);
    const ObColDesc &column = run_ctx.col_descs_->at(task.col_idx_);
    blocksstable::ObDatumRow &datum_row = rows[task.row_idx_];
    if (task.col_idx_ >= run_ctx.col_descs_->count() || task.row_idx_ >= row_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col idx or row idx is invalid", K(ret), K(task), KPC(run_ctx.col_descs_), K(row_count));
    } else if (OB_FAIL(ObLobTabletDmlHelper::process_lob_column_after_insert(run_ctx, datum_row, task))) {
      LOG_WARN("process_lob_column_after_insert fail", K(ret), K(column), K(i), K(task), K(datum_row));
    }
  }

  if (OB_SUCC(ret)) {
    run_ctx.lob_dml_ctx_.reuse();
  }
  return ret;
}

int ObLSTabletService::process_lob_after_update(
    ObTabletHandle &tablet_handle,
    ObDMLRunningCtx &run_ctx,
    const ObIArray<int64_t> &update_idx,
    const bool rowkey_change,
    const int64_t row_count,
    blocksstable::ObDatumRow *old_datum_rows,
    blocksstable::ObDatumRow *new_datum_rows)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < run_ctx.lob_dml_ctx_.task_count(); ++i) {
    ObLobDataInsertTask &task = run_ctx.lob_dml_ctx_.task(i);
    blocksstable::ObDatumRow &old_datum_row = old_datum_rows[task.row_idx_];
    blocksstable::ObDatumRow &new_datum_row = new_datum_rows[task.row_idx_];
    if (task.col_idx_ >= run_ctx.col_descs_->count() || task.row_idx_ >= row_count) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("col idx or row idx is invalid", K(ret), K(task), KPC(run_ctx.col_descs_));
    } else if (OB_FAIL(ObLobTabletDmlHelper::process_lob_column_after_update(
        run_ctx, old_datum_row, new_datum_row, rowkey_change, task))) {
      LOG_WARN("process_lob_column_after_update fail", K(ret), K(i), K(task));
    }
  }
  if (OB_SUCC(ret)) {
    run_ctx.lob_dml_ctx_.reuse();
  }
  return ret;
}

int ObLSTabletService::estimate_skip_index_sortedness(
    const uint64_t &table_id,
    const common::ObTabletID &tablet_id,
    const int64_t timeout_us,
    const common::ObIArray<uint64_t> &column_ids,
    const common::ObIArray<uint64_t> &sample_counts,
    common::ObIArray<double> &sortedness,
    common::ObIArray<uint64_t> &res_sample_counts)
{
  int ret = OB_SUCCESS;

  int64_t tenant_id = MTL_ID();
  ObTabletHandle tablet_handle;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObSSTable *latest_major_sstable = nullptr;

  ObMultiVersionSchemaService *schema_service = MTL(ObTenantSchemaService *)->get_schema_service();
  ObSchemaGetterGuard schema_guard;
  const ObTableSchema *table_schema = nullptr;

  ObSkipIndexSortedness calcer;

  // when co_sstable does't have cg_sstable, we consider it as a row store table to calc
  bool use_row_store_table_to_calc = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("Not init", KR(ret), K_(is_inited));
  } else if (OB_UNLIKELY(column_ids.count() != sample_counts.count())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("The length of column_ids is not match sample_counts", K(column_ids.count()), K(sample_counts.count()), KR(ret));
  } else if (OB_FAIL(sortedness.reserve(column_ids.count()))) {
    LOG_WARN("Fail to reserve sortedness array", KR(ret));
  } else if (OB_FAIL(res_sample_counts.reserve(column_ids.count()))) {
    LOG_WARN("Fail to reserve sortedness array", KR(ret));
  } else if (OB_FAIL(get_tablet(tablet_id, tablet_handle))) {
    LOG_WARN("Fail to get tablet", KR(ret), K(tablet_id));
  } else if (OB_FAIL(tablet_handle.get_obj()->fetch_table_store(table_store_wrapper))) {
    LOG_WARN("Fail to fetch table store", KR(ret));
  } else if (FALSE_IT(latest_major_sstable = static_cast<ObSSTable *>(
                          table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(
                              true)))) {
  } else if (OB_ISNULL(latest_major_sstable) || latest_major_sstable->is_empty()) {
    // there is no major sstable, we default return the sortedness as 0
    LOG_DEBUG("No major sstable, sortedness will be set to 0",
             K(table_id),
             K(tablet_id),
             K(column_ids));
    for (int i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
      if (OB_FAIL(sortedness.push_back(0))) {
        LOG_WARN("Fail to push back to sortedness when no major sstable", KR(ret));
      } else if (OB_FAIL(res_sample_counts.push_back(0))) {
        LOG_WARN("Fail to push back to res_sample_counts when no major sstable", KR(ret));
      }
    }
  } else if (OB_ISNULL(schema_service)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get schema service", KR(ret));
  } else if (OB_FAIL(schema_service->get_tenant_schema_guard(tenant_id, schema_guard))) {
    LOG_WARN("Fail to get schema guard", KR(ret), K(tenant_id));
  } else if (OB_FAIL(schema_guard.get_table_schema(tenant_id, table_id, table_schema))) {
    LOG_WARN("Fail to get table schema", KR(ret), K(tenant_id), K(tablet_id));
  } else if (OB_ISNULL(table_schema)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("Fail to get table schema", KR(ret), K(tenant_id), K(tablet_id));
  } else if (latest_major_sstable->is_co_sstable()) {
    // for column store table, there are a few points to note:
    //   1. Firstly use cg_sstable to calculate skip index sortedness
    //   2. It may not have cg_sstable here
    //   3. notice that transform column_id to cg_idx
    ObCOSSTableV2 *cosstable = static_cast<ObCOSSTableV2 *>(latest_major_sstable);
    const ObITableReadInfo *index_read_info = nullptr;

    // the column_param is used to transform column_id to cg_idx
    common::ObArenaAllocator allocator(common::ObMemAttr(tenant_id, "SkipIndexEsti"));
    ObColumnParam *column_param = nullptr;

    if (cosstable->is_cgs_empty_co_table()) {
      // there are not cg_sstable here
      // we consider this sstable as a row store table
      use_row_store_table_to_calc = true;
    } else if (OB_FAIL(ObTableParam::alloc_column(allocator, column_param))) {
      LOG_WARN("Fail to alloc column param", KR(ret));
    } else if (OB_FAIL(MTL(ObTenantCGReadInfoMgr *)->get_index_read_info(index_read_info))) {
      LOG_WARN("Fail to get cg index read info", KR(ret));
    } else {
      // the table contains cg_sstable, we need do as follows:
      //   1. transform the column_id to cg_idx
      //   2. get the cg sstable and calc skip index sortedness
      for (int i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
        ObSSTableWrapper cg_wrapper;
        ObSSTable *cg_sstable = nullptr;
        double tmp_sortedness = 0.0;

        const ObColumnSchemaV2 *column_schema = nullptr;
        int32_t cg_idx = 0;

        if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_ids.at(i)))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("Fail to get column schema", KR(ret), K(table_schema), K(column_ids), K(i));
        } else if (column_schema->is_virtual_generated_column()) {
          // for virtual column, the sortedness is 0
          if (OB_FAIL(sortedness.push_back(0))) {
            LOG_WARN("Fail to push back sortedness", KR(ret));
          } else if (OB_FAIL(res_sample_counts.push_back(0))) {
            LOG_WARN("Fail to push back to res_sample_counts when no major sstable", KR(ret));
          }
        } else if (OB_FAIL(ObTableParam::convert_column_schema_to_param(*column_schema,
                                                                        *column_param))) {
          LOG_WARN("Fail to convert column schema to param", KR(ret));
        } else if (OB_FAIL(table_schema->get_column_group_index(*column_param, false, cg_idx))) {
          LOG_WARN("Fail to get column group idx", KR(ret), KPC(column_param));
        } else if (cg_idx >= cosstable->get_cs_meta().get_column_group_count() || cg_idx < 0) {
          // no cg sstable for this column, the sortedness is 0
          if (OB_FAIL(sortedness.push_back(0))) {
            LOG_WARN("Fail to push back sortedness", KR(ret));
          } else if (OB_FAIL(res_sample_counts.push_back(0))) {
            LOG_WARN("Fail to push back to res_sample_counts when no major sstable", KR(ret));
          }
        } else if (OB_FAIL(cosstable->fetch_cg_sstable(cg_idx, cg_wrapper))) {
          LOG_WARN("Fail to get cg sstable", KR(ret), K(cg_idx), K(i));
        } else if (OB_FAIL(cg_wrapper.get_loaded_column_store_sstable(cg_sstable))) {
          LOG_WARN("Fail to get sstable from wrapper", K(ret), K(cg_wrapper));
        } else if (cg_sstable->is_empty()) {
          // fast path for empty sstable
          if (OB_FAIL(sortedness.push_back(0))) {
            LOG_WARN("Fail to push back sortedness", KR(ret));
          } else if (OB_FAIL(res_sample_counts.push_back(0))) {
            LOG_WARN("Fail to push back to res_sample_counts when no major sstable", KR(ret));
          }
        } else if (FALSE_IT(calcer.reset())) {
        } else if (OB_FAIL(calcer.init(*cg_sstable,
                                       *table_schema,
                                       index_read_info,
                                       sample_counts.at(i),
                                       column_ids.at(i),
                                       res_sample_counts))) {
          LOG_WARN("Fail to init skip index sortedness",
                   KR(ret),
                   KPC(cg_sstable),
                   KPC(table_schema),
                   K(column_ids),
                   K(sample_counts),
                   K(cg_idx),
                   K(i));
        } else if (OB_FAIL(calcer.sample_and_calc(tmp_sortedness))) {
          LOG_WARN("Fail to sample and calc sortedness", KR(ret));
        } else if (OB_FAIL(sortedness.push_back(tmp_sortedness))) {
          LOG_WARN("Fail to push back sortedness", KR(ret));
        }
      }
    }
  } else {
    use_row_store_table_to_calc = true;
  }

  if (OB_SUCC(ret) && use_row_store_table_to_calc) {
    // for row store table, there are a few points to note:
    //   1. column in row store table may not have skip index, we should check it
    //   2. the read_info is not same as column store table
    const ObITableReadInfo &read_info = tablet_handle.get_obj()->get_rowkey_read_info();

    for (int i = 0; OB_SUCC(ret) && i < column_ids.count(); i++) {
      const ObColumnSchemaV2 *column_schema = nullptr;
      double tmp_sortedness = 0;
      if (OB_ISNULL(column_schema = table_schema->get_column_schema(column_ids.at(i)))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("Fail to get column schema", KR(ret), KPC(table_schema), K(column_ids), K(i));
      } else if (!column_schema->get_skip_index_attr().has_min_max()
                 || column_schema->is_virtual_generated_column()) {
        // when there's not skip index, we consider the sortedness as 0
        LOG_WARN("No skip index in column", K(table_id), KPC(column_schema));
        if (OB_FAIL(sortedness.push_back(0))) {
          LOG_WARN("Fail to push back sortedness", KR(ret));
        } else if (OB_FAIL(res_sample_counts.push_back(0))) {
          LOG_WARN("Fail to push back to res_sample_counts when no major sstable", KR(ret));
        }
      } else if (FALSE_IT(calcer.reset())) {
      } else if (OB_FAIL(calcer.init(*latest_major_sstable,
                                     *table_schema,
                                     &read_info,
                                     sample_counts.at(i),
                                     column_ids.at(i),
                                     res_sample_counts))) {
        LOG_WARN("Fail to init skip index sortedness",
                 KR(ret),
                 KPC(latest_major_sstable),
                 KPC(table_schema),
                 K(column_ids),
                 K(sample_counts),
                 K(i));
      } else if (OB_FAIL(calcer.sample_and_calc(tmp_sortedness))) {
        LOG_WARN("Fail to sample and calc sortedness", KR(ret));
      } else if (OB_FAIL(sortedness.push_back(tmp_sortedness))) {
        LOG_WARN("Fail to push back sortedness", KR(ret));
      }
    }
  }

  return ret;
}

int ObLSTabletService::scan_block_stat(
    const ObTabletHandle &tablet_handle,
    ObBlockStatScanParam &scan_param,
    ObBlockStatIterator &iter)
{
  int ret = OB_SUCCESS;
  bool allow_to_read = false;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!tablet_handle.is_valid() || !scan_param.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(tablet_handle), K(scan_param));
  } else if (FALSE_IT(allow_to_read_mgr_.load_allow_to_read_info(allow_to_read))) {
  } else if (OB_UNLIKELY(!allow_to_read)) {
    ret = OB_REPLICA_NOT_READABLE;
    LOG_WARN("ls not allow to read", K(ret), KPC_(ls));
  } else if (OB_FAIL(prepare_scan_table_param(*scan_param.get_scan_param(), *(MTL(ObTenantSchemaService *)->get_schema_service())))) {
    LOG_WARN("fail to prepare scan table param", K(ret), K(scan_param), K(tablet_handle));
  } else if (OB_UNLIKELY(scan_param.get_scan_param()->fb_snapshot_.is_min())) {
    ret = OB_SNAPSHOT_DISCARDED;
  } else if (OB_FAIL(iter.init(tablet_handle, scan_param))) {
    LOG_WARN("fail to init block stat iterator", K(ret), K(scan_param), K(tablet_handle));
  }
  return ret;
}

#ifdef OB_BUILD_SHARED_STORAGE
int ObLSTabletService::update_tablet_ss_change_version(
    const share::SCN &reorg_scn,
    const common::ObTabletID &tablet_id,
    const share::SCN &ss_change_version,
    const bool &fully_applied)
{
  int ret = OB_SUCCESS;
  const ObTabletMapKey key(ls_->get_ls_id(), tablet_id);
  ObTabletHandle old_handle;
  ObTimeGuard time_guard("ObLSTabletService::update_tablet_ss_change_version", 1_s);
  ObBucketHashWLockGuard lock_guard(bucket_lock_, tablet_id.hash());
  uint64_t data_version = 0;
  time_guard.click("Lock");
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("not inited", K(ret), K_(is_inited));
  } else if (OB_UNLIKELY(!reorg_scn.is_valid() || !tablet_id.is_valid() || !ss_change_version.is_valid_and_not_min())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid arguments", K(ret), K(reorg_scn), K(tablet_id), K(ss_change_version));
  } else if (OB_FAIL(ObTabletCreateDeleteHelper::get_tablet(key, old_handle))) {
    LOG_WARN("fail to direct get tablet", K(ret), K(key));
  } else if (OB_FAIL(GET_MIN_DATA_VERSION(MTL_ID(), data_version))) {
    LOG_WARN("fail to get min data version", K(ret));
  } else {
    time_guard.click("get_tablet");
    struct UpdateSSChangeVersion : public ObITabletMetaModifier {
      UpdateSSChangeVersion(const share::SCN &ss_change_version)
        : ss_change_version_(ss_change_version) {}
      int modify_tablet_meta(ObTabletMeta &meta) override {
        int ret = OB_SUCCESS;
        if (meta.min_ss_tablet_version_ > ss_change_version_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("ss tablet version monotonicity violated", K(ret),
                    K(meta.min_ss_tablet_version_), K(ss_change_version_), K(meta));
        } else {
          meta.min_ss_tablet_version_ = ss_change_version_;
        }
        return ret;
      }
      const share::SCN &ss_change_version_;
    } modifier (ss_change_version);
    ObMetaDiskAddr disk_addr;
    ObTabletHandle new_handle;
    const ObTablet &old_tablet = *old_handle.get_obj();
    const share::SCN &old_ss_change_version = old_tablet.get_min_ss_tablet_version();
    const share::SCN &tablet_pointer_ss_change_version = fully_applied ? ss_change_version : share::SCN::invalid_scn();
    int32_t private_transfer_epoch = -1;
    int64_t tablet_meta_version = 0;
    if (OB_FAIL(old_tablet.get_private_transfer_epoch(private_transfer_epoch))) {
      LOG_WARN("failed to get transfer epoch", K(ret), "old_tablet_meta", old_tablet.get_tablet_meta());
    } else if (OB_FAIL(alloc_private_tablet_meta_version_without_lock(key, tablet_meta_version))) {
      LOG_WARN("failed to alloc tablet meta version", K(ret), K(key));
    }
    const ObTabletPersisterParam persist_param(data_version,
                                               ls_->get_ls_id(),
                                               ls_->get_ls_epoch(),
                                               tablet_id,
                                               private_transfer_epoch,
                                               tablet_meta_version);
    if (FAILEDx(ObTabletPersister::persist_and_transform_only_tablet_meta(reorg_scn, persist_param, old_tablet, modifier, new_handle))) {
      LOG_WARN("fail to persist and transform only tablet meta", K(ret), K(old_tablet), K(ss_change_version));
    } else if (FALSE_IT(time_guard.click("Persist"))) {
    } else if (FALSE_IT(disk_addr = new_handle.get_obj()->tablet_addr_)) {
    } else if (OB_FAIL(safe_update_cas_tablet(key, disk_addr, old_handle, new_handle, time_guard, tablet_pointer_ss_change_version))) {
      LOG_WARN("fail to safe compare and swap tablet", K(ret), K(disk_addr), K(old_handle), K(new_handle));
    } else {
      LOG_INFO("succ to update tablet ss_change_version", K(ret),
               K(key), K(old_ss_change_version), K(ss_change_version), K(fully_applied), K(time_guard));
    }
  }
  return ret;
}
#endif

} // namespace storage
} // namespace oceanbase
