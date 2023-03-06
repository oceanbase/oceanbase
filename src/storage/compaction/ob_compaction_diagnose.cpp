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

#define USING_LOG_PREFIX STORAGE_COMPACTION
#include "ob_compaction_diagnose.h"
#include "ob_tenant_compaction_progress.h"
#include "ob_tablet_merge_task.h"
#include "ob_partition_merge_policy.h"
#include "share/scheduler/ob_dag_warning_history_mgr.h"
#include "ob_tenant_tablet_scheduler.h"
#include "storage/memtable/ob_memtable.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/tx_storage/ob_ls_map.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "rootserver/freeze/ob_major_freeze_service.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/tablet/ob_tablet.h"
namespace oceanbase
{
using namespace storage;
using namespace share;

namespace compaction
{

int64_t ObScheduleSuspectInfo::hash() const
{
  int64_t hash_value = ObMergeDagHash::inner_hash();
  hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  return hash_value;
}

bool ObScheduleSuspectInfo::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(merge_type_ <= INVALID_MERGE_TYPE || merge_type_ >= MERGE_TYPE_MAX
      || !ls_id_.is_valid()
      || !tablet_id_.is_valid())) {
    bret = false;
  }
  return bret;
}

ObScheduleSuspectInfo & ObScheduleSuspectInfo::operator = (const ObScheduleSuspectInfo &other)
{
  tenant_id_ = other.tenant_id_;
  merge_type_ = other.merge_type_;
  ls_id_ = other.ls_id_;
  tablet_id_ = other.tablet_id_;
  add_time_ = other.add_time_;
  strncpy(suspect_info_, other.suspect_info_, strlen(other.suspect_info_));
  return *this;
}

int64_t ObScheduleSuspectInfo::gen_hash(int64_t tenant_id, int64_t dag_hash)
{
  int64_t hash_value = dag_hash;
  hash_value = common::murmurhash(&tenant_id, sizeof(tenant_id), hash_value);
  return hash_value;
}

ObScheduleSuspectInfoMgr::ObScheduleSuspectInfoMgr()
  : is_inited_(false),
    allocator_("scheSuspectInfo", OB_SERVER_TENANT_ID),
    lock_(common::ObLatchIds::INFO_MGR_LOCK)
{
}

int ObScheduleSuspectInfoMgr::init()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(info_map_.create(SUSPECT_INFO_BUCKET_NUM, "scheSuspectInfo"))) {
    COMMON_LOG(WARN, "failed to create dap map", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

void ObScheduleSuspectInfoMgr::destroy()
{
  if (info_map_.created()) {
    info_map_.destroy();
  }
}

int ObScheduleSuspectInfoMgr::add_suspect_info(const int64_t key, ObScheduleSuspectInfo &input_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObScheduleSuspectInfoMgr is not init", K(ret));
  } else {
    ObScheduleSuspectInfo *info = NULL;
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(info_map_.get_refactored(key, info))) {
      if (OB_HASH_NOT_EXIST == ret && info_map_.size() < SUSPECT_INFO_LIMIT) { // first add
        void * buf = nullptr;
        if (OB_ISNULL(buf = allocator_.alloc(sizeof(ObScheduleSuspectInfo)))) {
          ret = common::OB_ALLOCATE_MEMORY_FAILED;
          COMMON_LOG(WARN, "failed to alloc dag", K(ret));
        } else {
          info = new (buf) ObScheduleSuspectInfo();
          *info = input_info;
          if (OB_FAIL(info_map_.set_refactored(key, info))) {
            STORAGE_LOG(WARN, "failed to set suspect info", K(ret), K(key), K(info));
            allocator_.free(info);
            info = nullptr;
          }
        }
      } else {
        STORAGE_LOG(WARN, "failed to get suspect info", K(ret), K(key), K(info));
      }
    } else { // update
      *info = input_info;
    }
  }
  return ret;
}

int ObScheduleSuspectInfoMgr::get_suspect_info(const int64_t key, ObScheduleSuspectInfo &ret_info)
{
  int ret = OB_SUCCESS;
  ObScheduleSuspectInfo *info = NULL;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObScheduleSuspectInfoMgr is not init", K(ret));
  } else {
    common::SpinRLockGuard guard(lock_);
    if (OB_FAIL(info_map_.get_refactored(key, info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to get schedule suspect info", K(ret), K(key), K(info));
      }
    } else {
      ret_info = *info;
    }
  }
  return ret;
}

int ObScheduleSuspectInfoMgr::del_suspect_info(const int64_t key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObScheduleSuspectInfoMgr is not init", K(ret));
  } else {
    ObScheduleSuspectInfo *info = nullptr;
    {
      common::SpinWLockGuard guard(lock_);
      if (OB_FAIL(info_map_.get_refactored(key, info))) {
        if (OB_HASH_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "failed to get schedule suspect info", K(ret), K(key), K(info));
        }
      } else if (OB_FAIL(info_map_.erase_refactored(key))) {
        if (OB_HASH_NOT_EXIST != ret) {
          STORAGE_LOG(WARN, "failed to get schedule suspect info", K(ret), K(key));
        }
      }
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(info)) {
      allocator_.free(info);
      info = nullptr;
    }
  }
  return ret;
}

int ObScheduleSuspectInfoMgr::gc_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObScheduleSuspectInfoMgr is not init", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    int64_t gc_cnt = 0;
    ObScheduleSuspectInfo *info = nullptr;
    const int64_t gc_time = ObTimeUtility::fast_current_time() - GC_INFO_TIME_LIMIT;
    common::SpinWLockGuard guard(lock_);
    for (InfoMap::iterator iter = info_map_.begin(); iter != info_map_.end(); ++iter) {
      if (OB_NOT_NULL(info = iter->second)) {
        if (info->add_time_ < gc_time) {
          if (OB_TMP_FAIL(info_map_.erase_refactored(iter->first))) {
            LOG_WARN("failed to erase from map", K(tmp_ret), K(iter->first));
          } else {
            gc_cnt++;
            allocator_.free(info);
            info = nullptr;
          }
        }
      }
    }
    STORAGE_LOG(INFO, "gc schedule suspect info", K(gc_time), K(gc_cnt), "rest_cnt", info_map_.size());
  }
  return ret;
}

/*
 * ObCompactionDiagnose implement
 * */

const char *ObCompactionDiagnoseInfo::ObDiagnoseStatusStr[DIA_STATUS_MAX] = {
    "NOT_SCHEDULE",
    "RUNNING",
    "FAILED",
    "UNCOMPACTED",
};

const char * ObCompactionDiagnoseInfo::get_diagnose_status_str(ObDiagnoseStatus status)
{
  STATIC_ASSERT(DIA_STATUS_MAX == ARRAYSIZEOF(ObDiagnoseStatusStr), "diagnose status str len is mismatch");
  const char *str = "";
  if (status >= DIA_STATUS_MAX || status < DIA_STATUS_NOT_SCHEDULE) {
    str = "invalid_status";
  } else {
    str = ObDiagnoseStatusStr[status];
  }
  return str;
}

ObCompactionDiagnoseMgr::ObCompactionDiagnoseMgr()
 : is_inited_(false),
   info_array_(nullptr),
   max_cnt_(0),
   idx_(0)
  {
  }

void ObCompactionDiagnoseMgr::reset()
{
  info_array_ = nullptr;
  max_cnt_ = 0;
  idx_ = 0;
  is_inited_ = false;
}

int ObCompactionDiagnoseMgr::init(ObCompactionDiagnoseInfo *info_array, const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(nullptr == info_array || max_cnt <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), K(info_array), K(max_cnt));
  } else {
    info_array_ = info_array;
    max_cnt_ = max_cnt;
    is_inited_ = true;
  }
  return ret;
}

int ObCompactionDiagnoseMgr::check_system_compaction_config(char *tmp_str, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  static const int64_t DEFAULT_COMPACT_TRIGGER = 2;
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
  if (tenant_config.is_valid()) {
    int64_t minor_compact_trigger = tenant_config->minor_compact_trigger;
    if (minor_compact_trigger > DEFAULT_COMPACT_TRIGGER) { // check minor_compact_trigger
      ADD_COMPACTION_INFO_PARAM(tmp_str, buf_len,
          K(minor_compact_trigger), "DEFAULT", DEFAULT_COMPACT_TRIGGER);
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_dag(
    storage::ObMergeType merge_type,
    ObLSID ls_id,
    ObTabletID tablet_id,
    const int64_t merge_version,
    ObTabletMergeDag &dag,
    ObDiagnoseTabletCompProgress &progress)
{
  int ret = OB_SUCCESS;
  // create a fake dag to get compaction progress
  ObTabletMergeDagParam param;
  param.merge_type_ = merge_type;
  param.merge_version_ = merge_version;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.for_diagnose_ = true;

  if (OB_FAIL(dag.init_by_param(&param))) {
    STORAGE_LOG(WARN, "failed to init dag", K(ret), K(param));
  } else if (is_minor_merge(merge_type)) {
    if (OB_FAIL(MTL(ObTenantDagScheduler *)->diagnose_minor_exe_dag(&dag, progress))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to diagnose minor execute dag", K(ret), K(ls_id), K(tablet_id), K(progress));
      }
    }
  } else if (OB_FAIL(MTL(ObTenantDagScheduler *)->diagnose_dag(&dag, progress))) {
    if (OB_HASH_NOT_EXIST != ret) {
      STORAGE_LOG(WARN, "failed to diagnose dag", K(ret), K(ls_id), K(tablet_id), K(progress));
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_all_tablets(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  omt::TenantIdList all_tenants;
  all_tenants.set_label(ObModIds::OB_TENANT_ID_LIST);
  if (OB_SYS_TENANT_ID == tenant_id) {
    GCTX.omt_->get_tenant_ids(all_tenants);
  } else if (OB_FAIL(all_tenants.push_back(tenant_id))) {
    LOG_WARN("failed to push back tenant_id", K(ret), K(tenant_id));
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < all_tenants.size(); ++i) {
    uint64_t tenant_id = all_tenants[i];
    if (!is_virtual_tenant_id(tenant_id)) { // skip virtual tenant
      MTL_SWITCH(tenant_id) {
        (void)diagnose_tenant_tablet();
        (void)diagnose_tenant_major_merge();
      } else {
        if (OB_TENANT_NOT_IN_SERVER != ret) {
          STORAGE_LOG(WARN, "switch tenant failed", K(ret), K(tenant_id));
        } else {
          ret = OB_SUCCESS;
          continue;
        }
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::get_suspect_info(
    const ObMergeType merge_type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObScheduleSuspectInfo &ret_info)
{
  int ret = OB_SUCCESS;
  ObScheduleSuspectInfo input_info;
  input_info.tenant_id_ = MTL_ID();
  input_info.merge_type_ = merge_type;
  input_info.ls_id_ = ls_id;
  input_info.tablet_id_ = tablet_id;
  if (OB_FAIL(ObScheduleSuspectInfoMgr::get_instance().get_suspect_info(input_info.hash(), ret_info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get suspect info", K(ret), K(input_info));
    }
  } else if (ret_info.add_time_ + SUSPECT_INFO_WARNING_THRESHOLD < ObTimeUtility::fast_current_time()) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tenant_tablet()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCompactionDiagnoseMgr is not init", K(ret));
  } else {
    ObSharedGuard<ObLSIterator> ls_iter_guard;
    ObLS *ls = nullptr;
    if (OB_FAIL(MTL(ObLSService *)->get_ls_iter(ls_iter_guard, ObLSGetMod::STORAGE_MOD))) {
      LOG_WARN("failed to get ls iterator", K(ret));
    } else {
      int tmp_ret = OB_SUCCESS;
      bool diagnose_major_flag = false;
      ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler*);
      int64_t compaction_scn = MAX(scheduler->get_frozen_version(), MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version());
      ObTenantFreezeInfoMgr::FreezeInfo freeze_info;

      if (compaction_scn > scheduler->get_merged_version()) { // check major merge
        diagnose_major_flag = true;
        const int64_t merged_version = scheduler->get_merged_version();
        if (merged_version == ObTenantTabletScheduler::INIT_COMPACTION_SCN) {
          // do nothing
        } else if (OB_TMP_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_behind_snapshot_version(merged_version, freeze_info))) {
          LOG_WARN("failed to get freeze info behind snapshot version", K(tmp_ret), K(merged_version));
          if (can_add_diagnose_info()
              && OB_TMP_FAIL(SET_DIAGNOSE_INFO(
                        info_array_[idx_++],
                        MEDIUM_MERGE,
                        MTL_ID(),
                        ObLSID(INT64_MAX),
                        ObTabletID(INT64_MAX),
                        ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                        ObTimeUtility::fast_current_time(),
                        "freeze_info is invalid, merged_version", merged_version))) {
            LOG_WARN("failed to add dignose info about freeze_info", K(tmp_ret), K(merged_version));
          }
        } else {
          compaction_scn = freeze_info.freeze_version;
        }
      }
      (void)diagnose_medium_scn_table(compaction_scn);
      // check tenant suspect info
      if (diagnose_major_flag) {
        ObScheduleSuspectInfo ret_info;
        if (OB_SUCC(get_suspect_info(MEDIUM_MERGE, share::ObLSID(INT64_MAX), ObTabletID(INT64_MAX), ret_info))
              && can_add_diagnose_info()) {
          SET_DIAGNOSE_INFO(
              info_array_[idx_++],
              MEDIUM_MERGE,
              ret_info.tenant_id_,
              share::ObLSID(INT64_MAX),
              ObTabletID(INT64_MAX),
              ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
              ret_info.add_time_,
              "schedule_suspect_info", ret_info.suspect_info_);
        }
      }

      while (OB_SUCC(ret) && can_add_diagnose_info()) { // loop all log_stream
        bool need_merge = false;
        if (OB_FAIL(ls_iter_guard.get_ptr()->get_next(ls))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("failed to get ls", K(ret), KP(ls_iter_guard.get_ptr()));
          }
        } else if (OB_ISNULL(ls)){
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("ls is nullptr", K(ret), KPC(ls));
        } else if (OB_TMP_FAIL(ObTenantTabletScheduler::check_ls_state(*ls, need_merge))) {
          LOG_WARN("failed to check ls state", K(tmp_ret), KPC(ls), K(need_merge));
        } else if (need_merge) {
          const ObLSID &ls_id = ls->get_ls_id();
          bool weak_read_ts_ready = ObTenantTabletScheduler::check_weak_read_ts_ready(compaction_scn, *ls);
          // check weak read ts
          if (diagnose_major_flag
              && !weak_read_ts_ready
              && can_add_diagnose_info()) {
            SET_DIAGNOSE_INFO(
                info_array_[idx_++],
                MEDIUM_MERGE,
                MTL_ID(),
                ls_id,
                ObTabletID(INT64_MAX),
                ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                ObTimeUtility::fast_current_time(),
                "weak read ts is not ready, compaction_scn",
                compaction_scn);
          }
          // check ls suspect info for memtable freezing
          ObScheduleSuspectInfo ret_info;
          if (OB_SUCC(get_suspect_info(MINI_MERGE, ls_id, ObTabletID(INT64_MAX), ret_info))
              && can_add_diagnose_info()) {
            SET_DIAGNOSE_INFO(
                info_array_[idx_++],
                MINI_MERGE,
                ret_info.tenant_id_,
                ls_id,
                ObTabletID(INT64_MAX),
                ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                ret_info.add_time_,
                "schedule_suspect_info", ret_info.suspect_info_);
          }
          ObLSTabletIterator tablet_iter(ObTabletCommon::NO_CHECK_GET_TABLET_TIMEOUT_US);
          if (OB_FAIL(ls->build_tablet_iter(tablet_iter))) {
            LOG_WARN("failed to build ls tablet iter", K(ret), K(ls));
          } else {
            ObTabletHandle tablet_handle;
            while (OB_SUCC(ret) && can_add_diagnose_info()) { // loop all tablets in ls
              if (OB_FAIL(tablet_iter.get_next_tablet(tablet_handle))) {
                if (OB_ITER_END == ret) {
                  ret = OB_SUCCESS;
                  break;
                } else {
                  LOG_WARN("failed to get tablet", K(ret), K(ls_id), K(tablet_handle));
                }
              } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("invalid tablet handle", K(ret), K(ls_id), K(tablet_handle));
              } else {
                if (diagnose_major_flag
                    && weak_read_ts_ready
                    && OB_TMP_FAIL(diagnose_tablet_major_merge(
                            compaction_scn,
                            ls_id,
                            *tablet_handle.get_obj()))) {
                  LOG_WARN("failed to get diagnose major merge", K(tmp_ret));
                }
                if (OB_TMP_FAIL(diagnose_tablet_mini_merge(ls_id, *tablet_handle.get_obj()))) {
                  LOG_WARN("failed to get diagnose mini merge", K(tmp_ret));
                }
                if (OB_TMP_FAIL(diagnose_tablet_minor_merge(ls_id, *tablet_handle.get_obj()))) {
                  LOG_WARN("failed to get diagnose minor merge", K(tmp_ret));
                }
                if (OB_TMP_FAIL(diagnose_tablet_medium_merge(ls_id, *tablet_handle.get_obj()))) {
                  LOG_WARN("failed to get diagnose medium merge", K(tmp_ret));
                }
              }
            } // end of while
            LOG_INFO("finish ls merge diagnose", K(ret), K(ls_id));
          }
        }
      } // end of while
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tenant_major_merge()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCompactionDiagnoseMgr is not init", K(ret));
  } else {
    rootserver::ObMajorFreezeService *major_freeze_service = nullptr;
    bool need_diagnose = false;
    // only leader need diagnose
    if (OB_FAIL(check_if_need_diagnose(major_freeze_service, need_diagnose))) {
      LOG_WARN("fail to check if need diagnose tenant major merge", KR(ret));
    } else if (need_diagnose) {
      if (OB_FAIL(do_tenant_major_merge_diagnose(major_freeze_service))) {
        LOG_WARN("fail to do tenant major merge diagnose", KR(ret));
      } else {
        LOG_INFO("finish diagnose tenant major merge", K(ret));
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::check_if_need_diagnose(
    rootserver::ObMajorFreezeService *&major_freeze_service,
    bool &need_diagnose) const
{
  int ret = OB_SUCCESS;
  need_diagnose = false;
  rootserver::ObPrimaryMajorFreezeService *primary_major_freeze_service = nullptr;
  rootserver::ObRestoreMajorFreezeService *restore_major_freeze_service = nullptr;
  if (OB_ISNULL(primary_major_freeze_service = MTL(rootserver::ObPrimaryMajorFreezeService*))
      || OB_ISNULL(restore_major_freeze_service = MTL(rootserver::ObRestoreMajorFreezeService*))) {
    ret = OB_ERR_UNEXPECTED;
    RS_LOG(ERROR, "primary or restore major_freeze_service is nullptr", KR(ret),
           KP(primary_major_freeze_service), KP(restore_major_freeze_service));
  } else {
    bool is_primary_service = true;
    if (OB_FAIL(rootserver::ObMajorFreezeUtil::get_major_freeze_service(primary_major_freeze_service,
                restore_major_freeze_service, major_freeze_service, is_primary_service))) {
      if (OB_LEADER_NOT_EXIST == ret) {
        ret = OB_SUCCESS; // ignore ret
        LOG_INFO("no need to diagnose tenant major merge on this server");
      } else {
        LOG_WARN("fail to get major_freeze_service", KR(ret));
      }
    } else if (OB_ISNULL(major_freeze_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("major_freeze_service is null", KR(ret));
    } else {
      need_diagnose = true;
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::do_tenant_major_merge_diagnose(
    rootserver::ObMajorFreezeService *major_freeze_service)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(major_freeze_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(major_freeze_service));
  } else {
    SMART_VAR(ObArray<ObTabletReplica>, uncompacted_tablets) {
      if (OB_FAIL(major_freeze_service->get_uncompacted_tablets(uncompacted_tablets))) {
        LOG_WARN("fail to get uncompacted tablets", KR(ret));
      } else {
        int64_t uncompacted_tablets_cnt = uncompacted_tablets.count();
        LOG_INFO("finish get uncompacted tablets for diagnose", K(ret), K(uncompacted_tablets_cnt));
        for (int64_t i = 0; (OB_SUCCESS == ret) && i < uncompacted_tablets_cnt; ++i) {
          if (can_add_diagnose_info()) {
            if (OB_FAIL(SET_DIAGNOSE_INFO(info_array_[idx_++], MAJOR_MERGE, MTL_ID(),
                  uncompacted_tablets.at(i).get_ls_id(),
                  uncompacted_tablets.at(i).get_tablet_id(),
                  ObCompactionDiagnoseInfo::DIA_STATUS_UNCOMPACTED,
                  ObTimeUtility::fast_current_time(),
                  "server", uncompacted_tablets.at(i).get_server(),
                  "status", uncompacted_tablets.at(i).get_status(),
                  "compaction_scn", uncompacted_tablets.at(i).get_snapshot_version(),
                  "report_scn", uncompacted_tablets.at(i).get_report_scn()))) {
              LOG_WARN("fail to set diagnose info", KR(ret), "uncompacted_tablet",
                       uncompacted_tablets.at(i));
              ret = OB_SUCCESS; // ignore ret, and process next uncompacted_tablet
            }
          } else {
            LOG_INFO("can not add diagnose info", K_(idx), K_(max_cnt), "uncompacted_tablet",
                     uncompacted_tablets.at(i));
          }
        }
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_mini_merge(
    const ObLSID &ls_id,
    ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  ObTabletTableStore &table_store = tablet.get_table_store();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObITable *first_frozen_memtable = nullptr;
  if (OB_FAIL(table_store.get_first_frozen_memtable(first_frozen_memtable))) {
    LOG_WARN("Fail to get sstables", K(ret));
  } else if (nullptr != first_frozen_memtable) { // have frozen memtable
    bool diagnose_flag = false;
    ObSSTable *latest_sstable = nullptr;
    memtable::ObIMemtable *frozen_memtable = static_cast<memtable::ObIMemtable *>(first_frozen_memtable);
    if (OB_ISNULL(latest_sstable =
        static_cast<ObSSTable*>(table_store.get_minor_sstables().get_boundary_table(true/*last*/)))) {
      diagnose_flag = true;
    } else {
      if (latest_sstable->get_end_scn() < frozen_memtable->get_end_scn()
          || tablet.get_snapshot_version() < frozen_memtable->get_snapshot_version()) { // not merge finish
        diagnose_flag = true;
      }
    }
    if (diagnose_flag) {
      ObTabletMiniMergeDag dag;
      if (OB_TMP_FAIL(diagnose_tablet_merge(
              dag,
              MINI_MERGE,
              ls_id,
              tablet_id))) {
        LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), K(tablet), KPC(latest_sstable));
      }
    } else { // mini compaction finish, but memtable have not release
      ObScheduleSuspectInfo ret_info;
      if (OB_SUCC(get_suspect_info(MINI_MERGE, ls_id, tablet_id, ret_info))
          && can_add_diagnose_info()) {
        SET_DIAGNOSE_INFO(
            info_array_[idx_++],
            MINI_MERGE,
            MTL_ID(),
            ls_id,
            tablet_id,
            ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
            ret_info.add_time_,
            "schedule_suspect_info", ret_info.suspect_info_);
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_minor_merge(const ObLSID &ls_id, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int64_t minor_compact_trigger = ObPartitionMergePolicy::DEFAULT_MINOR_COMPACT_TRIGGER;
  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      minor_compact_trigger = tenant_config->minor_compact_trigger;
    }
  }
  if (tablet.get_table_store().get_minor_sstables().count() >= minor_compact_trigger) {
    ObTabletMergeExecuteDag dag;
    if (OB_FAIL(diagnose_tablet_merge(
            dag,
            MINOR_MERGE,
            ls_id,
            tablet.get_tablet_meta().tablet_id_))) {
      LOG_WARN("diagnose failed", K(ret), K(ls_id), "tablet_id", tablet.get_tablet_meta().tablet_id_);
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_medium_merge(
    const ObLSID &ls_id,
    ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  const storage::ObMergeType merge_type = MEDIUM_MERGE;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const int64_t max_serialized_medium_scn = tablet.get_tablet_meta().max_serialized_medium_scn_;
  ObITable *last_major = tablet.get_table_store().get_major_sstables().get_boundary_table(true/*last*/);
  int64_t max_sync_medium_scn = 0;

  if (OB_ISNULL(last_major)) {
  } else if (OB_FAIL(tablet.get_max_sync_medium_scn(max_sync_medium_scn))){
    LOG_WARN("failed to get max sync medium scn", K(ret), K(ls_id), K(tablet_id));
  } else if (max_sync_medium_scn > last_major->get_snapshot_version()) {
    if (tablet.get_snapshot_version() < max_sync_medium_scn) { // wait mini compaction or tablet freeze
      if (ObTimeUtility::fast_current_time() > max_sync_medium_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL * 2
        && can_add_diagnose_info()
        && OB_FAIL(SET_DIAGNOSE_INFO(
                info_array_[idx_++],
                merge_type,
                MTL_ID(),
                ls_id,
                tablet_id,
                ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
                ObTimeUtility::fast_current_time(),
                "max_receive_medium_scn", max_sync_medium_scn,
                "max_serialized_medium_scn", max_serialized_medium_scn,
                "tablet_snapshot", tablet.get_snapshot_version()))) {
        LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id));
      }
    } else {
      ObTabletMajorMergeDag dag;
      if (OB_FAIL(diagnose_tablet_merge(
              dag,
              merge_type,
              ls_id,
              tablet_id,
              max_sync_medium_scn))) {
        LOG_WARN("diagnose failed", K(ret), K(ls_id), K(tablet_id), KPC(last_major));
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_major_merge(
    const int64_t compaction_scn,
    const ObLSID &ls_id,
    ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  const ObTabletTableStore &table_store = tablet.get_table_store();
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const ObMergeType merge_type = MEDIUM_MERGE;
  int64_t max_sync_medium_scn = 0;
  ObSSTable *latest_major_sstable = static_cast<ObSSTable*>(
      table_store.get_major_sstables().get_boundary_table(true/*last*/));
  if (OB_UNLIKELY(compaction_scn <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(compaction_scn));
  } else if (OB_FAIL(tablet.get_max_sync_medium_scn(max_sync_medium_scn))) {
    LOG_WARN("failed to get max sync medium snapshot", K(ret), K(tablet_id));
  } else {
    int tmp_ret = OB_SUCCESS;
    if (nullptr == latest_major_sstable
        || latest_major_sstable->get_snapshot_version() < compaction_scn) {
      if (max_sync_medium_scn < compaction_scn) {
        if (can_add_diagnose_info()
              && ObTimeUtility::fast_current_time() > compaction_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL
              && OB_FAIL(SET_DIAGNOSE_INFO(
                info_array_[idx_++],
                merge_type,
                MTL_ID(),
                ls_id,
                tablet_id,
                ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
                ObTimeUtility::fast_current_time(),
                "max_receive_medium_snapshot", max_sync_medium_scn))) {
          LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id));
        }
      } else {
        ObTabletMajorMergeDag dag;
        if (OB_TMP_FAIL(diagnose_tablet_merge(
                dag,
                merge_type,
                ls_id,
                tablet.get_tablet_meta().tablet_id_,
                compaction_scn))) {
          LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), K(tablet), KPC(latest_major_sstable));
        }
      }

    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_merge(
    ObTabletMergeDag &dag,
    const ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  ObDiagnoseTabletCompProgress progress;
  if (OB_FAIL(diagnose_dag(merge_type, ls_id, tablet_id, compaction_scn, dag, progress))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to diagnose dag", K(ret), K(ls_id), K(tablet_id));
    } else if (OB_FAIL(diagnose_no_dag(dag, merge_type, ls_id, tablet_id, compaction_scn))) {
      LOG_WARN("failed to dagnose no dag", K(ret), K(ls_id), K(tablet_id));
    }
  } else if (progress.is_valid()) { // dag exist, means compaction is running
    // check progress is normal
    if (progress.is_suspect_abormal_) { // progress is abnomal
      if (can_add_diagnose_info()
            && OB_FAIL(SET_DIAGNOSE_INFO(
              info_array_[idx_++],
              merge_type,
              MTL_ID(),
              ls_id,
              tablet_id,
              ObCompactionDiagnoseInfo::DIA_STATUS_RUNNING,
              ObTimeUtility::fast_current_time(),
              "current_status", "dag may hang",
              "merge_progress", progress))) {
        LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), K(progress));
      }
    }
  } else if (OB_FAIL(diagnose_no_dag(dag, merge_type, ls_id, tablet_id, compaction_scn))) {
    LOG_WARN("failed to dagnose no dag", K(ret), K(ls_id), K(tablet_id));
  }
  return ret;
}

int ObCompactionDiagnoseMgr::get_suspect_and_warning_info(
    ObTabletMergeDag &dag,
    const ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    ObScheduleSuspectInfo &info)
{
  int ret = OB_SUCCESS;

  ObDagWarningInfo *warning_info = nullptr;
  bool add_schedule_info = false;
  if (OB_FAIL(ObScheduleSuspectInfoMgr::get_instance().get_suspect_info(
      ObScheduleSuspectInfo::gen_hash(MTL_ID(), dag.hash()), info))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get suspect info", K(ret), K(ls_id), K(tablet_id));
    } else { // no schedule suspect info
      if (OB_FAIL(share::ObDagWarningHistoryManager::get_instance().get(dag.hash(), warning_info))) {
        // check __all_virtual_dag_warning_history
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get dag warning info", K(ret), K(ls_id), K(tablet_id));
        } else { // no execute failure
          ret = OB_SUCCESS;
          LOG_DEBUG("may wait for schedule", K(ret), K(ls_id), K(tablet_id));
        }
      } else if (can_add_diagnose_info()
            && OB_FAIL(SET_DIAGNOSE_INFO(
              info_array_[idx_++],
              merge_type,
              MTL_ID(),
              ls_id,
              tablet_id,
              ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
              warning_info->gmt_create_,
              "error_no", warning_info->dag_ret_,
              "last_error_time", warning_info->gmt_modified_,
              "error_trace", warning_info->task_id_,
              "warning", warning_info->warning_info_))) {
        LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), KPC(warning_info));
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_no_dag(
    ObTabletMergeDag &dag,
    const ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  ObScheduleSuspectInfo info;
  bool add_schedule_info = false;

  if (OB_FAIL(get_suspect_and_warning_info(dag, merge_type, ls_id, tablet_id, info))) {
    LOG_WARN("failed to get suspect and warning info", K(ret), K(ls_id), K(tablet_id));
  } else if (!info.is_valid()) {
    // do nothing
  } else if (MEDIUM_MERGE == merge_type) {
    if (OB_UNLIKELY(compaction_scn <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge version or freeze ts is invalid", K(ret), K(compaction_scn));
    } else {
      LOG_INFO("diagnose major", K(ret), K(ls_id), K(tablet_id), K(merge_type));
      ObDiagnoseTabletCompProgress progress;
      ObTabletMiniMergeDag mini_dag;
      if (OB_FAIL(diagnose_dag(MINI_MERGE, ls_id, tablet_id, ObVersionRange::MIN_VERSION, mini_dag, progress))) {
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to init dag", K(ret), K(ls_id), K(tablet_id));
        } else {
          add_schedule_info = true;
          ret = OB_SUCCESS;
        }
      } else if (progress.base_version_ < compaction_scn && progress.snapshot_version_ >= compaction_scn) {
        // a mini merge for major
        if (can_add_diagnose_info()
            && OB_FAIL(SET_DIAGNOSE_INFO(
                info_array_[idx_++],
                merge_type,
                MTL_ID(),
                ls_id,
                tablet_id,
                ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
                ObTimeUtility::fast_current_time(),
                "current_status", "wait for mini merge",
                "mini_merge_progress", progress))) {
          LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), K(progress));
        }
      } else { // no running mini dag
        add_schedule_info = true;
      }
    }
  } else { // is mini merge
    add_schedule_info = true;
  }

  if (OB_SUCC(ret) && add_schedule_info && can_add_diagnose_info()
      && OB_FAIL(SET_DIAGNOSE_INFO(
          info_array_[idx_++],
          merge_type,
          MTL_ID(),
          ls_id,
          tablet_id,
          ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
          info.add_time_,
          "schedule_suspect_info", info.suspect_info_))) {
    LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), K(info));
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_medium_scn_table(const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  int64_t error_tablet_cnt = 0;
  if (OB_FAIL(ObTabletMetaTableCompactionOperator::diagnose_compaction_scn(MTL_ID(), error_tablet_cnt))) {
    LOG_WARN("failed to diagnose compaction scn", K(ret));
  } else if (0 != error_tablet_cnt
      && can_add_diagnose_info()
      && OB_FAIL(SET_DIAGNOSE_INFO(
          info_array_[idx_++],
          MEDIUM_MERGE,
          MTL_ID(),
          ObLSID(INT64_MAX),
          ObTabletID(INT64_MAX),
          ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
          ObTimeUtility::fast_current_time(),
          "error_tablet_cnt", error_tablet_cnt))) {
    LOG_WARN("failed to add diagnose info", K(ret));
  }
  return ret;
}

/*
 * ObTabletCompactionProgressIterator implement
 * */

int ObCompactionDiagnoseIterator::get_diagnose_info(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  ObCompactionDiagnoseMgr diagnose_mgr;
  void * buf = nullptr;
  if (NULL == (buf = allocator_.alloc(sizeof(ObCompactionDiagnoseInfo) * MAX_DIAGNOSE_INFO_CNT))) {
    ret = common::OB_ALLOCATE_MEMORY_FAILED;
    COMMON_LOG(WARN, "failed to alloc info array", K(ret));
  } else if (FALSE_IT(info_array_ = new (buf) ObCompactionDiagnoseInfo[MAX_DIAGNOSE_INFO_CNT])) {
  } else if (OB_FAIL(diagnose_mgr.init(info_array_, MAX_DIAGNOSE_INFO_CNT))) {
    LOG_WARN("failed to init diagnose info mgr", K(ret));
  } else if (OB_FAIL(diagnose_mgr.diagnose_all_tablets(tenant_id))) {
    LOG_WARN("failed to diagnose major merge", K(ret));
  } else {
    cnt_ = diagnose_mgr.get_cnt();
  }
  return ret;
}

int ObCompactionDiagnoseIterator::open(const int64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("The ObCompactionDiagnoseIterator has been opened", K(ret));
  } else if (!::is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id));
  } else if (OB_FAIL(get_diagnose_info(tenant_id))) {
    LOG_WARN("failed to get diagnose info", K(ret));
  } else {
    cur_idx_ = 0;
    is_opened_ = true;
  }
  return ret;
}

void ObCompactionDiagnoseIterator::reset()
{
  if (OB_NOT_NULL(info_array_)) {
    allocator_.free(info_array_);
    info_array_ = nullptr;
  }
  cnt_ = 0;
  cur_idx_ = 0;
  is_opened_ = false;
}

int ObCompactionDiagnoseIterator::get_next_info(ObCompactionDiagnoseInfo &info)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (cur_idx_ >= cnt_) {
    ret = OB_ITER_END;
  } else if (OB_ISNULL(info_array_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array is null", K(ret));
  } else {
    info = info_array_[cur_idx_++];
  }
  return ret;
}

}//compaction
}//oceanbase
