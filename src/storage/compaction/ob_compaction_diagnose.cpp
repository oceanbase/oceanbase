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
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "rootserver/freeze/ob_major_freeze_service.h"
#include "rootserver/freeze/ob_major_freeze_util.h"
#include "share/ob_tablet_meta_table_compaction_operator.h"
#include "storage/compaction/ob_compaction_util.h"
#include "storage/compaction/ob_medium_compaction_func.h"
#include "storage/tablet/ob_tablet.h"
#include "storage/column_store/ob_column_oriented_sstable.h"
#include "storage/column_store/ob_co_merge_dag.h"

namespace oceanbase
{
using namespace storage;
using namespace share;

namespace compaction
{
/*
 * ObScheduleSuspectInfo implement
 * */
int64_t ObScheduleSuspectInfo::hash() const
{
  int64_t hash_value = ObMergeDagHash::inner_hash();
  hash_value = common::murmurhash(&tenant_id_, sizeof(tenant_id_), hash_value);
  return hash_value;
}

bool ObScheduleSuspectInfo::is_valid() const
{
  bool bret = true;
  if (OB_UNLIKELY(!is_valid_merge_type(merge_type_)
      || !ls_id_.is_valid()
      || !tablet_id_.is_valid())) {
    bret = false;
  }
  return bret;
}

int64_t ObScheduleSuspectInfo::gen_hash(int64_t tenant_id, int64_t dag_hash)
{
  int64_t hash_value = dag_hash;
  hash_value = common::murmurhash(&tenant_id, sizeof(tenant_id), hash_value);
  return hash_value;
}

void ObScheduleSuspectInfo::shallow_copy(ObIDiagnoseInfo *other)
{
  ObScheduleSuspectInfo *info = nullptr;
  if (OB_NOT_NULL(other) && OB_NOT_NULL(info = dynamic_cast<ObScheduleSuspectInfo *>(other))) {
    merge_type_ = info->merge_type_;
    ls_id_ = info->ls_id_;
    tablet_id_ = info->tablet_id_;
    tenant_id_ = info->tenant_id_;
    priority_ = info->priority_;
    add_time_ = info->add_time_;
    hash_ = info->hash_;
  }
}

int64_t ObScheduleSuspectInfo::get_add_time() const
{
  return add_time_;
}

int64_t ObScheduleSuspectInfo::get_hash() const
{
  return hash_;
}

/*
 * ObIDiagnoseInfoIter implement
 * */
int ObIDiagnoseInfoMgr::Iterator::open(const uint64_t version, ObIDiagnoseInfo *current_info, ObIDiagnoseInfoMgr *info_pool)
{
  int ret = OB_SUCCESS;
  if (is_opened_) {
    ret = OB_OPEN_TWICE;
    STORAGE_LOG(WARN, "iterator is opened", K(ret));
  } else if (OB_ISNULL(current_info) || OB_ISNULL(info_pool)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(current_info), KP(info_pool));
  } else {
    version_ = version;
    current_info_ = current_info;
    info_pool_ = info_pool;
    seq_num_ = 1; // header
    is_opened_ = true;
  }
  return ret;
}

int ObIDiagnoseInfoMgr::Iterator::get_next(ObIDiagnoseInfo *out_info, char *buf, const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (!is_opened_) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIDiagnoseInfoIter is not init", K(ret));
  } else if (OB_ISNULL(out_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(out_info));
  } else {
    common::SpinRLockGuard RLockGuard(info_pool_->rwlock_);
    while (OB_SUCC(next())) {
      // (current_info_->seq_num_ <= seq_num_) means info has been visited
      if (current_info_->seq_num_ > seq_num_ && !current_info_->is_deleted()) {
        seq_num_ = current_info_->seq_num_;
        out_info->shallow_copy(current_info_);
        if (OB_ISNULL(buf)) {
          // do nothing // allow
        } else if (OB_NOT_NULL(current_info_->info_param_)) {
          if (OB_FAIL(current_info_->info_param_->fill_comment(buf, buf_len))) {
            STORAGE_LOG(WARN, "failed to fill comment from info param", K(ret));
          }
        }
        break;
      }
    }
  }
  return ret;
}

int ObIDiagnoseInfoMgr::Iterator::next()
{
  int ret = OB_SUCCESS;
  if (version_ < info_pool_->version_) {
    // version changed, which means some infos have been purged, the current_info_ maybe invalid ptr
    version_ = info_pool_->version_;
    current_info_ = info_pool_->info_list_.get_header();
  } else if (version_ > info_pool_->version_) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "Unexpected version value", K(ret), "iter_version", version_,
        "pool_version", info_pool_->version_);
  }
  if (OB_SUCC(ret)) {
    if (OB_ISNULL(current_info_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "Unexpect value", K(ret), K(current_info_));
    } else if (0 == seq_num_) {
      // guarantee idempotency
      ret = OB_ITER_END;
    } else if (OB_ISNULL(current_info_ = current_info_->get_next())) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "failed to next", K(ret), K(current_info_));
    } else if (current_info_ == info_pool_->info_list_.get_header()) {
      // to ignore the version_ changing
      ret = OB_ITER_END;
      seq_num_ = 0; // tail
    }
  }
  return ret;
}
/*
 * ObIDiagnoseInfoMgr implement
 * */
void ObIDiagnoseInfoMgr::add_compaction_info_param(char *buf, const int64_t buf_size, const char* str)
{
  int64_t pos = strlen(buf);
  if (0 > pos || buf_size <= pos) {
  } else {
    int len = snprintf(buf + pos, buf_size - pos, "%s", str);
    if (OB_UNLIKELY(len < 0)) {
    } else if (OB_LIKELY(len < buf_size - pos)) {
      pos += len;
    } else {
      pos = buf_size - 1;  //skip '\0'
    }
    buf[pos] = '\0';
  }
}

int ObIDiagnoseInfoMgr::init(bool with_map,
           const uint64_t tenant_id,
           const char* basic_label,
           const int64_t page_size,
           int64_t max_size)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObIDiagnoseInfoMgr has already been initiated", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || OB_ISNULL(basic_label)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(tenant_id), K(basic_label));
  } else {
    (void)snprintf(pool_label_, sizeof(pool_label_), "%s%s", basic_label, "Mgr");
    page_size_ = std::max(page_size, static_cast<int64_t>(INFO_PAGE_SIZE_LIMIT));
    max_size = upper_align(max_size, page_size_);
    if (OB_FAIL(allocator_.init(ObMallocAllocator::get_instance(),
                                    page_size,
                                    lib::ObMemAttr(tenant_id, pool_label_),
                                    0,
                                    max_size,
                                    max_size))) {
      STORAGE_LOG(WARN, "failed to init allocator", K(ret));
    } else if (with_map) {
      (void)snprintf(bucket_label_, sizeof(bucket_label_), "%s%s", basic_label, "Bkt");
      (void)snprintf(node_label_, sizeof(node_label_), "%s%s", basic_label, "Node");
      if (OB_FAIL(info_map_.create(INFO_BUCKET_LIMIT, bucket_label_, node_label_, tenant_id))) {
        STORAGE_LOG(WARN, "failed to create dap map", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    version_ = 1;
    seq_num_ = 1;
    is_inited_ = true;
  } else {
    reset();
  }
  return ret;
}

void ObIDiagnoseInfoMgr::destroy()
{
  if (IS_INIT) {
    reset();
  }
}
void ObIDiagnoseInfoMgr::reset()
{
  common::SpinWLockGuard guard(lock_);
  common::SpinWLockGuard WLockGuard(rwlock_);
  clear_with_no_lock();
  if (info_map_.created()) {
    info_map_.destroy();
  }
  allocator_.reset();
  is_inited_ = false;
}

void ObIDiagnoseInfoMgr::clear()
{
  if (IS_INIT) {
    common::SpinWLockGuard guard(lock_);
    common::SpinWLockGuard WLockGuard(rwlock_);
    clear_with_no_lock();
  }
}

void ObIDiagnoseInfoMgr::clear_with_no_lock()
{
  if (info_map_.created()) {
    info_map_.clear();
  }
  DLIST_FOREACH_REMOVESAFE_NORET(iter, info_list_) {
    info_list_.remove(iter);
    if (allocator_.is_inited()) {
      iter->destroy(allocator_);
    }
  }
  info_list_.clear();
  version_ = 1;
  seq_num_ = 1;
}

int ObIDiagnoseInfoMgr::size()
{
  common::SpinRLockGuard guard(lock_);
  return info_list_.get_size();
}

int ObIDiagnoseInfoMgr::get_with_param(const int64_t key, ObIDiagnoseInfo *out_info, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIDiagnoseInfoMgr is not init", K(ret));
  } else if (OB_ISNULL(out_info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(out_info));
  } else {
    common::SpinWLockGuard guard(lock_);
    ObIDiagnoseInfo *info = NULL;
    if (OB_FAIL(get_with_no_lock(key, info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to get info from map", K(ret), K(key));
      }
    } else if (OB_ISNULL(info->info_param_)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "info_param is null", K(ret), K(info));
    } else {
      out_info->shallow_copy(info);
      if (OB_FAIL(info->info_param_->deep_copy(allocator, out_info->info_param_))) {
        STORAGE_LOG(WARN, "failed to deep copy info param", K(ret));
      }
    }
  }
  return ret;
}

int ObIDiagnoseInfoMgr::delete_info(const int64_t key)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIDiagnoseInfoMgr is not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if (OB_FAIL(del_with_no_lock(key, nullptr))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to delete info", K(ret));
      }
    }
  }
  return ret;
}

int ObIDiagnoseInfoMgr::set_max(const int64_t size)
{
  int ret = OB_SUCCESS;
  int64_t max_size = upper_align(size, page_size_);
  common::SpinWLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIDiagnoseInfoMgr is not init", K(ret));
  } else if (OB_FAIL(allocator_.set_max(max_size, true))) {
    STORAGE_LOG(WARN, "failed to set max", K(ret), "new max_size", max_size,
        "old max_size", allocator_.get_max());
  } else if (allocator_.total() <= allocator_.get_max()) {
  } else if (OB_FAIL(purge_with_rw_lock())) {
    STORAGE_LOG(WARN, "failed to purge info when resize", K(ret));
  }
  return ret;
}

int ObIDiagnoseInfoMgr::gc_info()
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIDiagnoseInfoMgr is not init", K(ret));
  } else {
    common::SpinWLockGuard guard(lock_);
    if ((allocator_.used() * 1.0) / allocator_.get_max() >= (GC_HIGH_PERCENTAGE * 1.0 / 100)) {
      if (OB_FAIL(purge_with_rw_lock())) {
        STORAGE_LOG(WARN, "failed to purge cuz gc_info", K(ret));
      }
    }
  }
  return ret;
}

int ObIDiagnoseInfoMgr::open_iter(Iterator &iter)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObIDiagnoseInfoMgr is not init", K(ret));
  } else {
    common::SpinRLockGuard guard(rwlock_);
    if (OB_FAIL(iter.open(version_, info_list_.get_header(), this))) {
      STORAGE_LOG(WARN, "failed to open iter", K(ret));
    }
  }
  return ret;
}

int ObIDiagnoseInfoMgr::add_with_no_lock(const int64_t key, ObIDiagnoseInfo *info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret));
  } else if (!info_list_.add_last(info)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "failed to add into info list", K(ret));
  } else if (info_map_.created()) {
    if (OB_FAIL(info_map_.set_refactored(key, info))) {
      STORAGE_LOG(WARN, "failed to set info into map", K(ret), K(key));
      if (OB_ISNULL(info_list_.remove(info))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "failed to remove info from list", K(ret));
        // unexpected
        ob_abort();
      }
    }
  }

  if (OB_SUCC(ret)) {
    info->seq_num_ = ++seq_num_;
  } else if (OB_NOT_NULL(info)) {
    info->destroy(allocator_);
    info = nullptr;
  }
  return ret;
}

int ObIDiagnoseInfoMgr::del_with_no_lock(const int64_t key, ObIDiagnoseInfo *info)
{
  int ret = OB_SUCCESS;
  if (info_map_.created()) {
    ObIDiagnoseInfo *old_info = nullptr;
    if (OB_FAIL(info_map_.get_refactored(key, old_info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to get info from map", K(ret), K(key), K(old_info));
      }
    } else if (nullptr != info && info->priority_ < old_info->priority_) {
      ret = OB_HASH_EXIST;
      STORAGE_LOG(INFO, "failed to del old info cause priority", K(ret),
          "old_priority", old_info->priority_, "new_priority", info->priority_);
    } else if (OB_FAIL(info_map_.erase_refactored(key))) {
      STORAGE_LOG(WARN, "failed to erase info from map", K(ret), K(key));
    }
    if (OB_SUCC(ret) && OB_NOT_NULL(old_info)) {
      old_info->set_deleted();
      if (OB_NOT_NULL(info)) {
        info->update(old_info);
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "info map is not created", K(ret));
  }
  return ret;
}

int ObIDiagnoseInfoMgr::get_with_no_lock(const int64_t key, ObIDiagnoseInfo *&info)
{
  int ret = OB_SUCCESS;
  info = NULL;
  if (info_map_.created()) {
    if (OB_FAIL(info_map_.get_refactored(key, info))) {
      if (OB_HASH_NOT_EXIST != ret) {
        STORAGE_LOG(WARN, "failed to get info from map", K(ret), K(key));
      }
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "info map is not created", K(ret));
  }
  return ret;
}

int ObIDiagnoseInfoMgr::purge_with_rw_lock(bool batch_purge)
{
  int ret = OB_SUCCESS;
  int64_t purge_count = 0;
  common::SpinWLockGuard WLockGuard(rwlock_);
  int batch_size = info_list_.get_size() / MAX_ALLOC_RETRY_TIMES;
  batch_size = std::max(batch_size, 10);
  DLIST_FOREACH_REMOVESAFE(iter, info_list_) {
    if (info_map_.created() && !iter->is_deleted()) {
      if (OB_FAIL(info_map_.erase_refactored(iter->get_hash()))) {
        STORAGE_LOG(WARN, "failed to erase from map", K(ret), "hash_key", iter->get_hash(),
            "is_deleted", iter->is_deleted(), "seq_num", iter->seq_num_);
      }
    }
    if (OB_SUCC(ret)) {
      if (OB_ISNULL(info_list_.remove(iter))) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(ERROR, "failed to remove info from list", K(ret));
        // unexpected
        ob_abort();
      }
      iter->destroy(allocator_);
      iter = nullptr;
      ++purge_count;
    }

    if (batch_purge && purge_count == batch_size) {
      break;
    } else if (!batch_purge && allocator_.total() <= allocator_.get_max() &&
        ((allocator_.used() * 1.0) / allocator_.get_max()) <= (GC_LOW_PERCENTAGE * 1.0 / 100)) {
      break;
    }
  }

  if (OB_SUCC(ret)) {
    STORAGE_LOG(INFO, "success to purge", K(ret), K(batch_purge), K(batch_size), "max_size", allocator_.get_max(),
      "used_size", allocator_.used(), "total_size", allocator_.total(), K(purge_count));
  }
  ++version_;
  return ret;
}
/*
 * ObScheduleSuspectInfoMgr implement
 * */
int ObScheduleSuspectInfoMgr::mtl_init(ObScheduleSuspectInfoMgr *&schedule_suspect_info)
{
  int64_t max_size = cal_max();
  return schedule_suspect_info->init(true, MTL_ID(), "SuspectInfo", INFO_PAGE_SIZE, max_size);
}

int64_t ObScheduleSuspectInfoMgr::cal_max()
{
  const uint64_t tenant_id = MTL_ID();
  int64_t max_size = std::min(static_cast<int64_t>(lib::get_tenant_memory_limit(tenant_id) * MEMORY_PERCENTAGE / 100),
                          static_cast<int64_t>(POOL_MAX_SIZE));
  return max_size;
}

int ObScheduleSuspectInfoMgr::add_suspect_info(const int64_t key, ObScheduleSuspectInfo &input_info)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    STORAGE_LOG(WARN, "ObScheduleSuspectInfoMgr is not init", K(ret));
  } else if (OB_ISNULL(input_info.info_param_)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument. info param is null", K(ret));
  } else if (OB_FAIL((alloc_and_add(key, &input_info)))) {
    STORAGE_LOG(WARN, "failed to alloc and add suspect info", K(ret));
  }
  return ret;
}
/*
 * ObCompactionDiagnose implement
 * */

#define ADD_DIAGNOSE_INFO(merge_type, ls_id, tablet_id, status, time, ...) \
SET_DIAGNOSE_INFO(info_array_[idx_++], merge_type, MTL_ID(), ls_id, tablet_id, status, time, __VA_ARGS__)
#define ADD_DIAGNOSE_INFO_FOR_TABLET(merge_type, status, time, ...) \
ADD_DIAGNOSE_INFO(merge_type, ls_id, tablet_id, status, time, __VA_ARGS__)
#define ADD_COMMON_DIAGNOSE_INFO(merge_type, status, time, ...) \
ADD_DIAGNOSE_INFO(merge_type, UNKNOW_LS_ID, UNKNOW_TABLET_ID, status, time, __VA_ARGS__)

#define ADD_MAJOR_WAIT_SCHEDULE(time, info) \
  if (ObTimeUtility::current_time_ns() > time) { \
    if (DIAGNOSE_TABELT_MAX_COUNT > diagnose_tablet_count_[COMPACTION_DIAGNOSE_MAJOR_NOT_SCHEDULE] \
      && can_add_diagnose_info() \
      && OB_TMP_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET( \
          MAJOR_MERGE, \
          gen_diagnose_status(compaction_scn), \
          ObTimeUtility::fast_current_time(), \
          "major not schedule for long time", info, \
          "max_receive_medium_snapshot", max_sync_medium_scn, \
          "compaction_scn", compaction_scn, \
          "tablet_snapshot", tablet.get_snapshot_version(), \
          "last_major_scn", last_major_snapshot_version))) { \
      LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id)); \
    } \
    ++diagnose_tablet_count_[COMPACTION_DIAGNOSE_MAJOR_NOT_SCHEDULE]; \
  }

#define ADD_MEDIUM_WAIT_SCHEDULE(time, info) \
  if (ObTimeUtility::current_time_ns() > time) { \
    if (DIAGNOSE_TABELT_MAX_COUNT > diagnose_tablet_count_[COMPACTION_DIAGNOSE_MEDIUM_NOT_SCHEDULE] \
      && can_add_diagnose_info() \
      && OB_TMP_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET( \
          MEDIUM_MERGE, \
          gen_diagnose_status(max_sync_medium_scn), \
          ObTimeUtility::fast_current_time(), \
          "medium not schedule for long time", info,\
          "max_receive_medium_scn", max_sync_medium_scn, \
          "tablet_snapshot", tablet.get_snapshot_version(), \
          "last_major_scn", last_major_snapshot_version))) { \
      LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id)); \
    } \
    ++diagnose_tablet_count_[COMPACTION_DIAGNOSE_MEDIUM_NOT_SCHEDULE]; \
  }

const char *ObCompactionDiagnoseInfo::ObDiagnoseStatusStr[DIA_STATUS_MAX] = {
    "NOT_SCHEDULE",
    "RUNNING",
    "WARN",
    "FAILED",
    "RS_UNCOMPACTED",
    "SPECIAL"
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

const char *ObCompactionDiagnoseMgr::ObCompactionDiagnoseTypeStr[COMPACTION_DIAGNOSE_TYPE_MAX] = {
    "MEDIUM_NOT_SCHEDULE",
    "MAJOR_NOT_SCHEDULE"
};

const char * ObCompactionDiagnoseMgr::get_compaction_diagnose_type_str(ObCompactionDiagnoseType type)
{
  STATIC_ASSERT(COMPACTION_DIAGNOSE_TYPE_MAX == ARRAYSIZEOF(ObCompactionDiagnoseTypeStr), "diagnose type str len is mismatch");
  const char *str = "";
  if (type >= COMPACTION_DIAGNOSE_TYPE_MAX || type < COMPACTION_DIAGNOSE_MEDIUM_NOT_SCHEDULE) {
    str = "invalid_status";
  } else {
    str = ObCompactionDiagnoseTypeStr[type];
  }
  return str;
}

ObMergeType ObCompactionDiagnoseMgr::get_compaction_diagnose_merge_type(ObCompactionDiagnoseType type)
{
  ObMergeType merge_type = INVALID_MERGE_TYPE;
  if (COMPACTION_DIAGNOSE_MEDIUM_NOT_SCHEDULE == type) {
    merge_type = MEDIUM_MERGE;
  } else if (COMPACTION_DIAGNOSE_MAJOR_NOT_SCHEDULE == type) {
    merge_type = MAJOR_MERGE;
  }
  return merge_type;
}

ObCompactionDiagnoseMgr::ObCompactionDiagnoseMgr()
 : is_inited_(false),
   normal_(true),
   info_array_(nullptr),
   max_cnt_(0),
   idx_(0)
  {
    MEMSET(suspect_tablet_count_, 0, sizeof(suspect_tablet_count_));
    MEMSET(suspect_merge_type_, -1, sizeof(suspect_merge_type_));
    MEMSET(diagnose_tablet_count_, 0, sizeof(diagnose_tablet_count_));
  }

void ObCompactionDiagnoseMgr::reset()
{
  info_array_ = nullptr;
  max_cnt_ = 0;
  idx_ = 0;
  is_inited_ = false;
  normal_ = true;
  MEMSET(suspect_tablet_count_, 0, sizeof(suspect_tablet_count_));
  MEMSET(suspect_merge_type_, -1, sizeof(suspect_merge_type_));
  MEMSET(diagnose_tablet_count_, 0, sizeof(diagnose_tablet_count_));
}

int ObCompactionDiagnoseMgr::init(
    common::ObIAllocator *allocator,
    ObCompactionDiagnoseInfo *info_array,
    const int64_t max_cnt)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObCompactionDiagnoseMgr has already been initiated", K(ret));
  } else if (OB_UNLIKELY(nullptr == info_array || max_cnt <= 0 || nullptr == allocator)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), K(info_array), K(max_cnt));
  } else {
    info_array_ = info_array;
    max_cnt_ = max_cnt;
    is_inited_ = true;
  }
  if (!is_inited_) {
    reset();
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_dag(
    compaction::ObMergeType merge_type,
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
  param.skip_get_tablet_ = true;
  param.is_reserve_mode_ = false;

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
  if (OB_HASH_NOT_EXIST == ret) {
    LOG_TRACE("dag not exist", K(ret), K(dag));
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
        (void)diagnose_tenant_tablet(); // storage side
        (void)diagnose_tenant_major_merge(); // RS side
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

int ObCompactionDiagnoseMgr::get_and_set_suspect_info(
    const ObMergeType merge_type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObScheduleSuspectInfo ret_info;
  char tmp_str[common::OB_DIAGNOSE_INFO_LENGTH] = "\0";
  share::ObSuspectInfoType suspect_info_type;
  if (OB_SUCC(get_suspect_info(merge_type, ls_id, tablet_id, ret_info, suspect_info_type, tmp_str, sizeof(tmp_str)))
      && can_add_diagnose_info()
      && OB_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET(
                merge_type,
                ObCompactionDiagnoseInfo::DIA_STATUS_FAILED, // TODO(@jingshui): use status by priority
                ret_info.add_time_,
                "schedule_suspect_info", tmp_str))) {
    LOG_WARN("failed to add dignose info", K(ret), K(tmp_str));
  } else if (OB_HASH_NOT_EXIST != ret) {
    LOG_WARN("failed get suspect info", K(ret), K(ls_id));
  }
  return ret;
}

int ObCompactionDiagnoseMgr::get_suspect_info(
    const ObMergeType merge_type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    ObScheduleSuspectInfo &ret_info,
    share::ObSuspectInfoType &suspect_info_type,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  suspect_info_type = share::ObSuspectInfoType::SUSPECT_INFO_TYPE_MAX;
  ObScheduleSuspectInfo input_info;
  input_info.tenant_id_ = MTL_ID();
  input_info.merge_type_ = merge_type;
  input_info.ls_id_ = ls_id;
  input_info.tablet_id_ = tablet_id;
  ObInfoParamBuffer allocator; // info_param_ will be invalid after return
  if (OB_FAIL(MTL(ObScheduleSuspectInfoMgr *)->get_with_param(input_info.hash(), &ret_info, allocator))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get suspect info", K(ret), K(input_info));
    }
  } else if (OB_FAIL(ret_info.info_param_->fill_comment(buf, buf_len))) {
    STORAGE_LOG(WARN, "failed to fill comment from info param", K(ret));
  } else {
    suspect_info_type = ret_info.info_param_->type_.suspect_type_;
    ret_info.info_param_ = nullptr;
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tenant(
    bool &diagnose_major_flag,
    ObTenantTabletScheduler *scheduler,
    int64_t &compaction_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool info_exist = false;
  ObScheduleSuspectInfo ret_info;
  char tmp_str[common::OB_DIAGNOSE_INFO_LENGTH] = "\0";
  share::ObSuspectInfoType suspect_info_type;
  if (OB_SUCCESS == get_suspect_info(MEDIUM_MERGE, UNKNOW_LS_ID, UNKNOW_TABLET_ID, ret_info, suspect_info_type, tmp_str, sizeof(tmp_str))) {
    info_exist = true;
  }
  share::ObFreezeInfo freeze_info;
  if (compaction_scn > scheduler->get_inner_table_merged_scn()) { // check major merge
    if (info_exist && can_add_diagnose_info()) {
      if (OB_TMP_FAIL(ADD_COMMON_DIAGNOSE_INFO(
          MEDIUM_MERGE,
          ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
          ret_info.add_time_,
          "schedule_suspect_info", tmp_str))) {
        LOG_WARN("failed to add dignose info in tenant level", K(tmp_ret), K(tmp_str));
      }
    }
    if ((!scheduler->could_major_merge_start()
      || scheduler->get_prohibit_medium_ls_map().get_transfer_flag_cnt() > 0)
            && can_add_diagnose_info()) {
        ADD_COMMON_DIAGNOSE_INFO(
            !scheduler->could_major_merge_start() ? MAJOR_MERGE : MEDIUM_MERGE,
            ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
            ObTimeUtility::fast_current_time(),
            "info", "major or medium may be paused",
            "could_major_merge", scheduler->could_major_merge_start(),
            "prohibit_medium_ls_info", scheduler->get_prohibit_medium_ls_map());
    }
    diagnose_major_flag = true;
    const int64_t merged_version = scheduler->get_inner_table_merged_scn();
    if (merged_version == ObTenantTabletScheduler::INIT_COMPACTION_SCN) {
      // do nothing
    } else if (OB_FAIL(MTL(ObTenantFreezeInfoMgr *)->get_freeze_info_behind_snapshot_version(merged_version, freeze_info))) {
      LOG_WARN("failed to get freeze info behind snapshot version", K(ret), K(merged_version));
      if (can_add_diagnose_info()
          && OB_TMP_FAIL(ADD_COMMON_DIAGNOSE_INFO(
                    MEDIUM_MERGE,
                    ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                    ObTimeUtility::fast_current_time(),
                    "error_code", ret,
                    "freeze_info is invalid, merged_version", merged_version))) {
        LOG_WARN("failed to add dignose info about freeze_info", K(ret), K(merged_version));
      }
    } else {
      compaction_scn = freeze_info.frozen_scn_.get_val_for_tx();
    }
  }
  (void)diagnose_medium_scn_table();
  return ret;
}

void ObCompactionDiagnoseMgr::diagnose_tenant_ls(
    const bool diagnose_major_flag,
    const bool weak_read_ts_ready,
    const int64_t compaction_scn,
    const ObLSID &ls_id)
{
  int tmp_ret = OB_SUCCESS;
   bool is_leader = false;
   if (OB_TMP_FAIL(ObMediumCompactionScheduleFunc::is_election_leader(ls_id, is_leader))) {
     if (OB_LS_NOT_EXIST != tmp_ret) {
       LOG_WARN_RET(tmp_ret, "failed to get palf handle role", K(ls_id));
     }
   }
  // check weak read ts // ls level
  if (diagnose_major_flag
      && !weak_read_ts_ready
      && can_add_diagnose_info()
      && OB_TMP_FAIL(ADD_DIAGNOSE_INFO(
                MEDIUM_MERGE,
                ls_id,
                UNKNOW_TABLET_ID,
                ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                ObTimeUtility::fast_current_time(),
                "weak read ts is not ready, compaction_scn",
                compaction_scn))) {
    LOG_WARN_RET(tmp_ret, "failed to add dignose info about weak read ts", K(tmp_ret), K(compaction_scn));
  }
  // check ls suspect info for memtable freezing // ls level
  (void) get_and_set_suspect_info(MINI_MERGE, ls_id, UNKNOW_TABLET_ID);

  // check ls suspect info for ls locality change
  if (is_leader && MTL(ObTenantMediumChecker*)->locality_cache_empty()) {
    if (can_add_diagnose_info()
        && OB_TMP_FAIL(ADD_DIAGNOSE_INFO(
            MEDIUM_MERGE,
            ls_id,
            UNKNOW_TABLET_ID,
            ObCompactionDiagnoseInfo::DIA_STATUS_WARN,
            ObTimeUtility::fast_current_time(),
            "maybe bad case",
            "ls leader is not in ls locality"))) {
      LOG_WARN_RET(tmp_ret, "failed to add dignose info about ls leader not in locality cache", K(tmp_ret));
    }
  }
}

void ObCompactionDiagnoseMgr::diagnose_failed_report_task(
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t compaction_scn)
{
  int tmp_ret = OB_SUCCESS;
  bool exist = false;
  bool processing = false;
  ObScheduleSuspectInfo ret_info;
  char tmp_str[common::OB_DIAGNOSE_INFO_LENGTH] = "\0";
  share::ObSuspectInfoType suspect_info_type;
  if (OB_TMP_FAIL(get_suspect_info(MEDIUM_MERGE, ls_id, tablet_id, ret_info, suspect_info_type, tmp_str, sizeof(tmp_str)))) {
    LOG_WARN_RET(tmp_ret, "failed to get suspect info", K(tmp_ret), K(ls_id), K(tablet_id));
  } else if (is_compaction_report_info(suspect_info_type)) {
    if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->check_exist(ls_id, tablet_id, exist))) {
      LOG_WARN_RET(tmp_ret, "failed to check task exist", K(tmp_ret), K(ls_id), K(tablet_id));
    } else if (!exist && OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->check_processing_exist(ls_id, tablet_id, processing))) {
      LOG_WARN_RET(tmp_ret, "failed to check processing task exist", K(tmp_ret), K(ls_id), K(tablet_id));
    }
  }
  if (can_add_diagnose_info()) {
    if ((ObSuspectInfoType::SUSPECT_COMPACTION_REPORT_ADD_FAILED == suspect_info_type && !exist && !processing)
        || (ObSuspectInfoType::SUSPECT_COMPACTION_REPORT_PROGRESS_FAILED == suspect_info_type && (exist || processing))) {
      if (OB_TMP_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET(
                    MEDIUM_MERGE,
                    ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                    ret_info.add_time_,
                    "compaction_scn", compaction_scn,
                    "schedule_suspect_info", tmp_str,
                    "is_waiting", exist,
                    "is_processing", processing))) {
        LOG_WARN_RET(tmp_ret, "failed to add dignose info", K(tmp_ret), K(tmp_str));
      }
    }
  }
}

void ObCompactionDiagnoseMgr::diagnose_existing_report_task()
{
  int tmp_ret = OB_SUCCESS;
  ObSEArray<observer::ObTabletTableUpdateTask, MAX_REPORT_TASK_DIAGNOSE_CNT> waiting_tasks;
  ObSEArray<observer::ObTabletTableUpdateTask, MAX_REPORT_TASK_DIAGNOSE_CNT> processing_tasks;
  if (OB_TMP_FAIL(MTL(observer::ObTabletTableUpdater*)->diagnose_existing_task(waiting_tasks, processing_tasks))) {
    LOG_WARN_RET(tmp_ret, "fail to diagnose existing task", K(tmp_ret));
  } else {
    FOREACH(iter, waiting_tasks) {
      if (can_add_diagnose_info()
          && OB_TMP_FAIL(ADD_DIAGNOSE_INFO(
                        MEDIUM_MERGE,
                        iter->get_ls_id(),
                        iter->get_tablet_id(),
                        ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                        ObTimeUtility::fast_current_time(),
                        "report task waiting for a long time: add_time", iter->get_add_timestamp()))) {
        LOG_WARN_RET(tmp_ret, "failed to add dignose info", K(tmp_ret), K(*iter));
      }
    }
    FOREACH(iter, processing_tasks) {
      if (can_add_diagnose_info()
          && OB_TMP_FAIL(ADD_DIAGNOSE_INFO(
                        MEDIUM_MERGE,
                        iter->get_ls_id(),
                        iter->get_tablet_id(),
                        ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                        ObTimeUtility::fast_current_time(),
                        "report task processing for a long time: add_time", iter->get_add_timestamp(),
                        "start_time", iter->get_start_timestamp()))) {
        LOG_WARN_RET(tmp_ret, "failed to add dignose info", K(tmp_ret), K(*iter));
      }
    }
  }
}

void ObCompactionDiagnoseMgr::diagnose_count_info()
{
  int tmp_ret = OB_SUCCESS;
  for (int64_t i = 0; i < share::ObSuspectInfoType::SUSPECT_INFO_TYPE_MAX; ++i) {
    if (suspect_tablet_count_[i] > DIAGNOSE_TABELT_MAX_COUNT && can_add_diagnose_info()) {
      if (OB_TMP_FAIL(ADD_COMMON_DIAGNOSE_INFO(
            suspect_merge_type_[i],
            ObCompactionDiagnoseInfo::DIA_STATUS_SPECIAL,
            ObTimeUtility::fast_current_time(),
            "schedule_suspect_info type", OB_SUSPECT_INFO_TYPES[i].info_str,
            "count of tablets with the same problem", suspect_tablet_count_[i]))) {
        LOG_WARN_RET(tmp_ret, "failed to add diagnose info", K(tmp_ret));
      }
    }
  }
  for (int64_t i = 0; i < COMPACTION_DIAGNOSE_TYPE_MAX; ++i) {
    if (diagnose_tablet_count_[i] > DIAGNOSE_TABELT_MAX_COUNT && can_add_diagnose_info()) {
      if (OB_TMP_FAIL(ADD_COMMON_DIAGNOSE_INFO(
            get_compaction_diagnose_merge_type(ObCompactionDiagnoseType(i)),
            ObCompactionDiagnoseInfo::DIA_STATUS_SPECIAL,
            ObTimeUtility::fast_current_time(),
            "diagnose info type", get_compaction_diagnose_type_str(ObCompactionDiagnoseType(i)),
            "count of tablets with the same problem", diagnose_tablet_count_[i]))) {
        LOG_WARN_RET(tmp_ret, "failed to add diagnose info", K(tmp_ret));
      }
    }
  }
}

int ObCompactionDiagnoseMgr::check_ls_status(
    const ObLSID &ls_id,
    const int64_t compaction_scn,
    const bool diagnose_major_flag,
    common::hash::ObHashMap<ObLSID, ObLSCheckStatus> &ls_map,
    ObLS *&ls,
    bool &need_merge,
    bool &weak_read_ts_ready)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  bool ls_hash_exist = false;
  ObLSCheckStatus ls_check_status;
  ObLSHandle ls_handle;
  if (ls_map.created() && OB_SUCCESS == (ls_map.get_refactored(ls_id, ls_check_status))) {
    ls_hash_exist = true;
    need_merge = ls_check_status.need_merge_;
    weak_read_ts_ready = ls_check_status.weak_read_ts_ready_;
  }
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::COMPACT_MODE))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K(ls_id));
  } else if (!ls_hash_exist) {
    if (OB_TMP_FAIL(ObTabletMergeChecker::check_ls_state(*ls, need_merge))) {
      LOG_WARN("failed to check ls state", K(tmp_ret), KPC(ls), K(need_merge));
    } else if (need_merge) {
      weak_read_ts_ready = ObTenantTabletScheduler::check_weak_read_ts_ready(compaction_scn, *ls);
      // diagnose ls only once when ls is not in ls map
      (void)diagnose_tenant_ls(diagnose_major_flag, weak_read_ts_ready, compaction_scn, ls_id);
    }
    if (ls_map.created() && OB_TMP_FAIL(ls_map.set_refactored(ls_id, ObLSCheckStatus(weak_read_ts_ready, need_merge)))) {
      LOG_WARN("failed to set ls map", K(tmp_ret), K(ls_id), K(weak_read_ts_ready), K(need_merge));
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tenant_tablet()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LSStatusMap checked_ls; // ls which has been checked, skip ls level diagnose
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCompactionDiagnoseMgr is not init", K(ret));
  } else if (OB_FAIL(checked_ls.create(OB_MAX_LS_NUM_PER_TENANT_PER_SERVER, ObMemAttr(MTL_ID(), "CompDiaLS")))) {
    LOG_WARN("failed to create checked ls map", K(ret));
  } else {
    // diagnose dags in dag scheduler
    if (OB_TMP_FAIL(MTL(ObTenantDagScheduler*)->diagnose_all_dags())) {
      LOG_WARN("failed to diagnose running task", K(tmp_ret));
    }
    bool diagnose_major_flag = false;
    ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler*);
    int64_t compaction_scn = MAX(scheduler->get_frozen_version(), MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version());

    // check tenant diagnose info // tenant level
    if (OB_TMP_FAIL(diagnose_tenant(diagnose_major_flag, scheduler, compaction_scn))) {
      LOG_WARN("failed to diagnose tenant", K(tmp_ret), K(MTL_ID()));
    }

    // loop all diagnose tablets
    DiagnoseTabletArray diagnose_tablets;
    DiagnoseTabletArray tablet_array;
    if (OB_TMP_FAIL(MTL(ObDiagnoseTabletMgr*)->get_diagnose_tablets(diagnose_tablets))) {
      LOG_WARN("failed to get all diagnose tablets", K(tmp_ret));
    }

    ObLS *ls = nullptr;
    ObTabletHandle tablet_handle;
    ARRAY_FOREACH_NORET(diagnose_tablets, idx) {
      normal_ = true;
      bool need_merge = false;
      bool weak_read_ts_ready = false;
      bool ls_hash_exist = false;
      const ObDiagnoseTablet &diagnose_tablet = diagnose_tablets.at(idx);
      const ObLSID &ls_id = diagnose_tablet.ls_id_;
      const ObTabletID &tablet_id = diagnose_tablet.tablet_id_;
      ObLSCheckStatus ls_check_status;
      if (IS_UNKNOW_LS_ID(ls_id)) {
        // skip
        continue;
      } else if (OB_FAIL(check_ls_status(ls_id, compaction_scn, diagnose_major_flag,
          checked_ls, ls, need_merge, weak_read_ts_ready))) {
        LOG_WARN("failed to check ls status", K(ret), K(ls_id), K(compaction_scn));
      } else if (need_merge) {
        // check tablet diganose info // tablet level
        if (IS_UNKNOW_TABLET_ID(tablet_id)) {
          // skip
        } else if (OB_TMP_FAIL(ls->get_tablet(
              tablet_id,
              tablet_handle,
              storage::ObTabletCommon::DEFAULT_GET_TABLET_NO_WAIT))) {
          LOG_WARN("failed to get tablet", K(tmp_ret), K(ls_id), K(tablet_id));
        } else if (OB_UNLIKELY(!tablet_handle.is_valid())) {
          tmp_ret = OB_ERR_UNEXPECTED;
          LOG_WARN("invalid tablet handle", K(tmp_ret), K(ls_id), K(tablet_handle));
        } else {
          if (diagnose_major_flag && weak_read_ts_ready
              && OB_TMP_FAIL(diagnose_tablet_major_merge(compaction_scn, ls_id, *tablet_handle.get_obj()))) {
            LOG_WARN("failed to get diagnose major merge", K(tmp_ret));
          }
          if (OB_TMP_FAIL(diagnose_tablet_medium_merge(diagnose_major_flag, compaction_scn,
              ls_id, *tablet_handle.get_obj()))) {
            LOG_WARN("failed to get diagnose medium merge", K(tmp_ret));
          }
          if (OB_TMP_FAIL(diagnose_tablet_mini_merge(ls_id, *tablet_handle.get_obj()))) {
            LOG_WARN("failed to get diagnose mini merge", K(tmp_ret));
          }
          if (OB_TMP_FAIL(diagnose_tablet_minor_merge(ls_id, *tablet_handle.get_obj()))) {
            LOG_WARN("failed to get diagnose minor merge", K(tmp_ret));
          }
        }
        // don't have any diagnose info, push_back this tablet
        if (normal_) {
          tablet_array.push_back(diagnose_tablet);
        }
      }
    } // end of foreach
    (void)MTL(ObDiagnoseTabletMgr*)->remove_diagnose_tablets(tablet_array);
    diagnose_count_info();
    diagnose_existing_report_task();
    LOG_TRACE("finish diagnose tenant tablets", K(diagnose_tablets));
    if (checked_ls.created()) {
      checked_ls.destroy();
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
  int tmp_ret = OB_SUCCESS;
  if (OB_ISNULL(major_freeze_service)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(major_freeze_service));
  } else if (major_freeze_service->is_paused()) {
    // major service in RS is paused, should not show suspect info
    DEL_SUSPECT_INFO(MAJOR_MERGE, UNKNOW_LS_ID, UNKNOW_TABLET_ID, ObDiagnoseTabletType::TYPE_RS_MAJOR_MERGE);
  } else {
    (void) get_and_set_suspect_info(MAJOR_MERGE, UNKNOW_LS_ID, UNKNOW_TABLET_ID); // get RS schedule suspect info

    SMART_VARS_2((ObArray<ObTabletReplica>, uncompacted_tablets), (ObArray<uint64_t>, uncompacted_table_ids)) {
      if (OB_FAIL(major_freeze_service->get_uncompacted_tablets(uncompacted_tablets, uncompacted_table_ids))) {
        LOG_WARN("fail to get uncompacted tablets", KR(ret));
      } else {
        (void) add_uncompacted_tablet_to_diagnose(uncompacted_tablets);
        (void) add_uncompacted_table_ids_to_diagnose(uncompacted_table_ids);
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::add_uncompacted_tablet_to_diagnose(
  const ObIArray<ObTabletReplica> &uncompacted_tablets)
{
  int ret = OB_SUCCESS;
  const int64_t frozen_scn = MAX(MTL(ObTenantTabletScheduler*)->get_frozen_version(), MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version());
  const int64_t uncompacted_tablets_cnt = uncompacted_tablets.count();
  LOG_INFO("finish get uncompacted tablets for diagnose", K(ret), K(uncompacted_tablets_cnt));
  for (int64_t i = 0; OB_SUCC(ret) && i < uncompacted_tablets_cnt; ++i) {
    if (can_add_diagnose_info()) {
      const bool compaction_scn_not_valid = frozen_scn > uncompacted_tablets.at(i).get_snapshot_version();
      const char *status =
          ObTabletReplica::SCN_STATUS_ERROR == uncompacted_tablets.at(i).get_status()
              ? "CHECKSUM_ERROR"
              : (compaction_scn_not_valid ? "compaction_scn_not_update" : "report_scn_not_update");
      if (OB_FAIL(ADD_DIAGNOSE_INFO(
              MAJOR_MERGE, uncompacted_tablets.at(i).get_ls_id(),
              uncompacted_tablets.at(i).get_tablet_id(),
              ObCompactionDiagnoseInfo::DIA_STATUS_RS_UNCOMPACTED,
              ObTimeUtility::fast_current_time(), "server",
              uncompacted_tablets.at(i).get_server(), "status", status,
              "frozen_scn", frozen_scn, "compaction_scn",
              uncompacted_tablets.at(i).get_snapshot_version(), "report_scn",
              uncompacted_tablets.at(i).get_report_scn()))) {
        LOG_WARN("fail to set diagnose info", KR(ret), "uncompacted_tablet",
                 uncompacted_tablets.at(i));
        ret = OB_SUCCESS; // ignore ret, and process next uncompacted_tablet
      }
    } else {
      LOG_INFO("can not add diagnose info", K_(idx), K_(max_cnt),
               "uncompacted_tablet", uncompacted_tablets.at(i));
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::add_uncompacted_table_ids_to_diagnose(const ObIArray<uint64_t> &uncompacted_table_ids)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < uncompacted_table_ids.count(); ++i) {
    if (can_add_diagnose_info()) {
      if (OB_FAIL(ADD_COMMON_DIAGNOSE_INFO(
        MAJOR_MERGE,
        ObCompactionDiagnoseInfo::DIA_STATUS_RS_UNCOMPACTED,
        ObTimeUtility::fast_current_time(),
        "table_id", uncompacted_table_ids.at(i)))) {
        LOG_WARN("fail to set diagnose info", KR(ret), "uncompacted_tablet",
                 uncompacted_table_ids.at(i));
        ret = OB_SUCCESS; // ignore ret, and process next uncompacted_tablet
      }
    } else {
      LOG_INFO("can not add diagnose info", K_(idx), K_(max_cnt),
               "uncompacted_table", uncompacted_table_ids.at(i));
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
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObITable *first_frozen_memtable = nullptr;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_FAIL(table_store_wrapper.get_member()->get_first_frozen_memtable(first_frozen_memtable))) {
    LOG_WARN("Fail to get sstables", K(ret));
  } else if (nullptr != first_frozen_memtable) { // have frozen memtable
    bool diagnose_flag = false;
    ObSSTable *latest_sstable = nullptr;
    ObIMemtable *frozen_memtable = static_cast<ObIMemtable *>(first_frozen_memtable);
    if (OB_ISNULL(latest_sstable = static_cast<ObSSTable*>(
        table_store_wrapper.get_member()->get_minor_sstables().get_boundary_table(true/*last*/)))) {
      diagnose_flag = true;
    } else {
      if (latest_sstable->get_end_scn() < frozen_memtable->get_end_scn()
          || tablet.get_snapshot_version() < frozen_memtable->get_snapshot_version()) { // not merge finish
        diagnose_flag = true;
      }
    }
    if (diagnose_flag) {
      if (OB_TMP_FAIL(diagnose_tablet_merge(
          MINI_MERGE,
          ls_id,
          tablet))) {
        LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), "tablet_id", tablet.get_tablet_meta().tablet_id_, KPC(latest_sstable));
      }
    } else {
      (void) get_and_set_suspect_info(MINI_MERGE, ls_id, tablet_id);
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
  if (tablet.get_minor_table_count() >= minor_compact_trigger) {
    if (OB_FAIL(diagnose_tablet_merge(
        MINOR_MERGE,
        ls_id,
        tablet))) {
      LOG_WARN("diagnose failed", K(ret), K(ls_id), "tablet_id", tablet.get_tablet_meta().tablet_id_);
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
  int tmp_ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();
  int64_t max_sync_medium_scn = 0;
  if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(tablet.get_max_sync_medium_scn(max_sync_medium_scn))){
    LOG_WARN("failed to get max sync medium scn", K(ret), K(ls_id), K(tablet_id));
  } else {
    LOG_TRACE("diagnose tablet major merge", K(ls_id), K(tablet_id), K(compaction_scn), K(max_sync_medium_scn), K(last_major_snapshot_version));
    if (last_major_snapshot_version < compaction_scn) {
      if (max_sync_medium_scn < compaction_scn) {
        // max_sync_medium_scn > last_major_snapshot_version means last compaction is not finished,
        // this will be diagnosed in diagnose_tablet_medium_merge
        if (max_sync_medium_scn == last_major_snapshot_version) {
          // now last compaction finish
          if (OB_HASH_NOT_EXIST == get_and_set_suspect_info(MEDIUM_MERGE, ls_id, tablet_id)) {
            const char *info = "medium info behind major";
            if (0 == last_major_snapshot_version) {
              info = "medium info behind major & no major sstable";
            }
            ADD_MAJOR_WAIT_SCHEDULE(compaction_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL * 2, info);
          }
        }
      } else if (tablet.get_snapshot_version() < compaction_scn) { // wait mini compaction or tablet freeze
        const char* info = "major wait for freeze";
        ADD_MAJOR_WAIT_SCHEDULE(compaction_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL, info);
      } else if (0 == last_major_snapshot_version) {
        const char* info = "no major sstable";
        ADD_MAJOR_WAIT_SCHEDULE(compaction_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL, info);
      }
      if (OB_TMP_FAIL(diagnose_tablet_merge(
          MEDIUM_MERGE,
          ls_id,
          tablet,
          compaction_scn))) {
        LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), K(tablet_id), K(compaction_scn));
      }
    } else if (last_major_snapshot_version == compaction_scn) {
      diagnose_failed_report_task(ls_id, tablet_id, compaction_scn);
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_medium_merge(
    const bool diagnose_major_flag,
    const int64_t compaction_scn,
    const ObLSID &ls_id,
    ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  const int64_t last_major_snapshot_version = tablet.get_last_major_snapshot_version();
  int64_t max_sync_medium_scn = 0;
  if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
  } else if (OB_FAIL(tablet.get_max_sync_medium_scn(max_sync_medium_scn))){
    LOG_WARN("failed to get max sync medium scn", K(ret), K(ls_id), K(tablet_id));
  } else {
    LOG_TRACE("diagnose tablet medium merge", K(ls_id), K(tablet_id), K(max_sync_medium_scn), K(compaction_scn), K(last_major_snapshot_version));
    if (!diagnose_major_flag || (diagnose_major_flag && max_sync_medium_scn < compaction_scn)) {
      if (max_sync_medium_scn > last_major_snapshot_version) {
        if (tablet.get_snapshot_version() < max_sync_medium_scn) { // wait mini compaction or tablet freeze
          const char *info = "medium wait for freeze";
          ADD_MEDIUM_WAIT_SCHEDULE(max_sync_medium_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL, info);
        } else if (0 == last_major_snapshot_version) {
          const char *info = "no major sstable";
          ADD_MEDIUM_WAIT_SCHEDULE(max_sync_medium_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL, info);
        } else if (OB_TMP_FAIL(diagnose_tablet_merge(
            MEDIUM_MERGE,
            ls_id,
            tablet,
            max_sync_medium_scn))) {
          LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), K(tablet_id), K(max_sync_medium_scn), K(last_major_snapshot_version));
        }
      }
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_row_store_dag(
    const ObMergeType merge_type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  ObTabletMajorMergeDag major_dag;
  ObTabletMergeExecuteDag minor_dag;
  ObTabletMiniMergeDag mini_dag;
  ObTabletMergeDag *dag = nullptr;
  if (is_major_merge_type(merge_type)) {
    dag = &major_dag;
  } else if (is_minor_merge(merge_type)) {
    dag = &minor_dag;
  } else if (is_mini_merge(merge_type)) {
    dag = &mini_dag;
  }
  if (OB_ISNULL(dag)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("failed to diagnose dag", K(ret), K(ls_id), K(tablet_id), K(merge_type));
  } else {
    ObDiagnoseTabletCompProgress progress;
    if (OB_FAIL(diagnose_dag(merge_type, ls_id, tablet_id, compaction_scn, *dag, progress))) {
      if (OB_HASH_NOT_EXIST != ret) {
        LOG_WARN("failed to diagnose dag", K(ret), K(ls_id), K(tablet_id));
      } else if (OB_FAIL(diagnose_no_dag(dag->hash(), merge_type, ls_id, tablet_id, compaction_scn))) {
        LOG_WARN("failed to diagnose no dag", K(ret), K(ls_id), K(tablet_id));
      }
    } else if (progress.is_valid()) { // dag exist, means compaction is running
      // check progress is normal
      if (progress.is_suspect_abormal_) { // progress is abnomal
        if (can_add_diagnose_info()
              && OB_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET(
                merge_type,
                ObCompactionDiagnoseInfo::DIA_STATUS_RUNNING,
                ObTimeUtility::fast_current_time(),
                "current_status", "dag may hang",
                "merge_progress", progress))) {
          LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), K(progress));
        }
      }
    } else if (OB_FAIL(diagnose_no_dag(dag->hash(), merge_type, ls_id, tablet_id, compaction_scn))) {
      LOG_WARN("failed to dagnose no dag", K(ret), K(ls_id), K(tablet_id));
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_column_store_dag(
    const ObMergeType merge_type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id,
    const lib::Worker::CompatMode &compat_mode,
    const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  // create a fake dag net
  ObCOMergeDagParam param;
  param.merge_type_ = merge_type;
  param.merge_version_ = compaction_scn;
  param.ls_id_ = ls_id;
  param.tablet_id_ = tablet_id;
  param.compat_mode_ = compat_mode;
  param.skip_get_tablet_ = true;

  int64_t dag_net_start_time = 0;
  ObCOMergeDagNet dag_net;
  ObDagId dag_net_id;
  common::ObSEArray<ObDiagnoseTabletCompProgress, 16> progress_list;
  progress_list.set_attr(ObMemAttr(MTL_ID(), "diagList"));
  if (OB_FAIL(dag_net.init_by_param(&param))) {
    STORAGE_LOG(WARN, "failed to init dag net", K(ret), K(param));
  } else if (OB_FAIL(MTL(ObTenantDagScheduler *)->diagnose_dag_net(&dag_net, progress_list, dag_net_id, dag_net_start_time))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to diagnose dag net", K(ret), K(ls_id), K(tablet_id));
    } else {
      LOG_INFO("dag net may be finished or not exist", K(ret), K(ls_id), K(tablet_id));
      // dag net may be finished
      ret = OB_SUCCESS;
    }
  } else if (progress_list.empty()) {
    if (TOLERATE_MEDIUM_SCHEDULE_INTERVAL < fast_current_time() - dag_net_start_time && can_add_diagnose_info()) {
      ADD_DIAGNOSE_INFO_FOR_TABLET(
          merge_type,
          ObCompactionDiagnoseInfo::DIA_STATUS_RUNNING,
          ObTimeUtility::fast_current_time(),
          "current_status", "dag net may hang",
          "dag_net_id", dag_net_id,
          "dag_net_start_time", dag_net_start_time);
    }
  } else {
    for (int i = 0; i < progress_list.count(); ++i) {
      ObDiagnoseTabletCompProgress &progress = progress_list.at(i);
      if (progress.is_valid()) { // dag exist, means compaction is running
        // check progress is normal
        if (progress.is_suspect_abormal_) { // progress is abnomal
          if (can_add_diagnose_info()) {
            ADD_DIAGNOSE_INFO_FOR_TABLET(
                merge_type,
                ObCompactionDiagnoseInfo::DIA_STATUS_RUNNING,
                ObTimeUtility::fast_current_time(),
                "current_status", "dag may hang",
                "merge_progress", progress);
          }
        }
      }
    }
  }
  if (OB_SUCC(ret)) {
    int64_t dag_key = dag_net.hash();
    if (OB_FAIL(diagnose_no_dag(dag_key, merge_type, ls_id, tablet_id, compaction_scn))) {
      LOG_WARN("failed to diagnose no dag", K(ret), K(dag_key), K(ls_id), K(tablet_id));
    }
  }
  LOG_TRACE("diagnose co dag net finished", K(ls_id), K(tablet_id));
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_merge(
    const ObMergeType merge_type,
    const ObLSID ls_id,
    ObTablet &tablet,
    const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  lib::Worker::CompatMode compat_mode = tablet.get_tablet_meta().compat_mode_;
  if (!compaction::is_major_merge_type(merge_type) || tablet.is_row_store()) {
    if (OB_FAIL(diagnose_row_store_dag(merge_type, ls_id, tablet_id, compaction_scn))) {
      LOG_WARN("failed to diagnose row store dag", K(ret), K(ls_id), K(tablet_id), K(merge_type), K(compaction_scn));
    }
  } else if (OB_FAIL(diagnose_column_store_dag(merge_type, ls_id, tablet_id, compat_mode, compaction_scn))) {
    LOG_WARN("failed to diagnose column store dag", K(ret), K(ls_id), K(tablet_id), K(merge_type), K(compaction_scn));
  }
  return ret;
}

int ObCompactionDiagnoseMgr::get_suspect_and_warning_info(
    const int64_t dag_key,
    const ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    ObScheduleSuspectInfo &info,
    ObSuspectInfoType &suspect_type,
    char *buf,
    const int64_t buf_len)
{
  int ret = OB_SUCCESS;

  suspect_type = ObSuspectInfoType::SUSPECT_INFO_TYPE_MAX;
  ObDagWarningInfo warning_info;
  bool add_schedule_info = false;
  ObInfoParamBuffer allocator;
  compaction::ObMergeDagHash dag_hash;
  dag_hash.merge_type_ = merge_type;
  dag_hash.ls_id_ = ls_id;
  dag_hash.tablet_id_ = tablet_id;
  if (OB_FAIL(MTL(ObScheduleSuspectInfoMgr *)->get_with_param(ObScheduleSuspectInfo::gen_hash(MTL_ID(), dag_hash.inner_hash()), &info, allocator))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get suspect info", K(ret), K(dag_hash));
    } else { // no schedule suspect info
      LOG_INFO("no schedule suspect info", K(ret), K(dag_hash));
      info.info_param_ = nullptr;
      allocator.reuse();
      char tmp_str[common::OB_DAG_WARNING_INFO_LENGTH] = "\0";
      if (OB_FAIL(MTL(ObDagWarningHistoryManager *)->get_with_param(
                    dag_key, &warning_info, allocator))) {
        // check __all_virtual_dag_warning_history
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get dag warning info", K(ret), K(dag_hash));
        } else { // no execute failure
          ret = OB_SUCCESS;
          LOG_INFO("no dag warning info. may wait for schedule", K(ret), K(dag_key), K(dag_hash));
        }
      } else if (can_add_diagnose_info()) {
        if (OB_FAIL(warning_info.info_param_->fill_comment(tmp_str, sizeof(tmp_str)))) {
          STORAGE_LOG(WARN, "failed to fill comment from info param", K(ret));
        } else if (warning_info.location_.is_valid()) {
          if (OB_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET(
                  merge_type,
                  ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                  warning_info.gmt_create_,
                  "error_no", warning_info.dag_ret_,
                  "last_error_time", warning_info.gmt_modified_,
                  "error_trace", warning_info.task_id_,
                  "location", warning_info.location_,
                  "warning", tmp_str))) {
            LOG_WARN("failed to add diagnose info", K(ret), K(dag_hash), K(warning_info));
          }
        } else if (OB_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET(
                merge_type,
                ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                warning_info.gmt_create_,
                "error_no", warning_info.dag_ret_,
                "last_error_time", warning_info.gmt_modified_,
                "error_trace", warning_info.task_id_,
                "warning", tmp_str))) {
          LOG_WARN("failed to add diagnose info", K(ret), K(dag_hash), K(warning_info));
        }
      }
    }
  } else if (OB_FAIL(info.info_param_->fill_comment(buf, buf_len))) {
    STORAGE_LOG(WARN, "failed to fill comment from info param", K(ret));
  } else if (FALSE_IT(suspect_type = info.info_param_->type_.suspect_type_)) {
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_no_dag(
    const int64_t dag_key,
    const ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  ObScheduleSuspectInfo info;
  bool add_schedule_info = false;
  ObSuspectInfoType suspect_type = SUSPECT_INFO_TYPE_MAX;

  char tmp_str[common::OB_DIAGNOSE_INFO_LENGTH] = "\0";
  if (OB_FAIL(get_suspect_and_warning_info(dag_key, merge_type, ls_id, tablet_id, info, suspect_type, tmp_str, sizeof(tmp_str)))) {
    LOG_WARN("failed to get suspect and warning info", K(ret), K(ls_id), K(tablet_id));
  } else if (!info.is_valid()) {
    // do nothing
  } else if (is_medium_merge(merge_type)) {
    if (OB_UNLIKELY(compaction_scn <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge version or freeze ts is invalid", K(ret), K(compaction_scn));
    } else {
      LOG_INFO("diagnose major", K(ret), K(ls_id), K(tablet_id), "merge_type", merge_type_to_str(merge_type));
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
            && OB_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET(
                merge_type,
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

  if (OB_SUCC(ret) && add_schedule_info && can_add_diagnose_info() && suspect_type < SUSPECT_INFO_TYPE_MAX) {
    // check tablet_type in get_diagnose_tablet_count
    if (suspect_tablet_count_[suspect_type] < DIAGNOSE_TABELT_MAX_COUNT) {
      if (OB_FAIL(ADD_DIAGNOSE_INFO_FOR_TABLET(
            merge_type,
            ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
            info.add_time_,
            "schedule_suspect_info", tmp_str))) {
        LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), K(info));
      }
    }
    ++suspect_tablet_count_[suspect_type];
    suspect_merge_type_[suspect_type] = merge_type;
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_medium_scn_table()
{
  int ret = OB_SUCCESS;
  int64_t error_tablet_cnt = MTL(ObTenantTabletScheduler*)->get_error_tablet_cnt();
  if (0 != error_tablet_cnt
      && can_add_diagnose_info()
      && OB_FAIL(ADD_COMMON_DIAGNOSE_INFO(
          MEDIUM_MERGE,
          ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
          ObTimeUtility::fast_current_time(),
          "checksum may error. error_tablet_cnt", error_tablet_cnt))) {
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
    STORAGE_LOG(WARN, "failed to alloc info array", K(ret));
  } else if (FALSE_IT(info_array_ = new (buf) ObCompactionDiagnoseInfo[MAX_DIAGNOSE_INFO_CNT])) {
  } else if (OB_FAIL(diagnose_mgr.init(&allocator_, info_array_, MAX_DIAGNOSE_INFO_CNT))) {
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
