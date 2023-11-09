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
  if (OB_UNLIKELY(merge_type_ <= INVALID_MERGE_TYPE || merge_type_ >= MERGE_TYPE_MAX
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

share::ObSuspectInfoType ObScheduleSuspectInfo::get_suspect_info_type() const
{
  share::ObSuspectInfoType type = share::ObSuspectInfoType::SUSPECT_INFO_TYPE_MAX;
  if (OB_NOT_NULL(info_param_)) {
    type = info_param_->type_.suspect_type_;
  }
  return type;
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
  common::SpinWLockGuard guard(lock_);
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
    STORAGE_LOG(INFO, "success to purge", K(ret), K(batch_purge), "max_size", allocator_.get_max(),
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

const char *ObCompactionDiagnoseInfo::ObDiagnoseStatusStr[DIA_STATUS_MAX] = {
    "NOT_SCHEDULE",
    "RUNNING",
    "FAILED",
    "FINISH",
    "RS_UNCOMPACTED"
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
   allocator_(nullptr),
   tablet_iter_(nullptr),
   tablet_allocator_("CompDia"),
   tablet_handle_(),
   iter_buf_(nullptr),
   info_array_(nullptr),
   medium_not_schedule_count_(0),
   major_not_schedule_count_(0),
   max_cnt_(0),
   idx_(0)
  {
    MEMSET(suspect_tablet_count_, 0, sizeof(suspect_tablet_count_));
  }

void ObCompactionDiagnoseMgr::reset()
{
  info_array_ = nullptr;
  max_cnt_ = 0;
  idx_ = 0;
  medium_not_schedule_count_ = 0;
  major_not_schedule_count_ = 0;
  is_inited_ = false;
  MEMSET(suspect_tablet_count_, 0, sizeof(suspect_tablet_count_));
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletIterator();
    tablet_iter_ = nullptr;
  }
  if (OB_NOT_NULL(iter_buf_) && OB_NOT_NULL(allocator_)) {
    allocator_->free(iter_buf_);
    iter_buf_ = nullptr;
  }
  tablet_handle_.reset();
  tablet_allocator_.reset();
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
  } else if (FALSE_IT(allocator_ = allocator)) {
  } else if (OB_ISNULL(iter_buf_ = allocator_->alloc(sizeof(ObTenantTabletIterator)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to alloc tablet iter buf", K(ret));
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

int ObCompactionDiagnoseMgr::get_suspect_info_and_print(
    const ObMergeType merge_type,
    const ObLSID &ls_id,
    const ObTabletID &tablet_id)
{
  int ret = OB_SUCCESS;
  ObScheduleSuspectInfo input_info;
  input_info.tenant_id_ = MTL_ID();
  input_info.merge_type_ = merge_type;
  input_info.ls_id_ = ls_id;
  input_info.tablet_id_ = tablet_id;
  ObInfoParamBuffer allocator;
  char tmp_str[common::OB_DIAGNOSE_INFO_LENGTH] = "\0";
  if (OB_FAIL(MTL(ObScheduleSuspectInfoMgr *)->get_with_param(input_info.hash(), &input_info, allocator))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get suspect info", K(ret), K(input_info));
    }
  } else if (OB_FAIL(input_info.info_param_->fill_comment(tmp_str, sizeof(tmp_str)))) {
    STORAGE_LOG(WARN, "failed to fill comment from info param", K(ret));
  } else if (can_add_diagnose_info()) {
    SET_DIAGNOSE_INFO(
        info_array_[idx_++],
        merge_type,
        input_info.tenant_id_,
        ls_id,
        tablet_id,
        ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
        input_info.add_time_,
        "schedule_suspect_info", tmp_str);
  }
  return ret;
}

void ObCompactionDiagnoseMgr::diagnose_tenant_ls(
    const bool diagnose_major_flag,
    const bool weak_read_ts_ready,
    const int64_t compaction_scn,
    const bool is_leader,
    const ObLSID &ls_id)
{
  int tmp_ret = OB_SUCCESS;
  // check weak read ts
  if (diagnose_major_flag
      && !weak_read_ts_ready
      && can_add_diagnose_info()
      && OB_TMP_FAIL(SET_DIAGNOSE_INFO(
                info_array_[idx_++],
                MEDIUM_MERGE,
                MTL_ID(),
                ls_id,
                ObTabletID(INT64_MAX),
                ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
                ObTimeUtility::fast_current_time(),
                "weak read ts is not ready, compaction_scn",
                compaction_scn))) {
    LOG_WARN_RET(tmp_ret, "failed to add dignose info about weak read ts", K(ls_id), K(compaction_scn));
  }
  // check ls suspect info for memtable freezing
  if (OB_TMP_FAIL(get_suspect_info_and_print(MINI_MERGE, ls_id, ObTabletID(INT64_MAX)))) {
    LOG_WARN_RET(tmp_ret, "failed to diagnose about memtable freezing", K(ls_id));
  }
  // check ls locality change and leader change
  if (is_leader && OB_TMP_FAIL(get_suspect_info_and_print(MEDIUM_MERGE, ls_id, ObTabletID(INT64_MAX)))) {
    LOG_WARN_RET(tmp_ret, "failed to diagnose about ls locality change", K(ls_id));
  }
}

int ObCompactionDiagnoseMgr::get_next_tablet(ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  tablet_handle_.reset();
  tablet_allocator_.reuse();
  ls_id.reset();
  if (nullptr == tablet_iter_) {
    tablet_allocator_.set_tenant_id(MTL_ID());
    ObTenantMetaMemMgr *t3m = MTL(ObTenantMetaMemMgr*);
    if (OB_ISNULL(tablet_iter_ = new (iter_buf_) ObTenantTabletIterator(*t3m, tablet_allocator_))) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "fail to new tablet_iter_", K(ret));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(tablet_iter_->get_next_tablet(tablet_handle_))) {
    if (OB_UNLIKELY(OB_ITER_END != ret)) {
      STORAGE_LOG(WARN, "fail to get tablet iter", K(ret));
    }
  } else if (OB_UNLIKELY(!tablet_handle_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "unexpected invalid tablet", K(ret), K(tablet_handle_));
  } else {
    ls_id = tablet_handle_.get_obj()->get_tablet_meta().ls_id_;
  }
  return ret;
}

void ObCompactionDiagnoseMgr::release_last_tenant()
{
  if (OB_NOT_NULL(tablet_iter_)) {
    tablet_iter_->~ObTenantTabletIterator();
    tablet_iter_ = nullptr;
  }
  tablet_handle_.reset();
  tablet_allocator_.reset();
  major_not_schedule_count_ = 0;
  medium_not_schedule_count_ = 0;
  MEMSET(suspect_tablet_count_, 0, sizeof(suspect_tablet_count_));
}

int ObCompactionDiagnoseMgr::gen_ls_check_status(
    const ObLSID &ls_id,
    const int64_t compaction_scn,
    ObLSCheckStatus &ls_status)
{
  int ret = OB_SUCCESS;
  ls_status.reset();
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  if (OB_FAIL(MTL(ObLSService *)->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD))) {
    LOG_WARN("failed to get log stream", K(ret), K(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())){
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is nullptr", K(ret), KPC(ls));
  } else if (OB_FAIL(ObTenantTabletScheduler::check_ls_state(*ls, ls_status.need_merge_))) {
    LOG_WARN("failed to check ls state", K(ret), KPC(ls), K(ls_status));
  } else if (ls_status.need_merge_) {
    ObRole role = INVALID_ROLE;
    if (OB_FAIL(ObMediumCompactionScheduleFunc::get_palf_role(ls_id, role))) {
      if (OB_LS_NOT_EXIST != ret) {
        LOG_WARN("failed to get palf handle role", K(ret), K(ls_id));
      }
    } else if (is_leader_by_election(role)) {
      ls_status.is_leader_ = true;
    }
    ls_status.weak_read_ts_ready_ = ObTenantTabletScheduler::check_weak_read_ts_ready(compaction_scn, *ls);
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tenant_tablet()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  LSStatusMap ls_status_map;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObCompactionDiagnoseMgr is not init", K(ret));
  } else if (OB_FAIL(ls_status_map.create(OB_MAX_LS_NUM_PER_TENANT_PER_SERVER, ObMemAttr(MTL_ID(), "CompDiaLS")))) {
    LOG_WARN("failed to create checked ls map", K(ret));
  } else {
    int tmp_ret = OB_SUCCESS;
    bool diagnose_major_flag = false;
    ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler*);
    int64_t compaction_scn = MAX(scheduler->get_frozen_version(), MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version());
    ObTenantFreezeInfoMgr::FreezeInfo freeze_info;

    if (compaction_scn > scheduler->get_inner_table_merged_scn()) { // check major merge
      diagnose_major_flag = true;
      const int64_t merged_version = scheduler->get_inner_table_merged_scn();
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
      if (OB_TMP_FAIL(get_suspect_info_and_print(MEDIUM_MERGE, share::ObLSID(INT64_MAX), ObTabletID(INT64_MAX)))) {
        LOG_WARN("failed get tenant merge suspect info", K(tmp_ret));
      }
      if ((!scheduler->could_major_merge_start()
          || scheduler->get_prohibit_medium_ls_map().get_transfer_flag_cnt() > 0)
            && can_add_diagnose_info()) {
        SET_DIAGNOSE_INFO(
            info_array_[idx_++],
            !scheduler->could_major_merge_start() ? MAJOR_MERGE : MEDIUM_MERGE,
            MTL_ID(),
            share::ObLSID(INT64_MAX),
            ObTabletID(INT64_MAX),
            ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
            ObTimeUtility::fast_current_time(),
            "info", "major or medium may be suspended",
            "could_major_merge", scheduler->could_major_merge_start(),
            "prohibit_medium_ls_info", scheduler->get_prohibit_medium_ls_map());
      }
    }

    ObLSID ls_id;
    ObLSCheckStatus ls_check_status;
    bool tenant_major_finish = true;
    bool tablet_major_finish = false;
    ObSEArray<ObLSID, 64> abnormal_ls_id;
    int64_t diagnose_tablets = 0;
    while (OB_SUCC(ret) && can_add_diagnose_info()) { // loop all log_stream
      bool need_merge = false;
      if (OB_FAIL(get_next_tablet(ls_id))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next tablet", K(ret));
        }
      } else if (OB_FAIL(ls_status_map.get_refactored(ls_id, ls_check_status))) {
        if (OB_HASH_NOT_EXIST == ret) {
          if (OB_FAIL(gen_ls_check_status(ls_id, compaction_scn, ls_check_status))) {
            LOG_WARN("failed to get ls check status", K(ret), K(ls_id), K(compaction_scn), K(ls_check_status));
          } else if (OB_FAIL(ls_status_map.set_refactored(ls_id, ls_check_status))) {
            LOG_WARN("failed to set ls check status", K(ret), K(ls_id), K(ls_check_status));
          } else if (!ls_check_status.need_merge_) {
            (void)abnormal_ls_id.push_back(ls_id);
          } else {
            (void)diagnose_tenant_ls(diagnose_major_flag, ls_check_status.weak_read_ts_ready_, compaction_scn,
                ls_check_status.is_leader_, ls_id);
          }
        } else {
          LOG_WARN("failed to get ls check status from map", K(ret));
        }
      }
      ++diagnose_tablets;
      if (OB_SUCC(ret)) {
        if (ls_check_status.need_merge_) {
          if (OB_TMP_FAIL(diagnose_tablet_major_and_medium(diagnose_major_flag,
              ls_check_status.weak_read_ts_ready_, compaction_scn,
              ls_id, *tablet_handle_.get_obj(), tablet_major_finish))) {
            LOG_WARN("failed to get diagnose major/medium merge", K(tmp_ret));
          }
          if (OB_TMP_FAIL(diagnose_tablet_mini_merge(ls_id, *tablet_handle_.get_obj()))) {
            LOG_WARN("failed to get diagnose mini merge", K(tmp_ret));
          }
          if (OB_TMP_FAIL(diagnose_tablet_minor_merge(ls_id, *tablet_handle_.get_obj()))) {
            LOG_WARN("failed to get diagnose minor merge", K(tmp_ret));
          }
        }
        tenant_major_finish &= tablet_major_finish;
      }
    } // end of while
    if (OB_SUCC(ret) && diagnose_major_flag && tenant_major_finish && can_add_diagnose_info()) {
      ObCompactionDiagnoseInfo &info = info_array_[idx_++];
      SET_DIAGNOSE_INFO(
        info,
        MEDIUM_MERGE,
        MTL_ID(),
        share::ObLSID(INT64_MAX),
        ObTabletID(INT64_MAX),
        ObCompactionDiagnoseInfo::DIA_STATUS_FINISH,
        ObTimeUtility::fast_current_time(),
        "compaction has finished in storage, please check RS. compaction_scn", compaction_scn);
      if (!abnormal_ls_id.empty()) {
        char * buf = info.diagnose_info_;
        const int64_t buf_len = common::OB_DIAGNOSE_INFO_LENGTH;
        ADD_COMPACTION_INFO_PARAM(buf, buf_len,
            "some ls may offline", abnormal_ls_id);
      }
    }
    LOG_INFO("finish diagnose tenant tablets", K(diagnose_tablets), K_(major_not_schedule_count), K_(medium_not_schedule_count));
    release_last_tenant();
  }
  if (ls_status_map.created()) {
    ls_status_map.destroy();
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
    ObTenantTabletScheduler *scheduler = MTL(ObTenantTabletScheduler*);
    const int64_t frozen_scn = MAX(scheduler->get_frozen_version(), MTL(ObTenantFreezeInfoMgr*)->get_latest_frozen_version());
    SMART_VAR(ObArray<ObTabletReplica>, uncompacted_tablets) {
      if (OB_FAIL(major_freeze_service->get_uncompacted_tablets(uncompacted_tablets))) {
        LOG_WARN("fail to get uncompacted tablets", KR(ret));
      } else {
        int64_t uncompacted_tablets_cnt = uncompacted_tablets.count();
        LOG_INFO("finish get uncompacted tablets for diagnose", K(ret), K(uncompacted_tablets_cnt));
        for (int64_t i = 0; (OB_SUCCESS == ret) && i < uncompacted_tablets_cnt; ++i) {
          if (can_add_diagnose_info()) {
            bool compaction_scn_not_valid = frozen_scn > uncompacted_tablets.at(i).get_snapshot_version();
            const char *status =
                ObTabletReplica::SCN_STATUS_ERROR == uncompacted_tablets.at(i).get_status()
                    ? "CHECKSUM_ERROR"
                    : (compaction_scn_not_valid ? "compaction_scn_not_update" : "report_scn_not_update");
            if (OB_FAIL(SET_DIAGNOSE_INFO(
                    info_array_[idx_++], MAJOR_MERGE, MTL_ID(),
                    uncompacted_tablets.at(i).get_ls_id(),
                    uncompacted_tablets.at(i).get_tablet_id(),
                    ObCompactionDiagnoseInfo::DIA_STATUS_RS_UNCOMPACTED,
                    ObTimeUtility::fast_current_time(), "server",
                    uncompacted_tablets.at(i).get_server(), "status", status,
                    "frozen_scn", frozen_scn,
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
    memtable::ObIMemtable *frozen_memtable = static_cast<memtable::ObIMemtable *>(first_frozen_memtable);
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
      ObTabletMiniMergeDag dag;
      if (OB_TMP_FAIL(diagnose_tablet_merge(
              dag,
              MINI_MERGE,
              ls_id,
              tablet_id))) {
        LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), K(tablet), KPC(latest_sstable));
      }
    } else if (OB_FAIL(get_suspect_info_and_print(MINI_MERGE, ls_id, tablet_id))) { // mini compaction finish, but memtable have not release
      LOG_WARN("failed get mini merge suspect info about memtable release", K(ret));
    }
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_tablet_minor_merge(const ObLSID &ls_id, ObTablet &tablet)
{
  int ret = OB_SUCCESS;
  int64_t minor_compact_trigger = ObPartitionMergePolicy::DEFAULT_MINOR_COMPACT_TRIGGER;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;

  {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(MTL_ID()));
    if (tenant_config.is_valid()) {
      minor_compact_trigger = tenant_config->minor_compact_trigger;
    }
  }

  if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (table_store_wrapper.get_member()->get_minor_sstables().count() >= minor_compact_trigger) {
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

int ObCompactionDiagnoseMgr::diagnose_tablet_major_and_medium(
    const bool diagnose_major_flag,
    const bool weak_read_ts_ready,
    const int64_t compaction_scn,
    const ObLSID &ls_id,
    ObTablet &tablet,
    bool &tablet_major_finish)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  tablet_major_finish = false;
  const ObTabletID &tablet_id = tablet.get_tablet_meta().tablet_id_;
  ObITable *last_major_sstable = nullptr;
  int64_t max_sync_medium_scn = 0;
  ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
  ObArenaAllocator allocator;
  const compaction::ObMediumCompactionInfoList *medium_list = nullptr;
  if (tablet_id.is_ls_inner_tablet()) {
    // do nothing
    tablet_major_finish = true;
  } else if (OB_FAIL(tablet.fetch_table_store(table_store_wrapper))) {
    LOG_WARN("fail to fetch table store", K(ret));
  } else if (OB_ISNULL(last_major_sstable =
      table_store_wrapper.get_member()->get_major_sstables().get_boundary_table(true/*last*/))) {
  } else if (OB_FAIL(tablet.read_medium_info_list(allocator, medium_list))) {
    LOG_WARN("failed to load medium info list", K(ret), K(tablet));
  } else if (OB_FAIL(ObMediumCompactionScheduleFunc::get_max_sync_medium_scn(
      tablet, *medium_list, max_sync_medium_scn))){
    LOG_WARN("failed to get max sync medium scn", K(ret), K(ls_id), K(tablet_id));
  } else {
    // diagnose medium
    LOG_TRACE("diagnose tablet medium merge", K(max_sync_medium_scn));
    if (!diagnose_major_flag || (diagnose_major_flag && max_sync_medium_scn < compaction_scn)) {
      if (max_sync_medium_scn > last_major_sstable->get_snapshot_version()) {
        if (tablet.get_snapshot_version() < max_sync_medium_scn) { // wait mini compaction or tablet freeze
          if (ObTimeUtility::current_time_ns() > max_sync_medium_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL) {
            if (DIAGNOSE_TABELT_MAX_COUNT > medium_not_schedule_count_ && can_add_diagnose_info()) {
              SET_DIAGNOSE_INFO(
                  info_array_[idx_++],
                  MEDIUM_MERGE,
                  MTL_ID(),
                  ls_id,
                  tablet_id,
                  gen_diagnose_status(max_sync_medium_scn),
                  ObTimeUtility::fast_current_time(),
                  "medium wait for freeze, interval", static_cast<int64_t>(WAIT_MEDIUM_SCHEDULE_INTERVAL / NS_TIME),
                  "max_receive_medium_scn", max_sync_medium_scn,
                  "tablet_snapshot", tablet.get_snapshot_version());
            }
            ++medium_not_schedule_count_;
          }
        } else {
          // last medium not finish or schedule
          ObTabletMajorMergeDag dag;
          if (OB_TMP_FAIL(diagnose_tablet_merge(
                  dag,
                  MEDIUM_MERGE,
                  ls_id,
                  tablet_id,
                  max_sync_medium_scn))) {
            LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), K(tablet_id), KPC(last_major_sstable));
          }
        }
      }
    }

    // diagnose major
    LOG_TRACE("diagnose tablet major merge", K(compaction_scn));
    if (diagnose_major_flag && weak_read_ts_ready) {
      if (tablet.get_tablet_meta().has_transfer_table()) {
        if (REACH_TENANT_TIME_INTERVAL(30 * 1000L * 1000L/*30s*/)) {
          LOG_INFO("The tablet in the transfer process does not do major_merge", "tablet_id", tablet.get_tablet_meta().tablet_id_);
        }
      } else if (nullptr == last_major_sstable
          || last_major_sstable->get_snapshot_version() < compaction_scn) {
        if (max_sync_medium_scn < compaction_scn
            && max_sync_medium_scn == last_major_sstable->get_snapshot_version()) {
          // last compaction finish
          if (OB_TMP_FAIL(get_suspect_info_and_print(MEDIUM_MERGE, ls_id, tablet_id))) {
            if (OB_HASH_NOT_EXIST != tmp_ret) {
              LOG_WARN("failed get major merge suspect info", K(ret), K(ls_id));
            }
          }
          if (OB_HASH_NOT_EXIST == tmp_ret
              && ObTimeUtility::current_time_ns() > compaction_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL * 2) {
            if (DIAGNOSE_TABELT_MAX_COUNT > major_not_schedule_count_
                && can_add_diagnose_info()
                && OB_TMP_FAIL(SET_DIAGNOSE_INFO(
                    info_array_[idx_++],
                    MAJOR_MERGE,
                    MTL_ID(),
                    ls_id,
                    tablet_id,
                    gen_diagnose_status(compaction_scn),
                    ObTimeUtility::fast_current_time(),
                    "major not schedule for long time, interval", static_cast<int64_t>(WAIT_MEDIUM_SCHEDULE_INTERVAL * 2 / NS_TIME),
                    "max_receive_medium_snapshot", max_sync_medium_scn,
                    "compaction_scn", compaction_scn))) {
              LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id));
            }
            ++major_not_schedule_count_;
          }
        } else if (max_sync_medium_scn == compaction_scn) {
          if (tablet.get_snapshot_version() < compaction_scn) { // wait mini compaction or tablet freeze
            if (ObTimeUtility::current_time_ns() > compaction_scn + WAIT_MEDIUM_SCHEDULE_INTERVAL) {
              if (DIAGNOSE_TABELT_MAX_COUNT > major_not_schedule_count_ && can_add_diagnose_info()) {
                SET_DIAGNOSE_INFO(
                    info_array_[idx_++],
                    MAJOR_MERGE,
                    MTL_ID(),
                    ls_id,
                    tablet_id,
                    gen_diagnose_status(compaction_scn),
                    ObTimeUtility::fast_current_time(),
                    "major wait for freeze, interval", static_cast<int64_t>(WAIT_MEDIUM_SCHEDULE_INTERVAL / NS_TIME),
                    "compaction_scn", compaction_scn,
                    "tablet_snapshot", tablet.get_snapshot_version());
              }
            }
            ++major_not_schedule_count_;
          } else {
            ObTabletMajorMergeDag dag;
            if (OB_TMP_FAIL(diagnose_tablet_merge(
                    dag,
                    MEDIUM_MERGE,
                    ls_id,
                    tablet.get_tablet_meta().tablet_id_,
                    compaction_scn))) {
              LOG_WARN("diagnose failed", K(tmp_ret), K(ls_id), K(tablet), KPC(last_major_sstable));
            }
          }
        }
      } else {
        tablet_major_finish = true;
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
      const char* current_status = progress.is_waiting_schedule_ ? "dag may be waiting for schedule" : "dag may hang";
      if (can_add_diagnose_info()
            && OB_FAIL(SET_DIAGNOSE_INFO(
              info_array_[idx_++],
              merge_type,
              MTL_ID(),
              ls_id,
              tablet_id,
              ObCompactionDiagnoseInfo::DIA_STATUS_RUNNING,
              ObTimeUtility::fast_current_time(),
              K(current_status),
              "merge_progress", progress))) {
        LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), K(progress));
      }
    }
    LOG_TRACE("dag exist", K(dag), K(progress));
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
  if (OB_FAIL(MTL(ObScheduleSuspectInfoMgr *)->get_with_param(ObScheduleSuspectInfo::gen_hash(MTL_ID(), dag.hash()), &info, allocator))) {
    if (OB_HASH_NOT_EXIST != ret) {
      LOG_WARN("failed to get suspect info", K(ret), K(ls_id), K(tablet_id));
    } else { // no schedule suspect info
      LOG_TRACE("suspect info not exist", K(ret), K(dag));
      info.info_param_ = nullptr;
      allocator.reuse();
      char tmp_str[common::OB_DAG_WARNING_INFO_LENGTH] = "\0";
      if (OB_FAIL(MTL(ObDagWarningHistoryManager *)->get_with_param(
                    dag.hash(), &warning_info, allocator))) {
        // check __all_virtual_dag_warning_history
        if (OB_HASH_NOT_EXIST != ret) {
          LOG_WARN("failed to get dag warning info", K(ret), K(ls_id), K(tablet_id));
        } else { // no execute failure
          ret = OB_SUCCESS;
          LOG_TRACE("may wait for schedule", K(ret), K(ls_id), K(tablet_id));
        }
      } else if (can_add_diagnose_info()) {
        if (OB_FAIL(warning_info.info_param_->fill_comment(tmp_str, sizeof(tmp_str)))) {
          STORAGE_LOG(WARN, "failed to fill comment from info param", K(ret));
        } else if (OB_FAIL(SET_DIAGNOSE_INFO(
              info_array_[idx_++],
              merge_type,
              MTL_ID(),
              ls_id,
              tablet_id,
              ObCompactionDiagnoseInfo::DIA_STATUS_FAILED,
              warning_info.gmt_create_,
              "error_no", warning_info.dag_ret_,
              "last_error_time", warning_info.gmt_modified_,
              "error_trace", warning_info.task_id_,
              "warning", tmp_str))) {
          LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id));
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
    ObTabletMergeDag &dag,
    const ObMergeType merge_type,
    const ObLSID ls_id,
    const ObTabletID tablet_id,
    const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  ObScheduleSuspectInfo info;
  bool add_schedule_info = false;
  ObSuspectInfoType suspect_type;

  char tmp_str[common::OB_DIAGNOSE_INFO_LENGTH] = "\0";
  if (OB_FAIL(get_suspect_and_warning_info(dag, merge_type, ls_id, tablet_id, info, suspect_type, tmp_str, sizeof(tmp_str)))) {
    LOG_WARN("failed to get suspect and warning info", K(ret), K(ls_id), K(tablet_id));
  } else if (!info.is_valid()) {
    // do nothing
  } else if (MEDIUM_MERGE == merge_type) {
    if (OB_UNLIKELY(compaction_scn <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("merge version or freeze ts is invalid", K(ret), K(compaction_scn));
    } else {
      LOG_TRACE("diagnose with suspect info", K(ret), K(ls_id), K(tablet_id), K(merge_type));
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

  if (OB_SUCC(ret) && add_schedule_info && can_add_diagnose_info()) {
    if (suspect_tablet_count_[suspect_type] < DIAGNOSE_TABELT_MAX_COUNT) {
      if (OB_FAIL(SET_DIAGNOSE_INFO(
            info_array_[idx_++],
            merge_type,
            MTL_ID(),
            ls_id,
            tablet_id,
            ObCompactionDiagnoseInfo::DIA_STATUS_NOT_SCHEDULE,
            info.add_time_,
            "schedule_suspect_info", tmp_str))) {
        LOG_WARN("failed to add diagnose info", K(ret), K(ls_id), K(tablet_id), K(info));
      }
    }
    ++suspect_tablet_count_[suspect_type];
  }
  return ret;
}

int ObCompactionDiagnoseMgr::diagnose_medium_scn_table(const int64_t compaction_scn)
{
  int ret = OB_SUCCESS;
  int64_t error_tablet_cnt = MTL(ObTenantTabletScheduler*)->get_error_tablet_cnt();
  if (0 != error_tablet_cnt
      && can_add_diagnose_info()
      && OB_FAIL(SET_DIAGNOSE_INFO(
          info_array_[idx_++],
          MEDIUM_MERGE,
          MTL_ID(),
          ObLSID(INT64_MAX),
          ObTabletID(INT64_MAX),
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
