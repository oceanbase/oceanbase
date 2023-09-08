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
 *
 * Tenant Data Struct in OBCDC
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_tenant.h"

#include "lib/allocator/ob_malloc.h"                                // OB_NEW OB_DELETE
#include "share/inner_table/ob_inner_table_schema_constants.h"      // OB_ALL_DDL_OPERATION_TID

#include "ob_log_tenant_mgr.h"                                      // ObLogTenantMgr
#include "ob_log_instance.h"                                        // TCTX
#include "ob_log_config.h"                                          // TCONF
#include "ob_log_timezone_info_getter.h"                            // ObCDCTimeZoneInfoGetter

#include "ob_log_start_schema_matcher.h"                            // ObLogStartSchemaMatcher

#define STAT(level, tag_str, args...) OBLOG_LOG(level, "[STAT] [TENANT] " tag_str, ##args)
#define ISTAT(tag_str, args...) STAT(INFO, tag_str, ##args)
#define DSTAT(tag_str, args...) STAT(DEBUG, tag_str, ##args)

using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
namespace libobcdc
{
ObLogTenant::ObLogTenant() :
    inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    start_schema_version_(OB_INVALID_VERSION),
    task_queue_(NULL),
    ls_mgr_(*this),
    part_mgr_(*this),
    tenant_state_(),
    sys_ls_progress_(OB_INVALID_TIMESTAMP),
    ddl_log_lsn_(),
    all_ddl_operation_table_schema_info_(),
    drop_tenant_tstamp_(OB_INVALID_TIMESTAMP),
    global_seq_and_schema_version_(),
    committer_trans_commit_version_(OB_INVALID_VERSION),
    committer_global_heartbeat_(OB_INVALID_VERSION),
    committer_cur_schema_version_(OB_INVALID_VERSION),
    committer_next_trans_schema_version_(OB_INVALID_VERSION),
    cf_handle_(NULL),
    lob_storage_cf_handle_(nullptr),
    lob_storage_clean_task_()
{
  tenant_name_[0] = '\0';
  global_seq_and_schema_version_.lo = 0;
  global_seq_and_schema_version_.hi = 0;
}

ObLogTenant::~ObLogTenant()
{
  reset();
}

int ObLogTenant::init(
    const uint64_t tenant_id,
    const char *tenant_name,
    const int64_t start_tstamp_ns,
    const int64_t start_seq,
    const int64_t start_schema_version,
    void *cf_handle,
    void *lob_storage_cf_handle,
    ObLogTenantMgr &tenant_mgr)
{
  int ret = OB_SUCCESS;
  int64_t pos = 0;

  if (OB_UNLIKELY(inited_)) {
    LOG_ERROR("ObLogTenant has been initialized");
    ret = OB_INIT_TWICE;
  } else if (OB_UNLIKELY(OB_INVALID_TENANT_ID == tenant_id)
      || OB_UNLIKELY(start_tstamp_ns <= 0)
      || OB_UNLIKELY(start_seq < 0)
      || OB_UNLIKELY(start_schema_version <= 0)
      || OB_ISNULL(tenant_name)
      || OB_ISNULL(cf_handle)) {
    LOG_ERROR("invalid argument", K(tenant_id), K(tenant_name), K(start_tstamp_ns), K(start_seq),
        K(start_schema_version), K(cf_handle));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ISNULL(task_queue_ = OB_NEW(ObLogTenantTaskQueue, ObModIds::OB_LOG_TENANT_TASK_QUEUE, *this))) {
    LOG_ERROR("create task queue fail", K(task_queue_));
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(task_queue_->init(start_seq))) {
    LOG_ERROR("task_queue_ init fail", KR(ret), K(start_seq));
  } else if (OB_FAIL(ls_mgr_.init(tenant_id, start_schema_version, tenant_mgr.ls_info_map_,
      tenant_mgr.ls_add_cb_array_, tenant_mgr.ls_rc_cb_array_))) {
    LOG_ERROR("ls_mgr_ init fail", KR(ret), K(tenant_id_), K(start_schema_version));
  } else if (OB_FAIL(part_mgr_.init(tenant_id, start_schema_version, tenant_mgr.enable_oracle_mode_match_case_sensitive_,
      tenant_mgr.gindex_cache_, tenant_mgr.table_id_cache_))) {
      LOG_ERROR("part_mgr_ init fail", KR(ret), K(tenant_id), K(start_schema_version));
  } else if (OB_FAIL(databuff_printf(tenant_name_, sizeof(tenant_name_), pos, "%s", tenant_name))) {
    LOG_ERROR("print tenant name fail", KR(ret), K(pos), K(tenant_id), K(tenant_name));
  } else if (OB_FAIL(TCTX.timezone_info_getter_->init_tenant_tz_info(tenant_id))) {
    LOG_ERROR("fail to init tenant timezone info", KR(ret), K(tenant_id));
  } else if (OB_FAIL(init_all_ddl_operation_table_schema_info_())) {
    LOG_ERROR("init_all_ddl_operation_table_schema_info_ failed", KR(ret), K(tenant_id));
  }

  if (OB_SUCC(ret)) {
    tenant_id_ = tenant_id;
    start_schema_version_ = start_schema_version;

    // init to NORMAL state
    tenant_state_.reset(TENANT_STATE_NORMAL);

    // 1. When a transaction with the same timestamp as the start timestamp exists in the data partition and has not been sent,
    // the progress is fetched at this point, as the "task to be output timestamp-1" is fetched and the heartbeat may fall back
    // 2. so initialize the progress to start timestamp-1
    sys_ls_progress_ = start_tstamp_ns - 1;
    ddl_log_lsn_.reset();
    drop_tenant_tstamp_ = OB_INVALID_TIMESTAMP;

    global_seq_and_schema_version_.lo = start_seq;
    global_seq_and_schema_version_.hi = start_schema_version;

    // NOTE: is safe if obcdc only serve one tenant, in OB4.0, tenant gts is isolated from each
    // other, then trans_commit_version is not comparable with other tenant, which means currently
    // we recommand one libobcdc instance serve one tenant and filter sys_tenant ddl.
    committer_trans_commit_version_ = start_tstamp_ns - 1;
    committer_global_heartbeat_ = OB_INVALID_VERSION;
    committer_cur_schema_version_ = start_schema_version;
    committer_next_trans_schema_version_ = start_schema_version;
    cf_handle_ = cf_handle;
    lob_storage_cf_handle_ = lob_storage_cf_handle;
    lob_storage_clean_task_.tenant_id_ = tenant_id;

    inited_ = true;

    LOG_INFO("init tenant succ", K(tenant_id), K(tenant_name), K(start_schema_version),
        K(start_tstamp_ns), K(start_seq));
  }

  return ret;
}

int ObLogTenant::init_all_ddl_operation_table_schema_info_()
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(all_ddl_operation_table_schema_info_.init())) {
    LOG_ERROR("all_ddl_operation_table_schema_info_ init failed", KR(ret));
  }

  return ret;
}

void ObLogTenant::reset()
{
  if (inited_) {
    LOG_INFO("destroy tenant", K_(tenant_id), K_(tenant_name), K_(start_schema_version));
  }

  inited_ = false;
  uint64_t tenant_id = tenant_id_;
  tenant_id_ = OB_INVALID_TENANT_ID;
  tenant_name_[0] = '\0';

  start_schema_version_ = OB_INVALID_VERSION;

  if (NULL != task_queue_) {
    task_queue_->reset();
    OB_DELETE(ObLogTenantTaskQueue, unused, task_queue_);
    task_queue_ = NULL;
  }

  ls_mgr_.reset();
  part_mgr_.reset();

  tenant_state_.reset();

  sys_ls_progress_ = OB_INVALID_TIMESTAMP;
  ddl_log_lsn_.reset();
  all_ddl_operation_table_schema_info_.reset();
  drop_tenant_tstamp_ = OB_INVALID_TIMESTAMP;

  global_seq_and_schema_version_.lo = 0;
  global_seq_and_schema_version_.hi = 0;
  committer_trans_commit_version_ = OB_INVALID_VERSION;
  committer_global_heartbeat_ = OB_INVALID_VERSION;
  committer_cur_schema_version_ = OB_INVALID_VERSION;
  committer_next_trans_schema_version_ = OB_INVALID_VERSION;
  cf_handle_ = NULL;
  lob_storage_cf_handle_ = nullptr;
  lob_storage_clean_task_.reset();
  ObMallocAllocator::get_instance()->recycle_tenant_allocator(tenant_id);
}

int ObLogTenant::alloc_global_trans_seq_and_schema_version_for_ddl(
    const int64_t base_schema_version,
    int64_t &new_seq,
    int64_t &new_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  int64_t end_time = get_timestamp() + timeout;

  while (OB_SUCC(ret)) {
    types::uint128_t old_v;
    types::uint128_t new_v;

    LOAD128(old_v, &global_seq_and_schema_version_);

    // Note: DDLs do not take up global serial numbers
    // Only the global Schema version number will be affected
    new_v.lo = old_v.lo;

    // Use int64_t to compare, use uint64_t to assign values
    new_v.hi =
        (static_cast<int64_t>(old_v.hi) < base_schema_version) ?
        static_cast<uint64_t>(base_schema_version) : old_v.hi;

    if (CAS128(&global_seq_and_schema_version_, old_v, new_v)) {
      new_seq = static_cast<int64_t>(new_v.lo);
      new_schema_version = static_cast<int64_t>(new_v.hi);

      int64_t old_seq = static_cast<int64_t>(old_v.lo);
      int64_t old_schema_version = static_cast<int64_t>(old_v.hi);

      LOG_DEBUG("Tenant alloc_global_trans_seq_and_schema_version_for_ddl", K(tenant_id_),
          K(new_seq), K(new_schema_version), K(old_seq), K(old_schema_version));
      break;
    }

    PAUSE();

    if (end_time <= get_timestamp()) {
      ret = OB_TIMEOUT;
      break;
    }
  }

  return ret;
}

int ObLogTenant::alloc_global_trans_seq_and_schema_version(const int64_t base_schema_version,
    int64_t &new_seq,
    int64_t &new_schema_version,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  while (! stop_flag) {
    types::uint128_t old_v;
    types::uint128_t new_v;

    LOAD128(old_v, &global_seq_and_schema_version_);

    new_v.lo = old_v.lo + 1;

    // Use int64_t to compare, use uint64_t to assign values
    new_v.hi =
        (static_cast<int64_t>(old_v.hi) < base_schema_version) ?
        static_cast<uint64_t>(base_schema_version) : old_v.hi;

    if (CAS128(&global_seq_and_schema_version_, old_v, new_v)) {
      new_seq = static_cast<int64_t>(new_v.lo) - 1;
      new_schema_version = static_cast<int64_t>(new_v.hi);
      LOG_DEBUG("ObLogTenant alloc_global_trans_seq_and_schema_version",
          K(new_seq), K(new_schema_version));

      int64_t old_seq = static_cast<int64_t>(old_v.lo);
      int64_t old_schema_version = static_cast<int64_t>(old_v.hi);

      LOG_DEBUG("alloc_global_trans_seq_and_schema_version_for_dml", K(tenant_id_),
          K(new_seq), K(new_schema_version), K(old_seq), K(old_schema_version));
      break;
    }
  }

  if (stop_flag) {
    ret = OB_IN_STOP_STATE;
  }

  return ret;
}

int ObLogTenant::alloc_global_trans_schema_version(const bool is_ddl_trans,
    const int64_t base_schema_version,
    int64_t &new_schema_version)
{
  int ret = OB_SUCCESS;
  int64_t cur_schema_version = global_seq_and_schema_version_.hi;

  if (is_ddl_trans) {
    int64_t max_schema_version = std::max(cur_schema_version, base_schema_version);
    global_seq_and_schema_version_.hi = max_schema_version;
    new_schema_version = max_schema_version;
    LOG_DEBUG("ObLogTenant alloc_global_trans_seq_and_schema_version", K(cur_schema_version),
        K(base_schema_version), K(new_schema_version));
  } else {
    new_schema_version = cur_schema_version;
  }

  return ret;
}

int ObLogTenant::drop_tenant(bool &tenant_can_be_dropped, const char *call_from)
{
  int ret = OB_SUCCESS;
  tenant_can_be_dropped = false;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else {
    ISTAT("[DROP_TENANT] BEGIN", K_(tenant_id), K_(tenant_name),
        "state", print_state(get_tenant_state()),
        "active_ls_count", get_active_ls_count(), K(call_from));

    // If the tenant is already OFFLINE, it is no longer necessary to drop
    if (TENANT_STATE_OFFLINE == get_tenant_state()) {
      ISTAT("[DROP_TENANT] END: tenant is dropped twice", "tenant", *this, K(call_from));
    } else if (OB_FAIL(ls_mgr_.drop_all_ls())) {
      LOG_ERROR("LSMgr drop_all_ls fail", KR(ret), KPC(this));
    } else {
      int64_t old_state = TENANT_STATE_INVALID;
      int64_t ref_cnt = 0;
      // Status changed to OFFLINE, returning old status and reference count
      if (tenant_state_.change_state(TENANT_STATE_OFFLINE, old_state, ref_cnt)) {
        tenant_can_be_dropped = (0 == ref_cnt);
      } else {
        LOG_INFO("tenant has been in offline state", KPC(this));
      }

      ISTAT("[DROP_TENANT] END",  K_(tenant_id), K_(tenant_name),
          K(tenant_can_be_dropped),
          "old_state", print_state(old_state),
          "cur_part_count", ref_cnt,
          K(call_from));
    }
  }
  return ret;
}

// Note: This interface and mark_drop_tenant_start() must be called serially, otherwise there are correctness issues
int ObLogTenant::update_sys_ls_info(const PartTransTask &task)
{
  int ret = OB_SUCCESS;
  // Progress information is required for all types of tasks
  const int64_t handle_progress = task.get_prepare_ts();
  // Invalid prepare log id and schema version for DDL heartbeat task,
  // ignored when checking parameters
  const palf::LSN &handle_log_lsn = task.get_prepare_log_lsn();
  const int64_t ddl_schema_version = task.get_local_schema_version();

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("not init", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! is_serving())) {
    // DDL progress information is not updated if the tenant is not in service
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_INFO("tenant state is not serving, need not update DDL info", KPC(this),
          K(handle_progress), K(handle_log_lsn), K(ddl_schema_version));
    }
  }
  // Only heartbeat and DDL transaction tasks are allowed
  else if (OB_UNLIKELY(! task.is_ddl_trans()
        && ! task.is_ls_op_trans()
        && ! task.is_sys_ls_heartbeat())) {
    ret = OB_NOT_SUPPORTED;
    LOG_ERROR("task is not DDL trans task, or LS Table, or HEARTBEAT, not supported", KR(ret), K(task));
  } else if (OB_UNLIKELY(handle_progress <= 0)) {
    // Progress must be effective
    LOG_ERROR("invalid argument", K(handle_progress), K(task));
    ret = OB_INVALID_ARGUMENT;
  }
  // Update the schema version if the schema version is valid
  else if (ddl_schema_version > 0
      // && ddl_schema_version > part_mgr_.get_schema_version()
      && OB_FAIL(part_mgr_.update_schema_version(ddl_schema_version))) {
    LOG_ERROR("part mgr update schema version fail", KR(ret), K(ddl_schema_version), K(task));
  }
  // update the progress for all types of tasks
  // Note: for DDL heartbeat tasks, handle_log_id is not valid
  else if (OB_FAIL(update_sys_ls_progress_(handle_progress, handle_log_lsn))) {
    LOG_ERROR("update ddl progress fail", KR(ret), K(handle_progress), K(handle_log_lsn), K(task));
  } else {
    // success
  }

  return ret;
}

int ObLogTenant::update_sys_ls_progress_(
    const int64_t handle_progress,
    const palf::LSN &handle_log_lsn)
{
  int ret = OB_SUCCESS;
  const int64_t old_handle_progress = ATOMIC_LOAD(&sys_ls_progress_);

  // Note: It is important here to ensure that the DDL progress does not fall back, otherwise it will cause the heartbeat progress to fall back and exit with an error.
  // The actual __all_ddl_operation of the new tenant will pull in DDL transactions with a timestamp less than or equal to the start-up timestamp, and the update DDL progress should be guaranteed to increment
  if (OB_INVALID_TIMESTAMP == sys_ls_progress_ || handle_progress > sys_ls_progress_) {
    ATOMIC_STORE(&sys_ls_progress_, handle_progress);

    // Note: It is possible that the handle_log_id passed in is an invalid value.
    if (handle_log_lsn.is_valid()) {
      ddl_log_lsn_ = handle_log_lsn;
    }

    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_DEBUG("update sys_ls_progress", K_(sys_ls_progress), K_(ddl_log_lsn), K(handle_log_lsn), K_(tenant_id));
    }
  }

  // Check if the progress value is greater than drop_tenant_tstamp_ for the first time
  if (OB_INVALID_TIMESTAMP != drop_tenant_tstamp_
      && old_handle_progress < drop_tenant_tstamp_
      && handle_progress >= drop_tenant_tstamp_) {
    bool need_drop_tenant = false;

    LOG_INFO("DDL progress is beyond drop_tenant_tstamp while updating ddl progress, "
        "start to drop tenant",
        K_(tenant_id), K_(drop_tenant_tstamp), K(old_handle_progress), K(handle_progress),
        "delta", drop_tenant_tstamp_ - handle_progress, K(handle_log_lsn),
        "state", print_state(get_tenant_state()),
        "active_ls_count", get_active_ls_count());

    if (OB_FAIL(start_drop_tenant_if_needed_(need_drop_tenant))) {
      LOG_ERROR("start_drop_tenant_if_needed_ fail", KR(ret), K(tenant_id_),
          K(drop_tenant_tstamp_), K(old_handle_progress), K(handle_progress));
    }
  }

  return ret;
}

// This interface is called when processing a deleted tenant DDL for a SYS tenant
// It is currently assumed that DDLs for all tenants are processed serially, if multiple tenants are processed in parallel there are concurrency issues here
int ObLogTenant::mark_drop_tenant_start(const int64_t drop_tenant_start_tstamp)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("tenant has not been initialized", K(inited_));
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(drop_tenant_start_tstamp <= 0)) {
    LOG_ERROR("invalid argument", K(drop_tenant_start_tstamp));
    ret = OB_INVALID_ARGUMENT;
  }
  // If it is already offline, it no longer needs to be processed
  else if (is_offlined()) {
    LOG_INFO("[DROP_TENANT] tenant has been offlined, need not mark drop tenant start", KPC(this),
        K(drop_tenant_start_tstamp));
  } else if (OB_UNLIKELY(OB_INVALID_TIMESTAMP != drop_tenant_tstamp_)) {
    LOG_ERROR("invalid drop_tenant_tstamp_ which should be invalid", K(drop_tenant_tstamp_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    // Mark the timestamp of the deleted tenant
    drop_tenant_tstamp_ = drop_tenant_start_tstamp;
    bool need_drop_tenant = false;

    if (OB_FAIL(start_drop_tenant_if_needed_(need_drop_tenant))) {
      LOG_ERROR("start_drop_tenant_if_needed_ fail", KR(ret), K(tenant_id_),
          K(drop_tenant_start_tstamp));
    }

    ISTAT("[DROP_TENANT] mark drop tenant start", KR(ret), K_(tenant_id), K(need_drop_tenant),
        K(drop_tenant_start_tstamp), K(sys_ls_progress_),
        "delta", drop_tenant_start_tstamp - sys_ls_progress_,
        "state", print_state(get_tenant_state()),
        "active_ls_count", get_active_ls_count());
  }
  return ret;
}

bool ObLogTenant::need_drop_tenant_() const
{
  // A tenant can only be deleted if the DDL processing progress is greater than the timestamp of the deleted tenant
  return (OB_INVALID_TIMESTAMP != drop_tenant_tstamp_ && sys_ls_progress_ >= drop_tenant_tstamp_);
}

int ObLogTenant::start_drop_tenant_if_needed_(bool &need_drop_tenant)
{
  int ret = OB_SUCCESS;
  need_drop_tenant = need_drop_tenant_();

  if (need_drop_tenant) {
    ISTAT("[DROP_TENANT] need_drop_tenant, begin drop DDL partition",
        K_(tenant_id), K_(tenant_name), K_(drop_tenant_tstamp), K_(sys_ls_progress),
        "delta", drop_tenant_tstamp_ - sys_ls_progress_,
        "state", print_state(get_tenant_state()),
        "active_ls_count", get_active_ls_count());

    // Delete the DDL partition if the tenant can be deleted, and let the DDL partition offline task trigger the deletion of the tenant
    if (OB_FAIL(drop_sys_ls_())) {
      LOG_ERROR("drop_sys_ls_ fail", KR(ret), K(tenant_id_));
    }
  }
  return ret;
}

int ObLogTenant::drop_sys_ls_()
{
  int ret = OB_SUCCESS;
  logservice::TenantLSID sys_ls(tenant_id_, share::SYS_LS);

  if (OB_FAIL(ls_mgr_.offline_ls(sys_ls))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_INFO("SYS LS has been offlined", KR(ret), K(sys_ls), K(tenant_id_));
      ret = OB_SUCCESS;
    } else {
      LOG_ERROR("ObLogLSMgr offline_ls fail", KR(ret), K(sys_ls));
    }
  }
  return ret;
}

const char *ObLogTenant::print_state(const int64_t state)
{
  const char *ret = nullptr;

  switch (state) {
    case TENANT_STATE_INVALID: {
      ret = "INVALID";
      break;
    }
    case TENANT_STATE_NORMAL: {
      ret = "NORMAL";
      break;
    }
    case TENANT_STATE_OFFLINE: {
      ret = "OFFLINE";
      break;
    }
    default: {
      ret = "UNKNOWN";
      break;
    }
  }

  return ret;
}

int ObLogTenant::inc_ls_count_on_serving(const logservice::TenantLSID &tls_id, bool &is_serving)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenant has not been initialized", K(inited_));
    ret = OB_NOT_INIT;
  } else {
    is_serving = false;

    // Only the NORMAL state can increase the reference count
    // If the tenant has been deleted, the add partition operation cannot be executed again
    int64_t target_state = TENANT_STATE_NORMAL;
    int64_t new_state = TENANT_STATE_INVALID;
    int64_t new_ref = 0;

    // Increase reference count, return latest status and reference count
    if (! tenant_state_.inc_ref(target_state, new_state, new_ref)) {
      is_serving = false;
    } else {
      is_serving = true;
    }

    ISTAT("[INC_PART_COUNT_ON_SERVING]",
        K_(tenant_id),
        K_(tenant_name),
        K(is_serving),
        "cur_state", print_state(new_state),
        "cur_part_count", new_ref,
        K(tls_id));
  }
  return ret;
}

int ObLogTenant::recycle_ls(const logservice::TenantLSID &tls_id, bool &tenant_can_be_dropped)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenant has not been initialized", K(inited_));
    ret = OB_NOT_INIT;
  } else {
    // First LSMgr reclaims the LS
    if (OB_FAIL(ls_mgr_.offline_and_recycle_ls(tls_id))) {
      if (OB_ENTRY_NOT_EXIST == ret) {
        // Partition does not exist, normal
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("ObLogLSMgr offline_and_recycle_ls fail", KR(ret), K(tls_id), K(tenant_id_));
      }
    }

    // Tenant structure minus reference count
    if (OB_SUCCESS == ret) {
      int64_t new_state = TENANT_STATE_INVALID;
      int64_t new_ref = 0;

      // Return the latest status and reference count value
      tenant_state_.dec_ref(new_state, new_ref);

      // The reference count cannot be 0, otherwise there is a bug
      if (OB_UNLIKELY(new_ref < 0)) {
        LOG_ERROR("tenant reference count is invalid after dec_ref()", K(new_ref), K(new_state),
            KPC(this));
        ret = OB_ERR_UNEXPECTED;
      } else {
        // Can a tenant  drop
        tenant_can_be_dropped = (TENANT_STATE_OFFLINE == new_state && 0 == new_ref);
      }

      ISTAT("[RECYCLE_LS]",
          K_(tenant_id),
          K_(tenant_name),
          "cur_state", print_state(new_state),
          "cur_part_count", new_ref,
          K(tenant_can_be_dropped),
          K(tls_id));
    }
  }

  return ret;
}

int ObLogTenant::update_committer_trans_commit_version(const int64_t trans_commit_version)
{
  int ret = OB_SUCCESS;
  const int64_t cur_trans_commit_version = ATOMIC_LOAD(&committer_trans_commit_version_);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenant not inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(! is_serving())) {
    LOG_INFO("won't update_committer_trans_commit_version for not serving tenant",
        KR(ret), K_(tenant_id), K(trans_commit_version));
  } else if (OB_UNLIKELY(trans_commit_version < cur_trans_commit_version)) {
    // trans_commit_version should advance forward(should not be rollback and may be has same
    // commit_version for different trans);
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("trans_commit_version rollbacked or not advanced", KR(ret),
        K(cur_trans_commit_version), "target_trans_commit_version", trans_commit_version);
  } else {
    ATOMIC_STORE(&committer_trans_commit_version_, trans_commit_version);
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_INFO("update tenant trans_commit_version",
          K_(tenant_id),
          K(cur_trans_commit_version),
          "target_trans_commit_version", trans_commit_version,
          "delay", NTS_TO_DELAY(trans_commit_version));
    } else {
      LOG_DEBUG("update tenant trans_commit_version",
          K_(tenant_id),
          K(cur_trans_commit_version),
          "target_trans_commit_version", trans_commit_version,
          "delay", NTS_TO_DELAY(trans_commit_version));
    }
  }

  return ret;
}

int ObLogTenant::update_committer_global_heartbeat(const int64_t global_heartbeat)
{
  int ret = OB_SUCCESS;
  const int64_t cur_global_heartbeat = ATOMIC_LOAD(&committer_global_heartbeat_);

  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogTenant not inited", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(! is_serving())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("update_committer_global_heartbeat for not serving tenant", KR(ret), KPC(this));
  } else if (OB_UNLIKELY(OB_INVALID_VERSION == cur_global_heartbeat && global_heartbeat < cur_global_heartbeat)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("global_heartbeat rollbacked", KR(ret),
        K(cur_global_heartbeat), "target_global_heartbeat", global_heartbeat);
  } else {
    ATOMIC_STORE(&committer_global_heartbeat_, global_heartbeat);
    if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
      LOG_INFO("update tenant global_heartbeat",
          K_(tenant_id),
          K(cur_global_heartbeat),
          "target_global_heartbeat", global_heartbeat,
          "delay", NTS_TO_DELAY(global_heartbeat));
    } else {
      LOG_DEBUG("update tenant global_heartbeat",
          K_(tenant_id),
          K(cur_global_heartbeat),
          "target_global_heartbeat", global_heartbeat,
          "delay", NTS_TO_DELAY(global_heartbeat));
    }
  }

  return ret;
}

int ObLogTenant::update_committer_next_trans_schema_version(int64_t schema_version)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenant has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(OB_INVALID_VERSION == schema_version)) {
    LOG_ERROR("invalid argument", K(schema_version));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_UNLIKELY(schema_version < ATOMIC_LOAD(&committer_next_trans_schema_version_))) {
    LOG_ERROR("global schema version reversed, unexpected", K(schema_version),
        K(committer_next_trans_schema_version_));
    ret = OB_ERR_UNEXPECTED;
  } else {
    ATOMIC_STORE(&committer_next_trans_schema_version_, schema_version);
  }

  return ret;
}

void ObLogTenant::print_stat_info()
{
  int64_t serving_ls_count = 0, offline_ls_count = 0, not_served_ls_count = 0;

  if (inited_) {
    // TODO modify
    const uint64_t ddl_table_id = OB_ALL_DDL_OPERATION_TID;

    // First call PartMgr to print the partition information and return the number of partitions served and the number of downstream partitions
    ls_mgr_.print_ls_info(serving_ls_count, offline_ls_count, not_served_ls_count);

    _LOG_INFO("[SERVE_INFO] TENANT=%lu(%s) STATE=%s(%ld) "
        "LS_COUNT(SERVE=%ld,OFFLINE=%ld,NOT_SERVE=%ld,ACTIVE=%ld) "
        "DDL_PROGRESS=%s DELAY=%s DDL_LOG_LSN=%lu "
        "QUEUE(DML=%ld) "
        "SEQ(GB=%ld,CMT=%ld) "
        "SCHEMA(GB=%ld,CUR=%ld) "
        "CMT_SCHEMA(CUR=%ld,NEXT=%ld) "
        "CHECKPOINT(TX=%ld(%s), GHB=%ld(%s)) "
        "DROP_TS=%s "
        "DDL_TABLE=%lu",
        tenant_id_, tenant_name_, print_state(get_tenant_state()), get_tenant_state(),
        serving_ls_count, offline_ls_count, not_served_ls_count, get_active_ls_count(),
        NTS_TO_STR(sys_ls_progress_), NTS_TO_DELAY(sys_ls_progress_), ddl_log_lsn_.val_,
        NULL == task_queue_ ? 0 : task_queue_->get_log_entry_task_count(),
        get_global_seq(), NULL == task_queue_ ? 0 : task_queue_->get_next_task_seq(),
        get_global_schema_version(), get_schema_version(),
        committer_cur_schema_version_, committer_next_trans_schema_version_,
        committer_trans_commit_version_, NTS_TO_STR(committer_trans_commit_version_),
        committer_global_heartbeat_, NTS_TO_STR(committer_global_heartbeat_),
        NTS_TO_STR(drop_tenant_tstamp_),
        ddl_table_id);
  }
}

int ObLogTenant::add_sys_ls(
    const int64_t start_tstamp_ns,
    const int64_t ddl_table_start_schema_version,
    const bool is_create_tenant)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenant has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! is_serving())) {
    LOG_ERROR("tenant is not serving", KPC(this));
    ret = OB_STATE_NOT_MATCH;
  } else if (OB_FAIL(ls_mgr_.add_sys_ls(start_tstamp_ns, ddl_table_start_schema_version,
      is_create_tenant))) {
    LOG_ERROR("LSMgr add_sys_ls fail", KR(ret), K(start_tstamp_ns),
        K(ddl_table_start_schema_version), K(is_create_tenant));
  } else {
    ISTAT("[ADD_DDL_TABLE] update tenant schema version after add ddl table", K_(tenant_id),
        //"cur_schema_version", part_mgr_.get_schema_version(),
        K_(start_schema_version), K(ddl_table_start_schema_version),
        K(start_tstamp_ns), K(is_create_tenant));

    if (OB_SUCCESS == ret) {
      // The starting schema version should also be updated to the DDL starting schema version, otherwise the DDL partition
      // will pull in a minor version of the schema operation when it is started, which will cause the schema version to be rolled back
      start_schema_version_ = ddl_table_start_schema_version;
    }
  }
  return ret;
}

int ObLogTenant::add_all_ls(
    const common::ObIArray<share::ObLSID> &ls_id_array,
    const int64_t start_tstamp_ns,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTenant has not been initialized");
    ret = OB_NOT_INIT;
  } else if (OB_UNLIKELY(! is_serving())) {
    ret = OB_STATE_NOT_MATCH;
    LOG_ERROR("tenant is not serving", KR(ret), KPC(this));
  } else {
    ret = ls_mgr_.add_all_ls(ls_id_array, start_tstamp_ns, start_schema_version, timeout);
  }

  return ret;
}

//////////////////////////// ObLogTenantGuard /////////////////////////
void ObLogTenantGuard::revert_tenant()
{
  IObLogTenantMgr *tenant_mgr = TCTX.tenant_mgr_;

  if (OB_NOT_NULL(tenant_) && OB_NOT_NULL(tenant_mgr)) {
    int revert_ret = tenant_mgr->revert_tenant(tenant_);
    if (OB_SUCCESS != revert_ret) {
      LOG_ERROR_RET(revert_ret, "revert ObLogTenant fail", K(revert_ret), KPC(tenant_));
    } else {
      tenant_ = NULL;
    }
  }
}

void ObLogTenant::update_global_data_schema_version(const int64_t data_start_schema_version)
{
  int64_t start_schema_version = global_seq_and_schema_version_.hi;
  global_seq_and_schema_version_.hi = std::max(start_schema_version, data_start_schema_version);

  LOG_INFO("set_data_start_schema_version succ", K_(tenant_id),
      K(start_schema_version), K(data_start_schema_version),
      "global_seq", global_seq_and_schema_version_.lo,
      "global_schema_version", global_seq_and_schema_version_.hi);
}

// This function only works in tenant split mode and is not responsible for checking if the mode is split or not
// This function updates start_schema_version only if data_start_schema_version is set correctly
int ObLogTenant::update_data_start_schema_version_on_split_mode()
{
  int ret = OB_SUCCESS;
  bool match = false;
  int64_t schema_version = 0;
  int64_t old_data_schema_version = global_seq_and_schema_version_.hi;
  IObLogStartSchemaMatcher *schema_matcher = TCTX.ss_matcher_;

  if (OB_ISNULL(schema_matcher)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("schema version matcher is NULL", KR(ret), K(schema_matcher));
  } else if (OB_FAIL(schema_matcher->match_data_start_schema_version(tenant_id_,
          match,
          schema_version))) {
    LOG_ERROR("match_data_start_schema_version failed",
        KR(ret), K(tenant_id_), K(match), K(schema_version));
  } else if (match) {
    global_seq_and_schema_version_.hi = std::max(schema_version, old_data_schema_version);
  } else {
    // No specified tenant found, original schema version used
  }

  if (OB_SUCC(ret) && match) {
    LOG_INFO("[UPDATE_START_SCHEMA] update_data_start_schema_version_on_split_mode succ", KR(ret),
        K(tenant_id_), K(schema_version), K(global_seq_and_schema_version_.hi),
        K(old_data_schema_version));
  }

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

#undef STAT
#undef ISTAT
#undef DSTAT
