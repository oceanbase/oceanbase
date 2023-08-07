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

#define USING_LOG_PREFIX OBLOG
#include "ob_log_ls_mgr.h"
#include "ob_log_config.h"                            // TCONF
#include "ob_log_tenant.h"                            // ObLogTenant

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[STAT] [LSMgr] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[STAT] [LSMgr] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define _DSTAT(fmt, args...) _STAT(DEBUG, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

#define REVERT_LS_INFO(info, ret) \
    do { \
      if (NULL != info && NULL != map_) { \
        int revert_ret = map_->revert(info); \
        if (OB_SUCCESS != revert_ret) { \
          LOG_ERROR("revert LSInfo fail", K(revert_ret), K(info)); \
          ret = OB_SUCCESS == ret ? revert_ret : ret; \
        } else { \
          info = NULL; \
        } \
      } \
    } while (0)


namespace oceanbase
{
namespace libobcdc
{
ObLogLSMgr::ObLogLSMgr(ObLogTenant &tenant) : host_(tenant)
{
  reset();
}

ObLogLSMgr::~ObLogLSMgr()
{
  reset();
}

int ObLogLSMgr::init(const uint64_t tenant_id,
    const int64_t start_schema_version,
    LSInfoMap &map,
    LSCBArray &ls_add_cb_array,
    LSCBArray &ls_rc_cb_array)
{
  int ret = OB_SUCCESS;

  if (OB_FAIL(schema_cond_.init(common::ObWaitEventIds::OBCDC_PART_MGR_SCHEMA_VERSION_WAIT))) {
    LOG_ERROR("schema_cond_ init fail", KR(ret));
  } else {
    tenant_id_ = tenant_id;
    map_ = &map;
    ls_add_cb_array_ = &ls_add_cb_array;
    ls_rc_cb_array_ = &ls_rc_cb_array;

    is_inited_ = true;
    LOG_INFO("init LSMgr succ", K(tenant_id), K(start_schema_version));
  }

  return ret;
}

void ObLogLSMgr::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_ID;
  map_ = NULL;
  ls_add_cb_array_ = NULL;
  ls_rc_cb_array_ = NULL;
  schema_cond_.destroy();
}

int ObLogLSMgr::add_sys_ls(
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const bool is_create_tenant)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else {
    bool add_succ = false;
    logservice::TenantLSID tls_id(tenant_id_, share::SYS_LS);

    if (OB_FAIL(add_served_ls_(
        tls_id,
        start_serve_tstamp,
        is_create_tenant,
        add_succ))) {
      LOG_ERROR("add_served_ls_ fail", KR(ret), K(tls_id), K(start_serve_tstamp),
          K(is_create_tenant), K(add_succ));
    } else if (add_succ) {
      ISTAT("[ADD_SYS_LS]", K_(tenant_id), K(tls_id),
          K(start_schema_version),
          K(start_serve_tstamp), K(is_create_tenant));
    } else {
      LOG_ERROR("DDL ls add fail", K_(tenant_id), K(tls_id),
          K(start_schema_version), K(start_serve_tstamp), K(is_create_tenant));
      ret = OB_ERR_UNEXPECTED;
    }
  }

  return ret;
}

int ObLogLSMgr::add_all_ls(
    const common::ObIArray<share::ObLSID> &ls_id_array,
    const int64_t start_serve_tstamp,
    const int64_t start_schema_version,
    const int64_t timeout)
{
  int ret = OB_SUCCESS;
  const int64_t served_ls_count_before = NULL != map_ ? map_->get_valid_count() : 0;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(start_serve_tstamp <= 0)
      || OB_UNLIKELY(start_schema_version <= 0)) {
    LOG_ERROR("invalid argument", K(start_serve_tstamp), K(start_schema_version));
    ret = OB_INVALID_ARGUMENT;
  } else {
    bool add_succ = false;
    const bool is_create_ls = false;

    ARRAY_FOREACH_N(ls_id_array, idx, count) {
      const share::ObLSID &ls_id = ls_id_array.at(idx);
      logservice::TenantLSID tls_id(tenant_id_, ls_id);

      if (OB_FAIL(add_served_ls_(
          tls_id,
          start_serve_tstamp,
          is_create_ls,
          add_succ))) {
        LOG_ERROR("add_served_ls_ fail", KR(ret), K(tls_id), K(start_serve_tstamp),
            K(is_create_ls), K(add_succ));
      } else if (add_succ) {
        ISTAT("[ADD_LS]", K_(tenant_id), K(tls_id),
            K(start_schema_version),
            K(start_serve_tstamp), K(is_create_ls));
      } else {}
    }

    const int64_t total_served_ls_count = map_->get_valid_count();
    ISTAT("[ADD_ALL_USED_LS]", K_(tenant_id),
        K(start_serve_tstamp),
        K(start_schema_version),
        "tenant_served_ls_count", total_served_ls_count - served_ls_count_before,
        K(total_served_ls_count));
  }

  return ret;
}

int ObLogLSMgr::drop_all_ls()
{
  int ret = OB_SUCCESS;
  LSInfoScannerByTenant scanner(tenant_id_);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  }
  // Iterate through the partitions being served by the current tenant
  else if (OB_FAIL(map_->for_each(scanner))) {
    LOG_ERROR("scan map fail", KR(ret), K(tenant_id_));
  } else {
    _ISTAT("[DDL] [DROP_TENANT] [DROP_ALL_TABLES] [BEGIN] TENANT=%ld TENANT_PART_COUNT=%ld "
        "TOTAL_PART_COUNT=%ld",
        tenant_id_, scanner.ls_array_.count(), map_->get_valid_count());

    for (int64_t index = 0; OB_SUCCESS == ret && index < scanner.ls_array_.count(); index++) {
      const logservice::TenantLSID &tls_id = scanner.ls_array_.at(index);
      ret = offline_ls_(tls_id);

      if (OB_ENTRY_NOT_EXIST == ret) {
        DSTAT("[DDL] [DROP_TENANT] partition not served", K(tls_id));
        ret = OB_SUCCESS;
      } else if (OB_SUCCESS != ret) {
        LOG_ERROR("offline partition fail", KR(ret), K(tls_id));
      } else {
        // succ
      }
    }

    _ISTAT("[DDL] [DROP_TENANT] [DROP_ALL_TABLES] [END] TENANT=%ld TOTAL_PART_COUNT=%ld",
        tenant_id_, map_->get_valid_count());
  }

  return ret;
}

int ObLogLSMgr::add_ls(
    const logservice::TenantLSID &tls_id,
    const int64_t start_serve_tstamp,
    const bool is_create_ls)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else {
    bool add_succ = false;

    if (OB_FAIL(add_served_ls_(
        tls_id,
        start_serve_tstamp,
        is_create_ls,
        add_succ))) {
      LOG_ERROR("add_served_ls_ fail", KR(ret), K(tls_id), K(start_serve_tstamp),
          K(is_create_ls), K(add_succ));
    } else if (add_succ) {
      ISTAT("[ADD_LS]", K_(tenant_id), K(tls_id),
          K(start_serve_tstamp), K(is_create_ls));
    } else {}
  }

  return ret;
}

int ObLogLSMgr::add_served_ls_(
    const logservice::TenantLSID &tls_id,
    const int64_t start_serve_tstamp,
    const bool is_create_ls,
    bool &add_succ)
{
  int ret = OB_SUCCESS;
  add_succ = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else {
    // Check if the LS is serviced
    bool is_served = is_ls_served_(tls_id);

    if (OB_FAIL(add_ls_(tls_id, start_serve_tstamp, is_create_ls, is_served))) {
      if (OB_ENTRY_EXIST == ret) {
        // ls already exist
        ret = OB_SUCCESS;
      } else {
        LOG_ERROR("add_ls_ fail", KR(ret), K(tls_id), K(start_serve_tstamp),
            K(is_create_ls), K(is_served));
      }
    } else {
      if (is_served) {
        add_succ = true;
      }
    }
  }

  return ret;
}

bool ObLogLSMgr::is_ls_served_(
    const logservice::TenantLSID &tls_id) const
{
  bool bool_ret = false;

  if (IS_NOT_INIT) {
    bool_ret = false;
  }
  // SYS LS must serve
  else if (tls_id.get_ls_id().is_sys_ls()) {
    bool_ret = true;
  } else {
    uint64_t hash_v = 0;

    hash_v = tls_id.get_ls_id().hash();

    bool_ret = ((hash_v % TCONF.instance_num) == TCONF.instance_index);
  }

  return bool_ret;
}

int ObLogLSMgr::add_ls_(
    const logservice::TenantLSID &tls_id,
    const int64_t start_serve_tstamp,
    const bool is_create_ls,
    const bool is_served)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else if (start_serve_tstamp <= 0) {
    LOG_ERROR("invalid argument", K(start_serve_tstamp));
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_ENTRY_EXIST == (ret = map_->contains_key(tls_id))) {
    // add repeatly
    LOG_INFO("ls has been added", KR(ret), K_(tenant_id), K(tls_id), K(start_serve_tstamp),
        K(is_create_ls), K(is_served));
  } else if (OB_UNLIKELY(OB_ENTRY_NOT_EXIST != ret)) {
    LOG_ERROR("check ls exist fail", KR(ret), K(tls_id), K(start_serve_tstamp),
        K(is_create_ls));
    ret = OB_SUCCESS == ret ? OB_ERR_UNEXPECTED : ret;
  } else {
    // If the element does not exist, continue adding
    ret = OB_SUCCESS;

    ObLogLSInfo *info = NULL;
    palf::LSN start_lsn = is_create_ls ? palf::LSN(palf::PALF_INITIAL_LSN_VAL) : palf::LSN(palf::LOG_INVALID_LSN_VAL);

    // If partitioning services, perform preflight checks
    if (is_served && OB_FAIL(add_served_ls_pre_check_(tls_id))) {
      LOG_ERROR("add_served_ls_pre_check_ fail", KR(ret), K(tls_id), K(is_served));
    }
    // Dynamic assignment of an element
    else if (OB_ISNULL(info = map_->alloc())) {
      LOG_ERROR("alloc part info fail", K(info), K(tls_id));
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    // Perform initialization
    else if (OB_FAIL(info->init(tls_id, is_create_ls, start_serve_tstamp, is_served))) {
      LOG_ERROR("init part info fail", KR(ret), K(tls_id), K(start_serve_tstamp),
          K(is_create_ls), K(is_served));
      map_->free(info);
      info = NULL;
    } else {
      // Print before performing map insertion, as the insertion will be seen by others afterwards and there is a concurrent modification scenario
      if (is_served) {
        LS_ISTAT(info, "[LS] [ADD_LS]");
      } else {
        LS_ISTAT(info, "[LS] [ADD_NOT_SERVED_LS]");
      }

      // Inserting elements, the insert interface does not have get() semantics and does not require revert
      if (OB_FAIL(map_->insert(tls_id, info))) {
        LOG_ERROR("insert part into map fail", KR(ret), K(tls_id), KPC(info));
      } else {
        // The info structure cannot be reapplied afterwards and may be deleted at any time
        info = NULL;

        // For partitioning of services, execute the add-ls callback
        if (is_served && OB_FAIL(call_add_ls_callbacks_(tls_id, start_serve_tstamp, start_lsn))) {
          // The add ls callback should not add ls repeatedly, here it returns OB_ERR_UNEXPECTED, an error exits
          if (OB_ENTRY_EXIST == ret) {
            ret = OB_ERR_UNEXPECTED;
            LOG_ERROR("call add-ls callbacks fail, add repeated ls", KR(ret), K(tls_id), K(start_serve_tstamp),
                K(start_lsn));
          } else {
            LOG_ERROR("call add-ls callbacks fail", KR(ret), K(tls_id), K(start_serve_tstamp),
                K(start_lsn));
          }
        }
      }
    }
  }

  return ret;
}

int ObLogLSMgr::add_served_ls_pre_check_(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  bool is_tenant_serving = false;

  // Request a LS slot from the tenant structure and if the tenant is no longer in service, it cannot be added further
  if (OB_FAIL(host_.inc_ls_count_on_serving(tls_id, is_tenant_serving))) {
    LOG_ERROR("inc_ls_count_on_serving fail", KR(ret), K(tls_id), K_(host));
  } else if (OB_UNLIKELY(! is_tenant_serving)) {
    // The tenant is not in service
    // The current implementation, when a tenant is not in service, does not perform DDL tasks for that tenant, so that cannot happen here
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("add ls when tenant is not serving, unexpected", KR(ret),
        K(is_tenant_serving), K(host_), K(tls_id));
  }

  return ret;
}

int ObLogLSMgr::call_add_ls_callbacks_(
    const logservice::TenantLSID &tls_id,
    const int64_t start_serve_tstamp,
    const palf::LSN &start_lsn)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else {
    logfetcher::ObLogFetcherStartParameters start_parameters;
    start_parameters.reset(start_serve_tstamp, start_lsn);

    for (int64_t index = 0; OB_SUCCESS == ret && index < ls_add_cb_array_->count(); index++) {
      LSAddCallback *cb = NULL;

      if (OB_FAIL(ls_add_cb_array_->at(index, reinterpret_cast<int64_t &>(cb)))) {
        LOG_ERROR("get callback from array fail", KR(ret), K(index));
      } else if (OB_ISNULL(cb)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_ERROR("get callback from array fail, callback is NULL", KR(ret), K(index), K(cb));
      } else if (OB_FAIL(cb->add_ls(tls_id, start_parameters))) {
        LOG_ERROR("add_ls fail", KR(ret), K(tls_id), K(start_serve_tstamp), K(cb),
            K(start_lsn));
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogLSMgr::call_recycle_ls_callbacks_(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else {
    for (int64_t index = 0; OB_SUCCESS == ret && index < ls_rc_cb_array_->count(); index++) {
      LSRecycleCallback *cb = NULL;

      if (OB_FAIL(ls_rc_cb_array_->at(index, (int64_t &)cb))) {
        LOG_ERROR("get callback from array fail", KR(ret), K(index));
      } else if (OB_ISNULL(cb)) {
        LOG_ERROR("get callback from array fail, callback is NULL", KR(ret), K(index), K(cb));
        ret = OB_ERR_UNEXPECTED;
      } else if (OB_FAIL(cb->recycle_ls(tls_id))) {
        if (OB_ENTRY_NOT_EXIST == ret) {
          // LS not exist as expected
          ret = OB_SUCCESS;
        } else {
          LOG_ERROR("recycle_ls fail", KR(ret), K(tls_id), K(cb));
        }
      } else {
        // succ
      }
    }
  }

  return ret;
}

int ObLogLSMgr::offline_ls(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  bool ensure_recycled_when_offlined = false;

  if (OB_FAIL(offline_ls_(tls_id, ensure_recycled_when_offlined))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // ls not exist
      ISTAT("offline ls, but not served", K(tls_id));
    } else {
      LOG_ERROR("offline_ls_ fail", K(tls_id));
    }
  } else {
    ISTAT("offline and recycle ls success", K_(tenant_id), K(tls_id));
  }

  return ret;
}

int ObLogLSMgr::offline_and_recycle_ls(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  bool ensure_recycled_when_offlined = true;

  if (OB_FAIL(offline_ls_(tls_id, ensure_recycled_when_offlined))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      // ls not exist
      ISTAT("offline and recycle ls, but not served", K(tls_id));
    } else {
      LOG_ERROR("offline_ls_ fail", K(tls_id));
    }
  } else {
    ISTAT("offline and recycle ls success", K_(tenant_id), K(tls_id));
  }
  return ret;
}

void ObLogLSMgr::print_ls_info(int64_t &serving_ls_count,
    int64_t &offline_ls_count,
    int64_t &not_served_ls_count)
{
  int ret = OB_SUCCESS;

  if (IS_INIT) {
    LSInfoPrinter ls_info_printer(tenant_id_);
    if (OB_FAIL(map_->for_each(ls_info_printer))) {
      LOG_ERROR("ObLogLSInfo map foreach fail", KR(ret));
    } else {
      // success
    }

    if (OB_SUCCESS == ret) {
      serving_ls_count = ls_info_printer.serving_ls_count_;
      offline_ls_count = ls_info_printer.offline_ls_count_;
      not_served_ls_count = ls_info_printer.not_served_ls_count_;
    }
  }
}

int ObLogLSMgr::offline_ls_(
    const logservice::TenantLSID &tls_id,
    const bool ensure_recycled_when_offlined)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else {
    ObLogLSInfo *info = NULL;
    bool enable_create = false;
    int64_t end_trans_count = 0;

    ret = map_->get(tls_id, info, enable_create);
    if (OB_ENTRY_NOT_EXIST == ret) {
      // not exist
    } else if (OB_SUCCESS != ret) {
      LOG_ERROR("get LSInfo from map fail", KR(ret), K(tls_id));
    } else if (OB_ISNULL(info)) {
      LOG_ERROR("get LSInfo from map fail, LSInfo is NULL", KR(ret), K(tls_id));
      ret = OB_ERR_UNEXPECTED;
    } else if (info->offline(end_trans_count)) {  // Atomic switch to OFFLINE state
      ISTAT("[OFFLINE_PART] switch offline state success", K(tls_id), K(end_trans_count),
          K(ensure_recycled_when_offlined), KPC(info));

      // Recycle the ls if there are no ongoing transactions on it
      if (0 == end_trans_count) {
        if (OB_FAIL(recycle_ls_(tls_id, info))) {
          LOG_ERROR("recycle_ls_ fail", KR(ret), K(tls_id), K(info));
        }
      }
      // An error is reported if a recycle operation must be performed when asked to go offline,
      // indicating that in this case there should be no residual transactions for the external guarantee
      else if (ensure_recycled_when_offlined) {
        LOG_ERROR("there are still transactions waited, can not recycle", K(tls_id),
            K(end_trans_count), KPC(info));
        ret = OB_INVALID_DATA;
      }
    } else {
      LS_ISTAT(info, "[OFFLINE_PART] ls has been in offline state");
    }

    REVERT_LS_INFO(info, ret);
  }

  return ret;
}

int ObLogLSMgr::recycle_ls_(
    const logservice::TenantLSID &tls_id,
    ObLogLSInfo *info)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else if (OB_ISNULL(info)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(info));
  } else {
    LS_ISTAT(info, "[RECYCLE_LS]");

    // Notify the modules that LS partition is being recycled
    if (OB_FAIL(call_recycle_ls_callbacks_(tls_id))) {
      LOG_ERROR("call recycle-ls callbacks fail", KR(ret), K(tls_id));
    } else if (OB_FAIL(map_->remove(tls_id))) { // Deleting records from Map
      LOG_ERROR("remove LSInfo from map fail", KR(ret), K(tls_id));
    } else {
      // succ
    }
  }

  return ret;
}

int ObLogLSMgr::inc_ls_trans_count_on_serving(bool &is_serving,
    const logservice::TenantLSID &tls_id,
    const palf::LSN &commit_log_lsn,
    const bool print_ls_not_serve_info,
    const int64_t timeout,
    volatile bool &stop_flag)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(! tls_id.is_valid())
      || OB_UNLIKELY(! commit_log_lsn.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(tls_id), K(commit_log_lsn));
  } else {
    // In test mode, determine if want to block the participant list confirmation process, and if so, wait for a period of time
    if (TCONF.test_mode_on) {
      int64_t block_time_us = TCONF.test_mode_block_verify_participants_time_sec * _SEC_;
      if (block_time_us > 0) {
        ISTAT("[INC_TRANS_COUNT] [TEST_MODE_ON] block to verify participants",
            K_(tenant_id), K(block_time_us));
        ob_usleep((useconds_t)block_time_us);
      }
    }
    // Then determine if the partitioned transaction is serviced, and if so, increase the number of transactions
    if (OB_FAIL(inc_trans_count_on_serving_(is_serving, tls_id, print_ls_not_serve_info))) {
      // Handle cases where participant do not exist
      if (OB_ENTRY_NOT_EXIST == ret) {
        ret = OB_SUCCESS;
        bool is_has_added = false;
        is_has_added = is_ls_served_(tls_id);

        // For example, the TRX1 transaction operates on log stream A, B, and C with A/B/C participant list
        // 1. In the multi-instance synchronization scenario, only synchronizes LS A and B.
        //    If C itself is not synchronized, than C determines that it does not serve,
        //    and aggregates A and B to complete distributed transaction assembly.
        // 2. If C is the newly created LS. Because user data is synchronized with the LS table, it may appear
        //    that has synchronized to TRX1 data, but the LS table has not been synchronized to the data of the newly created LS C.
        //    Because CDC synchronizes the complete data, it's easy to simplify the process and expect C to be served.
        if (! is_has_added) {
          is_serving = false;

          if (print_ls_not_serve_info) {
            _ISTAT("[INC_TRANS_COUNT] [LS_NOT_SERVE] TENANT=%lu LS=%s COMMIT_LSN=%s",
                tenant_id_, to_cstring(tls_id), to_cstring(commit_log_lsn));
          } else if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
            _ISTAT("[INC_TRANS_COUNT] [PART_NOT_SERVE] TENANT=%lu LS=%s COMMIT_LSN=%s",
                tenant_id_, to_cstring(tls_id), to_cstring(commit_log_lsn));
          } else {}
        } else {
          LOG_INFO("[INC_TRANS_COUNT] [FUTURE_LS] wait for future ls begin", K(tls_id));
          constexpr int64_t retry_interval = 10 * _MSEC_;
          RETRY_FUNC_ON_ERROR_WITH_USLEEP_MS(OB_ENTRY_NOT_EXIST, retry_interval, stop_flag, *this,
              inc_trans_count_on_serving_, is_serving, tls_id, print_ls_not_serve_info);
          LOG_INFO("[INC_TRANS_COUNT] [FUTURE_LS] wait for future ls end", K(tls_id));
        }
      } else {
        LOG_ERROR("inc_trans_count_on_serving_ failed", KR(ret), K(tls_id), K(is_serving));
      }
    }
  }

  return ret;
}

int ObLogLSMgr::inc_trans_count_on_serving_(bool &is_serving,
    const logservice::TenantLSID &tls_id,
    const bool print_ls_not_serve_info)
{
  int ret = OB_SUCCESS;
  ObLogLSInfo *info = NULL;
  bool enable_create = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!tls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_ls_id", KR(ret), K(tls_id));
  } else if (OB_FAIL(map_->get(tls_id, info, enable_create))) {
    if (OB_ENTRY_NOT_EXIST != ret) {
      LOG_ERROR("get LSInfo from map fail", KR(ret), K(tls_id));
    } else {
      // not exist
    }
  } else if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("LSInfo is NULL", KR(ret));
  } else {
    info->inc_trans_count_on_serving(is_serving);

    if (! is_serving) {
      if (print_ls_not_serve_info) {
        LS_ISTAT(info, "[INC_TRANS_COUNT] [LS_NOT_SERVE]");
      } else if (REACH_TIME_INTERVAL(PRINT_LOG_INTERVAL)) {
        LS_ISTAT(info, "[INC_TRANS_COUNT] [LS_NOT_SERVE]");
      } else {
        // do nothing
      }
    } else {
      LS_DSTAT(info, "[INC_TRANS_COUNT]");
    }
  }

  REVERT_LS_INFO(info, ret);
  return ret;
}

int ObLogLSMgr::dec_ls_trans_count(const logservice::TenantLSID &tls_id)
{
  int ret = OB_SUCCESS;
  bool need_remove = false;
  ObLogLSInfo *info = NULL;
  bool enable_create = false;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_ERROR("ObLogLSMgr has not been initialized", KR(ret));
  } else if (OB_UNLIKELY(!tls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid tenant_ls_id", KR(ret), K(tls_id));
  } else if (OB_FAIL(map_->get(tls_id, info, enable_create))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_ERROR("LSInfo does not exist", KR(ret), K(tls_id));
    } else {
      LOG_ERROR("get LSInfo from map fail", KR(ret), K(tls_id));
    }
  } else if (OB_ISNULL(info)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("LSInfo is NULL", KR(ret), K(info));
  } else if (OB_FAIL(info->dec_trans_count(need_remove))) {
    LOG_ERROR("dec_trans_count fail", KR(ret), K(*info));
  } else {
    LS_DSTAT(info, "[DEC_TRANS_COUNT]");

    if (need_remove) {
      if (OB_FAIL(recycle_ls_(tls_id, info))) {
        LOG_ERROR("recycle_ls_ fail", KR(ret), K(tls_id), K(info));
      }
    }
  }

  REVERT_LS_INFO(info, ret);

  return ret;
}

} // namespace libobcdc
} // namespace oceanbase

#undef _STAT
#undef STAT
#undef _ISTAT
#undef ISTAT
#undef _DSTAT
#undef DSTAT
