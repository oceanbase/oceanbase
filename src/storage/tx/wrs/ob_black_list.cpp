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

#include "ob_black_list.h"
#include "share/ob_thread_mgr.h"                                // set_thread_name
#include "observer/ob_server_struct.h"                          // for GCTX
#include "deps/oblib/src/common/ob_role.h"                      // role
#include "storage/tx/wrs/ob_weak_read_util.h"               // ObWeakReadUtil
#include "storage/tx/ob_ts_mgr.h"

namespace oceanbase
{
using namespace common;
using namespace storage;

namespace transaction
{
int ObBLService::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    TRANS_LOG(ERROR, "BLService init twice", K(*this));
  } else if (OB_FAIL(ls_bl_mgr_.init())) {
    TRANS_LOG(ERROR, "ls_bl_mgr_ init fail", KR(ret));
  } else if (OB_FAIL(ObThreadPool::init())) {
    TRANS_LOG(ERROR, "ThreadPool init fail", KR(ret));
  } else {
    is_inited_ = true;
    TRANS_LOG(INFO, "BLService init success", K(*this));
  }

  return ret;
}

void ObBLService::reset()
{
  is_running_ = false;
  is_inited_ = false;
  ObThreadPool::stop();
  ObThreadPool::wait();
  ls_bl_mgr_.reset();
}

void ObBLService::destroy()
{
  is_running_ = false;
  is_inited_ = false;
  ObThreadPool::stop();
  ObThreadPool::wait();
  ls_bl_mgr_.destroy();
}
int ObBLService::start()
{
  int ret = OB_SUCCESS;

  if (!is_inited_) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "BLService not inited", KR(ret));
  } else if (is_running_) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(ERROR, "BLService is already running", KR(ret));
  } else if (OB_FAIL(ObThreadPool::start())) {
    TRANS_LOG(ERROR, "ThreadPool start fail", KR(ret));
  } else {
    is_running_ = true;
    TRANS_LOG(INFO, "BLService start success");
  }

  return ret;
}

void ObBLService::stop()
{
  ObThreadPool::stop();
  TRANS_LOG(INFO, "BLService stop");
}

void ObBLService::wait()
{
  TRANS_LOG(INFO, "BLService wait begin");
  ObThreadPool::wait();
  TRANS_LOG(INFO, "BLService wait end");
}

int ObBLService::add(const ObBLKey &bl_key)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "BLService not inited", KR(ret));
  } else {
    switch(bl_key.get_type()) {
      case BLTYPE_LS: {
        if (OB_FAIL(ls_bl_mgr_.add(bl_key))) {
          TRANS_LOG(WARN, "ls_bl_mgr_ add failed", KR(ret), K(bl_key));
        }
        break;
      }
      default: {
        ret = OB_UNKNOWN_OBJ;
        TRANS_LOG(ERROR, "unknown key type", K(bl_key));
      }
    }
  }
  return ret;
}

void ObBLService::remove(const ObBLKey &bl_key)
{
  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG_RET(ERROR, OB_NOT_INIT, "BLService not inited");
  } else {
    switch(bl_key.get_type()) {
      case BLTYPE_LS: {
        ls_bl_mgr_.remove(bl_key);
        break;
      }
      default: {
        TRANS_LOG_RET(ERROR, OB_UNKNOWN_OBJ, "unknown key type", K(bl_key));
      }
    }
  }
}

int ObBLService::check_in_black_list(const ObBLKey &bl_key, bool &in_black_list) const
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    TRANS_LOG(ERROR, "BLService not inited", KR(ret));
  } else {
    switch(bl_key.get_type()) {
      case BLTYPE_LS: {
        if (OB_FAIL(ls_bl_mgr_.check_in_black_list(bl_key, in_black_list))) {
          TRANS_LOG(WARN, "ls_bl_mgr_ check failed", KR(ret), K(bl_key));
        }
        break;
      }
      default: {
        ret = OB_UNKNOWN_OBJ;
        TRANS_LOG(ERROR, "unknown blacklist key type", K(bl_key));
      }
    }
  }
  return ret;
}

void ObBLService::run1()
{
  const int64_t thread_index = get_thread_idx();
  int64_t last_print_stat_ts = 0;
  int64_t last_clean_up_ts = 0;
  lib::set_thread_name("BlackListService");
  TRANS_LOG(INFO, "blacklist refresh thread start", K(thread_index));

  if (OB_UNLIKELY(!is_inited_)) {
    TRANS_LOG_RET(ERROR, OB_NOT_INIT, "BLService not inited");
  } else {
    while (!has_set_stop()) {
      int64_t begin_tstamp = ObTimeUtility::current_time();
      do_thread_task_(begin_tstamp, last_print_stat_ts, last_clean_up_ts);
      int64_t end_tstamp = ObTimeUtility::current_time();
      int64_t wait_interval = BLACK_LIST_REFRESH_INTERVAL - (end_tstamp - begin_tstamp);
      if (wait_interval > 0) {
        thread_cond_.timedwait(wait_interval);
      }
    }
  }
  TRANS_LOG(INFO, "blacklist refresh thread end");
}

void ObBLService::do_thread_task_(const int64_t begin_tstamp,
                                  int64_t &last_print_stat_ts,
                                  int64_t &last_clean_up_ts)
{
  int ret = OB_SUCCESS;
  ObSqlString sql;
  ObMySQLProxy *sql_proxy = NULL;
  sqlclient::ObMySQLResult *result = NULL;

  // 查询ls内部表，根据时间戳信息决定是否将ls其加入黑名单
  SMART_VAR(ObISQLClient::ReadResult, res) {
    if (OB_ISNULL((sql_proxy = GCTX.sql_proxy_))) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "sql_proxy is null", KR(ret));
    } else if (OB_FAIL(sql.assign_fmt(BLACK_LIST_SELECT_LS_INFO_STMT))) {
      TRANS_LOG(WARN, "fail to append sql", KR(ret));
    } else if (OB_FAIL(sql_proxy->read(res, OB_SYS_TENANT_ID, sql.ptr()))) {
      TRANS_LOG(WARN, "fail to execute sql", KR(ret), K(sql));
    } else if (NULL == (result = res.get_result())) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(WARN, "fail to get sql result", KR(ret), K(sql));
    } else if (OB_FAIL(do_black_list_check_(result))) {
      TRANS_LOG(WARN, "fail to do black list check", KR(ret), K(sql));
    } else {
      // do nothing
    }
  }
  // 如果连续失败多次，黑名单失去了时效性，需要清空黑名单
  static int fail_cnt = 0;
  if (OB_SUCCESS != ret) {
    fail_cnt++;
    if (fail_cnt == BLACK_LIST_MAX_FAIL_COUNT) {
      ls_bl_mgr_.reset();
      TRANS_LOG(WARN, "failed too much times, reset blacklist ");
    }
  } else {
    fail_cnt = 0;
  }
  // 定期清理长久未更新的对象，它们实际上可能已经不存在了
  if (begin_tstamp > last_clean_up_ts + BLACK_LIST_CLEAN_UP_INTERVAL) {
    do_clean_up_();
    last_clean_up_ts = begin_tstamp;
  }
  // 定期打印黑名单内容
  if (ObTimeUtility::current_time() - last_print_stat_ts > BLACK_LIST_PRINT_INTERVAL) {
    print_stat_();
    last_print_stat_ts = begin_tstamp;
  }
}

int ObBLService::do_black_list_check_(sqlclient::ObMySQLResult *result)
{
  int ret = OB_SUCCESS;
  int64_t max_stale_time = 0;

  while (OB_SUCC(result->next())) {
    ObBLKey bl_key;
    ObLsInfo ls_info;
    SCN gts_scn;
    if (OB_FAIL(get_info_from_result_(*result, bl_key, ls_info))) {
      TRANS_LOG(WARN, "get_info_from_result_ fail ", KR(ret), K(result));
    } else if (ls_info.is_leader() && check_need_skip_leader_(bl_key.get_tenant_id())) {
      // cannot add leader into blacklist
    } else if (ls_info.weak_read_scn_ == 0) {
      // log stream is initializing, should't be put into blacklist
    } else if (OB_FAIL(OB_TS_MGR.get_gts(bl_key.get_tenant_id(), NULL, gts_scn))) {
      TRANS_LOG(WARN, "get gts scn error", K(ret), K(bl_key));
    } else {
      max_stale_time = get_tenant_max_stale_time_(bl_key.get_tenant_id());
      int64_t max_stale_time_ns = max_stale_time * 1000;
      if (gts_scn.get_val_for_gts() > ls_info.weak_read_scn_ + max_stale_time_ns
          || ls_info.tx_blocked_
          || ls_info.migrate_status_ != ObMigrationStatus::OB_MIGRATION_STATUS_NONE) {
        // scn is out-of-time，add this log stream into blacklist
        if (OB_FAIL(ls_bl_mgr_.update(bl_key, ls_info))) {
          TRANS_LOG(WARN, "ls_bl_mgr_ add fail ", K(bl_key), K(ls_info));
        }
      } else if (gts_scn.get_val_for_gts() + BLACK_LIST_WHITEWASH_INTERVAL_NS < ls_info.weak_read_scn_ + max_stale_time_ns) {
        // scn is new enough，remove this log stream in the blacklist
        ls_bl_mgr_.remove(bl_key);
      } else {
        // do nothing
      }
    }
  }
  if (OB_ITER_END == ret) {
    ret = OB_SUCCESS;
  } else {
    TRANS_LOG(WARN, "get next result fail ", KR(ret));
  }

  return ret;
}

bool ObBLService::check_need_skip_leader_(const uint64_t tenant_id)
{
  bool need_skip = true;
  int ret = OB_SUCCESS;
  MTL_SWITCH(tenant_id) {
    if (!MTL_TENANT_ROLE_CACHE_IS_PRIMARY()) {
      need_skip = false;
    }
  }
  if (!need_skip) {
    TRANS_LOG(INFO, "needn't skip leader", KR(ret), K(need_skip), K(tenant_id));
  }
  return need_skip;
}

int ObBLService::do_clean_up_()
{
  int ret = OB_SUCCESS;
  ObBLCleanUpFunctor<ObLsBLMgr> clean_up_functor(ls_bl_mgr_);
  if (OB_FAIL(ls_bl_mgr_.for_each(clean_up_functor))) {
    TRANS_LOG(WARN, "clean up blacklist fail ", KR(ret));
  }
  return ret;
}

int ObBLService::get_info_from_result_(sqlclient::ObMySQLResult &result, ObBLKey &bl_key, ObLsInfo &ls_info)
{
  int ret = OB_SUCCESS;

  ObString ip;
  int64_t port = 0;
  int64_t tenant_id = 0;
  int64_t id = ObLSID::INVALID_LS_ID;
  int64_t ls_role = -1;
  int64_t weak_read_scn = 0;
  int64_t migrate_status_int = -1;
  int64_t tx_blocked = -1;
  common::number::ObNumber weak_read_number;

  (void)GET_COL_IGNORE_NULL(result.get_varchar, "svr_ip", ip);
  (void)GET_COL_IGNORE_NULL(result.get_int, "svr_port", port);
  (void)GET_COL_IGNORE_NULL(result.get_int, "tenant_id", tenant_id);
  (void)GET_COL_IGNORE_NULL(result.get_int, "ls_id", id);
  (void)GET_COL_IGNORE_NULL(result.get_int, "role", ls_role);
  (void)GET_COL_IGNORE_NULL(result.get_number, "weak_read_scn", weak_read_number);
  (void)GET_COL_IGNORE_NULL(result.get_int, "migrate_status", migrate_status_int);
  (void)GET_COL_IGNORE_NULL(result.get_int, "tx_blocked", tx_blocked);

  ObLSID ls_id(id);
  common::ObAddr server;
  ObMigrationStatus migrate_status = ObMigrationStatus(migrate_status_int);

  if (false == server.set_ip_addr(ip, static_cast<uint32_t>(port))) {
    ret = OB_ERR_UNEXPECTED;
    TRANS_LOG(WARN, "invalid server address", K(ip), K(port));
  } else if (OB_FAIL(weak_read_number.cast_to_int64(weak_read_scn))) {
    TRANS_LOG(WARN, "failed to cast int", K(ret), K(weak_read_number));
  } else if (OB_FAIL(bl_key.init(server, tenant_id, ls_id))) {
    TRANS_LOG(WARN, "bl_key init fail", K(server), K(tenant_id), K(ls_id));
  } else if (OB_FAIL(ls_info.init(ls_role, weak_read_scn, migrate_status, (1 == tx_blocked) ? true : false))) {
    TRANS_LOG(WARN, "ls_info init fail", K(ls_role), K(weak_read_scn), K(migrate_status), K(tx_blocked));
  }
  if (OB_SUCC(ret)) {
    if (1 == tx_blocked) {
      TRANS_LOG(INFO, "current ls is blocked, need to put blacklist", K(bl_key), K(ls_info));
    } else if (0 != tx_blocked) {
      ret = OB_ERR_UNEXPECTED;
      TRANS_LOG(ERROR, "unexpected tx blocked", K(ret), K(tx_blocked), K(bl_key), K(ls_info));
    } else {
      // do nothing
    }
  }

  return ret;
}

int64_t ObBLService::get_tenant_max_stale_time_(uint64_t tenant_id)
{
  return ObWeakReadUtil::max_stale_time_for_weak_consistency(tenant_id, ObWeakReadUtil::IGNORE_TENANT_EXIST_WARN);
}

void ObBLService::print_stat_()
{
  TRANS_LOG(INFO, "start to print blacklist info");
  ObBLPrintFunctor print_fn;
  ls_bl_mgr_.for_each(print_fn);
}

} // transaction
} // oceanbase
