/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define USING_LOG_PREFIX CLOG
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"
#include "lib/time/ob_time_utility.h"
#include "logservice/palf_handle_guard.h"
#include "ob_restore_log_function.h"
#include "ob_log_restore_net_driver.h"
#include "share/ob_ls_id.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/tx_storage/ob_ls_service.h"    // ObLSService
#include "logservice/ob_log_service.h"      // ObLogService
#include "share/restore/ob_log_restore_source.h"   // ObLogRestoreSourceType
#include "logservice/logfetcher/ob_log_fetcher.h"  // ObLogFetcher
#include "observer/omt/ob_tenant_config_mgr.h"  // tenant_config

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"    // ObSqlString

namespace oceanbase
{
namespace logservice
{
#define GET_RESTORE_HANDLER_CTX(id)       \
  ObLS *ls = NULL;      \
  ObLSHandle ls_handle;         \
  ObLogRestoreHandler *restore_handler = NULL;       \
  if (OB_FAIL(ls_svr_->get_ls(id, ls_handle, ObLSGetMod::LOG_MOD))) {   \
    LOG_WARN("get ls failed", K(ret), K(id));     \
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {      \
    ret = OB_ERR_UNEXPECTED;       \
    LOG_INFO("get ls is NULL", K(ret), K(id));      \
  } else if (OB_ISNULL(restore_handler = ls->get_log_restore_handler())) {     \
    ret = OB_ERR_UNEXPECTED;      \
    LOG_INFO("restore_handler is NULL", K(ret), K(id));   \
  }    \
  if (OB_SUCC(ret))

using namespace oceanbase::storage;
ObLogRestoreNetDriver::ObLogRestoreNetDriver() :
  ObLogRestoreDriverBase(),
  lock_(),
  stop_flag_(false),
  source_(),
  restore_function_(),
  error_handler_(),
  ls_ctx_factory_(),
  ls_ctx_add_info_factory_(),
  cfg_(),
  fetcher_(NULL),
  proxy_()
{}

ObLogRestoreNetDriver::~ObLogRestoreNetDriver()
{
  destroy();
}

int ObLogRestoreNetDriver::init(const uint64_t tenant_id,
    ObLSService *ls_svr,
    ObLogService *log_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id == OB_INVALID_TENANT_ID
        || NULL == ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(tenant_id), K(ls_svr));
  } else if (OB_FAIL(restore_function_.init(tenant_id, ls_svr))) {
    LOG_WARN("restore_function_ init failed", K(tenant_id), K(ls_svr));
  } else if (OB_FAIL(error_handler_.init(ls_svr))) {
    LOG_WARN("error_handler_ init failed");
  } else if (OB_FAIL(cfg_.init())) {
    LOG_WARN("log fetcher context init failed");
  } else if (OB_FAIL(ls_ctx_factory_.init(tenant_id))) {
    LOG_WARN("ls_ctx_factory_ init failed");
  } else if (OB_FAIL(ObLogRestoreDriverBase::init(tenant_id, ls_svr, log_service))) {
    LOG_WARN("init failed", K(tenant_id), K(ls_svr));
  } else {
    LOG_INFO("ObLogRestoreNetDriver init succ");
  }
  return ret;
}

void ObLogRestoreNetDriver::destroy()
{
  int ret = OB_SUCCESS;
  WLockGuard guard(lock_);
  ObLogRestoreDriverBase::destroy();
  stop_flag_ = true;
  //TODO source destroy function
  restore_function_.destroy();
  error_handler_.destroy();
  cfg_.destroy();
  ls_ctx_factory_.destroy();
  proxy_.destroy();
  if (NULL != fetcher_) {
    int64_t count = 0;
    stop_fetcher_safely_();
    if (OB_FAIL(get_ls_count_in_fetcher_(count))) {
      LOG_WARN("get_ls_count_in_fetcher_ failed");
    } else if (count > 0) {
      ret = OB_ERR_UNEXPECTED;
      LOG_ERROR("fetcher not empty before destroy, memory may leak", K(count));
    } else {
      destroy_fetcher_();
    }
  }
}

int ObLogRestoreNetDriver::start()
{
  // fetcher will be build and start on demand
  return OB_SUCCESS;
}

void ObLogRestoreNetDriver::stop()
{
  // 1. Remove ls from Fetcher before it stopped
  // 2. Fetcher stop before the Log Restore Service Stop
  // 3. Prohibit ls add action after tenant stopped
  // 4. Prohibit new fetcher construction after tenant stopped
  //
  // So fetcher construction and LS add should be mutually exclusive with stop_flag_
  WLockGuard guard(lock_);
  stop_flag_ = true;
  if (NULL != fetcher_) {
    stop_fetcher_safely_();
    destroy_fetcher_();
  }
}

void ObLogRestoreNetDriver::wait()
{}

int ObLogRestoreNetDriver::do_schedule(share::ObRestoreSourceServiceAttr &source)
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("net driver not init", K(inited_));
  } else if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("net driver in stop state, just skip", K(stop_flag_));
  } else if (OB_UNLIKELY(!source.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("source not valid, just skip", K(source));
  } else if (OB_FAIL(copy_source_(source))) {
    LOG_WARN("copy_source_ failed", K(source));
  } else if (OB_FAIL(ObLogRestoreDriverBase::do_schedule())) {
    LOG_WARN("do_schedule failed");
  }
  return ret;
}

// Fetcher is free by LogRestoreService when the fetcher is stale,
// or by tenant mtl when the all threads stopped, including fetcher itself and tenant threads.
// So fetcher is safe in scan ls function.
int ObLogRestoreNetDriver::scan_ls(const share::ObLogRestoreSourceType &type)
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("net driver not init", K(inited_));
  } else if (NULL == fetcher_) {
  } else if (!is_service_log_source_type(type)) {
    LOG_INFO("not service source, clear fetcher", K(type));
    stop_fetcher_safely_();
    destroy_fetcher_();
  } else {
    common::ObArray<share::ObLSID> ls_array;
    if (OB_FAIL(fetcher_->get_all_ls(ls_array))) {
      LOG_WARN("get all ls failed");
    } else {
      for (int64_t i = 0; OB_SUCC(ret) && i < ls_array.count(); i++) {
        const share::ObLSID &id = ls_array.at(i);
        int64_t proposal_id = 0;
        bool is_stale = false;
        if (OB_FAIL(fetcher_->get_ls_proposal_id(id, proposal_id))) {
          if (OB_ENTRY_NOT_EXIST == ret) {
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("get ls proposal_id failed", K(id));
          }
        } else if (OB_FAIL(check_ls_stale_(id, proposal_id, is_stale))) {
          LOG_WARN("check_ls_stale_ failed", K(id));
        }  else if (!is_stale) {
          // do nothing
        } else if (OB_FAIL(fetcher_->remove_ls(id)) && OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("remove ls failed", K(id), K(is_stale));
        } else {
          ret = OB_SUCCESS;
          LOG_INFO("remove ls from log fetcher succ", K(id));
        }
      }

      if (OB_SUCC(ret)) {
        fetcher_->print_stat();
      }
    }
  }
  delete_fetcher_if_needed_with_lock_();
  update_config_();
  update_standby_preferred_upstream_log_region_();
  return ret;
}

void ObLogRestoreNetDriver::clean_resource()
{
  RLockGuard guard(lock_);
  if (NULL != fetcher_) {
    LOG_INFO("fetcher_ not NULL, do clean_resource");
    stop_fetcher_safely_();
    destroy_fetcher_();
  }
}

int ObLogRestoreNetDriver::copy_source_(const share::ObRestoreSourceServiceAttr &source)
{
  int ret = OB_SUCCESS;
  if (!(source_ == source)) {
    if (OB_FAIL(source_.assign(source))) {
      LOG_WARN("source assign failed", K(source));
    } else {
      LOG_INFO("source set succ", K(source_));
    }
  }
  return ret;
}

int ObLogRestoreNetDriver::refresh_fetcher_if_needed_(const share::ObRestoreSourceServiceAttr &source)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(stop_flag_)) {
    ret = OB_IN_STOP_STATE;
    LOG_WARN("log restore net driver stop, just skip it", K(stop_flag_));
  }

  // 1. Stale fetcher, destroy it here and reconstruct it
  //    Check a fetcher if is stale based on the source cluster id and the source tenant id,
  //    which means the log_restore_source tenant changed.
  if (OB_SUCC(ret)) {
    if (NULL != fetcher_ && is_fetcher_stale_(source.user_.cluster_id_, source.user_.tenant_id_)) {
      LOG_INFO("stale fetcher, need reconstruct it", K(source));
      stop_fetcher_safely_();
      destroy_fetcher_();
    }
  }

  // 2. Init or refresh proxy based on user info and server list
  if (OB_SUCC(ret)) {
    if (OB_FAIL(refresh_proxy_(source))) {
      LOG_WARN("refresh_proxy_ failed", K(source));
    }
  }

  // 3. Init fetcher
  if (OB_SUCC(ret) && NULL == fetcher_) {
    if (OB_FAIL(init_fetcher_if_needed_(source.user_.cluster_id_, source.user_.tenant_id_))) {
      LOG_WARN("init fetcher failed", K(source));
    } else {
      LOG_INFO("fetcher init succ");
    }
  }

  return ret;
}

int ObLogRestoreNetDriver::refresh_proxy_(const share::ObRestoreSourceServiceAttr &source)
{
  int ret = OB_SUCCESS;
  const char *db_name = common::ObCompatibilityMode::ORACLE_MODE == source.user_.mode_ ? OB_ORA_SYS_SCHEMA_NAME : OB_SYS_DATABASE_NAME;
  char passwd[OB_MAX_PASSWORD_LENGTH + 1] = {0};
  ObSqlString user;
  if (OB_FAIL(source.get_password(passwd, sizeof(passwd)))) {
    LOG_WARN("get_password failed", K(source));
  } else if (OB_FAIL(source.get_user_str_(user))) {
    LOG_WARN("get user str failed", K(source));
  } else if (proxy_.is_inited()) {
    if (OB_FAIL(proxy_.refresh_conn(source.addr_, user.ptr(), passwd, db_name))) {
      LOG_WARN("refresh_conn failed", K(source));
    }
  } else if (OB_FAIL(proxy_.init(MTL_ID(), source.addr_, user.ptr(), passwd, db_name))) {
    LOG_WARN("proxy init failed", K(source));
  }
  return ret;
}

int ObLogRestoreNetDriver::do_fetch_log_(ObLS &ls)
{
  int ret = OB_SUCCESS;
  bool can_fetch_log = false;
  int64_t proposal_id = -1;
  bool need_schedule = false;
  const ObLSID &id = ls.get_ls_id();
  if (OB_UNLIKELY(! id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(id));
  } else if (OB_FAIL(check_replica_status_(ls, can_fetch_log))) {
    LOG_WARN("check replica status failed", K(ls));
  } else if (! can_fetch_log) {
    // just skip
  } else if (OB_FAIL(check_need_schedule_(ls, proposal_id, need_schedule))) {
    LOG_WARN("check need schedule failed");
  } else if (! need_schedule) {
    LOG_TRACE("no need schedule, just skip", "ls_id", ls.get_ls_id());
  } else if (OB_FAIL(refresh_fetcher_if_needed_(source_))) {
    LOG_WARN("refresh_fetcher_if_needed_ failed", K(source_));
  } else if (OB_FAIL(add_ls_if_needed_with_lock_(ls.get_ls_id(), proposal_id))) {
    LOG_WARN("add ls failed", K(ls), K(proposal_id));
  }
  return ret;
}

int ObLogRestoreNetDriver::check_need_schedule_(ObLS &ls,
    int64_t &proposal_id,
    bool &need_schedule)
{
  int ret = OB_SUCCESS;
  ObLogRestoreHandler *restore_handler = NULL;
  ObRemoteFetchContext context;
  if (OB_ISNULL(restore_handler = ls.get_log_restore_handler())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("get restore_handler failed", "id", ls.get_ls_id());
  } else if (OB_FAIL(restore_handler->need_schedule(need_schedule, proposal_id, context))) {
    LOG_WARN("check need schedule failed");
  }
  return ret;
}

int ObLogRestoreNetDriver::add_ls_if_needed_with_lock_(const share::ObLSID &id, const int64_t proposal_id)
{
  int ret = OB_SUCCESS;
  palf::LSN end_lsn;
  share::SCN end_scn;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_ISNULL(fetcher_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("fetcher is NULL", K(ret), K(fetcher_));
  } else if (fetcher_->is_ls_exist(id)) {
    //TODO check ls in fetcher with proposal_id
  } else if (OB_FAIL(log_service_->open_palf(id, palf_handle_guard))) {
    LOG_WARN("open palf failed", K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_scn(end_scn))) {
    LOG_WARN("get end scn failed", K(id));
  } else if (OB_FAIL(palf_handle_guard.get_end_lsn(end_lsn))) {
    LOG_WARN("get end lsn failed", K(id));
  } else if (!end_lsn.is_valid() || !end_scn.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid end_lsn or end_scn", K(id), K(proposal_id), K(end_lsn), K(end_scn));
  } else {
    int64_t start_ts = end_scn.convert_to_ts() * 1000L;
    logfetcher::ObLogFetcherStartParameters param;
    param.reset(start_ts, end_lsn, proposal_id);

    if (OB_UNLIKELY(stop_flag_)) {
      ret = OB_IN_STOP_STATE;
      LOG_WARN("log restore net driver stop, just skip it", K(stop_flag_));
    } else if (OB_FAIL(fetcher_->add_ls(id, param))) {
      LOG_WARN("add_ls failed", K(id), K(param));
    } else {
      LOG_INFO("add ls succ", K(id), K(end_lsn), K(start_ts));
    }
  }
  return ret;
}

int ObLogRestoreNetDriver::init_fetcher_if_needed_(const int64_t cluster_id, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  if (NULL != fetcher_) {
    // fetcher already exist
  } else if (OB_ISNULL(fetcher_ = MTL_NEW(logfetcher::ObLogFetcher, "LogFetcher"))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else {
    const logfetcher::LogFetcherUser log_fetcher_user = logfetcher::LogFetcherUser::STANDBY;
    const bool is_loading_data_dict_baseline_data = false;
    const logfetcher::ClientFetchingMode fetching_mode = logfetcher::ClientFetchingMode::FETCHING_MODE_INTEGRATED;
    share::ObBackupPathString archive_dest;
    common::ObMySQLProxy *proxy = NULL;
    cfg_.fetch_log_rpc_timeout_sec = get_rpc_timeout_sec_();

    if (OB_FAIL(proxy_.get_sql_proxy(proxy))) {
      LOG_WARN("get sql proxy failed");
    } else if (OB_FAIL(fetcher_->init(log_fetcher_user, cluster_id, tenant_id, MTL_ID(),
            is_loading_data_dict_baseline_data, fetching_mode, archive_dest,
            cfg_, ls_ctx_factory_, ls_ctx_add_info_factory_,
            restore_function_, proxy, &error_handler_))) {
      CLOG_LOG(WARN, "fetcher init failed");
    } else if (OB_FAIL(fetcher_->start())) {
      CLOG_LOG(WARN, "fetcher start failed");
    } else {
      CLOG_LOG(INFO, "fetcher start succ");
    }
  }

  if (OB_FAIL(ret) && NULL != fetcher_) {
    destroy_fetcher_forcedly_();
  }
  return ret;
}

void ObLogRestoreNetDriver::delete_fetcher_if_needed_with_lock_()
{
  int ret = OB_SUCCESS;
  int64_t count = 0;

  if (NULL == fetcher_) {
    // do nothing
  } else if (OB_FAIL(get_ls_count_in_fetcher_(count))) {
    LOG_WARN("get_ls_count_in_fetcher_ failed");
  } else if (count > 0) {
    // fetcher not empty, just skip
  } else {
    LOG_INFO("no ls exist in fetcher, destroy it");
    stop_fetcher_safely_();
    destroy_fetcher_();
  }
}

void ObLogRestoreNetDriver::update_config_()
{
  if (NULL != fetcher_) {
    const int64_t fetch_log_rpc_timeout_sec = get_rpc_timeout_sec_();
    if (fetch_log_rpc_timeout_sec != cfg_.fetch_log_rpc_timeout_sec) {
      cfg_.fetch_log_rpc_timeout_sec = fetch_log_rpc_timeout_sec;
      fetcher_->configure(cfg_);
    }
  }
}

int64_t ObLogRestoreNetDriver::get_rpc_timeout_sec_()
{
  int64_t rpc_timeout = 0;
  const int64_t DEFAULT_FETECH_LOG_RPC_TIMEOUT = 15;   // 15s
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
  if (!tenant_config.is_valid()) {
    rpc_timeout = DEFAULT_FETECH_LOG_RPC_TIMEOUT;
  } else {
    rpc_timeout = tenant_config->standby_db_fetch_log_rpc_timeout / 1000 / 1000L;
  }
  return rpc_timeout;
}

void ObLogRestoreNetDriver::update_standby_preferred_upstream_log_region_()
{
  omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));

  if (! tenant_config.is_valid()) {
  } else {
    if (nullptr != fetcher_) {
      const char *region_string = tenant_config->standby_db_preferred_upstream_log_region;
      fetcher_->update_preferred_upstream_log_region(common::ObRegion(region_string));
    }
  }
}

bool ObLogRestoreNetDriver::is_fetcher_stale_(const int64_t cluster_id, const uint64_t tenant_id)
{
  bool bret = false;
  if (NULL == fetcher_) {
    bret = true;
  } else {
    bret = cluster_id != fetcher_->get_cluster_id() || tenant_id != fetcher_->get_source_tenant_id();
  }
  return bret;
}

int ObLogRestoreNetDriver::stop_fetcher_safely_()
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObLSID> ls_list;
  if (NULL == fetcher_) {
    // do nothing
  } else if (OB_FAIL(fetcher_->get_all_ls(ls_list))) {
    CLOG_LOG(WARN, "get all ls failed");
  } else {
    for (int64_t i = 0; i < ls_list.count() && OB_SUCC(ret); i++) {
      const share::ObLSID &ls_id = ls_list.at(i);
      if (OB_FAIL(fetcher_->remove_ls(ls_id)) && OB_ENTRY_NOT_EXIST != ret) {
        CLOG_LOG(ERROR, "remove ls failed", K(ls_id), K(i), K(ls_list));
      } else {
        ret = OB_SUCCESS;
        CLOG_LOG(INFO, "remove ls from log fetcher succ", K(ls_id));
      }
    }
  }
  return ret;
}

void ObLogRestoreNetDriver::destroy_fetcher_()
{
  int ret = OB_SUCCESS;
  if (NULL != fetcher_) {
    int64_t ls_count = 0;
    if (OB_FAIL(get_ls_count_in_fetcher_(ls_count))) {
      CLOG_LOG(WARN, "get ls count failed");
    } else if (0 != ls_count) {
      ret = OB_ERR_UNEXPECTED;
      CLOG_LOG(ERROR, "ls residual in log fetcher when it comes end", K(ls_count));
    } else {
      fetcher_->stop();
      fetcher_->destroy();
      MTL_DELETE(ObLogFetcher, "LogFetcher", fetcher_);
      fetcher_ = NULL;
    }
  }
  // destroy proxy after fetcher is destroyed
  if (NULL == fetcher_) {
    proxy_.destroy();
  }
}

void ObLogRestoreNetDriver::destroy_fetcher_forcedly_()
{
  if (NULL != fetcher_) {
    CLOG_LOG(INFO, "destroy_fetcher forcedly");
    fetcher_->stop();
    fetcher_->destroy();
    MTL_DELETE(ObLogFetcher, "LogFetcher", fetcher_);
    fetcher_ = NULL;
  }

  // destroy proxy after fetcher is destroyed
  if (NULL == fetcher_) {
    proxy_.destroy();
  }
}

// only delete ls which is restore leader
int ObLogRestoreNetDriver::check_ls_stale_(const share::ObLSID &id,
    const int64_t fetcher_proposal_id,
    bool &is_stale)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  common::ObRole role;
  int64_t proposal_id = 0;
  is_stale = false;
  GET_RESTORE_HANDLER_CTX(id) {
    if (OB_FAIL(restore_handler->get_role(role, proposal_id))) {
      LOG_WARN("get role failed", K(id));
    } else {
      is_stale = !is_strong_leader(role) || fetcher_proposal_id != proposal_id;
    }
  } else {
    is_stale = true;
    tmp_ret = ret;
    ret = OB_SUCCESS;
  }
  if (is_stale) {
    LOG_INFO("ls is stale, need remove it from fetcher", K(tmp_ret), K(id), K(role),
        K(proposal_id), K(fetcher_proposal_id));
  }
  return ret;
}

int ObLogRestoreNetDriver::get_ls_count_in_fetcher_(int64_t &count)
{
  int ret = OB_SUCCESS;
  common::ObArray<share::ObLSID> ls_list;
  count = 0;
  if (NULL == fetcher_) {
  } else if (OB_FAIL(fetcher_->get_all_ls(ls_list))) {
    LOG_WARN("get_all_ls failed");
  } else {
    count = ls_list.count();
  }
  return ret;
}

int ObLogRestoreNetDriver::set_restore_log_upper_limit()
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;
  share::SCN upper_limit_scn;
  if (NULL == fetcher_) {
    // do nothing
  } else if (OB_FAIL(ObLogRestoreDriverBase::get_upper_resotore_scn(upper_limit_scn))) {
    LOG_WARN("get upper limit scn failed");
  } else if (OB_FAIL(fetcher_->update_fetching_log_upper_limit(upper_limit_scn))) {
    LOG_WARN("set restore log upper limit failed", K(upper_limit_scn));
  }
  return ret;
}

int ObLogRestoreNetDriver::set_compressor_type(const common::ObCompressorType &compressor_type)
{
  RLockGuard guard(lock_);
  int ret = OB_SUCCESS;

  if (NULL == fetcher_) {
    // do nothing
  } else if (OB_FAIL(fetcher_->update_compressor_type(compressor_type))) {
    LOG_WARN("ObLogFetcher update_compressor_type failed", K(compressor_type));
  }

  return ret;
}

int ObLogRestoreNetDriver::LogErrHandler::init(storage::ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("LogErrHandler already init", K(inited_));
  } else if (OB_ISNULL(ls_svr)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ls_svr));
  } else {
    ls_svr_ = ls_svr;
    inited_ = true;
  }
  return ret;
}

void ObLogRestoreNetDriver::LogErrHandler::destroy()
{
  ls_svr_ = NULL;
  inited_ = false;
}

void ObLogRestoreNetDriver::LogErrHandler::handle_error(const share::ObLSID &ls_id,
      const ErrType &err_type,
      share::ObTaskId &trace_id,
      const palf::LSN &lsn,
      const int err_no,
      const char *fmt, ...)
{
  int ret = OB_SUCCESS;
  const ObLogRestoreErrorContext::ErrorType restore_err_type = ErrType::FETCH_LOG == err_type ?
    ObLogRestoreErrorContext::ErrorType::FETCH_LOG : ObLogRestoreErrorContext::ErrorType::SUBMIT_LOG;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("LogErrHandler not init", K(inited_));
  } else {
    GET_RESTORE_HANDLER_CTX(ls_id) {
      if (palf::LSN(palf::LOG_INVALID_LSN_VAL) == lsn ) {
        palf::LSN tmp_lsn = palf::LSN(palf::PALF_INITIAL_LSN_VAL);
        palf::PalfHandleGuard palf_handle_guard;
        if (OB_FAIL(MTL(ObLogService*)->open_palf(ls_id, palf_handle_guard))) {
          LOG_WARN("open palf failed", K(ls_id));
        } else if (OB_FAIL(palf_handle_guard.get_end_lsn(tmp_lsn))) {
          LOG_WARN("get end lsn failed", K(ls_id));
        } else {
          restore_handler->mark_error(trace_id, err_no, tmp_lsn, restore_err_type);
        }
      } else {
        restore_handler->mark_error(trace_id, err_no, lsn, restore_err_type);
      }
    }
  }
}

int ObLogRestoreNetDriver::LogFetcherLSCtxAddInfoFactory::alloc(const char *str, logfetcher::ObILogFetcherLSCtxAddInfo *&ptr)
{
  int ret =OB_SUCCESS;
  const char *label = ALLOC_LABEL;
  ptr = OB_NEW(LogFetcherLSCtxAddInfo, label);
  return ret;
}

void ObLogRestoreNetDriver::LogFetcherLSCtxAddInfoFactory::free(logfetcher::ObILogFetcherLSCtxAddInfo *ptr)
{
  const char *label = ALLOC_LABEL;
  LogFetcherLSCtxAddInfo *add_info = static_cast<LogFetcherLSCtxAddInfo*>(ptr);
  OB_DELETE(LogFetcherLSCtxAddInfo, label, add_info);
}

int ObLogRestoreNetDriver::LogFetcherLSCtxAddInfo::get_dispatch_progress(
      const share::ObLSID &ls_id,
      int64_t &progress,
      logfetcher::PartTransDispatchInfo &dispatch_info)
{
  int ret = OB_SUCCESS;
  share::SCN scn;
  palf::PalfHandleGuard palf_handle_guard;
  if (OB_FAIL(MTL(ObLogService*)->open_palf(ls_id, palf_handle_guard))) {
    LOG_WARN("open palf failed", K(ls_id));
  } else if (OB_FAIL(palf_handle_guard.get_end_scn(scn))) {
    LOG_WARN("get end scn failed", K(ls_id));
  } else if (OB_UNLIKELY(!scn.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid scn", K(ls_id), K(scn));
  } else {
    progress = scn.get_val_for_logservice();
  }

  return ret;
}
} // namespace logservice
} // namespace oceanbase
