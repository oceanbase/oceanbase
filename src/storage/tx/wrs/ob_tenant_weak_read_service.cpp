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

#define USING_LOG_PREFIX TRANS

#include "ob_tenant_weak_read_service.h"

#include "lib/allocator/ob_mod_define.h"                        // ObModIds
#include "lib/rc/ob_rc.h"
#include "share/ob_thread_mgr.h"                                // TG
#include "observer/ob_server_struct.h"                          // GCTX

#include "ob_weak_read_service_rpc.h"                           // ObIWrsRpc
#include "ob_weak_read_util.h"                                  // ObWeakReadUtil
#include "storage/ob_super_block_struct.h"
#include "rpc/obrpc/ob_rpc_net_handler.h"
#include "share/location_cache/ob_location_service.h"
#include "storage/tx_storage/ob_ls_service.h"

#define MOD_STR "[WRS] [TENANT_WEAK_READ_SERVICE] "

#define STAT(level, fmt, args...) TRANS_LOG(level, MOD_STR fmt, ##args);
#define _STAT(level, fmt, args...) _TRANS_LOG(level, MOD_STR fmt, ##args);
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)
#define WSTAT(fmt, args...) STAT(WARN, fmt, ##args)
#define _WSTAT(fmt, args...) _STAT(WARN, fmt, ##args)
#define DSTAT(fmt, args...) STAT(DEBUG, fmt, ##args)

#define DEFINE_TIME_GUARD(mod, time)                              \
      ModuleInfo moduleInfo(*this, mod);                          \
      TimeGuard time_guard(moduleInfo.str(), (time));

using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::obrpc;
using namespace oceanbase::storage;

namespace oceanbase
{
namespace transaction
{

ObTenantWeakReadService::ObTenantWeakReadService() :
    inited_(false),
    tenant_id_(OB_INVALID_ID),
    wrs_rpc_(NULL),
    self_(),
    svr_version_mgr_(),
    cluster_service_(),
    thread_cond_(),
    last_refresh_locaction_cache_tstamp_(0),
    last_post_cluster_heartbeat_tstamp_(0),
    post_cluster_heartbeat_count_(0),
    last_succ_cluster_heartbeat_tstamp_(0),
    succ_cluster_heartbeat_count_(0),
    cluster_heartbeat_interval_(MIN_CLUSTER_HEARTBEAT_INTERVAL),
    cluster_service_master_(),
    last_self_check_tstamp_(0),
    last_generate_cluster_version_tstamp_(0),
    force_self_check_(false),
    tg_id_(-1),
    server_version_epoch_tstamp_(0)
{}

ObTenantWeakReadService::~ObTenantWeakReadService()
{
  destroy();
}

int ObTenantWeakReadService::init(const uint64_t tenant_id,
    common::ObMySQLProxy &mysql_proxy,
    ObIWrsRpc &wrs_rpc,
    const common::ObAddr &self)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(cluster_service_.init(tenant_id, mysql_proxy))) {
    LOG_ERROR("init cluster service fail", KR(ret), K(tenant_id));
  } else {
    tenant_id_ = tenant_id;
    wrs_rpc_ = &wrs_rpc;
    self_ = self;
    last_refresh_locaction_cache_tstamp_ = 0;
    last_post_cluster_heartbeat_tstamp_ = 0;
    post_cluster_heartbeat_count_ = 0;
    last_succ_cluster_heartbeat_tstamp_ = 0;
    succ_cluster_heartbeat_count_ = 0;
    cluster_heartbeat_interval_ = MIN_CLUSTER_HEARTBEAT_INTERVAL;
    cluster_service_master_.reset();
    last_self_check_tstamp_ = 0;
    last_generate_cluster_version_tstamp_ = 0;
    local_cluster_version_.set_min();
    force_self_check_ = false;
    server_version_epoch_tstamp_ = 0;

    if (OB_FAIL(TG_CREATE_TENANT(lib::TGDefIDs::WeakReadService, tg_id_))) {
      LOG_ERROR("create tg failed", K(ret));
    } else if (OB_FAIL(TG_SET_RUNNABLE(tg_id_, *this))) {
      LOG_ERROR("start tenant weak read service thread pool fail", K(ret), K(tenant_id));
    } else {
      inited_ = true;

      LOG_INFO("tenant weak read service init succ", K(tenant_id), K(lbt()));
    }
  }
  return ret;
}

int ObTenantWeakReadService::start()
{
  int ret = OB_SUCCESS;

  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObTenantWeakReadService not init", K(ret));
  } else if (OB_FAIL(TG_START(tg_id_))) {
    LOG_WARN("ObTenantWeakReadService start error", K(ret));
  } else {
    // do nothing
  }

  return ret;
}

void ObTenantWeakReadService::stop()
{
  TG_STOP(tg_id_);
}

void ObTenantWeakReadService::wait()
{
  TG_WAIT(tg_id_);
}

void ObTenantWeakReadService::destroy()
{
  if (inited_) {
    LOG_INFO("tenant weak read service destroy", K_(tenant_id));

    cluster_service_.destroy();

    inited_ = false;
    tenant_id_ = OB_INVALID_ID;
    wrs_rpc_ = NULL;
    self_.reset();
    last_refresh_locaction_cache_tstamp_ = 0;
    last_post_cluster_heartbeat_tstamp_ = 0;
    post_cluster_heartbeat_count_ = 0;
    last_succ_cluster_heartbeat_tstamp_ = 0;
    succ_cluster_heartbeat_count_ = 0;
    cluster_heartbeat_interval_ = 0;
    cluster_service_master_.reset();
    last_self_check_tstamp_ = 0;
    last_generate_cluster_version_tstamp_ = 0;
    local_cluster_version_.reset();
    force_self_check_ = false;
    server_version_epoch_tstamp_ = 0;
    TG_DESTROY(tg_id_);
  }
}

SCN ObTenantWeakReadService::get_server_version() const
{
  return svr_version_mgr_.get_version();
}

// if weak_read_refresh_interval is 0, wrs close, and return OB_NOT_SUPPORTED
int ObTenantWeakReadService::get_cluster_version(SCN &cur_version)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!ObWeakReadUtil::check_weak_read_service_available())) {
    ret = OB_NOT_SUPPORTED;
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cluster weak read service is disabled, weak read");
    LOG_WARN("get cluster version fail when check weak read service is not available", KR(ret), K(tenant_id_));
  } else {
    //  permit to get version from remote server
    bool only_request_local = false;
    if (OB_FAIL(get_cluster_version_internal_(cur_version, only_request_local))) {
      int old_ret = ret;
      // FIXME: In case of any error, the error code is uniformly converted, and an external
      // retry is required, but there may be special circumstances that do not need to retry,
      // and the error code will be further subdivided here.
      ret = OB_TRANS_WEAK_READ_VERSION_NOT_READY;
      LOG_WARN("get cluster version fail, need retry", K(old_ret), KR(ret), K(tenant_id_));
    } else {
      SCN last_local_cluster_version = local_cluster_version_.atomic_get();
      local_cluster_version_.atomic_set(cur_version);
      LOG_TRACE("get cluster version", K(cur_version), K(last_local_cluster_version));
    }
  }
  return ret;
}

int ObTenantWeakReadService::get_cluster_version_internal_(SCN &version,
    const bool only_request_local)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(cluster_service_.get_cluster_version(version))) {
    LOG_WARN("get weak read cluster version fail", KR(ret), K(version), K(tenant_id_));
    if (OB_NEED_RETRY == ret) {
      // self may be WRS Leader while not ready, need retry
    } else if (OB_NOT_IN_SERVICE == ret || OB_NOT_MASTER == ret) {
      // if self is not in service or not wrs leader, and only_request_local is false
      // get version from remote server
      if (! only_request_local) {
        ret = OB_SUCCESS;
        ret = get_cluster_version_by_rpc_(version);
      } else {
        LOG_WARN("current server is not in service or is not service master "
            "for weak read cluster service", KR(ret), K(tenant_id_));
      }
    } else {
      LOG_WARN("get weak read cluster version fail", KR(ret), K(tenant_id_));
    }
  } else {
    LOG_TRACE("get weak read cluster version succ", K(version), K(tenant_id_));
  }
  return ret;
}

int ObTenantWeakReadService::get_cluster_version_by_rpc_(SCN &version)
{
  int ret = OB_SUCCESS;
  ObAddr cluster_service_master;
  ObWrsGetClusterVersionRequest req;
  ObWrsGetClusterVersionResponse resp;

  // reset request
  req.set(self_);

  if (OB_UNLIKELY(! inited_) || OB_ISNULL(wrs_rpc_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_cluster_service_master_(cluster_service_master))) {
    LOG_WARN("get weak read cluster service master fail", KR(ret));
  } else {
    // send RPC
    if (OB_FAIL(wrs_rpc_->get_cluster_version(cluster_service_master, tenant_id_, req, resp))) {
      LOG_WARN("get weak read cluster version RPC fail", KR(ret), K(tenant_id_),
          K(cluster_service_master));
    } else if (OB_SUCCESS != resp.err_code_) {
      LOG_WARN("get weak read cluster version RPC return error", K(resp), K(tenant_id_),
          K(cluster_service_master));
      ret = resp.err_code_;
    } else if (OB_UNLIKELY(!resp.version_.is_valid())) {
      LOG_WARN("invalid weak read cluster version from RPC", K(resp), K(tenant_id_),
          K(cluster_service_master));
      ret = OB_INVALID_ERROR;
    } else {
      version = resp.version_;
    }

    if (OB_SUCCESS != ret) {
      refresh_cluster_service_master_();
    }
  }
  return ret;
}

int ObTenantWeakReadService::get_cluster_service_master_(common::ObAddr &cluster_service_master)
{
  int ret = OB_SUCCESS;
  bool force_renew = false;  // force renew location cache or not
  ObTabletID tablet_id = cluster_service_.get_cluster_service_tablet_id();
  const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
  DEFINE_TIME_GUARD("get_cluster_service_master", LOCATION_CACHE_GET_WARN_THRESHOLD);
  ObLSID ls_id;
  const uint64_t superior_tenant_id = get_private_table_exec_tenant_id(MTL_ID());
  if (OB_ISNULL(GCTX.location_service_) || OB_ISNULL(GCTX.schema_service_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("GCTX not init", KR(ret));
  } else if (OB_FAIL(GCTX.location_service_->nonblock_get(MTL_ID(), tablet_id, ls_id))) {
    LOG_WARN("get lsid error", KR(ret), K(tablet_id), "tenant_id", MTL_ID());
  } else if (!GCTX.schema_service_->is_tenant_full_schema(superior_tenant_id)) {
    ret = OB_NEED_WAIT;
    if (REACH_TIME_INTERVAL(1000 * 1000L)) { // 1s
      LOG_WARN("tenant schema is not ready, need wait", KR(ret), K(superior_tenant_id));
    }
  } else if (OB_FAIL(GCTX.location_service_->get_leader(
                    cluster_id, MTL_ID(), ls_id, force_renew, cluster_service_master))) {
    LOG_WARN("get tenant weak read cluster service partition leader from location cache fail",
        KR(ret), K(tablet_id), K(force_renew));
  } else {
    // success
  }
  return ret;
}

int ObTenantWeakReadService::update_server_version_with_part_info(const int64_t epoch_tstamp,
    const bool need_skip,
    const bool is_user_part,
    const SCN version)
{
  return svr_version_mgr_.update_with_part_info(tenant_id_, epoch_tstamp, need_skip, is_user_part, version);
}

int ObTenantWeakReadService::generate_server_version(const int64_t epoch_tstamp,
                                                     const bool need_print_status)
{
  int ret = OB_SUCCESS;
  SCN base_version_when_no_valid_partition;
  if (OB_FAIL(ObWeakReadUtil::generate_min_weak_read_version(tenant_id_, base_version_when_no_valid_partition))) {
    TRANS_LOG(WARN, "generate min weak read version error", K(ret), K_(tenant_id));
  } else {
    ret = svr_version_mgr_.generate_new_version(tenant_id_,
                                                epoch_tstamp,
                                                base_version_when_no_valid_partition,
                                                need_print_status);
  }
  return ret;
}

void ObTenantWeakReadService::get_weak_read_stat(ObTenantWeakReadStat &wrs_stat) const
{
  bool in_service = 0;
  int64_t master_epoch = 0;

  SCN current_cluster_version;
  SCN min_cluster_version;
  SCN max_cluster_version;
  int64_t cur_tstamp = ObTimeUtility::current_time();

  cluster_service_.get_serve_info(in_service, master_epoch);
  wrs_stat.tenant_id_ = tenant_id_;

  const bool ignore_invalid = true;
  //get server info with lock
  ObTenantWeakReadServerVersionMgr::ServerVersion sv;
  svr_version_mgr_.get_version(sv);
  wrs_stat.server_version_ = sv.version_;
  wrs_stat.server_version_delta_ = cur_tstamp - wrs_stat.server_version_.convert_to_ts(ignore_invalid);
  wrs_stat.local_cluster_version_ = local_cluster_version_.atomic_get();
  wrs_stat.local_cluster_version_delta_ = cur_tstamp - wrs_stat.local_cluster_version_.convert_to_ts(ignore_invalid);
  wrs_stat.total_part_count_ = sv.total_part_count_;
  wrs_stat.valid_inner_part_count_ = sv.valid_inner_part_count_;
  wrs_stat.valid_user_part_count_ = sv.valid_user_part_count_;

  // heartbeat info
  wrs_stat.cluster_heartbeat_post_tstamp_ = ATOMIC_LOAD(&last_post_cluster_heartbeat_tstamp_);
  wrs_stat.cluster_heartbeat_post_count_ = ATOMIC_LOAD(&post_cluster_heartbeat_count_);
  wrs_stat.cluster_heartbeat_succ_tstamp_ = ATOMIC_LOAD(&last_succ_cluster_heartbeat_tstamp_);
  wrs_stat.cluster_heartbeat_succ_count_ = ATOMIC_LOAD(&succ_cluster_heartbeat_count_);

  // self check
  wrs_stat.self_check_tstamp_ = ATOMIC_LOAD(&last_self_check_tstamp_);
  wrs_stat.local_current_tstamp_ = cur_tstamp;

  wrs_stat.cluster_master_ = cluster_service_master_;
  wrs_stat.self_ = self_;

  // cluster info
  cluster_service_.get_cluster_version(current_cluster_version, min_cluster_version, max_cluster_version);
  wrs_stat.cluster_version_ = current_cluster_version;
  wrs_stat.cluster_version_delta_ = in_service? (cur_tstamp - wrs_stat.cluster_version_.convert_to_ts(ignore_invalid)):0;
  wrs_stat.min_cluster_version_ = min_cluster_version;
  wrs_stat.max_cluster_version_ = max_cluster_version;
  wrs_stat.cluster_version_gen_tstamp_ = ATOMIC_LOAD(&last_generate_cluster_version_tstamp_);

  // get server count
  wrs_stat.cluster_servers_count_ = cluster_service_.get_cluster_registered_server_count();
  wrs_stat.cluster_skipped_servers_count_ = cluster_service_.get_cluster_skipped_server_count();

  // cluster leader info
  wrs_stat.in_cluster_service_ = in_service?1:0;
  wrs_stat.cluster_service_epoch_ = master_epoch;
  wrs_stat.is_cluster_master_ = cluster_service_.is_service_master()?1:0;
}

void ObTenantWeakReadService::refresh_cluster_service_master_()
{
  int ret = OB_SUCCESS;
  int64_t cur_tstamp = ObTimeUtility::current_time();
  const int64_t last_refresh_tstamp = ATOMIC_LOAD(&last_refresh_locaction_cache_tstamp_);

  if (OB_NOT_NULL(GCTX.location_service_)
      && cur_tstamp - last_refresh_tstamp > REFRESH_LOCATION_CACHE_INTERVAL) {
    ObTabletID tablet_id = cluster_service_.get_cluster_service_tablet_id();
    const int64_t cluster_id = obrpc::ObRpcNetHandler::CLUSTER_ID;
    ObLSID ls_id;
    if (OB_FAIL(GCTX.location_service_->nonblock_get(MTL_ID(), tablet_id, ls_id))) {
      LOG_WARN("get lsid error", K(ret), K(tablet_id), "tenant_id", MTL_ID());
    } else if (OB_FAIL(GCTX.location_service_->nonblock_renew(cluster_id, MTL_ID(), ls_id))) {
      LOG_WARN("nonblock renew error", K(ret), K(cluster_id), "tennant_id", MTL_ID(), K(ls_id));
    } else {
      // do nothing
    }

    cur_tstamp = ObTimeUtility::current_time();
    ATOMIC_SET(&last_refresh_locaction_cache_tstamp_, cur_tstamp);
  }
}

void ObTenantWeakReadService::set_force_self_check_(bool need_stop_service)
{
  if (OB_UNLIKELY(need_stop_service)) {
    cluster_service_.stop_service();
  }
  ATOMIC_SET(&force_self_check_, true);
  // signal work threads self check
  thread_cond_.signal();
}

int ObTenantWeakReadService::process_get_cluster_version_rpc(SCN &version)
{
  int ret = OB_SUCCESS;
  bool only_request_local = true;
  if (OB_FAIL(get_cluster_version_internal_(version, only_request_local))) {
    LOG_WARN("get cluster version from local fail when process RPC", KR(ret), K(tenant_id_));
    if (OB_NOT_IN_SERVICE == ret || OB_NOT_MASTER == ret) {
      // if receive any RPC and self is not in service, maybe self is wrs leader and just takeover
      // need force self check
    }
  } else {
    // success
  }
  return ret;
}

void ObTenantWeakReadService::process_cluster_heartbeat_rpc_cb(
    const obrpc::ObRpcResultCode &rcode,
    const common::ObAddr &dst)
{
  const int err_code = rcode.rcode_;
  if (OB_SUCCESS == err_code) {
    // success
    int64_t cur_tstamp = ObTimeUtility::current_time();
    int64_t delta =  cur_tstamp - last_succ_cluster_heartbeat_tstamp_;
    if (delta > 500 * 1000L) {
      LOG_WARN_RET(OB_ERR_UNEXPECTED, "tenant weak read service cluster heartbeat cost too much time",
          K(tenant_id_), K(delta), K(last_succ_cluster_heartbeat_tstamp_));
    }

    ATOMIC_INC(&succ_cluster_heartbeat_count_);
    ATOMIC_SET(&last_succ_cluster_heartbeat_tstamp_, cur_tstamp);
  } else {
    int ret = err_code;
    LOG_WARN("tenant weak read service cluster heartbeat RPC fail", K(ret), K(rcode), K(tenant_id_),
        K(dst), "cluster_service_tablet_id", cluster_service_.get_cluster_service_tablet_id());
    // force refresh cluster service master
    refresh_cluster_service_master_();
  }
}

int ObTenantWeakReadService::process_cluster_heartbeat_rpc(const common::ObAddr &svr,
    const SCN version,
    const int64_t valid_part_count,
    const int64_t total_part_count,
    const int64_t generate_timestamp)
{
  int ret = OB_SUCCESS;
  bool need_self_check = false;
  if (OB_UNLIKELY(! inited_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(cluster_service_.update_server_version(svr, version, valid_part_count,
      total_part_count, generate_timestamp))) {
    // self is not in service
    if (OB_NOT_IN_SERVICE == ret) {
      LOG_WARN("process cluster heartbeat rpc: self is not in cluster service",
          KR(ret), K(tenant_id_), K(svr), K(version), K(valid_part_count), K(total_part_count),
          K(generate_timestamp));
      need_self_check = true;
    } else if (OB_NOT_MASTER == ret) {
      // self is in service but not wrs leader
      LOG_WARN("process cluster heartbeat rpc: self is not cluster service master",
          KR(ret), K(tenant_id_), K(svr), K(version), K(valid_part_count), K(total_part_count),
          K(generate_timestamp));
      need_self_check = true;
    } else {
      LOG_WARN("cluster service update server version fail", KR(ret), K(tenant_id_), K(svr),
          K(version), K(valid_part_count), K(total_part_count), K(generate_timestamp));
    }
  } else {
    // success
  }

  if (need_self_check) {
    // if receive any RPC and self is not in service, maybe self is wrs leader and just takeover
    // need force self check
    bool need_stop_service = false;
    set_force_self_check_(need_stop_service);
  }
  return ret;
}

void ObTenantWeakReadService::cluster_service_self_check_()
{
  cluster_service_.self_check();
  ATOMIC_SET(&force_self_check_, false);
  ATOMIC_SET(&last_self_check_tstamp_, ObTimeUtility::current_time());
}

void ObTenantWeakReadService::print_stat_()
{
  int get_cluster_version_err = 0;
  int64_t cur_tstamp = ObTimeUtility::current_time();
  ObTenantWeakReadServerVersionMgr::ServerVersion sv;
  SCN cluster_version;
  SCN min_cluster_version;
  SCN max_cluster_version;
  bool in_cluster_service = cluster_service_.is_in_service();
  int64_t weak_read_refresh_interval = GCONF.weak_read_version_refresh_interval;

  svr_version_mgr_.get_version(sv);
  if (in_cluster_service) {
    get_cluster_version_err = cluster_service_.get_cluster_version(cluster_version, min_cluster_version,
        max_cluster_version);
  }

  const bool ignore_invalid = true;
  ISTAT("[STAT]", K_(tenant_id),
      "server_version", sv,
      "server_version_delta", cur_tstamp - sv.version_.convert_to_ts(ignore_invalid),
      K(in_cluster_service),
      K(cluster_version),
      K(min_cluster_version),
      K(max_cluster_version),
      K(get_cluster_version_err),
      "cluster_version_delta", (in_cluster_service ? cur_tstamp - cluster_version.convert_to_ts(ignore_invalid) : -1),
      K_(cluster_service_master),
      "cluster_service_tablet_id", cluster_service_.get_cluster_service_tablet_id(),
      K_(post_cluster_heartbeat_count),
      K_(succ_cluster_heartbeat_count),
      K_(cluster_heartbeat_interval),
      K_(local_cluster_version),
      "local_cluster_delta", cur_tstamp - local_cluster_version_.convert_to_ts(ignore_invalid),
      K_(force_self_check),
      K(weak_read_refresh_interval));
}

void ObTenantWeakReadService::do_thread_task_(const int64_t begin_tstamp,
    int64_t &last_print_stat_ts)
{
  bool need_print = false;
  static const int64_t PRINT_INTERVAL = 5 * 1000 * 1000L;
  if (REACH_TIME_INTERVAL(PRINT_INTERVAL)) {
    need_print = true;
  } else {
    need_print = false;
  }

  DEFINE_TIME_GUARD("thread_timer_task", THREAD_RUN_INTERVAL);

  if (begin_tstamp - last_print_stat_ts > PRINT_INTERVAL) {
    print_stat_();
    last_print_stat_ts = begin_tstamp;
    time_guard.click("print_stat");
  }

  if (need_cluster_heartbeat_(begin_tstamp)) {
    do_cluster_heartbeat_();
    time_guard.click("do_cluster_heartbeat");
  }

  if (need_generate_cluster_version_(begin_tstamp)) {
    generate_cluster_version_();
    time_guard.click("generate_cluster_version");
  }

  if (need_self_check_(begin_tstamp)) {
    cluster_service_self_check_();
    time_guard.click("cluster_service_self_check");
  }

  // generate weak read timestamp
  generate_tenant_weak_read_timestamp_(need_print);
}

void ObTenantWeakReadService::generate_tenant_weak_read_timestamp_(bool need_print)
{
  int ret = OB_SUCCESS;
  storage::ObLSService *ls_svr = MTL(storage::ObLSService*);
  int64_t start_time = ObTimeUtility::current_time();

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MTL ObLSService is NULL", KR(ret), KP(ls_svr), K_(tenant_id));
  } else if (OB_FAIL(update_server_version_epoch_tstamp_(start_time))) {
    LOG_WARN("update server version epoch timestmap fail", KR(ret), K(start_time));
  } else if (OB_FAIL(scan_all_ls_(ls_svr))) {
    LOG_WARN("scan all partition fail", KR(ret));
  } else if (OB_FAIL(generate_server_version(server_version_epoch_tstamp_, need_print))) {
    LOG_WARN("generate server version for tenant fail", KR(ret), K_(tenant_id), K(index),
                                                        K(server_version_epoch_tstamp_));
  } else {
    // do nothing
  }
}

int ObTenantWeakReadService::update_server_version_epoch_tstamp_(const int64_t cur_time)
{
  if (cur_time > server_version_epoch_tstamp_) {
    server_version_epoch_tstamp_ = cur_time;
  } else {
    server_version_epoch_tstamp_++;
  }
  return OB_SUCCESS;
}

int ObTenantWeakReadService::scan_all_ls_(storage::ObLSService *ls_svr)
{
  int ret = OB_SUCCESS;
  common::ObSharedGuard<storage::ObLSIterator> iter;

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected ls service", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls_iter(iter, ObLSGetMod::TRANS_MOD))) {
    if (OB_NOT_RUNNING != ret) {
      LOG_WARN("fail to alloc ls iter", KR(ret));
    }
  } else {
    int64_t start_time = ObTimeUtility::current_time();
    while (OB_SUCCESS == ret) {
      ObLS *ls = NULL;
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          // do nothing
        } else {
          LOG_WARN("iterate next ls fail", KR(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_PARTITION_NOT_EXIST;
        LOG_WARN("iterate ls fail", KP(ls));
      } else if (OB_FAIL(handle_ls_(*ls))) {
        LOG_WARN("handle ls fail", "ls_id", ls->get_ls_id(), K(ret));
        ret = OB_SUCCESS;
      } else {
        // success
      }

      int64_t cur_time = ObTimeUtility::current_time();
      if (cur_time - start_time > 10000) {
        // too many ls, time cost too much in single scan
        // need sleep to prevent CPU cost too much
        //
        const uint32_t sleep_time = 1000; // 1ms
        ob_usleep(sleep_time); // 1ms
        start_time = cur_time + sleep_time;
      }
    } // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }

  return ret;
}

int ObTenantWeakReadService::handle_ls_(ObLS &ls)
{
  int ret = OB_SUCCESS;
  SCN version;
  bool need_skip = false;
  bool is_user_ls = false;
  const ObLSID &ls_id = ls.get_ls_id();
  uint64_t tenant_id = ls.get_tenant_id();

  // tenant maybe not exist, pass second parameter to ignore WARN log
  int64_t max_stale_time = ObWeakReadUtil::max_stale_time_for_weak_consistency(tenant_id,
      ObWeakReadUtil::IGNORE_TENANT_EXIST_WARN);
  // generate ls level weak read snapshot version
  if (OB_FAIL(ls.get_ls_wrs_handler()
                ->generate_ls_weak_read_snapshot_version(ls,
                                                         need_skip,
                                                         is_user_ls,
                                                         version,
                                                         max_stale_time))) {
    LOG_WARN("generate ls weak read snapshot version fail", KR(ret), K(ls_id));
  } else if (OB_UNLIKELY(!need_skip && !version.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("unexpected generated ls weak read snapshot version", KR(ret),
              K(ls_id), K(is_user_ls), K(version));
  } else if (OB_FAIL(update_server_version_with_part_info(server_version_epoch_tstamp_,
                                                          need_skip, is_user_ls, version))) {
    LOG_WARN("update tenant server version with ls info fail", KR(ret),
            K(server_version_epoch_tstamp_), K(need_skip), K(is_user_ls), K(version));
  } else {
    // do nothing
  }
  return ret;
}

// Tenant work thread
void ObTenantWeakReadService::run1()
{
  int64_t last_print_stat_ts = 0;
  ISTAT("thread start", K_(tenant_id));
  lib::set_thread_name("TenantWeakReadService");
  while (!has_set_stop()) {
    int64_t weak_read_refresh_interval = GCONF.weak_read_version_refresh_interval;
    int64_t begin_tstamp = ObTimeUtility::current_time();
    do_thread_task_(begin_tstamp, last_print_stat_ts);
    int64_t end_tstamp = ObTimeUtility::current_time();
    int64_t wait_interval = std::min(ObWeakReadUtil::replica_keepalive_interval(),
                                     weak_read_refresh_interval - (end_tstamp - begin_tstamp));
    if (wait_interval > 0) {
      thread_cond_.timedwait(wait_interval);
    }
  }
  ISTAT("thread end", K_(tenant_id));
}

bool ObTenantWeakReadService::check_can_skip_ls(ObLS *ls)
{
  bool bool_ret = false;
  int ret = OB_SUCCESS;
  share::SCN offline_scn;

  if (OB_NOT_NULL(ls)) {
    if (ls->get_ls_wrs_handler()->can_skip_ls()) {
      bool_ret = true;
    } else if (OB_SUCC(ls->get_offline_scn(offline_scn)) && offline_scn.is_valid()) {
      bool_ret = true;
      FLOG_INFO("ls offline scn is valid, skip it", K(ls->get_ls_id()), K(offline_scn));
    } else {
      bool_ret = false;
    }
  }

  return bool_ret;
}

int ObTenantWeakReadService::check_can_start_service(const SCN &current_gts,
                                                     bool &can_start_service,
                                                     SCN &min_version,
                                                     share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  storage::ObLSService *ls_svr = MTL(storage::ObLSService*);
  common::ObSharedGuard<storage::ObLSIterator> iter;
  ObLSID tmp_ls_id;
  SCN tmp_min_version;
  int64_t total_ls_cnt = 0;
  can_start_service = true;
  const int64_t MAX_STALE_TIME = 30 * 1000 * 1000;

  if (OB_ISNULL(ls_svr)) {
    ret = OB_ERR_UNEXPECTED;
    FLOG_ERROR("unexpected ls service", K(ret), KP(ls_svr));
  } else if (OB_FAIL(ls_svr->get_ls_iter(iter, ObLSGetMod::TRANS_MOD))) {
    if (OB_NOT_RUNNING != ret) {
      FLOG_WARN("fail to alloc ls iter", KR(ret));
    }
  } else {
    int64_t start_time = ObTimeUtility::current_time();
    while (OB_SUCCESS == ret) {
      ObLS *ls = NULL;
      if (OB_FAIL(iter->get_next(ls))) {
        if (OB_ITER_END == ret) {
          // do nothing
        } else {
          FLOG_WARN("iterate next ls fail", KR(ret));
        }
      } else if (OB_ISNULL(ls)) {
        ret = OB_PARTITION_NOT_EXIST;
        FLOG_WARN("iterate ls fail", KP(ls));
      } else if (check_can_skip_ls(ls)) {
        // do nothing
      } else {
        ++total_ls_cnt;
        if (tmp_min_version.is_valid()) {
          if (ls->get_ls_wrs_handler()->get_ls_weak_read_ts() < tmp_min_version) {
            tmp_min_version = ls->get_ls_wrs_handler()->get_ls_weak_read_ts();
            tmp_ls_id = ls->get_ls_id();
          }
        } else {
          tmp_min_version = ls->get_ls_wrs_handler()->get_ls_weak_read_ts();
          tmp_ls_id = ls->get_ls_id();
        }
      }
    } // while

    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (total_ls_cnt == 0) {
      can_start_service = true;
      FLOG_INFO("empty ls, no need to wait replaying log", K(current_gts), K(tmp_min_version), K(ls_id));
    } else if (OB_SUCC(ret)) {
      min_version = tmp_min_version;
      ls_id = tmp_ls_id;
      can_start_service = (tmp_min_version.is_valid() &&
                           current_gts.convert_to_ts() - MAX_STALE_TIME < tmp_min_version.convert_to_ts());
      if (!can_start_service) {
        FLOG_WARN("current ls can not start service, waiting for replaying log",
            "target_ts", current_gts.convert_to_ts(),
            "min_ts", tmp_min_version.convert_to_ts(),
            "delta_us", (current_gts.convert_to_ts() - tmp_min_version.convert_to_ts()),
            K(ls_id));
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

void ObTenantWeakReadService::set_cluster_service_master_(const ObAddr &addr)
{
  if (cluster_service_master_ != addr) {
    cluster_service_master_ = addr;
    ISTAT("cluster service master changed", K_(tenant_id), K_(cluster_service_master),
        "cluster_service_tablet_id", cluster_service_.get_cluster_service_tablet_id());
  }
}

// Cluster Heartbeat report local server weak read version of user table
void ObTenantWeakReadService::do_cluster_heartbeat_()
{
  int ret = OB_SUCCESS;
  int64_t total_part_count = 0, valid_part_count = 0;
  int64_t cur_tstamp = ObTimeUtility::current_time();
  // version generation timestamp
  // FIXME: if current timestamp not increase, self will be the bottleneck
  int64_t generate_timestamp = cur_tstamp;
  // default need send cluster heartbeat
  bool need_cluster_heartbeat_rpc = true;

  DEFINE_TIME_GUARD("do_cluster_heartbeat", 100 * 1000L);

  // get local server version
  SCN local_server_version = svr_version_mgr_.get_version(total_part_count, valid_part_count);

  time_guard.click("get_server_version");

  // if selef is Cluster Service, the local is preferred
  if (cluster_service_.is_in_service()) {
    if (OB_FAIL(process_cluster_heartbeat_rpc(self_, local_server_version, valid_part_count,
        total_part_count, generate_timestamp))) {
      LOG_WARN("process local cluster heartbeat request fail", KR(ret), K(self_),
          K(local_server_version), K(valid_part_count), K(total_part_count), K(generate_timestamp));

      if (OB_NOT_IN_SERVICE == ret || OB_NOT_MASTER == ret) {
        // If the local is not in service, or is no longer the MASTER, then send RPC
        ret = OB_SUCCESS;
        LOG_INFO("local cluster service is not in service or not master, request cluster heartbeat "
            "to remote cluster service through RPC", K(tenant_id_), K(local_server_version),
            K(valid_part_count), K(total_part_count), K(generate_timestamp));
      }
    } else {
      time_guard.click("do_local_cluster_heartbeat");
      need_cluster_heartbeat_rpc = false;
      set_cluster_service_master_(self_);
    }
  }

  if (OB_SUCCESS == ret && need_cluster_heartbeat_rpc) {
    // send cluster heartbeat RPC
    if (OB_FAIL(post_cluster_heartbeat_rpc_(local_server_version, valid_part_count,
        total_part_count, generate_timestamp))) {
      LOG_WARN("post cluster heartbeat rpc fail", KR(ret), K(tenant_id_), K(local_server_version),
          K(valid_part_count), K(total_part_count), K(generate_timestamp));
    }

    time_guard.click("post_cluster_heartbeat_rpc");
  }

  if (OB_SUCCESS != ret) {
    LOG_WARN("tenant weak read service do cluster heartbeat fail", KR(ret), K(tenant_id_),
        K(last_post_cluster_heartbeat_tstamp_), K(cluster_heartbeat_interval_),
        "cluster_service_tablet_id", cluster_service_.get_cluster_service_tablet_id(),
        K_(cluster_service_master));

    // increase the interval in case of failure
    cluster_heartbeat_interval_ = cluster_heartbeat_interval_ * 2;
    if (cluster_heartbeat_interval_ > MAX_CLUSTER_HEARTBEAT_INTERVAL) {
      cluster_heartbeat_interval_ = MAX_CLUSTER_HEARTBEAT_INTERVAL;
    }
  } else {
    // reset to the minimum value after success
    cluster_heartbeat_interval_ = MIN_CLUSTER_HEARTBEAT_INTERVAL;
    ATOMIC_INC(&post_cluster_heartbeat_count_);
  }

  // force update post timestamp
  // FIXME: in case of update failure, the time is also updated, to avoid frequent retries
  ATOMIC_SET(&last_post_cluster_heartbeat_tstamp_, ObTimeUtility::current_time());
}

int ObTenantWeakReadService::post_cluster_heartbeat_rpc_(const SCN version,
    const int64_t valid_part_count,
    const int64_t total_part_count,
    const int64_t generate_timestamp)
{
  int ret = OB_SUCCESS;
  ObAddr cluster_service_master;
  ObWrsClusterHeartbeatRequest req;
  req.set(self_, version, valid_part_count, total_part_count, generate_timestamp);
  if (OB_ISNULL(wrs_rpc_)) {
    ret = OB_NOT_INIT;
  } else if (OB_FAIL(get_cluster_service_master_(cluster_service_master))) {
    LOG_TRACE("get cluster service master fail", KR(ret), K(tenant_id_),
        "cluster_service_tablet_id", cluster_service_.get_cluster_service_tablet_id());
  } else if (OB_FAIL(wrs_rpc_->post_cluster_heartbeat(cluster_service_master, tenant_id_, req))) {
    LOG_WARN("post cluster heartbeat fail", KR(ret), K(cluster_service_master), K(tenant_id_),
        K(req), "cluster_service_tablet_id", cluster_service_.get_cluster_service_tablet_id());
  } else {
    // success
    LOG_DEBUG("post cluster heartbeat success", K_(tenant_id), K(cluster_service_master), K(req),
        K_(last_post_cluster_heartbeat_tstamp));
    set_cluster_service_master_(cluster_service_master);
  }

  if (OB_SUCCESS != ret) {
    // call the RPC processing fucntion directly in case of sending failure,
    // to facilitate the unified processing of the failure
    obrpc::ObRpcResultCode rcode;
    rcode.rcode_ = ret;
    (void)snprintf(rcode.msg_, sizeof(rcode.msg_), "post cluster heartbeat rpc failed, "
        "tenant_id=%lu", tenant_id_);
    process_cluster_heartbeat_rpc_cb(rcode, self_);
  }
  return ret;
}

// weak_read_version_refresh_interval is 0 means wrs close, no longer need send heartbeat
bool ObTenantWeakReadService::need_cluster_heartbeat_(const int64_t cur_tstamp)
{
  bool ret = ObWeakReadUtil::check_weak_read_service_available();
  if (ret) {
    ret = (cur_tstamp - last_post_cluster_heartbeat_tstamp_) > GCONF.weak_read_version_refresh_interval;
  } else {
    //nothing
  }
  return ret;
}

// weak_read_version_refresh_interval is 0 means wrs close, no longer need generate cluster wrs version
bool ObTenantWeakReadService::need_generate_cluster_version_(const int64_t cur_tstamp)
{
  bool ret = ObWeakReadUtil::check_weak_read_service_available();
  if (ret) {
    int64_t delta = (cur_tstamp - last_generate_cluster_version_tstamp_);
    ret = cluster_service_.is_in_service() && (delta > GCONF.weak_read_version_refresh_interval);
  } else {
    //nothing
  }
  return ret;
}

// if not wrs leader or not in service or need retry in process of generating cluster version
// need force self check
// if need retry and affected_row is 0 in persisting __all_weak_read_service
// need restart service
// bug:
bool ObTenantWeakReadService::need_force_self_check_(int ret,
    int64_t affected_rows,
    bool &need_stop_service)
{
  int need_self_check = false;
  if (OB_NOT_IN_SERVICE == ret || OB_NOT_MASTER == ret || OB_NEED_RETRY == ret) {
    need_self_check = true;
  }
  if (OB_UNLIKELY(OB_NEED_RETRY == ret && 0 == affected_rows)) {
    need_stop_service = true;
  } else {
    need_stop_service = false;
  }
  return need_self_check;
}
void ObTenantWeakReadService::generate_cluster_version_()
{
  int ret = OB_SUCCESS;
  int64_t affected_rows = 0;
  if (OB_FAIL(cluster_service_.update_cluster_version(affected_rows))) {
    bool need_stop_service = false;
    bool need_self_check = need_force_self_check_(ret, affected_rows, need_stop_service);
    if (need_self_check) {
      set_force_self_check_(need_stop_service);
    }
  } else {
    ATOMIC_SET(&last_generate_cluster_version_tstamp_, ObTimeUtility::current_time());
  }
}

bool ObTenantWeakReadService::need_self_check_(const int64_t cur_tstamp)
{
  int64_t self_check_delta = (cur_tstamp - last_self_check_tstamp_);
  return (ATOMIC_LOAD(&force_self_check_) || (self_check_delta > SELF_CHECK_INTERVAL));
}

int ObTenantWeakReadService::mtl_init(ObTenantWeakReadService* &twrs)
{
  int ret = OB_SUCCESS;
  ObMySQLProxy *mysql_proxy = GCTX.sql_proxy_;
  ObIWeakReadService *wrs = GCTX.weak_read_service_;
  const ObAddr &self = GCTX.self_addr();
  uint64_t tenant_id = MTL_ID();

  // virtual tenant NOT need weak read service
  if (is_virtual_tenant_id(tenant_id)) {
    LOG_WARN("[WRS] virtual tenant do not need tenant weak read service, need not init", K(tenant_id));
  } else if (OB_ISNULL(mysql_proxy) || OB_ISNULL(wrs)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("modules not ready, unexpected", KR(ret), K(mysql_proxy), K(wrs));
  } else if (OB_FAIL(twrs->init(tenant_id, *mysql_proxy, wrs->get_wrs_rpc(), self))) {
    LOG_ERROR("init tenant weak read service instance fail", KR(ret), K(tenant_id), K(mysql_proxy),
        K(wrs), K(self));
  } else {
    // success
  }

  if (OB_FAIL(ret) && NULL != twrs) {
    OB_DELETE(ObTenantWeakReadService, unused, twrs);
    twrs = NULL;
  }
  return ret;
}

/////////////////////////// ObTenantWeakReadService::ModuleInfo ////////////////////////////

ObTenantWeakReadService::ModuleInfo::ModuleInfo(ObTenantWeakReadService &twrs, const char *module)
{
  int64_t pos = 0;
  (void)databuff_printf(buf_, sizeof(buf_), pos, MOD_STR "tenant_id=%lu, %s", twrs.tenant_id_,
      module);
}

ObTenantWeakReadService::ModuleInfo::~ModuleInfo()
{
  memset(buf_, 0, sizeof(buf_)/sizeof(buf_[0]));
}
}
}
