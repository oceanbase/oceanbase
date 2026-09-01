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

#include "storage/tx_storage/ob_log_replica_checkpoint_ctx.h"

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log.h"
#include "lib/time/ob_time_utility.h"
#include "logservice/ob_log_handler.h"
#include "logservice/ob_log_service.h"
#include "observer/ob_server_struct.h"
#include "rootserver/ob_tenant_info_loader.h"
#include "share/location_cache/ob_location_service.h"
#include "share/ob_rpc_share.h"
#include "share/ob_share_util.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_get_ls_replica_checkpoint_info_rpc_proxy.h"

namespace oceanbase
{
using namespace share;
namespace storage
{
namespace checkpoint
{

#ifdef ERRSIM
bool need_errsim_block_clog_checkpoint(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id)
{
  int ret = OB_SUCCESS;
  bool need_block = false;
  const int64_t errsim_ls_id = GCONF.errsim_block_clog_checkpoint_ls_id;
  const common::ObString &errsim_server = GCONF.errsim_block_clog_checkpoint_server_addr.str();
  common::ObAddr errsim_addr;

  if (errsim_ls_id <= 0
      || !ls_id.is_valid()
      || ls_id.id() != errsim_ls_id
      || errsim_server.empty()) {
    // do nothing
  } else if (OB_FAIL(errsim_addr.parse_from_string(errsim_server))) {
    STORAGE_LOG(WARN, "failed to parse errsim block clog checkpoint server addr",
        KR(ret), K(errsim_server), K(tenant_id), K(ls_id));
  } else if (GCTX.self_addr() == errsim_addr) {
    need_block = true;
  }
  return need_block;
}
#endif

const int64_t LOG_REPLICA_CHECKPOINT_DELAY_ERROR_THRESHOLD_US =
    2LL * 60LL * 60LL * 1000LL * 1000LL;
const int64_t LOG_REPLICA_CHECKPOINT_DELAY_ERROR_REPORT_INTERVAL_US =
    10LL * 60LL * 1000LL * 1000LL;
const int64_t LOG_REPLICA_CHECKPOINT_DELAY_REPORT_MAP_BUCKET_NUM = 64;
const palf::offset_t LOG_REPLICA_CHECKPOINT_DISK_PRESSURE_SAFE_RATIO_DENOMINATOR = 20;

ObLogReplicaCheckpointCtx::ObLogReplicaCheckpointCtx(ObLS &ls)
  : ls_(ls),
    is_strict_clog_recycle_mode_(false),
    target_checkpoint_scn_(SCN::max_scn()),
    majority_min_replica_checkpoint_scn_(SCN::max_scn()),
    pure_readable_scn_(SCN::max_scn()),
    local_end_scn_(SCN::max_scn()),
    disk_pressure_safe_checkpoint_scn_(SCN::invalid_scn())
{}

ObLogReplicaCheckpointCtx::~ObLogReplicaCheckpointCtx()
{
  is_strict_clog_recycle_mode_ = false;
  target_checkpoint_scn_ = SCN::max_scn();
  majority_min_replica_checkpoint_scn_ = SCN::max_scn();
  pure_readable_scn_ = SCN::max_scn();
  local_end_scn_ = SCN::max_scn();
  disk_pressure_safe_checkpoint_scn_ = SCN::invalid_scn();
}

int ObLogReplicaCheckpointCtx::update_checkpoint(const bool log_disk_pressure)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(cal_logonly_replica_checkpoint_(log_disk_pressure))) {
    LOG_WARN("fail to cal logonly replica checkpoint", KR(ret), K(ls_.get_ls_id()), KPC(this));
  } else if (OB_FAIL(update_log_replica_checkpoint_())) {
    LOG_WARN("fail to update log replica checkpoint", KR(ret), K(ls_.get_ls_id()), KPC(this));
  }
  return ret;
}

int ObLogReplicaCheckpointCtx::cal_logonly_replica_checkpoint_(const bool log_disk_pressure)
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const ObLSID &ls_id = ls_.get_ls_id();
  rootserver::ObTenantInfoLoader *tenant_info_loader = MTL(rootserver::ObTenantInfoLoader *);
  logservice::ObILogHandler *log_handler = ls_.get_log_handler();
  is_strict_clog_recycle_mode_ =
      ObServerConfig::get_instance()._ob_enable_log_replica_strict_recycle_mode;
  // When clog recycle mode is strict (which is default value), we set clog recycle
  // point at pure_readable_scn. But pure_readable_scn can not promoted when
  // majority of F-replicas are down, thus making clog can not be recycled
  // In this case, we can set clog strict recycle mode to false.
  // When clog recycle mode is not strict, we set clog recycle point at sync_scn
  // sync_scn can promoted when majority of F-replicas are down
  if (OB_ISNULL(tenant_info_loader) || OB_ISNULL(log_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("tenant_info_loader is null or log handler is null",
        KR(ret), KP(tenant_info_loader), K(ls_id));
  } else if (OB_FAIL(log_handler->get_end_scn(local_end_scn_))) {
    LOG_WARN("fail to get local end scn", KR(ret), K(ls_id));
  } else if (!is_strict_clog_recycle_mode_) {
    // not strict recycle mode, recycle clog based on sync_scn
    if (OB_FAIL(tenant_info_loader->get_sync_scn(target_checkpoint_scn_))) {
      LOG_WARN("fail to get tenant sync scn", KR(ret));
    }
  } else {
    // is strict recycle mode, recycle clog based on pure_readable_scn
    if (OB_FAIL(tenant_info_loader->get_pure_readable_scn(pure_readable_scn_))) {
      LOG_WARN("fail to get tenant readable scn", KR(ret));
    } else if (OB_FAIL(cal_logonly_replica_checkpoint_strict_mode_(log_disk_pressure))) {
      LOG_WARN("fail to cal checkpoint scn in strict mode", KR(ret),
          K(tenant_id), K(ls_id), KPC(this));
    }
  }
  return ret;
}

int ObLogReplicaCheckpointCtx::get_majority_min_replica_checkpoint_scn_from_leader_(
    const uint64_t tenant_id,
    const share::ObLSID &ls_id,
    share::SCN &checkpoint_scn)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  checkpoint_scn = SCN::max_scn();
  obrpc::ObGetLSReplicaCheckpointInfoRpcProxy rpc_proxy;
  if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id) || !ls_id.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", KR(ret), K(tenant_id), K(ls_id));
  } else if (OB_ISNULL(GCTX.location_service_)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "location service is null", KR(ret), KP(GCTX.location_service_));
  } else if (OB_FAIL(share::init_obrpc_proxy(rpc_proxy))) {
    STORAGE_LOG(WARN, "failed to init replica checkpoint rpc proxy", KR(ret));
  } else {
    ObTimeoutCtx ctx;
    common::ObAddr leader;
    obrpc::ObGetLSReplicaCheckpointInfoArg arg;
    obrpc::ObGetLSReplicaCheckpointInfoRes result;
    if (OB_FAIL(share::ObShareUtil::set_default_timeout_ctx(ctx, GCONF.rpc_timeout))) {
      STORAGE_LOG(WARN, "fail to set timeout ctx", KR(ret));
    } else if (OB_FAIL(arg.init(tenant_id, ls_id))) {
      STORAGE_LOG(WARN, "failed to init arg", KR(ret), K(tenant_id), K(ls_id));
    } else if (OB_FAIL(GCTX.location_service_->nonblock_get_leader(
            GCONF.cluster_id, tenant_id, ls_id, leader))) {
      STORAGE_LOG(WARN, "failed to get ls leader", KR(ret), K(tenant_id), K(ls_id));
    } else if (!leader.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "leader is not valid", KR(ret), K(tenant_id), K(ls_id), K(leader));
    } else if (OB_FAIL(rpc_proxy.to(leader)
                           .by(tenant_id)
                           .timeout(ctx.get_timeout())
                           .get_ls_replica_checkpoint_info(arg, result))) {
      STORAGE_LOG(WARN, "failed to get replica checkpoint info", KR(ret),
          K(leader), K(tenant_id), K(ls_id), K(ctx), K(arg));
    } else if (!result.is_valid()) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "invalid RPC result", KR(ret), K(leader),
          K(tenant_id), K(ls_id), K(result));
    } else {
      checkpoint_scn = result.get_checkpoint_scn();
      STORAGE_LOG(INFO, "get majority min replica checkpoint scn from leader",
          K(tenant_id), K(ls_id), K(leader), K(checkpoint_scn));
    }
    if (OB_FAIL(ret) && OB_NEED_RETRY != ret && OB_EAGAIN != ret
        && OB_TMP_FAIL(GCTX.location_service_->nonblock_renew(
            GCONF.cluster_id, tenant_id, ls_id))) {
      STORAGE_LOG(WARN, "nonblock_renew ls leader failed",
          KR(ret), KR(tmp_ret), K(tenant_id), K(ls_id));
    }
  }
  return ret;
}

int ObLogReplicaCheckpointCtx::cal_logonly_replica_checkpoint_strict_mode_(
    const bool log_disk_pressure)
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = ls_.get_ls_id();
  bool is_enabled = false;
  target_checkpoint_scn_ = SCN::min(local_end_scn_, pure_readable_scn_);
  if (OB_FAIL(share::ObShareUtil::check_majority_min_replica_checkpoint_enabled(
      tenant_id, is_enabled))) {
    LOG_WARN("fail to check whether majority min replica checkpoint is enabled",
        KR(ret), K(tenant_id));
  } else if (!is_enabled) {
    LOG_INFO("use pure readable scn before replica checkpoint limit is available",
        K(tenant_id), K(ls_id), KPC(this));
  } else if (OB_TMP_FAIL(get_majority_min_replica_checkpoint_scn_from_leader_(
                 tenant_id, ls_id, majority_min_replica_checkpoint_scn_))) {
    LOG_WARN("fail to get majority min replica checkpoint scn", KR(tmp_ret),
        K(tenant_id), K(ls_id), KPC(this));
    target_checkpoint_scn_ = ls_.get_clog_checkpoint_scn();
  } else {
    target_checkpoint_scn_ = SCN::min(
        majority_min_replica_checkpoint_scn_, target_checkpoint_scn_);
  }
  // The pressure path is best effort. Keep the normally calculated target on
  // a transient PALF race and retry the safe-point calculation next round.
  if (log_disk_pressure && is_enabled
      && OB_TMP_FAIL(raise_target_checkpoint_for_disk_pressure_())) {
    LOG_WARN("fail to raise log replica checkpoint target under log disk pressure",
        KR(tmp_ret), K(tenant_id), K(ls_id), KPC(this));
  }
  return ret;
}

int ObLogReplicaCheckpointCtx::raise_target_checkpoint_for_disk_pressure_()
{
  int ret = OB_SUCCESS;
  const share::ObLSID &ls_id = ls_.get_ls_id();
  logservice::ObILogHandler *log_handler = ls_.get_log_handler();
  palf::LSN palf_disk_begin_lsn = palf::LSN();
  palf::LSN local_end_lsn = palf::LSN();
  palf::LSN disk_pressure_safe_lsn = palf::LSN();
  if (OB_ISNULL(log_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler is null", KR(ret), K(ls_id));
  } else if (OB_FAIL(log_handler->get_begin_lsn(palf_disk_begin_lsn))) {
    LOG_WARN("fail to get palf disk begin lsn", KR(ret), K(ls_id));
  } else if (OB_FAIL(log_handler->get_end_lsn(local_end_lsn))) {
    LOG_WARN("fail to get local end lsn", KR(ret), K(ls_id));
  } else if (palf_disk_begin_lsn >= local_end_lsn) {
    // There is no retained PALF range to recycle for this LS.
  } else if (OB_FAIL(calculate_disk_pressure_safe_lsn_(
      palf_disk_begin_lsn, local_end_lsn, disk_pressure_safe_lsn))) {
    LOG_WARN("fail to calculate disk pressure safe lsn", KR(ret),
        K(ls_id), K(palf_disk_begin_lsn), K(local_end_lsn));
  } else if (OB_FAIL(log_handler->locate_by_lsn_coarsely(
      disk_pressure_safe_lsn, disk_pressure_safe_checkpoint_scn_))) {
    LOG_WARN("fail to locate disk pressure safe checkpoint scn", KR(ret), K(ls_id),
        K(local_end_lsn), K(disk_pressure_safe_lsn));
  } else if (OB_UNLIKELY(!disk_pressure_safe_checkpoint_scn_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("disk pressure safe checkpoint scn is invalid", KR(ret), K(ls_id),
        K(palf_disk_begin_lsn), K(local_end_lsn), K(disk_pressure_safe_lsn));
  } else {
    const share::SCN checkpoint_upper_bound = SCN::min(pure_readable_scn_, local_end_scn_);
    const share::SCN target_checkpoint_scn_before_raise = target_checkpoint_scn_;
    // L 副本日志盘有压力并且多数派 F 最小checkpoint位点无法回收日志盘时，强制推高 L 副本的checkpoint，目的是避免影响业务
    // 约束 L 副本的回收位点不能越过安全上界 pure_readable_scn_，多数派F的最小可读点。否则影响多数派日志的安全性。
    // 例如2F1L，两个F的可读位点分别是 100/80，那么 L 副本日志回收位点不能越过80，否则 100 位点的F宕机之后，剩下的副本不能恢复
    target_checkpoint_scn_ = SCN::min(
        SCN::max(target_checkpoint_scn_, disk_pressure_safe_checkpoint_scn_),
        checkpoint_upper_bound);
    if (target_checkpoint_scn_ > target_checkpoint_scn_before_raise) {
      LOG_INFO("raise log replica checkpoint target under log disk pressure",
          K(ls_id), K(target_checkpoint_scn_before_raise),
          K(checkpoint_upper_bound), KPC(this));
    }
  }
  return ret;
}

int ObLogReplicaCheckpointCtx::calculate_disk_pressure_safe_lsn_(
    const palf::LSN &palf_disk_begin_lsn,
    const palf::LSN &local_end_lsn,
    palf::LSN &safe_lsn)
{
  int ret = OB_SUCCESS;
  safe_lsn.reset();
  if (OB_UNLIKELY(!palf_disk_begin_lsn.is_valid()
      || !local_end_lsn.is_valid()
      || local_end_lsn < palf_disk_begin_lsn)) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    const palf::offset_t retained_log_size = local_end_lsn - palf_disk_begin_lsn;
    // Select a boundary in the oldest 5% of this replica's retained PALF
    // range. Division by 20 avoids floating-point rounding and multiplication
    // overflow.
    const palf::offset_t safe_advance_size = retained_log_size
        / LOG_REPLICA_CHECKPOINT_DISK_PRESSURE_SAFE_RATIO_DENOMINATOR;
    // To push LogGetRecycableFileCandidate effectively.
    const palf::offset_t min_useful_advance_size = std::max(
        safe_advance_size, 2 * (palf::offset_t)palf::PALF_BLOCK_SIZE);
    safe_lsn = palf_disk_begin_lsn + std::min(retained_log_size, min_useful_advance_size);
  }
  return ret;
}

int ObLogReplicaCheckpointCtx::update_log_replica_checkpoint_()
{
  int ret = OB_SUCCESS;
  const uint64_t tenant_id = MTL_ID();
  const share::ObLSID &ls_id = ls_.get_ls_id();
  logservice::ObILogHandler *log_handler = ls_.get_log_handler();
  palf::LSN palf_base_lsn = palf::LSN();
  share::SCN checkpoint_scn = share::SCN();
  palf::LSN checkpoint_lsn = palf::LSN();
  if (OB_ISNULL(log_handler)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("log handler is null", KR(ret), K(ls_id));
#ifdef ERRSIM
  } else if (need_errsim_block_clog_checkpoint(tenant_id, ls_id)) {
    FLOG_INFO("[ERRSIM] block log replica clog checkpoint", K(tenant_id),
        K(ls_id), KPC(this), "self_addr", GCTX.self_addr());
#endif
  } else {
    checkpoint_scn = SCN::max(target_checkpoint_scn_, ls_.get_clog_checkpoint_scn());
    if (OB_FAIL(log_handler->locate_by_scn_coarsely(checkpoint_scn, checkpoint_lsn))) {
      LOG_WARN("fail to convert scn to lsn", KR(ret), K(ls_id), KPC(this));
    } else if (OB_FAIL(ls_.set_clog_checkpoint(checkpoint_lsn, checkpoint_scn))) {
      LOG_WARN("fail to set clog checkpoint",
          KR(ret), K(ls_id), K(checkpoint_lsn), K(checkpoint_scn), KPC(this));
    } else if (OB_FAIL(log_handler->get_base_lsn(palf_base_lsn))) {
      LOG_WARN("fail to get palf base lsn", KR(ret), K(ls_id), K(palf_base_lsn));
    } else if (checkpoint_lsn <= palf_base_lsn) { // do nothing
    } else if (OB_FAIL(log_handler->advance_base_lsn(checkpoint_lsn))) {
      LOG_WARN("fail to persist clog checkpoint before advancing palf",
          KR(ret), K(ls_id), K(checkpoint_lsn), KPC(this));
    }
  }
  return ret;
}

bool ObLogReplicaCheckpointCtx::get_checkpoint_delay(
    const share::SCN &checkpoint_scn,
    int64_t &checkpoint_delay_us) const
{
  bool has_delay = false;
  checkpoint_delay_us = 0;
  if (checkpoint_scn.is_valid()
      && !checkpoint_scn.is_base_scn()
      && local_end_scn_.is_valid()
      && !local_end_scn_.is_max()
      && local_end_scn_ > checkpoint_scn) {
    checkpoint_delay_us = local_end_scn_.convert_to_ts() - checkpoint_scn.convert_to_ts();
    has_delay = true;
  }
  return has_delay;
}

void ObLogReplicaCheckpointDelayReporter::destroy()
{
  log_disk_pressure_ = false;
  checkpoint_delay_report_info_map_.destroy();
}

void ObLogReplicaCheckpointDelayReporter::prepare_for_next_round()
{
  int ret = OB_SUCCESS;
  remove_expired_report_info_();
  if (OB_FAIL(get_log_disk_pressure_(log_disk_pressure_))) {
    STORAGE_LOG(WARN, "failed to get log disk pressure, suppress checkpoint delay alert",
        KR(ret), "tenant_id", MTL_ID());
  }
}

void ObLogReplicaCheckpointDelayReporter::report_if_needed(
    const share::ObLSID &ls_id,
    const share::SCN &checkpoint_scn,
    const ObLogReplicaCheckpointCtx &checkpoint_ctx)
{
  int ret = OB_SUCCESS;
  int64_t checkpoint_scn_delay_us = 0;
  if (OB_UNLIKELY(checkpoint_ctx.get_checkpoint_delay(checkpoint_scn, checkpoint_scn_delay_us)
      && log_disk_pressure_
      && checkpoint_scn_delay_us > LOG_REPLICA_CHECKPOINT_DELAY_ERROR_THRESHOLD_US
      && need_report_log_replica_checkpoint_delay_(ls_id,
          checkpoint_scn, common::ObTimeUtility::current_monotonic_time(),
          LOG_REPLICA_CHECKPOINT_DELAY_ERROR_REPORT_INTERVAL_US))) {
    ret = OB_ERR_TOO_MUCH_TIME;
    LOG_ERROR("log replica checkpoint scn falls behind local end scn", KR(ret),
        K(ls_id), K(checkpoint_scn_delay_us),
        "threshold_us", LOG_REPLICA_CHECKPOINT_DELAY_ERROR_THRESHOLD_US,
        K(checkpoint_ctx), K_(log_disk_pressure));
  }
}

bool ObLogReplicaCheckpointDelayReporter::need_report_log_replica_checkpoint_delay_(
    const share::ObLSID &ls_id,
    const share::SCN &checkpoint_scn,
    const int64_t current_ts,
    const int64_t report_interval_us)
{
  int ret = OB_SUCCESS;
  bool need_report = false;
  if (OB_UNLIKELY(!ls_id.is_valid()
      || !checkpoint_scn.is_valid()
      || current_ts <= 0
      || report_interval_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(ls_id), K(checkpoint_scn),
        K(current_ts), K(report_interval_us));
  } else if (!checkpoint_delay_report_info_map_.created()
      && OB_FAIL(checkpoint_delay_report_info_map_.create(
          LOG_REPLICA_CHECKPOINT_DELAY_REPORT_MAP_BUCKET_NUM,
          ObMemAttr(MTL_ID(), "CkptDelayReport")))) {
    LOG_WARN("fail to create log replica checkpoint delay report limiter",
        KR(ret));
  } else {
    CheckpointDelayReportInfo report_info;
    const int get_ret = checkpoint_delay_report_info_map_.get_refactored(ls_id, report_info);
    if (OB_HASH_NOT_EXIST == get_ret) {
      report_info.checkpoint_scn_ = checkpoint_scn;
      report_info.report_base_ts_ = current_ts;
      report_info.last_seen_ts_ = current_ts;
    } else if (OB_SUCCESS != get_ret) {
      ret = get_ret;
      LOG_WARN("fail to get log replica checkpoint delay report info",
          KR(ret), K(ls_id));
    } else if (checkpoint_scn != report_info.checkpoint_scn_
        || current_ts <= report_info.last_seen_ts_
        || current_ts <= report_info.report_base_ts_) {
      report_info.checkpoint_scn_ = checkpoint_scn;
      report_info.report_base_ts_ = current_ts;
      report_info.last_seen_ts_ = current_ts;
    } else {
      report_info.last_seen_ts_ = current_ts;
      if (current_ts - report_info.report_base_ts_ >= report_interval_us) {
        report_info.report_base_ts_ = current_ts;
        need_report = true;
      }
    }
    if (OB_SUCCESS == ret && OB_FAIL(checkpoint_delay_report_info_map_.set_refactored(
        ls_id, report_info, 1 /* overwrite */))) {
      need_report = false;
      LOG_WARN("fail to update log replica checkpoint delay report info",
          KR(ret), K(ls_id), K(checkpoint_scn), K(current_ts));
    }
  }
  return need_report;
}

int ObLogReplicaCheckpointDelayReporter::get_log_disk_pressure_(
    bool &log_disk_pressure)
{
  int ret = OB_SUCCESS;
  log_disk_pressure = false;
  logservice::ObLogService *log_service = MTL(logservice::ObLogService *);
  if (OB_ISNULL(log_service)) {
    ret = OB_ERR_UNEXPECTED;
    STORAGE_LOG(WARN, "log service is null", KR(ret), "tenant_id", MTL_ID());
  } else if (OB_FAIL(log_service->check_log_disk_under_pressure(
      log_disk_pressure))) {
    STORAGE_LOG(WARN, "failed to check whether log disk is under pressure",
        KR(ret), "tenant_id", MTL_ID());
  }
  return ret;
}

void ObLogReplicaCheckpointDelayReporter::remove_expired_report_info_()
{
  int tmp_ret = OB_SUCCESS;
  const int64_t current_ts = common::ObTimeUtility::current_monotonic_time();
  if (checkpoint_delay_report_info_map_.created()
      && !checkpoint_delay_report_info_map_.empty()) {
    CheckpointDelayReportInfoMap::iterator iter = checkpoint_delay_report_info_map_.begin();
    while (iter != checkpoint_delay_report_info_map_.end()) {
      const share::ObLSID ls_id = iter->first;
      const int64_t last_seen_ts = iter->second.last_seen_ts_;
      const bool is_expired = current_ts >= last_seen_ts
          && current_ts - last_seen_ts
              >= 2 * LOG_REPLICA_CHECKPOINT_DELAY_ERROR_REPORT_INTERVAL_US;
      ++iter;
      if (is_expired
          && OB_TMP_FAIL(checkpoint_delay_report_info_map_.erase_refactored(ls_id))) {
        LOG_WARN_RET(tmp_ret, "fail to remove expired log replica checkpoint delay report info",
            KR(tmp_ret), K(ls_id));
      }
    }
  }
}

} // namespace checkpoint
} // namespace storage
} // namespace oceanbase
