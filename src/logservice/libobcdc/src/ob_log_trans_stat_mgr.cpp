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
 * Transaction statistics
 */

#define USING_LOG_PREFIX OBLOG

#include "ob_log_trans_stat_mgr.h"
#include "ob_log_utils.h"

#define _STAT(level, fmt, args...) _OBLOG_LOG(level, "[TPS_STAT] " fmt, ##args)
#define STAT(level, fmt, args...) OBLOG_LOG(level, "[TPS_STAT] " fmt, ##args)
#define _ISTAT(fmt, args...) _STAT(INFO, fmt, ##args)
#define ISTAT(fmt, args...) STAT(INFO, fmt, ##args)

namespace oceanbase
{

using namespace common;

namespace libobcdc
{

double TransTpsStatInfo::calc_tps(const int64_t delta_time)
{
  double create_tps = 0.0;

  int64_t local_created_trans_count = ATOMIC_LOAD(&created_trans_count_);
  int64_t local_last_created_trans_count = ATOMIC_LOAD(&last_created_trans_count_);
  int64_t delta_create_count = local_created_trans_count - local_last_created_trans_count;

  if (delta_time > 0) {
    create_tps = (double)(delta_create_count) * 1000000.0 / (double)delta_time;
  }

  // Update the last statistics
  last_created_trans_count_ = local_created_trans_count;

  return create_tps;
}

double TransRpsStatInfo::calc_rps(const int64_t delta_time)
{
  double create_rps = 0.0;

  int64_t local_created_records_count = ATOMIC_LOAD(&created_records_count_);
  int64_t local_last_created_records_count = ATOMIC_LOAD(&last_created_records_count_);
  int64_t delta_create_count = local_created_records_count - local_last_created_records_count;

  if (delta_time > 0) {
    create_rps = (double)(delta_create_count) * 1000000.0 / (double)delta_time;
  }

  // Update the last statistics
  last_created_records_count_ = local_created_records_count;

  return create_rps;
}

///////////////////////////// TransTpsRpsStatInfo ///////////////////////////
void TransTpsRpsStatInfo::reset()
{
  tps_stat_info_.reset();
  rps_stat_info_.reset();
}

double TransTpsRpsStatInfo::calc_tps(const int64_t delta_time)
{
  return tps_stat_info_.calc_tps(delta_time);
}

double TransTpsRpsStatInfo::calc_rps(const int64_t delta_time)
{
  return rps_stat_info_.calc_rps(delta_time);
}

void DispatcherStatInfo::calc_and_print_stat(int64_t delta_time)
{
  int64_t dispatched_trans_count = 0;
  int64_t dispatched_redo_count = 0;

  get_and_reset_dispatcher_stat(dispatched_trans_count, dispatched_redo_count);

  if (delta_time > 0) {
    double dispatch_trans_tps = (double)(dispatched_trans_count) * 1000000.0 / (double)delta_time;
    double dispatch_redo_tps = (0 == dispatched_trans_count) ? (double)dispatched_redo_count
            :(double)(dispatched_redo_count) * 1000000.0 / (double)delta_time;
    _ISTAT("[DISPATCHER_STAT] DISPATCH_TRANS_TPS=%.3lf DISPATCH_REDO_TPS=%.3lf ",
      dispatch_trans_tps, dispatch_redo_tps);
  }
}

void SorterStatInfo::calc_and_print_stat(int64_t delta_time)
{
  int64_t sorted_trans_count = 0;
  int64_t sorted_br_count = 0;

  get_and_reset_sorter_stat(sorted_trans_count, sorted_br_count);

  if (delta_time > 0) {
    double sort_trans_tps = (double)(sorted_trans_count) * 1000000.0 / (double)delta_time;
    double sort_br_tps = (0 == sorted_trans_count) ? (double)sorted_br_count
            :(double)(sorted_br_count) * 1000000.0 / (double)delta_time;
    _ISTAT("[SORTER_STAT] SORT_TRANS_TPS=%.3lf SORT_BR_TPS=%.3lf ",
      sort_trans_tps, sort_br_tps);
  }
}

///////////////////////////// TransTpsRpsStatInfo ///////////////////////////
ObLogTransStatMgr::ObLogTransStatMgr() :
    inited_(false),
    tps_stat_info_(),
    rps_stat_info_before_filter_(),
    rps_stat_info_after_filter_(),
    tenant_stat_info_map_(),
    tenant_stat_info_pool_(),
    next_record_stat_(),
    release_record_stat_(),
    dispatcher_stat_(),
    sorter_stat_(),
    last_stat_time_(0)
{
}

ObLogTransStatMgr::~ObLogTransStatMgr()
{
  destroy();
}

int ObLogTransStatMgr::init()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(inited_)) {
    ret = OB_INIT_TWICE;
    LOG_ERROR("ObLogTransStatMgr has inited");
  } else if (OB_FAIL(tenant_stat_info_map_.init(ObModIds::OB_LOG_TENANT_STAT_MAP))) {
    LOG_ERROR("init tenant_stat_info_map_ fail", KR(ret));
  } else if (OB_FAIL(tenant_stat_info_pool_.init(CACHED_TENANT_STAT_INFO_COUNT,
      ObModIds::OB_LOG_TENANT_STAT_INFO))) {
    LOG_ERROR("init tenant_stat_info_pool_ fail", KR(ret), LITERAL_K(CACHED_TENANT_STAT_INFO_COUNT));
  } else {
    tps_stat_info_.reset();
    rps_stat_info_before_filter_.reset();
    rps_stat_info_after_filter_.reset();
    next_record_stat_.reset();
    release_record_stat_.reset();
    dispatcher_stat_.reset();
    sorter_stat_.reset();
    last_stat_time_ = 0;
    inited_ = true;
  }

  return ret;
}

void ObLogTransStatMgr::destroy()
{
  if (inited_) {
    clear_tenant_stat_info_();

    tps_stat_info_.reset();
    rps_stat_info_before_filter_.reset();
    rps_stat_info_after_filter_.reset();
    (void)tenant_stat_info_map_.destroy();
    tenant_stat_info_pool_.destroy();
    next_record_stat_.reset();
    release_record_stat_.reset();
    dispatcher_stat_.reset();
    sorter_stat_.reset();
    last_stat_time_ = 0;
    inited_ = false;
  }
}

void ObLogTransStatMgr::do_tps_stat()
{
  tps_stat_info_.do_tps_stat();
}

void ObLogTransStatMgr::do_rps_stat_before_filter(const int64_t record_count)
{
  rps_stat_info_before_filter_.do_rps_stat(record_count);
}

void ObLogTransStatMgr::do_rps_stat_after_filter(const int64_t record_count)
{
  rps_stat_info_after_filter_.do_rps_stat(record_count);
}

void ObLogTransStatMgr::do_drc_consume_tps_stat()
{
  next_record_stat_.do_tps_stat();
}

void ObLogTransStatMgr::do_drc_consume_rps_stat()
{
  next_record_stat_.do_rps_stat(1);
}

void ObLogTransStatMgr::do_drc_release_tps_stat()
{
  release_record_stat_.do_tps_stat();
}

void ObLogTransStatMgr::do_drc_release_rps_stat()
{
  release_record_stat_.do_rps_stat(1);
}

void ObLogTransStatMgr::do_dispatch_trans_stat()
{
  dispatcher_stat_.inc_dispatched_trans_count();
}

void ObLogTransStatMgr::do_dispatch_redo_stat()
{
  dispatcher_stat_.inc_dispatched_redo_count();
}

void ObLogTransStatMgr::do_sort_trans_stat()
{
  sorter_stat_.inc_sorted_trans_count();
}

void ObLogTransStatMgr::do_sort_br_stat()
{
  sorter_stat_.inc_sorted_br_count();
}

void ObLogTransStatMgr::print_stat_info()
{
  int ret = OB_SUCCESS;

  int64_t current_timestamp = get_timestamp();
  int64_t local_last_stat_time = last_stat_time_;
  int64_t delta_time = current_timestamp - local_last_stat_time;

  // calc and print stat info of dispatcher and sorter
  dispatcher_stat_.calc_and_print_stat(delta_time);
  sorter_stat_.calc_and_print_stat(delta_time);

  double create_tps = tps_stat_info_.calc_tps(delta_time);
  double create_rps_before_filter = rps_stat_info_before_filter_.calc_rps(delta_time);
  double create_rps_after_filter = rps_stat_info_after_filter_.calc_rps(delta_time);

  // Update the last statistics
  last_stat_time_ = current_timestamp;

  _ISTAT("TOTAL TPS=%.3lf RPS=%.3lf RPS_ALL=%.3lf ",
      create_tps, create_rps_after_filter, create_rps_before_filter);

  // Print Tenant Statistics
  TenantStatInfoPrinter printer(delta_time);
  if (OB_FAIL(tenant_stat_info_map_.for_each(printer))) {
    LOG_ERROR("TenantStatInfoMap for each fail", KR(ret));
  }

  // Print drc consumption statistics
  double next_record_tps = next_record_stat_.calc_tps(delta_time);
  double next_record_rps = next_record_stat_.calc_rps(delta_time);
  double release_record_tps = release_record_stat_.calc_tps(delta_time);
  double release_record_rps = release_record_stat_.calc_rps(delta_time);

  _ISTAT("[DRC] NEXT_RECORD_TPS=%.3lf RELEASE_RECORD_TPS=%.3lf "
      "NEXT_RECORD_RPS=%.3lf RELEASE_RECORD_RPS=%.3lf",
      next_record_tps, release_record_tps, next_record_rps, release_record_rps);
}

int ObLogTransStatMgr::add_served_tenant(const char *tenant_name, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTransStatMgr has not inited");
    ret = OB_NOT_INIT;
  } else if (OB_ISNULL(tenant_name) ||
      OB_UNLIKELY(OB_INVALID_ID == tenant_id)) {
    LOG_ERROR("invalid argument", K(tenant_name), K(tenant_id));
    ret = OB_INVALID_ARGUMENT;
  } else {
    int64_t pos = 0;
    TenantStatInfo *ts_info = NULL;
    TenantID tid(tenant_id);

    if (OB_FAIL(tenant_stat_info_pool_.alloc(ts_info))) {
      LOG_ERROR("alloc tenant stat info fail", KR(ret), K(tenant_id), K(tenant_name), KPC(ts_info));
    } else if (OB_FAIL(databuff_printf(ts_info->name_, sizeof(ts_info->name_), pos,
        "%s", tenant_name))) {
      LOG_ERROR("print tenant name fail", KR(ret), K(pos), K(tenant_id), K(tenant_name), KPC(ts_info));
    } else {
      if (OB_FAIL(tenant_stat_info_map_.insert(tid, ts_info))) {
        if (OB_ENTRY_EXIST != ret) {
          LOG_ERROR("insert into served_tenant_db_map_ fail", KR(ret),
              K(tid), KPC(ts_info));
        } else {
          ts_info->reset();
          tenant_stat_info_pool_.free(ts_info);
          ts_info = NULL;
          ret = OB_SUCCESS;
        }
      } else {
        _ISTAT("[ADD_TENANT] TENANT=%s(%lu) TOTAL_COUNT=%ld",
            tenant_name, tenant_id, tenant_stat_info_map_.count());
      }
    }

    // Recycle useless objects
    if (OB_SUCCESS != ret && NULL != ts_info) {
      ts_info->reset();
      tenant_stat_info_pool_.free(ts_info);
      ts_info = NULL;
    }
  }

  return ret;
}

int ObLogTransStatMgr::drop_served_tenant(const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;

  TenantStatInfoErase erase_fn(tenant_id, tenant_stat_info_pool_);

  if (OB_FAIL(tenant_stat_info_map_.erase_if(tenant_id, erase_fn))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      ret = OB_SUCCESS;
      LOG_WARN("tenant has been drop in trans stat mgr", K(tenant_id));
    } else {
      LOG_ERROR("tenant stat info map erase_if tenant fail", KR(ret), K(tenant_id));
    }
  } else {
    // do nothing
  }

  return ret;
}

int ObLogTransStatMgr::do_tenant_tps_rps_stat(const uint64_t tenant_id, int64_t record_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTransStatMgr has not inited");
    ret = OB_NOT_INIT;
  } else {
    TenantRpsBeforeFilterUpdater updater(tenant_id, record_count);
    TenantID tid(tenant_id);
    if (OB_FAIL(tenant_stat_info_map_.operate(tid, updater))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("dp tenant rps stat before filter fail", KR(ret), K(tid), K(record_count));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

int ObLogTransStatMgr::do_tenant_rps_stat_after_filter(const uint64_t tenant_id, int64_t record_count)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTransStatMgr has not inited");
    ret = OB_NOT_INIT;
  } else {
    TenantRpsAfterFilterUpdater updater(tenant_id, record_count);
    TenantID tid(tenant_id);
    if (OB_FAIL(tenant_stat_info_map_.operate(tid, updater))) {
      if (OB_ENTRY_NOT_EXIST != ret) {
        LOG_ERROR("dp tenant rps stat after filter fail", KR(ret), K(tid), K(record_count));
      } else {
        ret = OB_SUCCESS;
      }
    } else {
      // do nothing
    }
  }

  return ret;
}

void ObLogTransStatMgr::clear_tenant_stat_info_()
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! inited_)) {
    LOG_ERROR("ObLogTransStatMgr has not inited");
    ret = OB_NOT_INIT;
  } else {
    TenantStatInfoClear clear(tenant_stat_info_pool_);

    if (OB_FAIL(tenant_stat_info_map_.remove_if(clear))) {
      LOG_ERROR("clear tenant_stat_info_map fail", KR(ret));
    }
  }
}

bool ObLogTransStatMgr::TenantRpsBeforeFilterUpdater::operator()(const TenantID &tid, TenantStatInfo *ts_info)
{
  if (tid.tenant_id_ == tenant_id_) {
    if (OB_ISNULL(ts_info)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant stat info is null", K(tid), KPC(ts_info));
    } else {
      ts_info->tps_stat_info_.do_tps_stat();
      ts_info->rps_stat_info_before_filter_.do_rps_stat(record_count_);
    }
  }

  return tid.tenant_id_ == tenant_id_;
}

bool ObLogTransStatMgr::TenantRpsAfterFilterUpdater::operator()(const TenantID &tid, TenantStatInfo *ts_info)
{
  if (tid.tenant_id_ == tenant_id_) {
    if (OB_ISNULL(ts_info)) {
      LOG_ERROR_RET(OB_ERR_UNEXPECTED, "tenant stat info is null", K(tid), KPC(ts_info));
    } else {
      ts_info->rps_stat_info_after_filter_.do_rps_stat(record_count_);
    }
  }

  return tid.tenant_id_ == tenant_id_;
}

bool ObLogTransStatMgr::TenantStatInfoPrinter::operator()(const TenantID &tid,
    TenantStatInfo *ts_info)
{
  if (NULL != ts_info) {
    double tps = ts_info->tps_stat_info_.calc_tps(delta_time_);
    double rps_before_filter = ts_info->rps_stat_info_before_filter_.calc_rps(delta_time_);
    double rps_after_filter = ts_info->rps_stat_info_after_filter_.calc_rps(delta_time_);

    _ISTAT("TENANT=%s(%lu) TPS=%.3lf RPS=%.3lf RPS_ALL=%.3lf",
        ts_info->name_, tid.tenant_id_, tps, rps_after_filter, rps_before_filter);
  }

  return true;
}

bool ObLogTransStatMgr::TenantStatInfoErase::operator()(const TenantID &tid,
    TenantStatInfo *ts_info)
{
  if (tid.tenant_id_ == tenant_id_ && NULL != ts_info) {
    ts_info->reset();
    (void)pool_.free(ts_info);
    ts_info = NULL;

    _ISTAT("[DROP_TENANT] TENANT=%lu", tid.tenant_id_);
  }

  return tid.tenant_id_ == tenant_id_;
}

bool ObLogTransStatMgr::TenantStatInfoClear::operator()(const TenantID &tid,
    TenantStatInfo *ts_info)
{
  UNUSED(tid);
  if (NULL != ts_info) {
    ts_info->reset();
    (void)pool_.free(ts_info);
    ts_info = NULL;

    _ISTAT("[CLEAR_TENANT] TENANT=%s(%lu)", ts_info->name_, tid.tenant_id_);
  }

  return true;
}

}
}
