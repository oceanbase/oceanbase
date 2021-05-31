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

#define USING_LOG_PREFIX SERVER

#include "observer/ob_lease_state_mgr.h"
#include "share/ob_common_rpc_proxy.h"
#include "observer/ob_server.h"

namespace oceanbase {
using namespace common;
using namespace obrpc;
using namespace share;
namespace observer {
ObRefreshSchemaStatusTimerTask::ObRefreshSchemaStatusTimerTask()
{}

void ObRefreshSchemaStatusTimerTask::destroy()
{}

void ObRefreshSchemaStatusTimerTask::runTimerTask()
{
  int ret = OB_SUCCESS;
  ObSchemaStatusProxy* schema_status_proxy = GCTX.schema_status_proxy_;
  if (OB_ISNULL(schema_status_proxy)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get invalid schema status proxy", KR(ret));
  } else if (OB_FAIL(schema_status_proxy->load_refresh_schema_status())) {
    LOG_WARN("fail to load refresh schema status", KR(ret));
  } else {
    LOG_INFO("refresh schema status success");
  }
}

//////////////////////////////////////

ObLeaseStateMgr::ObLeaseStateMgr()
    : inited_(false),
      stopped_(false),
      lease_response_(),
      lease_expire_time_(0),
      timer_(),
      rs_mgr_(NULL),
      rpc_proxy_(NULL),
      heartbeat_process_(NULL),
      hb_(),
      renew_timeout_(RENEW_TIMEOUT),
      ob_service_(NULL),
      avg_calculator_(),
      baseline_schema_version_(0),
      heartbeat_expire_time_(0)
{}

ObLeaseStateMgr::~ObLeaseStateMgr()
{
  destroy();
}

void ObLeaseStateMgr::destroy()
{
  if (inited_) {
    stopped_ = false;
    timer_.destroy();
    rs_mgr_ = NULL;
    rpc_proxy_ = NULL;
    heartbeat_process_ = NULL;
    inited_ = false;
  }
}

// ObRsMgr should be inited by local config before call ObLeaseStateMgr.init
int ObLeaseStateMgr::init(ObCommonRpcProxy* rpc_proxy, ObRsMgr* rs_mgr, IHeartBeatProcess* heartbeat_process,
    ObService& service,
    const int64_t renew_timeout)  // default RENEW_TIMEOUT = 2s
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == rpc_proxy || NULL == rs_mgr || NULL == heartbeat_process || renew_timeout < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(rpc_proxy), KP(rs_mgr), KP(heartbeat_process), K(renew_timeout), K(ret));
  } else if (!rs_mgr->is_inited()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rs_mgr not inited", "rs_mgr inited", rs_mgr->is_inited(), K(ret));
  } else if (OB_FAIL(timer_.init("LeaseHB"))) {
    LOG_WARN("timer_ init failed", K(ret));
  } else if (OB_FAIL(avg_calculator_.init(STORE_RTT_NUM))) {
    LOG_WARN("avg calculator init fail", K(ret));
  } else {
    rs_mgr_ = rs_mgr;
    rpc_proxy_ = rpc_proxy;
    heartbeat_process_ = heartbeat_process;
    renew_timeout_ = renew_timeout;
    ob_service_ = &service;
    if (OB_FAIL(hb_.init(this))) {
      LOG_WARN("hb_.init failed", K(ret));
    } else {
      inited_ = true;
    }
  }
  return ret;
}

int ObLeaseStateMgr::register_self()
{
  int ret = OB_SUCCESS;
  LOG_INFO("begin register_self");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    if (OB_FAIL(do_renew_lease())) {
      LOG_WARN("do_renew_lease failed", K(ret));
    }

    LOG_INFO("start_heartbeat anyway");
    // ignore ret overwrite
    if (OB_FAIL(start_heartbeat())) {
      LOG_ERROR("start_heartbeat failed", K(ret));
    }
  }
  return ret;
}

int ObLeaseStateMgr::register_self_busy_wait()
{
  int ret = OB_SUCCESS;

  LOG_INFO("begin register_self_busy_wait");
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    while (!stopped_) {
      if (OB_FAIL(try_report_all_core_table_partition())) {
        LOG_WARN("fail to try report all core table partition");
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = rs_mgr_->renew_master_rootserver())) {
          LOG_WARN("renew_master_rootserver failed", K(tmp_ret));
          if (OB_SUCCESS != (tmp_ret = ob_service_->refresh_core_partition())) {
            LOG_WARN("fail to refresh core partition", K(tmp_ret));
          }
        }
      } else if (OB_FAIL(do_renew_lease())) {
        LOG_WARN("register failed, will try again", K(ret), "retry latency", REGISTER_TIME_SLEEP / 1000000);
        usleep(static_cast<useconds_t>(REGISTER_TIME_SLEEP));
        // ignore ret overwrite
        if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {
          LOG_WARN("renew_master_rootserver failed", K(ret));
          int tmp_ret = OB_SUCCESS;
          if (OB_SUCCESS != (tmp_ret = ob_service_->refresh_core_partition())) {
            LOG_WARN("fail to refresh core partition", K(tmp_ret));
          }
        } else {
          LOG_INFO("renew_master_rootserver successfully, try register again");
          if (OB_FAIL(do_renew_lease())) {
            LOG_WARN("register failed", K(ret));
          }
        }
      }

      if (OB_SUCC(ret)) {
        LOG_INFO("register self successfully!");
        if (OB_FAIL(start_heartbeat())) {
          LOG_ERROR("start_heartbeat failed", K(ret));
        }
        break;
      }
    }
  }
  if (stopped_) {
    ret = OB_CANCELED;
    LOG_WARN("fail to register_self_busy_wait", KR(ret));
  }
  LOG_INFO("end register_self_busy_wait");
  return ret;
}

int ObLeaseStateMgr::try_report_all_core_table_partition()
{
  int ret = OB_SUCCESS;
  common::ObPartitionKey core_pkey;
  storage::ObPartitionService* partition_service = GCTX.par_ser_;
  share::ObPartitionTableOperator* pt_operator = GCTX.pt_operator_;
  if (OB_UNLIKELY(nullptr == partition_service || nullptr == pt_operator || nullptr == ob_service_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("partition service or pt operator ptr is null", K(ret));
  } else if (OB_FAIL(core_pkey.init(combine_id(OB_SYS_TENANT_ID, OB_ALL_CORE_TABLE_TID),
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_ID,
                 ObIPartitionTable::ALL_CORE_TABLE_PARTITION_NUM))) {
    LOG_WARN("fail to init core pkey", K(ret));
  } else if (partition_service->is_partition_exist(core_pkey)) {
    share::ObPartitionReplica core_replica;
    if (OB_FAIL(ob_service_->fill_partition_replica(core_pkey, core_replica))) {
      LOG_WARN("fail to fill partition replica", K(ret), K(core_pkey));
    } else if (OB_FAIL(pt_operator->update(core_replica))) {
      LOG_WARN("fail to update core replica", K(ret));
    }
  } else {
  }  // core table not exist, no need to report
  return ret;
}

int ObLeaseStateMgr::renew_lease()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (stopped_) {
    ret = OB_SERVER_IS_STOPPING;
    LOG_WARN("lease manager is stopped", K(ret));
  } else {
    if (OB_FAIL(do_renew_lease())) {
      LOG_WARN("do_renew_lease failed", K(ret));
      if (OB_FAIL(rs_mgr_->renew_master_rootserver())) {
        LOG_WARN("renew_master_rootserver failed", K(ret));
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = ob_service_->refresh_core_partition())) {
          LOG_WARN("fail to refresh core partition", K(tmp_ret));
        }
      } else {
        LOG_INFO("renew_master_rootserver successfully, try renew lease again");
        if (OB_FAIL(try_report_all_core_table_partition())) {
          LOG_WARN("fail to try report all core table partition");
        } else if (OB_FAIL(do_renew_lease())) {
          LOG_WARN("try do_renew_lease again failed, will do it no next heartbeat", K(ret));
        }
      }
    }

    if (OB_SUCC(ret)) {
      LOG_DEBUG("renew_lease successfully!");
    }

    const bool repeat = false;
    if (OB_FAIL(timer_.schedule(hb_, DELAY_TIME, repeat))) {
      LOG_WARN("schedule failed", LITERAL_K(DELAY_TIME), K(repeat), K(ret));
    }
  }
  return ret;
}

int ObLeaseStateMgr::start_heartbeat()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    const bool repeat = false;
    if (OB_FAIL(timer_.schedule(hb_, DELAY_TIME, repeat))) {
      LOG_WARN("schedule failed", LITERAL_K(DELAY_TIME), K(repeat), K(ret));
    }
  }
  return ret;
}

int ObLeaseStateMgr::do_renew_lease()
{
  int ret = OB_SUCCESS;
  ObLeaseRequest lease_request;
  ObLeaseResponse lease_response;
  double avg_round_trip_time = 0;
  ObAddr rs_addr;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(heartbeat_process_->init_lease_request(lease_request))) {
    LOG_WARN("init lease request failed", K(ret));
  } else if (OB_FAIL(rs_mgr_->get_master_root_server(rs_addr))) {
    LOG_WARN("get master root service failed", K(ret));
  } else if (OB_FAIL(avg_calculator_.get_avg(avg_round_trip_time))) {
    LOG_WARN("get avg round_trip_time fail", K(ret));
  } else {
    lease_request.round_trip_time_ = static_cast<int64_t>(avg_round_trip_time);
    const int64_t begin = ObTimeUtility::current_time();
    lease_request.current_server_time_ = begin;
    ret = rpc_proxy_->to(rs_addr).timeout(renew_timeout_).renew_lease(lease_request, lease_response);
    const int64_t end = ObTimeUtility::current_time();
    if (lease_response.lease_expire_time_ > 0) {
      // for compatible with old version
      lease_response.heartbeat_expire_time_ = lease_response.lease_expire_time_;
    }
    if (OB_SUCC(ret)) {
      OBSERVER.get_gctx().par_ser_->set_global_max_decided_trans_version(
          lease_response.global_max_decided_trans_version_);
      int tmp_ret = OB_SUCCESS;
      if (baseline_schema_version_ < lease_response.baseline_schema_version_) {
        if (OB_SUCCESS !=
            (tmp_ret = GCTX.schema_service_->update_baseline_schema_version(lease_response.baseline_schema_version_))) {
          LOG_WARN("fail to update baseline schema version", KR(ret), KR(tmp_ret), K(lease_response));
        } else {
          LOG_INFO("update baseline schema version",
              KR(ret),
              "old_version",
              baseline_schema_version_,
              "new_version",
              lease_response.baseline_schema_version_);
          baseline_schema_version_ = lease_response.baseline_schema_version_;
        }
      }
      const int64_t now = ObTimeUtility::current_time();
      if (OB_SUCC(ret) && lease_response.heartbeat_expire_time_ > now) {
        LOG_DEBUG("renew_lease from  master_rs successfully", K(rs_addr));
        if (OB_FAIL(set_lease_response(lease_response))) {
          LOG_WARN("fail to set lease response", K(ret));
        } else if (OB_FAIL(avg_calculator_.calc_avg(end - begin))) {
          LOG_WARN("calculate avg round_trip_time fail", K(ret), "current_rtt", end - begin);
        } else if (OB_FAIL(heartbeat_process_->do_heartbeat_event(lease_response_))) {
          LOG_WARN("fail to process new lease info", K_(lease_response), K(ret));
        }
      }
    } else {
      LOG_WARN("can't get lease from rs", K(rs_addr), K(ret));
    }
  }
  return ret;
}

ObLeaseStateMgr::HeartBeat::HeartBeat() : inited_(false), lease_state_mgr_(NULL)
{}

ObLeaseStateMgr::HeartBeat::~HeartBeat()
{}

int ObLeaseStateMgr::HeartBeat::init(ObLeaseStateMgr* lease_state_mgr)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else if (NULL == lease_state_mgr) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KP(lease_state_mgr), K(ret));
  } else {
    lease_state_mgr_ = lease_state_mgr;
    inited_ = true;
  }
  return ret;
}

void ObLeaseStateMgr::HeartBeat::runTimerTask()
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(lease_state_mgr_->renew_lease())) {
    LOG_WARN("fail to renew lease", K(ret));
  }
}

ObLeaseStateMgr::AvgCalculator::AvgCalculator() : calc_buffer_(), inited_(false), limit_num_(0), head_(0), avg_(0)
{}

ObLeaseStateMgr::AvgCalculator::~AvgCalculator()
{
  calc_buffer_.destroy();
}

int ObLeaseStateMgr::AvgCalculator::init(int64_t limit_num)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret));
  } else {
    limit_num_ = limit_num;
    inited_ = true;
    head_ = 0;
    avg_ = 0;
  }
  return ret;
}

int ObLeaseStateMgr::AvgCalculator::get_avg(double& avg)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    avg = avg_;
  }
  return ret;
}

int ObLeaseStateMgr::AvgCalculator::calc_avg(int64_t new_value)
{
  int ret = OB_SUCCESS;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (0 == limit_num_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("limit num should not be zero", K(ret));
  } else if (calc_buffer_.count() > limit_num_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("array size is out of limit", K(ret), K(limit_num_), "array_size", calc_buffer_.count());
  } else if (calc_buffer_.count() < limit_num_) {
    if (OB_FAIL(calc_buffer_.push_back(new_value))) {
      LOG_WARN("array push_back fail", K(ret));
    } else {
      double num = static_cast<double>(calc_buffer_.count());
      avg_ = (num - 1) / num * avg_ + static_cast<double>(new_value) / num;
    }
  } else if (calc_buffer_.count() == limit_num_) {
    int64_t old_value = calc_buffer_[head_];
    calc_buffer_.at(head_) = new_value;
    avg_ += (static_cast<double>(new_value - old_value) / static_cast<double>(limit_num_));
    head_ = (head_ + 1) % limit_num_;
  }
  return ret;
}

}  // end namespace observer
}  // end namespace oceanbase
