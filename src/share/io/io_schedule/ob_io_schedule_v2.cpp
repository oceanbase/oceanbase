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

#define USING_LOG_PREFIX COMMON
#include "ob_io_schedule_v2.h"
#include "share/io/ob_io_struct.h"
#include "observer/ob_server.h"
namespace oceanbase
{
namespace common
{

static void io_req_finish(ObIORequest& req, const ObIORetCode& ret_code)
{
  if (OB_NOT_NULL(req.io_result_)) {
    req.io_result_->finish(ret_code, &req);
  }
}

int QSchedCallback::handle(TCRequest* tc_req)
{
  int ret = OB_SUCCESS;
  ObDeviceChannel *device_channel = nullptr;
  ObTimeGuard time_guard("submit_req", 100000); //100ms
  ObIORequest& req = *CONTAINER_OF(tc_req, ObIORequest, qsched_req_);
  ObIOResult* result = req.io_result_;
  if (OB_UNLIKELY(stop_submit_)) {
    ret = OB_STATE_NOT_MATCH;
    LOG_WARN("sender stop submit", K(ret), K(stop_submit_));
  } else if (OB_ISNULL(result)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret), K(req));
  } else if (OB_UNLIKELY(req.is_canceled())) {
    ret = OB_CANCELED;
  } else if (OB_FAIL(req.prepare())) {
    LOG_WARN("prepare io request failed", K(ret), K(req));
  } else if (FALSE_IT(time_guard.click("prepare_req"))) {
  } else if (OB_FAIL(OB_IO_MANAGER.get_device_channel(req, device_channel))) {
    LOG_WARN("get device channel failed", K(ret), K(req));
  } else if (FALSE_IT(result->time_log_.dequeue_ts_ = ObTimeUtility::fast_current_time())) {
  } else {
    // lock request condition to prevent canceling halfway
    ObThreadCondGuard guard(result->get_cond());
    if (OB_FAIL(guard.get_ret())) {
      LOG_ERROR("fail to guard master condition", K(ret));
    } else if (req.is_canceled()) {
      ret = OB_CANCELED;
    } else if (OB_FAIL(device_channel->submit(req))) {
      LOG_WARN("submit io to device failed");
    } else {
      time_guard.click("device_submit");
    }
  }

  if (time_guard.get_diff() > 100000) {// 100ms
    //print req
    LOG_INFO("submit_request cost too much time", K(ret), K(time_guard), K(req));
  }
  if (OB_FAIL(ret)) {
    if (ret == OB_EAGAIN) {
      if (REACH_TIME_INTERVAL(1 * 1000L * 1000L)) {
        LOG_INFO("device channel eagain", K(ret));
      }
      if (OB_FAIL(req.retry_io())) {
        LOG_WARN("retry io failed", K(ret), K(req));
        io_req_finish(req, ObIORetCode(ret));
      }
    } else {
      io_req_finish(req, ObIORetCode(ret));
    }
  }
  req.dec_ref("phyqueue_dec"); // ref for io queue
  return ret;
}

ObIOManagerV2 g_io_mgr2;

ObIOManagerV2::ObIOManagerV2(): root_qid_(-1)
{
  memset(sub_roots_, -1, sizeof(sub_roots_));
}

int ObIOManagerV2::init()
{
  int ret = OB_SUCCESS;
  if ((root_qid_ = qdisc_create(QDISC_ROOT, -1, "root")) < 0) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (0 != qsched_set_handler(root_qid_, &io_submitter_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if ((sub_roots_[(int)ObIOMode::READ] = qdisc_create(QDISC_WEIGHTED_QUEUE, root_qid_, "net_in")) < 0) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if ((sub_roots_[(int)ObIOMode::WRITE] = qdisc_create(QDISC_WEIGHTED_QUEUE, root_qid_, "net_out")) < 0) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if ((sub_roots_[(int)ObIOMode::MAX_MODE] = qdisc_create(QDISC_WEIGHTED_QUEUE, root_qid_, "disk")) < 0) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
  } else if (OB_FAIL(set_default_net_tc_limits())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("add net tc limits failed", K(ret));
  }
  return ret;
}

int ObIOManagerV2::start()
{
  int ret = OB_SUCCESS;
  if (0 != qsched_start(root_qid_, 2)) {
    ret = OB_ERR_SYS;
  } else if (OB_FAIL(io_submitter_.start())) {
  }
  return ret;
}

void ObIOManagerV2::stop()
{
  io_submitter_.set_stop();
  qsched_stop(root_qid_);
}

void ObIOManagerV2::wait()
{
  qsched_wait(root_qid_);
}

void ObIOManagerV2::destroy()
{}

int ObIOManagerV2::set_default_net_tc_limits()
{
  int ret = OB_SUCCESS;
  int r_qid = sub_roots_[(int)ObIOMode::READ];
  int w_qid = sub_roots_[(int)ObIOMode::WRITE];
  if (r_qid < 0 || w_qid < 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid qid", K(ret), K(r_qid), K(w_qid));
  } else if (OB_FAIL(qdisc_set_limit(r_qid, observer::ObServer::DEFAULT_ETHERNET_SPEED))) {
    LOG_WARN("set limit failed", K(ret), K(r_qid));
  } else if (OB_FAIL(qdisc_set_limit(w_qid, observer::ObServer::DEFAULT_ETHERNET_SPEED))) {
    LOG_WARN("set limit failed", K(ret), K(w_qid));
  }
  return ret;
}


ObTenantIOSchedulerV2::ObTenantIOSchedulerV2(): tenant_id_(0)
{
  memset(top_qid_, -1, sizeof(top_qid_));
  memset(default_qid_, -1, sizeof(default_qid_));
}

int ObTenantIOSchedulerV2::init(const uint64_t tenant_id, const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  for(int i = 0; OB_SUCC(ret) && i < N_SUB_ROOT; i++) {
    char name1[16];
    char name2[16];
    snprintf(name1, sizeof(name1), "t%ldm%d", tenant_id, i);
    snprintf(name2, sizeof(name2), "t%ldm%dgd", tenant_id, i);
    if ((top_qid_[i] = qdisc_create(QDISC_WEIGHTED_QUEUE, OB_IO_MANAGER_V2.get_sub_root_qid(i), name1)) < 0) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else if ((default_qid_[i] = qdisc_create(QDISC_BUFFER_QUEUE, top_qid_[i], name2)) < 0) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
  }
  if (OB_FAIL(ret)) {
    destroy();
  } else {
    tenant_id_ = tenant_id;
  }
  return ret;
}

void ObTenantIOSchedulerV2::destroy()
{
  for(int i = 0; i < qid_.count(); i++) {
    if (qid_.at(i) >= 0) {
      qdisc_destroy(qid_.at(i));
      qid_.at(i) = -1;
    }
  }
  for(int i = 0; i < N_SUB_ROOT; i++) {
    if (default_qid_[i] >= 0) {
      qdisc_destroy(default_qid_[i]);
      default_qid_[i] = -1;
    }
    if (top_qid_[i] >= 0) {
      qdisc_destroy(top_qid_[i]);
      top_qid_[i] = -1;
    }
  }
}

static int config_qdisc(int qid, int64_t weight, int64_t max_bw, int64_t min_bw)
{
  int ret = OB_SUCCESS;
  if (0 != qdisc_set_weight(qid, weight? weight: 100)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (0 != qdisc_set_limit(qid, max_bw)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (0 != qdisc_set_reserve(qid, min_bw)) {
    ret = OB_ERR_UNEXPECTED;
  } else {
    LOG_INFO("config qdisc success", K(ret), K(qid), K(weight), K(max_bw), K(min_bw));
  }
  return ret;
}

static int64_t calc_bw(const int64_t bw, const int64_t percentage)
{
  return static_cast<double>(bw) / 100 * percentage >= INT64_MAX ? INT64_MAX : static_cast<int64_t>(static_cast<double>(bw) / 100 * percentage);
}

static int config_group_qdisc(int qid, const int64_t tenant_id, const ObTenantIOConfig::UnitConfig& ucfg, const ObTenantIOConfig::GroupConfig &gcfg)
{
  int ret = OB_SUCCESS;
  int64_t unit_min_bw = gcfg.mode_ == ObIOMode::MAX_MODE ? (ucfg.min_iops_ * STANDARD_IOPS_SIZE < 0 ? INT64_MAX : ucfg.min_iops_ * STANDARD_IOPS_SIZE) : 0;
  int64_t unit_max_bw = gcfg.mode_ == ObIOMode::MAX_MODE ? (ucfg.max_iops_ * STANDARD_IOPS_SIZE <= 0 ? INT64_MAX : ucfg.max_iops_ * STANDARD_IOPS_SIZE) : ucfg.max_net_bandwidth_;
  if (OB_FAIL(config_qdisc(qid, gcfg.weight_percent_, calc_bw(unit_max_bw, gcfg.max_percent_), calc_bw(unit_min_bw, gcfg.min_percent_)))) {
    LOG_ERROR("config group_queue fail", K(ret), K(tenant_id), K(qid), K(ucfg), K(gcfg));
  } else {
    LOG_INFO("config group queue success", K(ret), K(tenant_id), K(qid), K(ucfg), K(gcfg), K(unit_min_bw), K(unit_max_bw));
  }
  return ret;
}

static bool is_valid_mode(int mode)
{
  return mode >= 0 && mode < ObTenantIOSchedulerV2::N_SUB_ROOT;
}

int ObTenantIOSchedulerV2::update_config(const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  // group config: READ, WRITE, MAX_MODE
  // qdisc       : READ, WRITE, MAX_MODE
  const int64_t all_group_num = io_config.group_configs_.count();
  const ObTenantIOConfig::UnitConfig &ucfg = io_config.unit_config_;
  int64_t ucfg_max_bw = ucfg.max_iops_ * STANDARD_IOPS_SIZE <= 0 ? INT64_MAX : ucfg.max_iops_ * STANDARD_IOPS_SIZE;
  int64_t ucfg_min_bw = ucfg.min_iops_ * STANDARD_IOPS_SIZE < 0 ? INT64_MAX : ucfg.min_iops_ * STANDARD_IOPS_SIZE;
  LOG_INFO("update config", K_(tenant_id), K(io_config), K(ucfg_min_bw), K(ucfg_min_bw));
  if (OB_FAIL(config_qdisc(top_qid_[(int)ObIOMode::MAX_MODE], ucfg.weight_, ucfg_max_bw, ucfg_min_bw))) {
    LOG_WARN("config local tenant_queue fail", K(ret), K(tenant_id_), K(ObIOMode::MAX_MODE));
  } else if (OB_FAIL(config_qdisc(top_qid_[(int)ObIOMode::READ], ucfg.net_bandwidth_weight_, ucfg.max_net_bandwidth_, 0))) {
    LOG_WARN("config remote read tenant_queue fail", K(ret), K(ObIOMode::READ));
  } else if (OB_FAIL(config_qdisc(top_qid_[(int)ObIOMode::WRITE], ucfg.net_bandwidth_weight_, ucfg.max_net_bandwidth_, 0))) {
    LOG_WARN("config remote write tenant_queue fail", K(ret), K(ObIOMode::WRITE));
  } else if (OB_FAIL(qid_.reserve(all_group_num))) {
    LOG_WARN("qid reserve fail", K(ret));
  } else {
    for(int i = qid_.count(); OB_SUCC(ret) && i < all_group_num; i++) {
      ret = qid_.push_back(-1);
    }
    for (int i = 0; OB_SUCC(ret) && i < all_group_num; ++i) {
      const ObTenantIOConfig::GroupConfig& grp_cfg = io_config.group_configs_.at(i);
      int qid = qid_.at(i);
      if (qid >= 0) {
      } else if (!is_valid_mode((int)grp_cfg.mode_)) {
        ret = OB_INVALID_ARGUMENT;
      } else {
        char name[16];
        snprintf(name, sizeof(name), "t%ldm%dg%ld", tenant_id_, (int)grp_cfg.mode_, grp_cfg.group_id_);
        if ((qid = qdisc_create(QDISC_BUFFER_QUEUE, top_qid_[(int)grp_cfg.mode_], name)) < 0) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
        } else {
          qid_.at(i) = qid;
        }
      }
      if (OB_FAIL(ret)) {
      } else if (OB_FAIL(config_group_qdisc(qid, tenant_id_, ucfg, grp_cfg))) {
        LOG_WARN("config other group fail", K(ret), K(qid), K(io_config));
      }
    }
  }
  return ret;
}

static void fill_qsched_req(ObIORequest& req, int qid)
{
  req.qsched_req_.qid_ = qid;
  req.qsched_req_.bytes_ = req.get_align_size();
  req.qsched_req_.norm_bytes_ = req.is_limit_net_bandwidth_req() ? req.qsched_req_.bytes_ : get_norm_bw(req.qsched_req_.bytes_, req.get_mode());
}
int64_t ObTenantIOSchedulerV2::get_qindex(ObIORequest& req)
{
  int ret = OB_SUCCESS;
  int64_t index = -1;
  const ObIOGroupKey grp_key = req.get_group_key();
  if (is_sys_group(grp_key.group_id_)) { //other
    index = static_cast<int64_t>(grp_key.mode_);
  } else if (!is_valid_group(grp_key.group_id_)) {
  } else if (OB_FAIL(req.tenant_io_mgr_.get_ptr()->get_group_index(grp_key, (uint64_t&)index))) {
    if (ret == OB_HASH_NOT_EXIST) {
      ret = OB_SUCCESS;
      if (REACH_TIME_INTERVAL(1 * 1000L * 1000L)) {
        LOG_INFO("get group index failed, but maybe it is ok", K(ret), K(grp_key), K(index)); // group is not build
      }
    } else {
      LOG_WARN("get group index failed", K(ret), K(grp_key), K(index));
    }
    index = -1;
  } else if (INT64_MAX == index) {
    index = -1;
  }
  return index;
}
int ObTenantIOSchedulerV2::get_qid(int64_t index, ObIORequest& req, bool& is_default_q)
{
  int qid = -1;
  if (req.is_local_clog_not_isolated()) {
    qid = OB_IO_MANAGER_V2.get_root_qid();
  }  else if (index >= 0 && index < qid_.count()) {
    qid = qid_.at(index);
  }
  if (qid < 0) {
    is_default_q = true;
    ObIOGroupKey grp_key = req.get_group_key();
    int default_index = static_cast<int>(grp_key.mode_);
    qid = default_qid_[default_index % N_SUB_ROOT];
  }
  return qid;
}
static uint32_t g_io_chan_id = 0;
static uint32_t assign_chan_id()
{
  return ATOMIC_FAA(&g_io_chan_id, 1);
}

int ObTenantIOSchedulerV2::schedule_request(ObIORequest &req)
{
  int ret = OB_SUCCESS;
  bool is_default_q = false;
  int root = OB_IO_MANAGER_V2.get_root_qid();
  int64_t index = get_qindex(req);
  int qid = get_qid(index, req, is_default_q);
  fill_qsched_req(req, qid);
  req.inc_ref("phyqueue_inc"); //ref for phy_queue
  if (OB_ISNULL(req.io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret), K(req));
  } else if (FALSE_IT(req.io_result_->time_log_.enqueue_ts_ = ObTimeUtility::fast_current_time())) {
  } else if (OB_UNLIKELY(is_default_q)) {
    if (0 != qsched_submit(root, &req.qsched_req_, assign_chan_id())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("qsched_submit fail", K(ret), K(req));
    }
  } else if (OB_FAIL(OB_IO_MANAGER.get_tc().register_bucket(req, qid))) {
    LOG_WARN("register bucket fail", K(ret), K(req));
  } else if (0 != qsched_submit(root, &req.qsched_req_, assign_chan_id())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("qsched_submit fail", K(ret), K(req));
  }

  if (OB_FAIL(ret)) {
    req.dec_ref("phyqueue_dec"); //ref for phy_queue
    io_req_finish(req, ObIORetCode(ret));
  }
  return ret;
}
}; // end namespace io_schedule
}; // end namespace oceanbase
