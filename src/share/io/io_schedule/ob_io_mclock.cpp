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

#include "ob_io_mclock.h"
#include "share/io/ob_io_manager.h"
#include "share/io/ob_io_calibration.h"
#include "lib/restore/ob_object_device.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

/******************             MClock              **********************/
ObMClock::ObMClock()
  : is_inited_(false),
    is_stopped_(false),
    is_unlimited_(false),
    limitation_clock_(),
    reservation_clock_(),
    proportion_clock_()
{

}

ObMClock::~ObMClock()
{
  destroy();
}

int ObMClock::init(const int64_t min_iops, const int64_t max_iops, const int64_t weight, const int64_t proportion_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(min_iops < 0 || max_iops < min_iops || weight < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_iops), K(max_iops), K(weight));
  } else {
    reservation_clock_.iops_ = min_iops;
    reservation_clock_.last_ns_ = 0;
    limitation_clock_.iops_ = max_iops;
    limitation_clock_.last_ns_ = 0;
    proportion_clock_.iops_ = weight;
    if (0 == proportion_ts) {
      proportion_clock_.last_ns_ = ObTimeUtility::fast_current_time() * 1000L;
    } else {
      proportion_clock_.last_ns_ = proportion_ts * 1000L;
    }
    is_unlimited_ = false;
    is_stopped_ = false;
    is_inited_ = true;
  }
  LOG_INFO("mclock init", K(ret), K(min_iops), K(max_iops), K(weight), K(proportion_ts), K(*this));
  return ret;
}

int ObMClock::update(const int64_t min_iops, const int64_t max_iops, const int64_t weight, const int64_t proportion_ts)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(min_iops < 0 || max_iops < min_iops || weight < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(min_iops), K(max_iops), K(weight));
  } else {
    reservation_clock_.iops_ = min_iops;
    limitation_clock_.iops_ = max_iops;
    proportion_clock_.iops_ = weight;
    proportion_clock_.last_ns_ = max(proportion_clock_.last_ns_, proportion_ts * 1000L);
  }
  LOG_INFO("mclock update", K(ret), K(*this), K(min_iops), K(max_iops), K(weight), K(proportion_ts));
  return ret;
}

void ObMClock::destroy()
{
  is_inited_ = false;
  is_stopped_ = false;
  is_unlimited_ = false;
  reservation_clock_.reset();
  limitation_clock_.reset();
  proportion_clock_.reset();
}

int ObMClock::dial_back_reservation_clock(const double iops_scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(iops_scale <= std::numeric_limits<double>::epsilon())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(iops_scale));
  } else {
    const int64_t delta_ns = reservation_clock_.iops_ == 0 ? 1000L * 1000L * 1000L / INT64_MAX : 1000L * 1000L * 1000L / (reservation_clock_.iops_ * iops_scale);
    ATOMIC_SAF(&reservation_clock_.last_ns_, delta_ns);
  }
  return ret;
}

int ObMClock::time_out_dial_back(const double iops_scale)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(iops_scale <= std::numeric_limits<double>::epsilon())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(iops_scale));
  } else {
    const int64_t r_delta_ns = 1000L * 1000L * 1000L / (reservation_clock_.iops_ * iops_scale);
    ATOMIC_SAF(&reservation_clock_.last_ns_, r_delta_ns);
    const int64_t l_delta_ns = 1000L * 1000L * 1000L / (limitation_clock_.iops_ * iops_scale);
    ATOMIC_SAF(&limitation_clock_.last_ns_, l_delta_ns);
    const int64_t p_delta_ns = 1000L * 1000L * 1000L / (proportion_clock_.iops_ * iops_scale);
    ATOMIC_SAF(&proportion_clock_.last_ns_, p_delta_ns);
  }
  return ret;
}

// may dial forward clock of new tenant is better?
int ObMClock::dial_back_proportion_clock(const int64_t delta_us)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(delta_us <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(delta_us));
  } else {
    ATOMIC_SAF(&proportion_clock_.last_ns_, delta_us * 1000L);
  }
  return ret;
}

int ObMClock::sync_proportion_clock(const int64_t clock_ns)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(clock_ns <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(clock_ns));
  } else {
    ATOMIC_STORE(&proportion_clock_.last_ns_, clock_ns);
  }
  return ret;
}

int64_t ObMClock::get_proportion_ts() const
{
  return 0 == proportion_clock_.last_ns_ ? INT64_MAX : (proportion_clock_.last_ns_ / 1000);
}
/******************             TenantIOClock              **********************/
ObTenantIOClock::ObTenantIOClock()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    group_clocks_(),
    io_config_(),
    io_usage_(nullptr)
{

}

ObTenantIOClock::~ObTenantIOClock()
{

}

int ObTenantIOClock::init(const uint64_t tenant_id, const ObTenantIOConfig &io_config, const ObIOUsage *io_usage)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!is_valid_tenant_id(tenant_id))) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(tenant_id));
  } else if (OB_UNLIKELY(!io_config.is_valid() || nullptr == io_usage)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(io_config), KP(io_usage));
  } else if (OB_FAIL(io_config_.deep_copy(io_config))) {
    LOG_WARN("get io config failed", K(ret), K(io_config));
  } else if (OB_FAIL(group_clocks_.reserve(io_config.group_configs_.count()))) {
    LOG_WARN("reserver group failed", K(ret), K(io_config.group_configs_.count()));
  } else {
    tenant_id_ = tenant_id;
    const ObTenantIOConfig::UnitConfig &unit_config = io_config.unit_config_;
    const int64_t all_group_num = io_config.group_configs_.count();
    for (int64_t i = 0; OB_SUCC(ret) && i < all_group_num; ++i) {
      const ObTenantIOConfig::GroupConfig &cur_config = io_config.group_configs_.at(i);
      ObMClock cur_clock;
      const int64_t min =    cur_config.mode_ == ObIOMode::MAX_MODE ? unit_config.min_iops_ : 0;
      const int64_t max =    cur_config.mode_ == ObIOMode::MAX_MODE ? unit_config.max_iops_ : unit_config.max_net_bandwidth_;
      const int64_t weight = cur_config.mode_ == ObIOMode::MAX_MODE ? unit_config.weight_   : unit_config.net_bandwidth_weight_;
      if (OB_UNLIKELY(!cur_config.is_valid())) {
        ret = OB_INVALID_CONFIG;
        LOG_WARN("config is not valid", K(ret), K(i), K(cur_config));
      } else if (OB_FAIL(cur_clock.init(calc_iops(min, cur_config.min_percent_),
                                        calc_iops(max, cur_config.max_percent_),
                                        calc_weight(weight, cur_config.weight_percent_)))) {
        LOG_WARN("init io clock failed", K(ret), K(i), K(cur_clock));
      } else if (OB_FAIL(group_clocks_.push_back(cur_clock))) {
        LOG_WARN("push back group io clock failed", K(ret), K(i), K(cur_clock));
      } else {
        LOG_INFO("init group clock success", K(i), K(unit_config), K(cur_config));
      }
    }
    if (OB_SUCC(ret)) {
      unit_clocks_[static_cast<int>(ObIOMode::READ)].iops_ = unit_config.max_net_bandwidth_;
      unit_clocks_[static_cast<int>(ObIOMode::READ)].last_ns_ = 0;
      unit_clocks_[static_cast<int>(ObIOMode::WRITE)].iops_ = unit_config.max_net_bandwidth_;
      unit_clocks_[static_cast<int>(ObIOMode::WRITE)].last_ns_ = 0;
      unit_clocks_[static_cast<int>(ObIOMode::MAX_MODE)].iops_ = unit_config.max_iops_;
      unit_clocks_[static_cast<int>(ObIOMode::MAX_MODE)].last_ns_ = 0;
      io_usage_ = io_usage;
      is_inited_ = true;
    }
  }
  return ret;
}

void ObTenantIOClock::destroy()
{
  is_inited_ = false;
  for (int64_t i = 0; i < group_clocks_.count(); ++i) {
    group_clocks_.at(i).destroy();
  }
  group_clocks_.destroy();
  io_usage_ = nullptr;
}

int ObTenantIOClock::calc_phyqueue_clock(ObPhyQueue *phy_queue, ObIORequest &req)
{
  int ret = OB_SUCCESS;
  const int64_t current_ts = ObTimeUtility::fast_current_time();
  bool is_unlimited = false;
  ObMClock *mclock = nullptr;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_ISNULL(req.io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else if (OB_UNLIKELY(!req.get_flag().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(req));
  } else if (req.get_flag().is_unlimited()) {
    is_unlimited = true;
  } else {
    uint64_t cur_queue_index = phy_queue->queue_index_;
    if (cur_queue_index < 0 || (cur_queue_index >= group_clocks_.count())) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index out of boundary", K(ret), K(cur_queue_index), K(group_clocks_.count()));
    } else if (OB_FAIL(get_mclock(cur_queue_index, mclock))) {
      LOG_WARN("get mclock failed", K(ret), K(cur_queue_index), K(group_clocks_.count()));
    } else {
      double iops_scale = 0;
      bool is_io_ability_valid = true;
      ObAtomIOClock* unit_clock = &unit_clocks_[static_cast<int>(ObIOMode::MAX_MODE)];
      if (req.fd_.device_handle_->is_object_device()) {
        iops_scale = 1.0 / req.get_align_size();
        unit_clock = &unit_clocks_[static_cast<int>(req.get_mode())];
      } else {
        ObIOCalibration::get_instance().get_iops_scale(req.get_mode(),
                                                       req.get_align_size(),
                                                       iops_scale,
                                                       is_io_ability_valid);
      }
      // if we want to enable iops_limit without configuration of __all_disk_io_calibration,
      // ignore is_io_ability_valid firstly.
      if (OB_UNLIKELY(is_io_ability_valid == false || OB_ISNULL(mclock) || mclock->is_unlimited())) {
        // unlimited
        is_unlimited = true;
      } else {
        const ObStorageIdMod &storage_info = ((ObObjectDevice*)(req.fd_.device_handle_))->get_storage_id_mod();
        if (req.fd_.device_handle_->is_object_device()) {
          if (OB_UNLIKELY(!storage_info.is_valid())) {
            LOG_WARN("invalid storage id", K(storage_info));
          } else {
            // bandwidth of nic & bucket
            OB_IO_MANAGER.get_tc().calc_clock(current_ts, req, phy_queue->limitation_ts_);
          }
        } else {
          // min_iops of group
          mclock->reservation_clock_.atom_update_reserve(current_ts  - PHY_QUEUE_BURST_USEC, iops_scale, phy_queue->reservation_ts_);
        }
        // iops/bandwidth weight of tenant & group, TODO fengshuo.fs: THIS IS NOT CORRECT
        mclock->proportion_clock_.atom_update(current_ts  - PHY_QUEUE_BURST_USEC, iops_scale, phy_queue->proportion_ts_);
        // max iops/bandwidth of group
        mclock->limitation_clock_.compare_and_update(current_ts  - PHY_QUEUE_BURST_USEC, iops_scale, phy_queue->limitation_ts_);
        // max iops/bandwidth of tenant
        unit_clock->compare_and_update(current_ts, iops_scale, phy_queue->limitation_ts_);
      }
    }
  }
  if (OB_FAIL(ret) || is_unlimited) {
    phy_queue->reservation_ts_ = current_ts;
    phy_queue->limitation_ts_ = current_ts;
    phy_queue->proportion_ts_ = current_ts;
  }
  return ret;
}

int ObTenantIOClock::sync_clocks(ObIArray<ObTenantIOClock *> &io_clocks)
{
  int ret = OB_SUCCESS;
  int64_t min_proportion_ts = INT64_MAX;
  for (int64_t i = 0; OB_SUCC(ret) && i < io_clocks.count(); ++i) {
    ObTenantIOClock *cur_clock = static_cast<ObTenantIOClock *>(io_clocks.at(i));
    if (OB_ISNULL(cur_clock)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("io clock is null", K(ret), K(i));
    } else {
      min_proportion_ts = min(min_proportion_ts, cur_clock->get_min_proportion_ts());
    }
  }

  const int64_t delta_us = min_proportion_ts - ObTimeUtility::fast_current_time();
  if (INT64_MAX != min_proportion_ts && delta_us > 0) {
    for (int i = 0; OB_SUCC(ret) && i < io_clocks.count(); ++i) {
      ObTenantIOClock *cur_clock = static_cast<ObTenantIOClock *>(io_clocks.at(i));
      if (OB_FAIL(cur_clock->adjust_proportion_clock(delta_us))) {
        LOG_WARN("dial back proportion clock failed", K(delta_us));
      }
    }
  }
  return ret;
}

int ObTenantIOClock::sync_tenant_clock(ObTenantIOClock *io_clock)
{
  int ret = OB_SUCCESS;
  int64_t max_proportion_ts = 0;
  if (OB_ISNULL(io_clock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io clock is null", K(ret));
  } else {
    max_proportion_ts = io_clock->get_max_proportion_ts();
    if (INT64_MAX != max_proportion_ts && max_proportion_ts > 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < group_clocks_.count(); ++i) {
        if (group_clocks_.at(i).is_valid() && !group_clocks_.at(i).is_stop()) {
          group_clocks_.at(i).sync_proportion_clock(max_proportion_ts);
        }
      }
    }
  }
  return ret;
}

int ObTenantIOClock::try_sync_tenant_clock(ObTenantIOClock *io_clock)
{
  int ret = OB_SUCCESS;
  bool need_sync = false;
  const int64_t cur_ts = ObTimeUtility::fast_current_time();
  const int64_t old_ts = last_sync_clock_ts_;
  if (cur_ts - old_ts >= MAX_IDLE_TIME_US) {
    need_sync = ATOMIC_BCAS(&last_sync_clock_ts_, old_ts, cur_ts);
  }
  if (need_sync) {
    ret = sync_tenant_clock(io_clock);
  }
  return ret;
}

int ObTenantIOClock::adjust_reservation_clock(ObPhyQueue *phy_queue, ObIORequest &req)
{
  int ret = OB_SUCCESS;
  uint64_t cur_queue_index = phy_queue->queue_index_;
  ObMClock *mclock = nullptr;
  if(cur_queue_index < 0 || (cur_queue_index >= group_clocks_.count() && cur_queue_index != 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("index out of boundary", K(ret), K(cur_queue_index));
  } else if (OB_ISNULL(req.io_result_)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("io result is null", K(ret));
  } else if (OB_FAIL(get_mclock(cur_queue_index, mclock))) {
    LOG_WARN("get mclock failed", K(ret), K(cur_queue_index), K(group_clocks_.count()));
  } else if (OB_ISNULL(mclock)) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("mclock is null", K(ret), K(cur_queue_index), K(group_clocks_.count()));
  } else {
    double iops_scale = 0;
    bool is_io_ability_valid = true;
    if (req.fd_.device_handle_->is_object_device()) {
      // there is no reservation clock in shared device, do nothing
    } else if (FALSE_IT(ObIOCalibration::get_instance().get_iops_scale(req.get_mode(),
                                                                       req.get_align_size(),
                                                                       iops_scale,
                                                                       is_io_ability_valid))) {
      LOG_WARN("get iops scale failed", K(ret), K(req));
    } else if (is_io_ability_valid && OB_FAIL(mclock->dial_back_reservation_clock(iops_scale))) {
      LOG_WARN("dial back reservation clock failed", K(ret), K(iops_scale), K(req), K(mclock));
    }
  }
  return ret;
}

int ObTenantIOClock::adjust_proportion_clock(const int64_t delta_us)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < group_clocks_.count(); ++i) {
    if (group_clocks_.at(i).is_valid() && !group_clocks_.at(i).is_stop()) {
      group_clocks_.at(i).dial_back_proportion_clock(delta_us);
    }
  }
  return ret;
}

int ObTenantIOClock::update_io_clocks(const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_config_.deep_copy(io_config))) {
    LOG_WARN("get io config failed", K(ret), K(io_config));
  } else {
    if (group_clocks_.count() < io_config.group_configs_.count()) {
      if (OB_FAIL(group_clocks_.reserve(io_config.group_configs_.count()))) {
        LOG_WARN("reserve group config failed", K(ret), K(group_clocks_), K(io_config.group_configs_.count()));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t all_group_num = io_config.group_configs_.count();
      for (int64_t i = 0; OB_SUCC(ret) && i < all_group_num; ++i) {
        if (OB_FAIL(update_io_clock(i, io_config))) {
          LOG_WARN("update cur clock failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        unit_clocks_[static_cast<int>(ObIOMode::READ)].iops_ = io_config.unit_config_.max_net_bandwidth_;
        unit_clocks_[static_cast<int>(ObIOMode::WRITE)].iops_ = io_config.unit_config_.max_net_bandwidth_;
        unit_clocks_[static_cast<int>(ObIOMode::MAX_MODE)].iops_ = io_config.unit_config_.max_iops_;
        is_inited_ = true;
      }
      LOG_INFO("update io unit and group clock success", K(tenant_id_), K(*this));
    }
  }
  return ret;
}

bool ObTenantIOClock::is_unlimited_config(const ObMClock &clock, const ObTenantIOConfig::GroupConfig &cur_config)
{
  return clock.is_stop() ||
         cur_config.deleted_ ||
         cur_config.cleared_ ||
         (cur_config.min_percent_ == INT64_MAX && cur_config.max_percent_ == INT64_MAX);
}

int ObTenantIOClock::update_io_clock(const int64_t index, const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  const ObTenantIOConfig::UnitConfig &unit_config = io_config.unit_config_;
  const int64_t min_proportion_ts = get_min_proportion_ts();
  const ObTenantIOConfig::GroupConfig &cur_config = io_config.group_configs_.at(index);
  const int64_t min =    cur_config.mode_ == ObIOMode::MAX_MODE ? unit_config.min_iops_ : 0;
  const int64_t max =    cur_config.mode_ == ObIOMode::MAX_MODE ? unit_config.max_iops_ : unit_config.max_net_bandwidth_;
  const int64_t weight = cur_config.mode_ == ObIOMode::MAX_MODE ? unit_config.weight_   : unit_config.net_bandwidth_weight_;
  if (index < group_clocks_.count()) {
    // 2. update exist clocks
    if (!group_clocks_.at(index).is_inited()) {
      LOG_WARN("clock is not init", K(ret), K(index), K(group_clocks_.at(index)));
    } else if (is_unlimited_config(group_clocks_.at(index), cur_config)) {
      group_clocks_.at(index).set_unlimited();
      LOG_INFO("clock set unlimited", K(group_clocks_.at(index)), K(cur_config));
    } else if (!cur_config.is_valid()) {
      LOG_WARN("config is not valid", K(ret), K(index), K(cur_config), K(group_clocks_.at(index)));
      // stop
      group_clocks_.at(index).stop();
    } else if (OB_FAIL(group_clocks_.at(index).update(calc_iops(min, cur_config.min_percent_),
                                                      calc_iops(max, cur_config.max_percent_),
                                                      calc_weight(weight, cur_config.weight_percent_),
                                                      min_proportion_ts))) {
      LOG_WARN("update group io clock failed", K(ret), K(index), K(unit_config), K(cur_config), K(group_clocks_.at(index)), K(group_clocks_.count()));
    } else {
      group_clocks_.at(index).start();
      if (!is_unlimited_config(group_clocks_.at(index), cur_config)) {
        group_clocks_.at(index).set_limited();
      }
      LOG_INFO("update group clock success", K(index), K(tenant_id_), K(unit_config), K(cur_config), K(group_clocks_.at(index)));
    }
  } else {
    // 3. add new clocks
    ObMClock cur_clock;
    if (OB_UNLIKELY(!cur_config.is_valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("config is not valid", K(ret), K(index), K(cur_config));
    } else if (OB_FAIL(cur_clock.init(calc_iops(min, cur_config.min_percent_),
                                      calc_iops(max, cur_config.max_percent_),
                                      calc_weight(weight, cur_config.weight_percent_)))) {
      LOG_WARN("init io clock failed", K(ret), K(index), K(cur_clock));
    } else if (OB_FAIL(group_clocks_.push_back(cur_clock))) {
      LOG_WARN("push back io clock failed", K(ret), K(index), K(cur_clock));
    } else {
      LOG_INFO("init new group clock success", K(tenant_id_), K(index), K(unit_config), K(cur_config), K(cur_clock));
    }
  }
  return ret;
}

int64_t ObTenantIOClock::get_min_proportion_ts()
{
  int64_t min_proportion_ts = INT64_MAX;
  for (int64_t i = 0; i < group_clocks_.count(); ++i) {
    if (group_clocks_.at(i).is_valid()) {
      min_proportion_ts = min(min_proportion_ts, group_clocks_.at(i).get_proportion_ts());
    }
  }
  return min_proportion_ts;
}

int64_t ObTenantIOClock::get_max_proportion_ts()
{
  int64_t max_proportion_ts = 0;
  for (int64_t i = 0; i < group_clocks_.count(); ++i) {
    if (group_clocks_.at(i).is_valid()) {
      max_proportion_ts = max(max_proportion_ts, group_clocks_.at(i).get_proportion_ts());
    }
  }
  return max_proportion_ts;
}

int ObTenantIOClock::get_mclock(const int64_t queue_index, ObMClock *&mclock)
{
  int ret = OB_SUCCESS;
  if (queue_index >= group_clocks_.count()) {
    ret = OB_INNER_STAT_ERROR;
    LOG_WARN("index out of boundary", K(ret), K(queue_index), K(group_clocks_.count()));
  } else {
    mclock = &group_clocks_.at(queue_index);
  }
  return ret;
}

int64_t ObTenantIOClock::calc_iops(const int64_t iops, const int64_t percentage)
{
  return static_cast<double>(iops) / 100 * percentage >= INT64_MAX ? INT64_MAX : static_cast<int64_t>(static_cast<double>(iops) / 100 * percentage);
}

int64_t ObTenantIOClock::calc_weight(const int64_t weight, const int64_t percentage)
{
  return static_cast<int64_t>(static_cast<double>(weight) * percentage);
}


void ObTenantIOClock::stop_clock(const uint64_t index)
{
  if (index < group_clocks_.count() && index >= 0) {
    group_clocks_.at(index).stop();
  }
}
