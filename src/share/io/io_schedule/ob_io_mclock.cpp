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

#include "share/io/io_schedule/ob_io_mclock.h"
#include "share/io/ob_io_manager.h"
#include "share/io/ob_io_calibration.h"
#include "lib/time/ob_time_utility.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;

/******************             MClock              **********************/
ObMClock::ObMClock()
  : is_inited_(false),
    is_stopped_(false),
    reservation_clock_(),
    limitation_clock_(),
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
    proportion_clock_.last_ns_ = proportion_ts * 1000L;
    is_stopped_ = false;
    is_inited_ = true;
  }
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
  return ret;
}

void ObMClock::start()
{
  is_stopped_ = false;
}

void ObMClock::stop()
{
  is_stopped_ = true;
}

void ObMClock::destroy()
{
  is_inited_ = false;
  is_stopped_ = false;
  reservation_clock_.reset();
  limitation_clock_.reset();
  proportion_clock_.reset();
}

bool ObMClock::is_inited() const
{
  return is_inited_;
}

bool ObMClock::is_valid() const
{
  return is_inited_ && !is_stopped_;
}

bool ObMClock::is_stop() const
{
  return is_stopped_;
}

int ObMClock::calc_phy_clock(const int64_t current_ts, const double iops_scale, const double weight_scale, ObPhyQueue *phy_queue)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(current_ts <= 0
        || iops_scale <= std::numeric_limits<double>::epsilon()
        || weight_scale <= std::numeric_limits<double>::epsilon())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(current_ts), K(iops_scale), K(weight_scale));
  } else {
    reservation_clock_.atom_update(current_ts, iops_scale, phy_queue->reservation_ts_);
    limitation_clock_.atom_update(current_ts, iops_scale, phy_queue->group_limitation_ts_);
    proportion_clock_.atom_update(current_ts, iops_scale * weight_scale, phy_queue->proportion_ts_);
  }
  return ret;
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

int64_t ObMClock::get_proportion_ts() const
{
  return 0 == proportion_clock_.last_ns_ ? INT64_MAX : proportion_clock_.last_ns_;
}
/******************             TenantIOClock              **********************/
ObTenantIOClock::ObTenantIOClock()
  : is_inited_(false),
    group_clocks_(),
    other_group_clock_(),
    io_config_(),
    io_usage_(nullptr)
{

}

ObTenantIOClock::~ObTenantIOClock()
{

}

int ObTenantIOClock::init(const ObTenantIOConfig &io_config, const ObIOUsage *io_usage)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!io_config.is_valid() || nullptr == io_usage)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(io_config), KP(io_usage));
  } else if (OB_FAIL(io_config_.deep_copy(io_config))) {
    LOG_WARN("get io config failed", K(ret), K(io_config));
  } else if (OB_FAIL(group_clocks_.reserve(io_config.group_num_))) {
    LOG_WARN("reserver group failed", K(ret), K(io_config.group_num_));
  } else {
    const ObTenantIOConfig::UnitConfig &unit_config = io_config.unit_config_;
    const int64_t all_group_num = io_config.get_all_group_num();
    for (int64_t i = 0; OB_SUCC(ret) && i < all_group_num; ++i) {
      if (i == all_group_num - 1) {
        //OTHER_GROUPS
        const ObTenantIOConfig::GroupConfig &cur_config = io_config.other_group_config_;
        if (OB_UNLIKELY(!cur_config.is_valid())) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("config is not valid", K(ret), K(i), K(cur_config));
        } else if (OB_FAIL(other_group_clock_.init(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                                   calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                                   calc_weight(unit_config.weight_, cur_config.weight_percent_)))) {
          LOG_WARN("init io clock failed", K(ret), K(i), K(other_group_clock_));
        } else {
          LOG_INFO("init other group clock success", K(i), K(unit_config), K(cur_config));
        }
      } else {
        //regular groups
        const ObTenantIOConfig::GroupConfig &cur_config = io_config.group_configs_.at(i);
        ObMClock cur_clock;
        if (OB_UNLIKELY(!cur_config.is_valid())) {
          ret = OB_INVALID_CONFIG;
          LOG_WARN("config is not valid", K(ret), K(i), K(cur_config));
        } else if (OB_FAIL(cur_clock.init(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                          calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                          calc_weight(unit_config.weight_, cur_config.weight_percent_)))) {
          LOG_WARN("init io clock failed", K(ret), K(i), K(cur_clock));
        } else if (OB_FAIL(group_clocks_.push_back(cur_clock))) {
          LOG_WARN("push back group io clock failed", K(ret), K(i), K(cur_clock));
        } else {
          LOG_INFO("init group clock success", K(i), K(unit_config), K(cur_config));
        }
      }
    }
    if (OB_SUCC(ret)) {
      unit_clock_.iops_ = unit_config.max_iops_;
      unit_clock_.last_ns_ = 0;
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
  other_group_clock_.destroy();
  group_clocks_.destroy();
  io_usage_ = nullptr;
}

int ObTenantIOClock::calc_phyqueue_clock(ObPhyQueue *phy_queue, const ObIORequest &req)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!is_inited_)) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret), K(is_inited_));
  } else if (OB_UNLIKELY(!req.get_flag().is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(req));
  } else if (req.get_flag().is_unlimited()) {
    const int64_t current_ts = ObTimeUtility::fast_current_time();
    phy_queue->reservation_ts_ = current_ts;
    phy_queue->group_limitation_ts_ = current_ts;
    phy_queue->tenant_limitation_ts_ = current_ts;
    phy_queue->proportion_ts_ = current_ts;
  } else {
    const int64_t current_ts = ObTimeUtility::fast_current_time();
    uint64_t cur_queue_index = phy_queue->queue_index_;
    if (cur_queue_index < 0 || (cur_queue_index >= group_clocks_.count() && cur_queue_index != INT64_MAX)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index out of boundary", K(ret), K(cur_queue_index), K(group_clocks_.count()));
    } else {
      ObMClock &mclock = get_mclock(cur_queue_index);
      double weight_scale = get_weight_scale(cur_queue_index);
      double iops_scale = 0;
      bool is_io_ability_valid = true;
      if (OB_FAIL(ObIOCalibration::get_instance().get_iops_scale(req.get_mode(),
                                                                 max(req.io_info_.size_, req.io_size_),
                                                                 iops_scale,
                                                                 is_io_ability_valid))) {
        LOG_WARN("get iops scale failed", K(ret), K(req));
      } else if (OB_UNLIKELY(is_io_ability_valid == false)) {
        //unlimited
        const int64_t current_ts = ObTimeUtility::fast_current_time();
        phy_queue->reservation_ts_ = current_ts;
        phy_queue->group_limitation_ts_ = current_ts;
        phy_queue->tenant_limitation_ts_ = current_ts;
        phy_queue->proportion_ts_ = current_ts;
      } else if (OB_FAIL(mclock.calc_phy_clock(current_ts, iops_scale, weight_scale, phy_queue))) {
        LOG_WARN("calculate clock of the request failed", K(ret), K(mclock), K(weight_scale));
      } else {
        // ensure not exceed max iops of the tenant
        unit_clock_.atom_update(current_ts, iops_scale, phy_queue->tenant_limitation_ts_);
      }
    }
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
  int64_t min_proportion_ts = INT64_MAX;
  if (OB_ISNULL(io_clock)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("io clock is null", K(ret));
  } else {
    min_proportion_ts = min(min_proportion_ts, io_clock->get_min_proportion_ts());
  }
  const int64_t delta_tenant_us = min_proportion_ts - ObTimeUtility::fast_current_time();
  if (OB_SUCC(ret) && INT64_MAX != min_proportion_ts && delta_tenant_us > 0) {
    if (OB_FAIL(io_clock->adjust_proportion_clock(delta_tenant_us))) {
      LOG_WARN("dial back proportion clock failed", K(delta_tenant_us));
    }
  }
  return ret;
}

int ObTenantIOClock::adjust_reservation_clock(ObPhyQueue *phy_queue, const ObIORequest &req)
{
  int ret = OB_SUCCESS;
  uint64_t cur_queue_index = phy_queue->queue_index_;
  if(cur_queue_index < 0 || (cur_queue_index >= group_clocks_.count() && cur_queue_index != INT64_MAX)) {
    LOG_WARN("index out of boundary", K(ret), K(cur_queue_index));
  } else {
    ObMClock &mclock = get_mclock(cur_queue_index);
    double iops_scale = 0;
    bool is_io_ability_valid = true;
    if (OB_FAIL(ObIOCalibration::get_instance().get_iops_scale(req.get_mode(),
                                                               max(req.io_info_.size_, req.io_size_),
                                                               iops_scale,
                                                               is_io_ability_valid))) {
      LOG_WARN("get iops scale failed", K(ret), K(req));
    } else if (OB_FAIL(mclock.dial_back_reservation_clock(iops_scale))) {
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
  if (other_group_clock_.is_valid() && !other_group_clock_.is_stop()) {
    other_group_clock_.dial_back_proportion_clock(delta_us);
  }
  return ret;
}

int ObTenantIOClock::update_io_clocks(const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(io_config_.deep_copy(io_config))) {
    LOG_WARN("get io config failed", K(ret), K(io_config));
  } else {
    if (group_clocks_.count() < io_config.group_num_) {
      if (OB_FAIL(group_clocks_.reserve(io_config.group_num_))) {
        LOG_WARN("reserve group config failed", K(ret), K(group_clocks_), K(io_config.group_num_));
      }
    }
    if (OB_SUCC(ret)) {
      const int64_t all_group_num = io_config.get_all_group_num();
      for (int64_t i = 0; OB_SUCC(ret) && i < all_group_num; ++i) {
        if (OB_FAIL(update_io_clock(i, io_config, all_group_num))) {
          LOG_WARN("update cur clock failed", K(ret), K(i));
        }
      }
      if (OB_SUCC(ret)) {
        unit_clock_.iops_ = io_config.unit_config_.max_iops_;
        is_inited_ = true;
      }
    }
  }
  return ret;
}

int ObTenantIOClock::update_io_clock(const int64_t index, const ObTenantIOConfig &io_config, const int64_t all_group_num)
{
  int ret = OB_SUCCESS;
  const ObTenantIOConfig::UnitConfig &unit_config = io_config.unit_config_;
  const int64_t min_proportion_ts = get_min_proportion_ts();
  if (index == all_group_num - 1) {
    //1. update other group
    const ObTenantIOConfig::GroupConfig &cur_config = io_config.other_group_config_;
    if (OB_UNLIKELY(!other_group_clock_.is_inited())) {
      LOG_WARN("clock is not init", K(ret), K(index), K(other_group_clock_));
    } else if (OB_UNLIKELY(!cur_config.is_valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("config is not valid", K(ret), K(index), K(cur_config));
      // stop
      other_group_clock_.stop();
    } else if (OB_FAIL(other_group_clock_.update(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                                 calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                                 calc_weight(unit_config.weight_, cur_config.weight_percent_),
                                                 min_proportion_ts))) {
      LOG_WARN("update other group io clock failed", K(ret), K(index), K(other_group_clock_));
    } else {
      other_group_clock_.start();
      LOG_INFO("update other group clock success", K(index), K(unit_config), K(cur_config));
    }
  } else if (index < group_clocks_.count()) {
    // 2. update exist clocks
    const ObTenantIOConfig::GroupConfig &cur_config = io_config.group_configs_.at(index);
    if (!group_clocks_.at(index).is_inited()) {
      LOG_WARN("clock is not init", K(ret), K(index), K(group_clocks_.at(index)));
    } else if (group_clocks_.at(index).is_stop() || cur_config.deleted_) {
      // group has been deleted, ignore
    } else if (!cur_config.is_valid()) {
      LOG_WARN("config is not valid", K(ret), K(index), K(cur_config), K(group_clocks_.at(index)));
      // stop
      group_clocks_.at(index).stop();
    } else if (OB_FAIL(group_clocks_.at(index).update(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                                      calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                                      calc_weight(unit_config.weight_, cur_config.weight_percent_),
                                                      min_proportion_ts))) {
      LOG_WARN("update group io clock failed", K(ret), K(index), K(unit_config), K(cur_config));
    } else {
      group_clocks_.at(index).start();
      LOG_INFO("update group clock success", K(index), K(unit_config), K(cur_config));
    }
  } else {
    // 3. add new clocks
    const ObTenantIOConfig::GroupConfig &cur_config = io_config.group_configs_.at(index);
    ObMClock cur_clock;
    if (OB_UNLIKELY(!cur_config.is_valid())) {
      ret = OB_INVALID_CONFIG;
      LOG_WARN("config is not valid", K(ret), K(index), K(cur_config));
    } else if (OB_FAIL(cur_clock.init(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                      calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                      calc_weight(unit_config.weight_, cur_config.weight_percent_)))) {
      LOG_WARN("init io clock failed", K(ret), K(index), K(cur_clock));
    } else if (OB_FAIL(group_clocks_.push_back(cur_clock))) {
      LOG_WARN("push back io clock failed", K(ret), K(index), K(cur_clock));
    } else {
      LOG_INFO("init new group clock success", K(index), K(unit_config), K(cur_config));
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
  if (other_group_clock_.is_valid()) {
    min_proportion_ts = min(min_proportion_ts, other_group_clock_.get_proportion_ts());
  }
  return min_proportion_ts;
}

ObMClock &ObTenantIOClock::get_mclock(const int64_t queue_index)
{
  ObMClock &io_clock = queue_index == INT64_MAX ? other_group_clock_ : group_clocks_.at(queue_index);
  return io_clock;
}

double ObTenantIOClock::get_weight_scale(const int64_t queue_index)
{
  double weight_scale = 1;
  if (OB_ISNULL(io_usage_)) {
    // do nothing
  } else {
    int64_t sum_weight_percent = 0;
    bool need_add_other_weight = true;
    for (int64_t i = 0; i < io_config_.group_num_; ++i) {
      int64_t usage_index = i + 1;
      if (usage_index < io_usage_->get_io_usage_num() && io_usage_->is_request_doing(usage_index)) {
        if (group_clocks_.at(i).is_valid()) {
          sum_weight_percent += io_config_.group_configs_.at(i).weight_percent_;
        }
      }
    }
    if (io_usage_->get_io_usage_num() > 0 && io_usage_->is_request_doing(0)) {
      sum_weight_percent += io_config_.other_group_config_.weight_percent_;
    }
    if (sum_weight_percent > 0) {
      weight_scale = 100.0 / sum_weight_percent;
    }
  }
  return weight_scale;
}

int64_t ObTenantIOClock::calc_iops(const int64_t iops, const int64_t percentage)
{
  return static_cast<double>(iops) / 100 * percentage >= INT64_MAX ? INT64_MAX : static_cast<int64_t>(static_cast<double>(iops) / 100 * percentage);
}

int64_t ObTenantIOClock::calc_weight(const int64_t weight, const int64_t percentage)
{
  return static_cast<int64_t>(static_cast<double>(weight) * percentage / 100);
}


void ObTenantIOClock::stop_clock(const uint64_t index)
{
  if (index < group_clocks_.count() && index >= 0) {
    group_clocks_.at(index).stop();
  }
}
