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
  } else if (OB_UNLIKELY(min_iops <= 0 || max_iops < min_iops || weight < 0)) {
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
  } else if (OB_UNLIKELY(min_iops <= 0 || max_iops < min_iops || weight < 0)) {
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
    limitation_clock_.atom_update(current_ts, iops_scale, phy_queue->category_limitation_ts_);
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
    const int64_t delta_ns = 1000L * 1000L * 1000L / (reservation_clock_.iops_ * iops_scale);
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
    category_clocks_(),
    other_clock_(),
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
  } else {
    io_config_ = io_config;
    const ObTenantIOConfig::UnitConfig &unit_config = io_config.unit_config_;
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int>(ObIOCategory::MAX_CATEGORY) + 1; ++i) {
      const ObTenantIOConfig::CategoryConfig &cur_config =
        static_cast<int>(ObIOCategory::MAX_CATEGORY) == i ? io_config.other_config_ : io_config.category_configs_[i];
      ObMClock &cur_clock = static_cast<int>(ObIOCategory::MAX_CATEGORY) == i ? other_clock_ : category_clocks_[i];
      if (cur_config.is_valid()) {
        if (OB_FAIL(cur_clock.init(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                   calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                   calc_weight(unit_config.weight_, cur_config.weight_percent_)))) {
          LOG_WARN("init category io clock failed", K(ret), K(i), K(unit_config), K(cur_config));
        } else {
          LOG_INFO("init category clock", K(i), K(unit_config), K(cur_config));
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
  for (int64_t i = 0; i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
    category_clocks_[i].destroy();
  }
  other_clock_.destroy();
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
    phy_queue->category_limitation_ts_ = current_ts;
    phy_queue->tenant_limitation_ts_ = current_ts;
    phy_queue->proportion_ts_ = current_ts;
  } else {
    const int64_t current_ts = ObTimeUtility::fast_current_time();
    int cate_index=phy_queue->category_index_;
    if(cate_index < 0 || cate_index >= static_cast<int>(ObIOCategory::MAX_CATEGORY)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("index out of boundary", K(ret), K(cate_index));
    } else {
      ObMClock &mclock = get_mclock(cate_index);
      double weight_scale = get_weight_scale(cate_index);
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
        phy_queue->category_limitation_ts_ = current_ts;
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

int ObTenantIOClock::sync_clocks(ObIArray<ObIOClock *> &io_clocks)
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
  int cate_index=phy_queue->category_index_;
  if(cate_index < 0 || cate_index >= static_cast<int>(ObIOCategory::MAX_CATEGORY)) {
    LOG_WARN("index out of boundary", K(ret), K(cate_index));
  } else {
    ObMClock &mclock = get_mclock(cate_index);
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
  for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
    if (category_clocks_[i].is_valid()) {
      category_clocks_[i].dial_back_proportion_clock(delta_us);
    }
  }
  if (other_clock_.is_valid()) {
    other_clock_.dial_back_proportion_clock(delta_us);
  }
  return ret;
}

int ObTenantIOClock::update_io_config(const ObTenantIOConfig &io_config)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!io_config.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(io_config));
  } else {
    io_config_ = io_config;
    const int64_t min_proportion_ts = get_min_proportion_ts();
    const ObTenantIOConfig::UnitConfig &unit_config = io_config.unit_config_;
    for (int64_t i = 0; OB_SUCC(ret) && i < static_cast<int>(ObIOCategory::MAX_CATEGORY) + 1; ++i) {
      const ObTenantIOConfig::CategoryConfig &cur_config =
        static_cast<int>(ObIOCategory::MAX_CATEGORY) == i ? io_config.other_config_ : io_config.category_configs_[i];
      ObMClock &cur_clock = static_cast<int>(ObIOCategory::MAX_CATEGORY) == i ? other_clock_ : category_clocks_[i];
      if (cur_config.is_valid() && cur_clock.is_inited()) {
        // update
        if (OB_FAIL(cur_clock.update(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                     calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                     calc_weight(unit_config.weight_, cur_config.weight_percent_),
                                     min_proportion_ts))) {
          LOG_WARN("update category io clock failed", K(ret), K(i), K(unit_config), K(cur_config));
        } else {
          cur_clock.start();
          LOG_INFO("update category clock", K(i), K(unit_config), K(cur_config));
        }
      } else if (cur_config.is_valid() && !cur_clock.is_inited()) {
        // init the new category
        if (OB_FAIL(cur_clock.init(calc_iops(unit_config.min_iops_, cur_config.min_percent_),
                                   calc_iops(unit_config.max_iops_, cur_config.max_percent_),
                                   calc_weight(unit_config.weight_, cur_config.weight_percent_),
                                   min_proportion_ts))) {
          LOG_WARN("init category io clock failed", K(ret), K(i), K(unit_config), K(cur_config));
        } else {
          LOG_INFO("init category clock", K(i), K(unit_config), K(cur_config));
        }
      } else if (!cur_config.is_valid() && cur_clock.is_inited()) {
        // stop
        cur_clock.stop();
      }
    }
    if (OB_SUCC(ret)) {
      unit_clock_.iops_ = unit_config.max_iops_;
      is_inited_ = true;
    }
  }
  return ret;
}

int64_t ObTenantIOClock::get_min_proportion_ts()
{
  int64_t min_proportion_ts = INT64_MAX;
  for (int64_t i = 0; i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
    if (category_clocks_[i].is_valid()) {
      min_proportion_ts = min(min_proportion_ts, category_clocks_[i].get_proportion_ts());
    }
  }
  if (other_clock_.is_valid()) {
    min_proportion_ts = min(min_proportion_ts, other_clock_.get_proportion_ts());
  }
  return min_proportion_ts;
}

ObMClock &ObTenantIOClock::get_mclock(const int category_index)
{
  const bool use_category_clock = category_clocks_[category_index].is_valid();
  ObMClock &io_clock = use_category_clock ? category_clocks_[category_index] : other_clock_;
  return io_clock;
}

double ObTenantIOClock::get_weight_scale(const int category_index)
{
  double weight_scale = 1;
  if (OB_ISNULL(io_usage_)) {
    // do nothing
  } else {
    const bool is_other_category = !category_clocks_[category_index].is_valid();
    int64_t sum_weight_percent = 0;
    bool need_add_other_weight = true;
    for (int64_t i = 0; i < static_cast<int>(ObIOCategory::MAX_CATEGORY); ++i) {
      if (io_usage_->is_request_doing(static_cast<ObIOCategory>(i))) {
        if (category_clocks_[i].is_valid()) {
          sum_weight_percent += io_config_.category_configs_[i].weight_percent_;
        } else if (need_add_other_weight) {
          sum_weight_percent += io_config_.other_config_.weight_percent_;
          need_add_other_weight = false;
        }
      }
    }
    if (sum_weight_percent > 0) {
      weight_scale = 100.0 / sum_weight_percent;
    }
  }
  return weight_scale;
}

int64_t ObTenantIOClock::calc_iops(const int64_t iops, const int64_t percentage)
{
  return max(1, static_cast<int64_t>(static_cast<double>(iops) * percentage / 100));
}

int64_t ObTenantIOClock::calc_weight(const int64_t weight, const int64_t percentage)
{
  return static_cast<int64_t>(static_cast<double>(weight) * percentage / 100);
}
