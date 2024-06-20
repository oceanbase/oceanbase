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

#ifndef OCEANBASE_LIB_STORAGE_IO_MCLOCK
#define OCEANBASE_LIB_STORAGE_IO_MCLOCK

#include "share/io/ob_io_define.h"
#include "lib/container/ob_heap.h"
#include "lib/container/ob_array_iterator.h"
#include "lib/container/ob_array_wrap.h"

namespace oceanbase
{
namespace common
{

class ObMClock final
{
public:
  ObMClock();
  ~ObMClock();
  int init(const int64_t min_iops, const int64_t max_iops, const int64_t weight, const int64_t proportion_ts = 0);
  int update(const int64_t min_iops, const int64_t max_iops, const int64_t weight, const int64_t proportion_ts);
  void start();
  void stop();
  void destroy();
  void set_unlimited() { is_unlimited_ = true; }
  void set_limited() { is_unlimited_ = false; }
  bool is_inited() const;
  bool is_valid() const;
  bool is_stop() const;
  bool is_unlimited() const;
  int calc_phy_clock(const int64_t current_ts, const double iops_scale, const double weight_scale, ObPhyQueue *phy_queue);
  int dial_back_reservation_clock(const double iops_scale);
  int time_out_dial_back(const double iops_scale, const double weight_scale);
  int dial_back_proportion_clock(const int64_t delta_us);
  int64_t get_proportion_ts() const;
  TO_STRING_KV(K(is_inited_), K(is_stopped_), K_(reservation_clock), K_(is_unlimited), K_(limitation_clock), K_(proportion_clock));
private:
  bool is_inited_;
  bool is_stopped_;
  bool is_unlimited_; // use this flag to send io_req in useless_queue out ASAP
  ObAtomIOClock reservation_clock_;
  ObAtomIOClock limitation_clock_;
  ObAtomIOClock proportion_clock_;
};

struct ObTenantIOConfig;
class ObTenantIOClock
{
public:
  ObTenantIOClock();
  virtual ~ObTenantIOClock();
  int init(const ObTenantIOConfig &io_config, const ObIOUsage *io_usage);
  void destroy();
  int calc_phyqueue_clock(ObPhyQueue *phy_queue, ObIORequest &req);
  static int sync_clocks(ObIArray<ObTenantIOClock *> &io_clocks);
  int sync_tenant_clock(ObTenantIOClock *ioclock);
  int adjust_clocks(ObPhyQueue *phy_queue, ObIORequest &req);
  int adjust_reservation_clock(ObPhyQueue *phy_queue, ObIORequest &req);
  int adjust_proportion_clock(const int64_t delta_us);
  int update_io_clocks(const ObTenantIOConfig &io_config);
  int update_io_clock(const int64_t index, const ObTenantIOConfig &io_config, const int64_t all_group_num);
  int update_clock_unit_config(const ObTenantIOConfig &io_config);
  int64_t get_min_proportion_ts();
  bool is_unlimited_config(const ObMClock &clock, const ObTenantIOConfig::GroupConfig &cur_config);
  void stop_clock(const uint64_t index);
  TO_STRING_KV(K(is_inited_), "group_clocks", group_clocks_, "other_clock", other_group_clock_,
      K_(unit_clock), K(io_config_), K(io_usage_));
private:
  ObMClock &get_mclock(const int64_t queue_index);
  double get_weight_scale(const int64_t queue_index);
  int64_t calc_iops(const int64_t iops, const int64_t percentage);
  int64_t calc_weight(const int64_t weight, const int64_t percentage);
private:
  bool is_inited_;
  ObSEArray<ObMClock, GROUP_START_NUM> group_clocks_;
  ObMClock other_group_clock_;
  ObAtomIOClock unit_clock_;
  ObTenantIOConfig io_config_;
  const ObIOUsage *io_usage_;
};
} // namespace common
} // namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_IO_MCLOCK
