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
  bool is_inited() const;
  bool is_valid() const;
  int calc_phy_clock(const int64_t current_ts, const double iops_scale, const double weight_scale, ObPhyQueue *phy_queue);
  int dial_back_reservation_clock(const double iops_scale);
  int dial_back_proportion_clock(const int64_t delta_us);
  int64_t get_proportion_ts() const;
  TO_STRING_KV(K(is_inited_), K(is_stopped_), K_(reservation_clock), K_(limitation_clock), K_(proportion_clock));
private:
  bool is_inited_;
  bool is_stopped_;
  ObAtomIOClock reservation_clock_;
  ObAtomIOClock limitation_clock_;
  ObAtomIOClock proportion_clock_;
};

struct ObTenantIOConfig;
class ObTenantIOClock : public ObIOClock
{
public:
  ObTenantIOClock();
  virtual ~ObTenantIOClock();
  virtual int init(const ObTenantIOConfig &io_config, const ObIOUsage *io_usage) override;
  virtual void destroy() override;
  int calc_phyqueue_clock(ObPhyQueue *phy_queue, const ObIORequest &req);
  virtual int sync_clocks(ObIArray<ObIOClock *> &io_clocks) override;
  int sync_tenant_clock(ObTenantIOClock *ioclock);
  int adjust_reservation_clock(ObPhyQueue *phy_queue, const ObIORequest &req);
  int adjust_proportion_clock(const int64_t delta_us);
  virtual int update_io_config(const ObTenantIOConfig &io_config) override;
  int64_t get_min_proportion_ts();
  TO_STRING_KV(K(is_inited_), "category_clocks", ObArrayWrap<ObMClock>(category_clocks_, static_cast<int>(ObIOCategory::MAX_CATEGORY)),
      K_(other_clock), K_(unit_clock), K(io_config_), K(io_usage_));
private:
  ObMClock &get_mclock(const int category_index);
  double get_weight_scale(const int category_index);
  int64_t calc_iops(const int64_t iops, const int64_t percentage);
  int64_t calc_weight(const int64_t weight, const int64_t percentage);
private:
  bool is_inited_;
  ObMClock category_clocks_[static_cast<int>(ObIOCategory::MAX_CATEGORY)];
  ObMClock other_clock_;
  ObAtomIOClock unit_clock_;
  ObTenantIOConfig io_config_;
  const ObIOUsage *io_usage_;
};
} // namespace common
} // namespace oceanbase

#endif//OCEANBASE_LIB_STORAGE_IO_MCLOCK
