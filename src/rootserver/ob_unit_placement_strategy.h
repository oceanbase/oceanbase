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

#ifndef _OB_UNIT_PLACEMENT_STRATEGY_H
#define _OB_UNIT_PLACEMENT_STRATEGY_H 1
#include "common/ob_unit_info.h"
#include "lib/container/ob_array.h"
#include "rootserver/ob_root_utils.h"
namespace oceanbase {
namespace rootserver {
class ObUnitPlacementStrategy {
public:
  struct ObServerResource : public ObIServerResource {
    common::ObAddr addr_;
    double capacity_[RES_MAX];
    double assigned_[RES_MAX];
    double max_assigned_[RES_MAX];

    ObServerResource()
    {
      memset(capacity_, 0, sizeof(capacity_));
      memset(assigned_, 0, sizeof(assigned_));
      memset(max_assigned_, 0, sizeof(max_assigned_));
    }
    // return -1 if resource_type is invalid
    virtual double get_assigned(ObResourceType resource_type) const override;
    virtual double get_capacity(ObResourceType resource_type) const override;
    virtual double get_max_assigned(ObResourceType resource_type) const override;

    TO_STRING_KV(K_(addr), "capacity", common::ObArrayWrap<double>(capacity_, RES_MAX), "assigned",
        common::ObArrayWrap<double>(assigned_, RES_MAX), "max_assigned",
        common::ObArrayWrap<double>(max_assigned_, RES_MAX));
  };

  ObUnitPlacementStrategy() = default;
  virtual ~ObUnitPlacementStrategy() = default;
  /**
   * choose the best server for the unit
   *
   * @param servers [in] candidates servers
   * @param unit_config [in] the unit config with resource demands
   * @param server [out] the one be selected
   *
   * @return OB_MACHINE_RESOURCE_NOT_ENOUGH when resource exhausted
   */
  virtual int choose_server(common::ObArray<ObServerResource>& servers, const share::ObUnitConfig& unit_config,
      common::ObAddr& server, const common::ObZone& zone, int64_t& find_index) = 0;

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitPlacementStrategy);
};

// New strategy of V1.4
// Dot-Product Greedy Vector Balancing
class ObUnitPlacementDPStrategy : public ObUnitPlacementStrategy {
public:
  ObUnitPlacementDPStrategy(double hard_limit = 1.0) : hard_limit_(hard_limit)
  {}
  virtual ~ObUnitPlacementDPStrategy() = default;

  // choose server with the max dot-product
  virtual int choose_server(common::ObArray<ObServerResource>& servers, const share::ObUnitConfig& unit_config,
      common::ObAddr& server, const common::ObZone& zone, int64_t& find_index) override;

private:
  bool have_enough_resource(const ObServerResource& server, const share::ObUnitConfig& unit_config);
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitPlacementDPStrategy);

private:
  double hard_limit_;
};

// Old Best Fit of V1.3, only consider cpu resource
class ObUnitPlacementBestFitStrategy : public ObUnitPlacementStrategy {
public:
  ObUnitPlacementBestFitStrategy(double soft_limit) : soft_limit_(soft_limit)
  {}
  virtual ~ObUnitPlacementBestFitStrategy() = default;

  virtual int choose_server(common::ObArray<ObServerResource>& servers, const share::ObUnitConfig& unit_config,
      common::ObAddr& server, const common::ObZone& zone, int64_t& find_index) override;

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitPlacementBestFitStrategy);
  // function members
  bool have_enough_resource(const ObServerResource& server, const share::ObUnitConfig& unit_config);

private:
  // data members
  double soft_limit_;
};

// Old Least Load First of V1.3, only consider cpu resource
class ObUnitPlacementFFDStrategy : public ObUnitPlacementStrategy {
public:
  ObUnitPlacementFFDStrategy(double hard_limit) : hard_limit_(hard_limit)
  {}
  virtual ~ObUnitPlacementFFDStrategy() = default;
  virtual int choose_server(common::ObArray<ObServerResource>& servers, const share::ObUnitConfig& unit_config,
      common::ObAddr& server, const common::ObZone& zone, int64_t& find_index) override;

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitPlacementFFDStrategy);
  // function members
  bool have_enough_resource(const ObServerResource& server, const share::ObUnitConfig& unit_config);

private:
  // data members
  double hard_limit_;
};

// Old strategy of V1.3
class ObUnitPlacementHybridStrategy : public ObUnitPlacementStrategy {
public:
  ObUnitPlacementHybridStrategy(double soft_limit, double hard_limit)
      : best_fit_first_(soft_limit), least_load_first_(hard_limit)
  {}
  virtual ~ObUnitPlacementHybridStrategy()
  {}

  virtual int choose_server(common::ObArray<ObServerResource>& servers, const share::ObUnitConfig& unit_config,
      common::ObAddr& server, const common::ObZone& zone, int64_t& find_index) override;

private:
  // types and constants
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitPlacementHybridStrategy);
  // function members
private:
  // data members
  ObUnitPlacementBestFitStrategy best_fit_first_;
  ObUnitPlacementFFDStrategy least_load_first_;
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif /* _OB_UNIT_PLACEMENT_STRATEGY_H */
