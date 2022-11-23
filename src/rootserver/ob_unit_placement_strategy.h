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
#include "share/unit/ob_unit_info.h"
#include "lib/container/ob_array.h"
#include "rootserver/ob_root_utils.h"
namespace oceanbase
{
namespace rootserver
{
class ObUnitPlacementStrategy
{
public:
  struct ObServerResource: public ObIServerResource
  {
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
    virtual const common::ObAddr &get_server() const { return addr_; }
    // return -1 if resource_type is invalid
    virtual double get_assigned(ObResourceType resource_type) const override;
    virtual double get_capacity(ObResourceType resource_type) const override;
    virtual double get_max_assigned(ObResourceType resource_type) const override;

    DECLARE_TO_STRING;
  };

  ObUnitPlacementStrategy() = default;
  virtual ~ObUnitPlacementStrategy() = default;
  /**
   * choose the best server for the unit
   *
   * @param [in] servers        candidates servers, all server should have enough resource to hold target unit
   *                            return error when servers resource is not enough
   *
   * @param [in] demand_resource    the demand resource
   * @param [in] module             module name
   * @param [out] server            the one be selected
   */
  virtual int choose_server(common::ObArray<ObServerResource> &servers,
                            const share::ObUnitResource &demands_resource,
                            const char *module,
                            common::ObAddr &server) = 0;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitPlacementStrategy);
};

// New strategy of V1.4
// Dot-Product Greedy Vector Balancing
class ObUnitPlacementDPStrategy: public ObUnitPlacementStrategy
{
public:
  ObUnitPlacementDPStrategy() {}
  virtual ~ObUnitPlacementDPStrategy() = default;

  // choose server with the max dot-product
  virtual int choose_server(common::ObArray<ObServerResource> &servers,
                            const share::ObUnitResource &demands_resource,
                            const char *module,
                            common::ObAddr &server) override;
private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(ObUnitPlacementDPStrategy);
};

} // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_UNIT_PLACEMENT_STRATEGY_H */
