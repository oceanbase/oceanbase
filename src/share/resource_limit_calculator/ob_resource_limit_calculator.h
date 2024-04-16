/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_SHARE_OB_RESOURCE_LIMIT_CALCULCATOR_H
#define OCEANBASE_SHARE_OB_RESOURCE_LIMIT_CALCULCATOR_H

#include "lib/lock/ob_spin_rwlock.h"
#include "share/resource_limit_calculator/ob_resource_commmon.h"

namespace oceanbase
{
namespace share
{

class ObIResourceLimitCalculatorHandler
{
public:
  virtual int get_current_info(ObResourceInfo &info) = 0;
  virtual int get_resource_constraint_value(ObResoureConstraintValue &constraint_value) = 0;
  virtual int cal_min_phy_resource_needed(ObMinPhyResourceResult &min_phy_res) = 0;
  virtual int cal_min_phy_resource_needed(const int64_t num, ObMinPhyResourceResult &min_phy_res) = 0;
};

class ObLogicResourceStatIterator
{
public:
  ObLogicResourceStatIterator() : is_ready_(false),
                                  curr_type_(0) {}
  ~ObLogicResourceStatIterator() {}
  void reset();

  int set_ready();
  bool is_ready() const { return is_ready_; }

  int get_next(ObResourceInfo &info);
  int get_next_type();
  int64_t get_curr_type() const { return curr_type_; }
private:
  bool is_ready_;
  int64_t curr_type_;
};

class ObResourceConstraintIterator
{
public:
  ObResourceConstraintIterator() : is_ready_(false),
                                   res_type_(0),
                                   curr_constraint_type_(0),
                                   res_()
  {}
  ~ObResourceConstraintIterator() {}
  void reset();

  int set_ready(const int64_t res_type);
  bool is_ready() const { return is_ready_; }
  int get_next(int64_t &val);
  int64_t get_curr_type() const { return curr_constraint_type_; }
private:
  bool is_ready_;
  // which resource's constraint
  int64_t res_type_;
  int64_t curr_constraint_type_;
  ObResoureConstraintValue res_;
};

struct ObUserResourceCalculateArg
{
  OB_UNIS_VERSION_V(1);
public:
  ObUserResourceCalculateArg() : needed_num_() {}
  ~ObUserResourceCalculateArg() {}
  void reset()
  {
    for (int i = 0; i < needed_num_.count(); i++) {
      needed_num_[i] = 0;
    }
  }
  int get_type_value(const int64_t type, int64_t &value) const;
  int set_type_value(const int64_t type, const int64_t value);
  int assign(const ObUserResourceCalculateArg &other);
  int64_t count() const
  {
    return needed_num_.count();
  }

  DECLARE_TO_STRING;
private:
  common::ObSArray<int64_t> needed_num_;
  DISALLOW_COPY_AND_ASSIGN(ObUserResourceCalculateArg);
};

class ObResourceLimitCalculator
{
  using RWLock = common::SpinRWLock;
  using RLockGuard = common::SpinRLockGuard;
  using WLockGuard = common::SpinWLockGuard;
public:
  ObResourceLimitCalculator() : is_inited_(false) { }
  ~ObResourceLimitCalculator() { destroy(); }
  static int mtl_init(ObResourceLimitCalculator *&calculator);
  int init();
  void destroy()
  {
    WLockGuard guard(lock_);
    is_inited_ = false;
    for (int i = 0; i < MAX_LOGIC_RESOURCE; i++) {
      handlers_[i] = NULL;
    }
  }

  // DISPLAYER
  int get_logic_resource_stat(const int64_t type,
                              ObResourceInfo &val);
  int get_logic_resource_constraint_value(const int64_t type,
                                          ObResoureConstraintValue &val);

  int get_tenant_logical_resource(ObUserResourceCalculateArg &arg);
  // CALCULCATOR
  int get_tenant_min_phy_resource_value(ObMinPhyResourceResult &res);
  int get_tenant_min_phy_resource_value(const ObUserResourceCalculateArg &arg,
                                        ObMinPhyResourceResult &res);
private:
  bool is_inited_;
  RWLock lock_;
  ObIResourceLimitCalculatorHandler *handlers_[MAX_LOGIC_RESOURCE];
};

} // end namespace share
} // end namespace oceanbase
#endif
