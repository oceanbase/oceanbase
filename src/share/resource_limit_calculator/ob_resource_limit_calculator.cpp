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

#define USING_LOG_PREFIX SHARE
#include "share/rc/ob_tenant_base.h"
#include "share/resource_limit_calculator/ob_resource_limit_calculator.h"
#include "storage/tx_storage/ob_ls_service.h"

namespace oceanbase
{
namespace share
{

void ObLogicResourceStatIterator::reset()
{
  is_ready_ = false;
  curr_type_ = 0;
}

int ObLogicResourceStatIterator::set_ready()
{
  int ret = OB_SUCCESS;

  if (is_ready_) {
    LOG_WARN("ObLogicResourceStatIterator is already ready");
    ret = OB_ERR_UNEXPECTED;
  } else {
    is_ready_ = true;
  }
  return ret;
}

int ObLogicResourceStatIterator::get_next(ObResourceInfo &info)
{
  int ret = OB_SUCCESS;
  if (!is_ready()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogicResourceStatIterator is not ready", K(ret));
  } else {
    bool need_retry = false;
    do {
      need_retry = false;
      curr_type_++;
      if (curr_type_ >= MAX_LOGIC_RESOURCE) {
        ret = OB_ITER_END;
      } else if (!is_valid_logic_res_type(curr_type_)) {
        need_retry = true;
      } else if (OB_FAIL(MTL(ObResourceLimitCalculator *)->get_logic_resource_stat(curr_type_,
                                                                                   info))) {
        LOG_WARN("get_next failed", K(ret), K(curr_type_));
      }
    } while (need_retry);
  }
  return ret;
}

int ObLogicResourceStatIterator::get_next_type()
{
  int ret = OB_SUCCESS;
  if (!is_ready()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObLogicResourceStatIterator is not ready", K(ret));
  } else {
    bool need_retry = false;
    do {
      need_retry = false;
      curr_type_++;
      if (curr_type_ >= MAX_LOGIC_RESOURCE) {
        ret = OB_ITER_END;
      } else if (!is_valid_logic_res_type(curr_type_)) {
        need_retry = true;
      }
    } while (need_retry);
  }
  return ret;
}

void ObResourceConstraintIterator::reset()
{
  is_ready_ = false;
  res_type_ = 0;
  curr_constraint_type_ = 0;
  res_.reset();
}

int ObResourceConstraintIterator::set_ready(const int64_t res_type)
{
  int ret = OB_SUCCESS;

  if (is_ready_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ObResourceConstraintIterator is already ready", K(ret));
  } else if (!is_valid_logic_res_type(res_type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(res_type));
  } else if (OB_FAIL(MTL(ObResourceLimitCalculator *)->get_logic_resource_constraint_value(res_type,
                                                                                           res_))) {
    LOG_WARN("get resource constraint value failed", K(ret), K(res_type));
  } else {
    res_type_ = res_type;
    is_ready_ = true;
  }
  return ret;
}

int ObResourceConstraintIterator::get_next(int64_t &val)
{
  int ret = OB_SUCCESS;
  if (!is_ready()) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObResourceConstraintIterator is not ready", K(ret));
  } else {
    bool need_retry = false;
    do {
      need_retry = false;
      curr_constraint_type_++;
      if (curr_constraint_type_ >= MAX_CONSTRAINT) {
        ret = OB_ITER_END;
      } else if (!is_valid_res_constraint_type(curr_constraint_type_)) {
        need_retry = true;
      } else if (OB_FAIL(res_.get_type_value(curr_constraint_type_, val))) {
        LOG_WARN("get type value failed", K(ret), K(curr_constraint_type_), K(val));
      }
    } while (need_retry);
  }
  return ret;
}

int ObUserResourceCalculateArg::set_type_value(const int64_t type, const int64_t value)
{
  int ret = OB_SUCCESS;
  if (!is_valid_logic_res_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else {
    int64_t count = needed_num_.count();
    while (OB_SUCC(ret) && count < MAX_LOGIC_RESOURCE) {
      if (OB_FAIL(needed_num_.push_back(0))) {
        LOG_WARN("set type value failed", K(ret));
      } else {
        count = needed_num_.count();
      }
    }
    if (OB_SUCC(ret)) {
      needed_num_[type] = value;
    }
  }
  return ret;
}

int ObUserResourceCalculateArg::get_type_value(const int64_t type, int64_t &value) const
{
  int ret = OB_SUCCESS;
  if (!is_valid_logic_res_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type));
  } else if (type < needed_num_.count()) {
    value = needed_num_[type];
  } else {
    value = 0;
  }
  return ret;
}

OB_SERIALIZE_MEMBER(ObUserResourceCalculateArg, needed_num_);

int ObUserResourceCalculateArg::assign(const ObUserResourceCalculateArg &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    if (OB_FAIL(needed_num_.assign(other.needed_num_))) {
      LOG_WARN("failed to needed num", KR(ret), K(other));
    }
  }
  return ret;
}

DEF_TO_STRING(ObUserResourceCalculateArg)
{
  int64_t pos = 0;
  J_OBJ_START();
  for (int64_t i = LOGIC_RESOURCE_LS; i < needed_num_.count(); i++) {
    J_KV(get_logic_res_type_name(i), needed_num_[i]);
    J_COMMA();
  }
  J_OBJ_END();
  return pos;
}

int ObResourceLimitCalculator::mtl_init(ObResourceLimitCalculator *&calculator)
{
  return calculator->init();
}

int ObResourceLimitCalculator::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("resource limit calculator already initialized", K(ret));
  } else {
    WLockGuard guard(lock_);
#define DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler)      \
    if (OB_SUCC(ret)) {                                               \
        handlers_[n] = subhandler;                                    \
    }
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_LIMIT_CALCULATOR
    is_inited_ = true;
  }
  return ret;
}

int ObResourceLimitCalculator::get_logic_resource_stat(
    const int64_t type,
    ObResourceInfo &val)
{
  int ret = OB_SUCCESS;
  ObIResourceLimitCalculatorHandler *handler = NULL;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("resource limit calculator not running", K(ret));
  } else if (!is_valid_logic_res_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource type", K(ret), K(type));
  } else if (OB_ISNULL(handler = handlers_[type])) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("the tenant may destroyed", K(ret), KP(handler));
  } else if (OB_FAIL(handler->get_current_info(val))) {
    LOG_WARN("get resource stat failed", K(ret), K(type));
  }
  return ret;
}

int ObResourceLimitCalculator::get_logic_resource_constraint_value(
    const int64_t type,
    ObResoureConstraintValue &val)
{
  int ret = OB_SUCCESS;
  ObIResourceLimitCalculatorHandler *handler = NULL;
  RLockGuard guard(lock_);
  if (IS_NOT_INIT) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("resource limit calculator not running", K(ret));
  } else if (!is_valid_logic_res_type(type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resource type", K(ret), K(type));
  } else if (OB_ISNULL(handler = handlers_[type])) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("the tenant may destroyed", K(ret), KP(handler));
  } else if (OB_FAIL(handler->get_resource_constraint_value(val))) {
    LOG_WARN("get resource stat failed", K(ret), K(type));
  }
  return ret;
}

int ObResourceLimitCalculator::get_tenant_min_phy_resource_value(
    ObMinPhyResourceResult &res)
{
  int ret = OB_SUCCESS;
  ObIResourceLimitCalculatorHandler *handler = NULL;
  ObMinPhyResourceResult min_res;
  ObMinPhyResourceResult tmp;
  if (IS_NOT_INIT) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("resource limit calculator not running", K(ret));
  } else {
    RLockGuard guard(lock_);
#define DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler)           \
    if (OB_SUCC(ret)) {                                                    \
      if (OB_ISNULL(handler = handlers_[n])) {                             \
        ret = OB_NOT_RUNNING;                                              \
        LOG_WARN("the tenant may destroyed", K(ret), K(n), KP(handler));   \
      } else if (OB_FAIL(handler->cal_min_phy_resource_needed(tmp))) {     \
        LOG_WARN("get resource stat failed", K(ret), K(n), K(#name));      \
      } else if (OB_FAIL(min_res.inc_update(tmp))) {                       \
        LOG_WARN("inc_update failed", K(min_res), K(tmp));                 \
      } else {                                                             \
        tmp.reset();                                                       \
      }                                                                    \
    }
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_LIMIT_CALCULATOR

    if (OB_SUCC(ret)) {
      res = min_res;
      ret = res.get_copy_assign_ret();
    }
  }
  return ret;
}

int ObResourceLimitCalculator::get_tenant_min_phy_resource_value(
    const ObUserResourceCalculateArg &arg,
    ObMinPhyResourceResult &res)
{
  int ret = OB_SUCCESS;
  ObIResourceLimitCalculatorHandler *handler = NULL;
  ObMinPhyResourceResult min_res;
  ObMinPhyResourceResult tmp;
  int64_t res_type = 0;
  int64_t need_num = 0;
  if (IS_NOT_INIT) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("resource limit calculator not running", K(ret));
  } else {
    RLockGuard guard(lock_);
#define DEF_RESOURCE_LIMIT_CALCULATOR(n, type, name, subhandler)              \
    if (OB_SUCC(ret)) {                                                       \
      if (OB_ISNULL(handler = handlers_[n])) {                                \
        ret = OB_NOT_RUNNING;                                                 \
        LOG_WARN("the tenant may destroyed", K(ret), K(n), KP(handler));      \
      } else if (OB_FAIL(arg.get_type_value(n, need_num))) {                  \
        LOG_WARN("get needed num failed", K(ret), K(n));                      \
      } else if (OB_FAIL(handler->cal_min_phy_resource_needed(need_num,       \
                                                              tmp))) {        \
        LOG_WARN("get resource stat failed", K(ret), K(n), K(need_num));      \
      } else if (OB_FAIL(min_res.inc_update(tmp))) {                          \
        LOG_WARN("inc_update failed", K(ret), K(min_res), K(tmp));            \
      } else {                                                                \
        tmp.reset();                                                          \
      }                                                                       \
    }
#include "share/resource_limit_calculator/ob_resource_limit_calculator_def.h"
#undef DEF_RESOURCE_LIMIT_CALCULATOR

    if (OB_SUCC(ret)) {
      res = min_res;
      ret = res.get_copy_assign_ret();
    }
  }
  return ret;
}

int ObResourceLimitCalculator::get_tenant_logical_resource(ObUserResourceCalculateArg &arg)
{
  int ret = OB_SUCCESS;
  arg.reset();
  ObLogicResourceStatIterator iter;
  if (IS_NOT_INIT) {
    ret = OB_NOT_RUNNING;
    LOG_WARN("resource limit calculator not running", KR(ret));
  } else if (OB_FAIL(iter.set_ready())) {
    LOG_WARN("failed to set ready", KR(ret));
  } else {
    ObResourceInfo info;
    while(OB_SUCC(ret)) {
      if (OB_FAIL(iter.get_next(info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next", KR(ret));
        }
      } else if (OB_FAIL(arg.set_type_value(iter.get_curr_type(), info.curr_utilization_))) {
        LOG_WARN("failed to set type value", KR(ret), K(info));
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

} // end namespace share
} // end namespace oceanbase
