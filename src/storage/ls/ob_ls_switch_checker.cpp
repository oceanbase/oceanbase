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

#include "ob_ls_switch_checker.h"
#include "ob_ls.h"

namespace oceanbase
{
namespace storage
{

int ObLSSwitchChecker::check_online(ObLS *ls)
{
  int ret = OB_SUCCESS;
  ls_ = ls;
  if (OB_ISNULL(ls)) {
    ret = OB_BAD_NULL_ERROR;
  } else {
    record_switch_epoch_ = ATOMIC_LOAD(&(ls_->switch_epoch_));
    if (!(record_switch_epoch_ & 1)) {
      ret = OB_LS_OFFLINE;
    }
  }
  return ret;
}

int ObLSSwitchChecker::check_ls_switch_state(ObLS *ls, bool &online_state)
{
  int ret = OB_SUCCESS;
  ls_ = ls;
  if (OB_ISNULL(ls)) {
    ret = OB_BAD_NULL_ERROR;
  } else {
    record_switch_epoch_ = ATOMIC_LOAD(&(ls_->switch_epoch_));
    if (!(record_switch_epoch_ & 1)) {
      online_state = false;
    } else {
      online_state = true;
    }
  }
  return ret;
}

int ObLSSwitchChecker::check_ls_switch_state_with_legacy_epoch(
    ObLS *ls,
    bool &online_state)
{
  int ret = OB_SUCCESS;
  is_legacy_online_ = false;
  if (OB_FAIL(check_ls_switch_state(ls, online_state))) {
  } else if (!online_state && 0 == record_switch_epoch_) {
    // An active LS may keep the initial epoch 0 in 4.2.
    ObLSLockGuard lock_guard(ls, true /* rdlock */);
    if (0 == ls->get_switch_epoch() && !ls->is_offline()) {
      online_state = true;
      is_legacy_online_ = true;
    }
  }
  return ret;
}

int ObLSSwitchChecker::double_check_epoch() const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(ls_)) {
    ret = OB_NOT_INIT;
  } else if (record_switch_epoch_ != ATOMIC_LOAD(&(ls_->switch_epoch_))) {
    ret = OB_VERSION_NOT_MATCH;
  }
  return ret;
}

int ObLSSwitchChecker::double_check_epoch_with_legacy_epoch() const
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(double_check_epoch())) {
  } else if (is_legacy_online_) {
    ObLSLockGuard lock_guard(ls_, true /* rdlock */);
    if (0 != ls_->get_switch_epoch() || ls_->is_offline()) {
      ret = OB_VERSION_NOT_MATCH;
    }
  }
  return ret;
}

}
}
