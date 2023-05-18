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

#include "share/ob_occam_time_guard.h"
#include "election_member_list.h"
#include "election_common_define.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

using namespace common;

MemberList::MemberList() : replica_num_(0)
{
  addr_list_.set_attr(ObMemAttr(OB_SERVER_TENANT_ID, "AddrList"));
}

bool MemberList::only_membership_version_different(const MemberList &rhs) const
{
  bool ret = true;
  if (*this == rhs) {
    ELECT_LOG(WARN, "even membership version is same", K(*this), K(rhs), KR(ret));
  } else if (replica_num_ != rhs.replica_num_) {
    ret = false;
  } else if (addr_list_.count() != rhs.addr_list_.count()) {
    ret = false;
  } else {
    for (int64_t idx = 0; idx < addr_list_.count(); ++idx) {
      if (addr_list_[idx] != rhs.addr_list_[idx]) {
        ret = false;
        break;
      }
    }
    if (ret) {
      if (membership_version_ == rhs.membership_version_) {
        ELECT_LOG(ERROR, "not found different element", K(*this), K(rhs), KR(ret));
      }
    }
  }
  return ret;
}

bool MemberList::operator==(const MemberList &rhs) const
{
  ELECT_TIME_GUARD(500_ms);
  bool ret = false;
  // 检查旧的成员组的信息是否一致
  int valid_member_list_count = 0;
  valid_member_list_count += rhs.is_valid() ? 1 : 0;
  valid_member_list_count += this->is_valid() ? 1 : 0;
  if (valid_member_list_count == 0) {// 两个都是无效的
    ret = true;
  } else if (valid_member_list_count == 2) {// 两个都是有效的
    if (membership_version_ == rhs.membership_version_ && replica_num_ == rhs.replica_num_) {
      // 成员版本号和副本数量相等
      if (addr_list_.count() == rhs.addr_list_.count()) {// 成员列表的数量一致
        ret = true;
        for (int64_t i = 0; i < addr_list_.count() && ret; ++i) {
          // 判断对于自己成员列表中的每一个成员是否都能在rhs中找到
          if (addr_list_[i] != rhs.addr_list_[i]) {// 要求成员列表的顺序和成员是一致的
            ret = false;
          }
        }
      } else {// 成员列表的数量不一致
        ret = false;
      }
    } else {// 成员版本号和副本数量不等
      ret = false;
    }
  } else {// 其中一个有效，一个无效
    ret = false;
  }
  return ret;
}

bool MemberList::operator!=(const MemberList &rhs) const { return !this->operator==(rhs); }

int MemberList::assign(const MemberList &rhs)
{
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(addr_list_.assign(rhs.get_addr_list()))) {
    ELECT_LOG(ERROR, "assign addrlist filed", KR(ret));
  } else {
    membership_version_ = rhs.membership_version_;
    replica_num_ = rhs.replica_num_;
  }
  return ret;
}

int MemberList::set_new_member_list(const common::ObArray<common::ObAddr> &addr_list,
                                    const LogConfigVersion membership_version,
                                    const int64_t replica_num)
{
  ELECT_TIME_GUARD(500_ms);
  int ret = OB_SUCCESS;
  if (CLICK_FAIL(addr_list_.assign(addr_list))) {
    ELECT_LOG(ERROR, "assign addrlist filed", KR(ret));
  } else {
    membership_version_ = membership_version;
    replica_num_ = replica_num;
  }
  return ret;
}

bool MemberList::is_valid() const
{
  bool ret = false;
  if (addr_list_.count() != 0 && membership_version_.is_valid() && replica_num_ > 0) {
    ret = true;
  }
  return ret;
}

LogConfigVersion MemberList::get_membership_version() const { return membership_version_; }

void MemberList::set_membership_version(const LogConfigVersion version) { membership_version_ = version; }

int64_t MemberList::get_replica_num() const { return replica_num_; }

const ObArray<ObAddr> &MemberList::get_addr_list() const { return addr_list_; }

}
}
}
