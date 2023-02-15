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

#include "common/ob_member_list.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace common
{
template <int64_t MAX_MEMBER_NUM>
ObMemberListBase<MAX_MEMBER_NUM>::ObMemberListBase()
    : member_number_(0)
{
}

template <int64_t MAX_MEMBER_NUM>
ObMemberListBase<MAX_MEMBER_NUM>::~ObMemberListBase()
{
}

template <int64_t MAX_MEMBER_NUM>
void ObMemberListBase<MAX_MEMBER_NUM>::reset()
{
  member_number_ = 0;
  for (int64_t i = 0; i < MAX_MEMBER_NUM; ++i) {
    member_[i].reset();
  }
}

template <int64_t MAX_MEMBER_NUM>
bool ObMemberListBase<MAX_MEMBER_NUM>::is_valid() const
{
  return member_number_ > 0;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::add_member(const ObMember &member)
{
  int ret = OB_SUCCESS;
  int64_t dst_idx = member_number_;
  if (OB_UNLIKELY(!member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (member_number_ >= MAX_MEMBER_NUM) {
    ret = OB_SIZE_OVERFLOW;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && i < member_number_; ++i) {
      if (member_[i].get_server() == member.get_server()) {
        ret = OB_ENTRY_EXIST;
      } else if (member_number_ == dst_idx
                 && member < member_[i]) {
        dst_idx = i;
      } else {
        // do nothing
      }
    }
    if (OB_SUCC(ret) && dst_idx < member_number_) {
      for (int64_t i = member_number_; OB_SUCC(ret) && i > dst_idx; --i) {
        member_[i] = member_[i-1];
      }
    }
  }
  if (OB_SUCC(ret)) {
    member_[dst_idx] = member;
    ++member_number_;
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::add_server(const ObAddr &server)
{
  return add_member(ObMember(server, OB_INVALID_TIMESTAMP));
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::remove_member(const ObMember &member)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (member_number_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && index == OB_INVALID_INDEX && i < member_number_; ++i) {
      if (member_[i] == member) {
        index = i;
      } else if (member_[i].get_server() == member.get_server()) {
        ret = OB_ITEM_NOT_MATCH;
        COMMON_LOG(WARN, "ObMemberListBase<MAX_MEMBER_NUM>::remove_member member not match", K(ret), "Member in list",
                   member_[i], "Argument member", member);
      }
    }
    if (OB_SUCCESS == ret && index == OB_INVALID_INDEX) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = index; i < member_number_ - 1; ++i) {
      member_[i] = member_[i + 1];
    }
    member_[member_number_ - 1].reset();
    --member_number_;
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::remove_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (member_number_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; index == OB_INVALID_INDEX && i < member_number_; ++i) {
      if (member_[i].get_server() == server) {
        index = i;
      }
    }
    if (index == OB_INVALID_INDEX) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = index; i < member_number_ - 1; ++i) {
      member_[i] = member_[i + 1];
    }
    member_[member_number_ - 1].reset();
    --member_number_;
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
int64_t ObMemberListBase<MAX_MEMBER_NUM>::get_member_number() const
{
  return member_number_;
}

template <int64_t MAX_MEMBER_NUM>
uint64_t ObMemberListBase<MAX_MEMBER_NUM>::hash() const
{
  uint64_t hash_val = 0;
  for (int i = 0; i < member_number_; ++i) {
    hash_val += member_[i].get_server().hash();
  }
  return hash_val;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::get_server_by_index(const int64_t index, common::ObAddr &server) const
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= member_number_) {
    ret = OB_SIZE_OVERFLOW;
  }
  if (OB_SUCC(ret)) {
    server = member_[index].get_server();
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::get_member_by_index(const int64_t index, common::ObMember &member) const
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= member_number_) {
    ret = OB_SIZE_OVERFLOW;
  }
  if (OB_SUCC(ret)) {
    member = member_[index];
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::get_member_by_addr(const common::ObAddr &server, common::ObMember &member) const
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  member.reset();
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (i = 0; i < member_number_; ++i) {
      if (member_[i].get_server() == server) {
        member = member_[i];
        break;
      }
    }
    if (member_number_ == i) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::get_addr_array(ObIArray<common::ObAddr> &addr_array) const
{
  int ret = OB_SUCCESS;
  addr_array.reset();
  for (int64_t i = 0; OB_SUCCESS == ret && i < member_number_; ++i) {
    if (OB_FAIL(addr_array.push_back(member_[i].get_server()))) {
      CLOG_LOG(WARN, "push back addr failed", K(ret));
    }
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
bool ObMemberListBase<MAX_MEMBER_NUM>::contains(const common::ObAddr &server) const
{
  int bool_ret = false;
  for (int64_t i = 0; i < member_number_; ++i) {
    if (member_[i].get_server() == server) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

template <int64_t MAX_MEMBER_NUM>
bool ObMemberListBase<MAX_MEMBER_NUM>::contains(const common::ObMember &member) const
{
  int bool_ret = false;
  for (int64_t i = 0; i < member_number_; ++i) {
    if (member_[i] == member) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::deep_copy(const ObMemberListBase<MAX_MEMBER_NUM> &member_list)
{
  int ret = OB_SUCCESS;
  reset();
  member_number_ = member_list.get_member_number();
  for (int64_t i = 0; i < member_number_; ++i) {
    // This function call is guaranteed not to fail
    member_list.get_member_by_index(i, member_[i]);
  }
  return ret;
}

template <int64_t MAX_MEMBER_NUM>
ObMemberListBase<MAX_MEMBER_NUM> &ObMemberListBase<MAX_MEMBER_NUM>::operator=(const ObMemberListBase<MAX_MEMBER_NUM> &member_list)
{
  if (this != &member_list) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != (tmp_ret = deep_copy(member_list))) {
      COMMON_LOG_RET(WARN, tmp_ret, "deep_copy failed", K(tmp_ret));
    }
  }
  return *this;
}

template <int64_t MAX_MEMBER_NUM>
bool ObMemberListBase<MAX_MEMBER_NUM>::member_addr_equal(const ObMemberListBase<MAX_MEMBER_NUM> &member_list) const
{
  bool bool_ret = true;
  if (member_number_ != member_list.get_member_number()) {
    bool_ret = false;
  } else {
    common::ObAddr addr;
    for (int64_t i = 0; true == bool_ret && i < member_number_; ++i) {
      if (!member_list.contains(member_[i].get_server())) {
        bool_ret = false;
      } else {}
    }
  }
  return bool_ret;
}

template <int64_t MAX_MEMBER_NUM>
int64_t ObMemberListBase<MAX_MEMBER_NUM>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  int tmp_ret = OB_SUCCESS;
  databuff_printf(buf, buf_len , pos, "%ld", member_number_);
  for (int64_t i = 0; i < member_number_; ++i) {
    if (OB_SUCCESS != (tmp_ret = databuff_print_obj(buf, buf_len, pos, member_[i]))) {
      COMMON_LOG_RET(WARN, tmp_ret, "databuff_print_objfailed", K(tmp_ret));
    }
  }
  return pos;
}

template <int64_t MAX_MEMBER_NUM>
int ObMemberListBase<MAX_MEMBER_NUM>::truncate(const int64_t count)
{
  int ret = OB_SUCCESS;

  if (count <= 0) {
    ret = OB_INVALID_ARGUMENT;
  } else if (member_number_ <= count) {
    // no need truncate
  } else {
    for (int64_t i = count; i < member_number_; ++i) {
      member_[i].reset();
    }
    member_number_ = count;
  }

  return ret;
}

// New serialize macro. It doesn't support array serialization now.
// OB_MAX_MEMBER_NUMBER = 7. It is ugly but it works.

OB_SERIALIZE_MEMBER_TEMP(template <int64_t MAX_MEMBER_NUM>, ObMemberListBase<MAX_MEMBER_NUM>, member_number_,
                    member_[0], member_[1], member_[2], member_[3], member_[4],
                    member_[5], member_[6]);

} // namespace common
} // namespace oceanbase
