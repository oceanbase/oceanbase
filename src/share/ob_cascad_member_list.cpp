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

#include "lib/ob_define.h"
#include "lib/utility/ob_print_utils.h"
#include "ob_cascad_member_list.h"

namespace oceanbase
{
using namespace common;

namespace share
{
ObCascadMemberList::ObCascadMemberList()
    : member_array_()
{}

ObCascadMemberList::~ObCascadMemberList()
{}

void ObCascadMemberList::reset()
{
  member_array_.reset();
}

bool ObCascadMemberList::is_valid() const
{
  return member_array_.count() > 0;
}

int ObCascadMemberList::add_member(const ObCascadMember &member)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(!member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (member_array_.count() >= OB_MAX_CHILD_MEMBER_NUMBER) {
    ret = OB_SIZE_OVERFLOW;
  }

  if (OB_SUCC(ret)) {
    if (contains(member.get_server())) {
      ret = OB_ENTRY_EXIST;
    } else if (OB_FAIL(member_array_.push_back(member))) {
      COMMON_LOG(ERROR, "member_array_ push_back failed", K(ret), K(member));
    }
  }

  return ret;
}

int ObCascadMemberList::remove_member(const ObCascadMember &member)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!member.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (member_array_.count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; OB_SUCC(ret) && index == OB_INVALID_INDEX && i < member_array_.count(); ++i) {
      if (member_array_[i] == member) {
        index = i;
      } else if (member_array_[i].get_server() == member.get_server()) {
        ret = OB_ITEM_NOT_MATCH;
        COMMON_LOG(WARN, "ObCascadMemberList::remove_member member not match", K(ret), "Member in list",
                   member_array_[i], "Argument member", member);
      }
    }
    if (OB_SUCCESS == ret && index == OB_INVALID_INDEX) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(member_array_.remove(index))) {
      COMMON_LOG(ERROR, "member_array_ remove failed", K(ret));
    }
  }
  return ret;
}

int ObCascadMemberList::remove_server(const ObAddr &server)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (member_array_.count() <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; index == OB_INVALID_INDEX && i < member_array_.count(); ++i) {
      if (member_array_[i].get_server() == server) {
        index = i;
      }
    }
    if (index == OB_INVALID_INDEX) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  if (OB_SUCC(ret)) {
    if (OB_FAIL(member_array_.remove(index))) {
      COMMON_LOG(ERROR, "member_array_ remove failed", K(ret));
    }
  }
  return ret;
}

int64_t ObCascadMemberList::get_member_number() const
{
  return member_array_.count();
}

int ObCascadMemberList::get_server_by_index(const int64_t index, common::ObAddr &server) const
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= member_array_.count()) {
    ret = OB_SIZE_OVERFLOW;
  }
  if (OB_SUCC(ret)) {
    server = member_array_[index].get_server();
  }
  return ret;
}

int ObCascadMemberList::get_member_by_index(const int64_t index, ObCascadMember &member) const
{
  int ret = OB_SUCCESS;
  if (index < 0 || index >= member_array_.count()) {
    ret = OB_SIZE_OVERFLOW;
  }
  if (OB_SUCC(ret)) {
    member = member_array_[index];
  }
  return ret;
}

int ObCascadMemberList::get_member_by_addr(const common::ObAddr &server, ObCascadMember &member) const
{
  int ret = OB_SUCCESS;
  int64_t i = 0;
  member.reset();
  if (!server.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    for (i = 0; i < member_array_.count(); ++i) {
      if (member_array_[i].get_server() == server) {
        member = member_array_[i];
        break;
      }
    }
    if (member_array_.count() == i) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  return ret;
}

bool ObCascadMemberList::contains(const common::ObAddr &server) const
{
  int bool_ret = false;
  for (int64_t i = 0; i < member_array_.count(); ++i) {
    if (member_array_[i].get_server() == server) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

bool ObCascadMemberList::contains(const ObCascadMember &member) const
{
  int bool_ret = false;
  for (int64_t i = 0; i < member_array_.count(); ++i) {
    if (member_array_[i] == member) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

int ObCascadMemberList::deep_copy(const common::ObMemberList &member_list, const int64_t dst_cluster_id)
{
  int ret = OB_SUCCESS;
  reset();
  const int64_t member_number = member_list.get_member_number();
  for (int64_t i = 0; OB_SUCC(ret) && i < member_number; ++i) {
    common::ObAddr cur_server;
    if (OB_FAIL(member_list.get_server_by_index(i, cur_server))) {
      COMMON_LOG(WARN, "get_server_by_index failed", K(ret), K(i), K(member_list));
    } else if (OB_FAIL(add_member(ObCascadMember(cur_server, dst_cluster_id)))) {
      COMMON_LOG(WARN, "add_member failed", K(ret), K(i), K(cur_server));
    } else {
      // do nothing
    }
  }
  return ret;
}

int ObCascadMemberList::deep_copy(const ObCascadMemberList &member_list)
{
  int ret = OB_SUCCESS;
  reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
    ObCascadMember member;
    if (OB_FAIL(member_list.get_member_by_index(i, member))) {
      COMMON_LOG(WARN, "get_member_by_index failed", K(ret), K(i));
    } else if (OB_FAIL(add_member(member))) {
      COMMON_LOG(WARN, "add_member failed", K(ret));
    }
  }
  return ret;
}

ObCascadMemberList &ObCascadMemberList::operator=(const ObCascadMemberList &member_list)
{
  if (this != &member_list) {
    int tmp_ret = OB_SUCCESS;
    if (OB_SUCCESS != ( tmp_ret = deep_copy(member_list))) {
      COMMON_LOG_RET(ERROR, tmp_ret, "deep_copy failed", K(tmp_ret));
    }
  }
  return *this;
}

bool ObCascadMemberList::member_addr_equal(const ObCascadMemberList &member_list) const
{
  bool bool_ret = true;
  if (get_member_number() != member_list.get_member_number()) {
    bool_ret = false;
  } else {
    common::ObAddr addr;
    for (int64_t i = 0; true == bool_ret && i < get_member_number(); ++i) {
      if (!member_list.contains(member_array_[i].get_server())) {
        bool_ret = false;
      } else {}
    }
  }
  return bool_ret;
}

OB_DEF_SERIALIZE(ObCascadMemberList)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  if (NULL == buf || pos < 0 || pos > buf_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = serialization::encode_i64(buf, buf_len, new_pos, get_member_number()))) {
    COMMON_LOG(WARN, "encode member_number failed", K(ret), "member_number", get_member_number());
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < get_member_number(); ++i) {
      if (OB_FAIL(member_array_[i].serialize(buf, buf_len, new_pos))) {
        COMMON_LOG(WARN, "member serialize failed", K(ret), K(i), "member", member_array_[i]);
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_DESERIALIZE(ObCascadMemberList)
{
  int ret = OB_SUCCESS;
  int64_t new_pos = pos;
  int64_t member_number = 0;
  if (NULL == buf || pos < 0 || pos > data_len) {
    ret = OB_INVALID_ARGUMENT;
  } else if (OB_SUCCESS != (ret = serialization::decode_i64(buf, data_len, new_pos, &member_number))) {
    COMMON_LOG(WARN, "decode member_number failed", K(ret), K(member_number));
  } else {
    for (int64_t i = 0; OB_SUCC(ret) && i < member_number; ++i) {
      ObCascadMember member;
      if (OB_FAIL(member.deserialize(buf, data_len, new_pos))) {
        COMMON_LOG(WARN, "decode member failed", K(ret), K(member_number));
      } else if (OB_FAIL(add_member(member))) {
        COMMON_LOG(WARN, "add_member failed", K(ret));
      }
    }
  }
  if (OB_SUCC(ret)) {
    pos = new_pos;
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObCascadMemberList)
{
  int64_t serialize_size = serialization::encoded_length_i64(get_member_number());
  for (int64_t i = 0; i < get_member_number(); ++i) {
    serialize_size += member_array_[i].get_serialize_size();
  }
  return serialize_size;
}

} // namespace common
} // namespace oceanbase
