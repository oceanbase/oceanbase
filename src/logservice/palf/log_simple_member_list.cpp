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

#include "log_simple_member_list.h"

namespace oceanbase
{
using namespace common;
namespace palf
{
int LogSimpleMemberList::add_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  bool found = false;
  for (int64_t i = 0; i < count_; i++) {
    if (server_[i] == server) {
      found = true;
    }
  }
  if (!found) {
    if (OB_UNLIKELY(count_ >= common::OB_MAX_MEMBER_NUMBER)) {
      ret = OB_ERROR_OUT_OF_RANGE;
    } else {
      server_[count_] = server;
      count_++;
    }
  }
  return ret;
}

bool LogSimpleMemberList::contains(const common::ObAddr &server) const
{
  bool bool_ret = false;
  for (int64_t i = 0; i < count_; i++) {
    if (server_[i] == server) {
      bool_ret = true;
      break;
    }
  }
  return bool_ret;
}

void LogSimpleMemberList::reset()
{
  count_ = 0;
  for (int64_t i = 0; i < OB_MAX_MEMBER_NUMBER; i++) {
    server_[i].reset();
  }
}

int64_t LogSimpleMemberList::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  databuff_printf(buf, buf_len , pos, "%ld", static_cast<int64_t>(count_));
  for (int64_t i = 0; i < count_; i++) {
    pos += server_[i].to_string(buf+pos, buf_len);
    databuff_printf(buf, buf_len, pos, "%s", ";");
  }
  return pos;
}

int LogSimpleMemberList::deep_copy(const common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  reset();
  ObAddr server;
  for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); ++i) {
    if (OB_FAIL(member_list.get_server_by_index(i, server))) {
      CLOG_LOG(ERROR, "get_server_by_index failed", K(ret));
    } else if (OB_FAIL(add_server(server))) {
      CLOG_LOG(ERROR, "add_server failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && get_count() != member_list.get_member_number()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "count not match", K(ret), "dst_count", get_count(), "src_count",
        member_list.get_member_number());
  }
  return ret;
}

int LogSimpleMemberList::deep_copy_to(common::ObMemberList &member_list)
{
  int ret = OB_SUCCESS;
  member_list.reset();
  for (int64_t i = 0; OB_SUCC(ret) && i < count_; ++i) {
    if (OB_FAIL(member_list.add_server(server_[i]))) {
      CLOG_LOG(ERROR, "add_server failed", K(ret));
    }
  }
  if (OB_SUCC(ret) && get_count() != member_list.get_member_number()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "count not match", K(ret), "src_count", get_count(), "dst_count",
        member_list.get_member_number());
  }
  return ret;
}

int LogSimpleMemberList::remove_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  int64_t index = OB_INVALID_INDEX;
  if (OB_UNLIKELY(!server.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
  } else if (count_ <= 0) {
    ret = OB_ENTRY_NOT_EXIST;
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = 0; index == OB_INVALID_INDEX && i < count_; ++i) {
      if (server_[i] == server) {
        index = i;
      }
    }
    if (index == OB_INVALID_INDEX) {
      ret = OB_ENTRY_NOT_EXIST;
    }
  }
  if (OB_SUCC(ret)) {
    for (int64_t i = index; i < count_ - 1; ++i) {
      server_[i] = server_[i + 1];
    }
    server_[count_ - 1].reset();
    --count_;
  }
  return ret;
}

int LogAckList::add_server(const common::ObAddr &server)
{
  int ret = OB_SUCCESS;
  bool found = false;
  const int64_t count = get_count();
  if (!server.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "add_server unexpected server");
  } else {
    for (int64_t i = 0; i < count; i++) {
      if (server_[i] == server) {
        found = true;
      }
    }
    if (!found) {
      if (count >= ACK_LIST_SERVER_NUM) {
        // do nothing
      } else {
        server_[count] = server;
      }
    }
    PALF_LOG(INFO, "add_server success", K(count), K(server), K(count));
  }
  return ret;
}

void LogAckList::reset()
{
  for (int64_t i = 0; i < ACK_LIST_SERVER_NUM; i++) {
    server_[i].reset();
  }
}

int64_t LogAckList::get_count() const
{
  int64_t count = 0;
  for (int64_t i = 0; i < ACK_LIST_SERVER_NUM; i++) {
    if (server_[i].is_valid()) {
      count++;
    }
  }
  return count;
}

int64_t LogAckList::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;
  const int64_t count = get_count();
  databuff_printf(buf, buf_len , pos, "%ld", count);
  for (int64_t i = 0; i < count; i++) {
    pos += server_[i].to_string(buf+pos, buf_len);
    databuff_printf(buf, buf_len, pos, "%s", ";");
  }
  return pos;
}
} // namespace palf
} // namespace oceanbase
