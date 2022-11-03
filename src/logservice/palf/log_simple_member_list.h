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

#ifndef OCEANBASE_LOGSERVICE_LOG_SIMPLE_MEMBER_LIST_H_
#define OCEANBASE_LOGSERVICE_LOG_SIMPLE_MEMBER_LIST_H_

#include "common/ob_member_list.h"
#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "share/ob_define.h"

namespace oceanbase
{
namespace palf
{
class LogSimpleMemberList final
{
public:
  LogSimpleMemberList() { reset(); }
public:
  int add_server(const common::ObAddr &server);
  bool contains(const common::ObAddr &server) const;
  int64_t get_count() const { return ATOMIC_LOAD(&count_); }
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
  int deep_copy(const common::ObMemberList &member_list);
  int deep_copy_to(common::ObMemberList &member_list);
  int remove_server(const common::ObAddr &server);
private:
  int8_t count_;
  common::ObAddr server_[common::OB_MAX_MEMBER_NUMBER];
};

typedef common::ObSEArray<LogSimpleMemberList, 16> LogSimpleMemberListArray;

class LogAckList
{
public:
  LogAckList() { reset(); }
  ~LogAckList() {}
public:
  int add_server(const common::ObAddr &server);
  int64_t get_count() const;
  void reset();
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  const static int64_t ACK_LIST_SERVER_NUM = common::OB_MAX_MEMBER_NUMBER / 2;
  common::ObAddr server_[ACK_LIST_SERVER_NUM];
};
} // namespace palf
} // namespace oceanbase

#endif // OCEANBASE_CLOG_OB_SIMPLE_MEMBER_LIST_H_
