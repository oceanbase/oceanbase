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

#ifndef LOGSERVICE_PALF_ELECTION_INTERFACE_OB_I_ELECTION_PRIORITY_H
#define LOGSERVICE_PALF_ELECTION_INTERFACE_OB_I_ELECTION_PRIORITY_H

#include "lib/ob_define.h"
#include "lib/guard/ob_unique_guard.h"
#include "lib/string/ob_string_holder.h"

namespace oceanbase
{
namespace palf
{
namespace election
{

class ElectionPriority
{
public:
  virtual ~ElectionPriority() {}
  // 在日志中打印priority的能力
  virtual int64_t to_string(char *buf, const int64_t buf_len) const = 0;
  // 优先级需要序列化能力，以便通过消息传递给其他副本
  virtual int serialize(char* buf, const int64_t buf_len, int64_t& pos) const = 0;
  virtual int deserialize(const char* buf, const int64_t data_len, int64_t& pos) = 0;
  virtual int64_t get_serialize_size(void) const = 0;
  // 主动刷新选举优先级的方法
  virtual int refresh() = 0;
  // 在priority间进行比较的方法
  virtual int compare_with(const ElectionPriority &rhs,
                           const uint64_t compare_version,
                           const bool decentralized_voting,
                           int &result,
                           common::ObStringHolder &reason) const = 0;
  virtual int get_size_of_impl_type() const = 0;
  virtual void placement_new_impl(void *ptr) const = 0;
  // 跳过RCS直接切主
  virtual bool has_fatal_failure() const = 0;
};

}
}
}
#endif