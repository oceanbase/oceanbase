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

#ifndef OCEANBASE_SHARE_OB_CASCAD_MEMBER_LIST_H_
#define OCEANBASE_SHARE_OB_CASCAD_MEMBER_LIST_H_

#include "common/ob_member_list.h"
#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "ob_cascad_member.h"

namespace oceanbase
{
namespace share
{
class ObCascadMemberList
{
  OB_UNIS_VERSION(1);
public:
  ObCascadMemberList();
  virtual ~ObCascadMemberList();
public:
  void reset();
  bool is_valid() const;
  int add_member(const ObCascadMember &member);
  int remove_member(const ObCascadMember &member);
  int remove_server(const common::ObAddr &server);
  int64_t get_member_number() const;
  int get_server_by_index(const int64_t index, common::ObAddr &server) const;
  int get_member_by_index(const int64_t index, ObCascadMember &member) const;
  int get_member_by_addr(const common::ObAddr &server, ObCascadMember &member) const;
  bool contains(const common::ObAddr &server) const;
  bool contains(const ObCascadMember &member) const;
  ObCascadMemberList &operator=(const ObCascadMemberList &member_list);
  bool member_addr_equal(const ObCascadMemberList &member_list) const;
  int deep_copy(const common::ObMemberList &member_list, const int64_t dst_cluster_id);
  int deep_copy(const ObCascadMemberList &member_list);
  TO_STRING_KV(K(member_array_));
private:
  typedef common::ObSEArray<ObCascadMember, 1> ObCascadMemberArray;
  ObCascadMemberArray member_array_;
};

} // namespace share
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_CASCAD_MEMBER_LIST_H_
