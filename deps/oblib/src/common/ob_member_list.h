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

#ifndef OCEANBASE_COMMON_OB_MEMBER_LIST_H_
#define OCEANBASE_COMMON_OB_MEMBER_LIST_H_

#include "lib/ob_define.h"
#include "lib/utility/ob_unify_serialize.h"
#include "common/ob_member.h"

namespace oceanbase
{
namespace common
{
template <int64_t MAX_MEMBER_NUM>
class ObMemberListBase
{
  OB_UNIS_VERSION(1);

public:
  ObMemberListBase();
  ~ObMemberListBase();

public:
  void reset();
  bool is_valid() const;
  int add_member(const common::ObMember &member);
  int add_server(const common::ObAddr &server);
  // This function will determine the timestamp corresponding to the server when remove_member
  int remove_member(const common::ObMember &member);
  // This function will not determine the timestamp corresponding to the server when removing_server
  int remove_server(const common::ObAddr &server);
  int64_t get_member_number() const;
  uint64_t hash() const;
  int get_server_by_index(const int64_t index, common::ObAddr &server) const;
  int get_member_by_index(const int64_t index, common::ObMember &server_ex) const;
  int get_member_by_addr(const common::ObAddr &server, common::ObMember &member) const;
  int get_addr_array(ObIArray<common::ObAddr> &addr_array) const;
  bool contains(const common::ObAddr &server) const;
  bool contains(const common::ObMember &member) const;
  int deep_copy(const ObMemberListBase<MAX_MEMBER_NUM> &member_list);
  ObMemberListBase &operator=(const ObMemberListBase<MAX_MEMBER_NUM> &member_list);
  bool member_addr_equal(const ObMemberListBase<MAX_MEMBER_NUM> &member_list) const;
  int truncate(const int64_t count);  // just save [0, count)

  int64_t to_string(char *buf, const int64_t buf_len) const;
  TO_YSON_KV(OB_ID(member), common::ObArrayWrap<common::ObMember>(member_, member_number_));
private:
  int64_t member_number_;
  common::ObMember member_[MAX_MEMBER_NUM];
};

inline bool is_valid_replica_num(const int64_t replica_num)
{
  return replica_num > 0 && replica_num <= OB_MAX_MEMBER_NUMBER;
}

typedef ObMemberListBase<OB_MAX_MEMBER_NUMBER> ObMemberList;
typedef ObSEArray<ObReplicaMember, OB_MAX_CHILD_MEMBER_NUMBER> ObChildReplicaList;

inline int member_list_to_string(const common::ObMemberList &member_list, ObSqlString &member_list_buf)
{
  int ret = OB_SUCCESS;
  member_list_buf.reset();
  if (0 > member_list.get_member_number()) {
    ret = OB_INVALID_ARGUMENT;
    COMMON_LOG(WARN, "invalid argument", K(ret), "member count", member_list.get_member_number());
  } else {
    bool need_comma = false;
    char ip_port[MAX_IP_PORT_LENGTH];
    for (int64_t i = 0; OB_SUCC(ret) && i < member_list.get_member_number(); i++) {
      ObMember member;
      member_list.get_member_by_index(i, member);
      if (OB_FAIL(member.get_server().ip_port_to_string(ip_port, sizeof(ip_port)))) {
        COMMON_LOG(WARN, "convert server to string failed", K(ret), K(member));
      } else if (need_comma && OB_FAIL(member_list_buf.append(","))) {
        COMMON_LOG(WARN, "failed to append comma to string", K(ret));
      } else if (OB_FAIL(member_list_buf.append_fmt("%.*s:%ld", static_cast<int>(sizeof(ip_port)), ip_port, member.get_timestamp()))) {
        COMMON_LOG(WARN, "failed to append ip_port to string", K(ret), K(member));
      } else {
        need_comma = true;
      }
    }
    COMMON_LOG(INFO, "member_list_to_string success", K(member_list), K(member_list_buf));
  }
  return ret;
}

} // namespace common
} // namespace oceanbase

#include "common/ob_member_list.ipp"
#endif // OCEANBASE_COMMON_OB_MEMBER_LIST_H_
