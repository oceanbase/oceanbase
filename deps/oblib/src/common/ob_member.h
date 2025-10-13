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

#ifndef OCEANBASE_COMMON_OB_MEMBER_H_
#define OCEANBASE_COMMON_OB_MEMBER_H_

#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"
#include "lib/string/ob_sql_string.h"
#include "common/ob_region.h"
#include "lib/json/ob_yson.h"

namespace oceanbase
{
namespace common
{
class ObMember
{
public:
  ObMember();
  ObMember(const common::ObAddr &server,
           const int64_t timestamp,
           const int64_t flag = 0);
  virtual ~ObMember() = default;
public:
  const common::ObAddr &get_server() const;
  int64_t get_timestamp() const;
  int64_t get_flag() const;
  void set_flag(const int64_t &flag) { flag_ = flag; }
  virtual void reset();
  virtual bool is_valid() const;

  friend bool operator==(const ObMember &lhs, const ObMember &rhs);
  friend bool operator<(const ObMember &lhs, const ObMember &rhs);
  ObMember &operator=(const ObMember &rhs);
  int assign(const ObMember &other);

  bool is_migrating() const;
  void set_migrating();
  void reset_migrating();

  bool is_logonly() const;
  void set_logonly();
  void reset_logonly();

  TO_STRING_KV(K_(server), K_(timestamp), K_(flag));
  TO_YSON_KV(OB_Y_(server), OB_ID(t), timestamp_, OB_Y_(flag));
  OB_UNIS_VERSION(1);
protected:
  static const int64_t MIGRATING_FLAG_BIT = 1;
  static const int64_t LOGONLY_FLAG_BIT = 2;
  common::ObAddr server_;
  int64_t timestamp_;
  int64_t flag_;
};
static const int64_t OB_DEFAULT_MEMBER_COUNT = 16;
typedef ObSEArray<ObMember, OB_DEFAULT_MEMBER_COUNT> ObMemberArray;
static const int64_t OB_DEFAULT_RETURN_COUNT = 16;
typedef ObSEArray<int, OB_DEFAULT_RETURN_COUNT> ObReturnArray;

inline bool operator==(const ObMember &lhs, const ObMember &rhs)
{
  return (lhs.server_ == rhs.server_) && (lhs.timestamp_ == rhs.timestamp_)
         && (lhs.flag_ == rhs.flag_);
}

inline bool operator<(const ObMember &lhs, const ObMember &rhs)
{
  return lhs.server_ < rhs.server_;
}

inline int member_to_string(const common::ObMember &member, ObSqlString &member_buf)
{
  int ret = OB_SUCCESS;
  member_buf.reset();
  char ip_port[MAX_IP_PORT_LENGTH];
  if (OB_FAIL(member.get_server().ip_port_to_string(ip_port, sizeof(ip_port)))) {
    COMMON_LOG(WARN, "convert server to string failed", K(ret), K(member));
  } else if (OB_FAIL(member_buf.append_fmt("%.*s:%ld", static_cast<int>(sizeof(ip_port)), ip_port, member.get_timestamp()))) {
    COMMON_LOG(WARN, "failed to append ip_port to string", K(ret), K(member));
  }
  return ret;
}

class ObReplicaMember : public ObMember
{
public:
  // default constructor
  ObReplicaMember()
    : ObMember(),
      replica_type_(REPLICA_TYPE_FULL),
      memstore_percent_(100)
  {}
  // construct with only server and timestamp, when we don't know or care about replica_type
  ObReplicaMember(const common::ObAddr &server,
                  const int64_t timestamp)
    : ObMember(ObMember(server, timestamp)),
      replica_type_(REPLICA_TYPE_FULL),
      memstore_percent_(100)
  {}
  // construct with server, timestamp and replica_type,
  //   this func will set logonly flag if replica_type is L.
  ObReplicaMember(const common::ObAddr &server,
                  const int64_t timestamp,
                  const common::ObReplicaType replica_type,
                  const int64_t memstore_percent = 100,
                  const int64_t flag = 0)
    : ObMember(ObMember(server, timestamp, flag)),
      replica_type_(replica_type),
      memstore_percent_(memstore_percent)
  {
    if (REPLICA_TYPE_LOGONLY == replica_type) {
      ObMember::set_logonly();
    }
  }
public:
  // init with server, timestamp, replica_type.
  // this func will set logonly flag if replica_type is L.
  int init(const common::ObAddr &server,
           const int64_t timestamp,
           const common::ObReplicaType replica_type);
  // init with existing member whose flag_ may have been set.
  // this function will check whether flag_ is consistent with replica_type.
  int init(const ObMember &member,
           const common::ObReplicaType replica_type);
  common::ObReplicaType get_replica_type() const;
  const common::ObRegion &get_region() const;
  int64_t get_memstore_percent() const { return memstore_percent_; }
  virtual void reset();
  virtual bool is_valid() const;
  virtual bool is_readonly_replica() const;
  ObReplicaMember &operator=(const ObReplicaMember &rhs);

  TO_STRING_KV(K_(server), K_(timestamp), K_(flag), K_(replica_type), K_(region), K_(memstore_percent));
  OB_UNIS_VERSION(1);
private:
  common::ObReplicaType replica_type_;
  int64_t memstore_percent_;                       // obsolate, only as placeholder
  common::ObRegion region_ = DEFAULT_REGION_NAME;  // obsolate, only as placeholder
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_MEMBER_H_
