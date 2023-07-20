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
           const int64_t timestamp);
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

  TO_STRING_KV(K_(server), K_(timestamp), K_(flag));
  TO_YSON_KV(OB_Y_(server), OB_ID(t), timestamp_, OB_Y_(flag));
  OB_UNIS_VERSION(1);
protected:
  static const int64_t MIGRATING_FLAG_BIT = 1;
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
  ObReplicaMember()
    : ObMember(),
      replica_type_(REPLICA_TYPE_FULL),
      region_(DEFAULT_REGION_NAME),
      memstore_percent_(100)
  {}
  ObReplicaMember(const common::ObAddr &server,
                  const int64_t timestamp)
    : ObMember(server, timestamp),
      replica_type_(REPLICA_TYPE_FULL),
      region_(DEFAULT_REGION_NAME),
      memstore_percent_(100)
  {}
  ObReplicaMember(const ObMember &member)
    : ObMember(member),
      replica_type_(REPLICA_TYPE_FULL),
      region_(DEFAULT_REGION_NAME),
      memstore_percent_(100)
  {}
  /* After the subsequent type conversion code is completed, remove the constructor */
  ObReplicaMember(const common::ObAddr &server,
                  const int64_t timestamp,
                  const common::ObReplicaType replica_type)
    : ObMember(server, timestamp),
      replica_type_(replica_type),
      region_(DEFAULT_REGION_NAME),
      memstore_percent_(100)
  {}
  ObReplicaMember(const common::ObAddr &server,
                  const int64_t timestamp,
                  const common::ObReplicaType replica_type,
                  const int64_t memstore_percent)
    : ObMember(server, timestamp),
      replica_type_(replica_type),
      region_(DEFAULT_REGION_NAME),
      memstore_percent_(memstore_percent)
  {}
  ObReplicaMember(const common::ObAddr &server,
                  const int64_t timestamp,
                  const common::ObReplicaType replica_type,
                  const common::ObRegion &region,
                  const int64_t memstore_percent)
    : ObMember(server, timestamp),
      replica_type_(replica_type),
      region_(region),
      memstore_percent_(memstore_percent)
  {}
public:
  common::ObReplicaType get_replica_type() const;
  int set_replica_type(const common::ObReplicaType replica_type);
  const common::ObRegion &get_region() const;
  int set_region(const common::ObRegion &region);
  int set_member(const ObMember &member);
  int64_t get_memstore_percent() const { return memstore_percent_; }
  void set_memstore_percent(const int64_t memstore_percent) { memstore_percent_ = memstore_percent; }
  virtual void reset();
  virtual bool is_valid() const;
  virtual bool is_readonly_replica() const;
  ObReplicaMember &operator=(const ObReplicaMember &rhs);

  TO_STRING_KV(K_(server), K_(timestamp), K_(flag), K_(replica_type), K_(region), K_(memstore_percent));
  OB_UNIS_VERSION(1);
private:
  common::ObReplicaType replica_type_;
  common::ObRegion region_;
  int64_t memstore_percent_;
};
} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_COMMON_OB_MEMBER_H_
