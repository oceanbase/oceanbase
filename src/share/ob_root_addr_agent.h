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

#ifndef OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_
#define OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_

#include "lib/net/ob_addr.h"
#include "lib/utility/utility.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/container/ob_iarray.h"
#include "share/ob_cluster_role.h"                    // ObClusterRole
#include "common/ob_role.h" //ObRole
#include "share/ob_errno.h"

namespace oceanbase
{
namespace common
{
class ObServerConfig;
class ObMySQLProxy;
}
namespace share
{
struct ObRootAddr
{
  OB_UNIS_VERSION(1);
public: 
  ObRootAddr() : server_(), role_(FOLLOWER), sql_port_(OB_INVALID_INDEX) {}
  virtual ~ObRootAddr() {}
  inline void reset();
  inline bool is_valid() const;
  inline bool operator==(const ObRootAddr &other) const
  {
    return server_ == other.server_
           && role_ == other.role_
           && sql_port_ == other.sql_port_;
  }
  inline const common::ObAddr &get_server() const { return server_; }
  inline const common::ObRole &get_role() const { return role_; }
  inline int64_t get_sql_port() const { return sql_port_; }
  inline int assign(const ObRootAddr &other);
  inline int init(
      const common::ObAddr &server,
      const common::ObRole &role,
      const int64_t &sql_port);
  TO_STRING_KV(
      K_(server),
      K_(role),
      K_(sql_port))

private:
  common::ObAddr server_;
  common::ObRole role_;
  int64_t sql_port_;
};

inline void ObRootAddr::reset()
{
  server_.reset();
  role_ = FOLLOWER;
  sql_port_ = OB_INVALID_INDEX;
}

inline bool ObRootAddr::is_valid() const
{
  return server_.is_valid()
         && OB_INVALID_INDEX != sql_port_;
}

inline int ObRootAddr::assign(const ObRootAddr &other)
{
  int ret = OB_SUCCESS;
  if (this != &other) {
    server_ = other.server_;
    role_ = other.role_;
    sql_port_ = other.sql_port_;
  }
  return ret;
}

inline int ObRootAddr::init(
    const common::ObAddr &server,
    const common::ObRole &role,
    const int64_t &sql_port)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_UNLIKELY(!server.is_valid()
      || OB_INVALID_INDEX == sql_port)) {
    ret = OB_INVALID_ARGUMENT;
    SHARE_LOG(WARN, "ObRootAddr init failed", KR(ret),
        K(server), K(role), K(sql_port));
  } else {
    server_ = server;
    role_ = role;
    sql_port_ = sql_port;
  }
  return ret;
}
typedef common::ObSArray<ObRootAddr> ObAddrList;
typedef common::ObIArray<ObRootAddr> ObIAddrList;

// store and fetch root server address list interface.
class ObRootAddrAgent
{
public:
  ObRootAddrAgent() : inited_(false), config_(NULL) {}
  virtual ~ObRootAddrAgent() {}

  virtual int init(common::ObServerConfig &config);
  virtual bool is_valid();

  virtual int store(const ObIAddrList &addr_list, const ObIAddrList &readonly_addr_list,
                    const bool force, const common::ObClusterRole cluster_role,
                    const int64_t timestamp) = 0;
  virtual int fetch(ObIAddrList &add_list,
                    ObIAddrList &readonly_addr_list) = 0;
protected:
  virtual int check_inner_stat() const;
protected:
  bool inited_;
  common::ObServerConfig *config_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObRootAddrAgent);
};

inline int ObRootAddrAgent::init(common::ObServerConfig &config)
{
  int ret = common::OB_SUCCESS;
  if (inited_) {
    ret = common::OB_INIT_TWICE;
    SHARE_LOG(WARN, "init twice", KR(ret));
  } else {
    config_ = &config;
    inited_ = true;
  }
  return ret;
};

inline int ObRootAddrAgent::check_inner_stat() const
{
  int ret = common::OB_SUCCESS;
  if (!inited_) {
    ret = common::OB_NOT_INIT;
    SHARE_LOG(WARN, "not init", KR(ret));
  } else if (OB_ISNULL(config_)) {
    ret = OB_ERR_UNEXPECTED;
    SHARE_LOG(WARN, "config_ is null", KR(ret));
  }
  return ret;
}

inline bool ObRootAddrAgent::is_valid()
{
  return inited_;
}

} // end namespace share
} // end oceanbase

#endif // OCEANBASE_SHARE_OB_ROOT_ADDR_AGENT_H_
