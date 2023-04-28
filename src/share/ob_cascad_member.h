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

#ifndef OCEANBASE_SHARE_OB_CASCAD_MEMBER_H_
#define OCEANBASE_SHARE_OB_CASCAD_MEMBER_H_

#include "lib/container/ob_se_array.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"
#include "lib/json/ob_yson.h"
#include "common/ob_region.h"

namespace oceanbase
{
namespace share
{
class ObCascadMember
{
  OB_UNIS_VERSION(1);
public:
  ObCascadMember();
  virtual ~ObCascadMember() = default;

  explicit ObCascadMember(const common::ObAddr &server,
                          const int64_t cluster_id);
public:
  const common::ObAddr get_server() const { return server_; }
  int set_server(const common::ObAddr &server);
  int64_t get_cluster_id() const { return ATOMIC_LOAD(&cluster_id_); }
  void set_cluster_id(const int64_t cluster_id) { ATOMIC_STORE(&cluster_id_, cluster_id); }
  virtual void reset();
  virtual bool is_valid() const;
  virtual int64_t hash() const;
  virtual int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  friend bool operator==(const ObCascadMember &lhs, const ObCascadMember &rhs);
  friend bool operator<(const ObCascadMember &lhs, const ObCascadMember &rhs);
  ObCascadMember &operator=(const ObCascadMember &rhs);

  TO_STRING_KV(K_(server), K_(cluster_id));
  TO_YSON_KV( OB_Y_(server), OB_ID(cluster_id), cluster_id_);
protected:
  common::ObAddr server_;
  int64_t cluster_id_;
};

inline bool operator==(const ObCascadMember &lhs, const ObCascadMember &rhs)
{
  return (lhs.server_ == rhs.server_) && (lhs.cluster_id_ == rhs.cluster_id_);
}

inline bool operator<(const ObCascadMember &lhs, const ObCascadMember &rhs)
{
  return lhs.server_ < rhs.server_;
}

} // namespace common
} // namespace oceanbase

#endif // OCEANBASE_SHARE_OB_CASCAD_MEMBER_H_
