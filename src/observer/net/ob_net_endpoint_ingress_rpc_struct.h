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

#ifndef OCEANBASE_ENDPOINT_INGRESS_RPC_STRUCT_H_
#define OCEANBASE_ENDPOINT_INGRESS_RPC_STRUCT_H_

#include "lib/utility/ob_unify_serialize.h"
#include "lib/hash_func/murmur_hash.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/ob_define.h"
#include "share/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "share/ob_errno.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"

namespace oceanbase
{
using namespace common;
namespace obrpc
{

class ObNetEndpointKey
{
  OB_UNIS_VERSION(1);

public:
  ObNetEndpointKey() : addr_(), group_id_(0)
  {}
  ObNetEndpointKey(const ObAddr &addr) : addr_(addr), group_id_(0)
  {}
  ObNetEndpointKey(const ObNetEndpointKey &other)
  {
    (void)assign(other);
  }
  int assign(const ObNetEndpointKey &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      addr_ = other.addr_;
      group_id_ = other.group_id_;
    }
    return ret;
  }
  bool is_valid() const
  {
    return addr_.is_valid() && group_id_ != OB_INVALID_ID;
  }
  void reset()
  {
    addr_.reset();
    group_id_ = 0;
  }
  uint64_t hash() const
  {
    return addr_.hash();
  }
  int hash(uint64_t &hash_val) const
  {
    hash_val = hash();
    return common::OB_SUCCESS;
  }
  int compare(const ObNetEndpointKey &other) const
  {
    int compare_ret = 0;
    if (&other == this) {
      compare_ret = 0;
    } else if ((compare_ret = addr_.compare(other.addr_))) {
    } else {
      compare_ret = group_id_ - other.group_id_;
    }
    return compare_ret;
  }
  bool operator==(const ObNetEndpointKey &other) const
  {
    return 0 == compare(other);
  }
  ObNetEndpointKey &operator=(const ObNetEndpointKey &other)
  {
    if (this != &other) {
      assign(other);
    }
    return (*this);
  }
  bool operator!=(const ObNetEndpointKey &other) const
  {
    return !operator==(other);
  }
  TO_STRING_KV(K_(addr), K_(group_id));

  ObAddr addr_;
  int32_t group_id_;
};

class ObNetEndpointValue
{
  OB_UNIS_VERSION(1);

public:
  ObNetEndpointValue() : predicted_bw_(-1), assigned_bw_(-1), expire_time_(0)
  {}
  ObNetEndpointValue(const ObNetEndpointValue &other)
  {
    (void)assign(other);
  }

  ObNetEndpointValue &operator=(const ObNetEndpointValue &other)
  {
    if (this != &other) {
      (void)assign(other);
    }
    return *this;
  }
  int assign(const ObNetEndpointValue &other)
  {
    int ret = OB_SUCCESS;
    if (this != &other) {
      predicted_bw_ = other.predicted_bw_;
      expire_time_ = other.expire_time_;
    }
    return ret;
  }
  TO_STRING_KV(K_(predicted_bw), K_(assigned_bw), K_(expire_time));

  int64_t predicted_bw_;
  int64_t assigned_bw_;
  int64_t expire_time_;
};

struct ObNetEndpointKeyValue
{
  ObNetEndpointKeyValue() : key_(), value_(nullptr)
  {}
  ObNetEndpointKeyValue(const ObNetEndpointKey &key, ObNetEndpointValue *value) : key_(key), value_(value)
  {}
  TO_STRING_KV(K(key_), KP(value_));

  ObNetEndpointKey key_;
  ObNetEndpointValue *value_;
};
typedef common::ObSEArray<ObNetEndpointKeyValue, 16> ObNetEndpointKVArray;

struct ObNetEndpointRegisterArg
{
  OB_UNIS_VERSION(1);

public:
  ObNetEndpointRegisterArg() : endpoint_key_(), expire_time_(0)
  {}
  ~ObNetEndpointRegisterArg()
  {}
  bool is_valid() const
  {
    return endpoint_key_.is_valid();
  }
  int assign(const ObNetEndpointRegisterArg &other);
  TO_STRING_KV(K_(endpoint_key), K_(expire_time));

  ObNetEndpointKey endpoint_key_;
  int64_t expire_time_;
};

struct ObNetEndpointPredictIngressArg
{
  OB_UNIS_VERSION(1);

public:
  ObNetEndpointPredictIngressArg() : endpoint_key_()
  {}
  ~ObNetEndpointPredictIngressArg()
  {}
  bool is_valid() const
  {
    return endpoint_key_.is_valid();
  }
  int assign(const ObNetEndpointPredictIngressArg &other);
  TO_STRING_KV(K_(endpoint_key));

  ObNetEndpointKey endpoint_key_;
};

struct ObNetEndpointPredictIngressRes
{
  OB_UNIS_VERSION(1);

public:
  ObNetEndpointPredictIngressRes() : predicted_bw_(0)
  {}
  ~ObNetEndpointPredictIngressRes()
  {}
  int assign(const ObNetEndpointPredictIngressRes &other);
  TO_STRING_KV(K_(predicted_bw));

  int64_t predicted_bw_;
};

struct ObNetEndpointSetIngressArg
{
  OB_UNIS_VERSION(1);

public:
  ObNetEndpointSetIngressArg() : endpoint_key_(), assigned_bw_(0)
  {}
  ~ObNetEndpointSetIngressArg()
  {}
  bool is_valid() const
  {
    return endpoint_key_.is_valid();
  }
  int assign(const ObNetEndpointSetIngressArg &other);
  TO_STRING_KV(K_(endpoint_key), K_(assigned_bw));

  ObNetEndpointKey endpoint_key_;
  int64_t assigned_bw_;
};

struct ObNetEndpointSetIngressRes
{
  OB_UNIS_VERSION(1);

public:
  ObNetEndpointSetIngressRes() : res_(common::OB_ERROR)
  {}
  ~ObNetEndpointSetIngressRes()
  {}
  int assign(const ObNetEndpointSetIngressRes &other);
  TO_STRING_KV(K_(res));

  int res_;
};

}  // namespace obrpc
}  // namespace oceanbase
#endif /* OCEANBASE_ENDPOINY_INGRESS_RPC_STRUCT_H_ */
