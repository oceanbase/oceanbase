/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#pragma once

#include "lib/utility/ob_unify_serialize.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/net/ob_addr.h"

namespace oceanbase {
namespace common {

struct ObDetectableId
{
  OB_UNIS_VERSION(1);
public:
  ObDetectableId() : first_(0), second_(0), tenant_id_(0) {}
  void operator=(const ObDetectableId &other)
  {
    first_ = other.first_;
    second_ = other.second_;
    tenant_id_ = other.tenant_id_;
  }
  bool operator==(const ObDetectableId &other) const
  {
    return first_ == other.first_ && second_ == other.second_ && tenant_id_ == other.tenant_id_;
  }
  bool operator!=(const ObDetectableId &other) const
  {
    return !(*this == other);
  }

  inline int64_t to_string(char *buf, const int64_t len) const
  {
    int64_t pos = 0;
    databuff_printf(buf, len, pos, "ObDetectableId: (%ld,%ld,%ld)", first_, second_, tenant_id_);
    return pos;
  }

  uint64_t hash() const
  {
    return first_;
  }

  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }

  bool is_invalid() const
  {
    return (0 == first_ && 0 == second_);
  }

public:
  uint64_t first_;
  uint64_t second_;
  uint64_t tenant_id_;
};

struct ObRegisterDmInfo
{
  OB_UNIS_VERSION(1);
public:
  ObRegisterDmInfo() :detectable_id_(), addr_() {}
  ObRegisterDmInfo(const common::ObDetectableId &detectable_id, const common::ObAddr &addr)
      : detectable_id_(detectable_id), addr_(addr) {}
  void operator=(const ObRegisterDmInfo& other)
  {
    detectable_id_ = other.detectable_id_;
    addr_ = other.addr_;
  }
  bool is_valid() const { return (!detectable_id_.is_invalid() && addr_.is_valid()); }
  TO_STRING_KV(K_(detectable_id), K_(addr));
  common::ObDetectableId detectable_id_;
  common::ObAddr addr_;
};

struct ObUnregisterDmInfo
{
public:
  ObUnregisterDmInfo() : detectable_id_(), node_sequence_id_(0) {}
  ObUnregisterDmInfo(const common::ObDetectableId &detectable_id, uint64_t node_sequence_id)
      : detectable_id_(detectable_id), node_sequence_id_(node_sequence_id) {}
  void operator=(const ObUnregisterDmInfo& other)
  {
    detectable_id_ = other.detectable_id_;
    node_sequence_id_ = other.node_sequence_id_;
  }
  bool is_valid() const { return (!detectable_id_.is_invalid() && node_sequence_id_ != 0); }
  TO_STRING_KV(K_(detectable_id), K_(node_sequence_id));
  common::ObDetectableId detectable_id_;
  uint64_t node_sequence_id_;
};

} // end namespace common
} // end namespace oceanbase
