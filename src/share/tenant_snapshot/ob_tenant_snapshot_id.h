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


#ifndef OCEANBASE_SHARE_OB_TENANT_SNAPSHOT_ID_H_
#define OCEANBASE_SHARE_OB_TENANT_SNAPSHOT_ID_H_

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"     // TO_STRING_KV

namespace oceanbase
{
namespace share
{
class ObTenantSnapshotID final
{
public:
  static const int64_t OB_INVALID_SNAPSHOT_ID = -1;

public:
  ObTenantSnapshotID() : id_(OB_INVALID_SNAPSHOT_ID) {}
  ObTenantSnapshotID(const ObTenantSnapshotID &other) : id_(other.id_) {}
  explicit ObTenantSnapshotID(const int64_t id) : id_(id) {}
  ~ObTenantSnapshotID() { reset(); }

public:
  int64_t id() const { return id_; }
  void reset() { id_ = OB_INVALID_SNAPSHOT_ID; }
  bool is_valid() const { return id_ != OB_INVALID_SNAPSHOT_ID; }
  // assignment
  ObTenantSnapshotID &operator=(const int64_t id) { id_ = id; return *this; }
  ObTenantSnapshotID &operator=(const ObTenantSnapshotID &other) { id_ = other.id_; return *this; }

  // compare operator
  bool operator == (const ObTenantSnapshotID &other) const { return id_ == other.id_; }
  bool operator >  (const ObTenantSnapshotID &other) const { return id_ > other.id_; }
  bool operator != (const ObTenantSnapshotID &other) const { return id_ != other.id_; }
  bool operator <  (const ObTenantSnapshotID &other) const { return id_ < other.id_; }
  int compare(const ObTenantSnapshotID &other) const
  {
    if (id_ == other.id_) {
      return 0;
    } else if (id_ < other.id_) {
      return -1;
    } else {
      return 1;
    }
  }

  uint64_t hash() const
  {
    OB_ASSERT(id_ != UINT64_MAX);
    return id_;
  }

  int hash(uint64_t &hash_val) const
  {
    int ret = OB_SUCCESS;
    hash_val = hash();
    return ret;
  }

  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(id));

private:
  int64_t id_;
};

} // end namespace share
} // end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_TENANT_SNAPSHOT_ID_H_
