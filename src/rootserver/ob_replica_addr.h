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

#ifndef _OB_REPLICA_ADDR_H
#define _OB_REPLICA_ADDR_H 1
#include "lib/net/ob_addr.h"
#include "lib/container/ob_se_array.h"
#include "lib/container/ob_array.h"
#include "lib/ob_replica_define.h"

namespace oceanbase
{
namespace rootserver
{
struct ObReplicaAddr
{
  uint64_t unit_id_;
  bool initial_leader_;
  common::ObAddr addr_;
  common::ObZone zone_;
  common::ObReplicaType replica_type_;
  common::ObReplicaProperty replica_property_;

  ObReplicaAddr()
      : unit_id_(common::OB_INVALID_ID),
        initial_leader_(false),
        addr_(),
        zone_(),
        replica_type_(common::REPLICA_TYPE_MAX),
        replica_property_() {}
  void reset() { *this = ObReplicaAddr(); }
  int64_t get_memstore_percent() const {return replica_property_.get_memstore_percent();}
  int set_memstore_percent(const int64_t mp) {return replica_property_.set_memstore_percent(mp);}
  TO_STRING_KV(K_(unit_id),
               K_(initial_leader),
               K_(addr),
               K_(zone),
               K_(replica_type),
               K_(replica_property));
};

// this struct is used to find initial leader for partitions when create table
struct ObPrimaryZoneReplicaCandidate
{
  ObPrimaryZoneReplicaCandidate()
    : is_full_replica_(false),
      zone_(),
      zone_score_(INT64_MAX),
      random_score_(INT64_MAX) {}

  bool is_full_replica_;
  common::ObZone zone_;
  int64_t zone_score_;
  int64_t random_score_;

  void reset() {
    is_full_replica_ = false;
    zone_.reset();
    zone_score_ = INT64_MAX;
    random_score_ = INT64_MAX;
  }

  TO_STRING_KV(K_(is_full_replica),
               K_(zone),
               K_(zone_score),
               K_(random_score));
};

struct ObPrimaryZoneReplicaCmp
{
  bool operator()(
       const ObPrimaryZoneReplicaCandidate &left,
       const ObPrimaryZoneReplicaCandidate &right) {
    bool cmp = false;
    if (left.is_full_replica_ && !right.is_full_replica_) {
      cmp = true;
    } else if (!left.is_full_replica_ && right.is_full_replica_) {
      cmp = false;
    } else {
      cmp = cmp_zone_score(left, right);
    }
    return cmp;
  }
  bool cmp_zone_score(
       const ObPrimaryZoneReplicaCandidate &left,
       const ObPrimaryZoneReplicaCandidate &right) {
    bool cmp = false;
    if (left.zone_score_ < right.zone_score_) {
      cmp = true;
    } else if (left.zone_score_ > right.zone_score_) {
      cmp = false;
    } else {
      cmp = cmp_random_score(left, right);
    }
    return cmp;
  }
  bool cmp_random_score(
       const ObPrimaryZoneReplicaCandidate &left,
       const ObPrimaryZoneReplicaCandidate &right) {
    bool cmp = false;
    if (left.random_score_ > right.random_score_) {
      cmp = true;
    } else {
      cmp = false;
    }
    return cmp;
  }
};

typedef common::ObArray<ObReplicaAddr> ObPartitionAddr;
typedef common::ObArray<ObPartitionAddr> ObTablePartitionAddr;
typedef common::ObIArray<ObPartitionAddr> ObITablePartitionAddr;

} // end namespace rootserver
} // end namespace oceanbase

#endif /* _OB_REPLICA_ADDR_H */
