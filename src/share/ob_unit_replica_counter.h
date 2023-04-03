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

#ifndef OCEANBASE_SHARE_OB_UNIT_REPLICA_COUNTER_H_
#define OCEANBASE_SHARE_OB_UNIT_REPLICA_COUNTER_H_

#include "lib/ob_define.h"
#include "lib/container/ob_vector.h"

namespace oceanbase
{
namespace share
{
struct UnitReplicaCounter
{
  int64_t f_replica_cnt_;
  int64_t d_replica_cnt_;
  int64_t l_replica_cnt_;
  int64_t e_replica_cnt_;
  int64_t r_replica_cnt_;
  int64_t index_num_;
  int64_t leader_cnt_;

public:

  UnitReplicaCounter()
      : f_replica_cnt_(0),
        d_replica_cnt_(0), 
        l_replica_cnt_(0),
        e_replica_cnt_(0),
        r_replica_cnt_(0),
        index_num_(0), leader_cnt_(0) {}
  void reset()
  {
    f_replica_cnt_ = 0;
    d_replica_cnt_ = 0;
    l_replica_cnt_ = 0;
    e_replica_cnt_ = 0;
    r_replica_cnt_ = 0;
    index_num_ = 0;
    leader_cnt_ = 0;
  }
  UnitReplicaCounter &operator=(const UnitReplicaCounter &unit_rep_cnt);
  int64_t get_full_replica_cnt() { return f_replica_cnt_; }
  int64_t get_d_replica_cnt() { return d_replica_cnt_; }
  int64_t get_logonly_replica_cnt() { return l_replica_cnt_; }
  int64_t get_encryption_logonly_replica_cnt() { return e_replica_cnt_; }; 
  int64_t get_readonly_replica_cnt() { return r_replica_cnt_; }
  int64_t get_all_replica_cnt() {
    return f_replica_cnt_
           + d_replica_cnt_
           + l_replica_cnt_
           + e_replica_cnt_
           + r_replica_cnt_;
  }
  int64_t get_index_num() { return index_num_; }
  int64_t get_leader_cnt() { return leader_cnt_; }
  TO_STRING_KV(K_(f_replica_cnt),
               K_(d_replica_cnt),
               K_(l_replica_cnt),
               K_(e_replica_cnt), 
               K_(r_replica_cnt),
               K_(index_num),
               K_(leader_cnt));
};

inline UnitReplicaCounter &UnitReplicaCounter::operator=(const UnitReplicaCounter &unit_rep_cnt)
{
    f_replica_cnt_ = unit_rep_cnt.f_replica_cnt_;
    d_replica_cnt_ = unit_rep_cnt.d_replica_cnt_;
    l_replica_cnt_ = unit_rep_cnt.l_replica_cnt_;
    e_replica_cnt_ = unit_rep_cnt.e_replica_cnt_;
    r_replica_cnt_ = unit_rep_cnt.r_replica_cnt_;
    index_num_ = unit_rep_cnt.index_num_;
    leader_cnt_ = unit_rep_cnt.leader_cnt_;
    return *this;
}

struct ObTURepSortKey
{
  ObTURepSortKey() : tenant_id_(common::OB_INVALID_ID), unit_id_(common::OB_INVALID_ID)
  {}
  ObTURepSortKey(const uint64_t tenant_id, const uint64_t unit_id)
      : tenant_id_(tenant_id), unit_id_(unit_id)
  {}
  bool operator==(const ObTURepSortKey &rtur) const
  {
    return (tenant_id_ == rtur.tenant_id_) && (unit_id_ == rtur.unit_id_);
  }
  bool operator!=(const ObTURepSortKey &rtur) const
  { return !(*this == rtur); }
  bool operator<(const ObTURepSortKey &rtur) const
  {
    bool bret = tenant_id_ < rtur.tenant_id_;
    if (false == bret && tenant_id_ == rtur.tenant_id_) {
      bret = unit_id_ < rtur.unit_id_;
    }
    return bret;
  }

  TO_STRING_KV(K_(tenant_id), K_(unit_id));

  uint64_t tenant_id_;
  uint64_t unit_id_;
};

struct TenantUnitRepCnt
{
  UnitReplicaCounter unit_rep_cnt_;
  uint64_t unit_id_;
  uint64_t tenant_id_;
  int64_t non_table_cnt_;
  int64_t now_time_;
public:
  TenantUnitRepCnt()
    : unit_rep_cnt_(), unit_id_ (common::OB_INVALID_ID),
      tenant_id_(common::OB_INVALID_ID), non_table_cnt_(0),
      now_time_(common::OB_INVALID_TIMESTAMP) {}

  TenantUnitRepCnt(const uint64_t unit_id, 
                   const uint64_t tenant_id,
                   const UnitReplicaCounter &unit_rep_cnt)
    : unit_rep_cnt_(unit_rep_cnt), unit_id_(unit_id), tenant_id_(tenant_id),
      non_table_cnt_(0), now_time_(common::OB_INVALID_TIMESTAMP) {}

  static bool cmp(const TenantUnitRepCnt* ltur, const TenantUnitRepCnt* rtur)
  {
    return (NULL != ltur && NULL != rtur) ?
      (ltur->get_sort_key() < rtur->get_sort_key()) : false;
  }

  TenantUnitRepCnt &operator=(const TenantUnitRepCnt &other);
  ObTURepSortKey get_sort_key() const
  { return ObTURepSortKey(tenant_id_, unit_id_); }

  TO_STRING_KV(K_(unit_rep_cnt), K_(unit_id),
               K_(tenant_id), K_(non_table_cnt),
               K_(now_time));
};

inline TenantUnitRepCnt &TenantUnitRepCnt::operator=(const TenantUnitRepCnt &other)
{
  unit_rep_cnt_ = other.unit_rep_cnt_;
  tenant_id_ = other.tenant_id_;
  non_table_cnt_ = other.non_table_cnt_;
  unit_id_ = other.unit_id_;
  now_time_ = other.now_time_;
  return *this;
}

typedef common::ObSortedVector<TenantUnitRepCnt*> TenantUnitRepCntVec;


}//end namespace share
}//end namespace oceanbase

#endif //OCEANBASE_SHARE_OB_UNIT_REPLICA_COUNTER_H_

