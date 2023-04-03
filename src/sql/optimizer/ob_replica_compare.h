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

#ifndef OCEANBASE_SQL_OB_REPLICA_COMPARE_H
#define OCEANBASE_SQL_OB_REPLICA_COMPARE_H
#include "share/ob_define.h"
#include "sql/optimizer/ob_route_policy.h"
namespace oceanbase
{
namespace sql
{
class ObReplicaCompare
{
public:
  enum CompareType
  {
    IS_OTHER_REGION,
    ZONE_TYPE,
    MERGE_STATUS,
    POS_TYPE,
    CMP_CNT
  };
  enum CompareRes
  {
    EQUAL,
    LESS,
    GREATER
  };
public:
  explicit ObReplicaCompare(ObRoutePolicyType policy_type);
  virtual ~ObReplicaCompare() {}
  bool operator()(const ObRoutePolicy::CandidateReplica &replica1, const ObRoutePolicy::CandidateReplica &replica2);
  int get_sort_ret() const { return ret_; }

  inline CompareRes compare_other_region(const ObRoutePolicy::CandidateReplica &replica1,
                                         const ObRoutePolicy::CandidateReplica &replica2);
  inline CompareRes compare_zone_type(const ObRoutePolicy::CandidateReplica &replica1,
                                         const ObRoutePolicy::CandidateReplica &replica2);
  inline CompareRes compare_merge_status(const ObRoutePolicy::CandidateReplica &replica1,
                                         const ObRoutePolicy::CandidateReplica &replica2);
  inline CompareRes compare_pos_type(const ObRoutePolicy::CandidateReplica &replica1,
                                     const ObRoutePolicy::CandidateReplica &replica2);

  typedef CompareRes (ObReplicaCompare::*CmpFuncPtr)(const ObRoutePolicy::CandidateReplica &replica1,
                                                     const ObRoutePolicy::CandidateReplica &replica2);
private:
  int ret_;
  ObRoutePolicyType policy_type_;
  CmpFuncPtr cmp_func_array_[CMP_CNT];
  CompareType readonly_zone_first_[CMP_CNT];
  CompareType only_readonly_zone_[CMP_CNT];
  CompareType unmerge_zone_first_[CMP_CNT];
};


ObReplicaCompare::CompareRes ObReplicaCompare::compare_other_region(const ObRoutePolicy::CandidateReplica &replica1,
                                               const ObRoutePolicy::CandidateReplica &replica2)
{
  CompareRes cmp_ret = EQUAL;
  if (replica1.attr_.pos_type_ != ObRoutePolicy::OTHER_REGION
      && replica2.attr_.pos_type_ == ObRoutePolicy::OTHER_REGION) {
    cmp_ret = LESS;
  } else if (replica1.attr_.pos_type_ == ObRoutePolicy::OTHER_REGION
             && replica2.attr_.pos_type_ != ObRoutePolicy::OTHER_REGION) {
    cmp_ret = GREATER;
  } else {/*do nothing*/}
  return cmp_ret;
}

ObReplicaCompare::CompareRes ObReplicaCompare::compare_zone_type(const ObRoutePolicy::CandidateReplica &replica1,
                                               const ObRoutePolicy::CandidateReplica &replica2)
{
  CompareRes cmp_ret = EQUAL;
  if (replica1.attr_.zone_type_ == common::ZONE_TYPE_READONLY
      && replica2.attr_.zone_type_ != common::ZONE_TYPE_READONLY) {
    cmp_ret = LESS;
  } else if (replica1.attr_.zone_type_ != common::ZONE_TYPE_READONLY
             && replica2.attr_.zone_type_ == common::ZONE_TYPE_READONLY) {
    cmp_ret = GREATER;
  } else {/*do nothing*/}
  return cmp_ret;
}

ObReplicaCompare::CompareRes ObReplicaCompare::compare_merge_status(const ObRoutePolicy::CandidateReplica &replica1,
                                               const ObRoutePolicy::CandidateReplica &replica2)
{
  CompareRes cmp_ret = EQUAL;
  if (replica1.attr_.merge_status_ != ObRoutePolicy::MERGING
      && replica2.attr_.merge_status_ == ObRoutePolicy::MERGING) {
    cmp_ret = LESS;
  } else if (replica1.attr_.merge_status_ == ObRoutePolicy::MERGING
             && replica2.attr_.merge_status_ != ObRoutePolicy::MERGING) {
    cmp_ret = GREATER;
  } else {/*do nothing*/}
  return cmp_ret;
}

ObReplicaCompare::CompareRes ObReplicaCompare::compare_pos_type(const ObRoutePolicy::CandidateReplica &replica1,
                                               const ObRoutePolicy::CandidateReplica &replica2)
{
  CompareRes cmp_ret = EQUAL;
  if ((replica1.attr_.pos_type_ == ObRoutePolicy::SAME_SERVER || replica1.attr_.pos_type_ == ObRoutePolicy::SAME_IDC)
      && replica2.attr_.pos_type_ == ObRoutePolicy::SAME_REGION) {
    cmp_ret = LESS;
  } else if ((replica2.attr_.pos_type_ == ObRoutePolicy::SAME_SERVER || replica2.attr_.pos_type_ == ObRoutePolicy::SAME_IDC)
             && replica1.attr_.pos_type_ == ObRoutePolicy::SAME_REGION ) {
    cmp_ret = GREATER;
  } else {/*do nohtong*/  }
  return cmp_ret;
}

}//sql
}//oceanbase
#endif
