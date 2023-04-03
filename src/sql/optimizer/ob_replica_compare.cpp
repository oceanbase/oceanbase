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

#define USING_LOG_PREFIX SQL_OPT
#include "sql/optimizer/ob_replica_compare.h"
using namespace oceanbase::common;
namespace oceanbase
{
namespace sql
{

ObReplicaCompare::ObReplicaCompare(ObRoutePolicyType policy_type)
    :ret_(OB_SUCCESS),
     policy_type_(policy_type),
     readonly_zone_first_{IS_OTHER_REGION, ZONE_TYPE, MERGE_STATUS, POS_TYPE},
     only_readonly_zone_{ZONE_TYPE, IS_OTHER_REGION, MERGE_STATUS, POS_TYPE,},
     unmerge_zone_first_{IS_OTHER_REGION, MERGE_STATUS, ZONE_TYPE, POS_TYPE}
      {
        static_assert(sizeof(readonly_zone_first_) == sizeof(only_readonly_zone_), "invalid array size");
        static_assert(sizeof(readonly_zone_first_) == sizeof(unmerge_zone_first_), "invalid array size");
        static_assert((sizeof(readonly_zone_first_)/sizeof(CompareType)) == (sizeof(cmp_func_array_)/sizeof(CmpFuncPtr)), "invalid array size");

        cmp_func_array_[IS_OTHER_REGION] = &ObReplicaCompare::compare_other_region;
        cmp_func_array_[ZONE_TYPE] = &ObReplicaCompare::compare_zone_type;
        cmp_func_array_[MERGE_STATUS] = &ObReplicaCompare::compare_merge_status;
        cmp_func_array_[POS_TYPE] = &ObReplicaCompare::compare_pos_type;
      }


bool ObReplicaCompare::operator()(const ObRoutePolicy::CandidateReplica &replica1,
                                  const ObRoutePolicy::CandidateReplica &replica2)
{
  int ret = OB_SUCCESS;
  bool bool_ret = false;
  CompareType *cmp_type_array = NULL;
  if (OB_FAIL(ret_)) {//do nothing if we already have an error
  } else {
    if (READONLY_ZONE_FIRST == policy_type_) {
      cmp_type_array = readonly_zone_first_;
    } else if (ONLY_READONLY_ZONE == policy_type_) {
      cmp_type_array = only_readonly_zone_;
    } else if (UNMERGE_ZONE_FIRST == policy_type_) {
      cmp_type_array = unmerge_zone_first_;
    } else {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected policy type", K(policy_type_), K(ret));
    }
    CompareRes cmp_res = EQUAL;
    for(int64_t i = 0 ; OB_SUCC(ret) && cmp_res == EQUAL && i < CMP_CNT; ++i) {
      CompareType cmp_type = cmp_type_array[i];
      CmpFuncPtr cmp_fun = cmp_func_array_[cmp_type];
      if (OB_ISNULL(cmp_fun)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cmp fun is NULL", K(cmp_type), K(ret));
      } else {
        cmp_res = (this->*cmp_fun)(replica1, replica2);
      }
    }
    bool_ret = (cmp_res == LESS);
    ret_ = ret;
  }
  return bool_ret;
}

}//sql
}//oceanbase
