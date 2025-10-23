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

#ifdef THREAD_GROUP_STAT_DEF
THREAD_GROUP_STAT_DEF(OB_THREAD_GROUP_DEFAULT, 0, share::OBCG_DEFAULT)
THREAD_GROUP_STAT_DEF(OB_THREAD_GROUP_OLAP_ASYNC, 1, share::OBCG_OLAP_ASYNC_JOB)
THREAD_GROUP_STAT_DEF(OB_THREAD_GROUP_DBMS_SCHE, 2, share::OBCG_DBMS_SCHED_JOB)
THREAD_GROUP_STAT_DEF(OB_THREAD_GROUP_LARGE_QUERY,3, share::OBCG_LQ)
THREAD_GROUP_STAT_DEF(OB_THREAD_GROUP_PX, 4, OB_INVALID_GROUP_ID)
THREAD_GROUP_STAT_DEF(OB_THREAD_GROUP_DAG, 5, OB_INVALID_GROUP_ID)
#endif

#ifndef OB_TENANT_THREAD_GROUP_STAT_H
#define OB_TENANT_THREAD_GROUP_STAT_H

#include <stdint.h>
#include <sys/types.h>
#include "share/resource_manager/ob_cgroup_ctrl.h"


namespace oceanbase
{
namespace share
{

enum ObTenantThreadGroupId
{
#define THREAD_GROUP_STAT_DEF(name, group_id, cg_id) name = group_id,
#include "share/resource_manager/ob_tenant_thread_group_statistic.h"
#undef THREAD_GROUP_STAT_DEF
  OB_TENANT_THREAD_GROUP_MAXNUM,
};

class ObTenantThreadGroupInfo
{
public:
  ObTenantThreadGroupInfo() : cg_id_(OB_INVALID_GROUP_ID) {}
  void set_args(uint64_t cg_id) { cg_id_ = cg_id; }
  uint64_t cg_id_;
};

class ObTenantThreadGroupSet
{
public:
  ObTenantThreadGroupSet()
  {
#define THREAD_GROUP_STAT_DEF(name, group_id, cg_id) group_infos_[group_id].set_args(cg_id);
#include "share/resource_manager/ob_tenant_thread_group_statistic.h"
#undef THREAD_GROUP_STAT_DEF
  }
  static ObTenantThreadGroupSet &instance()
  {
    static ObTenantThreadGroupSet instance_;
    return instance_;
  }
  uint64_t get_group_cg_id(uint64_t group_id) const
  {
    uint64_t cg_id = OB_INVALID_GROUP_ID;
    if (group_id >= OB_TENANT_THREAD_GROUP_MAXNUM) {}
    else {
      cg_id = group_infos_[group_id].cg_id_;
    }
    return cg_id;
  }
private:
  ObTenantThreadGroupInfo group_infos_[OB_TENANT_THREAD_GROUP_MAXNUM];
};

}
}


#endif
