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

#ifndef OCEANBASE_PARTITION_TABLE_FAKE_PARTITION_TABLE_OPERATOR_H_
#define OCEANBASE_PARTITION_TABLE_FAKE_PARTITION_TABLE_OPERATOR_H_

#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_partition_info.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"

static void *null_ptr = (void *)0x8888; // instead of NULL to avoid coverity complain.

namespace oceanbase
{
namespace share
{
class FakePartitionTableOperator : public ObPartitionTableOperator
{
public:
  FakePartitionTableOperator() : ObPartitionTableOperator(*static_cast<ObIPartPropertyGetter *>(null_ptr)) {}

  virtual int get(const uint64_t table_id, const int64_t partition_id,
      ObPartitionInfo &partition_info)
  {
    int ret = common::OB_SUCCESS;
    partition_info.reuse();
    for (int64_t i = 0; i < partitions_.count(); i++) {
      if (partitions_.at(i).get_table_id() == table_id
          && partitions_.at(i).get_partition_id() == partition_id) {
        partition_info.assign(partitions_.at(i));
        break;
      }
    }
    return ret;
  }
  int set_original_leader(const uint64_t table_id, const int64_t partition_id,
      const bool is_original_leader)
  {
    int ret = common::OB_SUCCESS;
    FOREACH(p, partitions_) {
      if (p->get_table_id() == table_id && p->get_partition_id() == partition_id) {
        FOREACH_CNT(r, p->get_replicas_v2()) {
          if (common::LEADER == r->role_) {
            r->is_original_leader_ = is_original_leader;
          }
        }
      }
    }
    return ret;
  }

  int prefetch(const uint64_t tenant_id, const uint64_t table_id,
      const int64_t partition_id, common::ObIArray<ObPartitionInfo> &partition_infos,
      bool ignore_row_checksum, bool use_sys_tenant)
  {
    int ret = common::OB_SUCCESS;
    UNUSED(ignore_row_checksum);
    UNUSED(use_sys_tenant);
    FOREACH_CNT_X(p, partitions_, common::OB_SUCCESS == ret) {
      if (p->get_tenant_id() == tenant_id && (p->get_table_id() > table_id
          || (p->get_table_id() == table_id && p->get_partition_id() > partition_id))) {
        if (OB_FAIL(partition_infos.push_back(*p))) {
          SHARE_LOG(WARN, "push_back failed", K(ret));
        }
      }
    }
    return ret;
  }
int prefetch_by_table_id(const uint64_t tenant_id, const uint64_t start_table_id,  const int64_t start_partition_id,
                         ObIArray<ObPartitionInfo> &partition_infos,
                         bool use_sys_tenant)
{
  int ret = OB_SUCCESS;
  UNUSED(use_sys_tenant);
  FOREACH_CNT_X(p, partitions_, common::OB_SUCCESS == ret) {
    if (p->get_tenant_id() == tenant_id
        && p->get_table_id() == start_table_id
        && p->get_partition_id() >= start_partition_id) {
      if (OB_FAIL(partition_infos.push_back(*p))) {
        SHARE_LOG(WARN, "push_back failed", K(ret));
      }
    }
  }
  return ret;
}
  common::ObArray<ObPartitionInfo> partitions_;
};
} // end namespace share
} // end namespace oceanbase

#endif // OCEANBASE_PARTITION_TABLE_FAKE_PARTITION_TABLE_OPERATOR_H_
