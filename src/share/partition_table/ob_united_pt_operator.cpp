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

#define USING_LOG_PREFIX SHARE_PT
#include "share/partition_table/ob_united_pt_operator.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
#include "share/partition_table/ob_ipartition_table.h"
#include "observer/ob_server_struct.h"
#include "share/ob_multi_cluster_util.h"
namespace oceanbase {
namespace share {
ObUnitedPtOperator::ObUnitedPtOperator() : inited_(false), pt_operator_(NULL), remote_pt_operator_(NULL)
{}

int ObUnitedPtOperator::init(ObPartitionTableOperator* pt_operator, ObRemotePartitionTableOperator* remote_pt_operator)
{
  int ret = OB_SUCCESS;
  if (inited_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("init twice", KR(ret));
  } else if (OB_ISNULL(pt_operator) || OB_ISNULL(remote_pt_operator)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), K(pt_operator), K(remote_pt_operator));
  } else {
    inited_ = true;
    pt_operator_ = pt_operator;
    remote_pt_operator_ = remote_pt_operator;
  }
  return ret;
}

int ObUnitedPtOperator::united_get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition)
{
  int ret = OB_SUCCESS;
  partition.reuse();
  ModulePageAllocator allocator(ObNewModIds::OB_PARTITION_MIGRATE);
  ObPartitionInfo local_partition;
  ObPartitionInfo remote_partition;
  local_partition.set_allocator(&allocator);
  remote_partition.set_allocator(&allocator);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (OB_FAIL(get(table_id, partition_id, local_partition, remote_partition))) {
    LOG_WARN("fail to get", KR(ret));
  } else if (OB_FAIL(partition.assign(local_partition))) {
    LOG_WARN("fail to assign", KR(ret), K(local_partition));
  } else if (remote_partition.replica_count() > 0) {
    for (int64_t i = 0; i < remote_partition.get_replicas_v2().count() && OB_SUCC(ret); i++) {
      if (OB_FAIL(partition.add_replica(remote_partition.get_replicas_v2().at(i)))) {
        LOG_WARN("fail to add replica", KR(ret), K(i), K(remote_partition), K(partition));
      }
    }
  }
  return ret;
}

int ObUnitedPtOperator::get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& local_partition,
    ObPartitionInfo& remote_partition)
{
  int ret = OB_SUCCESS;
  int64_t start_time = ObTimeUtility::current_time();
  int64_t tenant_id = extract_tenant_id(table_id);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (!ObIPartitionTable::is_valid_key(table_id, partition_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), KT(table_id), K(partition_id));
  } else if (OB_FAIL(pt_operator_->get(table_id, partition_id, local_partition))) {
    LOG_WARN("fail to get partition info", KR(ret), K(table_id), K(partition_id));
  } else if (!GCTX.is_standby_cluster() || ObMultiClusterUtil::is_cluster_private_table(table_id)) {
    // nothing todo
  } else if (OB_FAIL(remote_pt_operator_->get(table_id, partition_id, remote_partition))) {
    LOG_WARN("fail to get remote partition info", KR(ret), K(table_id), K(partition_id));
  }
  return ret;
}
}  // namespace share
}  // namespace oceanbase
