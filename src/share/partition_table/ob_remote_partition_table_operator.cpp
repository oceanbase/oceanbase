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

#define USING_LOG_PREFIX SHARE
#include "lib/string/ob_sql_string.h"
#include "share/ob_define.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/schema/ob_schema_mgr.h"
#include "share/schema/ob_multi_version_schema_service.h"
#include "share/ob_remote_sql_proxy.h"
#include "share/partition_table/ob_partition_location.h"
#include "share/partition_table/ob_remote_partition_table_operator.h"
#include "common/ob_partition_key.h"

using namespace oceanbase::lib;
using namespace oceanbase::common;
using namespace oceanbase::common::sqlclient;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;

namespace oceanbase {
namespace share {
ObRemotePartitionTableOperator::ObRemotePartitionTableOperator(ObIPartPropertyGetter& prop_getter)
    : ObPartitionTableOperator(prop_getter), inited_(false), schema_service_(NULL), pt_(prop_getter)
{}

int ObRemotePartitionTableOperator::init(
    schema::ObMultiVersionSchemaService* schema_service, ObRemoteSqlProxy* remote_sql_proxy)
{
  UNUSEDx(schema_service, remote_sql_proxy);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::get(const uint64_t table_id, const int64_t partition_id,
    ObPartitionInfo& partition_info, const bool need_fetch_faillist, const int64_t cluster_id)
{
  UNUSEDx(table_id, partition_id, partition_info, need_fetch_faillist, cluster_id);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
    common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions, const int64_t cluster_id)
{
  UNUSEDx(keys, allocator, partitions, cluster_id);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::batch_execute(const common::ObIArray<ObPartitionReplica>& replicas)
{
  UNUSED(replicas);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::prefetch_by_table_id(const uint64_t tenant_id, const uint64_t table_id,
    const int64_t partition_id, common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist)
{

  UNUSEDx(tenant_id, table_id, partition_id);
  UNUSEDx(partition_infos, need_fetch_faillist);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::prefetch(const uint64_t tenant_id, const uint64_t table_id,
    const int64_t partition_id, common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
    const bool need_fetch_faillist)
{
  UNUSEDx(tenant_id, table_id, partition_id);
  UNUSEDx(partition_infos, ignore_row_checksum, need_fetch_faillist);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id,
    const uint64_t table_id, const int64_t partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
    const bool need_fetch_faillist)
{
  UNUSEDx(pt_table_id, pt_partition_id, table_id, partition_id);
  UNUSEDx(partition_infos, need_fetch_faillist);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::update(const ObPartitionReplica& replica)
{
  UNUSED(replica);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::remove(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server)
{
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(server);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::set_unit_id(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const uint64_t unit_id)
{
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(server);
  UNUSED(unit_id);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::set_original_leader(
    const uint64_t table_id, const int64_t partition_id, const bool is_original_leader)
{
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(is_original_leader);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::update_rebuild_flag(
    const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild)
{
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(server);
  UNUSED(rebuild);
  return OB_NOT_SUPPORTED;
}

int ObRemotePartitionTableOperator::update_fail_list(const uint64_t table_id, const int64_t partition_id,
    const common::ObAddr& server, const ObPartitionReplica::FailList& fail_list)
{
  UNUSED(table_id);
  UNUSED(partition_id);
  UNUSED(server);
  UNUSED(fail_list);
  return OB_NOT_SUPPORTED;
}

}  // namespace share
}  // namespace oceanbase
