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

#ifndef OCEANBASE_PARTITION_TABLE_OB_REMOTE_PARTITION_TABLE_OPERATOR
#define OCEANBASE_PARTITION_TABLE_OB_REMOTE_PARTITION_TABLE_OPERATOR
#include "lib/task/ob_timer.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "share/partition_table/ob_partition_table_operator.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"

namespace oceanbase {
namespace common {
class ObPartitionKey;
namespace sqlclient {
class ObMySQLServerProvider;
}
}  // namespace common
namespace share {
class ObRemoteSqlProxy;
class ObPartitionLocation;
namespace schema {
class ObMultiVersionSchemaService;
}
class ObRemotePartitionTableOperator : public ObPartitionTableOperator {
public:
  explicit ObRemotePartitionTableOperator(ObIPartPropertyGetter& prop_getter);
  virtual ~ObRemotePartitionTableOperator()
  {}
  int init(share::schema::ObMultiVersionSchemaService* schema_service, ObRemoteSqlProxy* remote_sql_proxy);
  virtual int get(const uint64_t table_id, const int64_t partition_id, ObPartitionInfo& partition_info,
      const bool need_fetch_faillist = false, const int64_t cluster_id = common::OB_INVALID_ID) override;
  virtual int batch_fetch_partition_infos(const common::ObIArray<common::ObPartitionKey>& keys,
      common::ObIAllocator& allocator, common::ObArray<ObPartitionInfo*>& partitions,
      const int64_t cluster_id = common::OB_INVALID_ID) override;

  // not supported interface
  virtual int batch_execute(const common::ObIArray<ObPartitionReplica>& replicas) override;
  virtual int prefetch_by_table_id(const uint64_t tenant_id, const uint64_t table_id, const int64_t partition_id,
      common::ObIArray<ObPartitionInfo>& partition_infos, const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t tenant_id, const uint64_t table_id, const int64_t partition_id,
      common::ObIArray<ObPartitionInfo>& partition_infos, bool ignore_row_checksum,
      const bool need_fetch_faillist = false) override;

  virtual int prefetch(const uint64_t pt_table_id, const int64_t pt_partition_id, const uint64_t table_id,
      const int64_t partition_id, common::ObIArray<ObPartitionInfo>& partition_infos,
      const bool need_fetch_faillist = false) override;

  virtual int update(const ObPartitionReplica& replica) override;

  virtual int remove(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server) override;

  virtual int set_unit_id(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const uint64_t unit_id) override;

  virtual int set_original_leader(
      const uint64_t table_id, const int64_t partition_id, const bool is_original_leader) override;

  virtual int update_rebuild_flag(
      const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server, const bool rebuild) override;

  virtual int update_fail_list(const uint64_t table_id, const int64_t partition_id, const common::ObAddr& server,
      const ObPartitionReplica::FailList& fail_list) override;

private:
  int get_partition_info(const uint64_t table_id, const int64_t partition_id, const bool filter_flag_replica,
      ObPartitionInfo& partition_info, common::ObConsistencyLevel consistency);

private:
  bool inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObPartitionTableOperator pt_;
};
}  // namespace share
}  // namespace oceanbase
#endif
