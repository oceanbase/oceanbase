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

#ifndef OCEANBASE_STORAGE_OB_DUP_REPLICA_CHECKER_H_
#define OCEANBASE_STORAGE_OB_DUP_REPLICA_CHECKER_H_

#include "common/ob_zone.h"
#include "common/ob_region.h"
#include "lib/container/ob_iarray.h"

namespace oceanbase {

namespace share {
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share

namespace common {
class ObPartitionKey;
class ObAddr;
}  // namespace common

namespace storage {
class ObLocalityManager;
enum class DupReplicaType {
  INVALID_TYPE = -1,
  NON_DUP_REPLICA = 0,
  DUP_REPLICA = 1,
};

class ObDupReplicaChecker {
public:
  ObDupReplicaChecker() : is_inited_(false), schema_service_(nullptr), locality_manager_(nullptr)
  {}
  virtual ~ObDupReplicaChecker()
  {}
  int init(share::schema::ObMultiVersionSchemaService* schema_service, storage::ObLocalityManager* locality_manager);
  /*   input:  pkey
   *   input:  server
   *   output:  the replica type (DUP_REPLICA, NON_DUP_REPLICA)
   *   return:  OB_SUCCESS
   *            OB_TABLE_NOT_EXIST
   */
  int get_dup_replica_type(
      const common::ObPartitionKey& pkey, const common::ObAddr& server, DupReplicaType& dup_replica_type);
  int get_dup_replica_type(const uint64_t table_id, const common::ObAddr& server, DupReplicaType& dup_replica_type);

private:
  int get_table_high_primary_zone_array(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, common::ObIArray<common::ObZone>& primary_zone_array);
  int get_table_high_primary_region_array(share::schema::ObSchemaGetterGuard& schema_guard,
      const share::schema::ObTableSchema& table_schema, common::ObIArray<common::ObRegion>& primary_region_array);

private:
  bool is_inited_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  storage::ObLocalityManager* locality_manager_;
};
}  // namespace storage
}  // namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_DUP_REPLICA_CHECKER_H_
