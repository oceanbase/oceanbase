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

#ifndef OCEANBASE_ROOTSERVER_OB_ALL_PARTITION_TABLE_H_
#define OCEANBASE_ROOTSERVER_OB_ALL_PARTITION_TABLE_H_

#include "share/ob_virtual_table_projector.h"
#include "share/partition_table/ob_partition_info.h"
#include "share/partition_table/ob_partition_table_iterator.h"
#include "lib/container/ob_array.h"
#include "rootserver/ob_server_manager.h"

namespace oceanbase {
namespace share {
class ObPartitionInfo;
class ObPartitionTableIterator;
class ObPartitionTableOperator;
namespace schema {
class ObMultiVersionSchemaService;
class ObTableSchema;
class ObSchemaGetterGuard;
}  // namespace schema
}  // namespace share
namespace rootserver {
class ObServerManager;

class ObAllPartitionTable : public common::ObVirtualTableProjector {
public:
  ObAllPartitionTable();
  virtual ~ObAllPartitionTable();

  int init(share::ObPartitionTableOperator& pt_operator, share::schema::ObMultiVersionSchemaService& schema_service,
      ObServerManager& server_mgr, share::schema::ObSchemaGetterGuard* schema_guard);

  virtual int inner_open();

  virtual int inner_get_next_row(common::ObNewRow*& row);

protected:
  virtual int get_condition(uint64_t& specific_tenant_id, uint64_t& specific_table_id);

private:
  int get_full_row(const share::schema::ObTableSchema* table, const share::ObPartitionReplica& replica,
      common::ObIArray<Column>& columns);
  bool inited_;
  share::ObPartitionTableOperator* pt_operator_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  ObServerManager* server_mgr_;
  share::schema::ObSchemaGetterGuard* schema_guard_;
  const share::schema::ObTableSchema* table_schema_;
  share::ObPartitionInfo partition_info_;
  share::ObPartitionTableIterator all_pt_iter_;
  share::ObTenantPartitionIterator tenant_pt_iter_;
  share::ObTablePartitionIterator table_pt_iter_;
  share::ObIPartitionTableIterator* pt_iter_;
  ObArenaAllocator arena_allocator_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllPartitionTable);
};

}  // end namespace rootserver
}  // end namespace oceanbase

#endif  // OCEANBASE_ROOTSERVER_OB_ALL_PARTITION_TABLE_H_
