/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_DIRECT_LOAD_PARTITION_EXCHANGE_H_
#define OCEANBASE_ROOTSERVER_OB_DIRECT_LOAD_PARTITION_EXCHANGE_H_

#include "rootserver/ob_partition_exchange.h"

namespace oceanbase
{
namespace rootserver
{
class ObDirectLoadPartitionExchange : public ObPartitionExchange
{
public:
  explicit ObDirectLoadPartitionExchange(ObDDLService &ddl_service, const uint64_t data_version);
  virtual ~ObDirectLoadPartitionExchange();
  int exchange_multipart_table_partitions(const uint64_t tenant_id,
                                          ObDDLSQLTransaction &trans,
                                          share::schema::ObSchemaGetterGuard &schema_guard,
                                          const share::schema::ObTableSchema &base_table_schema,
                                          const share::schema::ObTableSchema &inc_table_schema,
                                          const common::ObIArray<common::ObTabletID> &base_table_tablet_ids,
                                          const common::ObIArray<common::ObTabletID> &inc_table_tablet_ids);
private:
  virtual int check_table_conditions_in_common_(const share::schema::ObTableSchema &base_table_schema,
                                                const share::schema::ObTableSchema &inc_table_schema,
                                                const bool is_oracle_mode) override;
  int check_multipart_exchange_conditions(share::schema::ObSchemaGetterGuard &schema_guard,
                                          const share::schema::ObTableSchema &base_table_schema,
                                          const share::schema::ObTableSchema &inc_table_schema,
                                          const common::ObIArray<common::ObTabletID> &base_tablet_ids,
                                          const common::ObIArray<common::ObTabletID> &inc_tablet_ids,
                                          const bool is_oracle_mode);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDirectLoadPartitionExchange);
};
} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_DIRECT_LOAD_PARTITION_EXCHANGE_H_