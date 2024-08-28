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
                                                const share::schema::ObPartitionLevel exchange_partition_level,
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