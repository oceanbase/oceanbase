/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_VERTIAL_PARTITION_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_VERTIAL_PARTITION_BUILDER_H_
#include "share/ob_ddl_task_executor.h"
#include "share/schema/ob_schema_struct.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace rootserver
{
class ObDDLService;
class ObVertialPartitionBuilder
{
public:
  explicit ObVertialPartitionBuilder(ObDDLService &ddl_service);
  virtual ~ObVertialPartitionBuilder();

  int generate_aux_vp_table_schema(
      share::schema::ObSchemaService *schema_service,
      const obrpc::ObCreateVertialPartitionArg &vp_arg,
      const int64_t frozen_version,
      share::schema::ObTableSchema &data_schema,
      share::schema::ObTableSchema &aux_vp_table_schema);
  
  int set_primary_vp_table_options(const obrpc::ObCreateVertialPartitionArg&vp_arg,
                                   share::schema::ObTableSchema &data_schema);
  int generate_vp_table_name(
      const uint64_t new_table_id,
      char *buf,
      const int64_t buf_size,
      int64_t &pos);

private:
  int generate_schema(const obrpc::ObCreateVertialPartitionArg &vp_arg,
                      const int64_t frozen_version,
                      share::schema::ObTableSchema &data_schema,
                      share::schema::ObTableSchema &aux_vp_table_schema);
  
  int set_basic_infos(const int64_t frozen_version,
                      const share::schema::ObTableSchema &data_schema,
                      share::schema::ObTableSchema &aux_vp_table_schema);
  
  int set_aux_vp_table_columns(const obrpc::ObCreateVertialPartitionArg &vp_arg,
                               share::schema::ObTableSchema &data_schema,
                               share::schema::ObTableSchema &aux_vp_table_schema);

private:
  ObDDLService &ddl_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObVertialPartitionBuilder);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_VERTIAL_PARTITION_BUILDER_H_
