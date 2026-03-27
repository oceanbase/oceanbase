/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_ROOTSERVER_OB_LOB_DATA_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_LOB_DATA_BUILDER_H_
#include "share/ob_ddl_task_executor.h"
#include "share/schema/ob_schema_struct.h"
#include "rootserver/ob_ddl_service.h"

namespace oceanbase
{
namespace rootserver
{
class ObDDLService;
class ObLobPieceBuilder
{
public:
  explicit ObLobPieceBuilder(ObDDLService &ddl_service);
  virtual ~ObLobPieceBuilder();

  // won't fetch new table id if specified_table_id is valid
  int generate_aux_lob_piece_schema(
      share::schema::ObSchemaService *schema_service,
      const share::schema::ObTableSchema &data_schema,
      const uint64_t specified_table_id,
      share::schema::ObTableSchema &aux_vp_table_schema,
      bool generate_id);

  static int generate_lob_piece_table_name(
      const uint64_t new_table_id,
      char *buf,
      const int64_t buf_size,
      int64_t &pos);
private:
  int generate_schema(const share::schema::ObTableSchema &data_schema,
                      share::schema::ObTableSchema &aux_vp_table_schema);
  
  int set_basic_infos(const share::schema::ObTableSchema &data_schema,
                      share::schema::ObTableSchema &aux_vp_table_schema);

  int set_lob_table_column_store_if_need(share::schema::ObTableSchema &table_schema);
private:
  ObDDLService &ddl_service_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLobPieceBuilder);
};
}//end namespace rootserver
}//end namespace oceanbase

#endif //OCEANBASE_ROOTSERVER_OB_LOB_DATA_BUILDER_H_
