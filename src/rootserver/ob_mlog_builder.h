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

#ifndef OCEANBASE_ROOTSERVER_OB_MLOG_BUILDER_H_
#define OCEANBASE_ROOTSERVER_OB_MLOG_BUILDER_H_

#include "share/schema/ob_schema_struct.h"
#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace rootserver
{
class ObDDLService;
class ObMLogBuilder
{
public:
  explicit ObMLogBuilder(ObDDLService &ddl_service);
  virtual ~ObMLogBuilder();
  int init();
  int create_mlog(share::schema::ObSchemaGetterGuard &schema_guard,
                  const obrpc::ObCreateMLogArg &create_mlog_arg,
                  obrpc::ObCreateMLogRes &create_mlog_res);
  int do_create_mlog(share::schema::ObSchemaGetterGuard &schema_guard,
                     const obrpc::ObCreateMLogArg &create_mlog_arg,
                     const share::schema::ObTableSchema &table_schema,
                     obrpc::ObCreateMLogRes &create_mlog_res);
  int generate_mlog_schema(share::schema::ObSchemaGetterGuard &schema_guard,
                           const obrpc::ObCreateMLogArg &create_mlog_arg,
                           const share::schema::ObTableSchema &base_table_schema,
                           share::schema::ObTableSchema &mlog_schema);

private:
  class MLogColumnUtils
  {
  public:
    MLogColumnUtils(): mlog_table_column_array_() {}
    ~MLogColumnUtils() {}
    int check_column_type(const ObColumnSchemaV2 &column_schema);
    int add_base_table_columns(const obrpc::ObCreateMLogArg &create_mlog_arg,
                               common::ObRowDesc &row_desc,
                               const share::schema::ObTableSchema &base_table_schema);
    int add_base_table_pk_columns(common::ObRowDesc &row_desc,
                                  const share::schema::ObTableSchema &base_table_schema);
    int add_base_table_part_key_columns(common::ObRowDesc &row_desc,
                                        const share::schema::ObTableSchema &base_table_schema);
    int add_special_columns(share::schema::ObTableSchema &mlog_schema);
    int construct_mlog_table_columns(share::schema::ObTableSchema &mlog_schema);
  private:
    int add_pk_column(share::schema::ObTableSchema &mlog_schema);
    int add_dmltype_column();
    int add_old_new_column();
    int add_base_table_part_key_columns(const common::ObPartitionKeyInfo &part_key_info,
                                        common::ObRowDesc &row_desc,
                                        const share::schema::ObTableSchema &base_table_schema);
  public:
    ObArray<ObColumnSchemaV2> mlog_table_column_array_;
  };

private:
  int set_basic_infos(share::schema::ObSchemaGetterGuard &schema_guard,
                      const obrpc::ObCreateMLogArg &create_mlog_arg,
                      const share::schema::ObTableSchema &base_table_schema,
                      share::schema::ObTableSchema &mlog_schema);
  int set_table_columns(const obrpc::ObCreateMLogArg &create_mlog_arg,
                        const share::schema::ObTableSchema &base_table_schema,
                        share::schema::ObTableSchema &mlog_schema);
  int set_table_options(const obrpc::ObCreateMLogArg &create_mlog_arg,
                        const share::schema::ObTableSchema &base_table_schema,
                        share::schema::ObTableSchema &mlog_schema);

private:
  ObDDLService &ddl_service_;
  MLogColumnUtils mlog_column_utils_;
  bool is_inited_;
  DISALLOW_COPY_AND_ASSIGN(ObMLogBuilder);
};
} // namespace rootserver
} // namespace oceanbase
#endif  // OCEANBASE_ROOTSERVER_OB_MLOG_BUILDER_H_