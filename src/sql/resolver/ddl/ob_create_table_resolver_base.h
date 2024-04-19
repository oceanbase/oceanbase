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

#ifndef _OB_CREATE_TABLE_RESOLVER_BASE_H
#define _OB_CREATE_TABLE_RESOLVER_BASE_H 1
#include "sql/resolver/ddl/ob_create_table_stmt.h"
#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "lib/container/ob_se_array.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_struct.h"
#include "share/schema/ob_column_schema.h"
#include "share/config/ob_server_config.h"

namespace oceanbase
{
namespace sql
{

class ObCreateTableResolverBase: public ObDDLResolver
{
public:
  explicit ObCreateTableResolverBase(ObResolverParams &params);
  virtual ~ObCreateTableResolverBase();

protected:
  int resolve_auto_partition(const ParseNode *partition_node);
  //resolve partitoin option only used in ObCreateTableResolver now.
  int resolve_partition_option(ParseNode *node,
                               share::schema::ObTableSchema &table_schema,
                               const bool is_partition_option_node_with_opt);
  int set_table_option_to_schema(share::schema::ObTableSchema &table_schema);
  int add_primary_key_part(const ObString &column_name,
                           ObTableSchema &table_schema,
                           const int64_t cur_rowkey_size,
                           int64_t &pk_data_length,
                           ObColumnSchemaV2 *&col);

};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_CREATE_TABLE_RESOLVER_BASE_H */
