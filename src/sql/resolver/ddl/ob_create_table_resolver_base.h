/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  //resolve partition option only used in ObCreateTableResolver now.
  int resolve_partition_option(ParseNode *node,
                               share::schema::ObTableSchema &table_schema,
                               const bool is_partition_option_node_with_opt);
  int set_table_option_to_schema(share::schema::ObTableSchema &table_schema);
  int add_primary_key_part(const ObString &column_name,
                           ObTableSchema &table_schema,
                           const int64_t cur_rowkey_size,
                           int64_t &pk_data_length,
                           ObColumnSchemaV2 *&col,
                           const bool is_heap_table_clustering_key = false);

  int resolve_column_group_helper(const ParseNode *cg_node, ObTableSchema &table_schema);
  // check this type of table_schema should build column_group or not
  uint64_t gen_column_group_id();
  virtual int resolve_column_group(const ParseNode *cg_node) final;
  int resolve_table_organization(omt::ObTenantConfigGuard &tenant_config, ParseNode *node,
                                 bool &has_clustering_key, int64_t &clustering_key_index);
protected:
  uint64_t cur_column_group_id_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* _OB_CREATE_TABLE_RESOLVER_BASE_H */
