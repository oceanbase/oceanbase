/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_FTS_PARSER_RESOLVER_
#define OCEANBASE_FTS_PARSER_RESOLVER_

#include "sql/resolver/ddl/ob_ddl_resolver.h"
#include "storage/fts/ob_fts_parser_property.h"
#include "storage/fts/ob_fts_plugin_helper.h"
#include "share/schema/ob_schema_getter_guard.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObSchemaChecker;
class ObFTParserResolverHelper final
{
public:
  ObFTParserResolverHelper() = default;
  ~ObFTParserResolverHelper() = default;

  static int resolve_parser_properties(
      const common::ObString &index_database_name,
      const ParseNode &parse_tree,
      const uint64_t tenant_id,
      common::ObIAllocator &allocator,
      sql::ObSchemaChecker *schema_checker,
      common::ObString &parser_property);

  // Helper function to resolve dictionary table name and get table_id
  // If table name contains '.', extract database_name from table_name
  // If check_database_name is true, check it matches index_database_name (cross-database check)
  // If table name doesn't contain '.', use index_database_name as dictionary table database name
  // Returns table_id and full_table_name (database.table_name) through output parameters
  static int resolve_dict_table_name_and_id(const common::ObString &index_database_name,
                                            const common::ObString &table_name,
                                            const uint64_t tenant_id,
                                            share::schema::ObSchemaGetterGuard &schema_guard,
                                            common::ObIAllocator &allocator,
                                            const bool check_database_name,
                                            uint64_t &table_id,
                                            common::ObString &full_table_name);

private:
  static int resolve_fts_index_parser_properties(const common::ObString &index_database_name,
                                                 const ParseNode *node,
                                                 const uint64_t tenant_id,
                                                 storage::ObFTParserJsonProps &property,
                                                 common::ObIAllocator &allocator,
                                                 sql::ObSchemaChecker *schema_checker);

  // Helper function to resolve table configuration for parser properties
  // This function encapsulates the common logic for resolving stopword, dict, and quantifier tables
  static int resolve_table_config(
      const common::ObString &index_database_name,
      const ParseNode *node,
      const uint64_t tenant_id,
      const char *table_id_config_name,
      storage::ObFTParserJsonProps &property,
      common::ObIAllocator &allocator,
      sql::ObSchemaChecker &schema_checker);
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_FTS_PARSER_RESOLVER_ */
