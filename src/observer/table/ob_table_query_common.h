/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_OBSERVER_OB_TABLE_QUERY_COMMON_H_
#define OCEANBASE_OBSERVER_OB_TABLE_QUERY_COMMON_H_

#include "ob_table_filter.h"
#include "ob_table_context.h"

namespace oceanbase
{
namespace table
{

class ObTableQueryUtils
{
public:
  static int generate_query_result_iterator(ObIAllocator &allocator,
                                            const ObTableQuery &query,
                                            bool is_hkv,
                                            ObTableQueryResult &one_result,
                                            const ObTableCtx &tb_ctx,
                                            ObTableQueryResultIterator *&result_iter);

  template<typename ResultType>
  static int generate_htable_result_iterator(ObIAllocator &allocator,
                                             const ObTableQuery &query,
                                             ResultType &one_result,
                                             const ObTableCtx &tb_ctx,
                                             ObTableQueryResultIterator *&result_iter);
  static void destroy_result_iterator(ObTableQueryResultIterator *&result_iter);
  static int get_rowkey_column_names(ObKvSchemaCacheGuard &schema_cache_guard, ObIArray<ObString> &names);
  static int get_full_column_names(ObKvSchemaCacheGuard &schema_cache_guard, ObIArray<ObString> &names);
  static int get_scan_row_interator(const ObTableCtx &tb_ctx, ObTableApiScanRowIterator *&scan_iter);

  static int get_table_schemas(ObMultiVersionSchemaService *schema_service,
                                ObSchemaGetterGuard& schema_guard,
                                const ObString &arg_table_name,
                                bool is_tablegroup_name,
                                uint64_t arg_tenant_id,
                                uint64_t arg_database_id,
                                common::ObIArray<const schema::ObSimpleTableSchemaV2*> &table_schemas);

private:
  static int check_htable_query_args(const ObTableQuery &query, const ObTableCtx &tb_ctx);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableQueryUtils);
};

} // end namespace table
} // end namespace oceanbase

#endif /* OCEANBASE_OBSERVER_OB_TABLE_QUERY_COMMON_H_ */