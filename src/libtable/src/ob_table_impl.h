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

#ifndef _OB_TABLE_IMPL_H
#define _OB_TABLE_IMPL_H 1
#include "ob_table.h"
#include "ob_table_service_client.h"
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_strings.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "lib/mysqlclient/ob_single_mysql_connection_pool.h"

namespace oceanbase
{
namespace common
{
class ObSqlString;
} // end namespace common
namespace share
{
class ObDMLSqlSplicer;
} // end namespace share

namespace table
{
class ObTableServiceClientImpl;
// An implementation of ObTable using SQL
class ObTableImpl: public ObTable
{
public:
  int init(ObTableServiceClient &client, const ObString &table_name) override;
  void set_entity_factory(ObITableEntityFactory &entity_factory) override;
  /// executes an operation on a table
  int execute(const ObTableOperation &table_operation, const ObTableRequestOptions &request_options, ObTableOperationResult &result) override;
  /// executes a batch operation on a table as an atomic operation
  int batch_execute(const ObTableBatchOperation &batch_operation, const ObTableRequestOptions &request_options, ObITableBatchOperationResult &result) override;
  /// executes a query on a table
  int execute_query(const ObTableQuery &query, const ObTableRequestOptions &request_options, ObTableEntityIterator *&result) override;
  int execute_query_and_mutate(const ObTableQueryAndMutate &query_and_mutate, const ObTableRequestOptions &request_options, ObTableQueryAndMutateResult &result) override;
  /// execute a sync query on a table
  int query_start(const ObTableQuery& query, const ObTableRequestOptions &request_options, ObTableQueryAsyncResult *&result) override;
  int query_next(const ObTableRequestOptions &request_options, ObTableQueryAsyncResult *&result) override;
private:
  friend class ObTableServiceClientImpl;
  ObTableImpl();
  virtual ~ObTableImpl();

  void reuse_allocator();
  int generate_sql(const ObTableOperation &operation, common::ObSqlString &sql);
  int fill_rowkey_pairs(const ObTableOperation &operation, share::ObDMLSqlSplicer &splicer);
  int fill_property_pairs(const ObTableOperation &operation, share::ObDMLSqlSplicer &splicer);
  int fill_kv_pairs(const ObTableOperation &operation, share::ObDMLSqlSplicer &splicer);
  int generate_insert(const ObTableOperation &operation, common::ObSqlString &sql);
  int generate_del(const ObTableOperation &operation, common::ObSqlString &sql);
  int generate_insert_or_update(const ObTableOperation &operation, common::ObSqlString &sql);
  int generate_update(const ObTableOperation &operation, common::ObSqlString &sql);
  int generate_replace(const ObTableOperation &operation, common::ObSqlString &sql);
  int generate_get(const ObTableOperation &operation, common::ObSqlString &sql);
  int fill_get_result(const ObTableOperation &table_operation, common::sqlclient::ObMySQLResult &sql_result,
                      ObTableOperationResult &table_result);
  // functions for batch execute
  int batch_fill_kv_pairs(const ObTableBatchOperation &operation, share::ObDMLSqlSplicer &splicer);
  int batch_fill_rowkey_pairs(const ObTableBatchOperation &batch_operation, share::ObDMLSqlSplicer &splicer);
  int fill_multi_get_result(const ObTableBatchOperation &operation, common::sqlclient::ObMySQLResult &sql_result,
                            ObITableBatchOperationResult &table_result);
  int batch_execute_multi_get(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result);
  int batch_execute_insert(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result);
  int batch_execute_del(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result);
  int batch_execute_insert_or_update(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result);
  int batch_execute_replace(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result);
  class EntityRowkeyAdaptor;
  // disallow copy and assign
  DISALLOW_COPY_AND_ASSIGN(ObTableImpl);
private:
  bool inited_;
  ObTableServiceClient *client_;
  char table_name_buf_[common::OB_MAX_TABLE_NAME_LENGTH];
  ObString table_name_;
  common::ObMySQLProxy *sql_client_;
  common::ObStrings rowkey_columns_;
  common::ObArenaAllocator alloc_;
  ObITableEntityFactory *entity_factory_;
  ObTableEntityFactory<ObTableEntity> default_entity_factory_;
};

} // end namespace table
} // end namespace oceanbase


#endif /* _OB_TABLE_IMPL_H */
