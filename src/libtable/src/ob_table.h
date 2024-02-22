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

#ifndef _OB_TABLE_H
#define _OB_TABLE_H 1
#include "share/table/ob_table.h"

namespace oceanbase
{
namespace table
{
class ObTableServiceClient;

/// Interface of a table
class ObTable
{
public:
  ObTable()
      :entity_type_(ObTableEntityType::ET_DYNAMIC)
  {}
  virtual ~ObTable() = default;
  virtual int init(ObTableServiceClient &client, const ObString &table_name) = 0;
  virtual void set_entity_factory(ObITableEntityFactory &entity_factory) = 0;
  void set_default_request_options(const ObTableRequestOptions &options);
  void set_entity_type(ObTableEntityType type) { entity_type_ = type; }
  /// executes an operation on a table
  int execute(const ObTableOperation &table_operation, ObTableOperationResult &result);
  virtual int execute(const ObTableOperation &table_operation, const ObTableRequestOptions &request_options, ObTableOperationResult &result) = 0;
  /// executes a batch operation on a table as an atomic operation
  int batch_execute(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result);
  virtual int batch_execute(const ObTableBatchOperation &batch_operation, const ObTableRequestOptions &request_options, ObITableBatchOperationResult &result) = 0;
  /// executes a query on a table
  int execute_query(const ObTableQuery &query, ObTableEntityIterator *&result);
  virtual int execute_query(const ObTableQuery &query, const ObTableRequestOptions &request_options, ObTableEntityIterator *&result) = 0;
  /// executes query_and_mutate on a table
  virtual int execute_query_and_mutate(const ObTableQueryAndMutate &query_and_mutate, const ObTableRequestOptions &request_options, ObTableQueryAndMutateResult &result) = 0;
  int execute_query_and_mutate(const ObTableQueryAndMutate &query_and_mutate, ObTableQueryAndMutateResult &result);
  /// executes a sync query on a table
  virtual int query_start(const ObTableQuery& query, const ObTableRequestOptions &request_options, ObTableQueryAsyncResult *&result) = 0;
  int query_start(const ObTableQuery& query, ObTableQueryAsyncResult *&result);
  virtual int query_next(const ObTableRequestOptions &request_options, ObTableQueryAsyncResult *&result) = 0;
  int query_next(ObTableQueryAsyncResult *&result);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTable);
protected:
  ObTableRequestOptions request_options_;
  ObTableEntityType entity_type_;
};

inline void ObTable::set_default_request_options(const ObTableRequestOptions &options)
{
  request_options_ = options;
}

inline int ObTable::execute(const ObTableOperation &table_operation, ObTableOperationResult &result)
{
  return execute(table_operation, request_options_, result);
}

inline int ObTable::batch_execute(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result)
{
  return batch_execute(batch_operation, request_options_, result);
}

inline int ObTable::execute_query(const ObTableQuery &query, ObTableEntityIterator *&result)
{
  return execute_query(query, request_options_, result);
}

inline int ObTable::execute_query_and_mutate(const ObTableQueryAndMutate &query_and_mutate, ObTableQueryAndMutateResult &result)
{
  return this->execute_query_and_mutate(query_and_mutate, request_options_, result);
}

inline int ObTable::query_start(const ObTableQuery &query, ObTableQueryAsyncResult *&result)
{
  return query_start(query, request_options_, result);
}

inline int ObTable::query_next(ObTableQueryAsyncResult *&result)
{
  return query_next(request_options_, result);
}

/**
 * @example ob_table_example.cpp
 * This is an example of how to use the ObTable class.
 *
 */
} // end namespace table
} // end namespace oceanbase

#endif /* _OB_TABLE_H */
