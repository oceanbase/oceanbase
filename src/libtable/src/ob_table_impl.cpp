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

#define USING_LOG_PREFIX CLIENT
#include "ob_table_impl.h"
#include "lib/string/ob_sql_string.h"
#include "common/data_buffer.h"
#include "share/ob_dml_sql_splicer.h"
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::table;

ObTableImpl::ObTableImpl()
    :inited_(false),
     client_(NULL),
     table_name_(),
     sql_client_(NULL),
     entity_factory_(&default_entity_factory_)
{}

ObTableImpl::~ObTableImpl()
{}

int ObTableImpl::init(ObTableServiceClient &client, const ObString &table_name)
{
  int ret = OB_SUCCESS;
  ObDataBuffer buff(table_name_buf_, ARRAYSIZEOF(table_name_buf_));
  if (inited_) {
    ret = OB_INIT_TWICE;
  } else if (OB_FAIL(client.get_rowkey_columns(table_name, rowkey_columns_))) {
    LOG_WARN("failed to get rowkey columns for table", K(ret), K(table_name));
  } else if (OB_FAIL(ob_write_string(buff, table_name, table_name_))) {
    LOG_WARN("failed to store table name", K(ret), K(table_name));
  } else {
    client_ = &client;
    sql_client_ = &client.get_user_sql_client();
    LOG_DEBUG("init table succ", K_(table_name), K_(rowkey_columns));
    inited_ = true;
  }
  return ret;
}

void ObTableImpl::set_entity_factory(ObITableEntityFactory &entity_factory)
{
  entity_factory_ = &entity_factory;
}

void ObTableImpl::reuse_allocator()
{
  if (NULL != entity_factory_) {
    entity_factory_->free_and_reuse();
  }
  alloc_.reuse();
}

int ObTableImpl::generate_sql(const ObTableOperation &table_operation, ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  switch (table_operation.type()) {
    case ObTableOperationType::INSERT:
      ret = generate_insert(table_operation, sql);
      break;
    case ObTableOperationType::GET:
      ret = generate_get(table_operation, sql);
      break;
    case ObTableOperationType::DEL:
      ret = generate_del(table_operation, sql);
      break;
    case ObTableOperationType::UPDATE:
      ret = generate_update(table_operation, sql);
      break;
    case ObTableOperationType::INSERT_OR_UPDATE:
      ret = generate_insert_or_update(table_operation, sql);
      break;
    case ObTableOperationType::REPLACE:
      ret = generate_replace(table_operation, sql);
      break;
    default:
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("invalid table operation type", K(ret), K(table_operation));
      break;
  }
  return ret;
}

int ObTableImpl::fill_rowkey_pairs(const ObTableOperation &operation, share::ObDMLSqlSplicer &splicer)
{
  int ret = OB_SUCCESS;
  const ObITableEntity &entity = operation.entity();
  int64_t N = rowkey_columns_.count();
  if (N != entity.get_rowkey_size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("rowkey column count not match with the table schema", K(ret),
             "rowkey_columns", entity.get_rowkey_size(),
             "schema_columns", N);
  } else {
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      ObString cname_str;
      char* cname = NULL;
      ObObj value;
      if (OB_FAIL(rowkey_columns_.get_string(i, cname_str))) {
        LOG_WARN("failed to get string from strings", K(ret), K(i));
      } else if (OB_FAIL(ob_dup_cstring(alloc_, cname_str, cname))) { // @todo optimize
        LOG_WARN("failed to dup cstring", K(ret));
      } else if (OB_FAIL(entity.get_rowkey_value(i, value))) {
        LOG_WARN("failed to get rowkey value", K(ret), K(i));
      } else if (OB_FAIL(splicer.add_pk_column(cname, value))) {
        LOG_WARN("failed to add column", K(ret), K(i));
      }
    } // end for
  }
  return ret;
}

int ObTableImpl::fill_property_pairs(const ObTableOperation &operation, share::ObDMLSqlSplicer &splicer)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  if (OB_FAIL(operation.entity().get_properties(properties))) {  // @todo optimize, use iterator
    LOG_WARN("failed to get properties", K(ret));
  } else {
    int64_t N = properties.count();
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      const std::pair<ObString, ObObj> &prop = properties.at(i);
      char* cname = NULL;
      if (OB_FAIL(ob_dup_cstring(alloc_, prop.first, cname))) { // @todo optimize
        LOG_WARN("failed to dup cstring", K(ret));
      } else if (OB_FAIL(splicer.add_column(cname, prop.second))) {
        LOG_WARN("failed to add column", K(ret), K(i), "prop", prop.first);
      }
    } // end for
  }
  return ret;
}

int ObTableImpl::fill_kv_pairs(const ObTableOperation &operation, share::ObDMLSqlSplicer &splicer)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(fill_rowkey_pairs(operation, splicer))) {
    LOG_WARN("failed to fill rowkey", K(ret));
  } else if (OB_FAIL(fill_property_pairs(operation, splicer))) {
    LOG_WARN("failed to fill rowkey", K(ret));
  } else {}
  return ret;
}

int ObTableImpl::generate_insert(const ObTableOperation &operation, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_kv_pairs(operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else {
    char* tname = NULL;
    if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) { // @todo optimize
      LOG_WARN("failed to dup cstring", K(ret));
    } else if (OB_FAIL(dml.splice_insert_sql(tname, sql))) {
      LOG_WARN("splice sql failed", K(ret));
    }
  }
  return ret;
}

int ObTableImpl::generate_del(const ObTableOperation &operation, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_rowkey_pairs(operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else {
    char* tname = NULL;
    if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) {
      LOG_WARN("failed to dup cstring", K(ret));
    } else if (OB_FAIL(dml.splice_delete_sql(tname, sql))) {
      LOG_WARN("splice sql failed", K(ret));
    }
  }
  return ret;
}

int ObTableImpl::generate_insert_or_update(const ObTableOperation &operation, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_kv_pairs(operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else {
    char* tname = NULL;
    if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) {
      LOG_WARN("failed to dup cstring", K(ret));
    } else if (OB_FAIL(dml.splice_insert_update_sql(tname, sql))) {
      LOG_WARN("splice sql failed", K(ret));
    }
  }
  return ret;
}

int ObTableImpl::generate_update(const ObTableOperation &operation, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_kv_pairs(operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else {
    char* tname = NULL;
    if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) {
      LOG_WARN("failed to dup cstring", K(ret));
    } else if (OB_FAIL(dml.splice_update_sql(tname, sql))) {
      LOG_WARN("splice sql failed", K(ret));
    }
  }
  return ret;
}

int ObTableImpl::generate_replace(const ObTableOperation &operation, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  if (OB_FAIL(fill_kv_pairs(operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else {
    char* tname = NULL;
    if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) {
      LOG_WARN("failed to dup cstring", K(ret));
    } else if (OB_FAIL(dml.splice_replace_sql(tname, sql))) {
      LOG_WARN("splice sql failed", K(ret));
    }
  }
  return ret;
}

int ObTableImpl::generate_get(const ObTableOperation &operation, common::ObSqlString &sql)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer splicer;
  if (OB_FAIL(sql.assign_fmt("SELECT "))) {
    LOG_WARN("failed to assign sql", K(ret));
  }
  // append columns to project
  else if (OB_FAIL(fill_property_pairs(operation, splicer))) {
    LOG_WARN("failed to fill rowkey", K(ret));
  } else if (OB_FAIL(splicer.splice_column_names(sql))) {
    LOG_WARN("failed to fill project columns", K(ret));
  } else {
    splicer.reset();
    if (OB_FAIL(sql.append_fmt(" FROM %.*s WHERE ", table_name_.length(), table_name_.ptr()))) {
      LOG_WARN("failed to assign sql", K(ret));
    } else if (OB_FAIL(fill_rowkey_pairs(operation, splicer))) {
      LOG_WARN("failed to fill rowkey", K(ret));
    } else if (OB_FAIL(splicer.splice_predicates(sql))) {
      LOG_WARN("failed to fill predicates", K(ret));
    }
  }
  return ret;
}

int ObTableImpl::fill_get_result(const ObTableOperation &operation, sqlclient::ObMySQLResult &sql_result,
                                 ObTableOperationResult &table_result)
{
  int ret = OB_SUCCESS;
  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  ObITableEntity *result_entity = NULL;
  if (OB_FAIL(sql_result.next())) {
    if (OB_ITER_END == ret) {
      ret = OB_EMPTY_RESULT;
      LOG_DEBUG("result set is empty", K(ret));
    } else {
      LOG_WARN("failed to iterate result", K(ret));
    }
  } else if (OB_FAIL(operation.entity().get_properties(properties))) {  // @todo optimize, use iterator
    LOG_WARN("failed to get properties", K(ret));
  } else if (NULL == (result_entity = entity_factory_->alloc())) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("no memory for result entity", K(ret));
  } else {
    int64_t N = properties.count();
    ObObj obj;
    for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
    {
      if (OB_FAIL(sql_result.get_obj(i, obj))) {
        LOG_WARN("failed to get obj", K(ret), K(i));
      } else if (OB_FAIL(result_entity->set_property(properties.at(i).first, obj))) {
        LOG_WARN("failed to set property", K(ret));
      }
    } // end for
    if (OB_SUCC(ret)) {
      result_entity->set_rowkey(operation.entity());
      table_result.set_entity(*result_entity);
      table_result.set_type(operation.type());
    }
  }
  return ret;
}

int ObTableImpl::execute(const ObTableOperation &table_operation, const ObTableRequestOptions &request_options, ObTableOperationResult &result)
{
  int ret = OB_SUCCESS;
  UNUSED(request_options);
  ObSqlString sql;
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else {
    reuse_allocator();
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_FAIL(generate_sql(table_operation, sql))) {
      LOG_WARN("failed to generate sql", K(ret));
    } else {
      if (ObTableOperationType::GET == table_operation.type()) {
        SMART_VAR(ObMySQLProxy::MySQLResult, res) {
          sqlclient::ObMySQLResult *sql_result = NULL;
          if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
            LOG_WARN("execute sql failed", K(ret), K(sql));
          } else if (OB_ISNULL(sql_result = res.get_result())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("result is null", K(ret));
          } else if (OB_FAIL(fill_get_result(table_operation, *sql_result, result))) {
            LOG_WARN("failed to fill result", K(ret));
          } else {
            LOG_DEBUG("execute sql query succ", K(sql));
          }
        }
        result.set_errno(ret);
      } else {
        if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
          LOG_WARN("execute sql failed", K(sql), K(ret));
        } else {
          LOG_DEBUG("execute sql dml succ", K(sql));
        }
        result.set_type(table_operation.type());
        result.set_errno(ret);
      }
    }
  }
  return ret;
}

int ObTableImpl::batch_execute(const ObTableBatchOperation &batch_operation, const ObTableRequestOptions &request_options, ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  UNUSED(request_options);
  if (!inited_) {
    ret = OB_NOT_INIT;
    LOG_WARN("not init", K(ret));
  } else if (batch_operation.count() <= 0) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("no operation in the batch", K(ret));
  } else if (batch_operation.count() == 1) {
    // single operation
    ObTableOperationResult table_result;
    if (OB_FAIL(this->execute(batch_operation.at(0), request_options, table_result))) {
    } else if (OB_FAIL(result.push_back(table_result))) {
      LOG_WARN("failed to push back result", K(ret));
    }
  } else {
    reuse_allocator();
    // real batch operation
    if (batch_operation.is_readonly()) {
      if (batch_operation.is_same_properties_names()) {
        ret = batch_execute_multi_get(batch_operation, result);
      } else {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("batch get operation that retrieve different column set has not been supported yet");
      }
    } else if (batch_operation.is_same_type()) {
      switch(batch_operation.at(0).type()) {
        case ObTableOperationType::INSERT:
          ret = batch_execute_insert(batch_operation, result);
          break;
        case ObTableOperationType::DEL:
          ret = batch_execute_del(batch_operation, result);
          break;
        case ObTableOperationType::UPDATE:
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("batch update operation has not been supported yet");
          break;
        case ObTableOperationType::INSERT_OR_UPDATE:
          ret = batch_execute_insert_or_update(batch_operation, result);
          break;
        case ObTableOperationType::REPLACE:
          ret = batch_execute_replace(batch_operation, result);
          break;
        default:
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected operation type", "type", batch_operation.at(0).type());
          break;
      }
    } else {
      // @todo
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("batch operation with different operation type has not been supported yet");
    }
  }
  return ret;
}

class ObTableImpl::EntityRowkeyAdaptor
{
public:
  EntityRowkeyAdaptor()
      :entity_(NULL)
  {}
  EntityRowkeyAdaptor(const ObITableEntity *entity)
      :entity_(entity)
  {}
  int64_t hash() const { return entity_->hash_rowkey(); }
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator== (const EntityRowkeyAdaptor &other) const
  {
    bool bret = (entity_->get_rowkey_size() == other.entity_->get_rowkey_size());
    if (bret) {
      int64_t N = entity_->get_rowkey_size();
      ObObj value1;
      ObObj value2;
      for (int64_t i = 0; i < N && bret; ++i) {
        (void)entity_->get_rowkey_value(i, value1);
        (void)other.entity_->get_rowkey_value(i, value2);
        bret = (value1 == value2);
      }
    }
    return bret;
  }
private:
  const ObITableEntity *entity_;
};

int ObTableImpl::fill_multi_get_result(const ObTableBatchOperation &batch_operation, sqlclient::ObMySQLResult &sql_result,
                                       ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  hash::ObHashMap<EntityRowkeyAdaptor, ObITableEntity*> rowkey_entity_map;
  ObSEArray<std::pair<ObString, ObObj>, 8> properties;
  if (OB_FAIL(rowkey_entity_map.create(512, ObModIds::TABLE_CLIENT))) {
    LOG_WARN("failed to init hashmap", K(ret));
  } else if (OB_FAIL(batch_operation.at(0).entity().get_properties(properties))) {  // @todo optimize, use iterator
    LOG_WARN("failed to get properties", K(ret));
  } else {
    const int64_t rowkey_col_num = rowkey_columns_.count();
    while (OB_SUCC(ret) && OB_SUCC(sql_result.next())) {
      ObITableEntity *result_entity = NULL;
      if (NULL == (result_entity = entity_factory_->alloc())) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("no memory for result entity", K(ret));
      } else {
        ObObj obj;
        for (int64_t i = 0; OB_SUCCESS == ret && i < rowkey_col_num; ++i) {
          if (OB_FAIL(sql_result.get_obj(i, obj))) {
            LOG_WARN("failed to get obj", K(ret), K(i));
          } else if (OB_FAIL(result_entity->add_rowkey_value(obj))) {
            LOG_WARN("failed to add rowkey value", K(ret), K(obj));
          } else {
            LOG_DEBUG("rowkey cell", K(i), K(obj));
          }
        }
        int64_t prop_num = properties.count();
        for (int64_t i = 0; OB_SUCCESS == ret && i < prop_num; ++i)
        {
          if (OB_FAIL(sql_result.get_obj(i+rowkey_col_num, obj))) {
            LOG_WARN("failed to get obj", K(ret), K(i));
          } else if (OB_FAIL(result_entity->set_property(properties.at(i).first, obj))) {
            LOG_WARN("failed to set property", K(ret));
          }
        } // end for
        if (OB_SUCC(ret)) {
          EntityRowkeyAdaptor rowkey_wrapper(result_entity);
          if (OB_FAIL(rowkey_entity_map.set_refactored(rowkey_wrapper, result_entity))) {
            LOG_WARN("failed to insert into map", K(ret));
          }
        }
      }
    }  // end while
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      // fill result with the same order of batch_operation
      ObITableEntity *result_entity = NULL;
      const int64_t N = batch_operation.count();
      for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
      {
        ObTableOperationResult table_result;
        table_result.set_type(ObTableOperationType::GET);
        EntityRowkeyAdaptor rowkey_wrapper(&batch_operation.at(i).entity());
        if (OB_FAIL(rowkey_entity_map.get_refactored(rowkey_wrapper, result_entity))) {
          if (OB_HASH_NOT_EXIST == ret) {
            LOG_DEBUG("row not exists", "entity", batch_operation.at(i).entity());
            table_result.set_errno(OB_ENTRY_NOT_EXIST);
            ret = OB_SUCCESS;
          } else {
            LOG_WARN("failed to get from map", K(ret), "entity", batch_operation.at(i).entity());
          }
        } else {
          table_result.set_errno(OB_SUCCESS);
          table_result.set_entity(*result_entity);
        }
        if (OB_SUCC(ret)) {
          if (OB_FAIL(result.push_back(table_result))) {
            LOG_WARN("failed to push back result", K(ret));
          }
        }
      } // end for
    }
  }
  return ret;
}

int ObTableImpl::batch_execute_multi_get(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer splicer;
  ObSqlString sql;
  UNUSED(result);
  SMART_VAR(ObMySQLProxy::MySQLResult, res) {
    sqlclient::ObMySQLResult *sql_result = NULL;
    if (OB_FAIL(sql.assign_fmt("SELECT "))) {
      LOG_WARN("failed to assign sql", K(ret));
    }
    // append columns to project
    else if (OB_FAIL(fill_kv_pairs(batch_operation.at(0), splicer))) {
      LOG_WARN("failed to fill rowkey", K(ret));
    } else if (OB_FAIL(splicer.splice_column_names(sql))) {
      LOG_WARN("failed to fill project columns", K(ret));
    } else {
      splicer.reset();
      if (OB_FAIL(sql.append_fmt(" FROM %.*s WHERE ", table_name_.length(), table_name_.ptr()))) {
        LOG_WARN("failed to assign sql", K(ret));
      } else if (OB_FAIL(batch_fill_rowkey_pairs(batch_operation, splicer))) {
        LOG_WARN("failed to fill rowkey", K(ret));
      } else if (OB_FAIL(splicer.splice_batch_predicates_sql(sql))) {
        LOG_WARN("failed to fill predicates", K(ret));
      } else if (OB_FAIL(sql_client_->read(res, sql.ptr()))) {
        LOG_WARN("execute sql failed", K(ret), K(sql));
      } else if (OB_ISNULL(sql_result = res.get_result())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("result is null", K(ret));
      } else if (OB_FAIL(fill_multi_get_result(batch_operation, *sql_result, result))) {
        LOG_WARN("failed to fill result", K(ret));
      } else {
        LOG_DEBUG("execute sql query succ", K(sql));
      }
    }
  }
  return ret;
}

int ObTableImpl::batch_fill_kv_pairs(const ObTableBatchOperation &batch_operation, share::ObDMLSqlSplicer &splicer)
{
  int ret = OB_SUCCESS;
  const int64_t N = batch_operation.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &operation = batch_operation.at(i);
    if (OB_FAIL(fill_kv_pairs(operation, splicer))) {
      LOG_WARN("failed to fill kv pairs", K(ret));
    } else if (OB_FAIL(splicer.finish_row())) {
      LOG_WARN("failed to finish row", K(ret));
    }
  } // end for
  return ret;
}

int ObTableImpl::batch_execute_insert(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObTableOperationResult table_result;
  int64_t affected_rows = 0;
  char* tname = NULL;
  if (OB_FAIL(batch_fill_kv_pairs(batch_operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) { // @todo optimize
    LOG_WARN("failed to dup cstring", K(ret));
  } else if (OB_FAIL(dml.splice_batch_insert_sql(tname, sql))) {
    LOG_WARN("splice sql failed", K(ret));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else {
    LOG_DEBUG("execute sql dml succ", K(sql));
  }
  table_result.set_type(ObTableOperationType::INSERT);
  table_result.set_errno(ret);
  int64_t tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = result.push_back(table_result))) {
    LOG_WARN("failed to push back result", K(tmp_ret));
  }
  return ret;
}

int ObTableImpl::batch_fill_rowkey_pairs(const ObTableBatchOperation &batch_operation, share::ObDMLSqlSplicer &splicer)
{
  int ret = OB_SUCCESS;
  const int64_t N = batch_operation.count();
  for (int64_t i = 0; OB_SUCCESS == ret && i < N; ++i)
  {
    const ObTableOperation &operation = batch_operation.at(i);
    if (OB_FAIL(fill_rowkey_pairs(operation, splicer))) {
      LOG_WARN("failed to fill kv pairs", K(ret));
    } else if (OB_FAIL(splicer.finish_row())) {
      LOG_WARN("failed to finish row", K(ret));
    }
  } // end for
  return ret;
}

int ObTableImpl::batch_execute_del(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObTableOperationResult table_result;
  int64_t affected_rows = 0;
  char* tname = NULL;
  if (OB_FAIL(batch_fill_rowkey_pairs(batch_operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) { // @todo optimize
    LOG_WARN("failed to dup cstring", K(ret));
  } else if (OB_FAIL(dml.splice_batch_delete_sql(tname, sql))) {
    LOG_WARN("splice sql failed", K(ret));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else {
    LOG_DEBUG("execute sql dml succ", K(sql));
  }
  table_result.set_type(ObTableOperationType::DEL);
  table_result.set_errno(ret);
  int64_t tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = result.push_back(table_result))) {
    LOG_WARN("failed to push back result", K(tmp_ret));
  }
  return ret;
}

int ObTableImpl::batch_execute_insert_or_update(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObTableOperationResult table_result;
  int64_t affected_rows = 0;
  char* tname = NULL;
  if (OB_FAIL(batch_fill_kv_pairs(batch_operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) { // @todo optimize
    LOG_WARN("failed to dup cstring", K(ret));
  } else if (OB_FAIL(dml.splice_batch_insert_update_sql(tname, sql))) {
    LOG_WARN("splice sql failed", K(ret));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else {
    LOG_DEBUG("execute sql dml succ", K(sql));
  }
  table_result.set_type(ObTableOperationType::INSERT_OR_UPDATE);
  table_result.set_errno(ret);
  int64_t tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = result.push_back(table_result))) {
    LOG_WARN("failed to push back result", K(tmp_ret));
  }
  return ret;
}

int ObTableImpl::batch_execute_replace(const ObTableBatchOperation &batch_operation, ObITableBatchOperationResult &result)
{
  int ret = OB_SUCCESS;
  share::ObDMLSqlSplicer dml;
  ObSqlString sql;
  ObTableOperationResult table_result;
  int64_t affected_rows = 0;
  char* tname = NULL;
  if (OB_FAIL(batch_fill_kv_pairs(batch_operation, dml))) {
    LOG_WARN("failed to fill kv pairs", K(ret));
  } else if (OB_FAIL(ob_dup_cstring(alloc_, table_name_, tname))) { // @todo optimize
    LOG_WARN("failed to dup cstring", K(ret));
  } else if (OB_FAIL(dml.splice_batch_replace_sql(tname, sql))) {
    LOG_WARN("splice sql failed", K(ret));
  } else if (OB_FAIL(sql_client_->write(sql.ptr(), affected_rows))) {
    LOG_WARN("execute sql failed", K(sql), K(ret));
  } else {
    LOG_DEBUG("execute sql dml succ", K(sql));
  }
  table_result.set_type(ObTableOperationType::REPLACE);
  table_result.set_errno(ret);
  int64_t tmp_ret = OB_SUCCESS;
  if (OB_SUCCESS != (tmp_ret = result.push_back(table_result))) {
    LOG_WARN("failed to push back result", K(tmp_ret));
  }
  return ret;
}

int ObTableImpl::execute_query(const ObTableQuery &query, const ObTableRequestOptions &request_options, ObTableEntityIterator *&result)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(query);
  UNUSED(result);
  UNUSED(request_options);
  LOG_WARN("not implement", K(ret));
  return ret;
}

int ObTableImpl::execute_query_and_mutate(const ObTableQueryAndMutate &query_and_mutate, const ObTableRequestOptions &request_options, ObTableQueryAndMutateResult &result)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(query_and_mutate);
  UNUSED(request_options);
  UNUSED(result);
  LOG_WARN("not implement", K(ret));
  return ret;
}

int ObTableImpl::query_start(const ObTableQuery& query, const ObTableRequestOptions &request_options, ObTableQuerySyncResult *&result)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(query);
  UNUSED(request_options);
  UNUSED(result);
  return ret;
}

int ObTableImpl::query_next(const ObTableRequestOptions &request_options, ObTableQuerySyncResult *&result)
{
  int ret = OB_NOT_IMPLEMENT;
  UNUSED(request_options);
  UNUSED(result);
  return ret;
}
