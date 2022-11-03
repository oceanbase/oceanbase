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

#include "libobtable.h"
using namespace oceanbase::table;
using namespace oceanbase::common;

void usage(const char* progname)
{
  fprintf(stderr, "Usage: %s <observer_host> <port> <tenant> <user> <password> <database> <table> <rpc_port>\n", progname);
  fprintf(stderr, "Example: ./table_example '100.88.11.96' 50803 sys root '' test t2 50802\n");
}

#define CHECK_RET(ret) \
  if (OB_SUCCESS != (ret)) {                    \
    fprintf(stderr, "error: %d at %d\n", ret, __LINE__);       \
    exit(-1);                                   \
  }

// use the following DDL to create a table for testing
// create table t2 (c1 varbinary(32), c2 varchar(32), c3 bigint, v1 varbinary(64), v2 bigint, primary key(c1, c2, c3)) partition by key(c1) partitions 16;
void get_row_and_print(ObTable *table, ObRowkey &rk);
int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (argc != 9) {
    usage(argv[0]);
    return -1;
  }
  // parse the arguments
  const char* host = argv[1];
  int32_t port = atoi(argv[2]);
  const char* tenant = argv[3];
  const char* user = argv[4];
  const char* passwd = argv[5];
  const char* db = argv[6];
  const char* table_name = argv[7];
  int32_t rpc_port = atoi(argv[8]);

  // 1. init the library
  ret = ObTableServiceLibrary::init();
  // 2. init the client
  ObTableServiceClient* p_service_client = ObTableServiceClient::alloc_client();
  ObTableServiceClient &service_client = *p_service_client;
  ret = service_client.init(ObString::make_string(host), port, rpc_port,
                            ObString::make_string(tenant), ObString::make_string(user),
                            ObString::make_string(passwd), ObString::make_string(db),
                            ObString::make_string(""));
  CHECK_RET(ret);
  // 3. init a table using the client
  ObTable* table = NULL;
  ret = service_client.alloc_table(ObString::make_string(table_name), table);
  CHECK_RET(ret);
  // 4. insert
  ObObj key_objs[3];
  key_objs[0].set_varbinary("abc");
  key_objs[1].set_varchar("cq");
  key_objs[1].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  key_objs[2].set_int(1);
  ObRowkey rk(key_objs, 3);
  oceanbase::share::ObPartitionLocation table_location;
  uint64_t table_id = 0;
  uint64_t partition_id = 0;
  ret = service_client.get_partition_location(ObString::make_string(table_name), rk, table_location, table_id, partition_id);
  CHECK_RET(ret);
  ObTableEntity entity;
  entity.set_rowkey(rk);
  ObObj value;
  value.set_varchar("value1");
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ret = entity.set_property("v1", value);
  CHECK_RET(ret);
  value.set_int(123);
  ret = entity.set_property("v2", value);
  CHECK_RET(ret);
  ObTableOperation table_op = ObTableOperation::insert(entity);
  ObTableOperationResult result;

  ret = table->execute(table_op, result);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to insert row: %d\n", ret);
  } else {
    fprintf(stderr, "insert row succ. %s\n", S(result));
  }
  // get the row
  get_row_and_print(table, rk);
  ////////////////
  // 5. update
  entity.reset();
  entity.set_rowkey(rk);
  value.set_int(666);
  ret = entity.set_property("v2", value);
  CHECK_RET(ret);
  table_op = ObTableOperation::update(entity);
  ret = table->execute(table_op, result);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to update row: %d\n", ret);
  } else {
    fprintf(stderr, "update row succ. %s\n", S(result));
  }
  // get the row
  get_row_and_print(table, rk);
  ////////////////
  // 6. replace
  table_op = ObTableOperation::replace(entity);
  ret = table->execute(table_op, result);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to replace row: %d\n", ret);
  } else {
    fprintf(stderr, "replace row succ. %s\n", S(result));
  }
  // get the row
  get_row_and_print(table, rk);
  ////////////////
  // 7. insert_or_update: update
  entity.reset();
  entity.set_rowkey(rk);
  value.set_varchar("zhuweng");
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ret = entity.set_property("v1", value);
  CHECK_RET(ret);
  table_op = ObTableOperation::insert_or_update(entity);
  ret = table->execute(table_op, result);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to insert_or_update row: %d\n", ret);
  } else {
    fprintf(stderr, "insert_or_update row succ. %s\n", S(result));
  }
  // get the row
  get_row_and_print(table, rk);
  ////////////////
  // 8. delete one row
  table_op = ObTableOperation::del(entity);
  ret = table->execute(table_op, result);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to del row: %d\n", ret);
  } else {
    fprintf(stderr, "delete row succ. %s\n", S(result));
  }
  // get the row
  get_row_and_print(table, rk);
  ////////////////
  // 9. insert_or_update: insert
  entity.reset();
  entity.set_rowkey(rk);
  value.set_varchar("yzf");
  value.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
  ret = entity.set_property("v1", value);
  CHECK_RET(ret);
  table_op = ObTableOperation::insert_or_update(entity);
  ret = table->execute(table_op, result);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to insert_or_update row: %d\n", ret);
  } else {
    fprintf(stderr, "insert_or_update row succ. %s\n", S(result));
  }
  // get the row
  get_row_and_print(table, rk);
  table_op = ObTableOperation::del(entity);
  ret = table->execute(table_op, result);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to del row: %d\n", ret);
  } else {
    fprintf(stderr, "delete row succ. %s\n", S(result));
  }

  ////////////////
  // 10. free the table instance
  service_client.free_table(table);
  // 11. destroy the client
  service_client.destroy();
  ObTableServiceClient::free_client(p_service_client);
  // 12. destroy the library
  ObTableServiceLibrary::destroy();
  return ret;
}

void get_row_and_print(ObTable *table, ObRowkey &rk)
{
  int ret = OB_SUCCESS;
  ObTableEntity entity_get;
  ObTableOperationResult result_get;
  ObObj null_obj;
  ret = entity_get.set_rowkey(rk);
  CHECK_RET(ret);
  ret = entity_get.set_property(ObString::make_string("v1"), null_obj);
  CHECK_RET(ret);
  ret = entity_get.set_property(ObString::make_string("v2"), null_obj);
  CHECK_RET(ret);
  ObTableOperation get_op = ObTableOperation::retrieve(entity_get);
  ret = table->execute(get_op, result_get);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to get row: %d\n", ret);
  } else {
    fprintf(stderr, "get row succ. %s\n", S(result_get));
  }
}
