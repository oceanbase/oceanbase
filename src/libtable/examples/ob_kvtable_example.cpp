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
  fprintf(stderr, "Usage: %s <observer_host> <port> <tenant> <user> <password> <database> <table>\n", progname);
  fprintf(stderr, "Example: ./kvtable_example '100.88.11.96' 50803 sys root '' test t2\n");
}

#define CHECK_RET(ret) \
  if (OB_SUCCESS != (ret)) {                    \
    fprintf(stderr, "error: %d at %d\n", ret, __LINE__);       \
    exit(-1);                                   \
  }

/*
 * create a table as following before run this example
 * create table t3 (K varbinary(1024), CQ varchar(256), TS bigint, V varbinary(1024), primary key(K, CQ, TS));
 *
 */
int main(int argc, char *argv[])
{
  int ret = OB_SUCCESS;
  if (argc != 8) {
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

  // 1. init the library
  ret = ObTableServiceLibrary::init();
  // 2. init the client
  ObTableServiceClient* p_service_client = ObTableServiceClient::alloc_client();
  ObTableServiceClient &service_client = *p_service_client;
  ret = service_client.init(ObString::make_string(host), port, 8282,
                            ObString::make_string(tenant), ObString::make_string(user),
                            ObString::make_string(passwd), ObString::make_string(db),
                            ObString::make_string(""));
  CHECK_RET(ret);
  // 3. init a table using the client
  ObHKVTable* kv_table = NULL;
  ret = service_client.alloc_hkv_table(ObString::make_string(table_name), kv_table);
  CHECK_RET(ret);

  // put
  ObHKVTable::Key key;
  ObHKVTable::Value value;
  key.rowkey_ = ObString::make_string("abc");
  key.column_qualifier_ = ObString::make_string("cq");
  key.version_ = 123;
  value.set_varchar(ObString::make_string("value1"));
  ret = kv_table->put(key, value);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to put row: %d\n", ret);
  } else {
    fprintf(stderr, "put row succ\n");
  }
  // get
  ObHKVTable::Value value2;
  ret = kv_table->get(key, value2);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to get row: %d\n", ret);
  } else {
    fprintf(stderr, "get row succ. %s\n", S(value2));
  }
  // remove
  ret = kv_table->remove(key);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to remove row: %d\n", ret);
  } else {
    fprintf(stderr, "remove row succ\n");
  }
  // get
  ObHKVTable::Value value3;
  ret = kv_table->get(key, value3);
  if (OB_SUCCESS != ret) {
    fprintf(stderr, "failed to get row: %d\n", ret);
  } else {
    fprintf(stderr, "get row succ. %s\n", S(value3));
  }

  ////////////////
  // 10. free the table instance
  service_client.free_hkv_table(kv_table);
  // 11. destroy the client
  service_client.destroy();
  ObTableServiceClient::free_client(p_service_client);
  // 12. destroy the library
  ObTableServiceLibrary::destroy();
  return 0;
}
