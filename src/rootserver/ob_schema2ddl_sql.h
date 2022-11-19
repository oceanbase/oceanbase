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

#ifndef OCEANBASE_ROOTSERVER_OB_SCHEMA2DDL_SQL_H_
#define OCEANBASE_ROOTSERVER_OB_SCHEMA2DDL_SQL_H_

#include <stdint.h>

namespace oceanbase
{

namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
}
}

namespace rootserver
{

// Convert table schema to create table sql for creating table in mysql server.
class ObSchema2DDLSql
{
public:
  ObSchema2DDLSql();
  virtual ~ObSchema2DDLSql();

  static int convert(const share::schema::ObTableSchema &table_schema,
                     char *sql_buf, const int64_t buf_size);

private:
  static int type2str(const share::schema::ObColumnSchemaV2 &column_schema,
                      char *buf, const int64_t buf_size);
};
} // end namespace common
} // end namespace oceanbase
#endif // OCEANBASE_ROOTSERVER_OB_SCHEMA2DDL_SQL_H_
