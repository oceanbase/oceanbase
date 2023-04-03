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

#ifndef OCEANBASE_ENCODING_MYSQL_DUMP_DATA_READER_H_
#define OCEANBASE_ENCODING_MYSQL_DUMP_DATA_READER_H_

#include <vector>
#include <string>
#include <fstream>
#include "lib/string/ob_string.h"

namespace oceanbase
{
class MysqlDumpDataReader
{
public:
  MysqlDumpDataReader();
  virtual ~MysqlDumpDataReader() = default;
  int init(const char *file);
  // SQLs which not insert stmt
  const std::vector<std::string> &schema_sql() { return schema_sqls_; }
  int next_data(std::vector<common::ObString> &data);
  int reset();

private:
  int next_sql();
  int parse_insert_sql();

private:
  std::string cur_sql_;
  std::string cur_line_;
  std::vector<std::string> schema_sqls_;
  std::ifstream stream_;
  int64_t value_cnt_;
  int64_t data_index_;
  std::vector<common::ObString> datas_;
};
} // end namespace oceanbase

#endif // OCEANBASE_ENCODING_MYSQL_DUMP_DATA_READER_H_
