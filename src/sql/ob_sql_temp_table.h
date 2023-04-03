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

#ifndef OCEANBASE_SQL_TEMP_TABLE_
#define OCEANBASE_SQL_TEMP_TABLE_

#include "sql/resolver/dml/ob_dml_stmt.h"
#include "sql/resolver/expr/ob_raw_expr.h"
#include "sql/optimizer/ob_sharding_info.h"
#include "observer/ob_server_struct.h"

namespace oceanbase
{
namespace sql
{
struct ObTempTableResultInfo
{
  OB_UNIS_VERSION(1);
public:
  ObTempTableResultInfo() : addr_(),
                         interm_result_ids_() {}
  virtual ~ObTempTableResultInfo() {}

  TO_STRING_KV(K_(addr),
               K_(interm_result_ids));
  //数据所在server
  ObAddr addr_;
  //数据集的key
  ObSEArray<uint64_t, 2> interm_result_ids_;
};

class ObSqlTempTableCtx
{
  OB_UNIS_VERSION(1);
public:
  ObSqlTempTableCtx() : interm_result_infos_(),
                        temp_table_id_(0),
                        is_local_interm_result_(true) {}
  virtual ~ObSqlTempTableCtx() {}

  TO_STRING_KV(K_(interm_result_infos),
               K_(temp_table_id),
               K_(is_local_interm_result));

  //结果集的分布信息：所在机器及KEY
  ObSEArray<ObTempTableResultInfo, 2> interm_result_infos_;
  //结果集所属的temp table
  uint64_t temp_table_id_;
  //结果集是否在本地
  bool is_local_interm_result_;
};

class ObSqlTempTableInfo
{
public:
  ObSqlTempTableInfo() : temp_table_id_(OB_INVALID_ID),
                         table_name_(),
                         table_query_(NULL),
                         table_plan_(NULL) {}
  virtual ~ObSqlTempTableInfo() {}

  TO_STRING_KV(K_(temp_table_id),
               K_(table_name));

public:
  uint64_t temp_table_id_;
  common::ObString table_name_;
  ObSelectStmt *table_query_;
  ObLogicalOperator *table_plan_;
};

} /* ns sql*/
} /* ns oceanbase */

#endif //OCEANBASE_SQL_TEMP_TABLE_
