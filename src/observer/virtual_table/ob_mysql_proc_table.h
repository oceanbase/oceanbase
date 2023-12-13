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

#ifndef OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_PROC_TABLE_H_
#define OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_PROC_TABLE_H_

#include "share/ob_virtual_table_scanner_iterator.h"
#include "sql/session/ob_basic_session_info.h"

namespace oceanbase
{
namespace sql
{
class ObSQLSessionInfo;
class ObExecEnv;
}
namespace observer
{
class ObMySQLProcTable : public common::ObVirtualTableScannerIterator
{
private:
  enum MySQLProcTableColumns {
    DB = 16,
    NAME,
    TYPE,
    SPECIFIC_NAME,
    LANGUAGE,
    SQL_DATA_ACCESS,
    IS_DETERMINISTIC,
    SECURITY_TYPE,
    PARAM_LIST,
    RETURNS,
    BODY,
    DEFINER,
    CREATED,
    MODIFIED,
    SQL_MODE,
    COMMENT,
    CHARACTER_SET_CLIENT,
    COLLATION_CONNECTION,
    DB_COLLATION,
    BODY_UTF8,
  };
public:
  ObMySQLProcTable();
  virtual ~ObMySQLProcTable();

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();
  inline void set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; }
  int get_info_from_all_routine(const uint64_t col_id,
                                const share::schema::ObRoutineInfo *routine_info,
                                int64_t &routine_time);

private:
  uint64_t tenant_id_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObMySQLProcTable);

  static int extract_create_node_from_routine_info(ObIAllocator &alloc, const ObRoutineInfo &routine_info, const sql::ObExecEnv &exec_env, ParseNode *&create_node);
};
}
}

#endif /* OCEANBASE_SRC_OBSERVER_VIRTUAL_TABLE_OB_MYSQL_PROC_TABLE_H_ */
