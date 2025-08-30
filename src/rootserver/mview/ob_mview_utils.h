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

#ifndef OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_UTILS_H_
#define OCEANBASE_ROOTSERVER_MVIEW_OB_MVIEW_UTILS_H_

#include "lib/ob_define.h"
#include "share/schema/ob_table_schema.h"
#include "sql/session/ob_sql_session_mgr.h"

namespace oceanbase
{
namespace rootserver
{
class ObMViewUtils
{
public:
  static int create_inner_session(const uint64_t tenant_id,
                                  share::schema::ObSchemaGetterGuard &schema_guard,
                                  sql::ObFreeSessionCtx &free_session_ctx,
                                  sql::ObSQLSessionInfo *&session);
  static void release_inner_session(sql::ObFreeSessionCtx &free_session_ctx,
                                    sql::ObSQLSessionInfo *&session);
  static int submit_build_mlog_task(const uint64_t tenant_id, 
                                    ObIArray<ObString> &mlog_columns,
                                    share::schema::ObSchemaGetterGuard &schema_guard,
                                    const share::schema::ObTableSchema *base_table_schema,
                                    int64_t &task_id);
  static int get_base_table_name_for_print(const uint64_t tenant_id,
                                      const share::schema::ObTableSchema *base_table_schema,
                                      share::schema::ObSchemaGetterGuard &schema_guard,
                                      ObString &base_table_name);
  static int get_mlog_column_list_str(const ObIArray<ObString> &mlog_columns,
                                      ObSqlString &column_list_str);
};

class ObMViewAutoMlogEventInfo
{
public:
  ObMViewAutoMlogEventInfo()
      : allocator_(), tenant_id_(),
        ret_(OB_SUCCESS), task_id_(OB_INVALID_ID), start_ts_(OB_INVALID_TIMESTAMP),
        base_table_id_(OB_INVALID_ID), base_table_name_(),
        old_mlog_id_(OB_INVALID_ID), new_mlog_id_(OB_INVALID_ID),
        related_create_mview_ddl_(), mlog_columns_() {}
  ~ObMViewAutoMlogEventInfo() {}
  int set_tenant_id(const uint64_t tenant_id) { tenant_id_ = tenant_id; return OB_SUCCESS; }
  int set_allocator(ObArenaAllocator *allocator) { allocator_ = allocator; return OB_SUCCESS; }
  int set_ret(const int64_t ret) { ret_ = ret; return OB_SUCCESS; }
  int set_task_id(const int64_t task_id) { task_id_ = task_id; return OB_SUCCESS; }
  int64_t get_task_id() const { return task_id_; }
  int set_start_ts(const int64_t start_ts) { start_ts_ = start_ts; return OB_SUCCESS; }
  int set_base_table_id(const uint64_t base_table_id) { base_table_id_ = base_table_id; return OB_SUCCESS; }
  uint64_t get_base_table_id() const { return base_table_id_; }
  int set_base_table_name(const ObString &base_table_name);
  int set_old_mlog_id(const uint64_t old_mlog_id) { old_mlog_id_ = old_mlog_id; return OB_SUCCESS; }
  int set_new_mlog_id(const uint64_t new_mlog_id) { new_mlog_id_ = new_mlog_id; return OB_SUCCESS; }
  int set_related_create_mview_ddl(const ObString &related_create_mview_ddl);
  int set_mlog_columns(const ObString &mlog_columns);
  void submit_event();
  TO_STRING_KV(K(tenant_id_), K(ret_), K(task_id_), K(start_ts_), K(base_table_id_), K(base_table_name_), K(old_mlog_id_), K(new_mlog_id_), K(related_create_mview_ddl_), K(mlog_columns_));
private:
  ObArenaAllocator *allocator_;
  uint64_t tenant_id_;
  int64_t ret_;
  int64_t task_id_;
  int64_t start_ts_;
  uint64_t base_table_id_;
  ObString base_table_name_;
  uint64_t old_mlog_id_;
  uint64_t new_mlog_id_;
  ObString related_create_mview_ddl_;
  ObString mlog_columns_;
};

} // namespace rootserver
} // namespace oceanbase
#endif
