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

#ifndef OCEANBASE_SHARE_OB_RESTORE_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_RESTORE_TABLE_OPERATOR_H_

#include <cstdint>
#include "lib/ob_define.h"
#include "lib/utility/ob_macro_utils.h"
#include "share/ob_dml_sql_splicer.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
namespace oceanbase
{
namespace share
{
class ObLogRestoreSourceItem;
class ObTenantRestoreTableOperator
{
public:
  ObTenantRestoreTableOperator();

  // init restore table operator
  int init(const uint64_t user_tenant_id, ObISQLClient *proxy);

  // insert log restore source
  int insert_source(const ObLogRestoreSourceItem &item);

  // update log restore source until ts, success only if until ts is modified bigger
  int update_source_until_scn(const ObLogRestoreSourceItem &item);

  // delete log restore source
  int delete_source();

  int get_source(ObLogRestoreSourceItem &item);

  int get_source_for_update(ObLogRestoreSourceItem &item, ObMySQLTransaction &trans);

private:
  int fill_log_restore_source_(const ObLogRestoreSourceItem &item, ObDMLSqlSplicer &dml);
  int fill_select_source_(common::ObSqlString &sql);
  int parse_log_restore_source_(sqlclient::ObMySQLResult &result, ObLogRestoreSourceItem &itm);
  uint64_t get_exec_tenant_id_() const;
private:
  bool is_inited_;
  uint64_t user_tenant_id_;   // user tenant id
  ObISQLClient *proxy_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTenantRestoreTableOperator);
};
} // namespace share
} // namespace oceanbase
#endif /* OCEANBASE_SHARE_OB_RESTORE_TABLE_OPERATOR_H_ */
