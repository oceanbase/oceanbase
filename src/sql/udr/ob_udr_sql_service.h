/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OB_SQL_UDR_OB_UDR_SQL_SERVICE_H_
#define OB_SQL_UDR_OB_UDR_SQL_SERVICE_H_
#include "lib/mysqlclient/ob_mysql_result.h"
#include "sql/udr/ob_udr_struct.h"

namespace oceanbase
{
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class ObDMLSqlSplicer;
}

namespace sql
{

class ObUDRSqlService
{
public:
  ObUDRSqlService()
  : inited_(false),
    sql_proxy_(nullptr)
  {}
  ~ObUDRSqlService() {}
  int init(ObMySQLProxy *proxy);
  int insert_rule(ObUDRInfo &arg);
  int remove_rule(ObUDRInfo &arg);
  int alter_rule_status(ObUDRInfo &arg);
  int fetch_max_rule_version(const uint64_t tenant_id,
                             int64_t &max_rule_version);
  int get_need_sync_rule_infos(ObIAllocator& allocator,
                               const uint64_t tenant_id,
                               const int64_t local_rule_version,
                               ObIArray<ObUDRInfo>& rule_infos);
  int clean_up_items_marked_for_deletion(const uint64_t tenant_id);

private:
  int fetch_new_rule_id(const uint64_t tenant_id,
                        int64_t &new_rule_id);
  int fetch_new_rule_version(const uint64_t tenant_id,
                             int64_t &new_rule_version);
  int gen_insert_rule_dml(const ObUDRInfo &arg,
                          const uint64_t tenant_id,
                          oceanbase::share::ObDMLSqlSplicer &dml);
  int gen_modify_rule_status_dml(const ObUDRInfo &arg,
                                 const uint64_t tenant_id,
                                 oceanbase::share::ObDMLSqlSplicer &dml);
  int gen_recyclebin_rule_name(const int64_t rule_version,
                               const int64_t buf_len,
                               char *buf,
                               ObString &recyclebin_rule_name);

private:
  // Trigger delete DATE interval threshold
  static const int64_t DELETE_DATE_INTERVAL_THRESHOLD = 2;
  bool inited_;
  ObMySQLProxy* sql_proxy_;
};

} // namespace sql end
} // namespace oceanbase end

#endif
