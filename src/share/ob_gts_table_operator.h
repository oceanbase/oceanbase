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

#ifndef OCEANBASE_SHARE_OB_GTS_TABLE_OPERATOR_H_
#define OCEANBASE_SHARE_OB_GTS_TABLE_OPERATOR_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/string/ob_sql_string.h"
#include "share/ob_gts_info.h"
#include "share/partition_table/ob_partition_info.h"

namespace oceanbase {
namespace share {
class ObGtsTableOperator {
public:
  ObGtsTableOperator();
  ~ObGtsTableOperator();

public:
  int init(common::ObMySQLProxy* proxy);
  int get_gts_infos(common::ObIArray<common::ObGtsInfo>& gts_infos) const;
  int get_gts_tenant_infos(common::ObIArray<common::ObGtsTenantInfo>& gts_tenant_infos) const;
  int insert_tenant_gts(const uint64_t tenant_id, const uint64_t gts_id);
  int erase_tenant_gts(const uint64_t tenant_id);
  int insert_gts_instance(const common::ObGtsInfo& gts_info);
  int get_gts_standby(const uint64_t gts_id, common::ObAddr& standby) const;
  int update_gts_member_list(const uint64_t gts_id, const int64_t curr_epoch_id, const int64_t new_epoch_id,
      const common::ObMemberList& member_list, const common::ObAddr& standby);
  int update_gts_member_list_and_standby(const uint64_t gts_id, const int64_t curr_epoch_id, const int64_t new_epoch_id,
      const common::ObMemberList& member_list, const common::ObAddr& standby);
  int get_gts_info(const uint64_t gts_id, common::ObGtsInfo& gts_info) const;
  int try_update_standby(const common::ObGtsInfo& gts_info, const common::ObAddr& new_standby, bool& do_update);

private:
  int read_gts_infos_(const common::ObSqlString& sql, common::ObIArray<common::ObGtsInfo>& gts_infos) const;
  int read_gts_info_(const common::sqlclient::ObMySQLResult& result, common::ObGtsInfo& gts_info) const;
  int read_gts_tenant_infos_(
      const common::ObSqlString& sql, common::ObIArray<common::ObGtsTenantInfo>& gts_tenant_infos) const;
  int read_gts_tenant_info_(
      const common::sqlclient::ObMySQLResult& result, common::ObGtsTenantInfo& gts_tenant_info) const;
  int read_gts_standby_(const common::ObSqlString& sql, common::ObAddr& standby) const;
  int read_gts_standby_(const common::sqlclient::ObMySQLResult& result, common::ObAddr& standby) const;
  static int convert_member_list_(
      const ObPartitionReplica::MemberList& orig_member_list, common::ObMemberList& curr_member_list);
  static int convert_member_list_(
      const common::ObMemberList& orig_member_list, ObPartitionReplica::MemberList& curr_member_list);

private:
  bool is_inited_;
  common::ObMySQLProxy* proxy_;

private:
  DISALLOW_COPY_AND_ASSIGN(ObGtsTableOperator);
};
}  // namespace share
}  // namespace oceanbase

#endif  // OCEANBASE_SHARE_OB_GTS_TABLE_OPERATOR_H_
