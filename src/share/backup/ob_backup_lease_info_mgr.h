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

#ifndef OCEANBASE_SRC_SHARE_BACKUP_OB_BACKUP_LEASE_INFO_MGR_H_
#define OCEANBASE_SRC_SHARE_BACKUP_OB_BACKUP_LEASE_INFO_MGR_H_

#include "share/ob_define.h"
#include "share/ob_force_print_log.h"
#include "lib/mysqlclient/ob_isql_client.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "lib/mysqlclient/ob_mysql_statement.h"
#include "lib/mysqlclient/ob_mysql_connection_pool.h"
#include "common/ob_role.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_common_rpc_proxy.h"

namespace oceanbase {
namespace share {

struct ObBackupLeaseInfo {

  static const int64_t MAX_LEASE_TIME = 30 * 1000 * 1000;
  static const int64_t MAX_LEASE_TAKEOVER_TIME = 10 * 60 * 1000 * 1000L;

  ObBackupLeaseInfo();
  void reset();
  int set_new_lease(const int64_t start_ts, const int64_t epoch, const int64_t takeover_ts, const int64_t round);
  int release_lease(const int64_t round);
  int update_lease_start_ts(const int64_t now_ts);
  bool is_valid() const;
  TO_STRING_KV(K_(is_leader), K_(lease_start_ts), K_(leader_epoch), K_(leader_takeover_ts), K_(round));
  bool is_leader_;
  int64_t lease_start_ts_;
  int64_t leader_epoch_;
  int64_t leader_takeover_ts_;
  int64_t round_;
};

class ObBackupLeaseInfoMgr {
public:
  ObBackupLeaseInfoMgr();
  virtual ~ObBackupLeaseInfoMgr();
  int init(const common::ObAddr& addr, common::ObMySQLProxy& sql_proxy);
  int renew_lease(const int64_t can_be_leader_ts, const int64_t next_round, const ObBackupLeaseInfo& old_lease_info,
      ObBackupLeaseInfo& new_lease_info, const char*& msg);
  int clean_backup_lease_info(
      const int64_t next_round, const ObBackupLeaseInfo& old_lease_info, ObBackupLeaseInfo& new_lease_info);

private:
  int get_backup_scheduler_leader(
      common::ObISQLClient& sql_client, common::ObAddr& backup_leader_addr, bool& has_leader);
  int update_backup_scheduler_leader(const common::ObAddr& backup_leader_addr, common::ObISQLClient& sql_client);
  int clean_backup_scheduler_leadear_();
  int do_renew_lase_(const int64_t leader_epoch, const int64_t takeover_time, const int64_t can_be_leader_ts,
      const int64_t next_round, const ObBackupLeaseInfo& old_lease_info, ObBackupLeaseInfo& new_lease_info,
      const char*& msg);
  int get_core_table_info_(ObRole& role, int64_t& leader_epoch, int64_t& takeover_time);

private:
  bool is_inited_;
  common::ObPartitionKey all_core_table_key_;
  common::ObAddr local_addr_;
  common::ObMySQLProxy* sql_proxy_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupLeaseInfoMgr);
};

class ObIBackupLeaseService {
public:
  virtual ~ObIBackupLeaseService()
  {}
  virtual int check_lease() = 0;
  virtual int get_lease_status(bool& is_lease_valid) = 0;
  DEFINE_VIRTUAL_TO_STRING();
};

class ObFakeBackupLeaseService : public ObIBackupLeaseService {
public:
  virtual int check_lease();
  virtual int get_lease_status(bool& is_lease_valid);
};

}  // namespace share
}  // namespace oceanbase

#define BACKUP_LEASE_MGR (::oceanbase::share::ObBackupLeaseInfoMgr::get_instance())

#endif /* OCEANBASE_SRC_SHARE_BACKUP_OB_BACKUP_LEASE_INFO_MGR_H_ */
