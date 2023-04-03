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

#ifndef OCEANBASE_SHARE_OB_TENANT_ARCHIVE_ROUND_H_
#define OCEANBASE_SHARE_OB_TENANT_ARCHIVE_ROUND_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/backup/ob_archive_struct.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "share/backup/ob_archive_checkpoint.h"

namespace oceanbase
{
namespace share
{


class ObArchiveRoundHandler
{
public:
  ObArchiveRoundHandler();
  ~ObArchiveRoundHandler() {}
  int init(
      const uint64_t tenant_id, 
      const int64_t incarnation,
      common::ObMySQLProxy &sql_proxy);

  int enable_archive(const int64_t dest_no, ObTenantArchiveRoundAttr &round);
  int disable_archive(const int64_t dest_no, ObTenantArchiveRoundAttr &round);
  int defer_archive(const int64_t dest_no, ObTenantArchiveRoundAttr &round);
  
  bool can_start_archive(const ObTenantArchiveRoundAttr &round) const;
  bool can_stop_archive(const ObTenantArchiveRoundAttr &round) const;
  bool can_suspend_archive(const ObTenantArchiveRoundAttr &round) const;
  int can_enable_archive(const int64_t dest_no, bool &can);

  int start_archive(const ObTenantArchiveRoundAttr &round, ObTenantArchiveRoundAttr &new_round);
  virtual int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round,
      const common::ObIArray<ObTenantArchivePieceAttr> &pieces);
  int checkpoint_to(
      const ObTenantArchiveRoundAttr &old_round, 
      const ObTenantArchiveRoundAttr &new_round);

  common::ObMySQLProxy *get_sql_proxy() const { return sql_proxy_; }

  TO_STRING_KV(K_(is_inited), K_(tenant_id), K_(incarnation));

private:
  int get_current_round_(
      common::ObISQLClient &proxy, 
      bool need_lock, 
      ObTenantArchiveRoundAttr &round);
  uint64_t get_exec_tenant_id_() const;
  int start_trans_(common::ObMySQLTransaction &trans);
  int prepare_new_dest_round_(const int64_t dest_no, ObMySQLTransaction &trans, ObTenantArchiveRoundAttr &round);
  int prepare_beginning_dest_round_(const ObTenantArchiveRoundAttr &round, ObTenantArchiveRoundAttr &new_round);
  int decide_start_scn_(share::SCN &start_ts);

  bool is_inited_;
  uint64_t tenant_id_; // user tenant id
  int64_t incarnation_;
  common::ObMySQLProxy *sql_proxy_;
  ObArchivePersistHelper archive_table_op_;
  DISALLOW_COPY_AND_ASSIGN(ObArchiveRoundHandler);
};

}
}


#endif