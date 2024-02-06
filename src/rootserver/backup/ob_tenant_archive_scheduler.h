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

#ifndef OCEANBASE_ROOTSERVER_OB_TENANT_ARCHIVE_SCHEDULER_H_
#define OCEANBASE_ROOTSERVER_OB_TENANT_ARCHIVE_SCHEDULER_H_

#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_transaction.h"
#include "share/backup/ob_tenant_archive_round.h"
#include "share/backup/ob_archive_checkpoint.h"

namespace oceanbase
{

namespace obrpc {
  class ObSrvRpcProxy;
}

namespace common {
  class ObMySQLProxy;
}

namespace rootserver
{

class ObArchiveHandler final
{
public:
  ObArchiveHandler();
  ~ObArchiveHandler() {}

  int init(
      const uint64_t tenant_id,
      share::schema::ObMultiVersionSchemaService *schema_service,
      obrpc::ObSrvRpcProxy &rpc_proxy,
      common::ObMySQLProxy &sql_proxy);

  int open_archive_mode();
  int close_archive_mode();

  // Just mark archive is open, actual actions to open archive is a background task.
  int enable_archive(const int64_t dest_no);
  // Just mark archive is stop, actual actions to close archive is a background task.
  int disable_archive(const int64_t dest_no);
  // Just mark archive is suspend, actual actions to suspend archive is a background task.
  int defer_archive(const int64_t dest_no);
  int check_can_do_archive(bool &can) const;
  int checkpoint();

  TO_STRING_KV(K_(tenant_id), K_(round_handler));

private:
  int checkpoint_(share::ObTenantArchiveRoundAttr &round_attr);
  int start_archive_(share::ObTenantArchiveRoundAttr &round_attr);
  // notify archive start/end event to each log stream.
  int notify_(const share::ObTenantArchiveRoundAttr &round_attr);
  int do_checkpoint_(share::ObTenantArchiveRoundAttr &round_info);
  int check_archive_dest_validity_(const int64_t dest_no);
  int get_max_checkpoint_scn_(const uint64_t tenant_id, share::SCN &max_checkpoint_scn) const;

private:
  bool is_inited_;
  uint64_t tenant_id_; // user tenant id
  obrpc::ObSrvRpcProxy *rpc_proxy_;
  common::ObMySQLProxy *sql_proxy_;
  share::schema::ObMultiVersionSchemaService *schema_service_;
  share::ObArchiveRoundHandler round_handler_;
  share::ObArchivePersistHelper archive_table_op_;

  DISALLOW_COPY_AND_ASSIGN(ObArchiveHandler);
};

}
}


#endif
