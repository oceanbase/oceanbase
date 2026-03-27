/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_SHARE_OB_BACKUP_HELPER_H_
#define OCEANBASE_SHARE_OB_BACKUP_HELPER_H_

#include "share/ob_inner_kv_table_operator.h"
#include "share/backup/ob_backup_struct.h"

namespace oceanbase
{
namespace share
{

class ObBackupHelper final : public ObIExecTenantIdProvider
{
public:
  ObBackupHelper();
  virtual ~ObBackupHelper() {}

  // Return tenant id to execute sql.
  uint64_t get_exec_tenant_id() const override;

  int init(const uint64_t tenant_id, common::ObISQLClient &sql_proxy);
  int get_backup_dest(share::ObBackupPathString &backup_dest) const;
  
  int set_backup_dest(const share::ObBackupPathString &backup_dest) const;

  TO_STRING_KV(K_(is_inited), K_(tenant_id));

private:
  DISALLOW_COPY_AND_ASSIGN(ObBackupHelper);

  int init_backup_parameter_table_operator_(ObInnerKVTableOperator &kv_table_operator) const;

private:
  bool is_inited_;
  uint64_t tenant_id_; // user tenant id
  common::ObISQLClient *sql_proxy_;
};

}
}

#endif