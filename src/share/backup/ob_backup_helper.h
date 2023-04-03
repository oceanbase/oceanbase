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