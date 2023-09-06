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

#ifndef OCEANBASE_SHARE_RECOVER_TABLE_PERSIST_HELPER_H
#define OCEANBASE_SHARE_RECOVER_TABLE_PERSIST_HELPER_H

#include "lib/ob_define.h"
#include "share/ob_inner_table_operator.h"
#include "share/restore/ob_import_table_struct.h"

namespace oceanbase
{
namespace share
{
class ObRecoverTablePersistHelper final : public ObIExecTenantIdProvider
{
public:
  ObRecoverTablePersistHelper();
  virtual ~ObRecoverTablePersistHelper() {}
  int init(const uint64_t tenant_id);
  void reset() { is_inited_ = false; }
  uint64_t get_exec_tenant_id() const override { return gen_meta_tenant_id(tenant_id_); }
  int insert_recover_table_job(common::ObISQLClient &proxy, const ObRecoverTableJob &job) const;

  int get_all_recover_table_job(common::ObISQLClient &proxy, common::ObIArray<ObRecoverTableJob> &jobs) const;

  int get_recover_table_job(common::ObISQLClient &proxy, const uint64_t tenant_id, const int64_t job_id,
      ObRecoverTableJob &job) const;

  int is_recover_table_job_exist(common::ObISQLClient &proxy, const uint64_t target_tenant_id, bool &is_exist) const;

  int advance_status(common::ObISQLClient &proxy,
      const ObRecoverTableJob &job, const ObRecoverTableStatus &next_status) const;
  int force_cancel_recover_job(common::ObISQLClient &proxy) const;

  int get_recover_table_job_by_initiator(common::ObISQLClient &proxy,
      const ObRecoverTableJob &initiator_job, ObRecoverTableJob &target_job) const;

  int delete_recover_table_job(common::ObISQLClient &proxy, const ObRecoverTableJob &job) const;
  int insert_recover_table_job_history(common::ObISQLClient &proxy, const ObRecoverTableJob &job) const;

  int get_recover_table_job_history_by_initiator(common::ObISQLClient &proxy,
      const ObRecoverTableJob &initiator_job, ObRecoverTableJob &target_job) const;
  TO_STRING_KV(K_(is_inited), K_(tenant_id));
private:
  DISALLOW_COPY_AND_ASSIGN(ObRecoverTablePersistHelper);
  bool is_inited_;
  uint64_t tenant_id_; // sys or user tenant id
  ObInnerTableOperator table_op_;
};

}
}

#endif