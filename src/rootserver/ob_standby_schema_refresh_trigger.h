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

#ifndef OCEANBASE_ROOTSERVER_STANDBY_SCHEMA_REFRESH_TRIGGER_H
#define OCEANBASE_ROOTSERVER_STANDBY_SCHEMA_REFRESH_TRIGGER_H

#include "lib/thread/ob_reentrant_thread.h"//ObRsReentrantThread
#include "lib/utility/ob_print_utils.h" //TO_STRING_KV
#include "share/ob_tenant_info_proxy.h"//ObAllTenantInfo
#include "rootserver/ob_primary_ls_service.h"//ObTenantThreadHelper

namespace oceanbase {
namespace common
{
class ObMySQLProxy;
}
namespace share
{
class SCN;
}
namespace rootserver
{

class ObStandbySchemaRefreshTrigger : public ObTenantThreadHelper
{
public:
  ObStandbySchemaRefreshTrigger() : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID), sql_proxy_(NULL) {}
  virtual ~ObStandbySchemaRefreshTrigger() {}
  int init();
  void destroy();
  virtual void do_work() override;

  DEFINE_MTL_FUNC(ObStandbySchemaRefreshTrigger)

private:
  int check_inner_stat_();
  int submit_tenant_refresh_schema_task_();
  const static int64_t DEFAULT_IDLE_TIME = 1000 * 1000;  // 1s

public:
 TO_STRING_KV(K_(is_inited), K_(tenant_id), KP_(sql_proxy));

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
};

} // namespace rootserver
} // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_STANDBY_SCHEMA_REFRESH_TRIGGER_H */