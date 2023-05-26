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

#ifndef OCEANBASE_ROOTSERVER_CREATE_STANDBY_FROM_NET_ACTOR_H
#define OCEANBASE_ROOTSERVER_CREATE_STANDBY_FROM_NET_ACTOR_H

#include "lib/atomic/ob_atomic.h"           // ATOMIC_**
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

class ObCreateStandbyFromNetActor : public ObTenantThreadHelper
{
public:
  ObCreateStandbyFromNetActor() : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID), sql_proxy_(NULL), schema_broadcasted_(false), idle_time_(DEFAULT_IDLE_TIME) {}
  virtual ~ObCreateStandbyFromNetActor() {}
  int init();
  void destroy();
  virtual void do_work() override;
  static int check_has_user_ls(const uint64_t tenant_id, common::ObMySQLProxy *sql_proxy, bool &has_user_ls);

  DEFINE_MTL_FUNC(ObCreateStandbyFromNetActor)

private:
  int check_inner_stat_();
  int do_creating_standby_tenant();
  int finish_restore_if_possible_();
  int64_t get_idle_interval_us_() { return ATOMIC_LOAD(&idle_time_); }
  int set_idle_interval_us_(const int64_t idle_time);
  int refresh_schema_();

  const static int64_t DEFAULT_IDLE_TIME = 1000 * 1000;  // 1s
  const static int64_t MAX_IDLE_TIME = 3600L * 1000 * 1000;  // 3600s

public:
 TO_STRING_KV(K_(is_inited), K_(tenant_id), KP_(sql_proxy), K_(schema_broadcasted), K_(idle_time));

private:
  bool is_inited_;
  uint64_t tenant_id_;
  common::ObMySQLProxy *sql_proxy_;
  bool schema_broadcasted_;
  int64_t idle_time_;
};

} // namespace rootserver
} // namespace oceanbase

#endif /* !OCEANBASE_ROOTSERVER_CREATE_STANDBY_FROM_NET_ACTOR_H */