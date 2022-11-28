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

#ifndef OCEANBASE_OBSERVER_OB_SERVER_META_TABLE_CHECKER
#define OCEANBASE_OBSERVER_OB_SERVER_META_TABLE_CHECKER

#include "lib/task/ob_timer.h" // ObTimerTask
#include "observer/omt/ob_multi_tenant.h" // ObMultiTenant, TenantIdList

namespace oceanbase
{
namespace share
{
class ObLSTableOperator;
class ObTabletTableOperator;

namespace schema
{
class ObMultiVersionSchemaService;
} // end namespace schema
} // end namespace share

namespace observer
{
class ObServerMetaTableChecker;

class ObServerLSMetaTableCheckTask : public common::ObTimerTask
{
public:
  explicit ObServerLSMetaTableCheckTask(ObServerMetaTableChecker &checker);
  virtual ~ObServerLSMetaTableCheckTask() {}
  virtual void runTimerTask() override;
private:
  ObServerMetaTableChecker &checker_;
};

class ObServerTabletMetaTableCheckTask : public common::ObTimerTask
{
public:
  explicit ObServerTabletMetaTableCheckTask(ObServerMetaTableChecker &checker);
  virtual ~ObServerTabletMetaTableCheckTask() {}
  virtual void runTimerTask() override;
private:
  ObServerMetaTableChecker &checker_;
};

// ObServerMetaTableChecker is used to remove residual info in __all_tablet_meta_table
// and __all_ls_meta_table in following scenarios:
// 1. The tenant has been dropped and ObTenantMetaChecker can't work.
// 2. After transferring, there has no resources of the tenant in local.
class ObServerMetaTableChecker
{
public:
  enum ObMetaTableCheckType
  {
    CHECK_LS_META_TABLE = 0,
    CHECK_TABLET_META_TABLE
  };

public:
  ObServerMetaTableChecker();
  virtual ~ObServerMetaTableChecker() {}
  int init(
      share::ObLSTableOperator *lst_operator,
      share::ObTabletTableOperator *tt_operator,
      omt::ObMultiTenant *omt,
      share::schema::ObMultiVersionSchemaService *schema_service);
  int start();
  void stop();
  void wait();
  void destroy();
  // check residual ls or tablet info in __all_ls_meta_table and __all_tablet_meta_table
  // @param[in] check_type, CHECK_LS_META_TABLE or CHECK_TABLET_META_TABLE
  int check_meta_table(const ObMetaTableCheckType check_type);
  int schedule_ls_meta_check_task();
  int schedule_tablet_meta_check_task();
private:
  int check_ls_table_(
      const uint64_t tenant_id,
      int64_t &residual_count);
  int check_tablet_table_(
      const uint64_t tenant_id,
      int64_t &meta_residual_count,
      int64_t &checksum_residual_count);

  bool inited_;
  bool stopped_;
  int ls_tg_id_;
  int tablet_tg_id_;
  ObServerLSMetaTableCheckTask ls_meta_check_task_; // timer task to check ls meta table
  ObServerTabletMetaTableCheckTask tablet_meta_check_task_; // timer task to check tablet meta table
  share::ObLSTableOperator *lst_operator_; // operator to process __all_ls_meta_table
  share::ObTabletTableOperator *tt_operator_; // operator to process __all_tablet_meta_table
  omt::ObMultiTenant *omt_; // used to get tenant list on local server
  share::schema::ObMultiVersionSchemaService *schema_service_; // used to get all tenant list
};

} // end namespace observer
} // end namespace oceanbase
#endif