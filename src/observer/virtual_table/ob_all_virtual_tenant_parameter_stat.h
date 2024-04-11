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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_PARAMETER_STAT_H_

#include "share/ob_virtual_table_iterator.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace observer
{

// __all_tenant_parameter_stat
//
// show all server parameter's, for every server:
// 1. if effective tenant is SYS tenant: show all tenant parameter and cluster parameter
// 2. if effective tenant is USER tenant: show self tenant parameter and cluster parameter
class ObAllVirtualTenantParameterStat : public common::ObVirtualTableIterator
{
public:
  ObAllVirtualTenantParameterStat();
  virtual ~ObAllVirtualTenantParameterStat();

  // param[in] show_seed  whether to show seed config
  int init(const bool show_seed);

  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  typedef common::ObConfigContainer::const_iterator CfgIter;

  int inner_sys_get_next_row(common::ObNewRow *&row);
  int inner_tenant_get_next_row(common::ObNewRow *&row);
  int update_seed();
  int inner_seed_get_next_row(common::ObNewRow *&row);
  int fill_row_(common::ObNewRow *&row,
      CfgIter &iter,
      const common::ObConfigContainer &cfg_container,
      const uint64_t *tenant_id_ptr);
  enum TENANT_PARAMETER_STAT_COLUMN {
    ZONE = common::OB_APP_MIN_COLUMN_ID,
    SERVER_TYPE,
    SERVER_IP,
    SERVER_PORT,
    NAME,
    DATA_TYPE,
    VALUE,
    INFO,
    SECTION,
    SCOPE,
    SOURCE,
    EDIT_LEVEL,
    TENANT_ID,
    DEFAULT_VALUE,
    ISDEFAULT,
  };

private:
  static const int64_t DEFAULT_TENANT_COUNT = 100;

  bool inited_;
  bool show_seed_;                // whether to show seed config

  CfgIter sys_iter_;              // iterator for cluster config
  CfgIter tenant_iter_;           // iterator for every tenant
  int64_t cur_tenant_idx_;        // current tenant index for tenant_id_list_
  common::ObSEArray<uint64_t, DEFAULT_TENANT_COUNT> tenant_id_list_;  // all tenant in observer
  omt::ObTenantConfigGuard tenant_config_;      // current tenant config

  omt::ObTenantConfig seed_config_;   // seed config

private:
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantParameterStat);
};
} // namespace observer
} // namespace oceanbase

#endif

