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

namespace oceanbase {
namespace observer {

class ObAllVirtualTenantParameterStat : public common::ObVirtualTableIterator {
public:
  ObAllVirtualTenantParameterStat();
  ObAllVirtualTenantParameterStat(uint64_t tenant_id);
  void set_exec_tenant(uint64_t tenant_id);
  void set_show_seed(bool show_seed);
  virtual ~ObAllVirtualTenantParameterStat();
  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  int inner_sys_get_next_row(common::ObNewRow*& row);
  int inner_tenant_get_next_row(common::ObNewRow*& row);
  int update_seed();
  int inner_seed_get_next_row(common::ObNewRow*& row);
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
  };
  uint64_t exec_tenant_id_;
  common::ObConfigContainer::const_iterator sys_iter_;
  omt::ObTenantConfigGuard tenant_config_;
  common::ObConfigContainer::const_iterator tenant_iter_;
  bool show_seed_;
  omt::ObTenantConfig seed_config_;
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantParameterStat);
};
}  // namespace observer
}  // namespace oceanbase

#endif
