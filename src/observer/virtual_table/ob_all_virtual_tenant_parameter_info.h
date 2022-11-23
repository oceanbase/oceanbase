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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_H_
#define OCEANBASE_OBSERVER_VIRTUAL_TABLE_OB_ALL_VIRTUAL_TENANT_PARAMETER_INFO_H_

#include "lib/container/ob_array.h"
#include "lib/container/ob_array_iterator.h"
#include "share/ob_virtual_table_iterator.h"
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config_mgr.h"

namespace oceanbase
{
namespace observer
{

class ObAllVirtualTenantParameterInfo : public common::ObVirtualTableIterator
{
public:
  ObAllVirtualTenantParameterInfo();
  virtual ~ObAllVirtualTenantParameterInfo();
  virtual int inner_open();
  virtual void reset();
  virtual int inner_get_next_row(common::ObNewRow *&row);
private:
  enum ALL_TENANT_PARAMETER_INFO_COLUMN {
    TENANT_ID = common::OB_APP_MIN_COLUMN_ID,
    ZONE,
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
  common::ObArray<omt::TenantConfigInfo> all_config_;
  common::ObArray<omt::TenantConfigInfo>::iterator config_iter_;
  char ip_buf_[common::OB_IP_STR_BUFF];
  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualTenantParameterInfo);
};
} // namespace observer
} // namespace oceanbase

#endif

