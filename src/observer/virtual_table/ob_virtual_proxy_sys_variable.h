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

#ifndef OCEANBASE_OBSERVER_VIRTUAL_PROXY_SYS_VARIABLE_H_
#define OCEANBASE_OBSERVER_VIRTUAL_PROXY_SYS_VARIABLE_H_

#include "share/ob_virtual_table_projector.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObTableSchema;
class ObColumnSchemaV2;
class ObMultiVersionSchemaService;
class ObSysVariableSchema;
}
}
namespace common
{
class ObMySQLProxy;
class ObServerConfig;
}
namespace observer
{
class ObVirtualProxySysVariable : public common::ObVirtualTableScannerIterator
{
public:
  ObVirtualProxySysVariable();
  virtual ~ObVirtualProxySysVariable();

  int init(share::schema::ObMultiVersionSchemaService &schema_service, common::ObServerConfig *config);

  virtual int inner_get_next_row(common::ObNewRow *&row);
  virtual void reset();

private:
  enum PROXY_SYSVAR_COLUMN
  {
    NAME = common::OB_APP_MIN_COLUMN_ID,
    TENANT_ID,
    DATA_TYPE,
    VALUE,
    FLAGS,
    MODIFIED_TIME
  };

  int get_next_sys_variable();
  int get_all_sys_variable();

  bool is_inited_;
  const share::schema::ObTableSchema *table_schema_;
  const share::schema::ObTenantSchema *tenant_info_;
  common::ObServerConfig *config_;
  const share::schema::ObSysVariableSchema *sys_variable_schema_;

  DISALLOW_COPY_AND_ASSIGN(ObVirtualProxySysVariable);
};

}//end namespace observer
}//end namespace oceanbase
#endif  /*OCEANBASE_OBSERVER_VIRTUAL_PROXY_SYS_VARIABLE_H_*/
