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

#ifndef _OCEANBASE_ROOTSERVER_OB_LOCATION_DDL_SERVICE_H_
#define _OCEANBASE_ROOTSERVER_OB_LOCATION_DDL_SERVICE_H_

#include "rootserver/ob_ddl_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"


namespace oceanbase
{
namespace rootserver
{
class ObLocationDDLService
{
  public:
  ObLocationDDLService(ObDDLService *ddl_service)
    : ddl_service_(ddl_service)
  {}
  virtual ~ObLocationDDLService() {}
  int create_location(const obrpc::ObCreateLocationArg &arg, const ObString *ddl_stmt_str);
  int drop_location(const obrpc::ObDropLocationArg &arg, const ObString *ddl_stmt_str);
  static int check_location_constraint(const ObTableSchema &schema);
private:
  ObDDLService *ddl_service_;
};

} // end namespace rootserver
} // end namespace oceanbase
#endif // _OCEANBASE_ROOTSERVER_OB_LOCATION_DDL_SERVICE_H_