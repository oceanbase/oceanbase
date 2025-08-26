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

#ifndef _OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_DDL_SERVICE_H_
#define _OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_DDL_SERVICE_H_

#include "rootserver/ob_ddl_service.h"
#include "share/ob_rpc_struct.h"
#include "share/schema/ob_schema_service.h"


namespace oceanbase
{
namespace rootserver
{
class ObObjPrivMysqlDDLService
{
public:
ObObjPrivMysqlDDLService(ObDDLService *ddl_service)
    : ddl_service_(ddl_service)
  {}
  virtual ~ObObjPrivMysqlDDLService() {}
  int grant_object(const share::schema::ObObjMysqlPrivSortKey &object_key,
                   const ObPrivSet priv_set,
                   const uint64_t option,
                   share::schema::ObSchemaGetterGuard &schema_guard,
                   const common::ObString &grantor = "",
                   const common::ObString &grantor_host = "");
  int revoke_object(const share::schema::ObObjMysqlPrivSortKey &object_key,
                    const ObPrivSet priv_set,
                    const common::ObString &grantor = "",
                    const common::ObString &grantor_host = "");
private:
  ObDDLService *ddl_service_;
};
}
}
#endif // _OCEANBASE_ROOTSERVER_OB_OBJPRIV_MYSQL_DDL_SERVICE_H_