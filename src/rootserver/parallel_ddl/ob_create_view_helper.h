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
#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_VIEW_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_VIEW_HELPER_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
}
}
namespace obrpc
{
class ObCreateTableArg;
class ObCreateTableRes;
}
namespace rootserver
{
class ObCreateViewHelper : public ObDDLHelper
{
public:
  ObCreateViewHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObCreateTableArg &arg,
    obrpc::ObCreateTableRes &res);
  virtual ~ObCreateViewHelper();

  virtual int execute() override;
private:
  int lock_objects_();
  int generate_schemas_();
  int create_schemas_();
private:
  const obrpc::ObCreateTableArg &arg_;
  obrpc::ObCreateTableRes &res_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateViewHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_CREATE_VIEW_HELPER_H_
