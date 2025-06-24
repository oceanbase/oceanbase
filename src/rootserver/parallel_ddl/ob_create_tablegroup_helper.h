/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_TABLEGROUP_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_TABLEGROUP_H_

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
class ObCreateTablegroupArg;
}
namespace rootserver
{
class ObCreateTablegroupHelper : public ObDDLHelper
{
public:
 ObCreateTablegroupHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObCreateTablegroupArg &arg,
    obrpc::ObCreateTableGroupRes &res,
    ObDDLSQLTransaction *external_trans = nullptr);
  virtual ~ObCreateTablegroupHelper();
private:
  virtual int check_inner_stat_() override;
  virtual int init_();
  virtual int lock_objects_() override;
  virtual int generate_schemas_() override;
  virtual int calc_schema_version_cnt_() override;
  virtual int operate_schemas_() override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;
private:
  int check_tablegroup_name_();
private:
  const obrpc::ObCreateTablegroupArg &arg_;
  obrpc::ObCreateTableGroupRes &res_;
  ObTablegroupSchema* tablegroup_schema_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTablegroupHelper);
};

} // end namespace rootserver
} // end namespace oceanbase

 #endif//OCEANBASE_ROOTSERVER_OB_CREATE_TABLEGROUP_H_
