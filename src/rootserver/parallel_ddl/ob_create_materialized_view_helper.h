/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FITNESS FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_MATERIALIZED_VIEW_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_MATERIALIZED_VIEW_HELPER_H_

#include "rootserver/parallel_ddl/ob_create_view_helper.h"
#include "rootserver/parallel_ddl/ob_table_helper.h"

namespace oceanbase
{
namespace rootserver
{
class ObCreateMaterializedViewHelper : public ObCreateViewHelper, public ObTableHelper
{
public:
  ObCreateMaterializedViewHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObCreateTableArg &arg,
    obrpc::ObCreateTableRes &res,
    ObDDLSQLTransaction *external_trans = nullptr,
    bool enable_ddl_parallel = true)
    : ObDDLHelper(schema_service, tenant_id, "[parallel create materialized view]", external_trans, enable_ddl_parallel), // 虚基
      ObCreateViewHelper(schema_service, tenant_id, arg, res, external_trans, enable_ddl_parallel),
      ObTableHelper(schema_service, tenant_id, "[parallel create materialized view]", external_trans, enable_ddl_parallel),
      task_record_() {}
  virtual ~ObCreateMaterializedViewHelper() {}

protected:
  virtual int generate_schemas_() override;
  virtual int generate_table_schema_() override { return generate_container_table_schema_(); }
  virtual int generate_aux_table_schemas_() override;
  virtual int generate_foreign_keys_() override { return OB_SUCCESS; };
  virtual int generate_sequence_object_() override { return OB_SUCCESS; };

  virtual int operate_schemas_() override;
  virtual int calc_schema_version_cnt_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;


private:
  int append_generate_schema_for_mv_();
  int generate_container_table_schema_();
  int create_schemas_for_mv_();
private:
  ObDDLTaskRecord task_record_;
};

} // end namespace rootserver
} // end namespace oceanbase

#endif // OCEANBASE_ROOTSERVER_OB_CREATE_MATERIALIZED_VIEW_HELPER_H_
