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

#ifndef OCEANBASE_ROOTSERVER_OB_UPDATE_INDEX_STATUS_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_UPDATE_INDEX_STATUS_HELPER_H_

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
class ObUpdateIndexStatusArg;
}
namespace rootserver
{
class ObUpdateIndexStatusHelper : public ObDDLHelper
{
public:
  ObUpdateIndexStatusHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObUpdateIndexStatusArg &arg,
    obrpc::ObParallelDDLRes &res);
  virtual ~ObUpdateIndexStatusHelper();
private:
  virtual int lock_objects_() override;
  virtual int generate_schemas_() override;
  int calc_schema_version_cnt_();
  virtual int operate_schemas_() override;
  int lock_database_by_obj_name_();
  virtual int init_() override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;
private:
  const obrpc::ObUpdateIndexStatusArg &arg_;
  obrpc::ObParallelDDLRes &res_;
  const ObTableSchema *orig_index_table_schema_;
  ObTableSchema* new_data_table_schema_;
  share::schema::ObIndexStatus new_status_;
  bool index_table_exist_;
  uint64_t database_id_;
};
}
}
#endif