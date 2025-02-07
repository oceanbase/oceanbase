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
#ifndef OCEANBASE_ROOTSERVER_OB_COMMENT_H_
#define OCEANBASE_ROOTSERVER_OB_COMMENT_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"
#include "lib/hash/ob_hashmap.h"

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
class ObSetCommentArg;
class ObSetCommenttRes;
}
namespace rootserver
{
class ObSetCommentHelper : public ObDDLHelper
{
public:
  ObSetCommentHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObSetCommentArg &arg,
    obrpc::ObParallelDDLRes &res);
  virtual ~ObSetCommentHelper();
  virtual int execute() override;
private:
  virtual int check_inner_stat_() override;
  int lock_objects_();
  int check_database_legitimacy_();
  int generate_schemas_();
  virtual int calc_schema_version_cnt_() override;
  int alter_schema_();
  int lock_databases_by_obj_name_();
  int lock_objects_by_id_();
  int lock_objects_by_name_();
  int lock_for_common_ddl_();
  int check_table_legitimacy_();
private:
  const obrpc::ObSetCommentArg &arg_;
  obrpc::ObParallelDDLRes &res_;
  uint64_t database_id_;
  uint64_t table_id_;
  const ObTableSchema* orig_table_schema_;
  ObTableSchema* new_table_schema_;
  common::ObSArray<ObColumnSchemaV2*> new_column_schemas_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObSetCommentHelper);

};

} // end namespace rootserver
} // end namespace oceanbase

#endif//OCEANBASE_ROOTSERVER_OB_COMMENT_H_