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
#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_LIKE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_LIKE_HELPER_H_

#include "rootserver/parallel_ddl/ob_table_helper.h"
#include "lib/hash/ob_hashmap.h"

namespace oceanbase
{
namespace share
{
namespace schema
{
class ObMultiVersionSchemaService;
class ObMockFKParentTableSchema;
}
}
namespace rootserver
{
class MockFKParentTableNameWrapper;
class ObCreateTableLikeHelper : public ObTableHelper
{
public:
  ObCreateTableLikeHelper(share::schema::ObMultiVersionSchemaService *schema_service,
                          const uint64_t tenant_id,
                          const obrpc::ObCreateTableLikeArg &arg,
                          obrpc::ObCreateTableRes &res,
                          bool enable_ddl_parallel,
                          ObDDLSQLTransaction *external_trans);
  virtual ~ObCreateTableLikeHelper();

  virtual int init_() override;
  virtual int lock_objects_() override;
  virtual int operate_schemas_() override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;
  int check_schema_valid_(const ObTableSchema *&orig_table_schema, uint64_t &new_database_id);
  int check_and_set_parent_table_id_();
  virtual int generate_foreign_keys_() override;
  virtual int generate_table_schema_() override;
  virtual int generate_aux_table_schemas_() override;
  virtual int generate_sequence_object_() override { return OB_SUCCESS; };

private:
  const obrpc::ObCreateTableLikeArg &arg_;
  obrpc::ObCreateTableRes &res_;
  uint64_t orig_table_id_;
  uint64_t replace_mock_fk_parent_table_id_;
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableLikeHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_LIKE_HELPER_H_