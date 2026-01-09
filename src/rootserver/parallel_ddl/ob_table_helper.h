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
#ifndef OCEANBASE_ROOTSERVER_OB_TABLE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_TABLE_HELPER_H_

#include "rootserver/parallel_ddl/ob_ddl_helper.h"
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
class ObTableHelper : public virtual ObDDLHelper
{
public:
  ObTableHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const char* parallel_ddl_type,
    ObDDLSQLTransaction *external_trans,
    bool enable_ddl_parallel);
  virtual ~ObTableHelper() {};
protected:
  int generate_audit_schema_();
  int try_replace_mock_fk_parent_table_(
      const uint64_t replace_mock_fk_parent_table_id,
      share::schema::ObMockFKParentTableSchema *&new_mock_fk_parent_table);
  int check_fk_columns_type_for_replacing_mock_fk_parent_table_(
      const share::schema::ObTableSchema &parent_table_schema,
      const share::schema::ObMockFKParentTableSchema &mock_parent_table_schema);
  virtual int generate_schemas_() override;
  int inner_generate_table_schema_(const ObCreateTableArg &arg, ObTableSchema &new_table);
  virtual int generate_table_schema_() = 0;
  virtual int generate_aux_table_schemas_() = 0;
  virtual int generate_foreign_keys_() = 0;
  virtual int generate_sequence_object_() = 0;
  int inner_create_table_(const ObString *ddl_stmt_str,
                          const uint64_t replace_mock_fk_parent_table_id);
  int create_schemas_(const ObString *ddl_stmt_str,
                      const uint64_t replace_mock_fk_parent_table_id);
  int create_tables_(const ObString *ddl_stmt_str);
  int inner_generate_aux_table_schema_(const ObCreateTableArg &arg);
  int create_sequences_();
  int deal_with_mock_fk_parent_tables_(
      const uint64_t replace_mock_fk_parent_table_id);
  int create_audits_();
  int create_tablets_();
  virtual int calc_schema_version_cnt_() override;
  void adjust_create_if_not_exist_(int &ret, bool if_not_exist, bool &do_nothing);

protected:
  common::ObArray<ObTableSchema> new_tables_;
  common::ObArray<ObSequenceSchema *> new_sequences_;
  common::ObArray<ObSAuditSchema *> new_audits_;
  common::ObArray<ObMockFKParentTableSchema *> new_mock_fk_parent_tables_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObTableHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_TABLE_HELPER_H_