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
class ObTableHelper : public ObDDLHelper
{
public:
  ObTableHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const char* parallel_ddl_type,
    bool enable_ddl_parallel);
  virtual ~ObTableHelper() {};
protected:
  int generate_audit_schema_(
      const common::ObArray<ObTableSchema> &new_tables,
      const common::ObArray<ObSequenceSchema *> &sequences,
      common::ObArray<ObSAuditSchema *> &new_audits);
  int try_replace_mock_fk_parent_table_(
      const uint64_t replace_mock_fk_parent_table_id,
      const common::ObArray<ObTableSchema> &new_tables,
      share::schema::ObMockFKParentTableSchema *&new_mock_fk_parent_table);
  int check_fk_columns_type_for_replacing_mock_fk_parent_table_(
      const share::schema::ObTableSchema &parent_table_schema,
      const share::schema::ObMockFKParentTableSchema &mock_parent_table_schema);
  int inner_generate_schemas_(common::ObArray<ObTableSchema> &new_tables,
                              const common::ObArray<ObSequenceSchema *> &sequences,
                              common::ObArray<ObSAuditSchema *> &new_audits);
  virtual int generate_table_schema_() = 0;
  virtual int generate_aux_table_schemas_() = 0;
  virtual int generate_foreign_keys_() = 0;
  virtual int generate_sequence_object_() = 0;
  int inner_create_table_(common::ObArray<ObTableSchema> &new_tables,
                          const ObString *ddl_stmt_str,
                          const common::ObArray<ObSequenceSchema *> &new_sequences,
                          const common::ObArray<ObSAuditSchema *> &new_audits,
                          const uint64_t replace_mock_fk_parent_table_id,
                          common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables);
  int create_schemas_(common::ObArray<ObTableSchema> &new_tables,
                      const ObString *ddl_stmt_str,
                      const common::ObArray<ObSequenceSchema *> &new_sequences,
                      const common::ObArray<ObSAuditSchema *> &new_audits,
                      const uint64_t replace_mock_fk_parent_table_id,
                      common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables);
  int create_tables_(common::ObArray<ObTableSchema> &new_tables,
                     const ObString *ddl_stmt_str);
  int create_sequences_(const common::ObArray<ObSequenceSchema *> &new_sequences);
  int deal_with_mock_fk_parent_tables_(
      const uint64_t replace_mock_fk_parent_table_id,
      const common::ObArray<ObTableSchema> &new_tables,
      common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables);
  int create_audits_(common::ObArray<ObSAuditSchema *> new_audits);
  int create_tablets_(const common::ObArray<ObTableSchema> &new_tables);
  int inner_calc_schema_version_cnt_(const common::ObArray<ObTableSchema> &new_tables,
                                     const common::ObArray<ObSequenceSchema *> &new_sequences,
                                     const common::ObArray<ObSAuditSchema *> &new_audits,
                                     const common::ObArray<ObMockFKParentTableSchema *> &new_mock_fk_parent_tables);
  void adjust_create_if_not_exist_(int &ret, bool if_not_exist, bool &do_nothing);

private:
  DISALLOW_COPY_AND_ASSIGN(ObTableHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_TABLE_HELPER_H_