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
#ifndef OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_HELPER_H_
#define OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_HELPER_H_

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
namespace obrpc
{
class ObCreateTableArg;
class ObCreateTableRes;
}
namespace rootserver
{
class ObCreateTableHelper : public ObTableHelper
{
public:

class MockFKParentTableNameWrapper {
public:
  MockFKParentTableNameWrapper()
   : parent_database_(),
     parent_table_() {}
  MockFKParentTableNameWrapper(
     const common::ObString &parent_database,
     const common::ObString &parent_table)
   : parent_database_(parent_database),
     parent_table_(parent_table) {}
  ~MockFKParentTableNameWrapper() {}
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  bool operator==(const MockFKParentTableNameWrapper &rv) const;
  TO_STRING_KV(K_(parent_database), K_(parent_table));
private:
  common::ObString parent_database_;
  common::ObString parent_table_;
};

public:
  ObCreateTableHelper(
    share::schema::ObMultiVersionSchemaService *schema_service,
    const uint64_t tenant_id,
    const obrpc::ObCreateTableArg &arg,
    obrpc::ObCreateTableRes &res,
    ObDDLSQLTransaction *external_trans = nullptr,
    bool enable_ddl_parallel = true);
  virtual ~ObCreateTableHelper();
  TO_STRING_KV(K_(arg),
               K_(res),
               K_(replace_mock_fk_parent_table_id),
               K_(new_tables),
               K_(new_mock_fk_parent_tables),
               K_(new_audits),
               K_(new_sequences),
               K_(has_index));
private:
  virtual int init_() override;
  virtual int lock_objects_() override;
  virtual int operate_schemas_() override;
  virtual int operation_before_commit_() override;
  virtual int clean_on_fail_commit_() override;
  virtual int construct_and_adjust_result_(int &return_ret) override;
  int add_index_name_to_cache_();

  int lock_database_by_obj_name_();
  int lock_objects_by_name_();
  int lock_objects_by_id_();
  int post_lock_objects_by_id_();
  int check_ddl_conflict_();

  int prefetch_schemas_();
  int check_and_set_database_id_();
  int check_sslog_table_exist_(
      const uint64_t tenant_id,
      const uint64_t database_id,
      const ObString &table_name);
  int check_table_name_();
  int set_tablegroup_id_();
  int check_and_set_parent_table_id_();

  virtual int generate_table_schema_() override;
  virtual int generate_aux_table_schemas_() override;
  virtual int generate_foreign_keys_() override;
  virtual int generate_sequence_object_() override;
  int get_mock_fk_parent_table_info_(
      const obrpc::ObCreateForeignKeyArg &foreign_key_arg,
      share::schema::ObForeignKeyInfo &foreign_key_info,
      share::schema::ObMockFKParentTableSchema *&new_mock_fk_parent_table_schema);
private:
  const obrpc::ObCreateTableArg &arg_;
  obrpc::ObCreateTableRes &res_;
  // replace_mock_fk_parent_table_id_ is valid if table name is same with existed mock fk parent table
  uint64_t replace_mock_fk_parent_table_id_;
  // new table schema for data/index/lob tables
  common::hash::ObHashMap<MockFKParentTableNameWrapper, share::schema::ObMockFKParentTableSchema*> new_mock_fk_parent_table_map_;
  bool has_index_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObCreateTableHelper);
};

} // end namespace rootserver
} // end namespace oceanbase


#endif//OCEANBASE_ROOTSERVER_OB_CREATE_TABLE_HELPER_H_
