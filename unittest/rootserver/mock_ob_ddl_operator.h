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

namespace oceanbase {
namespace rootserver {

class MockObDDLOperator : public ObDDLOperator {
 public:
  MOCK_METHOD3(create_tenant,
      int(share::schema::TenantSchema &tenant_schema, share::schema::ObTenantResource &tenant_resource, common::ObMySQLTransaction &trans));
  MOCK_METHOD2(drop_tenant,
      int(const uint64_t tenant_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD2(create_database,
      int(share::schema::DatabaseSchema &database_schema, common::ObMySQLTransaction &trans));
  MOCK_METHOD3(drop_database,
      int(const uint64_t tenant_id, const uint64_t database_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD2(create_tablegroup,
      int(share::schema::TablegroupSchema &tablegroup_schema, common::ObMySQLTransaction &trans));
  MOCK_METHOD3(drop_tablegroup,
      int(const uint64_t tenant_id, const uint64_t tablegroup_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD5(init_default_tenant_env,
      int(const uint64_t tenant_id, const uint64_t database_id, const uint64_t tablegroup_id, const uint64_t user_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD2(create_table,
      int(const share::schema::TableSchema &table_schema, common::ObMySQLTransaction &trans));
  MOCK_METHOD3(init_tenant_tablegroup,
      int(const uint64_t tenant_id, const uint64_t tablegroup_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD3(init_tenant_database,
      int(const uint64_t tenant_id, const uint64_t database_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD3(init_tenant_user,
      int(const uint64_t tenant_id, const uint64_t user_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD4(init_tenant_database_priv,
      int(const uint64_t tenant_id, const uint64_t user_id, const uint64_t database_id, common::ObMySQLTransaction &trans));
  MOCK_METHOD2(init_tenant_sys_params,
      int(const uint64_t tenant_id, common::ObMySQLTransaction &trans));
};

}  // namespace rootserver
}  // namespace oceanbase
