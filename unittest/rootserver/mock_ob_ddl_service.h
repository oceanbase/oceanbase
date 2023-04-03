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

class MockObDDLService : public ObDDLService {
 public:
  MOCK_METHOD1(set_test_server_list,
      int(const rpc::ObAddrInfoList& server_list));
  MOCK_METHOD1(create_sys_table,
      int(share::schema::TableSchema &table_schema));
  MOCK_METHOD3(create_user_table,
      int(const bool if_not_exist, share::schema::TableSchema &table_schema, const int64_t frozen_version));
  MOCK_METHOD2(drop_table,
      int(const bool if_not_exist, const common::ObString &table_name));
  MOCK_METHOD1(create_virtual_table,
      int(share::schema::TableSchema &table_schema));
  MOCK_METHOD2(create_tenant,
      int(share::schema::TenantSchema &tenant_schema, share::schema::ObTenantResource &tenant_resource));
  MOCK_METHOD1(drop_tenant,
      int(const uint64_t tenant_id));
  MOCK_METHOD1(create_database,
      int(share::schema::DatabaseSchema &database_schema));
  MOCK_METHOD2(drop_database,
      int(const uint64_t tenant_id, const uint64_t database_id));
  MOCK_METHOD1(create_tablegroup,
      int(share::schema::TablegroupSchema &tablegroup_schema));
  MOCK_METHOD2(drop_tablegroup,
      int(const uint64_t tenant_id, const uint64_t tablegroup_id));
  MOCK_METHOD4(init_default_tenant_env,
      int(const uint64_t tenant_id, const uint64_t database_id, const uint64_t tablegroup_id, const uint64_t user_id));
  MOCK_METHOD0(refresh_schema,
      int());
  MOCK_METHOD3(is_table_exist,
      bool(const uint64_t tenant_id, const uint64_t database_id, const common::ObString &table_name));
  MOCK_METHOD1(is_table_exist,
      bool(const uint64_t table_id));
  MOCK_METHOD1(create_table,
      int(share::schema::TableSchema &table_schema));
  MOCK_METHOD4(create_partitions,
      int(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id, const int64_t total_part_num));
  MOCK_METHOD5(choose_partition_servers,
      int(const uint64_t tenant_id, const uint64_t database_id, const uint64_t table_id, const int64_t total_part_num, rpc::ObPartitionServerList &partition_server_list));
};

}  // namespace rootserver
}  // namespace oceanbase
