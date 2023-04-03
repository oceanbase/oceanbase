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

class MockObBaseBootstrap : public ObBaseBootstrap {
 public:
  MOCK_METHOD1(check_bootstrap_rs_list,
      int(const obrpc::ObServerInfoList &rs_list));
  MOCK_METHOD1(create_partition,
      int(const uint64_t table_id));
};

}  // namespace rootserver
}  // namespace oceanbase

namespace oceanbase {
namespace rootserver {

class MockObPreBootstrap : public ObPreBootstrap {
 public:
  MOCK_METHOD1(prepare_bootstrap,
      int(common::ObAddr &master_rs));
  MOCK_METHOD1(check_is_all_server_empty,
      int(bool &is_empty));
  MOCK_METHOD1(wait_elect_master_partition,
      int(common::ObAddr &master_rs));
};

}  // namespace rootserver
}  // namespace oceanbase

namespace oceanbase {
namespace rootserver {

class MockObBootstrap : public ObBootstrap {
 public:
  MOCK_METHOD0(execute_bootstrap,
      int());
  MOCK_METHOD1(check_is_already_bootstrap,
      int(bool &is_bootstrap));
  MOCK_METHOD0(create_core_tables,
      int());
  MOCK_METHOD0(create_sys_tables,
      int());
  MOCK_METHOD0(create_virtual_tables,
      int());
  MOCK_METHOD0(init_system_data,
      int());
  MOCK_METHOD0(init_all_sys_stat,
      int());
  MOCK_METHOD0(wait_all_rs_online,
      int());
};

}  // namespace rootserver
}  // namespace oceanbase
