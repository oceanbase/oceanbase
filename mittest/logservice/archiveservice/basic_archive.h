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

#ifndef FAKE_SIMPLE_ARCHIVE_H_
#define FAKE_SIMPLE_ARCHIVE_H_
#include "lib/container/ob_se_array.h"
#include <cstdint>
#include <unordered_map>
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "lib/mysqlclient/ob_mysql_result.h"
#include "share/backup/ob_archive_struct.h"
#include "share/ob_ls_id.h"

namespace oceanbase
{
namespace unittest
{
struct TenantArchiveEnv
{
  void init() {incarnation_ = 1; round_ = 0;}
  void get_next_round(int64_t &incarnation, int64_t &round) { incarnation = incarnation_; round_++; round =round_; }
  int64_t incarnation_;
  int64_t round_;
};

typedef std::unordered_map<uint64_t, TenantArchiveEnv> TenantArchiveEnvMap;
class ObSimpleArchive : public ObSimpleClusterTestBase
{
public:
  typedef ObSEArray<uint64_t, 100> TenantArray;
  ObSimpleArchive() : ObSimpleClusterTestBase("test_archive_env_"), tenant_ids_(), env_() {}
  int prepare();
  int prepare_dest();
  int run_archive(const uint64_t tenant_id);
  int stop_archive();
  int run_observer_archive(const uint64_t tenant_id);
  int stop_observer_archive(const uint64_t tenant_id);
  int check_rs_beginning(const uint64_t tenant_id, const int64_t round_id);
  int check_rs_doing(const uint64_t tenant_id, const int64_t round_id);
  int check_rs_stop(const uint64_t tenant_id, const int64_t round_id);
  int check_archive_progress(const uint64_t tenant_id, const bool check_piece_advance = false);
  int check_ls_archive_task(const uint64_t tenant_id);
  int fake_stop_component(const uint64_t tenant_id);
  int check_task_finish(const uint64_t tenant_id);
  int fake_piece_info_after_fake_stop(const uint64_t tenant_id, const int64_t piece_interval);
  int fake_restart_component(const uint64_t tenant_id);
  int fake_remove_ls(const uint64_t tenant_id);
  int check_rs_archive_progress(const uint64_t tenant_id);
  int check_observer_component(const uint64_t tenant_id, const int64_t round_id);

  int fake_archive_attr_(const uint64_t tenant_id, share::ObTenantArchiveRoundAttr &attr);

  int create_table_();
  void signal_worker(const uint64_t tenant_id);
  virtual void TestBody() {}
  TenantArray tenant_ids_;
  TenantArchiveEnv env_;
  TenantArchiveEnvMap map_;
};
} // namespace unittest
} // namespace oceanbase

#endif
