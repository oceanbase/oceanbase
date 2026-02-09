/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SERVER
#include <gtest/gtest.h>
#include <iostream>
#define private public
#define protected public

#include "simple_server/vector_index/test_vector_index_utils.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"

namespace oceanbase {
namespace unittest {
class TestRunCtx {
public:
  uint64_t tenant_id_ = 0;
  int time_sec_ = 0;
};
TestRunCtx R;

using namespace oceanbase::common;
using namespace oceanbase::share;

class TestVectorIndexFreeze : public TestVectorIndexBase {
public:
  static constexpr char tenant_name[] = "vdb";

  TestVectorIndexFreeze()
      : TestVectorIndexBase()
  {
  }
  virtual ~TestVectorIndexFreeze() = default;
  void SetUp() override
  {
    TestVectorIndexBase::SetUp();
  }

private:
  DISALLOW_COPY_AND_ASSIGN(TestVectorIndexFreeze);
};

TEST_F(TestVectorIndexFreeze, simple_test)
{
  const char* tenant_name = "vdb";
  LOGI("create tenant begin");
  int64_t affected_rows = 0;
  // 创建普通租户tt1
  ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_name, "12G", "16G", false, 10));
  // 获取租户tt1的tenant_id
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, tenant_name));
  ASSERT_NE(0, R.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(tenant_name));
  uint64_t tenant_id = R.tenant_id_;
  LOGI("create tenant finish");

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  const std::string table_name = "t1";
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _persist_vector_index_incremental = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='50MB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table t1 (c1 int auto_increment, embedding vector(512), primary key(c1), vector index vec_idx1(embedding) with (distance=cosine,type=hnsw_bq,lib=vsag))", affected_rows));
  int64_t inc_tablet_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select t2.tablet_id val from oceanbase.__all_table t1 join oceanbase.__all_tablet_to_ls t2 on t1.table_id = t2.table_id where t1.table_name like '__idx_%_vec_idx1' limit 1",
            inc_tablet_id));
  ASSERT_GT(inc_tablet_id, 0);
  int64_t table_ls_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select ls_id val from oceanbase.__all_tablet_to_ls where tablet_id in (select tablet_id from oceanbase.__all_table where table_name like '__idx_%_vec_idx1')",
            table_ls_id));
  ASSERT_GT(table_ls_id, 0);

  sleep(10);

  static int64_t check_ret = 0;
  bool stop_check = false;
  std::thread * check_thread = new std::thread([inc_tablet_id, table_ls_id, &stop_check]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(R.tenant_id_) {
      while (OB_SUCC(ret) && ! stop_check) {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        ObTabletID tablet_id(inc_tablet_id);
        ObLSID ls_id(table_ls_id);
        ObPluginVectorIndexAdapterGuard adaptor_guard;
        ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
        ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
        ASSERT_NE(nullptr, adaptor);
        ASSERT_TRUE(adaptor->is_complete());
        if (adaptor->get_frozen_data()->has_frozen()) {
          if (adaptor->get_frozen_data()->ret_code_ != OB_EAGAIN) {
            ret = adaptor->get_frozen_data()->ret_code_;
            ASSERT_EQ(ret, OB_SUCCESS);
          }
        }
        sleep(1);
      }
      check_ret = ret;
      std::cout << "check thread exit, ret=" << ret << std::endl;
    }
  });

  std::vector<std::thread> worker_threads;
  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->insert_data(i, 9250); };
      }));
  }

  std::vector<std::thread> select_threads;
  for (int i = 0; i < 2; i++) {
    select_threads.push_back(std::thread([this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->search_data(rand(), 1000); }
    }));
  }

  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ASSERT_NE(nullptr, vec_index_service);
    auto& ls_map =  vec_index_service->get_ls_index_mgr_map();
    FOREACH(iter, ls_map) {
      const ObLSID &ls_id = iter->first;
      ObPluginVectorIndexMgr *ls_index_mgr = iter->second;
      ASSERT_NE(nullptr, ls_index_mgr);
      const VectorIndexAdaptorMap& partial_map = ls_index_mgr->get_partial_adapter_map();
      const VectorIndexAdaptorMap& complete_map = ls_index_mgr->get_complete_adapter_map();
      ASSERT_EQ(0, partial_map.size());
      ASSERT_EQ(3, complete_map.size());
    }
  }

  int64_t total_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, 18500);

  sleep(10);

  ObString statistics;
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    LOG_INFO("test adaptor info", KPC(adaptor));
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
    ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 1);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HGRAPH);
      ASSERT_EQ(seg_meta.has_segment_meta_row_, 1);
      ASSERT_EQ(seg_meta.reserved_, 0);
      ASSERT_EQ(check_segment_meta_row(adaptor, ls_id, seg_meta), OB_SUCCESS);
    }

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, 9250); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, 37000);

  sleep(10);

  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_EQ(meta.incrs_.count(), 2);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HGRAPH);
      ASSERT_EQ(seg_meta.has_segment_meta_row_, 1);
      ASSERT_EQ(seg_meta.reserved_, 0);
      ASSERT_EQ(check_segment_meta_row(adaptor, ls_id, seg_meta), OB_SUCCESS);
    }

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  for (auto &my_thread : select_threads) {
    my_thread.join();
  }
  select_threads.clear();
  sleep(10);

  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
    ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);

    adaptor_guard.~ObPluginVectorIndexAdapterGuard();
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    ASSERT_EQ(incr_vec_cnt, 0);
    ASSERT_EQ(snap_vec_cnt, 37000);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.incrs_.count(), 0);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.bases_.count(), 1);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  stop_check = true;
  std::cout << "signal stop" << std::endl;
  check_thread->join();
  ASSERT_EQ(check_ret, OB_SUCCESS);
  delete check_thread;
}

TEST_F(TestVectorIndexFreeze, heap_table)
{
  // const char* tenant_name = "vdb";
  // LOGI("create tenant begin");
  // // 创建普通租户tt1
  // ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_name, "12G", "16G", false, 10));
  // 获取租户tt1的tenant_id
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, tenant_name));
  ASSERT_NE(0, R.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  // ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(tenant_name));
  uint64_t tenant_id = R.tenant_id_;
  LOGI("create tenant finish");

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  const std::string table_name = "t1";
  int64_t affected_rows;
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set vector_index_optimize_duty_time='[24:00:00, 24:00:00]'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop table if exists t1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='50MB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table t1 (c1 int auto_increment, embedding vector(512), primary key(c1), vector index vec_idx1(embedding) with (distance=cosine,type=hnsw_bq,lib=vsag)) ORGANIZATION HEAP", affected_rows));
  int64_t inc_tablet_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select t2.tablet_id val from oceanbase.__all_table t1 join oceanbase.__all_tablet_to_ls t2 on t1.table_id = t2.table_id where t1.table_name like '__idx_%_vec_idx1' limit 1",
            inc_tablet_id));
  ASSERT_GT(inc_tablet_id, 0);
  int64_t table_ls_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select ls_id val from oceanbase.__all_tablet_to_ls where tablet_id in (select tablet_id from oceanbase.__all_table where table_name like '__idx_%_vec_idx1')",
            table_ls_id));
  ASSERT_GT(table_ls_id, 0);

  sleep(10);

  static int64_t check_ret = 0;
  bool stop_check = false;
  std::thread * check_thread = new std::thread([inc_tablet_id, table_ls_id, &stop_check]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(R.tenant_id_) {
      while (OB_SUCC(ret) && ! stop_check) {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        ObTabletID tablet_id(inc_tablet_id);
        ObLSID ls_id(table_ls_id);
        ObPluginVectorIndexAdapterGuard adaptor_guard;
        ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
        ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
        ASSERT_NE(nullptr, adaptor);
        ASSERT_TRUE(adaptor->is_complete());
        if (adaptor->get_frozen_data()->has_frozen()) {
          if (adaptor->get_frozen_data()->ret_code_ != OB_EAGAIN) {
            ret = adaptor->get_frozen_data()->ret_code_;
            ASSERT_EQ(ret, OB_SUCCESS);
          }
        }
        sleep(1);
      }
      check_ret = ret;
      std::cout << "check thread exit, ret=" << ret << std::endl;
    }
  });

  std::vector<std::thread> worker_threads;
  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->insert_data(i, 9250); };
      }));
  }

  std::vector<std::thread> select_threads;
  for (int i = 0; i < 2; i++) {
    select_threads.push_back(std::thread([this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->search_data(rand(), 1000); }
    }));
  }

  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ASSERT_NE(nullptr, vec_index_service);
    auto& ls_map =  vec_index_service->get_ls_index_mgr_map();
    FOREACH(iter, ls_map) {
      const ObLSID &ls_id = iter->first;
      ObPluginVectorIndexMgr *ls_index_mgr = iter->second;
      ASSERT_NE(nullptr, ls_index_mgr);
      const VectorIndexAdaptorMap& partial_map = ls_index_mgr->get_partial_adapter_map();
      const VectorIndexAdaptorMap& complete_map = ls_index_mgr->get_complete_adapter_map();
      ASSERT_EQ(0, partial_map.size());
      ASSERT_EQ(3, complete_map.size());
    }
  }

  int64_t total_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, 18500);

  sleep(10);

  ObString statistics;
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    LOG_INFO("test adaptor info", KPC(adaptor));
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
    ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 1);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HGRAPH);
      ASSERT_EQ(seg_meta.has_segment_meta_row_, 1);
      ASSERT_EQ(seg_meta.reserved_, 0);
      ASSERT_EQ(check_segment_meta_row(adaptor, ls_id, seg_meta), OB_SUCCESS);
    }

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, 9250); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, 37000);

  sleep(10);

  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 2);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HGRAPH);
      ASSERT_EQ(seg_meta.has_segment_meta_row_, 1);
      ASSERT_EQ(seg_meta.reserved_, 0);
      ASSERT_EQ(check_segment_meta_row(adaptor, ls_id, seg_meta), OB_SUCCESS);
    }

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  for (auto &my_thread : select_threads) {
    my_thread.join();
  }
  select_threads.clear();
  sleep(10);

  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
    ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);

    adaptor_guard.~ObPluginVectorIndexAdapterGuard();
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    ASSERT_EQ(incr_vec_cnt, 0);
    ASSERT_EQ(snap_vec_cnt, 37000);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.incrs_.count(), 0);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.bases_.count(), 1);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  stop_check = true;
  std::cout << "signal stop" << std::endl;
  check_thread->join();
  ASSERT_EQ(check_ret, OB_SUCCESS);
  delete check_thread;
}


TEST_F(TestVectorIndexFreeze, partition_table)
{
  // const char* tenant_name = "vdb";
  // LOGI("create tenant begin");
  // // 创建普通租户tt1
  // ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_name, "12G", "16G", false, 10));
  // 获取租户tt1的tenant_id
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, tenant_name));
  ASSERT_NE(0, R.tenant_id_);
  // 初始化普通租户tt1的sql proxy
  // ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(tenant_name));
  uint64_t tenant_id = R.tenant_id_;
  LOGI("create tenant finish");

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  const std::string table_name = "t1";
  int64_t affected_rows;
  int ret = OB_SUCCESS;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set vector_index_optimize_duty_time='[24:00:00, 24:00:00]'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop table if exists t1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='0MB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table t1 (c1 int auto_increment, embedding vector(512), primary key(c1), vector index vec_idx1(embedding) with (distance=cosine,type=hnsw_bq,lib=vsag)) partition by key(c1) partitions 2", affected_rows));
  int64_t inc_tablet_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select t2.tablet_id val from oceanbase.__all_table t1 join oceanbase.__all_tablet_to_ls t2 on t1.table_id = t2.table_id where t1.table_name like '__idx_%_vec_idx1' limit 1",
            inc_tablet_id));
  ASSERT_GT(inc_tablet_id, 0);
  int64_t table_ls_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select t2.ls_id val from oceanbase.__all_table t1 join oceanbase.__all_tablet_to_ls t2 on t1.table_id = t2.table_id where t1.table_name like '__idx_%_vec_idx1' limit 1",
            table_ls_id));
  ASSERT_GT(table_ls_id, 0);

  sleep(10);

  static int64_t check_ret = 0;
  bool stop_check = false;
  std::thread * check_thread = new std::thread([inc_tablet_id, table_ls_id, &stop_check]() {
    int ret = OB_SUCCESS;
    MTL_SWITCH(R.tenant_id_) {
      while (OB_SUCC(ret) && ! stop_check) {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        ObTabletID tablet_id(inc_tablet_id);
        ObLSID ls_id(table_ls_id);
        ObPluginVectorIndexAdapterGuard adaptor_guard;
        ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
        ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
        ASSERT_NE(nullptr, adaptor);
        ASSERT_TRUE(adaptor->is_complete());
        if (adaptor->get_frozen_data()->has_frozen()) {
          if (adaptor->get_frozen_data()->ret_code_ != OB_EAGAIN) {
            ret = adaptor->get_frozen_data()->ret_code_;
            ASSERT_EQ(ret, OB_SUCCESS);
          }
        }
        sleep(1);
      }
      check_ret = ret;
      std::cout << "check thread exit, ret=" << ret << std::endl;
    }
  });

  std::vector<std::thread> worker_threads;
  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->insert_data(i, 9250); };
      }));
  }

  std::vector<std::thread> select_threads;
  for (int i = 0; i < 2; i++) {
    select_threads.push_back(std::thread([this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->search_data(rand(), 1000); }
    }));
  }

  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='1MB'", affected_rows));
  sleep(10);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='0MB'", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ASSERT_NE(nullptr, vec_index_service);
    auto& ls_map =  vec_index_service->get_ls_index_mgr_map();
    FOREACH(iter, ls_map) {
      const ObLSID &ls_id = iter->first;
      ObPluginVectorIndexMgr *ls_index_mgr = iter->second;
      ASSERT_NE(nullptr, ls_index_mgr);
      const VectorIndexAdaptorMap& partial_map = ls_index_mgr->get_partial_adapter_map();
      const VectorIndexAdaptorMap& complete_map = ls_index_mgr->get_complete_adapter_map();
      ASSERT_EQ(0, partial_map.size());
      ASSERT_EQ(6, complete_map.size());
    }
  }

  int64_t total_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, 18500);

  sleep(10);

  ObString statistics;
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    LOG_INFO("test adaptor info", KPC(adaptor));
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
    ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 1);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HGRAPH);
      ASSERT_EQ(seg_meta.has_segment_meta_row_, 1);
      ASSERT_EQ(seg_meta.reserved_, 0);
      ASSERT_EQ(check_segment_meta_row(adaptor, ls_id, seg_meta), OB_SUCCESS);
    }

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, 9250); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='1MB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, 37000);

  sleep(10);

  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 2);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HGRAPH);
      ASSERT_EQ(seg_meta.has_segment_meta_row_, 1);
      ASSERT_EQ(seg_meta.reserved_, 0);
      ASSERT_EQ(check_segment_meta_row(adaptor, ls_id, seg_meta), OB_SUCCESS);
    }

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  for (auto &my_thread : select_threads) {
    my_thread.join();
  }
  select_threads.clear();
  sleep(10);

  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "__all_virtual_vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
    ASSERT_EQ(check_vector_index_task_success(), OB_SUCCESS);

    adaptor_guard.~ObPluginVectorIndexAdapterGuard();
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    // ASSERT_EQ(incr_vec_cnt, 0);
    // ASSERT_EQ(snap_vec_cnt, 37000);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.incrs_.count(), 0);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.bases_.count(), 1);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }

  stop_check = true;
  std::cout << "signal stop" << std::endl;
  check_thread->join();
  ASSERT_EQ(check_ret, OB_SUCCESS);
  delete check_thread;
}


}  // namespace unittest
}  // namespace oceanbase

int main(int argc, char **argv)
{
  char *log_level = (char*)"INFO";
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);

  LOG_INFO("main>>>");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}