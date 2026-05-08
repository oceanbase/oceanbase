/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SERVER
#include <gtest/gtest.h>
#include <iostream>
#include <atomic>
#include <random>
#include <thread>
#include <vector>
#define private public
#define protected public

#include "simple_server/vector_index/test_vector_index_utils.h"
#include "simple_server/env/ob_simple_cluster_test_base.h"
#include "mittest/env/ob_simple_server_helper.h"
#include "share/vector_index/ob_plugin_vector_index_service.h"
#include "share/vector_index/ob_plugin_vector_index_scheduler.h"
#include "share/vector_index/ob_plugin_vector_index_adaptor.h"
#include "share/vector_index/ob_plugin_vector_index_utils.h"
#include "share/vector_index/ob_plugin_vector_index_serialize.h"
#include "storage/lob/ob_lob_util.h"
#include "storage/tx/ob_trans_service.h"

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
  int ret = OB_SUCCESS;
  const int64_t kFreezeSearchIters = 1000;
  // Phase 1: 建立租户
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

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, frequency = 1", affected_rows));
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  const std::string table_name = "t1";
  // Phase 2: 设置配置项及建表（含索引 tablet / ls 查询）
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _persist_vector_index_incremental = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='100GB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table t1 (c1 int auto_increment, embedding vector(18), primary key(c1), vector index vec_idx1(embedding) with (distance=cosine,type=hnsw_bq,lib=vsag))", affected_rows));
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

  // Phase 3: 等待 adaptor 进入 complete（后台轮询校验 frozen 状态；与后续读写并发）
  {
    MTL_SWITCH(R.tenant_id_) {
      while (OB_SUCC(ret)) {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        ObTabletID tablet_id(inc_tablet_id);
        ObLSID ls_id(table_ls_id);
        ObPluginVectorIndexAdapterGuard adaptor_guard;
        if (OB_SUCC(vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard))) {
          ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
          if (OB_NOT_NULL(adaptor) && adaptor->get_create_type() == CreateTypeComplete) {
            break;
          }
        }
        LOGI("adaptor not complete, wait for complete");
        sleep(1);
      }
      std::cout << "wait adaptor complete finish" << ",ret=" << ret << std::endl;
    }
  }

  // Phase 4: 启动多路读写线程（写满第一轮数据，查询并发跑）
  std::vector<std::thread> worker_threads;
  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kFreezeRowsPerWorker); };
      }));
  }

  std::vector<std::thread> select_threads;
  for (int i = 0; i < 2; i++) {
    select_threads.push_back(std::thread([this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->search_data(rand(), kFreezeSearchIters); }
    }));
  }

  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  int64_t total_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kFreezeRowsPerWorker*2);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ObString statistics;
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

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

    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 1);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW_BQ);
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
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kFreezeRowsPerWorker); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kFreezeRowsPerWorker*4);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

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
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW_BQ);
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

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 0, occur = 1, frequency = 0", affected_rows));
  sleep(3);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.compact_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));

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

    adaptor_guard.~ObPluginVectorIndexAdapterGuard();
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    ASSERT_EQ(incr_vec_cnt, 0);
    ASSERT_EQ(snap_vec_cnt, kFreezeRowsPerWorker*4);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.incrs_.count(), 0);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.bases_.count(), 1);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }
}

TEST_F(TestVectorIndexFreeze, heap_table)
{
  const char* tenant_name = "vdb";
  LOGI("create tenant begin");
  // 创建普通租户tt1
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
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop table if exists t1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _persist_vector_index_incremental = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='50MB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table t1 (c1 int auto_increment, embedding vector(18), primary key(c1), vector index vec_idx1(embedding) with (distance=cosine,type=hnsw_sq,lib=vsag)) ORGANIZATION HEAP", affected_rows));
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

  // Phase 3: 等待 adaptor 进入 complete（后台轮询校验；与后续读写并发）
  {
    MTL_SWITCH(R.tenant_id_) {
      while (OB_SUCC(ret)) {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        ObTabletID tablet_id(inc_tablet_id);
        ObLSID ls_id(table_ls_id);
        ObPluginVectorIndexAdapterGuard adaptor_guard;
        if (OB_SUCC(vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard))) {
          ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
          if (OB_NOT_NULL(adaptor) && adaptor->get_create_type() == CreateTypeComplete) {
            break;
          }
        }
        LOGI("adaptor not complete, wait for complete");
        sleep(1);
      }
      std::cout << "wait adaptor complete finish" << ",ret=" << ret << std::endl;
    }
  }

  std::vector<std::thread> worker_threads;
  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kFreezeRowsPerWorker); };
      }));
  }

  std::vector<std::thread> select_threads;
  for (int i = 0; i < 2; i++) {
    select_threads.push_back(std::thread([this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->search_data(rand(), kFreezeSearchIters); }
    }));
  }

  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  int64_t total_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kFreezeRowsPerWorker * 2);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ObString statistics;
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

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
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 1);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW_SQ);
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
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kFreezeRowsPerWorker); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kFreezeRowsPerWorker * 4);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

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
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW_SQ);
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

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 0, occur = 1, frequency = 0", affected_rows));
  sleep(3);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.compact_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));

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

    adaptor_guard.~ObPluginVectorIndexAdapterGuard();
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    ASSERT_EQ(incr_vec_cnt, 0);
    ASSERT_EQ(snap_vec_cnt, kFreezeRowsPerWorker * 4);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.incrs_.count(), 0);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.bases_.count(), 1);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }
}


TEST_F(TestVectorIndexFreeze, partition_table)
{
  const char* tenant_name = "vdb";
  LOGI("create tenant begin");
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
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop table if exists t1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _persist_vector_index_incremental = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='0MB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table t1 (c1 int auto_increment, embedding vector(18), primary key(c1), vector index vec_idx1(embedding) with (distance=cosine,type=hnsw,lib=vsag)) partition by key(c1) partitions 2", affected_rows));
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

  // Phase 3: 等待 adaptor 进入 complete（后台轮询校验；与后续读写并发）
  {
    MTL_SWITCH(R.tenant_id_) {
      while (OB_SUCC(ret)) {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        ObTabletID tablet_id(inc_tablet_id);
        ObLSID ls_id(table_ls_id);
        ObPluginVectorIndexAdapterGuard adaptor_guard;
        if (OB_SUCC(vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard))) {
          ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
          if (OB_NOT_NULL(adaptor) && adaptor->get_create_type() == CreateTypeComplete) {
            break;
          }
        }
        LOGI("adaptor not complete, wait for complete");
        sleep(1);
      }
      std::cout << "wait adaptor complete finish" << ",ret=" << ret << std::endl;
    }
  }

  std::vector<std::thread> worker_threads;
  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kFreezeRowsPerWorker); };
      }));
  }

  std::vector<std::thread> select_threads;
  for (int i = 0; i < 2; i++) {
    select_threads.push_back(std::thread([this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->search_data(rand(), kFreezeSearchIters); }
    }));
  }

  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  int64_t total_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kFreezeRowsPerWorker * 2);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ObString statistics;
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

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
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HNSW);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_TRUE(meta.incrs_.count() == 1);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW);
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
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kFreezeRowsPerWorker); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kFreezeRowsPerWorker * 4);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    ObTabletID tablet_id(inc_tablet_id);
    ObLSID ls_id(table_ls_id);
    ObPluginVectorIndexAdapterGuard adaptor_guard;
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_TRUE(adaptor->is_complete());
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HNSW);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    // don't not trigger rebuild, so only has incr
    ASSERT_EQ(meta.bases_.count(), 0);
    ASSERT_EQ(meta.incrs_.count(), 2);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW);
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

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 0, occur = 1, frequency = 0", affected_rows));
  sleep(3);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.compact_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));

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

    adaptor_guard.~ObPluginVectorIndexAdapterGuard();
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    ASSERT_EQ(incr_vec_cnt, 0);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.incrs_.count(), 0);
    ASSERT_EQ(adaptor->get_snap_data()->meta_.bases_.count(), 1);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HNSW);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }
}

TEST_F(TestVectorIndexFreeze, with_base)
{
  int ret = OB_SUCCESS;
  const int64_t kPreloadRows = 100;
  const int64_t kRowsPerWorkerAfterIndex = 100;
  const char* tenant_name = "vdb";
  LOGI("create tenant begin");
  int64_t affected_rows = 0;
  // ASSERT_EQ(OB_SUCCESS, create_tenant(tenant_name, "12G", "16G", false, 10));
  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, tenant_name));
  ASSERT_NE(0, R.tenant_id_);
  // ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2(tenant_name));
  uint64_t tenant_id = R.tenant_id_;
  LOGI("create tenant finish");

  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);
  const std::string table_name = "t1";

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop table if exists t1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set _persist_vector_index_incremental = true", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set ob_vector_index_active_segment_max_size='0MB'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create table t1 (c1 int auto_increment, embedding vector(18), primary key(c1)) ORGANIZATION HEAP", affected_rows));

  std::vector<std::thread> worker_threads;
  for (int i = 0; i < 1; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i - 1, kPreloadRows); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();
  int64_t total_cnt = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kPreloadRows);

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("create vector index vec_idx1 on t1(embedding) with (distance=cosine,type=hnsw_sq,lib=vsag)", affected_rows));

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

  search_data(rand(), kFreezeSearchIters);
  // Phase 3: 等待 adaptor 进入 complete（与后续读写并发）
  {
    MTL_SWITCH(R.tenant_id_) {
      while (OB_SUCC(ret)) {
        ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
        ObTabletID tablet_id(inc_tablet_id);
        ObLSID ls_id(table_ls_id);
        ObPluginVectorIndexAdapterGuard adaptor_guard;
        if (OB_SUCC(vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard))) {
          ObPluginVectorIndexAdaptor* adaptor = adaptor_guard.get_adatper();
          if (OB_NOT_NULL(adaptor) && adaptor->get_create_type() == CreateTypeComplete) {
            break;
          }
        }
        LOGI("adaptor not complete, wait for complete");
        sleep(1);
      }
      std::cout << "wait adaptor complete finish" << ",ret=" << ret << std::endl;
    }
  }

  for (int i = 0; i < 2; i++) {
    worker_threads.push_back(std::thread([i, this]() {
      int ret = OB_SUCCESS;
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kRowsPerWorkerAfterIndex); };
    }));
  }
  std::vector<std::thread> select_threads;
  for (int i = 0; i < 2; i++) {
    select_threads.push_back(std::thread([this]() {
        int ret = OB_SUCCESS;
        MTL_SWITCH(R.tenant_id_) { this->search_data(rand(), kFreezeSearchIters); }
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kPreloadRows + kRowsPerWorkerAfterIndex * 2);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ObString statistics;
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

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
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);
    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    ASSERT_EQ(meta.bases_.count(), 1);
    ASSERT_EQ(meta.incrs_.count(), 1);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW_SQ);
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
      MTL_SWITCH(R.tenant_id_) { this->insert_data(i, kRowsPerWorkerAfterIndex); };
    }));
  }
  for (auto &my_thread : worker_threads) {
    my_thread.join();
  }
  worker_threads.clear();

  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
            connection, "select count(*) val from t1",
            total_cnt));
  ASSERT_EQ(total_cnt, kPreloadRows + kRowsPerWorkerAfterIndex * 4);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.flush_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, SSH::select_varchar(
            connection, "select statistics val from oceanbase.__all_virtual_vector_index_info limit 1",
            statistics));
  std::cout << "vector_index_info:" << std::string(statistics.ptr(), statistics.length()) << std::endl;

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
    ASSERT_EQ(meta.bases_.count(), 1);
    ASSERT_EQ(meta.incrs_.count(), 2);

    for (int64_t i = 0; i < meta.incrs_.count(); ++i) {
      ObVectorIndexSegmentMeta &seg_meta = meta.incrs_.at(i);
      ASSERT_EQ(seg_meta.index_type_, ObVectorIndexAlgorithmType::VIAT_HNSW_SQ);
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

  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 0, occur = 1, frequency = 0", affected_rows));
  sleep(3);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.compact_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("call dbms_vector.compact_index('t1', 'vec_idx1')", affected_rows));
  ASSERT_EQ(check_vector_index_task_finished(), OB_SUCCESS);
  ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().get_sql_proxy().write("alter system set_tp tp_name = ERRSIM_VEC_DISABLE_MERGE, error_code = 1, occur = 1, frequency = 1", affected_rows));

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

    adaptor_guard.~ObPluginVectorIndexAdapterGuard();
    ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, tablet_id, adaptor_guard));
    adaptor = adaptor_guard.get_adatper();
    ASSERT_NE(nullptr, adaptor);
    ASSERT_EQ(adaptor->get_incr_index_type(), ObVectorIndexAlgorithmType::VIAT_HGRAPH);

    ObVectorIndexMeta meta;
    ASSERT_EQ(get_snapshot_metadata(adaptor, ls_id, meta), OB_SUCCESS);
    ASSERT_TRUE(meta.is_valid());
    ASSERT_EQ(meta.bases_.count(), 1);
    ASSERT_EQ(meta.incrs_.count(), 0);

    int64_t incr_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_inc_index_row_cnt(incr_vec_cnt), OB_SUCCESS);
    int64_t snap_vec_cnt = 0;
    ASSERT_EQ(adaptor->get_snap_index_row_cnt(snap_vec_cnt), OB_SUCCESS);
    ASSERT_EQ(incr_vec_cnt, 0);
    ASSERT_EQ(snap_vec_cnt, kPreloadRows + kRowsPerWorkerAfterIndex * 4);
    LOGI("cnt: %ld, %ld", incr_vec_cnt, snap_vec_cnt);
  }
}

/*
 * Stress ObPluginVectorIndexAdaptor::add_snap_index (mem_data_rwlock_) vs
 * serialize_snapshot (snap_data_->complete_lock_) on the same adaptor.
 * Covers the multi-threaded buffer/build path documented in add_snap_index (SQ/BQ buffer mode).
 */
TEST_F(TestVectorIndexFreeze, DISABLED_concurrent_add_snap_and_serialize_snapshot)
{
  int ret = OB_SUCCESS;

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

  ASSERT_EQ(OB_SUCCESS, get_tenant_id(R.tenant_id_, tenant_name));
  ASSERT_NE(0, R.tenant_id_);
  ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy2();
  sqlclient::ObISQLConnection *connection = nullptr;
  ASSERT_EQ(OB_SUCCESS, sql_proxy.acquire(connection));
  ASSERT_NE(nullptr, connection);

  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("alter system set vector_index_optimize_duty_time='[24:00:00, 24:00:00]'", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write("drop table if exists t1", affected_rows));
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(
      "create table t1 (c1 int auto_increment, embedding vector(512), primary key(c1), vector index vec_idx1(embedding) with (distance=cosine,type=hnsw_bq,lib=vsag)) ORGANIZATION HEAP",
      affected_rows));
  int64_t inc_tablet_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
      connection,
      "select t2.tablet_id val from oceanbase.__all_table t1 join oceanbase.__all_tablet_to_ls t2 on t1.table_id = t2.table_id where t1.table_name like '__idx_%_vec_idx1' limit 1",
      inc_tablet_id));
  ASSERT_GT(inc_tablet_id, 0);
  int64_t table_ls_id = 0;
  ASSERT_EQ(OB_SUCCESS, SSH::select_int64(
      connection,
      "select ls_id val from oceanbase.__all_tablet_to_ls where tablet_id in (select tablet_id from oceanbase.__all_table where table_name like '__idx_%_vec_idx1')",
      table_ls_id));
  ASSERT_GT(table_ls_id, 0);
  ASSERT_EQ(0, sql_proxy.close(connection, true));

  sleep(10);

  constexpr int64_t kDim = 512;
  constexpr int kBatch = 1000;
  constexpr int kRounds = 48;
  std::vector<float> vec_buf(static_cast<size_t>(kDim * kBatch));
  std::mt19937 rng(42);
  std::uniform_real_distribution<float> dist(0.0F, 1.0F);
  for (size_t i = 0; i < vec_buf.size(); ++i) {
    vec_buf[i] = dist(rng);
  }

  std::atomic<int32_t> add_snap_fail(0);
  std::atomic<int32_t> serialize_infra_fail(0);

  MTL_SWITCH(R.tenant_id_) {
    ObPluginVectorIndexService *vec_index_service = MTL(ObPluginVectorIndexService *);
    const ObTabletID inc_tid(inc_tablet_id);
    const ObLSID ls_id(table_ls_id);
    {
      ObPluginVectorIndexAdapterGuard adaptor_guard;
      ASSERT_EQ(OB_SUCCESS, vec_index_service->get_adapter_inst_guard(ls_id, inc_tid, adaptor_guard));
      ObPluginVectorIndexAdaptor *adaptor = adaptor_guard.get_adatper();
      ASSERT_NE(nullptr, adaptor);
      ASSERT_TRUE(adaptor->is_complete());
      ASSERT_EQ(OB_SUCCESS, adaptor->init_snap_data_for_build(false));
      ASSERT_EQ(OB_SUCCESS, adaptor->set_snapshot_key_prefix(
          adaptor->get_snap_tablet_id().id(),
          static_cast<uint64_t>(ObTimeUtility::fast_current_time()),
          256));
    }

    std::vector<std::thread> workers;
    const int kAddThreads = 4;
    const int kSerThreads = 2;
    for (int t = 0; t < kAddThreads; ++t) {
      workers.emplace_back([&, inc_tid, ls_id, t]() {
        int local_fail = 0;
        std::vector<int64_t> local_vids(static_cast<size_t>(kBatch));
        MTL_SWITCH(R.tenant_id_) {
          ObPluginVectorIndexService *svc = MTL(ObPluginVectorIndexService *);
          ObPluginVectorIndexAdapterGuard guard;
          int ret = svc->get_adapter_inst_guard(ls_id, inc_tid, guard);
          if (OB_SUCCESS != ret) {
            local_fail++;
          } else {
            ObPluginVectorIndexAdaptor *adp = guard.get_adatper();
            for (int r = 0; r < kRounds; ++r) {
              for (int b = 0; b < kBatch; ++b) {
                local_vids[static_cast<size_t>(b)] =
                    static_cast<int64_t>(t) * 1000000L + static_cast<int64_t>(r) * kBatch + b;
              }
              ret = adp->add_snap_index(vec_buf.data(), local_vids.data(), nullptr, 0, kBatch);
              if (OB_SUCCESS != ret) {
                local_fail++;
              }
            }
          }
        }
        add_snap_fail.fetch_add(local_fail);
      });
    }
    sleep(10);
    for (int st = 0; st < kSerThreads; ++st) {
      (void)st;
      workers.emplace_back([&, inc_tid, ls_id]() {
        int infra_fail = 0;
        MTL_SWITCH(R.tenant_id_) {
          ObPluginVectorIndexService *svc = MTL(ObPluginVectorIndexService *);
          ObPluginVectorIndexAdapterGuard guard;
          int ret = svc->get_adapter_inst_guard(ls_id, inc_tid, guard);
          if (OB_SUCCESS != ret) {
            infra_fail++;
          } else {
            ObPluginVectorIndexAdaptor *adp = guard.get_adatper();
            ObArenaAllocator arena("VecIdxSerT", OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID());
            transaction::ObTxDesc *tx_desc = nullptr;
            transaction::ObTxReadSnapshot snapshot;
            const int64_t timeout_ts =
                ObTimeUtility::fast_current_time() + storage::ObInsertLobColumnHelper::LOB_ACCESS_TX_TIMEOUT;
            ret = storage::ObInsertLobColumnHelper::start_trans(ls_id, false, timeout_ts, tx_desc);
            if (OB_SUCCESS != ret) {
              infra_fail++;
            } else {
              transaction::ObTransService *txs = MTL(transaction::ObTransService *);
              if (OB_ISNULL(txs)) {
                infra_fail++;
              } else {
                ret = txs->get_ls_read_snapshot(
                    *tx_desc, transaction::ObTxIsolationLevel::RC, ls_id, timeout_ts, snapshot);
                if (OB_SUCCESS != ret) {
                  infra_fail++;
                } else {
                  for (int r = 0; r < kRounds; ++r) {
                    ObVecIdxSnapshotDataWriteCtx vctx;
                    vctx.get_ls_id() = ls_id;
                    vctx.get_data_tablet_id() = adp->get_data_tablet_id();
                    vctx.get_snap_tablet_id() = adp->get_snap_tablet_id();
                    vctx.get_lob_meta_tablet_id() = adp->get_snap_tablet_id();
                    vctx.get_lob_piece_tablet_id() = adp->get_snap_tablet_id();
                    ObHNSWSerializeCallback::CbParam param;
                    param.vctx_ = &vctx;
                    param.allocator_ = &arena;
                    param.tmp_allocator_ = &arena;
                    param.tx_desc_ = tx_desc;
                    param.snapshot_ = &snapshot;
                    param.timeout_ = timeout_ts;
                    param.lob_inrow_threshold_ = 8192;
                    param.tablet_id_ = adp->get_snap_tablet_id();
                    param.snapshot_version_ = ObTimeUtility::fast_current_time() + r;
                    param.is_vec_tablet_rebuild_ = false;
                    (void)adp->serialize_snapshot(param);
                  }
                  ret = storage::ObInsertLobColumnHelper::end_trans(tx_desc, true, timeout_ts);
                  if (OB_SUCCESS != ret) {
                    infra_fail++;
                  }
                }
              }
            }
          }
        }
        serialize_infra_fail.fetch_add(infra_fail);
      });
    }
    for (auto &th : workers) {
      th.join();
    }
  }
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