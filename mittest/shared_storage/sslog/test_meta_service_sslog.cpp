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
#include <gtest/gtest.h>
#include <thread>
#include <iostream>
#define protected public
#define private public

#include "storage/schema_utils.h"
#include "storage/incremental/sslog/ob_sslog_table_proxy.h"
#include "storage/incremental/share/ob_shared_meta_iter_guard.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "lib/string/ob_string.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "share/scn.h"
#include "mittest/simple_server/env/ob_simple_server_restart_helper.h"
#include "mittest/shared_storage/atomic_protocol/test_ss_atomic_util.h"
#include "storage/test_tablet_helper.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/init_basic_struct.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/incremental/garbage_collector/ob_ss_garbage_collector_service.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"

namespace oceanbase
{
namespace sslog
{

oceanbase::unittest::ObMockPalfKV PALF_KV;

int get_sslog_table_guard(const ObSSLogTableType type,
                          const int64_t tenant_id,
                          ObSSLogProxyGuard &guard)
{
  int ret = OB_SUCCESS;

  switch (type)
  {
    case ObSSLogTableType::SSLOG_TABLE: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogTableProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogTableProxy *sslog_table_proxy = new (proxy) ObSSLogTableProxy(tenant_id);
        if (OB_FAIL(sslog_table_proxy->init())) {
          SSLOG_LOG(WARN, "fail to inint", K(ret));
        } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        }
      }
      break;
    }
    case ObSSLogTableType::SSLOG_PALF_KV: {
      void *proxy = share::mtl_malloc(sizeof(ObSSLogKVProxy), "ObSSLogTable");
      if (nullptr == proxy) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
      } else {
        ObSSLogKVProxy *sslog_kv_proxy = new (proxy) ObSSLogKVProxy(&PALF_KV);
        // if (OB_FAIL(sslog_kv_proxy->init(GCONF.cluster_id, tenant_id))) {
        //   SSLOG_LOG(WARN, "init palf kv failed", K(ret));
        // } else {
          guard.set_sslog_proxy((ObISSLogProxy *)proxy);
        // }
      }
      break;
    }
    default: {
      ret = OB_INVALID_ARGUMENT;
      SSLOG_LOG(WARN, "invalid sslog type", K(type));
      break;
    }
  }

  return ret;
}
} // namespace sslog
} // namespace oceanbase

uint64_t tenant_id = 0;

namespace oceanbase
{

char *shared_storage_info = NULL;
namespace common {
bool is_shared_storage_sslog_table(const uint64_t tid)
{
  return OB_ALL_SSLOG_TABLE_TID == tid;
}

bool is_shared_storage_sslog_exist()
{
  return true;
}
}

namespace storage {

static int64_t lease_epoch = 1;

static bool global_is_sswriter = true;
void mock_switch_sswriter()
{
  ATOMIC_INC(&lease_epoch);
  TRANS_LOG(INFO, "mock switch sswriter", K(lease_epoch));
}

int ObSSWriterService::check_lease(
    const ObSSWriterKey &key,
    bool &is_sswriter,
    int64_t &epoch)
{
  is_sswriter = global_is_sswriter;
  epoch = ATOMIC_LOAD(&lease_epoch);
  TRANS_LOG(INFO, "check lease sswriter", K(lease_epoch));
  return OB_SUCCESS;
}

int ObSSWriterService::get_sswriter_addr(
    const ObSSWriterKey &key,
    ObSSWriterAddr &sswriter_addr,
    const int64_t timeout_us)
{
  int ret = OB_SUCCESS;
  sswriter_addr.addr_ = GCTX.self_addr();
  return ret;
}

// int ObTablet::check_meta_addr() const
// {
//   int ret = OB_SUCCESS;
//   return ret;
// }

static const uint64_t INITED_TABLET_ID_ = 202020;
static uint64_t init_tablet_id_ = INITED_TABLET_ID_;

class MockSSTableGenerator
{
private:
  void generate_table_key_(
                           const ObITable::TableType &type,
                           const ObTabletID &tablet_id,
                           const int64_t base_version,
                           const int64_t snapshot_version,
                           ObITable::TableKey &table_key);
public:
  int mock_sstable(
                   ObArenaAllocator &allocator,
                   const ObTableSchema &table_schema,
                   const ObITable::TableType &type,
                   const ObTabletID &tablet_id,
                   const int64_t base_version,
                   const int64_t snapshot_version,
                   ObTableHandleV2 &table_handle);
};

void MockSSTableGenerator::generate_table_key_(
    const ObITable::TableType &type,
    const ObTabletID &tablet_id,
    const int64_t base_version,
    const int64_t snapshot_version,
    ObITable::TableKey &table_key)
{
  table_key.reset();
  table_key.tablet_id_ = tablet_id;
  table_key.table_type_ = type;
  table_key.version_range_.base_version_ = base_version;
  table_key.version_range_.snapshot_version_ = snapshot_version;
}

int MockSSTableGenerator::mock_sstable(
  ObArenaAllocator &allocator,
  const ObTableSchema &table_schema,
  const ObITable::TableType &type,
  const ObTabletID &tablet_id,
  const int64_t base_version,
  const int64_t snapshot_version,
  ObTableHandleV2 &table_handle)
{
  int ret = OB_SUCCESS;

  ObITable::TableKey table_key;
  generate_table_key_(type, tablet_id, base_version, snapshot_version, table_key);

  ObTabletCreateSSTableParam param;
  ObSSTable *sstable = nullptr;

  ObStorageSchema storage_schema;
  if (OB_FAIL(storage_schema.init(allocator, table_schema, lib::Worker::CompatMode::MYSQL))) {
    LOG_WARN("failed to init storage schema", K(ret));
  } else if (OB_FAIL(param.init_for_empty_major_sstable(tablet_id, storage_schema, 100, -1, false, false))) {
    LOG_WARN("failed to build create sstable param", K(ret), K(table_key));
  } else {
    param.table_key_ = table_key;
    param.max_merged_trans_version_ = 200;
    param.filled_tx_scn_ = table_key.get_end_scn();
    if (OB_FAIL(ObTabletCreateDeleteHelper::create_sstable(param, allocator, table_handle))) {
      LOG_WARN("failed to create sstable", K(param));
    }
  }

  if (FAILEDx(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  }
  return ret;
}


}

namespace unittest
{

using namespace common;
using namespace oceanbase::blocksstable;
using namespace oceanbase::storage;
using namespace oceanbase::logservice;

class TestRunCtx
{
public:
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;
  int64_t time_sec_ = 0;
};

TestRunCtx RunCtx;

#define EXE_SQL(sql_str)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign(sql_str));                       \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

#define EXE_SQL_FMT(...)                                            \
  ASSERT_EQ(OB_SUCCESS, sql.assign_fmt(__VA_ARGS__));               \
  ASSERT_EQ(OB_SUCCESS, sql_proxy.write(sql.ptr(), affected_rows));

class ObTestSSLogMetaService : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  ObTestSSLogMetaService() : ObSimpleClusterTestBase("test_shared_storage_meta_service_", "50G", "50G", "50G")
  {
    const int64_t LS_ID = 1111;
    ObLSID ls_id(LS_ID);
    ls_id_ = ls_id;
  }
  void wait_sys_to_leader()
  {
    share::ObTenantSwitchGuard tenant_guard;
    int ret = OB_ERR_UNEXPECTED;
    ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(1));
    ObLS *ls = nullptr;
    ObLSID ls_id(ObLSID::SYS_LS_ID);
    ObLSHandle handle;
    ObLSService *ls_svr = MTL(ObLSService *);
    EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
    ls = handle.get_ls();
    ASSERT_NE(nullptr, ls);
    ASSERT_EQ(ls_id, ls->get_ls_id());
    for (int i=0; i<100; i++) {
      ObRole role;
      int64_t proposal_id = 0;
      ASSERT_EQ(OB_SUCCESS, ls->get_log_handler()->get_role(role, proposal_id));
      if (role == ObRole::LEADER) {
        ret = OB_SUCCESS;
        break;
      }
      ob_usleep(10 * 1000);
    }
    ASSERT_EQ(OB_SUCCESS, ret);
  }

  void create_test_tenant(uint64_t &tenant_id)
  {
    TRANS_LOG(INFO, "create_tenant start");
    wait_sys_to_leader();
    int ret = OB_SUCCESS;
    int retry_cnt = 0;
    do {
      if (OB_FAIL(create_tenant("tt1"))) {
        TRANS_LOG(WARN, "create_tenant fail, need retry", K(ret));
        ob_usleep(15 * 1000 * 1000); // 15s
      } else {
        break;
      }
      retry_cnt++;
    } while (OB_FAIL(ret) && retry_cnt < 10);
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(OB_SUCCESS, get_tenant_id(tenant_id));
    ASSERT_EQ(OB_SUCCESS, get_curr_simple_server().init_sql_proxy2());
  }
  const ObTabletID get_next_tablet_id();
  int build_update_table_store_param_(ObArenaAllocator &allocator,
                                      const ObTableSchema &schema,
                                      const ObLSHandle &ls_handle,
                                      const ObTabletHandle &tablet_handle,
                                      const compaction::ObMergeType &merge_type,
                                      ObTableHandleV2 &table_handle, // for sstable life
                                      ObUpdateTableStoreParam &param);
public:
  ObLSID ls_id_;
};

const ObTabletID ObTestSSLogMetaService::get_next_tablet_id()
{
  return ObTabletID(init_tablet_id_++);
}

int ObTestSSLogMetaService::build_update_table_store_param_(ObArenaAllocator &allocator,
                                                       const ObTableSchema &schema,
                                                       const ObLSHandle &ls_handle,
                                                       const ObTabletHandle &tablet_handle,
                                                       const compaction::ObMergeType &merge_type,
                                                       ObTableHandleV2 &table_handle, // for sstable life
                                                       ObUpdateTableStoreParam &param)
{
  int ret = OB_SUCCESS;
  ObITable::TableType table_type = merge_type == compaction::ObMergeType::MDS_MINOR_MERGE ? ObITable::TableType::MDS_MINOR_SSTABLE : ObITable::TableType::MAJOR_SSTABLE;
  MockSSTableGenerator sstable_gen;
  ObSSTable *sstable = nullptr;
  if (OB_FAIL(sstable_gen.mock_sstable(
      allocator, schema,
      table_type,
      tablet_handle.get_obj()->get_tablet_id(), 0, 200, table_handle))) {
    LOG_WARN("failed to generate new sstable", K(ret), K(schema), KPC(tablet_handle.get_obj()));
  } else if (OB_FAIL(table_handle.get_sstable(sstable))) {
    LOG_WARN("failed to get sstable", K(ret), K(table_handle));
  } else if (OB_ISNULL(sstable)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("sstable should not be nullptr");
  } else {
    SCN clog_checkpoint_scn = sstable->get_end_scn();
    param.snapshot_version_ = tablet_handle.get_obj()->get_snapshot_version();
    param.multi_version_start_ = tablet_handle.get_obj()->get_multi_version_start();

    ObStorageSchema *schema_on_tablet = NULL;
    if (OB_FAIL(tablet_handle.get_obj()->load_storage_schema(allocator, schema_on_tablet))) {
      LOG_WARN("failed to load storage schema", K(ret), K(tablet_handle));
    } else {
      param.storage_schema_ = schema_on_tablet;
    }

    param.rebuild_seq_ = ls_handle.get_ls()->get_rebuild_seq();
    const bool need_check_sstable = true;
    param.ddl_info_.update_with_major_flag_ = false;

    param.sstable_ = sstable;
    param.allow_duplicate_sstable_ = true;

    if (FAILEDx(param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
            need_check_sstable,
            true /*need_check_transfer_seq*/)))) {
      LOG_WARN("failed to init with ha info", KR(ret));
    } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                      merge_type,
                      clog_checkpoint_scn,
                      false /*need_report*/,
                      tablet_handle.get_obj()->has_truncate_info())))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else {
      LOG_INFO("success to init ObUpdateTableStoreParam", KR(ret), K(param), KPC(param.sstable_));
    }
  }
  return ret;
}

TEST_F(ObTestSSLogMetaService, test_create_ls)
{
  TRANS_LOG(INFO, "create tenant start");
  create_test_tenant(tenant_id);
  TRANS_LOG(INFO, "create tenant end");

  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  ObGarbageCollector *gc_service = MTL(logservice::ObGarbageCollector *);
  ASSERT_EQ(true, gc_service != NULL);
  gc_service->stop();
  ObSSGarbageCollectorService *ss_gc_service = MTL(ObSSGarbageCollectorService *);
  ASSERT_EQ(true, ss_gc_service != NULL);
  ss_gc_service->stop();

  int ret = OB_SUCCESS;
  ObLSService *ls_svr = MTL(ObLSService *);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;

  const int64_t TABLET_ID = 49401;
  ObTabletID tablet_id(TABLET_ID);
  ObTabletHandle tablet_hdl;
  ObTablet *tablet =NULL;

  global_is_sswriter = false;

  // create ls
  ASSERT_EQ(OB_SUCCESS, gen_create_ls_arg(tenant_id, ls_id, arg));
  ASSERT_EQ(OB_SUCCESS, ls_svr->create_ls(arg));
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_ls(arg));
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ASSERT_EQ(ls_id, ls->get_ls_id());

  // get ls meta
  ObSSLSMeta ls_meta;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, ls_meta));
  ASSERT_EQ(ls_id, ls_meta.ls_id_);

  // get inner tablet
  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  const SCN transfer_scn = SCN::min_scn();
  SCN row_scn;
  TRANS_LOG(INFO, "start to get tablet 1");
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_from_ss_(ls_id,
                                                      tablet_id,
                                                      transfer_scn,
                                                      allocator,
                                                      tablet_hdl,
                                                      row_scn));
  ASSERT_NE(nullptr, tablet_hdl.get_obj());
  ASSERT_EQ(tablet_id, tablet_hdl.get_obj()->get_tablet_id());

  LOG_INFO("test_create_ls finish");
}

TEST_F(ObTestSSLogMetaService, test_update_ls_meta)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  LOG_INFO("test_update_ls_meta");
  uint64_t tenant_id = MTL_ID();
  ObLSService *ls_svr = MTL(ObLSService*);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObCreateLSArg arg;
  ObLSHandle handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;

  // 1. get origin ls meta
  ObSSLSMeta orig_ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, orig_ls_meta));
  ASSERT_EQ(ls_id, orig_ls_meta.ls_id_);


  global_is_sswriter = true;
  // 2. get cache ls meta
  ObSSLSMeta *cache_ls_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta_cache_(ls_id, cache_ls_meta));
  // cache is disabled
  ASSERT_EQ(NULL, cache_ls_meta);
  // ASSERT_EQ(ls_id, cache_ls_meta->ls_id_);
  LOG_INFO("get ls meta", K(orig_ls_meta), KPC(cache_ls_meta));

  // 3. update ls gc state
  SCN offline_scn = SCN::base_scn();
  ASSERT_EQ(OB_SUCCESS, meta_svr->update_ls_gc_state(ls_id,
                                                     LSGCState::LS_OFFLINE,
                                                     offline_scn));

  // ASSERT_EQ(cache_ls_meta->offline_scn_, offline_scn);
  // ASSERT_EQ(cache_ls_meta->gc_state_, LSGCState::LS_OFFLINE);

  // 4. clean cache and read from share storge
  cache_ls_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->ls_ssmeta_cache_.clear());
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta_cache_(ls_id, cache_ls_meta));
  ASSERT_EQ(nullptr, cache_ls_meta);

  ObSSLSMeta updated_ls_meta;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta(ls_id, updated_ls_meta));
  ASSERT_EQ(ls_id, updated_ls_meta.ls_id_);
  ASSERT_EQ(updated_ls_meta.offline_scn_, offline_scn);
  ASSERT_EQ(updated_ls_meta.gc_state_, LSGCState::LS_OFFLINE);
}

TEST_F(ObTestSSLogMetaService, test_acquire_first_created_ss_tablet_)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));

  int ret = OB_SUCCESS;
  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = TestTabletHelper::create_tablet(ls_handle, cur_tablet_id, schema, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle orig_tablet_handle;
  ret = ls->get_tablet(cur_tablet_id, orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObUpdateTableStoreParam update_param;
  ObTableHandleV2 table_handle;
  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObTabletHandle new_tablet_handle;
  ret = meta_svr->acquire_first_created_ss_tablet_(
    allocator,
    orig_tablet_handle,
    new_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);
}

TEST_F(ObTestSSLogMetaService, test_create_tablet)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  LOG_INFO("test_create_tablet");
  ObLSID ls_id = ls_id_;
  ObTabletID tablet_id = get_next_tablet_id();
  ObTablet *tablet =NULL;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle handle;
  ObLS *ls = NULL;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  // 1. there is no ss tablet.
  const SCN transfer_scn = SCN::min_scn();
  ObTabletHandle tablet_hdl1;
  SCN row_scn;
  ASSERT_EQ(OB_OBJECT_NOT_EXIST, meta_svr->get_tablet_from_ss_(ls_id,
                                                               tablet_id,
                                                               transfer_scn,
                                                               allocator,
                                                               tablet_hdl1,
                                                               row_scn));

  ObTabletHandle tablet_hdl;
  ASSERT_EQ(OB_TABLET_NOT_EXIST, meta_svr->get_tablet(ls_id,
                                                      tablet_id,
                                                      transfer_scn,
                                                      allocator,
                                                      tablet_hdl));
  // 2. create tablet while there is no ss tablet.
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
  ls = handle.get_ls();
  ASSERT_NE(nullptr, ls);
  // create the tablet local
  share::schema::ObTableSchema table_schema;
  uint64_t table_id = 12345;
  ASSERT_EQ(OB_SUCCESS, build_test_schema(table_schema, table_id));
  ASSERT_EQ(OB_SUCCESS, TestTabletHelper::create_tablet(handle, tablet_id, table_schema, allocator));
  // create the tablet at shared storage
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id,
                                                tablet_id,
                                                transfer_scn));

  // 3. check the created tablet
  ObTabletHandle tablet_hdl2;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet_from_ss_(ls_id,
                                                      tablet_id,
                                                      transfer_scn,
                                                      allocator,
                                                      tablet_hdl2,
                                                      row_scn));
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(ls_id,
                                             tablet_id,
                                             transfer_scn,
                                             allocator,
                                             tablet_hdl));
  ASSERT_EQ(tablet_id, tablet_hdl.get_obj()->get_tablet_id());

  // 4. create again while there is ss tablet.
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id,
                                                tablet_id,
                                                transfer_scn));

}

// for first time upload
TEST_F(ObTestSSLogMetaService, test_update_tablet_table_store)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObTabletHandle orig_tablet_handle;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObLSID ls_id = ls_id_;
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = ls->get_tablet(ObTabletID(INITED_TABLET_ID_), orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema); // same as table_schema when create tablet
  ObUpdateTableStoreParam update_param;
  ObTableHandleV2 table_handle;
  ObTabletID tablet_id = ObTabletID(INITED_TABLET_ID_);
  const SCN transfer_scn = SCN::min_scn();
  int64_t sstable_op_id = 100;
  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  // create the tablet at shared storage
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id_,
                                                tablet_id,
                                                transfer_scn));

  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      tablet_id,
      transfer_scn,
      ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
      sstable_op_id,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  SCN read_scn;
  ret = meta_svr->get_max_committed_meta_scn(read_scn);
  ASSERT_EQ(OB_SUCCESS, ret);
  ObSSMetaReadParam read_param;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    ObSSLogMetaType::SSLOG_TABLET_META,
                                    ls_id_,
                                    tablet_id,
                                    transfer_scn);
  ObRawMetaRow row;
  ret = meta_svr->get_raw_meta_row(
      read_param,
      read_scn,
      allocator,
      row);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("read raw meta row", K(row));

  ObSSLogMetaKey sslog_meta_key;
  ObSSMetaUpdateMetaInfo meta_info;
  ObAtomicExtraInfo extra_info;
  ObSSTabletSSLogValue value;

  ObMetaDiskAddr tablet_addr;
  uint64_t op_id = 1;
  int64_t pos = 0;
  if (OB_FAIL(value.deserialize_meta_info(row.meta_value_.ptr(),
                                          row.meta_value_.length(),
                                          pos))) {
    LOG_WARN("deserialize meta info failed", K(ret));
  } else if (FALSE_IT(meta_info = value.get_meta_info())) {
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(ObAtomicTabletMetaFile::get_tablet_addr(ls_id,
                                                             tablet_id,
                                                             op_id,
                                                             row.meta_value_.length(),
                                                             tablet_addr))) {
    LOG_WARN("get tablet addr failed", K(ret));
  } else if (OB_FAIL(value.deserialize(allocator,
                                       tablet_addr,
                                       row.meta_value_.ptr(),
                                       row.meta_value_.length(),
                                       pos))) {
    LOG_WARN("deserialize tablet fail", K(ret), K(tablet_addr), K(pos));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(extra_info.deserialize(row.extra_info_.ptr(),
                                            row.extra_info_.length(),
                                            pos))) {
    LOG_WARN("get extra info failed", K(ret), K(row));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(sslog_meta_key.deserialize(row.meta_key_.ptr(),
                                                row.meta_key_.length(),
                                                pos))) {
    LOG_WARN("deserialize meta key fail", K(ret), K(row));
  }
  ASSERT_EQ(ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE, meta_info.update_reason_);
  ASSERT_EQ(sstable_op_id, meta_info.sstable_op_id_);
  LOG_INFO("get the result first upload", K(row), K(sslog_meta_key), K(meta_info), K(value), K(extra_info));
}

TEST_F(ObTestSSLogMetaService, test_update_tablet_table_store_update)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  const SCN transfer_scn = SCN::min_scn();
  ObLSID ls_id = ls_id_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = TestTabletHelper::create_tablet(ls_handle, cur_tablet_id, schema, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id_,
                                                cur_tablet_id,
                                                transfer_scn));

  LOG_INFO("test_meta_service debug : create tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  ObTabletHandle orig_tablet_handle;
  ret = ls->get_tablet(cur_tablet_id, orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObUpdateTableStoreParam update_param;
  ObTableHandleV2 table_handle;
  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("test_meta_service debug : ", K(orig_tablet_handle.get_obj()->get_pointer_handle()));
  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      cur_tablet_id,
      orig_tablet_handle.get_obj()->get_reorganization_scn(),
      ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
      100,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("test_meta_service debug : reg ss tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MAJOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      cur_tablet_id,
      orig_tablet_handle.get_obj()->get_reorganization_scn(),
      ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
      100,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = build_update_table_store_param_(
    allocator,
    schema,
    ls_handle,
    orig_tablet_handle,
    compaction::ObMergeType::MDS_MINOR_MERGE,
    table_handle,
    update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  ret = meta_svr->update_tablet_table_store(
      ls_id_,
      cur_tablet_id,
      orig_tablet_handle.get_obj()->get_reorganization_scn(),
      ObMetaUpdateReason::TABLET_COMPACT_ADD_MDS_MINOR_SSTABLE,
      100,
      update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  LOG_INFO("test_meta_service debug : update ss tablet success", K(ret), K(ls_id_), K(cur_tablet_id));
}

TEST_F(ObTestSSLogMetaService, test_tablet_meta_snapshot_read)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  const SCN transfer_scn = SCN::min_scn();
  ObLSID ls_id = ls_id_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;
  ObTablet *tablet = NULL;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  // 1. get max committed scn before write
  SCN before_write_scn;
  SCN after_create_scn;
  SCN after_update_scn;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(before_write_scn));

  LOG_INFO("before write", K(before_write_scn));

  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = TestTabletHelper::create_tablet(ls_handle, cur_tablet_id, schema, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id_,
                                                cur_tablet_id,
                                                transfer_scn));

  EXPECT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(after_create_scn));
  LOG_INFO("after create", K(after_create_scn));

  ObTabletHandle orig_tablet_handle;
  ret = ls->get_tablet(cur_tablet_id, orig_tablet_handle);
  ASSERT_EQ(OB_SUCCESS, ret);

  ObUpdateTableStoreParam update_param;
  ObTableHandleV2 table_handle;
  ret = build_update_table_store_param_(allocator,
                                        schema,
                                        ls_handle,
                                        orig_tablet_handle,
                                        compaction::ObMergeType::MAJOR_MERGE,
                                        table_handle,
                                        update_param);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = meta_svr->update_tablet_table_store(ls_id_,
                                            cur_tablet_id,
                                            orig_tablet_handle.get_obj()->get_reorganization_scn(),
                                            ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
                                            100,
                                            update_param);
  ASSERT_EQ(OB_SUCCESS, ret);

  EXPECT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(after_update_scn));
  LOG_INFO("after update", K(after_update_scn));

  // 2. check the three version
  EXPECT_EQ(true, after_create_scn > before_write_scn);
  EXPECT_EQ(true, after_update_scn > after_create_scn);

  // 3. read with snapshot
  ObSSMetaReadParam read_param;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    ObSSLogMetaType::SSLOG_TABLET_META,
                                    ls_id,
                                    cur_tablet_id,
                                    transfer_scn);
  EXPECT_EQ(true, read_param.is_valid());

  ObTabletHandle tablet_handle;
  EXPECT_EQ(OB_TABLET_NOT_EXIST, meta_svr->get_tablet(read_param,
                                                      before_write_scn,
                                                      allocator,
                                                      tablet_handle));
  LOG_INFO("read content of not exist tablet", K(tablet_handle));

  EXPECT_EQ(OB_SUCCESS, meta_svr->get_tablet(read_param,
                                             after_create_scn,
                                             allocator,
                                             tablet_handle));
  LOG_INFO("read content after created", K(tablet_handle));

  EXPECT_EQ(OB_SUCCESS, meta_svr->get_tablet(read_param,
                                             after_update_scn,
                                             allocator,
                                             tablet_handle));
  LOG_INFO("read content after updated", K(tablet_handle));

  // 4. read with version range
  ObMetaVersionRange range;
  range.version_start_ = before_write_scn;
  range.version_end_ = after_update_scn;

  ObSSTabletIterator *iter = nullptr;
  ObSSMetaIterGuard<ObSSTabletIterator> iter_guard;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_tablet(read_param,
                                             range,
                                             iter_guard));
  iter_guard.get_iter(iter);
  LOG_INFO("read with version range", K(range));

  ObAtomicExtraInfo extra_info;
  EXPECT_EQ(OB_SUCCESS, iter->get_next(allocator,
                                       tablet_handle,
                                       extra_info));
  LOG_INFO("iter first one", K(extra_info), K(tablet_handle));

  EXPECT_EQ(OB_SUCCESS, iter->get_next(allocator,
                                       tablet_handle,
                                       extra_info));
  LOG_INFO("iter second one", K(extra_info), K(tablet_handle));

  EXPECT_EQ(OB_SUCCESS, iter->get_next(allocator,
                                       tablet_handle,
                                       extra_info));
  LOG_INFO("iter third one", K(extra_info), K(tablet_handle));

  EXPECT_EQ(OB_ITER_END, iter->get_next(allocator,
                                        tablet_handle,
                                        extra_info));
  LOG_INFO("iter forth one", K(extra_info), K(tablet_handle));
}

TEST_F(ObTestSSLogMetaService, test_ls_ids_snapshot_read)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  SCN curr_scn;
  // 1. get read snapshot
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(curr_scn));
  LOG_INFO("current meta scn", K(curr_scn));

  // 2. get the iter
  ObSSMetaReadParam read_param;
  read_param.set_ls_level_param(ObSSMetaReadParamType::TENANT_PREFIX,
                                ObSSMetaReadResultType::READ_ONLY_KEY,
                                ObSSLogMetaType::SSLOG_LS_META,
                                ls_id_);
  EXPECT_EQ(true, read_param.is_valid());
  ObSSLSIDIterator *iter = nullptr;
  ObSSMetaIterGuard<ObSSLSIDIterator> iter_guard;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_ls_ids(read_param,
                                             curr_scn,
                                             iter_guard));
  LOG_INFO("read ls ids with snapshot", K(curr_scn));
  iter_guard.get_iter(iter);
  while (OB_SUCC(iter->get_next(ls_id))) {
    LOG_INFO("get ls ids", K(ls_id));
  }
}

TEST_F(ObTestSSLogMetaService, test_tablet_ids_snapshot_read)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  ObLSID ls_id;
  ObTabletID tablet_id;
  SCN transfer_scn;

  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  SCN curr_scn;
  // 1. get read snapshot
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(curr_scn));
  LOG_INFO("current meta scn", K(curr_scn));

  // 2. get the iter
  ObSSMetaReadParam read_param;
  read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                ObSSMetaReadResultType::READ_ONLY_KEY,
                                ObSSLogMetaType::SSLOG_TABLET_META,
                                ls_id_);
  EXPECT_EQ(true, read_param.is_valid());
  ObSSTabletIDIterator *iter = nullptr;
  ObSSMetaIterGuard<ObSSTabletIDIterator> iter_guard;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_tablet_ids(read_param,
                                                 curr_scn,
                                                 iter_guard));
  LOG_INFO("read tablet ids with snapshot", K(curr_scn));
  iter_guard.get_iter(iter);
  while (OB_SUCC(iter->get_next(ls_id,
                                tablet_id,
                                transfer_scn))) {
    LOG_INFO("get tablet ids", K(ls_id), K(tablet_id), K(transfer_scn));
  }
}

TEST_F(ObTestSSLogMetaService, test_get_tablet_iter)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  ObTablet *tablet = nullptr;
  ObAtomicExtraInfo extra_info;
  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  SCN curr_scn;
  // 1. get read snapshot
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(curr_scn));
  LOG_INFO("current meta scn", K(curr_scn));

  // 2. get the iter
  ObSSMetaReadParam read_param;
  read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                ObSSLogMetaType::SSLOG_TABLET_META,
                                ls_id_);
  EXPECT_EQ(true, read_param.is_valid());
  ObSSTabletIterator *iter = nullptr;
  ObSSMetaIterGuard<ObSSTabletIterator> iter_guard;
  ObTabletHandle tablet_handle;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_tablet_iter(read_param,
                                                  curr_scn,
                                                  iter_guard));
  LOG_INFO("read tablet iter with snapshot", K(curr_scn));
  iter_guard.get_iter(iter);
  while (OB_SUCC(iter->get_next(allocator,
                                tablet_handle,
                                extra_info))) {
    LOG_INFO("get tablet", K(tablet_handle), KPC(tablet_handle.get_obj()), K(extra_info));
  }
}

TEST_F(ObTestSSLogMetaService, test_raw_meta_row)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  SCN curr_scn;
  // 1. get read snapshot
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(curr_scn));
  LOG_INFO("current meta scn", K(curr_scn));

  // 2. get the iter
  ObSSMetaReadParam read_param;
  read_param.set_ls_level_param(ObSSMetaReadParamType::LS_PREFIX,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                ObSSLogMetaType::SSLOG_TABLET_META,
                                ls_id_);
  EXPECT_EQ(true, read_param.is_valid());
  ObRawMetaRowIterator *iter = nullptr;
  ObSSMetaIterGuard<ObRawMetaRowIterator> iter_guard;
  ObRawMetaRow row;
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_raw_tablet_meta_row_iter(read_param,
                                                               curr_scn,
                                                               iter_guard));
  LOG_INFO("read raw tablet meta row iter with snapshot", K(curr_scn));
  iter_guard.get_iter(iter);
  while (OB_SUCC(iter->get_next(row))) {
    LOG_INFO("get raw row", K(row));
  }

  // 3. test range read with meta row.
  ObSSTabletSSLogValue meta_v;
  ObSSLogMetaKey sslog_meta_key;
  ObSSMetaUpdateMetaInfo meta_info;
  int64_t pos = 0;
  if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(sslog_meta_key.deserialize(row.meta_key_.ptr(),
                                                row.meta_key_.length(),
                                                pos))) {
    LOG_WARN("deserialize meta key fail", K(ret), K(row));
  } else if (FALSE_IT(pos = 0)) {
  } else if (OB_FAIL(meta_v.deserialize_meta_info(row.meta_value_.ptr(),
                                                  row.meta_value_.length(),
                                                  pos))) {
    LOG_WARN("deserialize meta info failed", K(ret));
  } else if (FALSE_IT(meta_info = meta_v.get_meta_info())) {
  }

  // tablet raw row range read.
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    ObSSLogMetaType::SSLOG_TABLET_META,
                                    sslog_meta_key.tablet_meta_key_.ls_id_,
                                    sslog_meta_key.tablet_meta_key_.tablet_id_,
                                    sslog_meta_key.tablet_meta_key_.reorganization_scn_);
  EXPECT_EQ(true, read_param.is_valid());
  ObMetaVersionRange range;
  range.version_start_ = share::SCN::min_scn();
  range.version_end_ = curr_scn;
  LOG_INFO("read tablet raw row range param", K(read_param), K(range));
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_raw_tablet_meta_row_iter(read_param,
                                                               range,
                                                               iter_guard));
  LOG_INFO("read raw tablet meta row iter with version range", K(range));
  iter_guard.get_iter(iter);
  while (OB_SUCC(iter->get_next(row))) {
    LOG_INFO("get raw row of range", K(row));
  }

  // 4. get the ls meta row
  read_param.set_ls_level_param(ObSSMetaReadParamType::LS_KEY,
                                ObSSMetaReadResultType::READ_WHOLE_ROW,
                                ObSSLogMetaType::SSLOG_LS_META,
                                ls_id_);
  EXPECT_EQ(true, read_param.is_valid());
  EXPECT_EQ(OB_SUCCESS, meta_svr->get_raw_meta_row(read_param,
                                                   curr_scn,
                                                   allocator,
                                                   row));
  pos = 0;
  sslog_meta_key.deserialize(row.meta_key_.ptr(),
                             row.meta_key_.length(),
                             pos);
  LOG_INFO("read raw ls meta row iter with snapshot", K(curr_scn), K(row));
  LOG_INFO("read raw ls meta row iter with snapshot", K(curr_scn), K(sslog_meta_key));
}

TEST_F(ObTestSSLogMetaService, test_ls_meta_rpc)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));


  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSID ls_id = ls_id_;
  bool is_sswriter = false;
  ObSSLSMeta *cache_ls_meta = nullptr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->check_lease_(ls_id, is_sswriter));
  ASSERT_EQ(is_sswriter, true);

  ObAddr addr;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_leader_(ls_id, addr));
  LOG_INFO("the leader is", K(addr), K(GCTX.self_addr()));

  // 1. update with rpc
  SCN offline_scn = SCN::base_scn();
  offline_scn = SCN::plus(offline_scn, 1);
  const int64_t curr_time = ObTimeUtility::current_time();
  const int64_t timeout_us = 10_s;
  ObUpdateSharedLSMetaArg arg(ls_id, LSGCState::LS_OFFLINE, offline_scn,
                              curr_time + timeout_us);

  ObSharedRpcTransmitResult res;
  ASSERT_EQ(OB_SUCCESS, meta_svr->forward_request_(addr,
                                                   ObSharedRpcTransmitArg::RPCType::RPC_TYPE_SHARED_META,
                                                   arg,
                                                   res));
  LOG_INFO("the rpc result", K(arg), K(res));
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_ls_meta_cache_(ls_id, cache_ls_meta));
  // ls meta cache disabled
  ASSERT_EQ(NULL, cache_ls_meta);
  // ASSERT_EQ(cache_ls_meta->offline_scn_, offline_scn);
  // ASSERT_EQ(cache_ls_meta->gc_state_, LSGCState::LS_OFFLINE);

  offline_scn = SCN::plus(offline_scn, 1);
  global_is_sswriter = false;

  // 2. update failed with rpc
  // should return NOT MASTER
  arg.gc_arg_.ls_id_ = ls_id;
  arg.gc_arg_.gc_state_ = LSGCState::LS_OFFLINE;
  arg.gc_arg_.offline_scn_ = offline_scn;
  ASSERT_EQ(OB_NOT_MASTER, meta_svr->forward_request_(addr,
                                                      ObSharedRpcTransmitArg::RPCType::RPC_TYPE_SHARED_META,
                                                      arg,
                                                      res));
  LOG_INFO("the rpc result", K(arg), K(res));
  // ASSERT_NE(cache_ls_meta->offline_scn_, offline_scn);

  // 3. check timeout
  arg.abs_timeout_ts_ = ObTimeUtility::current_time();
  usleep(1000 * 1000);
  ASSERT_EQ(OB_TIMEOUT, meta_svr->forward_request_(addr,
                                                   ObSharedRpcTransmitArg::RPCType::RPC_TYPE_SHARED_META,
                                                   arg,
                                                   res));
  LOG_INFO("the rpc result", K(arg), K(res));
  // ASSERT_NE(cache_ls_meta->offline_scn_, offline_scn);
  global_is_sswriter = true;
}

TEST_F(ObTestSSLogMetaService, test_tablet_handle_assign)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  const SCN transfer_scn = SCN::min_scn();
  ObLSID ls_id = ls_id_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                    OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = TestTabletHelper::create_tablet(ls_handle, cur_tablet_id, schema, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id_,
                                                cur_tablet_id,
                                                transfer_scn));

  LOG_INFO("test_meta_service debug : create tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  ObTabletHandle tmp_tablet_handle;

  ObTabletHandle t3m_tablet_handle;
  ret = ls->get_tablet(cur_tablet_id, t3m_tablet_handle);
  LOG_INFO("get tablet_handle FROM_T3M", K(t3m_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletHandle::ObTabletHdlType::FROM_T3M, t3m_tablet_handle.type_);
  ASSERT_EQ(true, t3m_tablet_handle.is_valid());
  ASSERT_EQ(OB_SUCCESS, tmp_tablet_handle.assign(t3m_tablet_handle)); // FROM_T3M

  ObTabletHandle ss_tablet_handle;
  ret = meta_svr->get_tablet(ls_id,
                             cur_tablet_id,
                             transfer_scn,
                             allocator,
                             ss_tablet_handle);
  LOG_INFO("get tablet_handle SSLOCAL", K(ss_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletHandle::ObTabletHdlType::STANDALONE, ss_tablet_handle.type_);
  ASSERT_EQ(true, ss_tablet_handle.is_valid());
  ASSERT_EQ(OB_ERR_UNEXPECTED, tmp_tablet_handle.assign(ss_tablet_handle)); // STANDALONE

  ObTabletHandle stack_temp_tablet_handle;
  ret = ObTabletCreateDeleteHelper::acquire_tmp_tablet(ObTabletMapKey(ls_id, cur_tablet_id),
                                                                       allocator,
                                                                       stack_temp_tablet_handle);
  LOG_INFO("get tablet_handle STACK_TEMP", K(stack_temp_tablet_handle));
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(ObTabletHandle::ObTabletHdlType::STANDALONE, stack_temp_tablet_handle.type_);
  ASSERT_EQ(true, stack_temp_tablet_handle.is_valid());
  ASSERT_EQ(OB_ERR_UNEXPECTED, tmp_tablet_handle.assign(stack_temp_tablet_handle)); // STANDALONE

  t3m_tablet_handle.obj_pool_ = nullptr;
  t3m_tablet_handle.type_ = ObTabletHandle::ObTabletHdlType::COPY_FROM_T3M;
  t3m_tablet_handle.allocator_ = &allocator;
  LOG_INFO("get tablet_handle ALLOC", K(t3m_tablet_handle));
  ASSERT_EQ(OB_ERR_UNEXPECTED, tmp_tablet_handle.assign(t3m_tablet_handle)); // COPY_FROM_T3M
}

TEST_F(ObTestSSLogMetaService, test_BasePointerHandle)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  const SCN transfer_scn = SCN::min_scn();
  ObLSID ls_id = ls_id_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ObLSService *ls_svr = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = NULL;

  common::ObArenaAllocator allocator("TestTabletMeta",
                                    OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  EXPECT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, ls_handle, ObLSGetMod::STORAGE_MOD));
  ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ret = TestTabletHelper::create_tablet(ls_handle, cur_tablet_id, schema, allocator);
  ASSERT_EQ(OB_SUCCESS, ret);
  ASSERT_EQ(OB_SUCCESS, meta_svr->create_tablet(ls_id_,
                                                cur_tablet_id,
                                                transfer_scn));
  LOG_INFO("test_meta_service debug : create tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  ObTabletHandle ss_tablet_handle;
  ret = meta_svr->get_tablet(ls_id,
                             cur_tablet_id,
                             transfer_scn,
                             allocator,
                             ss_tablet_handle);
  LOG_INFO("get tablet_handle SSLOCAL", K(ss_tablet_handle), K(ss_tablet_handle.get_obj()->get_pointer_handle()));
  ASSERT_EQ(true, ss_tablet_handle.get_obj()->get_pointer_handle().is_valid());
  ASSERT_EQ(1, ss_tablet_handle.get_obj()->get_pointer_handle().base_pointer_->get_ref_cnt());
}

} // unittest
} // oceanbase

int main(int argc, char **argv)
{
  int64_t c = 0;
  int64_t time_sec = 0;
  char *log_level = (char*)"INFO";
  char buf[1000];
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  memset(buf, 1000, sizeof(buf));
  databuff_printf(buf, sizeof(buf), "%s/%lu_meta_service?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=2000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT, oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  while(EOF != (c = getopt(argc,argv,"t:l:"))) {
    switch(c) {
    case 't':
      time_sec = atoi(optarg);
      break;
    case 'l':
     log_level = optarg;
     oceanbase::unittest::ObSimpleClusterTestBase::enable_env_warn_log_ = false;
     break;
    default:
      break;
    }
  }
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level(log_level);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  LOG_INFO("main>>>", K(cur_time_ns));
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
