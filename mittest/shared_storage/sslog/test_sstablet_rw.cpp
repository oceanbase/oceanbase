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
#include "lib/ob_lib_config.h"
#include "storage/tablet/ob_tablet_macro_info_iterator.h"
#include "storage/tablet/ob_sstablet_persister.h"

#undef private
#undef protected

namespace oceanbase
{
OB_MOCK_PALF_KV_FOR_REPLACE_SYS_TENANT
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
} // end namespace oceanbase

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
} // end namespace common

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
} // end namespace storage

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

class TestSSTabletRW : public ObSimpleClusterTestBase
{
public:
  // 指定case运行目录前缀 test_ob_simple_cluster_
  TestSSTabletRW() : ObSimpleClusterTestBase("test_sstablet_rw_", "50G", "50G", "50G")
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
    ASSERT_EQ(OB_SUCCESS, ls_svr->get_ls(ls_id, handle, ObLSGetMod::STORAGE_MOD));
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

const ObTabletID TestSSTabletRW::get_next_tablet_id()
{
  return ObTabletID(init_tablet_id_++);
}

int TestSSTabletRW::build_update_table_store_param_(ObArenaAllocator &allocator,
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
  static int64_t base_version = 0;
  if (OB_FAIL(sstable_gen.mock_sstable(
      allocator, schema,
      table_type,
      tablet_handle.get_obj()->get_tablet_id(), base_version, base_version + 200, table_handle))) {
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
    param.ddl_info_.update_with_major_flag_ = false;

    param.sstable_ = sstable;
    param.allow_duplicate_sstable_ = true;

    if (FAILEDx(param.init_with_ha_info(ObHATableStoreParam(
            tablet_handle.get_obj()->get_tablet_meta().transfer_info_.transfer_seq_,
            true /*need_check_transfer_seq*/)))) {
      LOG_WARN("failed to init with ha info", KR(ret));
    } else if (OB_FAIL(param.init_with_compaction_info(ObCompactionTableStoreParam(
                      merge_type,
                      clog_checkpoint_scn,
                      false /*need_report*/,
                      tablet_handle.get_obj()->has_merged_with_mds_info())))) {
      LOG_WARN("failed to init with compaction info", KR(ret));
    } else {
      LOG_INFO("success to init ObUpdateTableStoreParam", KR(ret), K(param), KPC(param.sstable_));
    }
  }
  base_version += 200;
  return ret;
}

static int collect_all_sstable_cache_keys(
    const ObTabletTableStore &table_store,
    /* out */vector<ObStorageMetaKey> &keys)
{
  int ret = OB_SUCCESS;

  auto process_sstable_array =
    [](const ObSSTableArray &sst_array,
       vector<ObStorageMetaKey> &key)->int
    {
      int ret = OB_SUCCESS;
      uint64_t tenant_id = MTL_ID();

      for (int i = 0; OB_SUCC(ret) && i < sst_array.count(); ++i) {
        ObSSTable *sstable = nullptr;
        if (OB_ISNULL(sstable = sst_array.at(i))) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null sstable", K(ret), KP(sstable));
        } else if (!sstable->get_addr().is_disked()) {
          LOG_INFO("sstable is not disked", K(ret));
        } else {
          key.push_back(ObStorageMetaKey(tenant_id, sstable->get_addr()));
        }
      }
      return ret;
    };

 if (OB_FAIL(process_sstable_array(table_store.get_major_sstables(), keys))) {
    LOG_WARN("failed to process major sstables", K(ret));
  } else if (OB_FAIL(process_sstable_array(table_store.get_inc_major_sstables(), keys))) {
    LOG_WARN("failed to process inc major sstables", K(ret));
  } else if (OB_FAIL(process_sstable_array(table_store.get_minor_sstables(), keys))) {
    LOG_WARN("failed to process minor sstables", K(ret));
  } else if (OB_FAIL(process_sstable_array(table_store.get_ddl_sstables(), keys))) {
    LOG_WARN("failed to process ddl sstables", K(ret));
  } else if (OB_FAIL(process_sstable_array(table_store.get_inc_major_ddl_sstables(), keys))) {
    LOG_WARN("failed to process inc major ddl sstables", K(ret));
  } else if (OB_FAIL(process_sstable_array(table_store.get_mds_sstables(), keys))) {
    LOG_WARN("failed to process mds sstables", K(ret));
  } else if (OB_FAIL(process_sstable_array(table_store.get_meta_major_sstables(), keys))) {
    LOG_WARN("failed to process meta major sstables", K(ret));
  }
  return ret;
}

static int clear_meta_cache(storage::ObStorageMetaCache &meta_cache)
{
  int ret = OB_SUCCESS;
  vector<ObStorageMetaKey> all_keys;
  ObKVCacheIterator iterator;

  if (OB_FAIL(meta_cache.get_iterator(iterator))) {
    LOG_WARN("failed to get meta cache iterator", K(ret));
  } else {
    while (OB_SUCC(ret)) {
      const ObStorageMetaKey *tmp_key = nullptr;
      const ObStorageMetaValue *tmp_val = nullptr;
      ObKVCacheHandle tmp_handle;

      if (OB_FAIL(iterator.get_next_kvpair(tmp_key, tmp_val, tmp_handle))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("failed to get next", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else if (OB_ISNULL(tmp_key) || OB_ISNULL(tmp_val)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null kv", K(ret), KP(tmp_key), KP(tmp_val));
      } else {
        all_keys.push_back(*tmp_key);
      }
    }

    if (OB_FAIL(ret)) {
    } else {
      LOG_INFO("collect N cache keys", K(ret), "N", all_keys.size());
      for (const ObStorageMetaKey &key : all_keys) {
        (void)meta_cache.erase(key);
      }
      LOG_INFO("cache size after clear", K(ret), "N", meta_cache.count(MTL_ID()));
    }
  }
  iterator.reset();
  return ret;
}

static int64_t calc_expected_prewarm_sstable_cnt(const int64_t n)
{
  return MIN(n, ObStorageMetaCache::MAX_PREWARM_SSTABLE_META_CNT);
}

TEST_F(TestSSTabletRW, test_create_ls)
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

TEST_F(TestSSTabletRW, test_sstablet_persister_write_buffer)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_user_tenant(MTL_ID()));

  common::ObArenaAllocator allocator("TestTabletMeta",
                                     OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);

  const ObLSID ls_id(1001);
  const ObTabletID tablet_id(200001);

  global_is_sswriter = true;

  ObAtomicFileMgr *atomic_file_mgr = MTL(decltype(atomic_file_mgr));
  ASSERT_NE(nullptr, atomic_file_mgr);
  GET_TABLET_META_HANLDE_DEFAULT(file_handle, 1, ls_id, tablet_id);
  ObAtomicTabletMetaFile *file = file_handle1.get_atomic_file();
  ASSERT_NE(nullptr, file);
  ObAtomicTabletMetaFile *tablet_meta_file = file;
  uint64_t tablet_cur_op_id = 0;
  CREATE_TABLET_META_WRITE_OP_WITH_RECONFIRM(op_handle);
  ASSERT_NE(nullptr, file);
  ObAtomicTabletMetaOp *op = op_handle.get_atomic_op();
  ASSERT_NE(nullptr, op);

  uint64_t data_version = 0;
  ASSERT_EQ(OB_SUCCESS, GET_MIN_DATA_VERSION(MTL_ID(), data_version));
  LOG_INFO("current data version", K(ret), KDV(data_version));
  ObTabletPersisterParam persist_param(data_version,
                                       ls_id,
                                       tablet_id,
                                       ObMetaUpdateReason::CREATE_TABLET,
                                       -1,
                                       0,
                                       &op_handle,
                                       file,
                                       nullptr,
                                       nullptr,
                                       SCN::min_scn().get_val_for_tx(),
                                       nullptr);
  ASSERT_TRUE(persist_param.is_valid());
  ASSERT_TRUE(persist_param.is_inc_shared_object());




  const int64_t buf_size_limit = ObSSTabletPersister::WRITE_BUFFER_SIZE_LIMIT;

  struct Util final
  {
    static void make_write_info(
      const bool write_to_oss,
      ObArenaAllocator &allocator,
      ObSharedObjectWriteInfo &write_info,
      std::string &str)
    {
      static const char *alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGUIJKLMNOPQRSTUVWXYZ";
      static const size_t total_cnt = strlen(alphabet);
      str.clear();
      write_info.reset();
      const size_t str_len = write_to_oss ? (size_t)upper_align(ObSSTabletPersister::OBJECT_LIMIT + 1, DIO_READ_ALIGN_SIZE) : 4096ul;
      char *buf = (char *)allocator.alloc(str_len);
      ASSERT_NE(nullptr, buf);
      for (size_t i = 0; i < str_len; ++i) {
        const int64_t idx = ObRandom::rand(0, total_cnt - 1);
        buf[i] = alphabet[i];
      }
      write_info.buffer_ = buf;
      write_info.offset_ = 0;
      write_info.size_ = str_len;
      write_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_COMPACT_WRITE);
      write_info.ls_epoch_ = 0;
      ASSERT_TRUE(write_info.is_valid());
      str.append(buf, str_len);
      ASSERT_EQ(str_len, str.size());
      ASSERT_EQ(0, memcmp(buf, str.data(), str_len));
    }


    static void check_write(
      const ObMetaDiskAddr &addr,
      const std::string &expected_str)
    {
      ObArenaAllocator allocator(lib::ObMemAttr(MTL_ID(), "CheckWrite"));
      ObSharedObjectReadInfo read_info;
      read_info.addr_ = addr;
      read_info.io_desc_.set_mode(ObIOMode::READ);
      read_info.io_desc_.set_wait_event(ObWaitEventIds::DB_FILE_DATA_READ);
      read_info.ls_epoch_ = 0;
      read_info.io_timeout_ms_ = GCONF._data_storage_io_timeout / 1000;
      ObSharedObjectReadHandle io_handle(allocator);
      ASSERT_EQ(OB_SUCCESS, ObSharedObjectReaderWriter::async_read(read_info, io_handle));
      ASSERT_EQ(OB_SUCCESS, io_handle.wait());
      char *buf = nullptr;
      int64_t buf_len = 0;
      ASSERT_EQ(OB_SUCCESS, io_handle.get_data(allocator, buf, buf_len));
      ASSERT_NE(nullptr, buf);
      ASSERT_EQ((size_t)buf_len, expected_str.size());
      ASSERT_EQ(0, memcmp(buf, expected_str.data(), buf_len));
    }
  };

  struct AddrComp final
  {
    bool operator()(const ObMetaDiskAddr &a, const ObMetaDiskAddr &b) const
    {
      return MEMCMP(&a, &b, sizeof(ObMetaDiskAddr)) < 0;
    }
  };
  int64_t macro_seq = 0;
  std::map<ObMetaDiskAddr, std::string, AddrComp> written_data;
  ObSharedObjectWriteInfo write_info;
  ObMetaDiskAddr addr;

  ObSSTabletPersister::InnerWriteBuffer write_buffer(persist_param, lib::ObMemAttr(MTL_ID(), "WriteBuf"), buf_size_limit, allocator);

  ASSERT_EQ(OB_SUCCESS, write_buffer.init(ObSSTabletPersister::WriteStrategy::BATCH));

  common::ObArray<ObSharedObjectWriteInfo> write_infos;
  vector<bool> is_write_to_oss;
  vector<std::string> strs;

  const int64_t total_write_op = 16;
  int64_t oss_write_ops = 0, table_write_ops = 0;
  for (int64_t i = 0; i < total_write_op; ++i) {
    const bool write_to_oss = ObRandom::rand(0, 5) < 3;
    std::string tmp_str;
    Util::make_write_info(write_to_oss, allocator, write_info, tmp_str);
    ASSERT_EQ(OB_SUCCESS, write_infos.push_back(write_info));
    is_write_to_oss.push_back(write_to_oss);
    strs.emplace_back(std::move(tmp_str));
    int64_t tmp = write_to_oss ? ++oss_write_ops : ++table_write_ops;
  }

  common::ObArray<ObMetaDiskAddr> addrs;
  ASSERT_EQ(OB_SUCCESS, write_buffer.append(write_infos, macro_seq, addrs));
  ASSERT_EQ(macro_seq, total_write_op);
  ASSERT_EQ(addrs.count(), total_write_op);
  for (int64_t i = 0; i < total_write_op; ++i) {
    const ObMetaDiskAddr &addr = addrs.at(i);
    MacroBlockId block_id;
    ASSERT_EQ(OB_SUCCESS, addr.get_macro_block_id(block_id));
    ASSERT_EQ(is_write_to_oss[i], !block_id.is_shared_tablet_sub_meta_in_table());
    ASSERT_TRUE(written_data.insert({addr, std::move(strs[i])}).second);
  }

  ASSERT_EQ(OB_SUCCESS, write_buffer.sync_batch());

  for (const auto &iter : written_data) {
    const ObMetaDiskAddr &addr = iter.first;
    const std::string &expected_str = iter.second;
    Util::check_write(addr, expected_str);
  }

  const ObSSTabletPersister::InnerWriteSummary &summary = write_buffer.write_summary();
  ASSERT_EQ(oss_write_ops, summary.write_ops_for_oss_);
  ASSERT_EQ(table_write_ops, summary.write_ops_for_table_);

  fprintf(stdout, "test finished(summary:%s)\n", ObCStringHelper().convert(summary));
}

TEST_F(TestSSTabletRW, test_load_macro_info_from_sslog)
{
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_user_tenant(MTL_ID()));

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  const SCN transfer_scn = SCN::min_scn();
  ObLSID ls_id = ls_id_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
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

  LOG_INFO("create tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  global_is_sswriter = true;

  static const int64_t SSTABLE_CNT = 32;
  // gen sstable
  {
    ObTabletHandle orig_tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet(cur_tablet_id, orig_tablet_handle));
    for (int64_t i = 0; i < SSTABLE_CNT; ++i) {
      ObUpdateTableStoreParam update_param;
      ObTableHandleV2 table_handle;
      ASSERT_EQ(OB_SUCCESS, build_update_table_store_param_(
                              allocator,
                              schema,
                              ls_handle,
                              orig_tablet_handle,
                              compaction::ObMergeType::MAJOR_MERGE,
                              table_handle,
                              update_param));
      ASSERT_EQ(OB_SUCCESS, meta_svr->update_tablet_table_store(
                              ls_id_,
                              cur_tablet_id,
                              orig_tablet_handle.get_obj()->get_reorganization_scn(),
                              ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
                              100 + i,
                              update_param));
    }
  }


  ObSSMetaReadParam read_param;
  SCN read_scn;
  read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                    ObSSMetaReadResultType::READ_WHOLE_ROW,
                                    true, /*try read local*/
                                    ObSSLogMetaType::SSLOG_TABLET_META,
                                    ls_id,
                                    cur_tablet_id,
                                    transfer_scn);
  ASSERT_TRUE(read_param.is_valid());
  ObTabletHandle tablet_handle;
  ObTabletMacroInfo *expected_macro_info = nullptr;
  ObMetaDiskAddr fake_addr;
  {
    ObTablet *tablet = nullptr;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(share::SYS_LS, read_scn));
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(read_param, read_scn, allocator, tablet_handle));
    ASSERT_NE(nullptr, tablet = tablet_handle.get_obj());
    fake_addr = tablet->get_tablet_addr();
    ASSERT_EQ(OB_SUCCESS, tablet->load_macro_info(0, allocator, expected_macro_info));
    ASSERT_NE(nullptr, expected_macro_info);
    ASSERT_TRUE(expected_macro_info->is_valid());
  }

  // test read from atomic file
  ObRawMetaRow meta_row;
  ObSSTabletSSLogValue value;
  ObUpdateTabletLog update_log;
  char *tablet_raw_buf = nullptr;
  int64_t tablet_raw_buf_len = 0;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, meta_svr->get_raw_meta_row(read_param, read_scn, allocator, meta_row));
  ASSERT_NE(nullptr, meta_row.meta_value_.ptr());
  ASSERT_EQ(OB_SUCCESS, ObSSTabletSSLogValue::get_tablet_raw_buffer(
                          meta_row.meta_value_.ptr(),
                          meta_row.meta_value_.length(),
                          allocator,
                          tablet_raw_buf,
                          tablet_raw_buf_len));
  ASSERT_EQ(OB_SUCCESS, value.deserialize(
                          allocator,
                          fake_addr,
                          meta_row.meta_value_.ptr(),
                          meta_row.meta_value_.length(),
                          pos));
  ASSERT_EQ(OB_SUCCESS, value.update_log_.convert_to_update_log(meta_row.row_scn_, update_log));
  pos = 0;
  void *ptr = nullptr;
  ObTablet *tablet_ptr = nullptr;
  ASSERT_NE(nullptr, ptr = allocator.alloc(sizeof(ObTablet)));
  tablet_ptr = new(ptr) ObTablet();
  tablet_ptr->set_tablet_addr(update_log.disk_addr_);
  ASSERT_EQ(OB_SUCCESS, tablet_ptr->deserialize_for_replay(allocator, tablet_raw_buf, tablet_raw_buf_len, pos));
  tablet_ptr->set_allocator(&allocator);
  ObTabletMacroInfo *macro_info = nullptr;
  // enforce to load macro info from disk
  tablet_ptr->macro_info_addr_.ptr_ = nullptr;
  ASSERT_EQ(OB_SUCCESS, tablet_ptr->load_macro_info(0, allocator, macro_info));
  ASSERT_NE(nullptr, macro_info);
  ASSERT_TRUE(macro_info->is_valid());

  {
    int64_t expected_iter_cnt = 0, result_iter_cnt = 0;
    ObMacroInfoIterator expected_iter, result_iter;
    ASSERT_EQ(OB_SUCCESS, expected_iter.init(ObTabletMacroType::MAX, *expected_macro_info));
    ASSERT_EQ(OB_SUCCESS, result_iter.init(ObTabletMacroType::MAX, *macro_info));
    while (OB_SUCC(ret)) {
      ObTabletBlockInfo ex_block_info, res_block_info;
      if (OB_FAIL(expected_iter.get_next(ex_block_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("expected_iter failed to get next", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ++expected_iter_cnt;
      }

      if (FAILEDx(result_iter.get_next(res_block_info))) {
        if (OB_ITER_END != ret) {
          LOG_WARN("result_iter failed to get next", K(ret));
        } else {
          ret = OB_SUCCESS;
          break;
        }
      } else {
        ++result_iter_cnt;
      }

      if (OB_FAIL(ret)) {
      } else if (ex_block_info.block_type_ != res_block_info.block_type_
                 || ex_block_info.macro_id_ != res_block_info.macro_id_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("res_block_info is incorrect", K(ret), K(ex_block_info), K(res_block_info));
      }
    }
    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(expected_iter_cnt, result_iter_cnt);
  }
}

TEST_F(TestSSTabletRW, test_range_read_basic)
{
share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id));
  int ret = OB_SUCCESS;
  ASSERT_TRUE(is_user_tenant(MTL_ID()));

  share::schema::ObTableSchema schema;
  TestSchemaUtils::prepare_data_schema(schema);
  ObTabletID cur_tablet_id = get_next_tablet_id();
  const SCN transfer_scn = SCN::min_scn();
  ObLSID ls_id = ls_id_;
  ObSSMetaService *meta_svr = MTL(ObSSMetaService *);
  ASSERT_NE(nullptr, meta_svr);
  ObLSService *ls_svr = MTL(ObLSService *);
  ASSERT_NE(nullptr, ls_svr);
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

  LOG_INFO("create tablet success", K(ret), K(ls_id_), K(cur_tablet_id));

  global_is_sswriter = true;

  static const int64_t SSTABLE_CNT = 32;
  // gen sstable
  {

    ObTabletHandle orig_tablet_handle;
    ASSERT_EQ(OB_SUCCESS, ls->get_tablet(cur_tablet_id, orig_tablet_handle));
    for (int64_t i = 0; i < SSTABLE_CNT; ++i) {
      ObUpdateTableStoreParam update_param;
      ObTableHandleV2 table_handle;
      ASSERT_EQ(OB_SUCCESS, build_update_table_store_param_(
                              allocator,
                              schema,
                              ls_handle,
                              orig_tablet_handle,
                              compaction::ObMergeType::MAJOR_MERGE,
                              table_handle,
                              update_param));
      ASSERT_EQ(OB_SUCCESS, meta_svr->update_tablet_table_store(
                              ls_id_,
                              cur_tablet_id,
                              orig_tablet_handle.get_obj()->get_reorganization_scn(),
                              ObMetaUpdateReason::TABLET_COMPACT_ADD_DATA_MAJOR_SSTABLE,
                              100 + i,
                              update_param));
    }
  }

  // test fetch tablet table store
  {
    ObSSMetaReadParam read_param;
    SCN read_scn;
    read_param.set_tablet_level_param(ObSSMetaReadParamType::TABLET_KEY,
                                      ObSSMetaReadResultType::READ_WHOLE_ROW,
                                      true, /*try read local*/
                                      ObSSLogMetaType::SSLOG_TABLET_META,
                                      ls_id,
                                      cur_tablet_id,
                                      transfer_scn);
    ASSERT_TRUE(read_param.is_valid());
    ObTabletHandle tablet_handle;
    ObTablet *tablet = nullptr;
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_max_committed_meta_scn(share::SYS_LS, read_scn));
    ASSERT_EQ(OB_SUCCESS, meta_svr->get_tablet(read_param, read_scn, allocator, tablet_handle));
    ASSERT_NE(nullptr, tablet = tablet_handle.get_obj());

    ASSERT_TRUE(tablet->table_store_addr_.is_disk_object());
    ASSERT_TRUE(tablet->storage_schema_addr_.is_disk_object());
    MacroBlockId table_store_block_id, storage_schema_block_id;
    ASSERT_EQ(OB_SUCCESS, tablet->table_store_addr_.addr_.get_macro_block_id(table_store_block_id));
    ASSERT_EQ(OB_SUCCESS, tablet->storage_schema_addr_.addr_.get_macro_block_id(storage_schema_block_id));
    LOG_INFO("tablet meta member addr", K(ret), "table_store_addr", tablet->table_store_addr_,
      "storage_schema_addr", tablet->storage_schema_addr_);

    ASSERT_TRUE(table_store_block_id.is_shared_tablet_sub_meta_in_table());
    ASSERT_LT(table_store_block_id.get_macro_seq(), storage_schema_block_id.get_macro_seq());
    int64_t expected_prewarm_sst_cnt = calc_expected_prewarm_sstable_cnt(storage_schema_block_id.get_macro_seq() - table_store_block_id.get_macro_seq() - 1);
    LOG_INFO("expected prewarm sstable cnt", K(ret), K(expected_prewarm_sst_cnt));

    ObTabletMemberWrapper<ObTabletTableStore> table_store_wrapper;
    const ObTabletTableStore *table_store_ptr = nullptr;
    storage::ObStorageMetaCache &meta_cache = OB_STORE_CACHE.get_storage_meta_cache();
    // clear meta cache
    ASSERT_EQ(OB_SUCCESS, clear_meta_cache(meta_cache));

    ObStorageMetaHandle table_store_handle;
    {
      const ObStorageMetaKey key(MTL_ID(), tablet->table_store_addr_.addr_);
      const int end_seq = storage_schema_block_id.get_macro_seq();
      ASSERT_EQ(OB_SUCCESS, meta_cache.get_table_store_and_prewarm_sstable_meta(key, end_seq - 1, *tablet, table_store_handle));
      ASSERT_TRUE(table_store_handle.is_valid());
      const ObStorageMetaValue *value = nullptr;
      ASSERT_EQ(OB_SUCCESS, table_store_handle.get_value(value));
      ASSERT_NE(nullptr, value);
      ASSERT_EQ(OB_SUCCESS, value->get_table_store(table_store_ptr));
      ASSERT_NE(nullptr, table_store_ptr);
    }


    // sstable meta should be prewarmed
    int64_t prewarm_sst_cnt = 0;
    vector<ObStorageMetaKey> cache_keys;
    ASSERT_EQ(OB_SUCCESS, collect_all_sstable_cache_keys(*table_store_ptr, cache_keys));

    for (const ObStorageMetaKey &key : cache_keys) {
      const ObStorageMetaValue *value = nullptr;
      const ObSSTable *sstable = nullptr;
      ObKVCacheHandle tmp_handle;
      if (OB_FAIL(meta_cache.get(key, value, tmp_handle))) {
        if (OB_ENTRY_NOT_EXIST != ret) {
          LOG_WARN("failed to get meta cache", K(ret), K(key));
          break;
        } else {
          ret = OB_SUCCESS;
        }
      } else if (OB_ISNULL(value)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null cache value", K(ret), K(key), KP(value), K(tmp_handle));
      } else if (OB_FAIL(value->get_sstable(sstable))) {
        LOG_WARN("failed to get sstable from cache value", K(ret), K(key), KPC(value));
      } else if (OB_ISNULL(sstable)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected null sstable", K(ret), KP(sstable), K(key), KPC(value));
      } else if (OB_UNLIKELY(!sstable->is_valid())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("unexpected invalid sstable", K(ret), KP(sstable), K(key), KPC(value));
      } else {
        ++prewarm_sst_cnt;
      }
    }

    ASSERT_EQ(OB_SUCCESS, ret);
    ASSERT_EQ(expected_prewarm_sst_cnt, prewarm_sst_cnt);
  }
}

} // end namespace unittest
} // end namespace oceanbase

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
  // Enable trace log for ObTimeGuard to work properly
  oceanbase::lib::reload_trace_log_config(true);
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  LOG_INFO("main>>>", K(cur_time_ns));
  oceanbase::unittest::RunCtx.time_sec_ = time_sec;
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
