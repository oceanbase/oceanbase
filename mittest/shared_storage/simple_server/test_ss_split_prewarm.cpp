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
#ifndef USING_LOG_PREFIX
#define USING_LOG_PREFIX STORAGETEST
#endif
#include <gtest/gtest.h>

#define protected public
#define private public
#include "lib/utility/ob_test_util.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_service.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "mittest/simple_server/env/ob_simple_cluster_test_base.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/ddl/ob_tablet_split_task.h"
#include "unittest/storage/sslog/test_mock_palf_kv.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_i_sslog_proxy.h"
#include "close_modules/shared_storage/storage/incremental/sslog/ob_sslog_kv_proxy.h"

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
} // namespace oceanbase


using namespace oceanbase::transaction;
using namespace oceanbase::storage;

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{

struct TestTabletCacheInfo
{
  uint64_t tenant_id_ = 0;
  ObTabletID tablet_id_;
  ObSEArray<MacroBlockId, 10> macros_in_cache_;
  ObSEArray<ObSSMicroBlockId, 10> micros_in_cache_;
  TO_STRING_KV(K(tenant_id_), K(tablet_id_), K(macros_in_cache_), K(micros_in_cache_));


  void reuse()
  {
    macros_in_cache_.reuse();
    micros_in_cache_.reuse();
    tenant_id_ = 0;
    tablet_id_ = OB_INVALID_ID;
  }
};

struct TestSplitInfo
{
  ObTabletID src_tablet_id_;
  ObSEArray<ObTabletID, 2> dest_tablet_ids_;
  TO_STRING_KV(K(src_tablet_id_), K(dest_tablet_ids_));

};

struct TestRunSplitCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObSEArray<TestSplitInfo, 5> split_infos_;
  ObSEArray<TestTabletCacheInfo, 5> cache_infos_;
  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(split_infos_), K(cache_infos_));
};

class ObSplitPrewarmerTest : public ObSimpleClusterTestBase
{
public:
  ObSplitPrewarmerTest()
    : ObSimpleClusterTestBase("test_split_prewarmer_","50G", "50G", "50G"),
      tenant_created_(false),
      tenant_id_(0),
      ctx_()
    {}

virtual void SetUp() override
{
  ObSimpleClusterTestBase::SetUp();
  if (!tenant_created_) {
    OK(create_tenant("tt1", "10G", "10G", false/*oracle_mode*/, 4));
    OK(get_tenant_id(tenant_id_));
    ASSERT_NE(0, tenant_id_);
    OK(get_curr_simple_server().init_sql_proxy2());
    ctx_.tenant_id_ = tenant_id_;
    tenant_created_ = true;
  }
}

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  int exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy2().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  int sys_exe_sql(const char *sql_str)
  {
    int ret = OB_SUCCESS;
    ObSqlString sql;
    int64_t affected_rows = 0;
    if (OB_ISNULL(sql_str)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("sql str is null", KR(ret), KP(sql_str));
    } else if (OB_FAIL(sql.assign(sql_str))) {
      LOG_WARN("fail to assign sql", KR(ret), K(sql_str));
    } else if (OB_FAIL(get_curr_simple_server().get_sql_proxy().write(sql.ptr(), affected_rows))) {
      LOG_WARN("fail to write sql", KR(ret), K(sql_str), K(sql));
    }
    return ret;
  }

  void wait_major_finish();
  void set_ls_and_tablet_id_for_run_ctx();
  void add_task(SCPTabletTaskMap &tablet_tasks, ObStorageCacheTaskStatusType status, int64_t end_time, const int64_t tablet_id);
  void create_table_and_insert_data();
  void check_dest_caches();
  void split_table_p1_partition();
  void add_source_micro_to_cache(ObTabletID &tablet_id, ObArenaAllocator &allocator);
  void check_all_dest_data_macros_exist(const ObTabletID &dest_tablet_id, ObDualMacroMetaIterator &meta_iter, bool &all_exist);
  // for future usage
  void check_all_dest_micros_exist(const ObTabletID &dest_tablet_id, ObMicroBlockIndexIterator &micro_iter, bool &all_exist);

  void find_cache_info(ObTabletID &tablet_id, TestTabletCacheInfo& cache_info);

private:
  bool tenant_created_;
  uint64_t tenant_id_;
  TestRunSplitCtx ctx_;
};

void ObSplitPrewarmerTest::wait_major_finish()
{
  int ret = OB_SUCCESS;
  LOG_INFO("wait major begin");
  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();

  ObSqlString sql;
  int64_t affected_rows = 0;
  static int64_t old_major_scn = 1;
  int64_t new_major_scn = 1;
  int64_t scn = 0;
  do {
    OK(sql.assign_fmt(
        "select frozen_scn, (frozen_scn - last_scn) as result"
        " from oceanbase.CDB_OB_MAJOR_COMPACTION where tenant_id=%lu;",
        ctx_.tenant_id_));
    SMART_VAR(ObMySQLProxy::MySQLResult, res) {
      OK(sql_proxy.read(res, sql.ptr()));
      sqlclient::ObMySQLResult *result = res.get_result();
      ASSERT_NE(nullptr, result);
      OK(result->next());
      OK(result->get_int("result", scn));
      OK(result->get_int("frozen_scn", new_major_scn));
    }
    LOG_INFO("major result", K(scn), K(new_major_scn));
    ob_usleep(100 * 1000); // 100_ms
  } while (0 != scn || old_major_scn == new_major_scn);

  old_major_scn = new_major_scn;
  LOG_INFO("major finished", K(new_major_scn));
}

void ObSplitPrewarmerTest::split_table_p1_partition()
{
  int ret = OB_SUCCESS;
  OK(exe_sql("alter table split_test_table reorganize partition p1 into (partition p1_0 values less than (5000), "
      "partition p1_1 values less than (10000));"));


  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  uint64_t dest_tablet_id = 0;
  //get table_id
  ASSERT_TRUE(ctx_.split_infos_.count() > 0);
  uint64_t tablet_id = ctx_.split_infos_.at(0).src_tablet_id_.id();
  OK(sql.assign_fmt("select dest_tablet_id from oceanbase.__all_virtual_tablet_reorganize_history where src_tablet_id=%ld;", tablet_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1,sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);

    while(OB_SUCC(ret)) {
      dest_tablet_id = 0;
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("iterate to the end", K(ret));
          break;
        } else {
          LOG_WARN("failed to get next", K(ret));
          //abort
          ASSERT_TRUE(false);
        }
      } else {
        OK(result->get_uint("dest_tablet_id", dest_tablet_id));
        ctx_.split_infos_.at(0).dest_tablet_ids_.push_back(ObTabletID(dest_tablet_id));
      }
    }
  }
  ASSERT_EQ(ret, OB_SUCCESS);

}

void ObSplitPrewarmerTest::create_table_and_insert_data()
{
  int ret = OB_SUCCESS;
  LOG_INFO("start to create table and insert data", K(ret));
  OK(sys_exe_sql("alter system set undo_retention = 0;"));
  OK(exe_sql("create sequence seq_small INCREMENT BY 1 start with 1 minvalue 1 maxvalue 100000 nocycle noorder nocache;"));
  OK(exe_sql("create sequence seq_large INCREMENT BY 1 start with 5001 minvalue 1 maxvalue 100000 nocycle noorder nocache;"));
  OK(exe_sql("create table split_test_table(c1 bigint, c2 bigint, c3 longtext, key(c2), primary key(c1)) partition by range(c1) "
  "( partition p0 values less than (1), "
  "partition p1 values less than (10000), "
  "partition p2 values less than MAXVALUE);"));

  OK(exe_sql("insert into split_test_table select seq_small.nextval, random(10), randstr(1000, random(10)) from table(generator(300));"));
  OK(exe_sql("insert into split_test_table select seq_small.nextval, random(10), randstr(1000, random(10)) from table(generator(300));"));
  OK(exe_sql("insert into split_test_table select seq_small.nextval, random(10), randstr(1000, random(10)) from table(generator(300));"));
  OK(exe_sql("insert into split_test_table select seq_small.nextval, random(10), randstr(1000, random(10)) from table(generator(300));"));

  common::ObMySQLProxy &sql_proxy = get_curr_simple_server().get_sql_proxy();
  ObSqlString sql;
  int64_t table_id = 0;
  //get table_id
  OK(sql.assign("select table_id from oceanbase.__all_virtual_table where table_name='split_test_table';"));
  SMART_VAR(ObMySQLProxy::MySQLResult, res1) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res1,sql.ptr()));
    sqlclient::ObMySQLResult *result = res1.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_int("table_id", table_id));
  }

  TestSplitInfo split_info;
  //get tablet_id
  OK(sql.assign_fmt("select tablet_id from oceanbase.__all_virtual_part where table_id=%ld and part_name = 'p1';", table_id));
  SMART_VAR(ObMySQLProxy::MySQLResult, res3) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res3, sql.ptr()));
    sqlclient::ObMySQLResult *result = res3.get_result();
    ASSERT_NE(nullptr, result);
    while(OB_SUCC(ret)) {
      if (OB_FAIL(result->next())) {
        if (OB_ITER_END == ret) {
          ret = OB_SUCCESS;
          LOG_INFO("iterate to the end", K(ret), K(table_id));
          break;
        } else {
          LOG_WARN("failed to get next", K(ret));
          //abort
          ASSERT_TRUE(false);
        }
      } else {
        uint64_t id = 0;
        OK(result->get_uint("tablet_id", id));
        split_info.src_tablet_id_.id_ = id;
        ctx_.split_infos_.push_back(split_info);
      }
    }
  }
  //get ls_id
  ASSERT_TRUE(ctx_.split_infos_.count() > 0);
  sql.reset();
  int64_t id = 0;
  uint64_t tablet_id = ctx_.split_infos_.at(0).src_tablet_id_.id();
  OK(sql.assign_fmt("select ls_id from oceanbase.__all_virtual_tablet_to_ls where tablet_id=%ld and tenant_id = %ld;", tablet_id, ctx_.tenant_id_));
  SMART_VAR(ObMySQLProxy::MySQLResult, res4) {
    ASSERT_EQ(OB_SUCCESS, sql_proxy.read(res4, sql.ptr()));
    sqlclient::ObMySQLResult *result = res4.get_result();
    ASSERT_NE(nullptr, result);
    OK(result->next());
    OK(result->get_int("ls_id", id));
  }
  ObLSID ls_id(id);

  ObLSHandle ls_handle;
  OK(MTL(ObLSService*)->get_ls(ls_id, ls_handle, ObLSGetMod::TXSTORAGE_MOD));
  ObLS *ls = ls_handle.get_ls();
  ASSERT_NE(nullptr, ls);
  ctx_.ls_id_ = ls->get_ls_id();
  LOG_INFO("check ctx_", K(ret), K(ctx_));
}

void ObSplitPrewarmerTest::check_dest_caches()
{
  int ret = OB_SUCCESS;
  common::ObArenaAllocator allocator("splprewarm",
  OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  ObSSMetaReadParam param;
  ObTablet *dest_tablet = nullptr;
  ObTabletHandle tablet_handle;
  share::SCN transfer_scn = share::SCN::min_scn();
  ObTableStoreIterator table_store_iterator;
  ObTabletHandle src_tablet_handle;
  ObITable *table = nullptr;
  ObSEArray<ObSSTable *, ObTabletTableStore::MAX_SSTABLE_CNT> src_sstables;
  ObMicroBlockIndexIterator micro_iter;

  LOG_INFO("split info", K(ret), K(ctx_.split_infos_));
  for (int64_t i = 0; i < ctx_.split_infos_.count(); ++i) {
    TestSplitInfo &split_info = ctx_.split_infos_.at(i);
    for (int64_t j = 0; j < split_info.dest_tablet_ids_.count(); ++j) {
      const ObTabletID &dest_tablet_id = split_info.dest_tablet_ids_.at(j);
      table_store_iterator.reset();
      src_tablet_handle.reset();
      src_sstables.reuse();
      if (OB_FAIL(meta_service->get_tablet(ctx_.ls_id_, dest_tablet_id, transfer_scn, allocator, tablet_handle))) {
        LOG_WARN("get tablet from meta service failed", KR(ret), K(param));
      } else if (OB_FALSE_IT(dest_tablet = tablet_handle.get_obj())) {
      } else if (OB_ISNULL(dest_tablet)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("get tablet failed ", KR(ret), KP(dest_tablet), K(param));
      } else if (OB_FAIL(dest_tablet->get_all_tables(table_store_iterator))) {
        LOG_WARN("failed to get all tables", K(ret));
      }
      ObSSTable *sstable = nullptr;
      ObSSTableMetaHandle sstable_meta_hdl;
      const ObITableReadInfo *read_info = nullptr;
      ObDatumRange whole_range;
      whole_range.set_whole_range();
      SMART_VAR(ObDualMacroMetaIterator, meta_iter) {
         while (OB_SUCC(ret)) {
          table = nullptr;
          sstable = nullptr;
          sstable_meta_hdl.reset();
          meta_iter.reset();
          micro_iter.reset();
          if (OB_FAIL(table_store_iterator.get_next(table))) {
            if (OB_UNLIKELY(OB_ITER_END == ret)) {
              ret = OB_SUCCESS;
              break;
            } else {
              LOG_WARN("get next table failed", K(ret), K(table_store_iterator));
            }
          } else if (!table->is_sstable()) {
            //skip
          } else if (OB_FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
          } else if (OB_FAIL(dest_tablet->get_sstable_read_info(sstable, read_info))) {
            LOG_WARN("fail to get index read info ", KR(ret), K(sstable));
          } else if (OB_FAIL(sstable->get_meta(sstable_meta_hdl))) {
            LOG_WARN("failed to get meat", K(ret), K(sstable));
          } else if (!sstable_meta_hdl.get_sstable_meta().is_split_table() || table->is_mds_sstable()) {
          } else if (OB_FAIL(meta_iter.open(*sstable, whole_range, *read_info, allocator))) {
            LOG_WARN("open dual macro meta iter failed", K(ret), K(sstable));
          } else if (OB_FAIL(micro_iter.open(*sstable, whole_range, dest_tablet->get_rowkey_read_info(), allocator, true))) {
            LOG_WARN("failed to open micro iter", K(ret), K(sstable), K(dest_tablet_id));
          } else {
            bool all_exist = false;
            check_all_dest_data_macros_exist(dest_tablet_id, meta_iter, all_exist);
            ASSERT_TRUE(all_exist);
            all_exist = false;
            check_all_dest_micros_exist(dest_tablet_id, micro_iter, all_exist);
            ASSERT_TRUE(all_exist);
          }
        }
      }
    }
  }
  OK(ret);
}



void ObSplitPrewarmerTest::check_all_dest_data_macros_exist(const ObTabletID &dest_tablet_id, ObDualMacroMetaIterator &meta_iter, bool &all_exist)
{
  int ret = OB_SUCCESS;
  ObDataMacroBlockMeta macro_meta;
  ObMacroBlockDesc data_macro_desc;
  const int64_t MACRO_BATCH_SIZE = 100;
  ObSSLocalCacheService *local_cache_service = MTL(ObSSLocalCacheService *);
  macro_meta.reset();
  data_macro_desc.reuse();
  data_macro_desc.macro_meta_ = &macro_meta;
  int cmp_ret = 0;
  ObSSMacroCacheMgr *macro_cache_mgr = nullptr;
  ASSERT_NE(macro_cache_mgr = MTL(ObSSMacroCacheMgr *), nullptr);
  all_exist = true;
  int64_t hit_macro_number = 0;
  int64_t exist_macro_number = 0;
  int64_t total_macro_number = 0;
  while (OB_SUCC(ret) && all_exist) {
    if (OB_FAIL(meta_iter.get_next_macro_block(data_macro_desc))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get data macro meta failed", K(ret));
      }
    } else {
      bool is_exist = false;
      if (OB_ISNULL(macro_cache_mgr = MTL(ObSSMacroCacheMgr *))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("macro cache mgr is null", KR(ret));
      } else if (OB_FAIL((macro_cache_mgr->exist(data_macro_desc.macro_block_id_, is_exist)))) {
        LOG_WARN("failed to check macro block exist", KR(ret), K(data_macro_desc.macro_block_id_));
      }
      all_exist &= is_exist;
      bool is_hit_cache = false;
      ObSSFdCacheHandle fd_handle;
      if (OB_FAIL(macro_cache_mgr->get(data_macro_desc.macro_block_id_, dest_tablet_id, 0, is_hit_cache, fd_handle))) {
        LOG_WARN("failed to get", K(ret));
      }
      OK(ret);
      all_exist &= is_hit_cache;
      if (is_hit_cache) {
        hit_macro_number++;
        LOG_INFO("hit macro:", K(ret), K(data_macro_desc.macro_block_id_), K(dest_tablet_id));
      }
      if (is_exist) {
        exist_macro_number++;
      }
      total_macro_number++;
    }
  }
  LOG_INFO("cache info", K(hit_macro_number), K(exist_macro_number), K(total_macro_number));
  OK(ret);
}




void ObSplitPrewarmerTest::add_source_micro_to_cache(ObTabletID &tablet_id, ObArenaAllocator &allocator)
{
  int ret = OB_SUCCESS;
  ObSSMetaService *meta_service = MTL(ObSSMetaService *);
  share::SCN transfer_scn = share::SCN::min_scn();
  ObTabletHandle tablet_handle;
  ObTablet *tablet = nullptr;
  ObTableStoreIterator table_store_iterator;
  ObITable *table = nullptr;
  ObSSTable *sstable = nullptr;
  ObMicroBlockIndexIterator micro_iter;
  const ObITableReadInfo *read_info = nullptr;
  ObDatumRange whole_range;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);

  whole_range.set_whole_range();
  LOG_INFO("split into", K(tablet_id));
  if (OB_FAIL(meta_service->get_tablet(ctx_.ls_id_, tablet_id, transfer_scn, allocator, tablet_handle))) {
    LOG_WARN("get tablet from meta service failed", KR(ret));
  } else if (OB_FALSE_IT(tablet = tablet_handle.get_obj())) {
  } else if (OB_ISNULL(tablet)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("get tablet failed ", KR(ret), KP(tablet));
  } else if (OB_FAIL(tablet->get_all_tables(table_store_iterator))) {
    LOG_WARN("failed to get all tables", K(ret));
  }
  ObSSTableMetaHandle sstable_meta_hdl;

  while (OB_SUCC(ret)) {
    table = nullptr;
    sstable = nullptr;
    micro_iter.reset();
    sstable_meta_hdl.reset();
    if (OB_FAIL(table_store_iterator.get_next(table))) {
      if (OB_UNLIKELY(OB_ITER_END == ret)) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("get next table failed", K(ret), K(table_store_iterator));
      }
    } else if (!table->is_sstable()) {
      //skip
    } else if (OB_FALSE_IT(sstable = static_cast<ObSSTable *>(table))) {
    } else if (OB_FAIL(tablet->get_sstable_read_info(sstable, read_info))) {
      LOG_WARN("fail to get index read info ", KR(ret), K(sstable));
    } else if (OB_FAIL(sstable->get_meta(sstable_meta_hdl))) {
        LOG_WARN("failed to get meat", K(ret), K(sstable));
    } else if (table->is_mds_sstable()) {
    } else if (OB_FAIL(micro_iter.open(*sstable, whole_range, tablet->get_rowkey_read_info(), allocator, true))) {
        LOG_WARN("failed to open micro iter", K(ret), K(sstable), K(tablet_id));
    } else {
      while (OB_SUCC(ret)) {
        ObMicroIndexInfo micro_index_info;
        ObSSMicroBlockCacheKey micro_cache_key;
        micro_index_info.reset();
        if (OB_FAIL(micro_iter.get_next(micro_index_info))) {
          if (OB_ITER_END != ret) {
            LOG_WARN("get data micro meta failed", K(ret));
          } else {
            ret = OB_SUCCESS;
            break;
          }
        } else {
          void * mock_data = nullptr;
          if (OB_ISNULL(mock_data = allocator.alloc(micro_index_info.row_header_->get_block_size()))) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to allocate memory", K(ret));
          }

          ObSSMicroBlockMetaInfo micro_meta_info;
          ObSSMicroCacheHitType hit_type;
          micro_cache_key.micro_id_.macro_id_ = micro_index_info.row_header_->get_macro_id();
          micro_cache_key.micro_id_.offset_ = micro_index_info.row_header_->get_block_offset();
          micro_cache_key.micro_id_.size_ = micro_index_info.row_header_->get_block_size();
          micro_cache_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
          ASSERT_EQ(OB_SUCCESS, micro_cache->add_micro_block_cache(micro_cache_key, static_cast<char*>(mock_data),
                                                                  micro_index_info.row_header_->get_block_size(),
                                                                  tablet_id.id(),
                                                                  ObSSMicroCacheAccessType::DDL_PREWARM_TYPE));
          LOG_INFO("add to micro_cache", K(ret), K(micro_cache_key));
      }
     }
    }
  }
  ASSERT_EQ(ret, OB_SUCCESS);
}

void ObSplitPrewarmerTest::check_all_dest_micros_exist(const ObTabletID &dest_tablet_id, ObMicroBlockIndexIterator &micro_iter, bool &all_exist)
{
  int ret = OB_SUCCESS;
  ObMicroIndexInfo micro_index_info;
  ObSSMicroBlockCacheKey micro_cache_key;
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  all_exist = true;
  const int64_t MICRO_BATCH_SIZE = 50;
  ObSEArray<ObSSMicroBlockCacheKey, MICRO_BATCH_SIZE> micro_keys;
  int missed = 0;
  int total = 0;
  while (OB_SUCC(ret)) {
    micro_index_info.reset();
    if (OB_FAIL(micro_iter.get_next(micro_index_info))) {
      if (OB_ITER_END != ret) {
        LOG_WARN("get data micro meta failed", K(ret));
      } else {
        ret = OB_SUCCESS;
        break;
      }
    } else {
      ObSSMicroBlockMetaInfo micro_meta_info;
      ObSSMicroCacheHitType hit_type;
      if (micro_index_info.row_header_->has_valid_logic_micro_id()) {
        micro_cache_key.logic_micro_id_ = micro_index_info.row_header_->get_logic_micro_id();
        micro_cache_key.mode_ = ObSSMicroBlockCacheKeyMode::LOGICAL_KEY_MODE;
        micro_cache_key.micro_crc_ = micro_index_info.get_data_checksum();
      } else {
        micro_cache_key.micro_id_.macro_id_ = micro_index_info.row_header_->get_macro_id();
        micro_cache_key.micro_id_.offset_ = micro_index_info.row_header_->get_block_offset();
        micro_cache_key.micro_id_.size_ = micro_index_info.row_header_->get_block_size();
        micro_cache_key.mode_ = ObSSMicroBlockCacheKeyMode::PHYSICAL_KEY_MODE;
      }
      ASSERT_EQ(OB_SUCCESS, micro_cache->check_micro_block_exist(micro_cache_key,
                                                                micro_meta_info, hit_type));
      if (hit_type == ObSSMicroCacheHitType::SS_CACHE_MISS) {
        LOG_WARN("ly435438 print missed micro cache key", K(ret), K(micro_cache_key));
        missed++;
      }
      total++;
      if (ObSSMicroCacheHitType::SS_CACHE_MISS == hit_type) {
        all_exist = false;
      }
    }
  }
  OK(ret);
  LOG_INFO("micro stat", K(ret), K(missed), K(total));
}
//for future usage
void ObSplitPrewarmerTest::find_cache_info(ObTabletID &tablet_id, TestTabletCacheInfo& cache_info)
{
  cache_info.reuse();
  bool find = false;
  for (int64_t i = 0; !find && i < ctx_.cache_infos_.count(); ++i) {
    TestTabletCacheInfo &cur_cache_info = ctx_.cache_infos_.at(i);
    if (cur_cache_info.tablet_id_ == tablet_id) {
      cache_info = cur_cache_info;
      find = true;
    }
  }
  ASSERT_TRUE(find);
}

TEST_F(ObSplitPrewarmerTest, basic)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(ctx_.tenant_id_));
  common::ObArenaAllocator allocator("splprewarm",
  OB_MALLOC_NORMAL_BLOCK_SIZE, MTL_ID(), ObCtxIds::DEFAULT_CTX_ID);
  OK(sys_exe_sql("alter system set ob_compaction_schedule_interval = '3s' tenant tt1;"));
  // OK(sys_exe_sql("ALTER SYSTEM SET syslog_level='debug';"));
  OK(exe_sql("alter system set _ss_schedule_upload_interval = '1s';"));
  OK(exe_sql("alter system set inc_sstable_upload_thread_score = 20;"));
  OK(sys_exe_sql("alter system set minor_compact_trigger = 2 tenant tt1;"));
  OK(sys_exe_sql("alter system set _ss_major_compaction_prewarm_level = 0 tenant tt1;"));

  LOG_INFO("finish sys_exe_sql");
  create_table_and_insert_data();
  LOG_INFO("finish create_table_and_insert_data");

  ctx_.split_infos_.count();
  ASSERT_GT(ctx_.split_infos_.count(), 0);
  OK(sys_exe_sql("alter system major freeze tenant tt1;"));
  LOG_INFO("finish freeze");
  wait_major_finish();
  LOG_INFO("finish wait_major_finish");
  add_source_micro_to_cache(ctx_.split_infos_.at(0).src_tablet_id_, allocator);
  LOG_INFO("finish add_source_micro_to_cache");
  split_table_p1_partition();
  LOG_INFO("finish split part p1");
  check_dest_caches();
  LOG_INFO("finish check_dest_caches");
}

} // end unittest
} // end oceanbase
int main(int argc, char **argv)
{
  char buf[1000] = {0};
  const int64_t cur_time_ns = ObTimeUtility::current_time_ns();
  databuff_printf(buf, sizeof(buf),
      "%s/%lu?host=%s&access_id=%s&access_key=%s&s3_region=%s&max_iops=10000&max_bandwidth=200000000B&scope=region",
      oceanbase::unittest::S3_BUCKET, cur_time_ns, oceanbase::unittest::S3_ENDPOINT,
      oceanbase::unittest::S3_AK, oceanbase::unittest::S3_SK, oceanbase::unittest::S3_REGION);
  oceanbase::shared_storage_info = buf;
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  GCONF.ob_startup_mode.set_value("shared_storage");
  GCONF.datafile_size.set_value("100G");
  GCONF.memory_limit.set_value("20G");
  GCONF.system_memory.set_value("5G");

  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
