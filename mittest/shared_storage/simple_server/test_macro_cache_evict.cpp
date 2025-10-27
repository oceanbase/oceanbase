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
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/prewarm/ob_storage_cache_policy_prewarmer.h"
#include "storage/shared_storage/storage_cache_policy/ob_storage_cache_tablet_scheduler.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
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
using namespace oceanbase::common;
using namespace oceanbase::lib;

namespace oceanbase
{
char *shared_storage_info = nullptr;
namespace unittest
{

struct TestRunCtx
{
  uint64_t tenant_id_ = 1;
  int64_t tenant_epoch_ = 0;
  ObLSID ls_id_;
  int64_t ls_epoch_;
  ObTabletID tablet_id_;

  TO_STRING_KV(K(tenant_id_), K(tenant_epoch_), K(ls_id_), K(ls_epoch_), K(tablet_id_));
};


enum TestType {
  TEST_MACRO_BLOCK = 0,
  TEST_META_FILE = 1,
  TEST_TMP_FILE = 2,
  TEST_HOT_TABLET_MACRO_BLOCK = 3,
  TEST_EVICT_SCENE1 = 4,
  TEST_EVICT_SCENE2 = 5,
  TEST_WRITE_CACHE_FLUSH = 6,
  TEST_MAX_NUM = 7
};

class ObMacroCacheEvictTest : public ObSimpleClusterTestBase
{
public:
  ObMacroCacheEvictTest()
      : ObSimpleClusterTestBase("test_macro_cache_evict_dir", "50G", "50G", "50G"),
        tenant_created_(false),
        run_ctx_()
  {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();
    if (!tenant_created_) {
      OK(create_tenant("tt1", "5G", "10G", false/*oracle_mode*/, 8, "2G"));
      OK(get_tenant_id(run_ctx_.tenant_id_));
      ASSERT_NE(0, run_ctx_.tenant_id_);
      tenant_created_ = true;
      {
        share::ObTenantSwitchGuard tguard;
        OK(tguard.switch_to(run_ctx_.tenant_id_));
        OK(TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
      }
    }

    // construct write info
    write_buf_[0] = '\0';
    const int64_t mid_offset = WRITE_IO_SIZE / 2;
    memset(write_buf_, 'a', mid_offset);
    memset(write_buf_ + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
    write_info_.io_desc_.set_wait_event(1);
    write_info_.buffer_ = write_buf_;
    write_info_.offset_ = 0;
    write_info_.size_ = WRITE_IO_SIZE;
    write_info_.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info_.mtl_tenant_id_ = run_ctx_.tenant_id_;

  }

  static void TearDownTestCase()
  {
    ResidualDataCleanerHelper::clean_in_mock_env();
    ObSimpleClusterTestBase::TearDownTestCase();
  }

  void info_log(const ObSSMacroCacheType cur_type, const int64_t i, ObTenantDiskSpaceManager *disk_space_mgr,
                ObSSMacroCacheMgr *macro_cache_mgr, const int64_t test_type, const bool is_all = false)
  {
    const int64_t macro_cache_size = disk_space_mgr->get_macro_cache_size();
    const int64_t disk_size = disk_space_mgr->get_total_disk_size();
    const int64_t meta_file_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::META_FILE)].used_;
    const int64_t tmp_file_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].used_;
    const int64_t macro_block_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    const int64_t hot_tablet_macro_block_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].used_;
    const int64_t reserved_size = disk_space_mgr->get_reserved_disk_size();
    const int64_t write_cache_size = macro_cache_mgr->get_write_cache_size();
    const char *cur_type_name = get_ss_macro_cache_type_str(cur_type);
    if (!is_all) {
      const int64_t cur_type_free_size = disk_space_mgr->get_macro_cache_free_size();
      LOG_INFO("evict_test_info_log", "macro_cache_size(MB)", macro_cache_size/ObTenantFileManager::MB,
                                      "disk_size(MB)", disk_size/ObTenantFileManager::MB,
                                      "reserved_size(MB)", reserved_size/ObTenantFileManager::MB,
                                      "meta_file_used_size(MB)", meta_file_used_size/ObTenantFileManager::MB,
                                      "tmp_file_used_size(MB)", tmp_file_used_size/ObTenantFileManager::MB,
                                      "macro_block_used_size(MB)", macro_block_used_size/ObTenantFileManager::MB,
                                      "hot_tablet_macro_block_used_size(MB)", hot_tablet_macro_block_used_size/ObTenantFileManager::MB,
                                      "cur_type_free_size(MB)", cur_type_free_size/ObTenantFileManager::MB,
                                      "write_cache_size(MB)", write_cache_size / ObTenantFileManager::MB,
                                      "write_cache_size(%)", (write_cache_size * 100 / (macro_cache_size - reserved_size)),
                                      K(cur_type_name), K(i), K(test_type));
    } else {
      const int64_t macro_block_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].get_weight() / 100;
      const int64_t meta_file_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::META_FILE)].get_weight() / 100;
      const int64_t tmp_file_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].get_weight() / 100;
      const int64_t hot_tablet_macro_block_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].get_weight() / 100;

      const int64_t meta_file_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::META_FILE)].get_min() / 100;
      const int64_t tmp_file_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].get_min() / 100;
      const int64_t macro_block_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].get_min() / 100;
      const int64_t hot_tablet_macro_block_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].get_min() / 100;

      const int64_t meta_file_max_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::META_FILE)].get_max() / 100;
      const int64_t tmp_file_max_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].get_max() / 100;
      const int64_t macro_block_max_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].get_max() / 100;
      const int64_t hot_tablet_macro_block_max_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].get_max() / 100;

      const int64_t macro_cache_free_size = disk_space_mgr->get_macro_cache_free_size();
      LOG_INFO("evict_test_info_log", "macro_cache_size(MB)", macro_cache_size/ObTenantFileManager::MB,
                                      "disk_size(MB)", disk_size/ObTenantFileManager::MB,
                                      "reserved_size(MB)", reserved_size/ObTenantFileManager::MB,
                                      "macro_block_weight_size(MB)", macro_block_weight_size/ObTenantFileManager::MB,
                                      "meta_file_weight_size(MB)", meta_file_weight_size/ObTenantFileManager::MB,
                                      "tmp_file_weight_size(MB)", tmp_file_weight_size/ObTenantFileManager::MB,
                                      "hot_tablet_macro_block_weight_size(MB)", hot_tablet_macro_block_weight_size/ObTenantFileManager::MB,
                                      "meta_file_min_size(MB)", meta_file_min_size/ObTenantFileManager::MB,
                                      "tmp_file_min_size(MB)", tmp_file_min_size/ObTenantFileManager::MB,
                                      "macro_block_min_size(MB)", macro_block_min_size/ObTenantFileManager::MB,
                                      "hot_tablet_macro_block_min_size(MB)", hot_tablet_macro_block_min_size/ObTenantFileManager::MB,
                                      K(test_type));
      LOG_INFO("evict_test_info_log", "meta_file_used_size(MB)", meta_file_used_size/ObTenantFileManager::MB,
                                      "tmp_file_used_size(MB)", tmp_file_used_size/ObTenantFileManager::MB,
                                      "macro_block_used_size(MB)", macro_block_used_size/ObTenantFileManager::MB,
                                      "hot_tablet_macro_block_used_size(MB)", hot_tablet_macro_block_used_size/ObTenantFileManager::MB,
                                      "macro_cache_free_size(MB)", macro_cache_free_size/ObTenantFileManager::MB,
                                      K(test_type));
    }
  }

  void check_file_remote(const ObSSMacroCacheType cur_type, ObTenantFileManager *tenant_file_mgr,
                         const MacroBlockId &macro_id, const int64_t test_type)
  {
    if (cur_type != ObSSMacroCacheType::TMP_FILE) {
      bool is_exist_remote = false;
      bool is_exist_local = false;

      ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(macro_id, META_FILE_LS_EPOCH_ID, is_exist_remote));
      ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_local_file(macro_id, META_FILE_LS_EPOCH_ID, is_exist_local));
      LOG_INFO("evict_test_info_log", K(is_exist_remote), K(is_exist_local), "cur_type_name", get_ss_macro_cache_type_str(cur_type), K(macro_id), K(test_type));
      ASSERT_EQ(true, is_exist_remote);
      ASSERT_EQ(false, is_exist_local);
    } else {
      MacroBlockId unsealed_remote_seg_id = macro_id;
      unsealed_remote_seg_id.set_storage_object_type((uint64_t)ObStorageObjectType::UNSEALED_REMOTE_SEG_FILE);
      unsealed_remote_seg_id.set_fourth_id(WRITE_IO_SIZE);
      bool is_exist_remote = false;
      ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->is_exist_remote_file(unsealed_remote_seg_id, META_FILE_LS_EPOCH_ID, is_exist_remote));
      LOG_INFO("evict_test_info_log", K(is_exist_remote), "cur_type_name", get_ss_macro_cache_type_str(cur_type), K(macro_id), K(test_type));
      ASSERT_EQ(true, is_exist_remote);
    }
  }

  void init_wr_macro_cache(ObTenantFileManager *tenant_file_mgr, const int64_t test_type)
  {
    /*
    The reason for using 9999 is to ensure that this id value will not be written back during subsequent tests.
    Because during initialization, 2M will be written first as an additional occupation to facilitate test evict.
    Some operations after initialization may still write data of the macro block type.
    However, generally, the id values start in sequence from 0, hundreds of them, and will not reach 9999
    */
    macro_cache_write(9999, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK, test_type);
    macro_cache_write(10000, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK, test_type);
    macro_cache_write(10001, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK, test_type);
  }

  void test_evict_scene1(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                         ObTenantFileManager *tenant_file_mgr, const int64_t test_type)
  {
    /* MACRO_BLOCK(>min(much)), TMP_FILE(>min(a little bit)), META_FILE(<min), HOT_TABLET_MACRO_BLOCK(close to 0)
    => result: evict macro block */
    const int64_t macro_cache_size = disk_space_mgr->get_macro_cache_size();
    const int64_t tmp_file_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].get_min() / 100;
    const int64_t macro_block_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].get_min() / 100;
    const int64_t macro_block_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].get_weight() / 100;
    const int64_t tmp_file_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].get_weight() / 100;
    const int64_t tmp_file_num = tmp_file_min_size / WRITE_IO_SIZE + 1;
    int64_t i = 0;
    int64_t macro_block_free_size = 0;
    int64_t macro_block_used_size = 0, macro_block_used_size_af_evict = 0;
    int64_t tmp_file_used_size = 0, tmp_file_used_size_af_evict = 0;
    // At first, write 6M of macro block into the macro cache, which is convenient for observation during subsequent test evict
    init_wr_macro_cache(tenant_file_mgr, test_type);
    // shut down evict
    macro_cache_mgr->evict_task_.is_inited_ = false;
    // init status
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type, true);
    while (i < tmp_file_num) {
      macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::TMP_FILE, test_type);
      info_log(ObSSMacroCacheType::TMP_FILE, i, disk_space_mgr, macro_cache_mgr, test_type);
      i = i+1;
    }
    i = 0;
    macro_block_free_size = disk_space_mgr->get_macro_cache_free_size();
    while (macro_block_free_size >= WRITE_IO_SIZE) {
      // write
      macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK, test_type);
      info_log(ObSSMacroCacheType::MACRO_BLOCK, i, disk_space_mgr, macro_cache_mgr, test_type);
      i = i+1;
      macro_block_free_size = disk_space_mgr->get_macro_cache_free_size();
    }
    ASSERT_LT(macro_block_free_size, WRITE_IO_SIZE);
    // write to bucket   ->   check write through
    macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK, test_type, true);
    // before evict
    macro_block_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    tmp_file_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].used_;
    info_log(ObSSMacroCacheType::MACRO_BLOCK, i, disk_space_mgr, macro_cache_mgr, test_type);
    i = i+1;
    // start evict
    macro_cache_mgr->evict_task_.is_inited_ = true;
    sleep(5);   // some time for evict
    // after evict
    macro_block_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    tmp_file_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].used_;
    info_log(ObSSMacroCacheType::MACRO_BLOCK, -1, disk_space_mgr, macro_cache_mgr, test_type);
    ASSERT_LT(macro_block_used_size_af_evict, macro_block_used_size);
    // delete file
    delete_all_wr_file(tmp_file_num, tenant_file_mgr, ObSSMacroCacheType::TMP_FILE);
    delete_all_wr_file(i, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK);

    info_log(ObSSMacroCacheType::MACRO_BLOCK, -1, disk_space_mgr, macro_cache_mgr, test_type);
  }

  void test_evict_scene2(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                         ObTenantFileManager *tenant_file_mgr, const int64_t test_type)
  {
    /* META_FILE(some), HOT_TABLET_MACRO_BLOCK(<min), MACRO_BLOCK(<min), TMP_FILE(close to 0)
    => result: evict macro block */
    const int64_t macro_cache_size = disk_space_mgr->get_macro_cache_size();
    const int64_t hot_tablet_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].get_min() / 100;
    const int64_t macro_block_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].get_weight() / 100;
    const int64_t hot_tablet_weight_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].get_weight() / 100;
    const int64_t macro_block_min_size = macro_cache_size * disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].get_min() / 100;
    const int64_t hot_tablet_num = hot_tablet_min_size / WRITE_IO_SIZE - 1;
    int64_t i = 0;
    int64_t meta_file_free_size = 0;
    int64_t macro_block_used_size = 0, macro_block_used_size_af_evict = 0;
    int64_t hot_tablet_used_size = 0, hot_tablet_used_size_af_evict = 0;

    // At first, write 6M of macro block into the macro cache, which is convenient for observation during subsequent test evict
    init_wr_macro_cache(tenant_file_mgr, test_type);
    // shut down evict
    macro_cache_mgr->evict_task_.is_inited_ = false;
    // init status
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type, true);
    while (i < hot_tablet_num) {
      macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, test_type);
      info_log(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, i, disk_space_mgr, macro_cache_mgr, test_type);
      i = i+1;
    }
    i = 0;
    meta_file_free_size = disk_space_mgr->get_macro_cache_free_size();
    while (meta_file_free_size >= WRITE_IO_SIZE) {
      // write
      macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::META_FILE, test_type);
      info_log(ObSSMacroCacheType::META_FILE, i, disk_space_mgr, macro_cache_mgr, test_type);
      i = i+1;
      meta_file_free_size = disk_space_mgr->get_macro_cache_free_size();
    }
    ASSERT_LT(meta_file_free_size, WRITE_IO_SIZE);
    // write to bucket   ->   check write through
    macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::META_FILE, test_type, true);
    // before evict
    macro_block_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    hot_tablet_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].used_;
    info_log(ObSSMacroCacheType::META_FILE, i, disk_space_mgr, macro_cache_mgr, test_type);
    i = i+1;
    // start evict
    macro_cache_mgr->evict_task_.is_inited_ = true;
    sleep(5);   // some time for evict
    // after evict
    macro_block_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    hot_tablet_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].used_;
    info_log(ObSSMacroCacheType::META_FILE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    ASSERT_LT(macro_block_used_size_af_evict, macro_block_used_size);
    // delete file
    delete_all_wr_file(hot_tablet_num, tenant_file_mgr, ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK);
    delete_all_wr_file(i, tenant_file_mgr, ObSSMacroCacheType::META_FILE);

    info_log(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, -1, disk_space_mgr, macro_cache_mgr, test_type);
  }

  void macro_cache_write(const int64_t i, ObTenantFileManager *tenant_file_mgr, const ObSSMacroCacheType cache_type,
                         const int64_t test_type, const bool if_check = false)
  {
    MacroBlockId macro_id;
    ObStorageObjectHandle write_object_handle;
    // macro id setting
    switch (cache_type) {
      case ObSSMacroCacheType::MACRO_BLOCK:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(MACRO_BLOCK_TABLET_ID); // tablet_id
        macro_id.set_third_id(MACRO_BLOCK_SERVER_ID); // server_id
        macro_id.set_macro_transfer_epoch(MACRO_BLOCK_TRANSFER_SEQ); // transfer_seq
        macro_id.set_tenant_seq(i); // tenant_seq
        write_info_.set_effective_tablet_id(MACRO_BLOCK_TABLET_ID); // effective_tablet_id
        break;
      case ObSSMacroCacheType::META_FILE:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
        macro_id.set_second_id(META_FILE_LS_ID);  // ls_id
        macro_id.set_third_id(META_FILE_TABLET_ID); // tablet_id
        macro_id.set_meta_transfer_epoch(META_FILE_TRANSFER_SEQ); //transfer_seq
        macro_id.set_meta_version_id(i); // meta_version_id
        write_info_.set_ls_epoch_id(META_FILE_LS_EPOCH_ID); // ls_epoch_id
        break;
      case ObSSMacroCacheType::TMP_FILE:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        macro_id.set_second_id(TMP_FILE_ID); // tmp_file_id
        macro_id.set_third_id(i); // segment_id
        write_info_.offset_ = 0;
        write_info_.size_ = WRITE_IO_SIZE;
        write_info_.set_tmp_file_valid_length(WRITE_IO_SIZE);
        write_info_.io_desc_.set_unsealed();
        break;
      case ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(HOT_TABLET_TABLET_ID);
        macro_id.set_third_id(HOT_TABLET_SERVER_ID);
        macro_id.set_macro_transfer_epoch(HOT_TABLET_TRANSFER_SEQ); // transfer_seq
        macro_id.set_tenant_seq(i); // tenant_seq
        write_info_.set_effective_tablet_id(HOT_TABLET_TABLET_ID); // effective_tablet_id
        break;
      default:
        ASSERT_TRUE(false);
        break;
    }
    ASSERT_TRUE(macro_id.is_valid());
    ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
    if (static_cast<TestType>(test_type) == TEST_WRITE_CACHE_FLUSH) {
      write_info_.set_is_write_cache(true); // write cache
    } else {
      write_info_.set_is_write_cache(false); // preread macro is read cache
    }
    if (cache_type == ObSSMacroCacheType::TMP_FILE) {
      ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_append_file(write_info_, write_object_handle));
    } else {
      ASSERT_EQ(OB_SUCCESS, ObSSObjectAccessUtil::async_write_file(write_info_, write_object_handle));
    }
    ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
    write_object_handle.reset();
    if (if_check) {
      check_file_remote(cache_type, tenant_file_mgr, macro_id, test_type);
    }
  }

  void delete_all_wr_file(const int64_t num, ObTenantFileManager *tenant_file_mgr, const ObSSMacroCacheType cache_type)
  {
    for (int64_t j = 0; j < num; j++) {
      MacroBlockId macro_id;
      int64_t ls_epoch_id = 0;
      switch (cache_type) {
        case ObSSMacroCacheType::MACRO_BLOCK:
          macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
          macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
          macro_id.set_second_id(MACRO_BLOCK_TABLET_ID); // tablet_id
          macro_id.set_third_id(MACRO_BLOCK_SERVER_ID); // server_id
          macro_id.set_macro_transfer_epoch(MACRO_BLOCK_TRANSFER_SEQ); // transfer_seq
          macro_id.set_tenant_seq(j); // tenant_seq
          break;
        case ObSSMacroCacheType::META_FILE:
          macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
          macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
          macro_id.set_second_id(META_FILE_LS_ID);  // ls_id
          macro_id.set_third_id(META_FILE_TABLET_ID); // tablet_id
          macro_id.set_meta_transfer_epoch(META_FILE_TRANSFER_SEQ); //transfer_seq
          macro_id.set_meta_version_id(j); // meta_version_id
          ls_epoch_id = META_FILE_LS_EPOCH_ID;
          break;
        case ObSSMacroCacheType::TMP_FILE:
          macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
          macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
          macro_id.set_second_id(TMP_FILE_ID); // tmp_file_id
          macro_id.set_third_id(j); // segment_id
          break;
        case ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK:
          macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
          macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
          macro_id.set_second_id(HOT_TABLET_TABLET_ID);
          macro_id.set_third_id(HOT_TABLET_SERVER_ID);
          macro_id.set_macro_transfer_epoch(HOT_TABLET_TRANSFER_SEQ); // transfer_seq
          macro_id.set_tenant_seq(j); // tenant_seq
          break;
        default:
          ASSERT_TRUE(false);
          break;
      }
      ASSERT_TRUE(macro_id.is_valid());
      ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->delete_file(macro_id, ls_epoch_id));
    }
  }

  void test_macro_cache_max(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                            ObTenantFileManager *tenant_file_mgr, const ObSSMacroCacheType cache_type, const int64_t test_type)
  {
    const int64_t hot_tablet_macro_block_min_size = (disk_space_mgr->get_macro_cache_size()) * (disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].get_min()) / 100;
    int64_t i = 0;
    int64_t cur_type_free_size = 0;
    int64_t meta_file_used_size = 0, tmp_file_used_size = 0, macro_block_used_size = 0, hot_tablet_used_size = 0;
    int64_t meta_file_used_size_af_evict = 0, tmp_file_used_size_af_evict = 0, macro_block_used_size_af_evict = 0, hot_tablet_used_size_af_evict = 0;
    // At first, write 6M of macro block into the macro cache, which is convenient for observation during subsequent test evict
    init_wr_macro_cache(tenant_file_mgr, test_type);
    // shut down evict
    macro_cache_mgr->evict_task_.is_inited_ = false;
    info_log(cache_type, -1, disk_space_mgr, macro_cache_mgr, test_type);
    cur_type_free_size = disk_space_mgr->get_macro_cache_free_size();
    while (cur_type_free_size >= WRITE_IO_SIZE) {
      macro_cache_write(i, tenant_file_mgr, cache_type, test_type);
      info_log(cache_type, i, disk_space_mgr, macro_cache_mgr, test_type);
      cur_type_free_size = disk_space_mgr->get_macro_cache_free_size();
      i = i+1;
    }
    ASSERT_LT(cur_type_free_size, WRITE_IO_SIZE);
    // write to bucket   ->   check write through
    macro_cache_write(i, tenant_file_mgr, cache_type, test_type, true);
    // before evict record
    meta_file_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::META_FILE)].used_;
    tmp_file_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].used_;
    macro_block_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    hot_tablet_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].used_;
    info_log(cache_type, i, disk_space_mgr, macro_cache_mgr, test_type);
    i = i+1;
    // start evict
    macro_cache_mgr->evict_task_.is_inited_ = true;
    sleep(5);   // some time for evict
    // after evict record
    meta_file_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::META_FILE)].used_;
    tmp_file_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].used_;
    macro_block_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    hot_tablet_used_size_af_evict = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].used_;
    info_log(cache_type, -1, disk_space_mgr, macro_cache_mgr, test_type);
    switch (cache_type) {
      case ObSSMacroCacheType::MACRO_BLOCK:
        ASSERT_LT(macro_block_used_size_af_evict, macro_block_used_size);
        break;
      case ObSSMacroCacheType::META_FILE:
        ASSERT_LT(macro_block_used_size_af_evict, macro_block_used_size);
        break;
      case ObSSMacroCacheType::TMP_FILE:
        ASSERT_LT(tmp_file_used_size_af_evict, tmp_file_used_size);
        break;
      case ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK:
        ASSERT_LT(hot_tablet_used_size_af_evict, hot_tablet_used_size);
        break;
      default:
        ASSERT_TRUE(false);
        break;
    }
    // sleep(3600);
    // delete file
    delete_all_wr_file(i, tenant_file_mgr, cache_type);
    info_log(cache_type, -1, disk_space_mgr, macro_cache_mgr, test_type);
  }

  void test_write_cache_flush(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                              ObTenantFileManager *tenant_file_mgr, const int64_t test_type)
  {
    int64_t write_cache_size = macro_cache_mgr->get_write_cache_size();
    int64_t write_cache_size_af_flush = 0;
    const int64_t macro_cache_size = disk_space_mgr->get_macro_cache_size();
    const int64_t reserved_size = disk_space_mgr->get_reserved_disk_size();
    int64_t i = 0;
    info_log(ObSSMacroCacheType::MACRO_BLOCK, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // shut down write cache flush
    macro_cache_mgr->write_cache_ctrl_task_.is_inited_ = false;
    // shut down evict
    macro_cache_mgr->evict_task_.is_inited_ = false;
    while ((write_cache_size * 100 / (macro_cache_size - reserved_size)) < macro_cache_mgr->WRITE_CACHE_THRESHOLD) {
      macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK, test_type);
      info_log(ObSSMacroCacheType::MACRO_BLOCK, i, disk_space_mgr, macro_cache_mgr, test_type);
      write_cache_size = macro_cache_mgr->get_write_cache_size();
      i = i + 1;
    }
    macro_cache_write(i, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK, test_type, true); // write to bucket -> check write through
    // before flush
    write_cache_size = macro_cache_mgr->get_write_cache_size();
    info_log(ObSSMacroCacheType::MACRO_BLOCK, i, disk_space_mgr, macro_cache_mgr, test_type);
    // start write cache flush
    macro_cache_mgr->write_cache_ctrl_task_.is_inited_ = true;
    i = i + 1;
    // some time for write cache flush
    for (int64_t j = 0; j < 80; j++) {
      sleep(1);
      write_cache_size_af_flush = macro_cache_mgr->get_write_cache_size();
      if (write_cache_size_af_flush < write_cache_size) {
        break;
      }
    }
    info_log(ObSSMacroCacheType::MACRO_BLOCK, -1, disk_space_mgr, macro_cache_mgr, test_type);
    ASSERT_LT(write_cache_size_af_flush, write_cache_size);
    // delete file
    delete_all_wr_file(i, tenant_file_mgr, ObSSMacroCacheType::MACRO_BLOCK);
    info_log(ObSSMacroCacheType::MACRO_BLOCK, -1, disk_space_mgr, macro_cache_mgr, test_type);
    macro_cache_mgr->evict_task_.is_inited_ = true; // restore evict
  }

  public:
    static constexpr int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
    static const uint64_t MACRO_BLOCK_TABLET_ID = 200004; // tablet_id for MACRO_BLOCK
    static const uint64_t TMP_FILE_ID = 100; // tmp_file_id for TMP_FILE
    static const uint64_t HOT_TABLET_TABLET_ID = 200005; // tablet_id for HOT_TABLET_MACRO_BLOCK
    static const uint64_t META_FILE_TABLET_ID = 200001; // tablet_id for META_FILE
    static const uint64_t META_FILE_LS_ID = 1001; // ls_id for META_FILE
    static const uint64_t META_FILE_LS_EPOCH_ID = 1; // ls_epoch_id for META_FILE
    static const uint64_t MACRO_BLOCK_TRANSFER_SEQ = 0; // transfer_seq for MACRO_BLOCK
    static const uint64_t HOT_TABLET_TRANSFER_SEQ = 0; // transfer_seq for HOT_TABLET_MACRO_BLOCK
    static const uint64_t META_FILE_TRANSFER_SEQ = 0; // transfer_seq for META_FILE
    static const uint64_t MACRO_BLOCK_SERVER_ID = 1; // server_id for MACRO_BLOCK
    static const uint64_t HOT_TABLET_SERVER_ID = 2; // server_id for HOT_TABLET_MACRO_BLOCK

    ObStorageObjectWriteInfo write_info_;
    char write_buf_[WRITE_IO_SIZE];

protected:
  bool tenant_created_;
  TestRunCtx run_ctx_;
};

TEST_F(ObMacroCacheEvictTest, test_evict)
{
  share::ObTenantSwitchGuard tguard;
  OK(tguard.switch_to(run_ctx_.tenant_id_));

  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ASSERT_NE(nullptr, policy_service);
  policy_service->update_tablet_status(200005, PolicyStatus::HOT); //HOT 0
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  for (int64_t test_type = TEST_MACRO_BLOCK; test_type < TEST_MAX_NUM; test_type++) {
    switch (static_cast<TestType>(test_type)) {
      case TEST_MACRO_BLOCK:                      /* write to macro cache -- MACRO_BLOCK */
        test_macro_cache_max(macro_cache_mgr, disk_space_mgr, tenant_file_mgr,
                              ObSSMacroCacheType::MACRO_BLOCK, test_type);
        break;
      case TEST_META_FILE:                        /* write to macro cache -- META_FILE */
        test_macro_cache_max(macro_cache_mgr, disk_space_mgr, tenant_file_mgr,
                              ObSSMacroCacheType::META_FILE, test_type);
        break;
      case TEST_TMP_FILE:                         /* write to macro cache -- TMP_FILE */
        test_macro_cache_max(macro_cache_mgr, disk_space_mgr, tenant_file_mgr,
                              ObSSMacroCacheType::TMP_FILE, test_type);
        break;
      case TEST_HOT_TABLET_MACRO_BLOCK:           /* write to macro cache -- HOT_TABLET_MACRO_BLOCK */
        test_macro_cache_max(macro_cache_mgr, disk_space_mgr, tenant_file_mgr,
                              ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK, test_type);
        break;
      case TEST_EVICT_SCENE1:                     /* evict priority  MACRO_BLOCK > TMP_FILE > META_FILE > HOT_TABLET_MACRO_BLOCK => result: evict macro block */
        test_evict_scene1(macro_cache_mgr, disk_space_mgr, tenant_file_mgr, test_type);
        break;
      case TEST_EVICT_SCENE2:                     /* evict priority  META_FILE > HOT_TABLET_MACRO_BLOCK(< min) > MACRO_BLOCK > TMP_FILE => result: evict macro block */
        test_evict_scene2(macro_cache_mgr, disk_space_mgr, tenant_file_mgr, test_type);
        break;
      case TEST_WRITE_CACHE_FLUSH:                /* write cache flush */
        test_write_cache_flush(macro_cache_mgr, disk_space_mgr, tenant_file_mgr, test_type);
        break;
      default:
        LOG_INFO("Invalid test type");
        break;
    }
  }
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
