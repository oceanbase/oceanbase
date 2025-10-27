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
#define USING_LOG_PREFIX STORAGETEST
#include <filesystem>
#define protected public
#define private public
#include "mittest/shared_storage/test_ss_common_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "observer/omt/ob_tenant_config_mgr.h"
#include "storage/shared_storage/ob_ss_object_access_util.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_mgr.h"
#include "mittest/shared_storage/test_ss_macro_cache_mgr_util.h"
#include "storage/shared_storage/ob_disk_space_manager.h"
#include "storage/shared_storage/ob_file_manager.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache.h"
#include "storage/shared_storage/macro_cache/ob_ss_macro_cache_common_meta.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "observer/ob_server.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::omt;
using namespace oceanbase::storage;
using namespace oceanbase::observer;
namespace fs = std::filesystem;

class MyObSSLocalCacheWriter : public ObSSLocalCacheWriter
{
protected:
  bool need_add_macro_cache(const blocksstable::ObStorageObjectType object_type) override;
};

class MyObSSMacroCacheUpdateCallback
{
public:
  MyObSSMacroCacheUpdateCallback() {}
  virtual ~MyObSSMacroCacheUpdateCallback() {}
  void operator()(const SSMacroCacheMetaMapPairType &entry);
  bool ret_;

private:
  const int64_t SAFE_ERASE_INTERVAL_US = 3600 * 1000L * 1000L; // 1h
};

void MyObSSMacroCacheUpdateCallback::operator()(const SSMacroCacheMetaMapPairType &entry)
{
  int ret = OB_SUCCESS;
  ObSSMacroCacheMeta *meta_ptr = nullptr;
  if (OB_ISNULL(meta_ptr = entry.second.get_ptr())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("meta ptr should not be null", KR(ret), K(entry));
  } else {
    // LOG_INFO("calibrate_test_info_log", K(meta_ptr->get_effective_tablet_id()));
    meta_ptr->set_last_access_time_us(ObTimeUtility::current_time_us() - SAFE_ERASE_INTERVAL_US);
  }
  ret_ = ret;
}

bool MyObSSLocalCacheWriter::need_add_macro_cache(const blocksstable::ObStorageObjectType object_type)
{
  return false;
}

class ObMacroCacheCalibrateTest : public ::testing::Test
{
public:
  ObMacroCacheCalibrateTest() = default;
  virtual ~ObMacroCacheCalibrateTest() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();

  void info_log(const ObSSMacroCacheType cur_type, const int64_t i, ObTenantDiskSpaceManager *disk_space_mgr,
                ObSSMacroCacheMgr *macro_cache_mgr, const int64_t test_type)
 {
    int64_t meta_map_size = macro_cache_mgr->meta_map_.size();
    int64_t meta_file_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::META_FILE)].used_;
    int64_t tmp_file_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::TMP_FILE)].used_;
    int64_t macro_block_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::MACRO_BLOCK)].used_;
    int64_t hot_tablet_macro_block_used_size = disk_space_mgr->macro_cache_stats_[static_cast<uint8_t>(ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK)].used_;
    const char *cur_type_name = get_ss_macro_cache_type_str(cur_type);

    LOG_INFO("calibrate_test_info_log", "meta_file_used_size(MB)", meta_file_used_size/ObTenantFileManager::MB,
                                        "tmp_file_used_size(MB)", tmp_file_used_size/ObTenantFileManager::MB,
                                        "macro_block_used_size(MB)", macro_block_used_size/ObTenantFileManager::MB,
                                        "hot_tablet_macro_block_used_size(MB)", hot_tablet_macro_block_used_size/ObTenantFileManager::MB,
                                        K(cur_type_name), K(meta_map_size), K(i), K(test_type));
  }

  void check_exist(const ObSSMacroCacheType cur_type, ObSSMacroCacheMgr *macro_cache_mgr,
                         const int64_t i, const int64_t test_type, const bool exist)
  {
    MacroBlockId macro_id;
    bool is_exist = false;
    switch (cur_type) {
      case ObSSMacroCacheType::MACRO_BLOCK:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(MACRO_BLOCK_TABLET_ID);       //tablet_id
        macro_id.set_third_id(MACRO_BLOCK_SERVER_ID);        //server_id
        macro_id.set_macro_transfer_epoch(MACRO_BLOCK_TRANSFER_SEQ);   // transfer_seq
        macro_id.set_tenant_seq(i);           // tenant_seq
        break;
      case ObSSMacroCacheType::META_FILE:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
        macro_id.set_second_id(META_FILE_LS_ID);  // ls_id
        macro_id.set_third_id(META_FILE_TABLET_ID); // tablet_id
        macro_id.set_meta_transfer_epoch(META_FILE_TRANSFER_SEQ); //transfer_seq
        macro_id.set_meta_version_id(i); // meta_version_id
        break;
      case ObSSMacroCacheType::TMP_FILE:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::TMP_FILE);
        macro_id.set_second_id(TMP_FILE_ID); // tmp_file_id
        macro_id.set_third_id(i); // segment_id
        break;
      case ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK:
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(HOT_TABLET_TABLET_ID);
        macro_id.set_third_id(HOT_TABLET_SERVER_ID); // server_id
        macro_id.set_macro_transfer_epoch(HOT_TABLET_TRANSFER_SEQ); // transfer_seq
        macro_id.set_tenant_seq(i); // tenant_seq
        break;
      default:
        ASSERT_TRUE(false);
        break;
    }
    ASSERT_TRUE(macro_id.is_valid());
    macro_cache_mgr->exist(macro_id, is_exist);
    ASSERT_TRUE(is_exist == exist);
  }

  void delete_all_wr_file(const int64_t id_start, const int64_t id_end, ObTenantFileManager *tenant_file_mgr, const ObSSMacroCacheType cache_type)
  {
    for (int64_t j = id_start; j < id_end; j++) {
      MacroBlockId macro_id;
      int64_t ls_epoch_id = 0;
      switch (cache_type) {
        case ObSSMacroCacheType::MACRO_BLOCK:
          macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
          macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
          macro_id.set_second_id(MACRO_BLOCK_TABLET_ID);
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
          macro_id.set_third_id(HOT_TABLET_SERVER_ID); // server_id
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

  void macro_cache_put_write(const int64_t i, ObSSMacroCacheMgr *macro_cache_mgr, const ObSSMacroCacheType cache_type,
                             const int64_t test_type, const bool if_write = false, const bool if_put = true)
  {
    MacroBlockId macro_id;
    ObStorageObjectHandle write_object_handle;
    MyObSSMacroCacheUpdateCallback update_callback;
    ObSSMacroCacheInfo macro_cache_info;
    MyObSSLocalCacheWriter my_local_cache_writer;
    uint64_t tablet_id = 0;
    // put or write
    switch (cache_type) {
      case ObSSMacroCacheType::MACRO_BLOCK:
        tablet_id = MACRO_BLOCK_TABLET_ID;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(tablet_id);       //tablet_id
        macro_id.set_third_id(MACRO_BLOCK_SERVER_ID);             //server_id
        macro_id.set_macro_transfer_epoch(MACRO_BLOCK_TRANSFER_SEQ);   // transfer_seq
        macro_id.set_tenant_seq(i);           // tenant_seq
        break;
      case ObSSMacroCacheType::META_FILE:
        tablet_id = META_FILE_TABLET_ID;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_TABLET_META);
        macro_id.set_second_id(META_FILE_LS_ID);  // ls_id
        macro_id.set_third_id(tablet_id); // tablet_id
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
        tablet_id = HOT_TABLET_TABLET_ID;
        macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
        macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::PRIVATE_DATA_MACRO);
        macro_id.set_second_id(tablet_id);
        macro_id.set_third_id(HOT_TABLET_SERVER_ID); //server_id
        macro_id.set_macro_transfer_epoch(HOT_TABLET_TRANSFER_SEQ); // transfer_seq
        macro_id.set_tenant_seq(i); // tenant_seq
        break;
      default:
        ASSERT_TRUE(false);
        break;
    }
    ASSERT_TRUE(macro_id.is_valid());
    macro_cache_info = ObSSMacroCacheInfo(tablet_id, WRITE_IO_SIZE, cache_type, false/*is_write_cache*/);
    ASSERT_TRUE(macro_cache_info.is_valid());
    if (if_put) {
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->put(macro_id, macro_cache_info));
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->meta_map_.atomic_refactored(macro_id, update_callback));
      ASSERT_EQ(OB_SUCCESS, update_callback.ret_);
      ASSERT_NE(nullptr, macro_cache_mgr->meta_map_.get(macro_id));
    }
    if (if_write) {
      ASSERT_EQ(OB_SUCCESS, write_object_handle.set_macro_block_id(macro_id));
      ASSERT_EQ(OB_SUCCESS, macro_cache_mgr->alloc_file_size(cache_type, WRITE_IO_SIZE, ObDiskSpaceType::FILE));
      ASSERT_EQ(OB_SUCCESS, my_local_cache_writer.aio_write_with_create_parent_dir(write_info_, write_object_handle));
      ASSERT_EQ(OB_SUCCESS, write_object_handle.wait());
      write_object_handle.reset();
    }
  }

  void test_macro_cache_calibrate(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                                  ObTenantFileManager *tenant_file_mgr, const ObSSMacroCacheType cache_type, const int64_t test_type)
  {
    const int64_t num = 20;
    const char *cur_type_name = get_ss_macro_cache_type_str(cache_type);
    bool is_exist = false;
    int64_t meta_map_size = 0, meta_map_size_af_calibrate = 0;
    // init status
    info_log(cache_type, -1, disk_space_mgr, macro_cache_mgr, test_type);
    for (int64_t i = 0; i < num; i++) {
      if (i < num / 2) {
        macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, true);
      } else {
        macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, false);
      }
    }
    // before calibrate
    meta_map_size = macro_cache_mgr->meta_map_.size();
    info_log(cache_type, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // check file exist
    for (int64_t i = 0; i < num; i++) {
      is_exist = true;
      check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
    }
    // calibrate
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
    // after clibrate
    meta_map_size_af_calibrate = macro_cache_mgr->meta_map_.size();
    info_log(cache_type, -1, disk_space_mgr, macro_cache_mgr, test_type);
    LOG_INFO("calibrate_test_info_log", K(meta_map_size - meta_map_size_af_calibrate), K(num / 2), K(cur_type_name), K(test_type));
    ASSERT_TRUE(meta_map_size - meta_map_size_af_calibrate == num / 2);
    // check file exist
    for (int64_t i = 0; i < num; i++) {
      if (i < num / 2) {
        is_exist = true;
        check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
      } else {
        is_exist = false;
        check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
      }
    }
    // delete file
    delete_all_wr_file(0, num / 2, tenant_file_mgr, cache_type);
    info_log(cache_type, -1, disk_space_mgr, macro_cache_mgr, test_type);
  }

  void test_calibrate_map_less(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                               ObTenantFileManager* tenant_file_mgr, const int64_t test_type)
  {
    const int64_t num = 20;
    bool is_exist = false;
    int64_t meta_map_size = 0, meta_map_size_af_calibrate = 0;
    // init status
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // put or write
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        if (i < num / 2) {
          macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, true, true);   // write & put
        } else {
          macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, true, false); // only write
        }
      }
    }
    // before calibrate
    meta_map_size = macro_cache_mgr->meta_map_.size();
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // check file exist
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        if (i < num / 2) {
          is_exist = true;
          check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
        } else {
          is_exist = false;
          check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
        }
      }
    }
    // calibrate
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
    // after clibrate
    meta_map_size_af_calibrate = macro_cache_mgr->meta_map_.size();
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // num / 2 * 2 -> Because two types of macro caches were written: MACRO_BLOCK, TMP_FILE
    LOG_INFO("calibrate_test_info_log", K(meta_map_size_af_calibrate - meta_map_size), K(num / 2 * 2), K(test_type));
    ASSERT_TRUE((meta_map_size_af_calibrate - meta_map_size) == (num / 2 * 2));
    // check file exist
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
          is_exist = true;
          check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
      }
    }
    // delete file
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      delete_all_wr_file(0, num, tenant_file_mgr, cache_type);
    }
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
  }

  void test_calibrate_map_more(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                                  ObTenantFileManager *tenant_file_mgr, const int64_t test_type)
  {
    const int64_t num = 20;
    bool is_exist = false;
    int64_t meta_map_size = 0, meta_map_size_af_calibrate = 0;
    // init status
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // put or write
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        if (i < num / 2) {
          macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, true, true);    // write & put
        } else {
          macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, false, true);   // only put
        }
      }
    }
    // before calibrate
    meta_map_size = macro_cache_mgr->meta_map_.size();
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // check file exist
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        is_exist = true;
        check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
      }
    }
    // calibrate
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
    // after clibrate
    meta_map_size_af_calibrate = macro_cache_mgr->meta_map_.size();
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // num / 2 * 2 -> Because two types of macro caches were written: MACRO_BLOCK, TMP_FILE
    LOG_INFO("calibrate_test_info_log", K(meta_map_size - meta_map_size_af_calibrate), K(num / 2 * 2), K(test_type));
    ASSERT_TRUE((meta_map_size - meta_map_size_af_calibrate) == (num / 2 * 2));
    // check file exist
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        if (i < num / 2) {
          is_exist = true;
          check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
        } else {
          is_exist = false;
          check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
        }
      }
    }
    // delete file
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      delete_all_wr_file(0, num, tenant_file_mgr, cache_type);
    }
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
  }

  void write_unexpected_path_file(const char *dir_path)
  {
    fs::path folderPath = dir_path;
    if (fs::exists(folderPath)) {
      fs::path filePath = folderPath / "abcde.txt";
      std::ofstream outFile(filePath);
      if (outFile.is_open()) {
        outFile << "abcdefghijklmnopkrstuvwxyz\n";
        outFile.close();
      } else {
        ASSERT_TRUE(false);
      }
      fs::path filePath2 = folderPath / "abcde.T8";
      std::ofstream outFile2(filePath2);
      if (outFile2.is_open()) {
        outFile2 << "abcdefghijklmnopkrstuvwxyz\n";
        outFile2.close();
      } else {
        ASSERT_TRUE(false);
      }
    } else {
      ASSERT_TRUE(false);
    }
  }

  void test_unexpected_path(ObSSMacroCacheMgr *macro_cache_mgr, ObTenantDiskSpaceManager *disk_space_mgr,
                                  ObTenantFileManager *tenant_file_mgr, const int64_t test_type)
  {
    const int64_t num = 20;
    bool is_exist = false;
    int64_t meta_map_size = 0, meta_map_size_af_calibrate = 0;
    char dir_path[common::MAX_PATH_SIZE] = {0};
    // init status
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // put or write
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        if (i < num / 2) {
          macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, true, true);    // write & put
        } else {
          macro_cache_put_write(i, macro_cache_mgr, cache_type, test_type, false, true);   // only put
        }
      }
      if (cache_type == ObSSMacroCacheType::TMP_FILE) {
        ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tmp_file_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), TMP_FILE_ID));
        write_unexpected_path_file(dir_path);
        LOG_INFO("calibrate_test_info_log", K(dir_path), K(test_type));
      } else if (cache_type == ObSSMacroCacheType::MACRO_BLOCK) {
        ASSERT_EQ(OB_SUCCESS, OB_DIR_MGR.get_local_tablet_id_macro_dir(dir_path, sizeof(dir_path), MTL_ID(), MTL_EPOCH_ID(), MACRO_BLOCK_TABLET_ID, MACRO_BLOCK_TRANSFER_SEQ, ObMacroType::DATA_MACRO));
        write_unexpected_path_file(dir_path);
        LOG_INFO("calibrate_test_info_log", K(dir_path), K(test_type));
      }
    }
    // before calibrate
    meta_map_size = macro_cache_mgr->meta_map_.size();
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // check file exist
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        is_exist = true;
        check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
      }
    }
    // calibrate
    ASSERT_EQ(OB_SUCCESS, tenant_file_mgr->calibrate_disk_space_task_.calibrate_disk_space());
    // after clibrate
    meta_map_size_af_calibrate = macro_cache_mgr->meta_map_.size();
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
    // num / 2 * 2 -> Because two types of macro caches were written: MACRO_BLOCK, TMP_FILE
    LOG_INFO("calibrate_test_info_log", K(meta_map_size - meta_map_size_af_calibrate), K(num / 2 * 2), K(test_type));
    ASSERT_TRUE((meta_map_size - meta_map_size_af_calibrate) == (num / 2 * 2));
    // check file exist
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      for (int64_t i = 0; i < num; i++) {
        if (i < num / 2) {
          is_exist = true;
          check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
        } else {
          is_exist = false;
          check_exist(cache_type, macro_cache_mgr, i, test_type, is_exist);
        }
      }
    }
    // delete file
    for (ObSSMacroCacheType cache_type = static_cast<ObSSMacroCacheType>(0); cache_type < ObSSMacroCacheType::MAX_TYPE;
        cache_type = static_cast<ObSSMacroCacheType>(static_cast<uint8_t>(cache_type) + 1)) {
      if (cache_type == ObSSMacroCacheType::META_FILE || cache_type == ObSSMacroCacheType::HOT_TABLET_MACRO_BLOCK) {
        continue; // skip META_FILE and HOT_TABLET_MACRO_BLOCK
      }
      delete_all_wr_file(0, num, tenant_file_mgr, cache_type);
    }
    info_log(ObSSMacroCacheType::MAX_TYPE, -1, disk_space_mgr, macro_cache_mgr, test_type);
  }

public:
  static const int64_t WRITE_IO_SIZE = 2 * 1024L * 1024L; // 2MB
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
};

void ObMacroCacheCalibrateTest::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  ASSERT_EQ(OB_SUCCESS, TestSSMacroCacheMgrUtil::wait_macro_cache_ckpt_replay());
}

void ObMacroCacheCalibrateTest::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void ObMacroCacheCalibrateTest::SetUp()
{
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
  write_info_.mtl_tenant_id_ = MTL_ID();
}

enum TestType {
  TEST_MACRO_BLOCK = 0,
  TEST_TMP_FILE = 1,
  TEST_CALIBRATE_MAP_LESS = 2,
  TEST_CALIBRATE_MAP_MORE = 3,
  TEST_UNEXPECTED_PATH = 4,
  TEST_MAX_NUM = 5
};

TEST_F(ObMacroCacheCalibrateTest, test_calibrate)
{
  ObSSMacroCacheMgr *macro_cache_mgr = MTL(ObSSMacroCacheMgr *);
  ASSERT_NE(nullptr, macro_cache_mgr);
  ObTenantDiskSpaceManager *disk_space_mgr = MTL(ObTenantDiskSpaceManager *);
  ASSERT_NE(nullptr, disk_space_mgr);
  ObStorageCachePolicyService *policy_service = MTL(ObStorageCachePolicyService *);
  ObTenantFileManager *tenant_file_mgr = MTL(ObTenantFileManager*);
  ASSERT_NE(nullptr, tenant_file_mgr);

  sleep(10);  // wait for background calibrate task to complete
  for (int64_t test_type = TEST_MACRO_BLOCK; test_type < TEST_MAX_NUM; test_type++) {
    switch (static_cast<TestType>(test_type)) {
      case TEST_MACRO_BLOCK:                      /* write to macro cache -- MACRO_BLOCK */
        test_macro_cache_calibrate(macro_cache_mgr, disk_space_mgr, tenant_file_mgr,
                              ObSSMacroCacheType::MACRO_BLOCK, test_type);
        break;
      case TEST_TMP_FILE:                         /* write to macro cache -- TMP_FILE */
        test_macro_cache_calibrate(macro_cache_mgr, disk_space_mgr, tenant_file_mgr,
                              ObSSMacroCacheType::TMP_FILE, test_type);
        break;
      case TEST_CALIBRATE_MAP_LESS:                           /* meta_map records less than the actual disk */
        test_calibrate_map_less(macro_cache_mgr, disk_space_mgr, tenant_file_mgr, test_type);
        break;
      case TEST_CALIBRATE_MAP_MORE:                           /*  meta_map records more than the actual disk */
        test_calibrate_map_more(macro_cache_mgr, disk_space_mgr, tenant_file_mgr, test_type);
        break;
      case TEST_UNEXPECTED_PATH:                              /* test unexpected path */
        test_unexpected_path(macro_cache_mgr, disk_space_mgr, tenant_file_mgr, test_type);
        break;
      default:
        LOG_INFO("Invalid test type");
        break;
    }
  }
}

} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_macro_cache_calibrate.log*");
  OB_LOGGER.set_file_name("test_ss_macro_cache_calibrate.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
