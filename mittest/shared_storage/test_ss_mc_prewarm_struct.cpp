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

#include <gmock/gmock.h>
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/shared_storage/ob_ss_reader_writer.h"
#include "storage/shared_storage/prewarm/ob_mc_prewarm_struct.h"
#include "mittest/shared_storage/clean_residual_data.h"
#include "storage/shared_storage/ob_file_helper.h"
#undef private
#undef protected

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::blocksstable;
using namespace oceanbase::common;
using namespace oceanbase::storage;
using namespace oceanbase::share;

class TestSSMCPrewarmStruct : public ::testing::Test
{
public:
  class TestHotTabletInfoWriterThread : public Threads
  {
  public:
    TestHotTabletInfoWriterThread(ObTenantBase *tenant_base,
                                  ObHotTabletInfoBaseWriter *hot_tablet_info_writer)
      : tenant_base_(tenant_base), hot_tablet_info_writer_(hot_tablet_info_writer)
    {}
    virtual void run(int64_t idx) final
    {
      ObTenantEnv::set_tenant(tenant_base_);
      ASSERT_NE(nullptr, hot_tablet_info_writer_);
      ObHotMacroInfo hot_macro_info;
      hot_macro_info.macro_id_.set_id_mode(static_cast<uint64_t>(ObMacroBlockIdMode::ID_MODE_SHARE));
      hot_macro_info.macro_id_.set_storage_object_type(static_cast<uint64_t>(ObStorageObjectType::SHARED_MAJOR_DATA_MACRO));
      hot_macro_info.macro_id_.set_second_id(hot_tablet_info_writer_->tablet_id_);
      hot_macro_info.macro_id_.set_third_id(idx);
      const int64_t hot_micro_info_cnt = ObHotTabletInfoReader::AGGREGATED_READ_THRESHOLD - 1 + idx;
      for (int64_t i = 0; i < hot_micro_info_cnt; ++i) {
        ObHotMicroInfo hot_micro_info;
        hot_micro_info.offset_ = (i + 1) * 4 * 1024;
        hot_micro_info.size_ = 4 * 1024;
        hot_micro_info.logic_micro_id_.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
        hot_micro_info.logic_micro_id_.offset_ = (i + 1) * 4 * 1024;
        hot_micro_info.logic_micro_id_.logic_macro_id_.data_seq_.macro_data_seq_ = 1000 * idx + i;
        hot_micro_info.logic_micro_id_.logic_macro_id_.logic_version_ = 100;
        hot_micro_info.logic_micro_id_.logic_macro_id_.tablet_id_ = hot_tablet_info_writer_->tablet_id_;
        hot_micro_info.micro_crc_ = i;
        ASSERT_EQ(OB_SUCCESS, hot_macro_info.hot_micro_infos_.push_back(hot_micro_info));
      }
      ObSEArray<ObHotMacroInfo, 8> hot_macro_infos;
      ASSERT_EQ(OB_SUCCESS, hot_macro_infos.push_back(hot_macro_info));
      ASSERT_EQ(OB_SUCCESS, hot_tablet_info_writer_->append(hot_macro_infos));
    }

  private:
    ObTenantBase *tenant_base_;
    ObHotTabletInfoBaseWriter *hot_tablet_info_writer_;
  };

public:
  TestSSMCPrewarmStruct() {}
  virtual ~TestSSMCPrewarmStruct() = default;
  static void SetUpTestCase();
  static void TearDownTestCase();
  virtual void SetUp();
  virtual void TearDown();
  void generate_shared_major_data_macro(const int64_t tablet_id);

public:
  const int64_t THREAD_CNT = 4;
};

void TestSSMCPrewarmStruct::SetUpTestCase()
{
  GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
  EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
}

void TestSSMCPrewarmStruct::TearDownTestCase()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ResidualDataCleanerHelper::clean_in_mock_env())) {
      LOG_WARN("failed to clean residual data", KR(ret));
  }
  MockTenantModuleEnv::get_instance().destroy();
}

void TestSSMCPrewarmStruct::SetUp()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
  ASSERT_EQ(OB_SUCCESS, micro_cache->init(MTL_ID(), (1L << 27))); // 128M
  ASSERT_EQ(OB_SUCCESS, micro_cache->start());
}

void TestSSMCPrewarmStruct::TearDown()
{
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  micro_cache->stop();
  micro_cache->wait();
  micro_cache->destroy();
}

void TestSSMCPrewarmStruct::generate_shared_major_data_macro(const int64_t tablet_id)
{
  for (int64_t i = 0; i < THREAD_CNT; ++i) {
    // construct write info
    ObStorageObjectWriteInfo write_info;
    const int64_t WRITE_IO_SIZE = 2 * 1024 * 1024L;
    char write_buf[WRITE_IO_SIZE];
    write_buf[0] = '\0';
    const int64_t mid_offset = WRITE_IO_SIZE / 2;
    memset(write_buf, 'a', mid_offset);
    memset(write_buf + mid_offset, 'b', WRITE_IO_SIZE - mid_offset);
    write_info.io_desc_.set_wait_event(1);
    write_info.buffer_ = write_buf;
    write_info.offset_ = 0;
    write_info.size_ = WRITE_IO_SIZE;
    write_info.io_timeout_ms_ = DEFAULT_IO_WAIT_TIME_MS;
    write_info.mtl_tenant_id_ = MTL_ID();

    // write
    MacroBlockId macro_id;
    macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
    macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
    macro_id.set_second_id(tablet_id); // tablet_id
    macro_id.set_third_id(i); // data_seq
    ASSERT_TRUE(macro_id.is_valid());
    ObStorageObjectHandle object_handle;
    ASSERT_EQ(OB_SUCCESS, object_handle.set_macro_block_id(macro_id));
    ObSSShareMacroWriter share_macro_writer;
    ASSERT_EQ(OB_SUCCESS, share_macro_writer.aio_write(write_info, object_handle));
    ASSERT_EQ(OB_SUCCESS, object_handle.wait());
  }
}

TEST_F(TestSSMCPrewarmStruct, serialize)
{
  ObLogicMicroBlockId logic_micro_id;
  logic_micro_id.version_ = ObLogicMicroBlockId::LOGIC_MICRO_ID_VERSION_V1;
  logic_micro_id.offset_ = 100;
  logic_micro_id.logic_macro_id_.data_seq_.macro_data_seq_ = 1;
  logic_micro_id.logic_macro_id_.logic_version_ = 100;
  logic_micro_id.logic_macro_id_.tablet_id_ = 200001;

  MacroBlockId macro_id;
  macro_id.set_id_mode((uint64_t)ObMacroBlockIdMode::ID_MODE_SHARE);
  macro_id.set_storage_object_type((uint64_t)ObStorageObjectType::SHARED_MAJOR_DATA_MACRO);
  macro_id.set_second_id(200001); // tablet_id
  macro_id.set_third_id(1); // data_seq

  // 1. ObHotMicroInfo
  ObHotMicroInfo hot_micro_info;
  hot_micro_info.offset_ = 100;
  hot_micro_info.size_ = 4096;
  hot_micro_info.logic_micro_id_ = logic_micro_id;
  hot_micro_info.micro_crc_ = 100;
  int64_t size = hot_micro_info.get_serialize_size();
  char buf1[size];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, hot_micro_info.serialize(buf1, size, pos));
  ASSERT_EQ(size, pos);

  ObHotMicroInfo tmp_hot_micro_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_hot_micro_info.deserialize(buf1, size, pos));
  ASSERT_EQ(hot_micro_info.offset_, tmp_hot_micro_info.offset_);
  ASSERT_EQ(hot_micro_info.size_, tmp_hot_micro_info.size_);
  ASSERT_EQ(hot_micro_info.logic_micro_id_, tmp_hot_micro_info.logic_micro_id_);
  ASSERT_EQ(hot_micro_info.micro_crc_, tmp_hot_micro_info.micro_crc_);


  // 2. ObHotMacroInfo
  ObHotMacroInfo hot_macro_info;
  hot_macro_info.macro_id_ = macro_id;
  ASSERT_EQ(OB_SUCCESS, hot_macro_info.hot_micro_infos_.push_back(hot_micro_info));
  size = hot_macro_info.get_serialize_size();
  char buf2[size];
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, hot_macro_info.serialize(buf2, size, pos));
  ASSERT_EQ(size, pos);

  ObHotMacroInfo tmp_hot_macro_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_hot_macro_info.deserialize(buf2, size, pos));
  ASSERT_EQ(hot_macro_info.macro_id_, tmp_hot_macro_info.macro_id_);
  ASSERT_EQ(1, tmp_hot_macro_info.hot_micro_infos_.count());
  ObHotMicroInfo &tmp_hot_micro_info_2 = tmp_hot_macro_info.hot_micro_infos_.at(0);
  ASSERT_EQ(hot_micro_info.offset_, tmp_hot_micro_info_2.offset_);
  ASSERT_EQ(hot_micro_info.size_, tmp_hot_micro_info_2.size_);
  ASSERT_EQ(hot_micro_info.logic_micro_id_, tmp_hot_micro_info_2.logic_micro_id_);
  ASSERT_EQ(hot_micro_info.micro_crc_, tmp_hot_micro_info_2.micro_crc_);


  // 3. ObHotTabletInfo
  ObHotTabletInfo hot_tablet_info;
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info.hot_macro_infos_.push_back(hot_macro_info));
  size = hot_tablet_info.get_serialize_size();
  char buf3[size];
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info.serialize(buf3, size, pos));
  ASSERT_EQ(size, pos);

  ObHotTabletInfo tmp_hot_tablet_info;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_hot_tablet_info.deserialize(buf3, size, pos));
  ASSERT_EQ(1, tmp_hot_tablet_info.hot_macro_infos_.count());
  ASSERT_EQ(1, tmp_hot_tablet_info.hot_macro_infos_.at(0).count());
  ObHotMicroInfo &tmp_hot_micro_info_3 = tmp_hot_tablet_info.hot_macro_infos_.at(0).hot_micro_infos_.at(0);
  ASSERT_EQ(hot_micro_info.offset_, tmp_hot_micro_info_3.offset_);
  ASSERT_EQ(hot_micro_info.size_, tmp_hot_micro_info_3.size_);
  ASSERT_EQ(hot_micro_info.logic_micro_id_, tmp_hot_micro_info_3.logic_micro_id_);
  ASSERT_EQ(hot_micro_info.micro_crc_, tmp_hot_micro_info_3.micro_crc_);
}

TEST_F(TestSSMCPrewarmStruct, parallel_append_hot_macro_infos)
{
  const int64_t tablet_id = 200001;
  generate_shared_major_data_macro(tablet_id);

  const int64_t compaction_scn = 1711086717622000;
  ObHotTabletInfoDataWriter hot_tablet_info_writer(tablet_id, compaction_scn);

  TestSSMCPrewarmStruct::TestHotTabletInfoWriterThread threads(ObTenantEnv::get_tenant(),
                                                               &hot_tablet_info_writer);
  threads.set_thread_count(THREAD_CNT);
  threads.start();
  threads.wait();
  threads.destroy();

  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_writer.complete());

  ObHotTabletInfoReader hot_tablet_info_reader(tablet_id, compaction_scn, 100/*prewarm_percent*/);
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_reader.load_hot_macro_infos());

  int64_t total_micro_cnt = 0;
  for (int64_t i = 0; i < THREAD_CNT; ++i) {
    total_micro_cnt += (ObHotTabletInfoReader::AGGREGATED_READ_THRESHOLD - 1 + i);
  }
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &micro_cache_stat = micro_cache->get_micro_cache_stat();
  ObSSMicroCacheHitStat &hit_stat = micro_cache_stat.hit_stat_;
  ASSERT_EQ(total_micro_cnt, hit_stat.major_compaction_prewarm_cnt_);
}

TEST_F(TestSSMCPrewarmStruct, prewarm_50_percent_data)
{
  const int64_t tablet_id = 200002;
  generate_shared_major_data_macro(tablet_id);

  const int64_t compaction_scn = 1711086717622000;
  ObHotTabletInfoDataWriter hot_tablet_info_writer(tablet_id, compaction_scn);

  TestSSMCPrewarmStruct::TestHotTabletInfoWriterThread threads(ObTenantEnv::get_tenant(),
                                                               &hot_tablet_info_writer);
  threads.set_thread_count(THREAD_CNT);
  threads.start();
  threads.wait();
  threads.destroy();

  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_writer.complete());

  ObHotTabletInfoReader hot_tablet_info_reader(tablet_id, compaction_scn, 50/*prewarm_percent*/);
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_reader.load_hot_macro_infos());

  int64_t total_micro_cnt = 0;
  for (int64_t i = 0; i < THREAD_CNT; ++i) {
    total_micro_cnt += (ObHotTabletInfoReader::AGGREGATED_READ_THRESHOLD - 1 + i);
  }
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &micro_cache_stat = micro_cache->get_micro_cache_stat();
  ObSSMicroCacheHitStat &hit_stat = micro_cache_stat.hit_stat_;
  ASSERT_EQ(total_micro_cnt / 2, hit_stat.major_compaction_prewarm_cnt_);
}

TEST_F(TestSSMCPrewarmStruct, empty_hot_macro_infos)
{
  const int64_t tablet_id = 200003;
  const int64_t compaction_scn = 1711086717622000;
  ObHotTabletInfoDataWriter hot_tablet_info_writer(tablet_id, compaction_scn);
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_writer.complete());

  ObBackupIoAdapter io_adapter;
  ObDeviceConfig device_config;
  ObStorageType type = common::ObStorageType::OB_STORAGE_MAX_TYPE;
  ObBackupStorageInfo storage_info;
  ASSERT_EQ(OB_SUCCESS, ObDeviceConfigMgr::get_instance().get_device_config(
            ObStorageUsedType::TYPE::USED_TYPE_DATA, device_config));
  ASSERT_EQ(OB_SUCCESS, get_storage_type_from_path(device_config.path_, type));
  ASSERT_EQ(OB_SUCCESS, storage_info.set(type, device_config.endpoint_, device_config.access_info_,
                                         device_config.extension_));

  // expect there is no prewarm data and index file
  ObTenantFileManager *file_manager = nullptr;
  ASSERT_NE(nullptr, file_manager = MTL(ObTenantFileManager *));
  ObStorageObjectHandle data_object_handle;
  ObStorageObjectOpt data_storage_opt;
  data_storage_opt.set_ss_major_prewarm_opt(ObStorageObjectType::MAJOR_PREWARM_DATA, tablet_id, compaction_scn);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.alloc_object(data_storage_opt, data_object_handle));
  ObPathContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(data_object_handle.get_macro_id(), 0/*ls_epoch_id*/, false/*is_local_cache*/));
  bool is_data_file_exist = false;
  ASSERT_EQ(OB_SUCCESS, io_adapter.is_exist(ctx.get_path(), &storage_info, is_data_file_exist));
  ASSERT_FALSE(is_data_file_exist);

  char index_file_path[ObBaseFileManager::OB_MAX_FILE_PATH_LENGTH];
  index_file_path[0] = '\0';
  ObStorageObjectHandle index_object_handle;
  ObStorageObjectOpt index_storage_opt;
  index_storage_opt.set_ss_major_prewarm_opt(ObStorageObjectType::MAJOR_PREWARM_DATA, tablet_id, compaction_scn);
  ASSERT_EQ(OB_SUCCESS, OB_STORAGE_OBJECT_MGR.alloc_object(index_storage_opt, index_object_handle));
  ASSERT_EQ(OB_SUCCESS, ctx.set_file_ctx(index_object_handle.get_macro_id(), 0/*ls_epoch_id*/, false/*is_local_cache*/));
  bool is_index_file_exist = false;
  ASSERT_EQ(OB_SUCCESS, io_adapter.is_exist(ctx.get_path(), &storage_info, is_index_file_exist));
  ASSERT_FALSE(is_index_file_exist);

  ObHotTabletInfoReader hot_tablet_info_reader(tablet_id, compaction_scn, 100/*prewarm_percent*/);
  ASSERT_EQ(OB_SUCCESS, hot_tablet_info_reader.load_hot_macro_infos());

  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  ASSERT_NE(nullptr, micro_cache);
  ObSSMicroCacheStat &micro_cache_stat = micro_cache->get_micro_cache_stat();
  ObSSMicroCacheHitStat &hit_stat = micro_cache_stat.hit_stat_;
  ASSERT_EQ(0, hit_stat.major_compaction_prewarm_cnt_);
}


} // namespace storage
} // namespace oceanbase

int main(int argc, char **argv)
{
  int ret = 0;
  system("rm -f ./test_ss_mc_prewarm_struct.log*");
  OB_LOGGER.set_file_name("test_ss_mc_prewarm_struct.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
