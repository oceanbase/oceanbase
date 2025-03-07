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

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#define private public
#define protected public

#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_linked_phy_block_writer.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::storage;

class TestSSLinkedPhyBlock : public ::testing::Test
{
public:
  TestSSLinkedPhyBlock(){}
  virtual ~TestSSLinkedPhyBlock() {}
  virtual void SetUp() override {}
  virtual void TearDown() override {}
  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    tnt_file_mgr_ = MTL(ObTenantFileManager *);
    ASSERT_NE(nullptr, tnt_file_mgr_);
    ASSERT_EQ(OB_SUCCESS, phy_blk_mgr_.init(MTL_ID(), BLOCK_SIZE * 4096, BLOCK_SIZE));
  }
  static void TearDownTestCase()
  {
    tnt_file_mgr_ = nullptr;
    phy_blk_mgr_.destroy();
    MockTenantModuleEnv::get_instance().destroy();
  }

private:
  static ObSSMicroCacheStat cache_stat_;
  static ObSSPhysicalBlockManager phy_blk_mgr_;
  static ObTenantFileManager *tnt_file_mgr_;
  static const int64_t BLOCK_SIZE;
};
ObSSMicroCacheStat TestSSLinkedPhyBlock::cache_stat_;
ObSSPhysicalBlockManager TestSSLinkedPhyBlock::phy_blk_mgr_(cache_stat_);
ObTenantFileManager *TestSSLinkedPhyBlock::tnt_file_mgr_ = nullptr;
const int64_t TestSSLinkedPhyBlock::BLOCK_SIZE = 2 * 1024 * 1024;

struct ItemInfo
{
  ItemInfo() : allocator_(), buf_(nullptr), buf_len_(0), crc_(0) {}
  ~ItemInfo()
  {
    allocator_.reset();
    buf_ = nullptr;
    buf_len_ = 0;
    crc_ = 0;
  }
  int init(const int64_t buf_len);

  ObArenaAllocator allocator_;
  char *buf_;
  int64_t buf_len_;
  uint32_t crc_;

  TO_STRING_KV(KP_(buf), K_(buf_len), K_(crc));
};

int ItemInfo::init(const int64_t buf_len)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(buf_len <= 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(buf_len));
  } else if (OB_ISNULL(buf_ = static_cast<char *>(allocator_.alloc(buf_len)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to buf", K(ret), K(buf_len));
  } else {
    const int64_t rv = ObRandom::rand(1, 10);
    if (rv % 2 == 0) {
      for (int64_t i = 0; OB_SUCC(ret) && i < buf_len; i++) {
        buf_[i] = static_cast<char>(ObRandom::rand(-128, 127));
      }
    } else {
      memset(buf_, 'a', buf_len);
    }

    if (OB_SUCC(ret)) {
      buf_len_ = buf_len;
      crc_ = static_cast<uint32_t>(ob_crc64(buf_, buf_len));
    }
  }
  return ret;
}

int build_items(std::vector<ItemInfo> &item_arr, const std::vector<int64_t> &item_buf_len_arr)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(item_arr.size() != item_buf_len_arr.size())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(item_arr.size()), K(item_buf_len_arr.size()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < item_arr.size(); i++) {
    if (OB_FAIL(item_arr[i].init(item_buf_len_arr[i]))) {
      LOG_WARN("fail to init item", K(ret), K(i), K(item_arr.size()), K(item_buf_len_arr[i]));
    }
  }
  return ret;
}

int write_items(ObSSLinkedPhyBlockItemWriter &writer, const std::vector<ItemInfo> &item_arr)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < item_arr.size(); i++) {
    if (OB_FAIL(writer.write_item(item_arr[i].buf_, item_arr[i].buf_len_))) {
      LOG_WARN("fail to write item", K(ret), K(i), K(item_arr[i]));
    }
  }
  if (FAILEDx(writer.close())) {
    LOG_WARN("fail to close writer", K(ret));
  }
  return ret;
}

int read_items(ObSSLinkedPhyBlockItemReader &reader, const std::vector<ItemInfo> &item_arr)
{
  int ret = OB_SUCCESS;
  char *tmp_buf = nullptr;
  int64_t tmp_buf_len = 0;
  int64_t item_idx = -1;
  uint32_t clac_crc = 0;

  std::map<uint32_t, int64_t> block_items_map;
  for (int64_t i = 0; OB_SUCC(ret) && i < item_arr.size(); i++) {
    block_items_map[item_arr[i].crc_] = i;
  }

  for (int64_t i = 0; OB_SUCC(ret) && i < item_arr.size(); i++) {
    tmp_buf = nullptr;
    tmp_buf_len = 0;
    item_idx = -1;
    clac_crc = 0;

    if (OB_FAIL(reader.get_next_item(tmp_buf, tmp_buf_len))) {
      LOG_WARN("fail to read item", K(ret), K(i), K(item_arr[i]));
    } else if (OB_ISNULL(tmp_buf) || OB_UNLIKELY(tmp_buf_len <= 0)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item buf is invalid", K(ret), K(i), K(tmp_buf_len));
    } else if (FALSE_IT(clac_crc = static_cast<uint32_t>(ob_crc64(tmp_buf, tmp_buf_len)))) {
    } else if (OB_UNLIKELY(block_items_map.find(clac_crc) == block_items_map.end())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item buf is invalid", K(ret), K(i), KP(tmp_buf), K(tmp_buf_len), K(clac_crc));
    } else if (FALSE_IT(item_idx = block_items_map[clac_crc])) {
    } else if (OB_UNLIKELY(item_arr[item_idx].buf_len_ != tmp_buf_len)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("item buf len is unexpected",
          K(ret), K(i), K(item_arr[item_idx]), KP(tmp_buf), K(tmp_buf_len), K(clac_crc));
    }
  }

  if (OB_SUCC(ret)) {
    ret = reader.get_next_item(tmp_buf, tmp_buf_len);
    if (OB_UNLIKELY(ret != OB_ITER_END)) {
      LOG_WARN("read after last item, should return OB_ITER_END", K(ret));
    } else {
      ret = OB_SUCCESS;
    }
  }
  return ret;
}

int check_rw(ObSSPhysicalBlockManager &phy_blk_mgr, const std::vector<int64_t> &item_buf_len_arr,
             const common::ObCompressorType compressor_type = ObCompressorType::INVALID_COMPRESSOR)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(item_buf_len_arr.empty())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("item_buf_len_arr is empty", K(ret));
  } else {
    ObArray<PhyBlockId> block_id_list;
    const int64_t item_arr_size = item_buf_len_arr.size();
    std::vector<ItemInfo> item_arr(item_arr_size);
    const uint64_t tenant_id = MTL_ID();
    ObSSLinkedPhyBlockItemWriter writer;
    ObSSLinkedPhyBlockItemReader reader;
    PhyBlockId entry_block_id = EMPTY_PHY_BLOCK_ID;

    if (OB_FAIL(build_items(item_arr, item_buf_len_arr))) {
      LOG_WARN("fail to build_items", K(ret), K(item_arr_size));
    } else if (OB_FAIL(writer.init(tenant_id, phy_blk_mgr, ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK, compressor_type))) {
      LOG_WARN("fail to init writer", K(ret));
    } else if (OB_FAIL(write_items(writer, item_arr))) {
      LOG_WARN("fail to write_items", K(ret));
    } else if (OB_FAIL(writer.get_entry_block(entry_block_id))) {
      LOG_WARN("fail to get_entry_block", K(ret));
    } else if (OB_FAIL(reader.init(entry_block_id, tenant_id, phy_blk_mgr))) {
      LOG_WARN("fail to init reader", K(ret));
    } else if (OB_FAIL(read_items(reader, item_arr))) {
      LOG_WARN("fail to read_items", K(ret));
    } else if (OB_FAIL(writer.get_block_id_list(block_id_list))) {
      LOG_WARN("fail to get_block_id_list", K(ret));
    } else {
      for (int64_t i = 0; i < block_id_list.count(); i++) {
        if (OB_UNLIKELY(block_id_list[i] != entry_block_id - i)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("block id unexpected", K(ret), K(i), K(entry_block_id), K(block_id_list[i]));
        }
      }
      LOG_INFO("finish item rw check", K(item_arr_size), K(block_id_list.count()), K(block_id_list));
    }
  }
  return ret;
}

TEST_F(TestSSLinkedPhyBlock, test_basic_rw)
{
  LOG_INFO("TEST: start test_basic_rw");
  ObArenaAllocator allocator;
  char * full_buf = static_cast<char *>(allocator.alloc(BLOCK_SIZE));
  ASSERT_NE(nullptr, full_buf);
  MEMSET(full_buf, 'a', BLOCK_SIZE);
  ObSSPhyBlockCommonHeader common_header;
  ObSSLinkedPhyBlockHeader block_header;
  const int64_t block_buf_len_for_item = BLOCK_SIZE - common_header.header_size_ - block_header.get_fixed_serialize_size();
  int64_t max_item_size = SS_SEG_BUF_SIZE;
  bool find_ret = false;
  do {
    ObSSLinkedPhyBlockItemHeader item_header;
    item_header.payload_size_ = max_item_size;
    if (max_item_size + item_header.get_serialize_size() <= SS_SEG_BUF_SIZE) {
      find_ret = true;
    } else {
      --max_item_size;
    }
  } while (!find_ret);
  LOG_INFO("result: ", K(max_item_size));

  LOG_INFO("=========================================TEST ITEM NO COMPRESS=========================================");
  {
    LOG_INFO("=========================================One Small Item=========================================");
    std::vector<int64_t> item_buf_len_arr = {1};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr));
  }
  {
    LOG_INFO("=========================================Partial Block=========================================");
    std::vector<int64_t> item_buf_len_arr = {1, 2, 4, 8, 16, 8, 4};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr));
  }
  {
    LOG_INFO("=========================================One Item Fill Single Block=========================================");
    std::vector<int64_t> item_buf_len_arr = {max_item_size};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr));
  }
  {
    LOG_INFO("=========================================One Item Less Than Block=========================================");
    std::vector<int64_t> item_buf_len_arr = {max_item_size - 1};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr));
  }
  {
    LOG_INFO("=========================================Exact Block=========================================");
    const int64_t buf_len = 377;
    const int64_t total_item_size = sizeof(ObSSLinkedPhyBlockItemHeader) + 13 + buf_len;
    const int64_t seg_item_num = SS_SEG_BUF_SIZE / total_item_size;
    const int64_t total_seg_size = sizeof(ObSSLinkedPhyBlockSegmentHeader) + SS_SEG_BUF_SIZE;
    const int64_t seg_num = block_buf_len_for_item / total_seg_size;
    const int64_t item_num = seg_num * seg_item_num;
    std::vector<int64_t> item_buf_len_arr(item_num, buf_len);
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr));
  }
  {
    LOG_INFO("=========================================Multiple Blocks=========================================");
    std::vector<int64_t> item_buf_len_arr;
    int64_t reamin_size = BLOCK_SIZE * 20;
    int64_t cur_item_size = 0;
    while (reamin_size > 0) {
      cur_item_size = ObRandom::rand(1, max_item_size);
      item_buf_len_arr.push_back(cur_item_size);
      reamin_size -= cur_item_size;
    }
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr));
  }

  LOG_INFO("=========================================TEST ITEM WITH COMPRESS=========================================");
  {
    LOG_INFO("=========================================One Small Item=========================================");
    std::vector<int64_t> item_buf_len_arr = {1};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::SNAPPY_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================Partial Block=========================================");
    std::vector<int64_t> item_buf_len_arr = {1, 2, 4, 8, 16, 8, 4};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::LZ4_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================One Item Fill Single Block=========================================");
    std::vector<int64_t> item_buf_len_arr = {max_item_size};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::ZLIB_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================One Item Less Than Block=========================================");
    std::vector<int64_t> item_buf_len_arr = {max_item_size - 1};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::ZSTD_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================Exact Block=========================================");
    const int64_t buf_len = 4377;
    const int64_t total_item_size = sizeof(ObSSLinkedPhyBlockItemHeader) + 13 + buf_len;
    const int64_t seg_item_num = SS_SEG_BUF_SIZE / total_item_size;
    const int64_t total_seg_size = sizeof(ObSSLinkedPhyBlockSegmentHeader) + SS_SEG_BUF_SIZE;
    const int64_t seg_num = block_buf_len_for_item / total_seg_size;
    const int64_t item_num = seg_num * seg_item_num;
    std::vector<int64_t> item_buf_len_arr(item_num, buf_len);
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::SNAPPY_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================Multiple Blocks=========================================");
    std::vector<int64_t> item_buf_len_arr;
    int64_t reamin_size = BLOCK_SIZE * 20;
    int64_t cur_item_size = 0;
    while (reamin_size > 0) {
      cur_item_size = ObRandom::rand(1024, max_item_size);
      item_buf_len_arr.push_back(cur_item_size);
      reamin_size -= cur_item_size;
    }
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::SNAPPY_COMPRESSOR));
  }

  allocator.clear();
}

TEST_F(TestSSLinkedPhyBlock, test_ckpt_writer)
{
  LOG_INFO("TEST: start test_ckpt_writer");
  const uint64_t tenant_id = MTL_ID();
  const ObSSPhyBlockType blk_type = ObSSPhyBlockType::SS_MICRO_META_CKPT_BLK;

  // 1. check 'close()': need write_block() and write_segment()
  ObCompressorType compress_type = ObCompressorType::NONE_COMPRESSOR;
  ObSSLinkedPhyBlockItemWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(tenant_id, phy_blk_mgr_, blk_type, compress_type));
  ItemInfo item_info;
  const int64_t item_size = 2 * 1024;
  ASSERT_EQ(OB_SUCCESS, item_info.init(2 * 1024));
  const int64_t seg_item_cnt = SS_SEG_BUF_SIZE / item_size;
  for (int64_t i = 0; i < seg_item_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, writer.write_item(item_info.buf_, item_info.buf_len_));
  }
  ObSSLinkedPhyBlockSegmentHeader seg_header;
  ASSERT_EQ(OB_SUCCESS, writer.build_seg_header_(seg_header));
  const int64_t total_seg_len = seg_header.get_serialize_size() + SS_SEG_BUF_SIZE;
  while (writer.get_remaining_size() > total_seg_len) {
    for (int64_t i = 0; i < seg_item_cnt; ++i) {
      ASSERT_EQ(OB_SUCCESS, writer.write_item(item_info.buf_, item_info.buf_len_));
    }
  }
  for (int64_t i = 0; i < seg_item_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, writer.write_item(item_info.buf_, item_info.buf_len_));
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_linked_phy_block.log*");
  OB_LOGGER.set_file_name("test_ss_linked_phy_block.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}