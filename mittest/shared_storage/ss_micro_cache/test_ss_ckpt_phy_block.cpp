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
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_struct.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_reader.h"
#include "storage/shared_storage/micro_cache/ckpt/ob_ss_ckpt_phy_block_writer.h"
#include "storage/shared_storage/micro_cache/ob_ss_micro_cache_stat.h"

namespace oceanbase
{
namespace storage
{
using namespace oceanbase::storage;

class TestSSCkptPhyBlock : public ::testing::Test
{
public:
  TestSSCkptPhyBlock(){}
  virtual ~TestSSCkptPhyBlock() {}
  virtual void SetUp() override {}
  virtual void TearDown() override {}
  static void SetUpTestCase()
  {
    GCTX.startup_mode_ = observer::ObServerMode::SHARED_STORAGE_MODE;
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
    tnt_file_mgr_ = MTL(ObTenantFileManager *);
    ASSERT_NE(nullptr, tnt_file_mgr_);
    const uint64_t tenant_id = MTL_ID();
    const int64_t max_mem_size = 10L * 1024L * 1024L * 1024L;
    ASSERT_EQ(OB_SUCCESS, allocator_.init(BLOCK_SIZE, ObMemAttr(tenant_id, "SSMicroCache"), max_mem_size));
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
  static ObConcurrentFIFOAllocator allocator_;
  static const int64_t BLOCK_SIZE;
  static const int64_t MAX_SEGMENT_SIZE;
};
ObSSMicroCacheStat TestSSCkptPhyBlock::cache_stat_;
ObConcurrentFIFOAllocator TestSSCkptPhyBlock::allocator_;
ObSSPhysicalBlockManager TestSSCkptPhyBlock::phy_blk_mgr_(cache_stat_, allocator_);
ObTenantFileManager *TestSSCkptPhyBlock::tnt_file_mgr_ = nullptr;
const int64_t TestSSCkptPhyBlock::BLOCK_SIZE = 2 * 1024 * 1024;
const int64_t TestSSCkptPhyBlock::MAX_SEGMENT_SIZE = 32 * 1024;

struct MockSegmentHeader
{
public:
  MockSegmentHeader() { reset(); }
  ~MockSegmentHeader() { reset(); }
  void reset();
  bool is_valid() const { return payload_size_ > 0; }
  int serialize(char *buf, const int64_t buf_len, int64_t& pos) const;
  int deserialize(const char *buf, const int64_t data_len, int64_t& pos);
  int64_t get_serialize_size() const;
  TO_STRING_KV(K_(header_size), K_(version), K_(payload_size), K_(payload_zsize), K_(payload_checksum),
    K_(tailer_offset), K_(tailer_length), K_(compressor_type), K_(new_add));

  int32_t header_size_;
  int32_t version_;
  int32_t payload_size_;
  int32_t payload_zsize_;
  uint32_t payload_checksum_;
  int32_t tailer_offset_;
  int32_t tailer_length_;
  union {
    uint32_t attr_;
    struct {
      uint32_t compressor_type_ : 8;
      uint32_t reserved_ : 24;
    };
  };
  uint32_t new_add_;
};

void MockSegmentHeader::reset()
{
  header_size_ = get_serialize_size();
  version_ = 2;
  payload_size_ = 0;
  payload_zsize_ = 0;
  payload_checksum_ = 0;
  compressor_type_ = static_cast<uint32_t>(ObCompressorType::INVALID_COMPRESSOR);
  tailer_offset_ = -1;
  tailer_length_ = 0;
  reserved_ = 0;
  new_add_ = 0;
}

int MockSegmentHeader::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(buf_len < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(buf_len));
  } else if (OB_UNLIKELY(pos + get_serialize_size() > buf_len)) {
    ret = OB_BUF_NOT_ENOUGH;
    LOG_WARN("data buffer is not enough", KR(ret), K(pos), K(buf_len), K(*this));
  } else if (OB_UNLIKELY(!is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("mock segment header is invalid", KR(ret), K(*this));
  } else {
    MockSegmentHeader *header = reinterpret_cast<MockSegmentHeader *>(buf + pos);
    header->header_size_ = get_serialize_size();
    header->version_ = version_;
    header->payload_size_ = payload_size_;
    header->payload_zsize_ = payload_zsize_;
    header->payload_checksum_ = payload_checksum_;
    header->tailer_offset_ = tailer_offset_;
    header->tailer_length_ = tailer_length_;
    header->attr_ = attr_;
    header->new_add_ = new_add_;
    pos += header->header_size_;
  }
  return ret;
}

int MockSegmentHeader::deserialize(const char *buf, const int64_t data_len, int64_t& pos)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(buf) || OB_UNLIKELY(data_len <= 0 || pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", KR(ret), KP(buf), K(data_len), K(pos));
  } else {
    const MockSegmentHeader *header_ptr = reinterpret_cast<const MockSegmentHeader *>(buf + pos);
    header_size_ = header_ptr->header_size_;
    version_ = header_ptr->version_;
    payload_size_ = header_ptr->payload_size_;
    payload_zsize_ = header_ptr->payload_zsize_;
    payload_checksum_ = header_ptr->payload_checksum_;
    tailer_offset_ = header_ptr->tailer_offset_;
    tailer_length_ = header_ptr->tailer_length_;
    attr_ = header_ptr->attr_;
    if (2 == version_) {
      new_add_ = header_ptr->new_add_;
    }

    if (OB_UNLIKELY(!is_valid())) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("deserialized mock segment header is invalid", KR(ret), K(*this));
    } else {
      pos += header_size_;
    }
  }
  return ret;
}

int64_t MockSegmentHeader::get_serialize_size() const
{
  return sizeof(MockSegmentHeader);
}

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

int write_items(ObSSCkptPhyBlockItemWriter &writer, const std::vector<ItemInfo> &item_arr)
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

int read_items(ObSSCkptPhyBlockItemReader &reader, const std::vector<ItemInfo> &item_arr)
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
    ObArray<int64_t> blk_id_list;
    ObArray<int64_t> r_blk_id_list;
    const int64_t item_arr_size = item_buf_len_arr.size();
    std::vector<ItemInfo> item_arr(item_arr_size);
    const uint64_t tenant_id = MTL_ID();
    ObSSCkptPhyBlockItemWriter writer;
    ObSSCkptPhyBlockItemReader reader;
    int64_t entry_blk_id = SS_INVALID_PHY_BLK_ID;

    if (OB_FAIL(build_items(item_arr, item_buf_len_arr))) {
      LOG_WARN("fail to build_items", K(ret), K(item_arr_size));
    } else if (OB_FAIL(writer.init(tenant_id, phy_blk_mgr, ObSSPhyBlockType::SS_MICRO_META_BLK, compressor_type))) {
      LOG_WARN("fail to init writer", K(ret));
    } else if (OB_FAIL(write_items(writer, item_arr))) {
      LOG_WARN("fail to write_items", K(ret));
    } else if (OB_FAIL(writer.get_entry_block(entry_blk_id))) {
      LOG_WARN("fail to get_entry_block", K(ret));
    } else if (OB_FAIL(reader.init(entry_blk_id, tenant_id, phy_blk_mgr))) {
      LOG_WARN("fail to init reader", K(ret), K(entry_blk_id), K(tenant_id));
    } else if (OB_FAIL(read_items(reader, item_arr))) {
      LOG_WARN("fail to read_items", K(ret), K(entry_blk_id));
    } else if (OB_FAIL(writer.get_block_id_list(blk_id_list))) {
      LOG_WARN("fail to get_block_id_list", K(ret));
    } else if (OB_FAIL(reader.get_block_id_list(r_blk_id_list))) {
      LOG_WARN("fail to get_block_id_list", K(ret));
    } else if (OB_UNLIKELY(blk_id_list.count() != r_blk_id_list.count())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("blk_id count should be the same", K(ret), K(blk_id_list), K(r_blk_id_list));
    } else {
      const int64_t blk_id_cnt = blk_id_list.count();
      for (int64_t i = 0; OB_SUCC(ret) && (i < blk_id_cnt); i++) {
        if (OB_UNLIKELY(blk_id_list[i] != entry_blk_id - i || blk_id_list[i] != r_blk_id_list[i])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("block id unexpected", K(ret), K(i), K(entry_blk_id), K(blk_id_list[i]), K(r_blk_id_list[i]));
        }
      }
      LOG_INFO("finish item rw check", K(item_arr_size), K(blk_id_list.count()), K(blk_id_list));
    }
  }
  return ret;
}

TEST_F(TestSSCkptPhyBlock, test_phy_ckpt_blk_scan)
{
  LOG_INFO("TEST_CASE: start test test_phy_ckpt_blk_scan");
  ObSSMicroCache *micro_cache = MTL(ObSSMicroCache *);
  const uint64_t tenant_id = MTL_ID();
  ObSSDoBlkCheckpointOp &blk_ckpt_op = micro_cache->task_runner_.blk_ckpt_task_.ckpt_op_;
  ObSSPhysicalBlockManager *phy_blk_mgr = blk_ckpt_op.phy_blk_mgr();
  ObSSMicroCacheSuperBlk &super_blk = phy_blk_mgr->super_blk_;
  ObSSBlkCkptInfo &blk_ckpt_info = super_blk.blk_ckpt_info_;
  const int64_t total_blk_cnt = phy_blk_mgr->blk_cnt_info_.total_blk_cnt_;
  ASSERT_GT(total_blk_cnt, 2);

  ObIArray<int64_t> &prev_ckpt_used_blk_list = super_blk.blk_ckpt_used_blk_list();
  prev_ckpt_used_blk_list.reset();
  // Mark the last two phy_blocks as those used in the previous checkpoint process for writing blk checkpoint data.
  prev_ckpt_used_blk_list.push_back(total_blk_cnt - 1); // the last phy_blk;
  prev_ckpt_used_blk_list.push_back(total_blk_cnt - 2); // the second last phy_blk;
  blk_ckpt_info.blk_ckpt_entry_ = total_blk_cnt - 1;

  ObSEArray<ObSSPhyBlockReuseInfo, 256> all_blk_info_arr;
  ASSERT_EQ(OB_SUCCESS, phy_blk_mgr->scan_blocks_to_ckpt(all_blk_info_arr));
  ASSERT_EQ(all_blk_info_arr.count(), total_blk_cnt - SS_SUPER_BLK_COUNT);
  phy_blk_mgr->reusable_blks_.reuse(); // clear reusable_blks
  ASSERT_EQ(OB_SUCCESS, blk_ckpt_op.mark_reusable_blocks(all_blk_info_arr));

  ObSSPhyBlockReuseInfo reuse_info_last_blk = all_blk_info_arr.at(all_blk_info_arr.count() - 1);
  ASSERT_EQ(true, reuse_info_last_blk.need_reuse_);
  ObSSPhyBlockReuseInfo reuse_info_second_last_blk = all_blk_info_arr.at(all_blk_info_arr.count() - 2);
  ASSERT_EQ(true, reuse_info_second_last_blk.need_reuse_);

  LOG_INFO("TEST_CASE: finish test test_phy_ckpt_blk_scan");
}

TEST_F(TestSSCkptPhyBlock, test_basic_rw)
{
  LOG_INFO("TEST: start test_basic_rw");
  ObArenaAllocator allocator;
  char * full_buf = static_cast<char *>(allocator.alloc(BLOCK_SIZE));
  ASSERT_NE(nullptr, full_buf);
  MEMSET(full_buf, 'a', BLOCK_SIZE);
  ObSSPhyBlockCommonHeader common_header;
  const int64_t blk_payload_max_size = BLOCK_SIZE - get_blk_payload_offset();
  ObSSCkptPhyBlockSegmentHeader seg_header;
  const int64_t seg_payload_max_size = blk_payload_max_size - seg_header.get_serialize_size();
  int64_t max_item_size = seg_payload_max_size;
  bool find_ret = false;
  do {
    ObSSCkptPhyBlockItemHeader item_header;
    item_header.payload_size_ = max_item_size;
    if (max_item_size + item_header.get_serialize_size() <= seg_payload_max_size) {
      find_ret = true;
    } else {
      --max_item_size;
    }
  } while (!find_ret);
  LOG_INFO("check: ", K(blk_payload_max_size), K(max_item_size));

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
    const int64_t total_item_size = sizeof(ObSSCkptPhyBlockItemHeader) + SS_SERIALIZE_EXTRA_BUF_LEN + buf_len;
    const int64_t item_num = seg_payload_max_size / total_item_size;
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
    LOG_INFO("=========================================One Small Item COMP=========================================");
    std::vector<int64_t> item_buf_len_arr = {35};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::SNAPPY_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================Partial Block COMP=========================================");
    std::vector<int64_t> item_buf_len_arr = {1, 2, 4, 8, 16, 8, 4};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::LZ4_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================One Item Fill Single Block COMP=========================================");
    std::vector<int64_t> item_buf_len_arr = {max_item_size};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::ZLIB_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================One Item Less Than Block COMP=========================================");
    std::vector<int64_t> item_buf_len_arr = {max_item_size - 1};
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::ZSTD_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================Exact Block COMP=========================================");
    const int64_t buf_len = 4377;
    const int64_t total_item_size = sizeof(ObSSCkptPhyBlockItemHeader) + SS_SERIALIZE_EXTRA_BUF_LEN + buf_len;
    const int64_t item_num = seg_payload_max_size / total_item_size;
    std::vector<int64_t> item_buf_len_arr(item_num, buf_len);
    ASSERT_EQ(OB_SUCCESS, check_rw(phy_blk_mgr_, item_buf_len_arr, ObCompressorType::SNAPPY_COMPRESSOR));
  }
  {
    LOG_INFO("=========================================Multiple Blocks COMP=========================================");
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
  LOG_INFO("TEST: finish test_basic_rw");
}

TEST_F(TestSSCkptPhyBlock, test_ckpt_writer)
{
  LOG_INFO("TEST: start test_ckpt_writer");
  const uint64_t tenant_id = MTL_ID();
  ObSSPhyBlockType blk_type = ObSSPhyBlockType::SS_MICRO_META_BLK;
  ASSERT_EQ(true, phy_blk_mgr_.exist_free_block(blk_type));
  // 1. check 'close()': need write_block() and write_segment()
  ObCompressorType compress_type = ObCompressorType::NONE_COMPRESSOR;
  ObSSCkptPhyBlockItemWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(tenant_id, phy_blk_mgr_, blk_type, compress_type));
  ItemInfo item_info;
  const int64_t item_size = 2 * 1024;
  ASSERT_EQ(OB_SUCCESS, item_info.init(2 * 1024));
  const int64_t seg_item_cnt = MAX_SEGMENT_SIZE / item_size;
  for (int64_t i = 0; i < seg_item_cnt; ++i) {
    ASSERT_EQ(OB_SUCCESS, writer.write_item(item_info.buf_, item_info.buf_len_));
  }
  ObSSCkptPhyBlockSegmentHeader seg_header;
  ASSERT_EQ(OB_SUCCESS, writer.do_build_seg_header_(seg_header));
  const int64_t total_seg_len = seg_header.get_serialize_size() + MAX_SEGMENT_SIZE;
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

TEST_F(TestSSCkptPhyBlock, test_ckpt_segment)
{
  LOG_INFO("TEST: start test_ckpt_segment");
  const uint64_t tenant_id = MTL_ID();
  ObSSPhyBlockType blk_type = ObSSPhyBlockType::SS_MICRO_META_BLK;
  ASSERT_EQ(true, phy_blk_mgr_.exist_free_block(blk_type));
  ObCompressorType compress_type = ObCompressorType::NONE_COMPRESSOR;
  ObSSCkptPhyBlockItemWriter writer;
  ASSERT_EQ(OB_SUCCESS, writer.init(tenant_id, phy_blk_mgr_, blk_type, compress_type));

  const int64_t seg_cnt = 10;
  for (int64_t i = 1; i <= seg_cnt; ++i) {
    const int64_t seg_item_cnt = i + 100;
    for (int64_t j = 0; j < seg_item_cnt; ++j) {
      ItemInfo item_info;
      const int64_t item_size = ObRandom::rand(50, 100);
      ASSERT_EQ(OB_SUCCESS, item_info.init(item_size));
      ASSERT_EQ(OB_SUCCESS, writer.write_item(item_info.buf_, item_info.buf_len_));
    }
    bool is_persisted = false;
    int64_t seg_offset = -1;
    int64_t seg_len = 0;
    ASSERT_EQ(OB_SUCCESS, writer.close_segment(is_persisted, seg_offset, seg_len));
    ASSERT_NE(-1, seg_offset);
    ASSERT_NE(0, seg_len);
    ASSERT_EQ(i, writer.segs_cnt_);
  }
  ASSERT_EQ(OB_SUCCESS, writer.close());
}

TEST_F(TestSSCkptPhyBlock, test_segment_header_compat)
{
  LOG_INFO("TEST: start test_segment_header_compat");
  ObSSCkptPhyBlockSegmentHeader seg_header;
  seg_header.payload_size_ = 318;
  seg_header.payload_zsize_ = 214;
  seg_header.payload_checksum_ = 50001;
  seg_header.tailer_offset_ = 15;
  seg_header.tailer_length_ = 1;
  seg_header.compressor_type_ = 3;

  const int64_t buf_size = 512;
  char seg_header_buf[buf_size];
  MEMSET(seg_header_buf, '\0', buf_size);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, seg_header.serialize(seg_header_buf, buf_size, pos));
  ASSERT_EQ(pos, seg_header.get_serialize_size());

  MockSegmentHeader mock_seg_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, mock_seg_header.deserialize(seg_header_buf, buf_size, pos));
  ASSERT_EQ(seg_header.payload_size_, mock_seg_header.payload_size_);
  ASSERT_EQ(seg_header.payload_zsize_, mock_seg_header.payload_zsize_);
  ASSERT_EQ(seg_header.payload_checksum_, mock_seg_header.payload_checksum_);
  ASSERT_EQ(seg_header.tailer_offset_, mock_seg_header.tailer_offset_);
  ASSERT_EQ(seg_header.tailer_length_, mock_seg_header.tailer_length_);
  ASSERT_EQ(seg_header.compressor_type_, mock_seg_header.compressor_type_);
  ASSERT_EQ(0, mock_seg_header.new_add_);
  ASSERT_EQ(pos, seg_header.get_serialize_size());

  MEMSET(seg_header_buf, '\0', buf_size);
  pos = 0;
  mock_seg_header.version_ = 2;
  mock_seg_header.new_add_ = 90003;
  ASSERT_EQ(OB_SUCCESS, mock_seg_header.serialize(seg_header_buf, buf_size, pos));
  ASSERT_EQ(pos, mock_seg_header.get_serialize_size());
  MockSegmentHeader tmp_mock_seg_header;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, tmp_mock_seg_header.deserialize(seg_header_buf, buf_size, pos));
  ASSERT_EQ(pos, mock_seg_header.get_serialize_size());
  ASSERT_EQ(tmp_mock_seg_header.payload_size_, mock_seg_header.payload_size_);
  ASSERT_EQ(tmp_mock_seg_header.payload_zsize_, mock_seg_header.payload_zsize_);
  ASSERT_EQ(tmp_mock_seg_header.payload_checksum_, mock_seg_header.payload_checksum_);
  ASSERT_EQ(tmp_mock_seg_header.tailer_offset_, mock_seg_header.tailer_offset_);
  ASSERT_EQ(tmp_mock_seg_header.tailer_length_, mock_seg_header.tailer_length_);
  ASSERT_EQ(tmp_mock_seg_header.compressor_type_, mock_seg_header.compressor_type_);
  ASSERT_EQ(tmp_mock_seg_header.new_add_, mock_seg_header.new_add_);
}

}  // namespace storage
}  // namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_ss_ckpt_phy_block.log*");
  OB_LOGGER.set_file_name("test_ss_ckpt_phy_block.log", true, true);
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}