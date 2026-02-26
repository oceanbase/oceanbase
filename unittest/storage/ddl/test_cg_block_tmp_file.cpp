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
#define private public
#define protected public

#include "storage/ddl/ob_cg_block_tmp_file.h"
#include "storage/ddl/ob_cg_block_tmp_files_iterator.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "mtlenv/mock_tenant_module_env.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;

class TestCGBlockTmpFile : public ::testing::Test
{
public:
  static const int64_t TENANT_MEMORY = 16L * 1024L * 1024L * 1024L /* 16 GB */;

public:
  TestCGBlockTmpFile() = default;
  virtual ~TestCGBlockTmpFile() = default;
  virtual void SetUp();
  virtual void TearDown();
  static void SetUpTestCase();
  static void TearDownTestCase();
};

static ObSimpleMemLimitGetter getter;

void TestCGBlockTmpFile::SetUpTestCase()
{
  ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

  CHUNK_MGR.set_limit(TENANT_MEMORY);
  ObMallocAllocator::get_instance()->set_tenant_limit(MTL_ID(), TENANT_MEMORY);
}

void TestCGBlockTmpFile::SetUp()
{
  int ret = OB_SUCCESS;

  const int64_t bucket_num = 1024L;
  const int64_t max_cache_size = 1024L * 1024L * 512;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;

  ASSERT_EQ(true, MockTenantModuleEnv::get_instance().is_inited());
  if (!ObKVGlobalCache::get_instance().inited_) {
    ASSERT_EQ(OB_SUCCESS, ObKVGlobalCache::get_instance().init(&getter,
        bucket_num,
        max_cache_size,
        block_size));
  }
}

void TestCGBlockTmpFile::TearDownTestCase()
{
  MockTenantModuleEnv::get_instance().destroy();
}

void TestCGBlockTmpFile::TearDown()
{
  ObKVGlobalCache::get_instance().destroy();
}

static int64_t random(const int64_t a, const int64_t b)
{
  static common::ObRandom RND;
  return RND.get(a, b);
}

static int prepare_cg_block_content_array(
    int64_t count,
    ObIArray<ObCGBlock *> &cg_blocks,
    ObIAllocator &allocator,
    const int64_t start_content = 0,
    const bool is_random = false)
{
  int ret = OB_SUCCESS;
  cg_blocks.reset();
  for (int64_t i = start_content; OB_SUCC(ret) && i < start_content + count; ++i) {
    ObCGBlock::StoreHeader store_header;
    ObCGBlock *cg_block = nullptr;
    int64_t pos = 0;
    store_header.store_version_ = ObCGBlock::DATA_VERSION_4_4_0_0_AFTER;
    store_header.is_complete_macro_block_ = is_random ? static_cast<const bool>(random(0, 1)) : false;
    store_header.macro_buffer_size_ = is_random ? random(1024 * 1024, 4 * 1024 * 1024) : sizeof(i);

    if (OB_ISNULL(cg_block = OB_NEWx(ObCGBlock, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to build cg block", K(ret), K(i));
    } else if (OB_FAIL(cg_block->init(store_header))) {
      LOG_WARN("fail to initialize cg block", K(ret), K(i), K(store_header));
    } else if (is_random) {
      char *buf = const_cast<char *>(cg_block->get_macro_block_buffer());
      for (int64_t j = 0; OB_SUCC(ret) && j < cg_block->get_macro_buffer_size(); ++j) {
        buf[j] = static_cast<char>(random(0, 127));
      }
    } else {
      if (OB_FAIL(serialization::encode_i64(const_cast<char *>(cg_block->get_macro_block_buffer()),
                                            cg_block->get_macro_buffer_size(),
                                            pos,
                                            i))) {
        LOG_WARN("fail to encode cg block", K(ret), K(i));
      }
    }
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(cg_blocks.push_back(cg_block))) {
      LOG_WARN("fail to push back cg block", K(ret), K(i), KPC(cg_block));
    }
  }
  return ret;
}

static int put(ObCGBlockFile &cg_block_file, ObIArray<ObCGBlock *> &cg_blocks)
{
  int ret = OB_SUCCESS;
  for (int64_t i = 0; OB_SUCC(ret) && i < cg_blocks.count(); ++i) {
    ObCGBlock *cg_block = cg_blocks.at(i);
    if (OB_FAIL(cg_block_file.append_cg_block(*cg_block))) {
      LOG_WARN("fail to append cg block", K(ret), K(i), KPC(cg_block), K(cg_block_file));
    }
  }
  return ret;
}

static int put_using_writer(ObCGBlockFile &cg_block_file, ObIArray<ObCGBlock *> &cg_blocks)
{
  int ret = OB_SUCCESS;
  ObCGBlockFileWriter cg_block_writer;
  if (OB_FAIL(cg_block_writer.init(&cg_block_file))) {
    LOG_WARN("fail to initializ cg block writer", K(ret), K(cg_block_file));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < cg_blocks.count(); ++i) {
    ObCGBlock *cg_block = cg_blocks.at(i);
    if (OB_FAIL(cg_block_writer.write(cg_block->get_macro_block_buffer(),
                                      cg_block->is_complete_macro_block(),
                                      cg_block->get_macro_buffer_size()))) {
      LOG_WARN("fail to write cg block", K(ret), K(i), KPC(cg_block), K(cg_block_writer));
    }
  }
  return ret;
}

static int get(ObCGBlockFile &cg_block_file, ObIArray<ObCGBlock *> &cg_blocks, ObIAllocator &allocator)
{
  int ret = OB_SUCCESS;
  while (OB_SUCC(ret)) {
    ObCGBlock *cg_block = nullptr;
    if (OB_ISNULL(cg_block = OB_NEWx(ObCGBlock, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to build cg block", K(ret));
    } else if (OB_FAIL(cg_block_file.get_next_cg_block(*cg_block))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next cg block", K(ret));
      }
    } else if (OB_FAIL(cg_blocks.push_back(cg_block))) {
      LOG_WARN("fail to push back cg block", K(ret), KPC(cg_block));
    }
  }
  return ret;
}

static int get(ObCGBlockFilesIterator &cg_block_files_iter, ObIArray<ObCGBlock *> &cg_blocks, ObIAllocator &allocator, const uint64_t count = INT64_MAX)
{
  int ret = OB_SUCCESS;
  int64_t curr_count = 0;
  cg_blocks.reset();
  while (OB_SUCC(ret) && curr_count++ < count) {
    ObCGBlock *cg_block = nullptr;
    if (OB_ISNULL(cg_block = OB_NEWx(ObCGBlock, &allocator))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("fail to build cg block", K(ret));
    } else if (OB_FAIL(cg_block_files_iter.get_next_cg_block(*cg_block))) {
      if (OB_ITER_END == ret) {
        ret = OB_SUCCESS;
        break;
      } else {
        LOG_WARN("fail to get next cg block using iter", K(ret));
      }
    } else if (OB_FAIL(cg_blocks.push_back(cg_block))) {
      LOG_WARN("fail to push back cg block", K(ret), KPC(cg_block));
    }
  }
  return ret;
}

static int cmp(ObCGBlock *read_block, ObCGBlock *write_block)
{
  int ret = OB_SUCCESS;
  if (nullptr == read_block || nullptr == write_block) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is null", K(ret), KPC(read_block), KPC(write_block));
  } else {
    int64_t pos = 0;
    int64_t read_block_content = -1;
    int64_t write_block_content = -1;
    if (OB_FAIL(serialization::decode_i64(read_block->get_macro_block_buffer(),
                                          read_block->get_macro_buffer_size(),
                                          pos,
                                          &read_block_content))) {
      LOG_WARN("fail to deserialize read block content", K(ret));
    }
    pos = 0;
    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(serialization::decode_i64(write_block->get_macro_block_buffer(),
                                                 write_block->get_macro_buffer_size(),
                                                 pos,
                                                 &write_block_content))) {
      LOG_WARN("fail to deserialize write block content", K(ret));
    } else if (read_block_content != write_block_content ||
               read_block->get_cg_block_offset() != write_block->get_cg_block_offset() ||
               read_block->get_macro_buffer_size() != write_block->get_macro_buffer_size() ||
               read_block->get_store_version() != write_block->get_store_version() ||
               read_block->is_complete_macro_block() != write_block->is_complete_macro_block()) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("these cg block is not matching",
          K(ret), KPC(read_block), KPC(write_block), K(read_block_content), K(write_block_content));
    }
  }
  return ret;
}

static int cmp_byte_by_byte(ObCGBlock *read_block, ObCGBlock *write_block)
{
  int ret = OB_SUCCESS;
  if (nullptr == read_block || nullptr == write_block) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is null", K(ret), KPC(read_block), KPC(write_block));
  } else if (read_block->get_cg_block_offset() != write_block->get_cg_block_offset() ||
             read_block->get_macro_buffer_size() != write_block->get_macro_buffer_size() ||
             read_block->get_store_version() != write_block->get_store_version() ||
             read_block->is_complete_macro_block() != write_block->is_complete_macro_block()) {
    LOG_WARN("these cg block is not matching",
              K(ret), KPC(read_block), KPC(write_block));
  } else {
    // macro_buffer_size == write_block->get_cg_block_offset()
    const int64_t macro_buffer_size = read_block->get_macro_buffer_size();
    const char *read_buf = read_block->get_macro_block_buffer();
    const char *write_buf = write_block->get_macro_block_buffer();
    for (int64_t i = 0; OB_SUCC(ret) && i < macro_buffer_size; ++i) {
      if (*(read_buf + i) != *(write_buf + i)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg block content is not matching", K(ret), K(i), K(*(read_buf + i)), K(*(write_buf + i)));
      }
    }
  }
  return ret;
}

static int cmp(ObIArray<ObCGBlock *> &read_arr, ObIArray<ObCGBlock *> &write_arr, const bool is_random = false)
{
  int ret = OB_SUCCESS;
  if (read_arr.count() != write_arr.count()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("cg block count is not matching", K(ret), K(read_arr.count()), K(write_arr.count()));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < read_arr.count(); ++i) {
    int64_t pos = 0;
    ObCGBlock *read_block = read_arr.at(i);
    ObCGBlock *write_block = write_arr.at(i);
    if (is_random) {
      if (OB_FAIL(cmp_byte_by_byte(read_block, write_block))) {
        LOG_WARN("cg block content is not matching", K(ret), K(i), KPC(read_block), KPC(write_block));
      }
    } else {
      if (OB_FAIL(cmp(read_block, write_block))) {
        LOG_WARN("cg block content is not matching", K(ret), K(i), KPC(read_block), KPC(write_block));
      }
    }
  }
  return ret;
}

static int put_back(ObCGBlock *cg_block, ObCGBlockFilesIterator &cg_block_files_iter, ObIAllocator &allocator, ObCGBlock *&tail_cg_block)
{
  int ret = OB_SUCCESS;
  const int64_t cg_block_offset = 2;
  const int64_t micro_block_idx = 1;
  tail_cg_block = nullptr;
  if (nullptr == cg_block) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("cg block is null", K(ret));
  } else {
    cg_block->set_cg_block_offset(cg_block_offset);
    cg_block->set_micro_block_idx(micro_block_idx);
    if (OB_FAIL(cg_block_files_iter.put_cg_block_back(*cg_block))) {
      LOG_WARN("fail to put cg block back", K(ret), KPC(cg_block));
    }
  }

  if (OB_SUCC(ret)) {
    ObArray<ObCGBlock *> read_arr;
    if (OB_FAIL(get(cg_block_files_iter, read_arr, allocator, 2))) {
      LOG_WARN("fail to get cg block from iterator", K(ret));
    } else {
      ObCGBlock *cg_block_0 = read_arr.at(0);
      ObCGBlock *cg_block_1 = read_arr.at(1);
      if (OB_FAIL(cmp_byte_by_byte(cg_block_0, cg_block))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("these cg blocks is not matching", K(ret), KPC(cg_block_0), KPC(cg_block));
      } else if (cg_block_1->get_cg_block_offset() != 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("cg block offset is not matching", K(ret), K(cg_block_1->get_cg_block_offset()));
      }
      tail_cg_block = cg_block_1;
    }
  }
  return ret;
}

TEST_F(TestCGBlockTmpFile, test_write_read_cg_block)
{
  const int64_t count = 10;
  ObArray<ObCGBlock *> read_arr;
  ObArray<ObCGBlock *> write_arr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TWRCGBlock"));
  ObCGBlockFile cg_block_file;
  EXPECT_EQ(OB_SUCCESS, cg_block_file.open(ObTabletID(8), 8, 8, 8));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr, allocator));
  EXPECT_EQ(OB_SUCCESS, put(cg_block_file, write_arr));
  EXPECT_EQ(OB_SUCCESS, get(cg_block_file, read_arr, allocator));
  EXPECT_EQ(OB_SUCCESS, cmp(read_arr, write_arr));
  EXPECT_EQ(OB_SUCCESS, cg_block_file.close());
}

TEST_F(TestCGBlockTmpFile, test_cg_block_tmp_file_iterator)
{
  ObCGBlockFilesIterator cg_block_files_iter;
  const int64_t count = 5;
  ObArray<ObCGBlock *> write_arr;
  ObArray<ObCGBlock *> write_arr_0;
  ObArray<ObCGBlock *> write_arr_1;
  ObArray<ObCGBlock *> read_arr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TCGBFIter"));
  ObMemAttr attr(MTL_ID(), "cg_blok_file");
  ObCGBlockFile *cg_block_file_0 = nullptr;
  ObCGBlockFile *cg_block_file_1 = nullptr;
  ObArray<ObCGBlockFile *> cg_block_files;
  cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
  cg_block_file_1 = OB_NEW(ObCGBlockFile, attr);
  EXPECT_EQ(true, nullptr != cg_block_file_0);
  EXPECT_EQ(true, nullptr != cg_block_file_1);
  EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(9), 9, 9, 9));
  EXPECT_EQ(OB_SUCCESS, cg_block_file_1->open(ObTabletID(10), 10, 10, 10));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr_0, allocator));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr_1, allocator));
  EXPECT_EQ(OB_SUCCESS, put(*cg_block_file_0, write_arr_0));
  EXPECT_EQ(OB_SUCCESS, put_using_writer(*cg_block_file_1, write_arr_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));
  EXPECT_EQ(OB_SUCCESS, get(cg_block_files_iter, read_arr, allocator));
  EXPECT_EQ(OB_SUCCESS, append(write_arr, write_arr_0));
  EXPECT_EQ(OB_SUCCESS, append(write_arr, write_arr_1));
  EXPECT_EQ(OB_SUCCESS, cmp(read_arr, write_arr));
}

TEST_F(TestCGBlockTmpFile, test_cg_block_tmp_file_iterator_put_back)
{
  ObCGBlockFilesIterator cg_block_files_iter;
  const int64_t count = 5;
  const int64_t start = 10;
  ObArray<ObCGBlock *> write_arr_0;
  ObArray<ObCGBlock *> write_arr_1;
  ObArray<ObCGBlock *> read_arr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TCGBFIPB"));
  ObMemAttr attr(MTL_ID(), "cg_blok_file");
  ObCGBlockFile *cg_block_file_0 = nullptr;
  ObCGBlockFile *cg_block_file_1 = nullptr;
  cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
  cg_block_file_1 = OB_NEW(ObCGBlockFile, attr);
  ObArray<ObCGBlockFile *> cg_block_files;
  ObCGBlock *tail_cg_block = nullptr;
  EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(11), 11, 11, 11));
  EXPECT_EQ(OB_SUCCESS, cg_block_file_1->open(ObTabletID(12), 12, 12, 12));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr_0, allocator, start));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr_1, allocator, start + count));
  EXPECT_EQ(OB_SUCCESS, put_using_writer(*cg_block_file_0, write_arr_0));
  EXPECT_EQ(OB_SUCCESS, put(*cg_block_file_1, write_arr_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));
  EXPECT_EQ(OB_SUCCESS, get(cg_block_files_iter, read_arr, allocator, count));
  EXPECT_EQ(OB_SUCCESS, put_back(read_arr.at(count - 1), cg_block_files_iter, allocator, tail_cg_block));
  EXPECT_EQ(OB_SUCCESS, put_back(tail_cg_block, cg_block_files_iter, allocator, tail_cg_block));
  EXPECT_EQ(OB_SUCCESS, put_back(tail_cg_block, cg_block_files_iter, allocator, tail_cg_block));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.put_cg_block_back(*read_arr.at(0)));
  EXPECT_EQ(OB_ERR_UNEXPECTED, cg_block_files_iter.put_cg_block_back(*read_arr.at(0)));
}

TEST_F(TestCGBlockTmpFile, test_write_cg_block_using_writer)
{
  const int64_t count = 10;
  ObArray<ObCGBlock *> read_arr;
  ObArray<ObCGBlock *> write_arr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TWRCGBlockUW"));
  ObCGBlockFile cg_block_file;
  EXPECT_EQ(OB_SUCCESS, cg_block_file.open(ObTabletID(13), 13, 13, 13));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr, allocator));
  EXPECT_EQ(OB_SUCCESS, put_using_writer(cg_block_file, write_arr));
  EXPECT_EQ(OB_SUCCESS, get(cg_block_file, read_arr, allocator));
  EXPECT_EQ(OB_SUCCESS, cmp(read_arr, write_arr));
  EXPECT_EQ(OB_SUCCESS, cg_block_file.close());
}

TEST_F(TestCGBlockTmpFile, test_write_cg_block_using_random_content)
{
  ObCGBlockFilesIterator cg_block_files_iter;
  const int64_t count = 16;
  const int64_t start = 0;
  const bool is_random = true;
  ObArray<ObCGBlock *> write_arr_0;
  ObArray<ObCGBlock *> write_arr_1;
  ObArray<ObCGBlock *> read_arr;
  ObArenaAllocator allocator(ObMemAttr(MTL_ID(), "TWCGBlockURC"));
  ObMemAttr attr(MTL_ID(), "cg_blok_file");
  ObCGBlockFile *cg_block_file_0 = nullptr;
  ObCGBlockFile *cg_block_file_1 = nullptr;
  cg_block_file_0 = OB_NEW(ObCGBlockFile, attr);
  cg_block_file_1 = OB_NEW(ObCGBlockFile, attr);
  ObArray<ObCGBlockFile *> cg_block_files;
  ObCGBlock *tail_cg_block = nullptr;
  EXPECT_EQ(OB_SUCCESS, cg_block_file_0->open(ObTabletID(14), 14, 14, 14));
  EXPECT_EQ(OB_SUCCESS, cg_block_file_1->open(ObTabletID(15), 15, 15, 15));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr_0, allocator, start, is_random));
  EXPECT_EQ(OB_SUCCESS, prepare_cg_block_content_array(count, write_arr_1, allocator, start, is_random));
  EXPECT_EQ(OB_SUCCESS, put_using_writer(*cg_block_file_0, write_arr_0));
  EXPECT_EQ(OB_SUCCESS, put(*cg_block_file_1, write_arr_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_0));
  EXPECT_EQ(OB_SUCCESS, cg_block_files.push_back(cg_block_file_1));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.push_back_cg_block_files(cg_block_files));
  EXPECT_EQ(OB_SUCCESS, get(cg_block_files_iter, read_arr, allocator, count));
  EXPECT_EQ(OB_SUCCESS, cmp(read_arr, write_arr_0, is_random));
  EXPECT_EQ(OB_SUCCESS, put_back(read_arr.at(count - 1), cg_block_files_iter, allocator, tail_cg_block));
  EXPECT_EQ(OB_SUCCESS, put_back(tail_cg_block, cg_block_files_iter, allocator, tail_cg_block));
  EXPECT_EQ(OB_SUCCESS, put_back(tail_cg_block, cg_block_files_iter, allocator, tail_cg_block));
  EXPECT_EQ(OB_SUCCESS, cg_block_files_iter.put_cg_block_back(*read_arr.at(0)));
  EXPECT_EQ(OB_ERR_UNEXPECTED, cg_block_files_iter.put_cg_block_back(*read_arr.at(0)));
}

} // end oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_cg_block_tmp_file.log*");
  ObLogger &logger = ObLogger::get_logger();
  logger.set_file_name("test_cg_block_tmp_file.log", true);
  logger.set_log_level("info");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}