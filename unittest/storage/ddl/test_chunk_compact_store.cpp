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

#define ASSERT_OK(x) ASSERT_EQ(OB_SUCCESS, (x))
#include <gtest/gtest.h>

#define private public
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/blocksstable/ob_row_generate.h"
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "src/sql/engine/basic/ob_chunk_datum_store.h"
#include "unittest/storage/blocksstable/ob_data_file_prepare.h"
#include "src/sql/engine/basic/chunk_store/ob_compact_store.h"
#include "src/sql/engine/basic/ob_temp_block_store.h"

#undef private

namespace oceanbase
{

using namespace common;
using namespace lib;
using namespace share;
using namespace sql;

//const int64_t COLUMN_CNT = 64;
const int64_t COLUMN_CNT = 64;
const int64_t BATCH_SIZE = 10000;
const int64_t ROUND[6] = {2,8,32,128,512, 1024};
int64_t RESULT_ADD[6] = {0,0,0,0,0,0};
int64_t RESULT_BUILD[6] = {0,0,0,0,0,0};
static ObSimpleMemLimitGetter getter;

typedef ObChunkDatumStore::StoredRow StoredRow;
//typedef ObChunkDatumStore::Block Block;
typedef ObTempBlockStore::Block Block;

class ObStoredRowGenerate {
public:
  int get_stored_row(StoredRow **&sr);
  int get_stored_row_irregular(StoredRow **&sr);

  common::ObArenaAllocator allocator_;
};

int ObStoredRowGenerate::get_stored_row(StoredRow **&sr)
{
  int ret = OB_SUCCESS;
  int64_t data_size = ((sizeof(ObDatum) + 8) * COLUMN_CNT + 8) * BATCH_SIZE;
  int32_t row_size = (sizeof(ObDatum) + 8) * COLUMN_CNT + 8;
  allocator_.reuse();
  void *buf = allocator_.alloc(data_size);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buff", K(ret));
  } else {
    MEMSET(buf, 0, data_size);
    for (int64_t i = 0; i < BATCH_SIZE; i++)
    {
      StoredRow * cur_sr = (StoredRow*) ((char*)buf + i * row_size);
      if (i == BATCH_SIZE) {
        cur_sr->row_size_ = 8 + 1042*COLUMN_CNT;
      } else {
        cur_sr->row_size_ = row_size;
      }
      cur_sr->cnt_ = COLUMN_CNT;
      for (int64_t j = 0; j < COLUMN_CNT; j++) {
        if (i != BATCH_SIZE) {
          int64_t datum_offset = sizeof(ObDatum) * j;
          int64_t data_offset = COLUMN_CNT * sizeof(ObDatum) + 8 * j + sizeof(StoredRow);
          ObDatum *datum_ptr = (ObDatum *)(cur_sr->payload_ + datum_offset);
          int64_t *data_ptr = (int64_t *)((char*)cur_sr + data_offset);
          datum_ptr->len_ = 8;
          //MEMCPY((void*)&datum_ptr->ptr_, &data_offset, 8);
          MEMCPY((void*)&datum_ptr->ptr_, &data_ptr, 8);
          *data_ptr = 1;
        } else {
          // wont't go here
          // generate var data
          int64_t datum_offset = sizeof(ObDatum) * j;
          int64_t data_offset = COLUMN_CNT * sizeof(ObDatum) + 8 * j + sizeof(StoredRow);
          ObDatum *datum_ptr = (ObDatum *)(cur_sr->payload_ + datum_offset);
          int64_t *data_ptr = (int64_t *)((char*)cur_sr + data_offset);
          datum_ptr->len_ = 1030;
          //MEMCPY((void*)&datum_ptr->ptr_, &data_offset, 8);
          MEMCPY((void*)&datum_ptr->ptr_, &data_ptr, 8);
          *data_ptr = 1;
        }
      }
    }
    sr = (StoredRow**)buf;
  }

  return ret;
}

int ObStoredRowGenerate::get_stored_row_irregular(StoredRow **&sr)
{
  int ret = OB_SUCCESS;
  int64_t data_size = ((sizeof(ObDatum) + 8) * COLUMN_CNT + 8) * BATCH_SIZE;
  int32_t row_size = (sizeof(ObDatum) + 8) * COLUMN_CNT + 8;
  allocator_.reuse();
  void *buf = allocator_.alloc(data_size);
  if (OB_ISNULL(buf)) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to alloc buff", K(ret));
  } else {
    MEMSET(buf, 0, data_size);
    for (int64_t i = 0; i < BATCH_SIZE; i++)
    {
      StoredRow * cur_sr = (StoredRow*) ((char*)buf + i * row_size);
      if (i == BATCH_SIZE) {
        cur_sr->row_size_ = 8 + 1042*COLUMN_CNT;
      } else {
        cur_sr->row_size_ = row_size;
      }
      cur_sr->cnt_ = COLUMN_CNT;
      for (int64_t j = 0; j < COLUMN_CNT; j++) {
        if (i != BATCH_SIZE) {
          int64_t datum_offset = sizeof(ObDatum) * j;
          int64_t data_offset = COLUMN_CNT * sizeof(ObDatum) + 8 * j + sizeof(StoredRow);
          ObDatum *datum_ptr = (ObDatum *)(cur_sr->payload_ + datum_offset);
          int64_t *data_ptr = (int64_t *)((char*)cur_sr + data_offset);
          datum_ptr->len_ = 8;
          //MEMCPY((void*)&datum_ptr->ptr_, &data_offset, 8);
          MEMCPY((void*)&datum_ptr->ptr_, &data_ptr, 8);
          *data_ptr = i * 1024 + j;
        } else {
          // wont't go here
          // generate var data
          int64_t datum_offset = sizeof(ObDatum) * j;
          int64_t data_offset = COLUMN_CNT * sizeof(ObDatum) + 8 * j + sizeof(StoredRow);
          ObDatum *datum_ptr = (ObDatum *)(cur_sr->payload_ + datum_offset);
          int64_t *data_ptr = (int64_t *)((char*)cur_sr + data_offset);
          datum_ptr->len_ = 1030;
          //MEMCPY((void*)&datum_ptr->ptr_, &data_offset, 8);
          MEMCPY((void*)&datum_ptr->ptr_, &data_ptr, 8);
          *data_ptr = 1;
        }
      }
    }
    sr = (StoredRow**)buf;
  }
  return ret;
}
class TestCompactChunk : public TestDataFilePrepare
{
public:
  TestCompactChunk() :
    TestDataFilePrepare(&getter, "TestTmpFile", 2 * 1024 * 1024, 2048) {};
  void SetUp();
  void TearDown();

protected:
  ObStoredRowGenerate row_generate_;
  ObArenaAllocator allocator_;
};
void TestCompactChunk::SetUp()
{
  int ret = OB_SUCCESS;
  const int64_t bucket_num = 1024;
  const int64_t max_cache_size = 1024 * 1024 * 1024;
  const int64_t block_size = common::OB_MALLOC_BIG_BLOCK_SIZE;
  TestDataFilePrepare::SetUp();
  ret = getter.add_tenant(1,
                          8L * 1024L * 1024L, 2L * 1024L * 1024L * 1024L);
  ASSERT_EQ(OB_SUCCESS, ret);
  ret = ObKVGlobalCache::get_instance().init(&getter, bucket_num, max_cache_size, block_size);
  if (OB_INIT_TWICE == ret) {
    ret = OB_SUCCESS;
  } else {
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  // set observer memory limit
  CHUNK_MGR.set_limit(8L * 1024L * 1024L * 1024L);
  ret = ObTmpFileManager::get_instance().init();
  ASSERT_EQ(OB_SUCCESS, ret);
  static ObTenantBase tenant_ctx(OB_SYS_TENANT_ID);
  ObTenantEnv::set_tenant(&tenant_ctx);
  ObTenantIOManager *io_service = nullptr;
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_new(io_service));
  EXPECT_EQ(OB_SUCCESS, ObTenantIOManager::mtl_init(io_service));
  EXPECT_EQ(OB_SUCCESS, io_service->start());
  tenant_ctx.set(io_service);
  ObTenantEnv::set_tenant(&tenant_ctx);
}

void TestCompactChunk::TearDown()
{
  ObTmpFileManager::get_instance().destroy();
  ObKVGlobalCache::get_instance().destroy();
  ObTmpFileStore::get_instance().destroy();
  allocator_.reuse();
  row_generate_.allocator_.reuse();
  TestDataFilePrepare::TearDown();
}

TEST_F(TestCompactChunk, test_read_writer_compact)
{
  int ret = OB_SUCCESS;
  ObCompactStore cs_chunk;

  cs_chunk.init(1, 1,
        ObCtxIds::DEFAULT_CTX_ID, "SORT_CACHE_CTX", true, 0, true);
  ChunkRowMeta row_meta(allocator_);
  row_meta.col_cnt_ = COLUMN_CNT;
  row_meta.fixed_cnt_ = COLUMN_CNT;
  row_meta.var_data_off_ = 8 * row_meta.fixed_cnt_;
  row_meta.column_length_.prepare_allocate(COLUMN_CNT);
  row_meta.column_offset_.prepare_allocate(COLUMN_CNT);
  for (int64_t i = 0; i < COLUMN_CNT; i++) {
    if (i != COLUMN_CNT) {
      row_meta.column_length_[i] = 8;
      row_meta.column_offset_[i] = 8 * i;
    } else {
      row_meta.column_length_[i] = 0;
      row_meta.column_offset_[i] = 0;
    }
  }
  cs_chunk.set_meta(&row_meta);


  StoredRow **sr;
  ret = row_generate_.get_stored_row(sr);
  ASSERT_EQ(ret, OB_SUCCESS);

  char *buf = reinterpret_cast<char*>(sr);
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
    StoredRow *tmp_sr = (StoredRow *)(buf + pos);
    ret = cs_chunk.add_row(*tmp_sr);
    ASSERT_EQ(ret, OB_SUCCESS);
    pos += tmp_sr->row_size_;
  }
  ret = cs_chunk.finish_add_row();
  ASSERT_EQ(ret, OB_SUCCESS);
  for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
    int64_t result = 0;
    const StoredRow *cur_sr = nullptr;
    ret = cs_chunk.get_next_row(cur_sr);
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    ASSERT_EQ(ret, OB_SUCCESS);
    for (int64_t k = 0; k < cur_sr->cnt_; k++) {
      ObDatum cur_cell = cur_sr->cells()[k];
      result += *(int64_t *)(cur_cell.ptr_);
    }
    OB_ASSERT(result == 64);
  }
}


TEST_F(TestCompactChunk, test_read_writer_compact_vardata)
{
  int ret = OB_SUCCESS;
  ObCompactStore cs_chunk;

  cs_chunk.init(1, 1,
        ObCtxIds::DEFAULT_CTX_ID, "SORT_CACHE_CTX", true, 0, true);
  ChunkRowMeta row_meta(allocator_);
  row_meta.col_cnt_ = COLUMN_CNT;
  row_meta.fixed_cnt_ = 0;
  row_meta.var_data_off_ = 0;
  row_meta.column_length_.prepare_allocate(COLUMN_CNT);
  row_meta.column_offset_.prepare_allocate(COLUMN_CNT);
  for (int64_t i = 0; i < COLUMN_CNT; i++) {
    if (i != COLUMN_CNT) {
      row_meta.column_length_[i] = 0;
      row_meta.column_offset_[i] = 0;
    } else {
      row_meta.column_length_[i] = 0;
      row_meta.column_offset_[i] = 0;
    }
  }
  cs_chunk.set_meta(&row_meta);

  StoredRow **sr;
  ret = row_generate_.get_stored_row(sr);
  ASSERT_EQ(ret, OB_SUCCESS);

  char *buf = reinterpret_cast<char*>(sr);
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
    StoredRow *tmp_sr = (StoredRow *)(buf + pos);
    ret = cs_chunk.add_row(*tmp_sr);
    ASSERT_EQ(ret, OB_SUCCESS);
    pos += tmp_sr->row_size_;
  }
  ret = cs_chunk.finish_add_row();
  ASSERT_EQ(ret, OB_SUCCESS);
  for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
    int64_t result = 0;
    const StoredRow *cur_sr = nullptr;
    ret = cs_chunk.get_next_row(cur_sr);
    if (ret == OB_ITER_END) {
      ret = OB_SUCCESS;
    }
    ASSERT_EQ(ret, OB_SUCCESS);
    for (int64_t k = 0; k < cur_sr->cnt_; k++) {
      ObDatum cur_cell = cur_sr->cells()[k];
      result += *(int64_t *)(cur_cell.ptr_);
    }
    OB_ASSERT(result == 64);
  }
}

TEST_F(TestCompactChunk, test_rescan_get_last_row_compact)
{
  int ret = OB_SUCCESS;
  ObCompactStore cs_chunk;
  cs_chunk.init(1, 1,
        ObCtxIds::DEFAULT_CTX_ID, "SORT_CACHE_CTX", true, 0, false/*disable trunc*/);
  ChunkRowMeta row_meta(allocator_);
  row_meta.col_cnt_ = COLUMN_CNT;
  row_meta.fixed_cnt_ = 0;
  row_meta.var_data_off_ = 0;
  row_meta.column_length_.prepare_allocate(COLUMN_CNT);
  row_meta.column_offset_.prepare_allocate(COLUMN_CNT);
  for (int64_t i = 0; i < COLUMN_CNT; i++) {
    if (i != COLUMN_CNT) {
      row_meta.column_length_[i] = 0;
      row_meta.column_offset_[i] = 0;
    } else {
      row_meta.column_length_[i] = 0;
      row_meta.column_offset_[i] = 0;
    }
  }
  cs_chunk.set_meta(&row_meta);
  StoredRow **sr;
  ret = row_generate_.get_stored_row_irregular(sr);
  ASSERT_EQ(ret, OB_SUCCESS);

  char *buf = reinterpret_cast<char*>(sr);
  int64_t pos = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
    StoredRow *tmp_sr = (StoredRow *)(buf + pos);
    ret = cs_chunk.add_row(*tmp_sr);
    ASSERT_EQ(ret, OB_SUCCESS);
    pos += tmp_sr->row_size_;
    // get last row
    const StoredRow *cur_sr = nullptr;
    ret = cs_chunk.get_last_stored_row(cur_sr);
    ASSERT_EQ(ret, OB_SUCCESS);
    int64_t res = 0;
    for (int64_t k = 0; k < cur_sr->cnt_; k++) {
      ObDatum cur_cell = cur_sr->cells()[k];
      res += *(int64_t *)(cur_cell.ptr_);
    }
    OB_ASSERT(res == ((1024 * i * COLUMN_CNT) + ((COLUMN_CNT - 1) * COLUMN_CNT / 2)));
  }

  ret = cs_chunk.finish_add_row();
  ASSERT_EQ(ret, OB_SUCCESS);
  for (int j = 0; OB_SUCC(ret) && j < 2; j++ ) {
    int64_t total_res = 0;
    cs_chunk.rescan();
    for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
      int64_t result = 0;
      const StoredRow *cur_sr = nullptr;
      ret = cs_chunk.get_next_row(cur_sr);
      if (ret == OB_ITER_END) {
        ret = OB_SUCCESS;
      }
      ASSERT_EQ(ret, OB_SUCCESS);
      for (int64_t k = 0; k < cur_sr->cnt_; k++) {
        ObDatum cur_cell = cur_sr->cells()[k];
        result += *(int64_t *)(cur_cell.ptr_);
        total_res += *(int64_t *)(cur_cell.ptr_);
      }
      OB_ASSERT(result == ((1024 * i * COLUMN_CNT) + ((COLUMN_CNT - 1) * COLUMN_CNT / 2)));
    }
    OB_ASSERT(total_res == ((1024 * (BATCH_SIZE-1) * BATCH_SIZE * COLUMN_CNT / 2) + BATCH_SIZE * ((COLUMN_CNT - 1) * COLUMN_CNT / 2)));
  }
}

// TEST_F(TestCompactChunk, test_rescan_add_storagedatum)
// {
//   int ret = OB_SUCCESS;
//   ObCompactStore cs_chunk;
//   cs_chunk.init(1, 1,
//         ObCtxIds::DEFAULT_CTX_ID, "SORT_CACHE_CTX", true, 0, false/*disable trunc*/, share::SORT_COMPACT_LEVEL);
//   ChunkRowMeta row_meta(allocator_);
//   row_meta.col_cnt_ = COLUMN_CNT;
//   row_meta.fixed_cnt_ = 0;
//   row_meta.var_data_off_ = 0;
//   row_meta.column_length_.prepare_allocate(COLUMN_CNT);
//   row_meta.column_offset_.prepare_allocate(COLUMN_CNT);
//   for (int64_t i = 0; i < COLUMN_CNT; i++) {
//     if (i != COLUMN_CNT) {
//       row_meta.column_length_[i] = 0;
//       row_meta.column_offset_[i] = 0;
//     } else {
//       row_meta.column_length_[i] = 0;
//       row_meta.column_offset_[i] = 0;
//     }
//   }
//   cs_chunk.set_meta(&row_meta);
//   StoredRow **sr;
//   ret = row_generate_.get_stored_row_irregular(sr);
//   ASSERT_EQ(ret, OB_SUCCESS);

//   char *buf = reinterpret_cast<char*>(sr);
//   int64_t pos = 0;
//   for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
//     StoredRow *tmp_sr = (StoredRow *)(buf + pos);
//     ObStorageDatum ssr[COLUMN_CNT];
//     for (int64_t k = 0; OB_SUCC(ret) && k < COLUMN_CNT; k++) {
//       ssr[k].shallow_copy_from_datum(tmp_sr->cells()[k]);
//     }
//     ret = cs_chunk.add_row(ssr, COLUMN_CNT, 0);
//     ASSERT_EQ(ret, OB_SUCCESS);
//     pos += tmp_sr->row_size_;
//     // get last row
//     const StoredRow *cur_sr = nullptr;
//     ret = cs_chunk.get_last_stored_row(cur_sr);
//     ASSERT_EQ(ret, OB_SUCCESS);
//     int64_t res = 0;
//     for (int64_t k = 0; k < cur_sr->cnt_; k++) {
//       ObDatum cur_cell = cur_sr->cells()[k];
//       res += *(int64_t *)(cur_cell.ptr_);
//     }
//     OB_ASSERT(res == ((1024 * i * COLUMN_CNT) + ((COLUMN_CNT - 1) * COLUMN_CNT / 2)));
//   }

//   ret = cs_chunk.finish_add_row();
//   ASSERT_EQ(ret, OB_SUCCESS);
//   for (int j = 0; OB_SUCC(ret) && j < 2; j++ ) {
//     int64_t total_res = 0;
//     cs_chunk.rescan();
//     for (int64_t i = 0; OB_SUCC(ret) && i < BATCH_SIZE; i++) {
//       int64_t result = 0;
//       const StoredRow *cur_sr = nullptr;
//       ret = cs_chunk.get_next_row(cur_sr);
//       if (ret == OB_ITER_END) {
//         ret = OB_SUCCESS;
//       }
//       ASSERT_EQ(ret, OB_SUCCESS);
//       for (int64_t k = 0; k < cur_sr->cnt_; k++) {
//         ObDatum cur_cell = cur_sr->cells()[k];
//         result += *(int64_t *)(cur_cell.ptr_);
//         total_res += *(int64_t *)(cur_cell.ptr_);
//       }
//       OB_ASSERT(result == ((1024 * i * COLUMN_CNT) + ((COLUMN_CNT - 1) * COLUMN_CNT / 2)));
//     }
//     OB_ASSERT(total_res == ((1024 * (BATCH_SIZE-1) * BATCH_SIZE * COLUMN_CNT / 2) + BATCH_SIZE * ((COLUMN_CNT - 1) * COLUMN_CNT / 2)));
//   }
// }

}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  system("rm -rf test_ddl_compact_store.log*");
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_ddl_compact_store.log", true);
  //testing::FLAGS_gtest_filter = "TestCompactChunk.test_dump_one_block";
  return RUN_ALL_TESTS();
}
