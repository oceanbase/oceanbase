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

// 测试 ObStorageVecSortImpl (MergeSortOp) 的功能

#define USING_LOG_PREFIX STORAGE



// 先包含 gtest 相关头文件
#include <gtest/gtest.h>
#include <limits>
#include <numeric>
#include <vector>

#include "lib/container/ob_fixed_array.h"
#include "share/vector/type_traits.h"
#include "share/vector/ob_continuous_base.h"
#include "storage/direct_load/ob_direct_load_vector_utils.h"
#include "test_ddl_pipeline_base.h"
#define private public
#define protected public
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#undef private
#undef protected
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/sort/ob_sort_vec_op_chunk.h"
#include "sql/engine/sort/ob_sort_basic_info.h"
#include "sql/engine/sort/ob_sort_compare_vec_op.h"
#include "storage/ddl/test_batch_rows_generater.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
using namespace oceanbase::storage;
using namespace oceanbase::share;
using namespace oceanbase::share::schema;
using namespace oceanbase::blocksstable;
using namespace oceanbase::observer;

namespace oceanbase
{
namespace unittest
{

class TestSortCompare : public CompareBase
{
public:
  explicit TestSortCompare(common::ObIAllocator &allocator) : CompareBase(allocator) {}
  void set_row_meta(const RowMeta *row_meta) {
    sk_row_meta_ = row_meta;
  }
  bool operator()(const ObSortKeyStore<false> *left, const ObSortKeyStore<false> *right) const
  {
    bool result = false;
    if (OB_NOT_NULL(left) && OB_NOT_NULL(right) && OB_NOT_NULL(sk_row_meta_)) {
      int64_t left_val = 0;
      int64_t right_val = 0;

      ObDatum left_datum = left->get_datum(*sk_row_meta_, 0);
      ObDatum right_datum = right->get_datum(*sk_row_meta_, 0);
      left_val = left_datum.get_int();
      right_val = right_datum.get_int();
      result = left_val < right_val;
    }
    return result;
  }
  // compare chunk for external merge sorter heap:
  // heap stores ChunkType* and uses this overload to order chunks by current row.
  bool operator()(const ObSortVecOpChunk<ObSortKeyStore<false>, false> *left,
                  const ObSortVecOpChunk<ObSortKeyStore<false>, false> *right) const
  {
    bool result = false;
    if (OB_NOT_NULL(left) && OB_NOT_NULL(right)) {
      // external merge sorter guarantees sk_row_ is prepared before pushing into heap
      const ObSortKeyStore<false> *l_row = left->sk_row_;
      const ObSortKeyStore<false> *r_row = right->sk_row_;
      if (OB_NOT_NULL(l_row) && OB_NOT_NULL(r_row)) {
        // Return the reverse order since the heap top is the maximum element.
        // Keep strict weak ordering: if equal, both directions are false.
        result = (*this)(r_row, l_row);
      }
    }
    return result;
  }

  bool operator()(const ObSortKeyStore<false> **left, const ObSortKeyStore<false> **right) const
  {
    bool result = (*this)(*left, *right);
    return result;
  }

  // 用于 std::upper_bound：比较 ObSortKeyStore 和 ObSEArray<ObDatum>
  bool operator()(const ObSortKeyStore<false> *left, const ObIArray<ObDatum> &right_datums) const
  {
    bool result = false;
    if (OB_NOT_NULL(left) && OB_NOT_NULL(sk_row_meta_) && right_datums.count() > 0) {
      int64_t left_val = 0;
      int64_t right_val = 0;

      ObDatum left_datum = left->get_datum(*sk_row_meta_, 0);
      const ObDatum &right_datum = right_datums.at(0);

      if (!left_datum.is_null() && !right_datum.is_null()) {
        left_val = left_datum.get_int();
        right_val = right_datum.get_int();
        result = left_val < right_val;
      } else if (left_datum.is_null() && !right_datum.is_null()) {
        result = true;  // NULL 排在前面
      }
    }
    return result;
  }

  // compare 函数返回 -1, 0, 1
  int compare(const ObSortKeyStore<false> *left, const ObIArray<ObDatum> &right_datums)
  {
    int result = 0;
    if (OB_ISNULL(left) || OB_ISNULL(sk_row_meta_) || right_datums.count() == 0) {
      return 0;
    }

    ObDatum left_datum = left->get_datum(*sk_row_meta_, 0);
    const ObDatum &right_datum = right_datums.at(0);

    if (left_datum.is_null() && right_datum.is_null()) {
      result = 0;
    } else if (left_datum.is_null()) {
      result = -1;  // NULL 排在前面
    } else if (right_datum.is_null()) {
      result = 1;
    } else {
      int64_t left_val = left_datum.get_int();
      int64_t right_val = right_datum.get_int();
      if (left_val < right_val) {
        result = -1;
      } else if (left_val > right_val) {
        result = 1;
      } else {
        result = 0;
      }
    }
    return result;
  }

  int get_error_code() const { return OB_SUCCESS; }

  void reset() {}
};

// 测试类：MergeSortOp (ObStorageVecSortImpl)
class TestMergeSortOp : public ObDDLPipelineTestBase
{
public:
  TestMergeSortOp()
    : ObDDLPipelineTestBase("test_merge_sort_op_"),
      mem_context_(ROOT_CONTEXT),
      monitor_node_(),
      sort_impl_(mem_context_, monitor_node_),
      row_meta_alloc_(),
      sk_row_meta_(),
      col_types_{ObIntType, ObVarcharType},
      rowkey_count_(2),
      col_count_(2),
      max_batch_size_(512),
      compare_(nullptr)
  {}

  virtual ~TestMergeSortOp() = default;

  virtual void SetUp() override
  {
    ObDDLPipelineTestBase::SetUp();
    allocator_.set_attr(ObMemAttr(tenant_id_, "MergeSortOpTest"));
  }

  virtual void TearDown() override
  {
    sort_impl_.reset();
    ObDDLPipelineTestBase::TearDown();
  }

  // 初始化排序实现
  int initialize_sort_impl()
  {
    int ret = OB_SUCCESS;
    common::ObArray<ColMetaInfo> expr_infos;
    if (OB_FAIL(expr_infos.reserve(2))) {
      STORAGE_LOG(WARN, "fail to reserve expr_infos", K(ret));
    } else {
      // 第一列：定长 int64
      ColMetaInfo int_info(true /*is_fixed*/, static_cast<uint32_t>(sizeof(int64_t)));
      // 第二列：变长 varchar
      ColMetaInfo varchar_info(false /*is_fixed*/, 0 /*fixed_length*/);
      if (OB_FAIL(expr_infos.push_back(int_info))) {
        STORAGE_LOG(WARN, "fail to push back int ColMetaInfo", K(ret));
      } else if (OB_FAIL(expr_infos.push_back(varchar_info))) {
        STORAGE_LOG(WARN, "fail to push back varchar ColMetaInfo", K(ret));
      } else if (OB_FAIL(sk_row_meta_.init(expr_infos, /*extra_size*/0, true, &row_meta_alloc_))) {
        STORAGE_LOG(WARN, "fail to init sk_row_meta_", K(ret));
      }
    }
    if (OB_FAIL(ret)) {
      STORAGE_LOG(WARN, "fail to init sk_row_meta_", K(ret));
    } else if (OB_ISNULL(compare_ = OB_NEWx(TestSortCompare, &row_meta_alloc_, row_meta_alloc_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      STORAGE_LOG(WARN, "fail to allocate compare", K(ret));
    }
    if (OB_SUCC(ret)) {
      lib::ObMemAttr mem_attr(tenant_id_, "MergeSortOpTest");
      if (OB_FAIL(sort_impl_.init(compare_,
                                  &sk_row_meta_,
                                  nullptr,  // addon_row_meta
                                  max_batch_size_,
                                  mem_attr,
                                  NONE_COMPRESSOR,
                                  false,    // enable_trunc
                                  4096,     // tempstore_read_alignment_size
                                  tenant_id_,
                                  false/*enable_encode_sortkey*/,
                                  nullptr))) {
        STORAGE_LOG(WARN, "fail to init sort impl", K(ret));
      } else {
        // TestSortCompare 需要 row_meta 才能正确从 ObSortKeyStore 解码 datum
        sort_impl_.comp_->set_row_meta(&sk_row_meta_);
      }
    }

    return ret;
  }

  // 生成测试数据
  int generate_test_data(ObBatchDatumRows &batch_rows, const int64_t batch_size, const bool need_sort = false)
  {
    int ret = OB_SUCCESS;
    ObBatchRowsGen batch_rows_gen;
    if (OB_FAIL(batch_rows_gen.init(col_types_, col_count_, rowkey_count_))) {
      STORAGE_LOG(WARN, "fail to init batch rows gen", K(ret));
    } else if (OB_FAIL(batch_rows_gen.get_batch_rows(batch_rows, batch_size))) {
      STORAGE_LOG(WARN, "fail to get batch rows", K(ret));
    } else if (need_sort) {
      STORAGE_LOG(INFO, "test data generated (sorted)", K(batch_size));
    } else {
      STORAGE_LOG(INFO, "test data generated (unsorted)", K(batch_size));
    }

    return ret;
  }

  int build_output_vectors(common::ObArray<ObIVector *> &vectors, const int64_t batch_size)
  {
    int ret = OB_SUCCESS;
    vectors.reset();
    for (int64_t i = 0; OB_SUCC(ret) && i < sk_row_meta_.col_cnt_; ++i) {
      const common::VectorFormat format = is_integer_column(col_types_[i]) ? common::VEC_FIXED : common::VEC_CONTINUOUS;
      const common::VecValueTypeClass value_tc = common::get_vec_value_tc(col_types_[i], 0, 0);
      common::ObIVector *vec = nullptr;
      if (OB_FAIL(storage::ObDirectLoadVectorUtils::new_vector(format, value_tc, allocator_, vec))) {
        STORAGE_LOG(WARN, "fail to new vector", K(ret));
      } else if (OB_FAIL(storage::ObDirectLoadVectorUtils::prepare_vector(vec, batch_size, allocator_))) {
        STORAGE_LOG(WARN, "fail to prepare vector", K(ret));
      } else {
        if (common::VEC_CONTINUOUS == format) {
          common::ObContinuousBase *continuous_vec = static_cast<common::ObContinuousBase *>(vec);
          const int64_t data_size = batch_size * CONTINUOUS_VECTOR_BUFFER_SIZE;
          char *data = static_cast<char *>(allocator_.alloc(data_size));
          if (OB_ISNULL(data)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "fail to alloc continuous vector payload", K(ret), K(data_size));
          } else {
            MEMSET(data, 0, data_size);
            continuous_vec->set_data(data);
          }
        }
        if (OB_FAIL(ret)) {
          // already handled logging above
        } else if (OB_FAIL(vectors.push_back(vec))) {
          STORAGE_LOG(WARN, "fail to push back vector pointer", K(ret));
        }
      }
    }
    return ret;
  }

  int verify_sorted_chunk(ObSortVecOpChunk<ObSortKeyStore<false>, false> *chunk, int64_t &row_count)
  {
    int ret = OB_SUCCESS;
    row_count = 0;
    if (OB_ISNULL(chunk)) {
      ret = OB_INVALID_ARGUMENT;
      STORAGE_LOG(WARN, "invalid argument", KP(chunk), K(ret));
    } else {
      chunk->reset_row_iter();
      if (OB_FAIL(chunk->init_row_iter())) {
        STORAGE_LOG(WARN, "fail to init chunk iterator", K(ret));
      } else {
        int64_t last_value = std::numeric_limits<int64_t>::min();
        bool first_row = true;
        while (OB_SUCC(ret)) {
          int tmp_ret = chunk->get_next_row();
          if (OB_ITER_END == tmp_ret) {
            ret = OB_SUCCESS;
            break;
          } else if (OB_FAIL(tmp_ret)) {
            ret = tmp_ret;
            STORAGE_LOG(WARN, "fail to get next row while verifying chunk", K(ret));
            break;
          }
          const ObSortKeyStore<false> *sk_row = chunk->sk_row_;
          if (OB_ISNULL(sk_row)) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "sort row is null", K(ret));
            break;
          }
          const RowMeta *row_meta = &sk_row_meta_;
          const int64_t current = sk_row->get_datum(*row_meta, 0).get_int();
          if (!first_row && current < last_value) {
            ret = OB_ERR_UNEXPECTED;
            STORAGE_LOG(WARN, "sort chunk out of order", K(current), K(last_value));
            break;
          }
          first_row = false;
          last_value = current;
          row_count++;
        }
      }
    }
    return ret;
  }

  int seal_in_memory_chunk_to_sort_chunks()
  {
    int ret = OB_SUCCESS;
    using ChunkType = ObSortVecOpChunk<ObSortKeyStore<false>, false>;
    ChunkType *sealed = sort_impl_.in_memory_chunk_;
    if (OB_ISNULL(sealed)) {
      ret = OB_ERR_UNEXPECTED;
      STORAGE_LOG(WARN, "in_memory_chunk_ is null", K(ret));
    } else {
      // 检查 chunk 是否有数据
      int64_t chunk_row_count = 0;
      if (sealed->use_inmem_data_) {
        chunk_row_count = sealed->inmem_rows_.count();
      } else {
        // 对于非 inmem 的 chunk，需要通过迭代器统计行数
        sealed->reset_row_iter();
        if (OB_FAIL(sealed->init_row_iter())) {
          STORAGE_LOG(WARN, "fail to init chunk iterator for row count", K(ret));
        } else {
          while (OB_SUCC(ret)) {
            int tmp_ret = sealed->get_next_row();
            if (OB_ITER_END == tmp_ret) {
              ret = OB_SUCCESS;
              break;
            } else if (OB_FAIL(tmp_ret)) {
              STORAGE_LOG(WARN, "fail to get next row while counting", K(ret));
              break;
            }
            chunk_row_count++;
          }
          sealed->reset_row_iter();
        }
      }
      if (OB_FAIL(ret)) {
        STORAGE_LOG(WARN, "fail to get chunk row count", K(ret));
      } else if (chunk_row_count == 0) {
        ret = OB_ERR_UNEXPECTED;
        STORAGE_LOG(WARN, "chunk is empty, cannot add to sort_chunks_", K(ret), K(chunk_row_count));
      } else {
        STORAGE_LOG(INFO, "sealing chunk with row count", K(chunk_row_count));
        if (OB_FAIL(sort_impl_.add_sort_chunk(1 /* level */, sealed))) {
          STORAGE_LOG(WARN, "fail to add sealed chunk", K(ret));
        } else {
          // allocate a fresh in-memory chunk for next add_batch()
          lib::ObMemAttr mem_attr(tenant_id_, "MergeSortOpTest");
          ChunkType *new_chunk = OB_NEWx(ChunkType, sort_impl_.get_allocator(), 0 /*level*/, *sort_impl_.get_allocator());
          if (OB_ISNULL(new_chunk)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            STORAGE_LOG(WARN, "failed to allocate new in-memory chunk", K(ret));
          } else if (OB_FAIL(new_chunk->sort_row_store_mgr_.init(
                         sk_row_meta_,
                         nullptr /*addon_row_meta*/,
                         max_batch_size_,
                         mem_attr,
                         NONE_COMPRESSOR,
                         INT64_MAX,
                         false /*enable_unique*/,
                         false /*enable_trunc*/,
                         4096 /*tempstore_read_alignment_size*/))) {
            STORAGE_LOG(WARN, "failed to init sort_row_store_mgr for new chunk", K(ret));
            new_chunk->~ChunkType();
            sort_impl_.get_allocator()->free(new_chunk);
            new_chunk = nullptr;
          } else {
            sort_impl_.in_memory_chunk_ = new_chunk;
          }
        }
      }
    }
    return ret;
  }

  OB_INLINE bool is_integer_column(const ObObjType obj_type) const
  {
    return obj_type == ObTinyIntType || obj_type == ObSmallIntType || obj_type == ObMediumIntType ||
           obj_type == ObInt32Type || obj_type == ObIntType || obj_type == ObUTinyIntType ||
           obj_type == ObUSmallIntType || obj_type == ObUMediumIntType || obj_type == ObUInt32Type ||
           obj_type == ObUInt64Type;
  }
  void test_get_next_chunk(const int64_t chunk_count);
protected:
  static constexpr int64_t CONTINUOUS_VECTOR_BUFFER_SIZE = 16LL * 1024;

  // 测试用例已移到类定义之后
  lib::MemoryContext mem_context_;
  sql::ObMonitorNode monitor_node_;
  ObStorageVecSortImpl<TestSortCompare, ObSortKeyStore<false>, false> sort_impl_;
  ObArenaAllocator row_meta_alloc_;
  RowMeta sk_row_meta_;
  ObObjType col_types_[2];
  const int64_t rowkey_count_;
  const int64_t col_count_;
  const int64_t max_batch_size_;
  TestSortCompare *compare_;
};


void TestMergeSortOp::test_get_next_chunk(const int64_t chunk_count)
{
  STORAGE_LOG(INFO, "=================test_get_next_chunk================", K(chunk_count));
  share::ObTenantSwitchGuard tenant_guard;
  ASSERT_EQ(OB_SUCCESS, tenant_guard.switch_to(tenant_id_));

  tablet_id_ = 3000000 + chunk_count;
  ASSERT_EQ(OB_SUCCESS, initialize_env(col_types_, col_count_, rowkey_count_));
  ASSERT_EQ(OB_SUCCESS, initialize_sort_impl());
  const int64_t batch_size = 512;
  const bool multi_chunk_mode = chunk_count > 1;
  int total_chunk_row_count = 0;
  for (int64_t chunk_idx = 0; chunk_idx < chunk_count; ++chunk_idx) {
    ObBatchDatumRows batch_rows;
    ASSERT_EQ(OB_SUCCESS, generate_test_data(batch_rows, batch_size, false));

    common::ObFixedArray<ObIVector *, common::ObIAllocator> sk_vec_ptrs(allocator_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.reserve(sk_row_meta_.col_cnt_));
    for (int64_t i = 0; i < sk_row_meta_.col_cnt_; ++i) {
      ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(batch_rows.vectors_.at(i)));
    }

    std::vector<uint16_t> selector(batch_size);
    std::iota(selector.begin(), selector.end(), 0);

    std::vector<ObSortKeyStore<false> *> sk_rows(batch_size);
    int64_t inmem_row_size = 0;
    ASSERT_EQ(OB_SUCCESS, sort_impl_.add_batch(
        sk_vec_ptrs,
        nullptr,
        selector.data(),
        batch_size,
        sk_rows.data(),
        nullptr,
        inmem_row_size));

    ASSERT_EQ(OB_SUCCESS, sort_impl_.sort());
    // 检查 sort() 后的状态
    const int64_t chunks_before = sort_impl_.get_sort_chunks_size();
    STORAGE_LOG(INFO, "after sort",
                K(chunk_idx),
                K(chunks_before),
                K(sort_impl_.rows_.count()));
    if (chunks_before == 0) {
      // 第一次排序：纯内存排序，数据在 in_memory_chunk_ 中
      ASSERT_NE(nullptr, sort_impl_.in_memory_chunk_);
      ASSERT_TRUE(sort_impl_.in_memory_chunk_->use_inmem_data_);
      int64_t chunk_row_count = 0;
      ASSERT_EQ(OB_SUCCESS, verify_sorted_chunk(sort_impl_.in_memory_chunk_, chunk_row_count));
      ASSERT_EQ(batch_size, chunk_row_count) << "chunk row count mismatch, expected=" << batch_size << ", actual=" << chunk_row_count;
      if (multi_chunk_mode) {
        ASSERT_EQ(OB_SUCCESS, seal_in_memory_chunk_to_sort_chunks());
      } else {
        sort_impl_.in_memory_chunk_->reset_row_iter(); // reset the row iter of in_memory_chunk_
      }
      total_chunk_row_count += chunk_row_count;
    } else {
      // 第二次及以后：do_dump() 已经创建了新的 chunk 并添加到 sort_chunks_ 中
      // in_memory_chunk_ 已经被 reuse()，不需要再添加
      ASSERT_EQ(chunks_before, chunk_idx + 1) << "expected chunk count after do_dump";
      // 验证 do_dump() 创建的最后一个 chunk 的有序性
      // 使用 get_last() 直接访问最后一个 chunk（测试代码已 #define private public）
      ObSortVecOpChunk<ObSortKeyStore<false>, false> *last_chunk = sort_impl_.sort_chunks_.get_last();
      ASSERT_NE(nullptr, last_chunk);
      ASSERT_NE(last_chunk, sort_impl_.sort_chunks_.get_header());
      int64_t chunk_row_count = 0;
      ASSERT_EQ(OB_SUCCESS, verify_sorted_chunk(last_chunk, chunk_row_count));
      ASSERT_EQ(batch_size, chunk_row_count) << "chunk row count mismatch, expected=" << batch_size << ", actual=" << chunk_row_count;
      STORAGE_LOG(INFO, "verified chunk created by do_dump", K(chunk_idx), K(chunks_before), K(chunk_row_count));
      total_chunk_row_count += chunk_row_count;
    }
  }

  int64_t total_output_rows = 0;
  int64_t last_value = std::numeric_limits<int64_t>::min();
  bool first_row = true;
  int batch_ret = OB_SUCCESS;
  do {
    ObIArray<ObIVector *> *output_sk_rows = nullptr;
    ObIArray<ObIVector *> *output_addon_rows = nullptr;
    int64_t output_row_cnt = 0;
    batch_ret = sort_impl_.get_next_batch(output_row_cnt, output_sk_rows, output_addon_rows);
    if (OB_ITER_END == batch_ret) {
      break;
    }
    ASSERT_EQ(OB_SUCCESS, batch_ret);
    ASSERT_GE(output_row_cnt, 0);

    for (int64_t row_idx = 0; row_idx < output_row_cnt; ++row_idx) {
      const int64_t current = output_sk_rows->at(0)->get_int(row_idx);
      if (!first_row) {
        if (current < last_value) {
          STORAGE_LOG(INFO,"current value is less than last value", K(current), K(last_value), K(total_chunk_row_count), K(total_output_rows));
        }
        ASSERT_GE(current, last_value);
      } else {
        first_row = false;
      }
      last_value = current;
    }
    total_output_rows += output_row_cnt;
  } while (batch_ret == OB_SUCCESS);

  ASSERT_EQ(OB_ITER_END, batch_ret);
  ASSERT_EQ(batch_size * chunk_count, total_output_rows);
  STORAGE_LOG(INFO, "single batch sort test completed", K(batch_size), K(total_output_rows));
  sort_impl_.reset();
}

// =============================================================================
// 测试用例定义
// =============================================================================
TEST_F(TestMergeSortOp, test_get_next_chunk_one)
{
  test_get_next_chunk(1);
}

TEST_F(TestMergeSortOp, test_get_next_chunk_three)
{
  test_get_next_chunk(3);
}



// test for sort impl, check to get merge sort single compact row chunk

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_merge_sort_op.log*");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_merge_sort_op.log", true, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
