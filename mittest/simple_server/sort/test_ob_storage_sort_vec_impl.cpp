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

// 只测试 ObStorageVecSortImpl 的 add_batch 接口，让内部自行触发 before_add_row / do_dump 等逻辑，
// 不直接调用 sort/do_dump 之类的内部接口。

#define USING_LOG_PREFIX SQL_ENG

#include <gtest/gtest.h>

// 为了在单测中访问必要的内部成员（例如 rows_ 等），这里临时打开 private/protected。
#define private public
#define protected public
#include "sql/engine/sort/ob_storage_sort_vec_impl.h"
#undef private
#undef protected

#include "sql/engine/basic/ob_compact_row.h"
#include "sql/engine/sort/ob_sort_key_vec_op.h"
#include "sql/engine/sort/ob_sort_chunk_builder.h"
#include "share/vector/ob_i_vector.h"
#include "lib/container/ob_fixed_array.h"
#include "lib/rc/context.h"
#include "share/diagnosis/ob_sql_plan_monitor_node_list.h"
#include "sql/engine/px/ob_px_dtl_msg.h"
#include "storage/blocksstable/ob_storage_datum.h"

#include "../env/ob_simple_cluster_test_base.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;

// 行类型：直接复用已有的 ObSortKeyStore（无 addon）
using ObTestStorageRow = ObSortKeyStore<false>;

// 比较器，需要满足 ObFullSortStrategy / ObStorageVecSortImpl 的接口约束：
//  - 构造函数(ObIAllocator &)
//  - operator()(const Store_Row *l, const Store_Row *r)
//  - operator()(const ChunkType *l, const ChunkType *r)  // 外排归并堆使用
//  - get_error_code() const                               // 堆接口调用
//  - reset()
//  - reuse()
//  - fallback_to_disable_encode_sortkey()
struct ObTestStorageCompare
{
  using ChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;

  ObTestStorageCompare() {}

  void reset() {}
  void reuse() {}
  void fallback_to_disable_encode_sortkey() {}

  bool operator()(const ObTestStorageRow *l,
                  const ObTestStorageRow *r) const
  {
    // 比较指针指向的实际数据内容，而不是指针地址
    if (l == nullptr && r == nullptr) return false;
    if (l == nullptr) return true;   // null 值排在前面
    if (r == nullptr) return false;

    // 为了简化比较，我们存储一个外部的全局RowMeta指针
    // 实际使用中，RowMeta应该通过其他方式传递
    extern const RowMeta *g_test_row_meta;
    const RowMeta *row_meta = g_test_row_meta;

    if (row_meta == nullptr) {
      // 如果没有RowMeta，回退到比较指针地址（虽然不正确，但避免崩溃）
      return l < r;
    }

    // 使用RowMeta提取数据并比较
    const char *payload_l = nullptr;
    const char *payload_r = nullptr;
    ObLength len_l = 0;
    ObLength len_r = 0;

    // 获取两行的数据
    const_cast<ObTestStorageRow*>(l)->get_cell_payload(*row_meta, 0, payload_l, len_l);
    const_cast<ObTestStorageRow*>(r)->get_cell_payload(*row_meta, 0, payload_r, len_r);

    // 比较数据（假设都是int64_t类型）
    if (payload_l == nullptr && payload_r == nullptr) return false;
    if (payload_l == nullptr) return true;
    if (payload_r == nullptr) return false;
    if (len_l != sizeof(int64_t) || len_r != sizeof(int64_t)) {
      // 如果长度不匹配，比较长度
      return len_l < len_r;
    }

    int64_t value_l = *reinterpret_cast<const int64_t *>(payload_l);
    int64_t value_r = *reinterpret_cast<const int64_t *>(payload_r);
    return value_l < value_r;
  }

  // 堆归并使用：比较 chunk 当前行
  bool operator()(const ChunkType *l_chunk, const ChunkType *r_chunk) const
  {
    const ObTestStorageRow *l = (nullptr == l_chunk) ? nullptr : l_chunk->sk_row_;
    const ObTestStorageRow *r = (nullptr == r_chunk) ? nullptr : r_chunk->sk_row_;
    // 反向比较，确保 ObBinaryHeap 构建最小堆（top 为最小值）
    return (*this)(r, l);
  }

  // 用于 std::upper_bound：比较 ObTestStorageRow 和 ObSEArray<ObDatum>
  bool operator()(const ObTestStorageRow *l, const ObIArray<ObDatum> &r_datums) const
  {
    if (l == nullptr) return true;
    if (r_datums.count() == 0) return false;

    extern const RowMeta *g_test_row_meta;
    const RowMeta *row_meta = g_test_row_meta;

    if (row_meta == nullptr) {
      return false;  // 无法比较
    }

    // 提取左侧数据
    const char *payload_l = nullptr;
    ObLength len_l = 0;
    const_cast<ObTestStorageRow*>(l)->get_cell_payload(*row_meta, 0, payload_l, len_l);

    // 获取右侧数据
    const ObDatum &r_datum = r_datums.at(0);

    // 处理 NULL 值
    if (payload_l == nullptr && r_datum.is_null()) return false;
    if (payload_l == nullptr) return true;
    if (r_datum.is_null()) return false;

    // 比较数据（假设都是int64_t类型）
    if (len_l != sizeof(int64_t) || r_datum.len_ != sizeof(int64_t)) {
      return len_l < r_datum.len_;
    }

    int64_t value_l = *reinterpret_cast<const int64_t *>(payload_l);
    int64_t value_r = *reinterpret_cast<const int64_t *>(r_datum.ptr_);
    return value_l < value_r;
  }

  // compare 函数返回 -1, 0, 1
  int compare(const ObTestStorageRow *l, const ObIArray<ObDatum> &r_datums) {
    if (l == nullptr && r_datums.count() == 0) return 0;
    if (l == nullptr) return -1;
    if (r_datums.count() == 0) return 1;

    extern const RowMeta *g_test_row_meta;
    const RowMeta *row_meta = g_test_row_meta;

    if (row_meta == nullptr) {
      return 0;  // 无法比较
    }

    // 提取左侧数据
    const char *payload_l = nullptr;
    ObLength len_l = 0;
    const_cast<ObTestStorageRow*>(l)->get_cell_payload(*row_meta, 0, payload_l, len_l);

    // 获取右侧数据
    const ObDatum &r_datum = r_datums.at(0);

    // 处理 NULL 值
    if (payload_l == nullptr && r_datum.is_null()) return 0;
    if (payload_l == nullptr) return -1;
    if (r_datum.is_null()) return 1;

    // 比较数据（假设都是int64_t类型）
    if (len_l != sizeof(int64_t) || r_datum.len_ != sizeof(int64_t)) {
      return (len_l < r_datum.len_) ? -1 : (len_l > r_datum.len_) ? 1 : 0;
    }

    int64_t value_l = *reinterpret_cast<const int64_t *>(payload_l);
    int64_t value_r = *reinterpret_cast<const int64_t *>(r_datum.ptr_);

    if (value_l < value_r) return -1;
    if (value_l > value_r) return 1;
    return 0;
  }

  int get_error_code() const { return OB_SUCCESS; }
};

// 全局变量用于比较器访问RowMeta（仅用于测试）
const RowMeta *g_test_row_meta = nullptr;

// 扩展比较器以支持 slice 决策所需的接口
struct ObTestStorageCompareWithSlice
{
  using ChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;
  using DatumArray = common::ObSEArray<ObDatum, 8>;

  ObTestStorageCompareWithSlice() : row_meta_(nullptr), error_code_(OB_SUCCESS) {}

  void reset() {}
  void reuse() {}
  void fallback_to_disable_encode_sortkey() {}

  void init_for_slice(const RowMeta *row_meta)
  {
    row_meta_ = row_meta;
    // 添加比较信息：单列int64_t
    CmpInfo info;
    info.col_idx_ = 0;
    cmp_infos_.push_back(info);
  }

  // 比较两个 Store_Row（基础比较，用于排序）
  bool operator()(const ObTestStorageRow *l, const ObTestStorageRow *r) const
  {
    // 使用与 ObTestStorageCompare 相同的逻辑
    if (l == nullptr && r == nullptr) return false;
    if (l == nullptr) return true;
    if (r == nullptr) return false;

    const RowMeta *row_meta = row_meta_;
    if (row_meta == nullptr) {
      return l < r;
    }

    const char *payload_l = nullptr;
    const char *payload_r = nullptr;
    ObLength len_l = 0;
    ObLength len_r = 0;

    const_cast<ObTestStorageRow*>(l)->get_cell_payload(*row_meta, 0, payload_l, len_l);
    const_cast<ObTestStorageRow*>(r)->get_cell_payload(*row_meta, 0, payload_r, len_r);

    if (payload_l == nullptr && payload_r == nullptr) return false;
    if (payload_l == nullptr) return true;
    if (payload_r == nullptr) return false;
    if (len_l != sizeof(int64_t) || len_r != sizeof(int64_t)) {
      return len_l < len_r;
    }

    int64_t value_l = *reinterpret_cast<const int64_t *>(payload_l);
    int64_t value_r = *reinterpret_cast<const int64_t *>(payload_r);
    return value_l < value_r;
  }

  // 堆归并使用：比较 chunk 当前行
  bool operator()(const ChunkType *l_chunk, const ChunkType *r_chunk) const
  {
    const ObTestStorageRow *l = (nullptr == l_chunk) ? nullptr : l_chunk->sk_row_;
    const ObTestStorageRow *r = (nullptr == r_chunk) ? nullptr : r_chunk->sk_row_;
    return (*this)(r, l);
  }

  // 比较两个DatumArray（用于slice决策）
  bool operator()(const DatumArray &l_datums, const DatumArray &r_datums)
  {
    bool less = false;

    if (OB_UNLIKELY(OB_SUCCESS != error_code_)) {
      return false;
    }

    if (l_datums.count() != r_datums.count() || l_datums.count() == 0) {
      error_code_ = OB_INVALID_ARGUMENT;
      return false;
    }

    // 简单比较：假设都是int64_t类型
    int64_t l_val = l_datums.at(0).get_int();
    int64_t r_val = r_datums.at(0).get_int();
    LOG_INFO("compare value_l < value_r", K(l_val), K(r_val));
    return l_val < r_val;
  }


  bool operator()(const DatumArray &l_datums, const ObTestStorageRow *r) {
    int cmp = compare(r, l_datums);
    return cmp > 0;
  }

  bool operator()(const ObTestStorageRow *l, const DatumArray &r_datums) {
    int cmp = compare(l, r_datums);
    return cmp < 0;
  }

  int compare(const ObTestStorageRow *l, const DatumArray &r_datums) {
    int cmp = 0;
    // 从 ObTestStorageRow 中提取排序键的值
    const char *payload_l = nullptr;
    ObLength len_l = 0;
    const_cast<ObTestStorageRow*>(l)->get_cell_payload(*row_meta_, 0, payload_l, len_l);

    // 从 DatumArray 中提取值
    int64_t l_val = *reinterpret_cast<const int64_t *>(payload_l);
    int64_t r_val = r_datums.at(0).get_int();

    cmp = l_val > r_val ? 1 : (l_val < r_val ? -1 : 0);

    LOG_INFO("compare row < datums", K(l_val), K(r_val), K(cmp));
    return cmp;
  }

  int get_error_code() const { return error_code_; }

  struct CmpInfo {
    int64_t col_idx_;

    int64_t to_string(char *buf, const int64_t buf_len) const
    {
      int64_t pos = 0;
      J_OBJ_START();
      J_KV(K_(col_idx));
      J_OBJ_END();
      return pos;
    }
  };

  const RowMeta *row_meta_;
  common::ObSEArray<CmpInfo, 8> cmp_infos_;
  mutable int error_code_;
};

// 一个可以包含多行数据的 MockIVector：可以返回指定行的 int64_t 数据
class MockBatchDataIVector : public ObIVector
{
public:
  explicit MockBatchDataIVector(const int64_t *values, int64_t count)
    : values_(values), count_(count) {}
  virtual ~MockBatchDataIVector() {}

  virtual VectorFormat get_format() const override { return VEC_FIXED; }

  virtual void get_payload(const int64_t row_idx, const char *&payload, ObLength &length) const override
  {
    if (row_idx >= 0 && row_idx < count_) {
      payload = reinterpret_cast<const char *>(&values_[row_idx]);
      length = sizeof(int64_t);
    } else {
      payload = nullptr;
      length = 0;
    }
  }
  virtual void get_payload(const int64_t row_idx, bool &is_null,
                           const char *&payload, ObLength &length) const override
  {
    if (row_idx >= 0 && row_idx < count_) {
      is_null = false;
      payload = reinterpret_cast<const char *>(&values_[row_idx]);
      length = sizeof(int64_t);
    } else {
      is_null = true;
      payload = nullptr;
      length = 0;
    }
  }
  virtual const char *get_payload(const int64_t row_idx) const override
  {
    if (row_idx >= 0 && row_idx < count_) {
      return reinterpret_cast<const char *>(&values_[row_idx]);
    }
    return nullptr;
  }
  virtual ObLength get_length(const int64_t row_idx) const override
  {
    if (row_idx >= 0 && row_idx < count_) {
      return sizeof(int64_t);
    }
    return 0;
  }
  virtual void set_length(const int64_t, const ObLength) override {}
  virtual void set_payload(const int64_t, const void *, const ObLength) override {}
  virtual void set_payload_shallow(const int64_t, const void *, const ObLength) override {}

  virtual bool has_null() const override { return false; }
  virtual void set_has_null() override {}
  virtual void reset_has_null() override {}
  virtual bool is_batch_ascii() const override { return false; }
  virtual void reset_is_batch_ascii() override {}
  virtual void set_is_batch_ascii() override {}
  virtual void set_has_non_ascii() override {}
  virtual bool is_null(const int64_t row_idx) const override { return false; }
  virtual void set_null(const int64_t) override {}
  virtual void unset_null(const int64_t) override {}

  virtual int default_hash(BATCH_EVAL_HASH_ARGS) const override { return OB_SUCCESS; }
  virtual int murmur_hash(BATCH_EVAL_HASH_ARGS) const override { return OB_SUCCESS; }
  virtual int murmur_hash_v3(BATCH_EVAL_HASH_ARGS) const override { return OB_SUCCESS; }
  virtual int murmur_hash_v3_for_one_row(EVAL_HASH_ARGS_FOR_ROW) const override
  {
    return OB_SUCCESS;
  }

  virtual int null_first_cmp(VECTOR_ONE_COMPARE_ARGS) const override { return OB_SUCCESS; }
  virtual int null_last_cmp(VECTOR_ONE_COMPARE_ARGS) const override { return OB_SUCCESS; }
  virtual int no_null_cmp(VECTOR_NOT_NULL_COMPARE_ARGS) const override { return OB_SUCCESS; }
  virtual int null_first_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const override { return OB_SUCCESS; }
  virtual int null_last_mul_cmp(VECTOR_MUL_COMPARE_ARGS) const override { return OB_SUCCESS; }
  virtual int null_first_cmp_batch_rows(VECTOR_COMPARE_BATCH_ROWS_ARGS) const override
  {
    return OB_SUCCESS;
  }
  virtual int no_null_cmp_batch_rows(VECTOR_COMPARE_BATCH_ROWS_ARGS) const override
  {
    return OB_SUCCESS;
  }

  virtual int from_rows(const RowMeta &,
                        const ObCompactRow **,
                        const int64_t,
                        const int64_t) override
  {
    return OB_SUCCESS;
  }
  virtual int from_rows(const RowMeta &,
                        const ObCompactRow **,
                        const uint16_t[],
                        const int64_t,
                        const int64_t) override
  {
    return OB_SUCCESS;
  }
  virtual int from_row(const RowMeta &,
                       const ObCompactRow *,
                       const int64_t,
                       const int64_t) override
  {
    return OB_SUCCESS;
  }

  virtual int to_rows(const RowMeta &row_meta,
                      ObCompactRow **stored_rows,
                      const uint16_t selector[],
                      const int64_t size,
                      const int64_t col_idx) const override
  {
    // 将向量中的数据复制到存储的行中
    for (int64_t i = 0; i < size; i++) {
      const uint16_t row_idx = selector[i];
      const char *payload = nullptr;
      ObLength len = 0;

      // 获取当前行的数据
      get_payload(row_idx, payload, len);

      if (payload != nullptr && len > 0) {
        // 将数据复制到存储行中
        stored_rows[i]->set_cell_payload(row_meta, col_idx, payload, len);
      }
    }
    return OB_SUCCESS;
  }
  virtual int to_rows(const RowMeta &,
                      ObCompactRow **,
                      const int64_t,
                      const int64_t) const override
  {
    return OB_SUCCESS;
  }
  // 注意：签名必须与 ObIVector 中完全一致（row_idx 为 uint64_t，且有两个重载）
  virtual int to_row(const RowMeta &,
                     ObCompactRow * /*stored_row*/,
                     const uint64_t /*row_idx*/,
                     const int64_t /*col_idx*/) const override
  {
    return OB_SUCCESS;
  }
  virtual int to_row(const RowMeta &,
                     ObCompactRow * /*stored_row*/,
                     const uint64_t /*row_idx*/,
                     const int64_t /*col_idx*/,
                     const int64_t /*remain_size*/,
                     const bool /*is_fixed_length_data*/,
                     int64_t &/*row_size*/) const override
  {
    return OB_SUCCESS;
  }

private:
  const int64_t *values_;
  int64_t count_;
};

// 验证存储的行数据是否与输入数据一致
void verify_stored_rows(ObTestStorageRow **sk_rows, const int64_t *expected_values, int64_t count, const RowMeta &row_meta)
{
  for (int64_t i = 0; i < count; i++) {
    ASSERT_NE(nullptr, sk_rows[i]) << "sk_rows[" << i << "] should not be null";

    // 获取存储的行数据
    const char *payload = nullptr;
    ObLength len = 0;
    sk_rows[i]->get_cell_payload(row_meta, 0, payload, len);

    ASSERT_EQ(sizeof(int64_t), len) << "Row " << i << " data length should be 8 bytes";

    // 比较数据值
    int64_t stored_value = *reinterpret_cast<const int64_t *>(payload);
    EXPECT_EQ(expected_values[i], stored_value)
      << "Row " << i << " stored value should match input value";
  }
}

// 验证chunk中的排序数据
void verify_chunk_sorted_data(ObSortVecOpChunk<ObTestStorageRow, false> *chunk,
                              const int64_t *expected_values, int64_t expected_count)
{
  ASSERT_NE(nullptr, chunk) << "Chunk should not be null";

  // 初始化并迭代chunk中的行
  ASSERT_EQ(OB_SUCCESS, chunk->init_row_iter());

  int64_t row_idx = 0;

  while (OB_SUCCESS == chunk->get_next_row()) {
    ASSERT_LT(row_idx, expected_count) << "Should not have more than " << expected_count << " rows";

    // 获取行数据
    const char *payload = nullptr;
    ObLength len = 0;
    chunk->sk_row_->get_cell_payload(*chunk->sort_row_store_mgr_.get_sk_row_meta(), 0, payload, len);

    ASSERT_EQ(sizeof(int64_t), len) << "Row " << row_idx << " data length should be 8 bytes";
    int64_t actual_value = *reinterpret_cast<const int64_t *>(payload);
    ASSERT_EQ(expected_values[row_idx], actual_value)
      << "Row " << row_idx << " value should be " << expected_values[row_idx]
      << " but got " << actual_value;

    row_idx++;
  }

  // 验证总共读取了预期数量的数据
  ASSERT_EQ(expected_count, row_idx) << "Should have exactly " << expected_count << " rows";
}

namespace oceanbase
{
namespace unittest
{

class ObStorageSortVecImplAddBatchTest : public ObSimpleClusterTestBase
{
public:
  ObStorageSortVecImplAddBatchTest() : ObSimpleClusterTestBase("test_storage_sort_vec_impl_add_batch_"),
                                       sort_impl_(ROOT_CONTEXT, monitor_node_) {}

  virtual void SetUp() override
  {
    ObSimpleClusterTestBase::SetUp();

    // 1. 构造一个最简单的 RowMeta：单列、定长 int64
    common::ObArray<ColMetaInfo> expr_infos;
    ASSERT_EQ(OB_SUCCESS, expr_infos.reserve(1));
    ColMetaInfo info(true /*is_fixed*/, static_cast<uint32_t>(sizeof(int64_t)));
    ASSERT_EQ(OB_SUCCESS, expr_infos.push_back(info));
    ASSERT_EQ(OB_SUCCESS, row_meta_.init(expr_infos, /*extra_size*/0, true, &alloc_));

    // 设置全局RowMeta指针供比较器使用
    g_test_row_meta = &row_meta_;

    // 2. 初始化sort_impl_
    ObMemAttr mem_attr(OB_SYS_TENANT_ID, "SSVImplAdd", ObCtxIds::DEFAULT_CTX_ID);
    ASSERT_EQ(OB_SUCCESS,
              sort_impl_.init(&compare_,
                              &row_meta_,
                              /*addon_row_meta*/nullptr,
                              /*max_batch_size*/1024,
                              mem_attr,
                              ObCompressorType::NONE_COMPRESSOR,
                              /*enable_trunc*/false,
                              /*tempstore_read_alignment_size*/1,
                              /*tenant_id*/OB_SYS_TENANT_ID,
                              /*enable_encode_sortkey*/false,
                              /*slice_decider*/nullptr));

    // 打开算子 dump 开关，保证 need_dump 路径有机会被走到
    GCONF.enable_sql_operator_dump.set_value("True");
  }

  virtual void TearDown() override
  {
    // 清理全局RowMeta指针
    g_test_row_meta = nullptr;
    sort_impl_.reset();
    ObSimpleClusterTestBase::TearDown();
  }

protected:
  // 使用ObSimpleClusterTestBase提供的内存上下文
  ObMonitorNode monitor_node_;
  ObTestStorageCompare compare_;
  ObStorageVecSortImpl<ObTestStorageCompare, ObTestStorageRow, /*has_addon*/false> sort_impl_;
  ObArenaAllocator alloc_;
  RowMeta row_meta_;
};

// 只调用 add_batch，不直接调用 sort / do_dump：
//  - 第一次 add_batch 使用 size=0，只验证接口 & before_add_row 流程
//  - 随后构造两行真实数据，再次调用 add_batch，
//    让 before_add_row 能看到已有行，从而有机会触发 sort_resource_mgr_ 的内存检查 / dump 决策
TEST_F(ObStorageSortVecImplAddBatchTest, test_add_batch_only)
{
  // 1) 第一次 add_batch：size=0，只是打通接口和 before_add_row/after_add_row 逻辑
  {
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(0));
    const ObFixedArray<ObIVector *, ObIAllocator> *addon_vec_ptrs = nullptr;

    uint16_t selector[1] = {0};
    ObTestStorageRow *sk_rows[1] = {nullptr};
    int64_t inmem_row_size = 0;

    ASSERT_EQ(OB_SUCCESS,
              sort_impl_.add_batch(sk_vec_ptrs,
                                   addon_vec_ptrs,
                                   selector,
                                   /*size*/0,
                                   sk_rows,
                                   /*addon_rows*/nullptr,
                                   inmem_row_size));
  }

  // 2) 构造两行真实数据，测试 add_batch 的完整流程
  {
    // 构造包含两行数据的向量：值为 100 和 200
    int64_t data_values[2] = {100, 200};
    MockBatchDataIVector batch_vec(data_values, 2);

    // 构造向量数组（单列排序键）
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));

    const ObFixedArray<ObIVector *, ObIAllocator> *addon_vec_ptrs = nullptr;

    // 构造 selector 数组：选择两行数据 [0, 1]
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;

    // 调用 add_batch 处理两行数据
    ASSERT_EQ(OB_SUCCESS,
              sort_impl_.add_batch(sk_vec_ptrs,
                                   addon_vec_ptrs,
                                   selector,
                                   /*size*/2,  // 两行数据
                                   sk_rows,
                                   /*addon_rows*/nullptr,
                                   inmem_row_size));

    // 验证存储的行数据与输入数据一致
    verify_stored_rows(sk_rows, data_values, 2, row_meta_);
  }

  // 3) 再次调用 add_batch，测试内存检查和 dump 逻辑
  {
    // 再次添加包含两行数据的向量：值为 50 和 150
    int64_t data_values2[2] = {50, 150};
    MockBatchDataIVector batch_vec2(data_values2, 2);

    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec2));

    const ObFixedArray<ObIVector *, ObIAllocator> *addon_vec_ptrs = nullptr;

    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;

    ASSERT_EQ(OB_SUCCESS,
              sort_impl_.add_batch(sk_vec_ptrs,
                                   addon_vec_ptrs,
                                   selector,
                                   /*size*/2,
                                   sk_rows,
                                   /*addon_rows*/nullptr,
                                   inmem_row_size));

    // 验证存储的行数据与输入数据一致
    verify_stored_rows(sk_rows, data_values2, 2, row_meta_);
  }

  // 3) 验证排序前的数据顺序
  LOG_INFO("=== Before sorting ===");
  LOG_INFO("Test: should have 4 rows before sorting");

  // 4) 执行内存数据排序
  LOG_INFO("Calling sort_inmem_data()");
  ASSERT_EQ(OB_SUCCESS, sort_impl_.sort_inmem_data(false));
  LOG_INFO("sort_inmem_data() completed successfully");

  // 5) 验证sort_chunks的数量和内容
  using SortVecOpChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;
  common::ObArray<SortVecOpChunkType *> sort_chunks;

  // 添加调试信息
  LOG_INFO("=== After sort_inmem_data() ===");
  LOG_INFO("Testing get_sort_chunks");
  int ret = sort_impl_.get_sort_chunks(sort_chunks);
  LOG_INFO("get_sort_chunks returned", K(ret), "chunk_count", sort_chunks.count());

  ASSERT_EQ(OB_SUCCESS, ret);

  // 验证chunk数量为1
  ASSERT_EQ(1, sort_chunks.count()) << "Should have exactly 1 chunk after sort";

  // 验证每个chunk中的数据
  SortVecOpChunkType *chunk = sort_chunks[0];

  // 验证排序后的数据：应该是 {50, 100, 150, 200}
  int64_t expected_sorted_values[4] = {50, 100, 150, 200};
  verify_chunk_sorted_data(chunk, expected_sorted_values, 4);
}

TEST_F(ObStorageSortVecImplAddBatchTest, test_add_batch_with_dump)
{
  // 1) 插入第一批数据: 100, 200
  {
    int64_t data_values[2] = {100, 200};
    MockBatchDataIVector batch_vec(data_values, 2);
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));

    const ObFixedArray<ObIVector *, ObIAllocator> *addon_vec_ptrs = nullptr;
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;

    ASSERT_EQ(OB_SUCCESS,
              sort_impl_.add_batch(sk_vec_ptrs,
                                   addon_vec_ptrs,
                                   selector,
                                   /*size*/2,
                                   sk_rows,
                                   /*addon_rows*/nullptr,
                                   inmem_row_size));
    verify_stored_rows(sk_rows, data_values, 2, row_meta_);
  }

  // 2) 强制 Dump，模拟内存不足触发落盘
  // 由于使用了 #define private public，我们可以直接调用 do_dump
  LOG_INFO("Force dump triggered");
  ASSERT_EQ(OB_SUCCESS, sort_impl_.do_dump());

  // 验证 rows_ 被清空
  ASSERT_EQ(0, sort_impl_.rows_.count());

  // 3) 插入第二批数据: 50, 150
  {
    int64_t data_values2[2] = {50, 150};
    MockBatchDataIVector batch_vec2(data_values2, 2);
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec2));

    const ObFixedArray<ObIVector *, ObIAllocator> *addon_vec_ptrs = nullptr;
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;

    ASSERT_EQ(OB_SUCCESS,
              sort_impl_.add_batch(sk_vec_ptrs,
                                   addon_vec_ptrs,
                                   selector,
                                   /*size*/2,
                                   sk_rows,
                                   /*addon_rows*/nullptr,
                                   inmem_row_size));
    verify_stored_rows(sk_rows, data_values2, 2, row_meta_);
  }

  // 4) 处理剩余的内存数据 (生成第二个 chunk)
  LOG_INFO("Calling sort_inmem_data()");
  ASSERT_EQ(OB_SUCCESS, sort_impl_.sort_inmem_data(false));

  // 5) 验证 chunks 数量和内容
  using SortVecOpChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;
  common::ObArray<SortVecOpChunkType *> sort_chunks;
  ASSERT_EQ(OB_SUCCESS, sort_impl_.get_sort_chunks(sort_chunks));

  // 应该有两个 chunk：一个来自 dump (100, 200)，一个来自内存 (50, 150)
  ASSERT_EQ(2, sort_chunks.count());

  // 验证 Chunk 1 (Dumped): {100, 200}
  int64_t expected_1[2] = {100, 200};
  verify_chunk_sorted_data(sort_chunks[0], expected_1, 2);

  // 验证 Chunk 2 (In-Memory): {50, 150}
  int64_t expected_2[2] = {50, 150};
  verify_chunk_sorted_data(sort_chunks[1], expected_2, 2);
}

TEST_F(ObStorageSortVecImplAddBatchTest, test_multiple_dumps_empty_mem)
{
  // 1) Batch 1: {10, 20} -> Dump
  {
    int64_t data_values[2] = {10, 20};
    MockBatchDataIVector batch_vec(data_values, 2);
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;
    ASSERT_EQ(OB_SUCCESS, sort_impl_.add_batch(sk_vec_ptrs, nullptr, selector, 2, sk_rows, nullptr, inmem_row_size));
    ASSERT_EQ(OB_SUCCESS, sort_impl_.do_dump());
    ASSERT_EQ(0, sort_impl_.rows_.count());
  }

  // 2) Batch 2: {30, 40} -> Dump
  {
    int64_t data_values[2] = {30, 40};
    MockBatchDataIVector batch_vec(data_values, 2);
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;
    ASSERT_EQ(OB_SUCCESS, sort_impl_.add_batch(sk_vec_ptrs, nullptr, selector, 2, sk_rows, nullptr, inmem_row_size));
    ASSERT_EQ(OB_SUCCESS, sort_impl_.do_dump());
    ASSERT_EQ(0, sort_impl_.rows_.count());
  }

  // 3) Verify
  using SortVecOpChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;
  common::ObArray<SortVecOpChunkType *> sort_chunks;
  ASSERT_EQ(OB_SUCCESS, sort_impl_.get_sort_chunks(sort_chunks));

  ASSERT_EQ(2, sort_chunks.count());

  int64_t exp1[2] = {10, 20};
  verify_chunk_sorted_data(sort_chunks[0], exp1, 2);
  int64_t exp2[2] = {30, 40};
  verify_chunk_sorted_data(sort_chunks[1], exp2, 2);
}

TEST_F(ObStorageSortVecImplAddBatchTest, test_merge_sort_chunks_external)
{
  using SortVecOpChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;

  auto add_and_dump = [&](const int64_t v1, const int64_t v2) {
    int64_t data_values[2] = {v1, v2};
    MockBatchDataIVector batch_vec(data_values, 2);
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;
    ASSERT_EQ(OB_SUCCESS, sort_impl_.add_batch(sk_vec_ptrs, nullptr, selector, 2, sk_rows, nullptr, inmem_row_size));
    verify_stored_rows(sk_rows, data_values, 2, row_meta_);
    ASSERT_EQ(OB_SUCCESS, sort_impl_.do_dump());
    ASSERT_EQ(0, sort_impl_.rows_.count());
  };

  // Two dumped chunks will be merged by external sorter
  add_and_dump(30, 10); // unsorted input to verify chunk sort
  add_and_dump(25, 20);

  ASSERT_GE(sort_impl_.sort_chunks_.get_size(), 2);

  SortVecOpChunkType *merged_chunk = nullptr;
  ASSERT_EQ(OB_SUCCESS, sort_impl_.merge_sort_chunks(merged_chunk));
  ASSERT_NE(nullptr, merged_chunk);

  int64_t expected[4] = {10, 20, 25, 30};
  verify_chunk_sorted_data(merged_chunk, expected, 4);
}

TEST_F(ObStorageSortVecImplAddBatchTest, test_sort_full_external_merge_path)
{
  using SortVecOpChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;

  auto add_and_dump = [&](const int64_t v1, const int64_t v2) {
    int64_t data_values[2] = {v1, v2};
    MockBatchDataIVector batch_vec(data_values, 2);
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;
    ASSERT_EQ(OB_SUCCESS, sort_impl_.add_batch(sk_vec_ptrs, nullptr, selector, 2, sk_rows, nullptr, inmem_row_size));
    verify_stored_rows(sk_rows, data_values, 2, row_meta_);
    ASSERT_EQ(OB_SUCCESS, sort_impl_.do_dump());
    ASSERT_EQ(0, sort_impl_.rows_.count());
  };

  // Prepare two external chunks
  add_and_dump(300, 100);
  add_and_dump(400, 50);

  // Keep one batch in memory to cover mixed path
  {
    int64_t data_values[2] = {250, 200};
    MockBatchDataIVector batch_vec(data_values, 2);
    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));
    uint16_t selector[2] = {0, 1};
    ObTestStorageRow *sk_rows[2] = {nullptr, nullptr};
    int64_t inmem_row_size = 0;
    ASSERT_EQ(OB_SUCCESS, sort_impl_.add_batch(sk_vec_ptrs, nullptr, selector, 2, sk_rows, nullptr, inmem_row_size));
    verify_stored_rows(sk_rows, data_values, 2, row_meta_);
  }

  // Execute full sort; should merge external chunks and in-memory batch
  ASSERT_EQ(OB_SUCCESS, sort_impl_.sort());

  SortVecOpChunkType *final_chunk = nullptr;
  if (sort_impl_.sort_chunks_.get_size() >= 2) {
    // 内部还留有多个 chunk，直接调用 merge_sort_chunks 模拟最终归并
    ASSERT_EQ(OB_SUCCESS, sort_impl_.merge_sort_chunks(final_chunk));
    ASSERT_NE(nullptr, final_chunk);
  } else {
    common::ObArray<SortVecOpChunkType *> sort_chunks;
    ASSERT_EQ(OB_SUCCESS, sort_impl_.get_sort_chunks(sort_chunks));
    ASSERT_EQ(1, sort_chunks.count());
    final_chunk = sort_chunks[0];
  }

  int64_t expected_sorted[6] = {50, 100, 200, 250, 300, 400};
  verify_chunk_sorted_data(final_chunk, expected_sorted, 6);
}

//测试 slice 决策器在 ObStorageVecSortImpl 中的集成
TEST_F(ObStorageSortVecImplAddBatchTest, test_slice_decider_integration)
{
  // 创建带 slice 支持的比较器
  ObTestStorageCompareWithSlice slice_compare;
  slice_compare.init_for_slice(&row_meta_);

  // 创建 ObPxTabletRange: 3个slice需要2个边界 [100, 200]
  // Slice 0: < 100
  // Slice 1: [100, 200)
  // Slice 2: >= 200
  ObPxTabletRange tablet_range;
  tablet_range.tablet_id_ = 1;

  // 设置边界值
  int64_t boundaries[2] = {100, 200};
  ObStorageDatum datum1;
  datum1.reuse();
  datum1.set_int(boundaries[0]);

  ObStorageDatum datum2;
  datum2.reuse();
  datum2.set_int(boundaries[1]);

  ObPxTabletRange::DatumKey datum_key;
  ASSERT_EQ(OB_SUCCESS, datum_key.push_back(datum1));
  ASSERT_EQ(OB_SUCCESS, tablet_range.range_cut_.push_back(datum_key));
  datum_key.reuse();
  ASSERT_EQ(OB_SUCCESS, datum_key.push_back(datum2));
  ASSERT_EQ(OB_SUCCESS, tablet_range.range_cut_.push_back(datum_key));

  // 创建 slice 决策器
  ObSortChunkSliceDecider<ObTestStorageCompareWithSlice, ObTestStorageRow> slice_decider;
  ASSERT_EQ(OB_SUCCESS, slice_decider.init(&tablet_range, &slice_compare));

  // 创建新的 sort_impl 并设置 slice_decider
  ObStorageVecSortImpl<ObTestStorageCompareWithSlice, ObTestStorageRow, false> sort_impl_with_slice(ROOT_CONTEXT, monitor_node_);

  ObMemAttr mem_attr(OB_SYS_TENANT_ID, "SSVSlice", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS,
            sort_impl_with_slice.init(&slice_compare,
                                      &row_meta_,
                                      /*addon_row_meta*/nullptr,
                                      /*max_batch_size*/1024,
                                      mem_attr,
                                      ObCompressorType::NONE_COMPRESSOR,
                                      /*enable_trunc*/false,
                                      /*tempstore_read_alignment_size*/1,
                                      /*tenant_id*/OB_SYS_TENANT_ID,
                                      /*enable_encode_sortkey*/false,
                                      &slice_decider));

  // 添加数据到不同的 slice
  // Slice 0 (< 100): 50, 80
  // Slice 1 ([100, 200)): 120, 150
  // Slice 2 (>= 200): 250, 300

  // 批次1: 数据跨越所有3个slice
  {
    int64_t data_values[6] = {50, 120, 250, 80, 150, 300};
    MockBatchDataIVector batch_vec(data_values, 6);

    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));

    uint16_t selector[6] = {0, 1, 2, 3, 4, 5};
    ObTestStorageRow *sk_rows[6] = {nullptr};
    int64_t inmem_row_size = 0;

    ASSERT_EQ(OB_SUCCESS,
              sort_impl_with_slice.add_batch(sk_vec_ptrs,
                                             nullptr,
                                             selector,
                                             6,
                                             sk_rows,
                                             nullptr,
                                             inmem_row_size));

    // 验证数据已存储
    verify_stored_rows(sk_rows, data_values, 6, row_meta_);
  }

  // 执行排序并构建 chunk
  LOG_INFO("Building chunks with slice information");
  ASSERT_EQ(OB_SUCCESS, sort_impl_with_slice.do_dump());

  // 验证 chunks 及其 slice_id
  using SortVecOpChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;
  common::ObArray<SortVecOpChunkType *> sort_chunks;
  ASSERT_EQ(OB_SUCCESS, sort_impl_with_slice.get_sort_chunks(sort_chunks));

  // 应该有3个chunk，分别对应3个slice
  LOG_INFO("Chunk count", "count", sort_chunks.count());
  ASSERT_EQ(3, sort_chunks.count()) << "Should have 3 chunks for 3 slices";

  // 验证每个 chunk 的 slice_id 和数据
  // Slice 0: {50, 80}
  ASSERT_EQ(0, sort_chunks[0]->slice_id_);
  int64_t expected_slice0[2] = {50, 80};
  verify_chunk_sorted_data(sort_chunks[0], expected_slice0, 2);

  // Slice 1: {120, 150}
  ASSERT_EQ(1, sort_chunks[1]->slice_id_);
  int64_t expected_slice1[2] = {120, 150};
  verify_chunk_sorted_data(sort_chunks[1], expected_slice1, 2);

  // Slice 2: {250, 300}
  ASSERT_EQ(2, sort_chunks[2]->slice_id_);
  int64_t expected_slice2[2] = {250, 300};
  verify_chunk_sorted_data(sort_chunks[2], expected_slice2, 2);

  sort_impl_with_slice.reset();
}

// 测试边界值的处理
TEST_F(ObStorageSortVecImplAddBatchTest, test_slice_decider_boundary_values)
{
  ObTestStorageCompareWithSlice slice_compare;
  slice_compare.init_for_slice(&row_meta_);

  // 创建 range: [100, 200]
  ObPxTabletRange tablet_range;
  tablet_range.tablet_id_ = 1;

  int64_t boundaries[2] = {100, 200};
  ObStorageDatum datum1;
  datum1.reuse();
  datum1.set_int(boundaries[0]);

  ObStorageDatum datum2;
  datum2.reuse();
  datum2.set_int(boundaries[1]);

  ObPxTabletRange::DatumKey datum_key;
  ASSERT_EQ(OB_SUCCESS, datum_key.push_back(datum1));
  ASSERT_EQ(OB_SUCCESS, tablet_range.range_cut_.push_back(datum_key));
  datum_key.reuse();
  ASSERT_EQ(OB_SUCCESS, datum_key.push_back(datum2));
  ASSERT_EQ(OB_SUCCESS, tablet_range.range_cut_.push_back(datum_key));

  ObSortChunkSliceDecider<ObTestStorageCompareWithSlice, ObTestStorageRow> slice_decider;
  ASSERT_EQ(OB_SUCCESS, slice_decider.init(&tablet_range, &slice_compare));

  ObStorageVecSortImpl<ObTestStorageCompareWithSlice, ObTestStorageRow, false> sort_impl_with_slice(ROOT_CONTEXT, monitor_node_);

  ObMemAttr mem_attr(OB_SYS_TENANT_ID, "SSVSlice2", ObCtxIds::DEFAULT_CTX_ID);
  ASSERT_EQ(OB_SUCCESS,
            sort_impl_with_slice.init(&slice_compare,
                                      &row_meta_,
                                      nullptr,
                                      1024,
                                      mem_attr,
                                      ObCompressorType::NONE_COMPRESSOR,
                                      false,
                                      1,
                                      OB_SYS_TENANT_ID,
                                      false,
                                      &slice_decider));


  // 测试边界值：99(slice0), 100(slice1), 199(slice1), 200(slice2), 201(slice2)
  {
    int64_t data_values[5] = {99, 100, 199, 200, 201};
    MockBatchDataIVector batch_vec(data_values, 5);

    ObFixedArray<ObIVector *, ObIAllocator> sk_vec_ptrs(alloc_);
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.init(1));
    ASSERT_EQ(OB_SUCCESS, sk_vec_ptrs.push_back(&batch_vec));

    uint16_t selector[5] = {0, 1, 2, 3, 4};
    ObTestStorageRow *sk_rows[5] = {nullptr};
    int64_t inmem_row_size = 0;

    ASSERT_EQ(OB_SUCCESS,
              sort_impl_with_slice.add_batch(sk_vec_ptrs,
                                             nullptr,
                                             selector,
                                             5,
                                             sk_rows,
                                             nullptr,
                                             inmem_row_size));
  }

  ASSERT_EQ(OB_SUCCESS, sort_impl_with_slice.do_dump());

  using SortVecOpChunkType = ObSortVecOpChunk<ObTestStorageRow, false>;
  common::ObArray<SortVecOpChunkType *> sort_chunks;
  ASSERT_EQ(OB_SUCCESS, sort_impl_with_slice.get_sort_chunks(sort_chunks));

  ASSERT_EQ(3, sort_chunks.count());

  // Slice 0: {99}
  ASSERT_EQ(0, sort_chunks[0]->slice_id_);
  int64_t expected_s0[1] = {99};
  verify_chunk_sorted_data(sort_chunks[0], expected_s0, 1);

  // Slice 1: {199, 200}
  ASSERT_EQ(1, sort_chunks[1]->slice_id_);
  int64_t expected_s1[2] = {100, 199};
  verify_chunk_sorted_data(sort_chunks[1], expected_s1, 2);

  // Slice 2: {201}
  ASSERT_EQ(2, sort_chunks[2]->slice_id_);
  int64_t expected_s2[2] = {200,201};
  verify_chunk_sorted_data(sort_chunks[2], expected_s2, 2);

  sort_impl_with_slice.reset();
}

}
}

int main(int argc, char **argv)
{
  system("rm -f test_storage_sort_vec_impl.log*");
  oceanbase::unittest::init_log_and_gtest(argc, argv);
  OB_LOGGER.set_log_level("INFO");
  OB_LOGGER.set_file_name("test_storage_sort_vec_impl.log", true, true);
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
