/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gmock/gmock.h>
#include <cstdlib>
#include <type_traits>

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public

#include "mtlenv/mock_tenant_module_env.h"
#include "storage/blocksstable/ob_storage_datum.h"
#include "storage/compaction/vectorization/ob_compaction_vector.h"

namespace oceanbase
{
using namespace common;
using namespace compaction;
using namespace blocksstable;

namespace unittest
{

static_assert(!std::is_copy_constructible<ObCompactionGrowableBuffer>::value,
              "growable buffer ownership must not be copyable");
static_assert(!std::is_copy_assignable<ObCompactionGrowableBuffer>::value,
              "growable buffer ownership must not be assignable");

class TestCompactionVector : public ::testing::Test
{
public:
  static void SetUpTestCase()
  {
    ASSERT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());
  }

  static void TearDownTestCase()
  {
    MockTenantModuleEnv::get_instance().destroy();
  }

  static void destroy_vector(ObCompactionVector *&vector, ObIAllocator &allocator)
  {
    if (nullptr != vector) {
      vector->~ObCompactionVector();
      allocator.free(vector);
      vector = nullptr;
    }
  }
};

class FailingAllocator : public ObIAllocator
{
public:
  explicit FailingAllocator(const int64_t fail_at)
    : fail_at_(fail_at),
      alloc_count_(0),
      outstanding_count_(0)
  {}

  virtual void *alloc(const int64_t size) override
  {
    void *ptr = nullptr;
    ++alloc_count_;
    if (alloc_count_ != fail_at_ && size > 0) {
      ptr = std::malloc(size);
      if (nullptr != ptr) {
        ++outstanding_count_;
      }
    }
    return ptr;
  }

  virtual void *alloc(const int64_t size, const ObMemAttr &attr) override
  {
    UNUSED(attr);
    return alloc(size);
  }

  virtual void free(void *ptr) override
  {
    if (nullptr != ptr) {
      std::free(ptr);
      --outstanding_count_;
    }
  }

  int64_t outstanding_count() const { return outstanding_count_; }

private:
  int64_t fail_at_;
  int64_t alloc_count_;
  int64_t outstanding_count_;
};

TEST_F(TestCompactionVector, growable_buffer_is_noncopyable_and_has_empty_sentinel)
{
  ObCompactionGrowableBuffer buffer(MTL_ID(), "CompVecTest", 16);
  char *dst = nullptr;
  int64_t expand_delta = 0;

  EXPECT_EQ(OB_SUCCESS, buffer.append_copy(nullptr, 0, dst));
  EXPECT_NE(nullptr, dst);
  EXPECT_EQ(0, buffer.size());
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.append_copy(nullptr, -1, dst));
  EXPECT_EQ(OB_INVALID_ARGUMENT, buffer.ensure(-1, expand_delta));
}

TEST_F(TestCompactionVector, continuous_empty_null_and_payload)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_CONTINUOUS, VEC_TC_STRING, 4, allocator, vector));
  ASSERT_NE(nullptr, vector);

  ObContinuousBase *continuous = static_cast<ObContinuousBase *>(vector->get_vector());
  ASSERT_NE(nullptr, continuous->get_data());

  ObStorageDatum empty;
  empty.reuse();
  ASSERT_FALSE(empty.is_null());
  ASSERT_EQ(OB_SUCCESS, vector->append_datum(0, empty));

  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(0, result));
  EXPECT_FALSE(result.is_null());
  EXPECT_EQ(0, result.len_);
  EXPECT_NE(nullptr, result.ptr_);
  EXPECT_NE(nullptr, continuous->get_payload(0));

  ObStorageDatum null_datum;
  null_datum.set_null();
  ASSERT_EQ(OB_SUCCESS, vector->append_datum(1, null_datum));
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(1, result));
  EXPECT_TRUE(result.is_null());

  const char payload[] = "abc";
  ObStorageDatum string_datum;
  ASSERT_EQ(OB_SUCCESS, string_datum.from_buf_enhance(payload, 3));
  ASSERT_EQ(OB_SUCCESS, vector->append_datum(2, string_datum));
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(2, result));
  ASSERT_EQ(3, result.len_);
  ASSERT_NE(nullptr, result.ptr_);
  EXPECT_EQ(0, MEMCMP(payload, result.ptr_, 3));

  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, continuous_checks_bounds_order_and_reuse_capacity)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_CONTINUOUS, VEC_TC_STRING, 2, allocator, vector));

  ObStorageDatum empty;
  empty.reuse();
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(1, empty));
  EXPECT_EQ(OB_SUCCESS, vector->append_datum(0, empty));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(0, empty));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(2, empty));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_null(2));

  ObDatum result;
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->get_datum(-1, result));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->get_datum(2, result));

  // An oversized reuse is a release-mode no-op, so the next valid append
  // remains index 1 rather than resetting or writing out of bounds.
  vector->reuse(3);
  EXPECT_EQ(OB_SUCCESS, vector->append_datum(1, empty));

  vector->reuse(2);
  EXPECT_EQ(OB_SUCCESS, vector->append_datum(0, empty));

  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, continuous_rejects_uint32_offset_overflow)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *base_vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_CONTINUOUS, VEC_TC_STRING, 1, allocator, base_vector));
  ObCompactionContinuousVector *vector =
      static_cast<ObCompactionContinuousVector *>(base_vector);

  vector->buffer_.size_ = UINT32_MAX;
  vector->offsets_[0] = UINT32_MAX;
  const char value = 'x';
  ObStorageDatum datum;
  ASSERT_EQ(OB_SUCCESS, datum.from_buf_enhance(&value, 1));
  EXPECT_EQ(OB_SIZE_OVERFLOW, vector->append_datum(0, datum));

  // Restore the white-box state before destruction.
  vector->buffer_.size_ = 0;
  vector->offsets_[0] = 0;
  destroy_vector(base_vector, allocator);
}

TEST_F(TestCompactionVector, fixed_vector_rejects_short_and_extended_datums)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_FIXED, VEC_TC_INTEGER, 2, allocator, vector));

  ObStorageDatum integer;
  integer.reuse();
  integer.set_int(42);
  ASSERT_EQ(OB_SUCCESS, vector->append_datum(0, integer));

  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(0, result));
  ASSERT_EQ(sizeof(int64_t), result.len_);
  EXPECT_EQ(42, result.get_int());

  const char short_value = 1;
  ObStorageDatum short_datum;
  ASSERT_EQ(OB_SUCCESS, short_datum.from_buf_enhance(&short_value, 1));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(1, short_datum));

  ObStorageDatum nop;
  nop.set_nop();
  EXPECT_TRUE(nop.is_ext());
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(1, nop));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(2, integer));

  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, fixed_interval_ds_accepts_logical_twelve_byte_datum)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_FIXED, VEC_TC_INTERVAL_DS, 1, allocator, vector));

  const ObIntervalDSValue value(123456, 789);
  ObStorageDatum datum;
  datum.reuse();
  datum.set_interval_ds(value);
  ASSERT_EQ(ObIntervalDSValue::get_store_size(), datum.len_);
  ASSERT_EQ(OB_SUCCESS, vector->append_datum(0, datum));

  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(0, result));
  ASSERT_EQ(sizeof(ObIntervalDSValue), result.len_);
  const ObIntervalDSValue &actual =
      *reinterpret_cast<const ObIntervalDSValue *>(result.ptr_);
  EXPECT_EQ(value.get_nsecond(), actual.get_nsecond());
  EXPECT_EQ(value.get_fs(), actual.get_fs());

  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, discrete_empty_datum_has_nonnull_payload)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_DISCRETE, VEC_TC_STRING, 1, allocator, vector));

  ObStorageDatum empty;
  empty.reuse();
  ASSERT_EQ(OB_SUCCESS, vector->append_datum(0, empty));
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(0, result));
  EXPECT_FALSE(result.is_null());
  EXPECT_EQ(0, result.len_);
  EXPECT_NE(nullptr, result.ptr_);

  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, discrete_force_deep_copy_owns_nonlocal_payload)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_DISCRETE, VEC_TC_STRING, 1, allocator, vector));

  char source[] = "decoder";
  ObStorageDatum datum;
  ASSERT_EQ(OB_SUCCESS, datum.from_buf_enhance(source, sizeof(source) - 1));
  ASSERT_FALSE(datum.is_local_buf());
  ASSERT_EQ(OB_SUCCESS, vector->append_datum_deep_copy(0, datum));

  MEMSET(source, 'x', sizeof(source) - 1);
  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(0, result));
  ASSERT_EQ(sizeof(source) - 1, result.len_);
  EXPECT_EQ(0, MEMCMP("decoder", result.ptr_, result.len_));

  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, variable_vectors_reject_extended_datums)
{
  ObArenaAllocator allocator("CompVecTest");
  ObStorageDatum nop;
  nop.set_nop();
  ASSERT_TRUE(nop.is_ext());

  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_DISCRETE, VEC_TC_STRING, 1, allocator, vector));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(0, nop));
  destroy_vector(vector, allocator);

  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_CONTINUOUS, VEC_TC_STRING, 1, allocator, vector));
  EXPECT_EQ(OB_INVALID_ARGUMENT, vector->append_datum(0, nop));
  ObStorageDatum empty;
  empty.reuse();
  EXPECT_EQ(OB_SUCCESS, vector->append_datum(0, empty));
  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, discrete_expansion_repairs_out_of_order_owned_pointer)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = nullptr;
  ASSERT_EQ(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_DISCRETE, VEC_TC_STRING, 3, allocator, vector));

  char first[800];
  char second[800];
  MEMSET(first, 'A', sizeof(first));
  MEMSET(second, 'B', sizeof(second));
  ObStorageDatum first_datum;
  ObStorageDatum second_datum;
  ASSERT_EQ(OB_SUCCESS, first_datum.from_buf_enhance(first, sizeof(first)));
  ASSERT_EQ(OB_SUCCESS, second_datum.from_buf_enhance(second, sizeof(second)));
  ASSERT_EQ(OB_SUCCESS, vector->append_datum_deep_copy(2, first_datum));
  ASSERT_EQ(OB_SUCCESS, vector->append_datum_deep_copy(0, second_datum));

  ObDatum result;
  ASSERT_EQ(OB_SUCCESS, vector->get_datum(2, result));
  ASSERT_EQ(sizeof(first), result.len_);
  for (int64_t i = 0; i < result.len_; ++i) {
    ASSERT_EQ('A', result.ptr_[i]);
  }
  destroy_vector(vector, allocator);
}

TEST_F(TestCompactionVector, failed_factory_leaves_output_null)
{
  ObArenaAllocator allocator("CompVecTest");
  ObCompactionVector *vector = reinterpret_cast<ObCompactionVector *>(0x1);

  EXPECT_NE(OB_SUCCESS,
            ObCompactionVector::create_vector(
                VEC_DISCRETE, VEC_TC_INTEGER, 2, allocator, vector));
  EXPECT_EQ(nullptr, vector);

  vector = reinterpret_cast<ObCompactionVector *>(0x1);
  EXPECT_EQ(OB_INVALID_ARGUMENT,
            ObCompactionVector::create_vector(
                VEC_CONTINUOUS, VEC_TC_STRING, 0, allocator, vector));
  EXPECT_EQ(nullptr, vector);

  EXPECT_EQ(OB_SIZE_OVERFLOW,
            ObCompactionVector::create_vector(
                VEC_CONTINUOUS, VEC_TC_STRING, INT64_MAX, allocator, vector));
  EXPECT_EQ(nullptr, vector);
}

TEST_F(TestCompactionVector, failed_factory_releases_partial_vector_allocations)
{
  // Allocation order: wrapper, null bitmap, lengths, pointers.  Fail the
  // pointer allocation after the first three allocations have succeeded.
  FailingAllocator allocator(4);
  ObCompactionVector *vector = nullptr;
  EXPECT_EQ(OB_ALLOCATE_MEMORY_FAILED,
            ObCompactionVector::create_vector(
                VEC_DISCRETE, VEC_TC_STRING, 2, allocator, vector));
  EXPECT_EQ(nullptr, vector);
  EXPECT_EQ(0, allocator.outstanding_count());
}

} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_compaction_vector.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
