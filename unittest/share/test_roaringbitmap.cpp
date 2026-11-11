/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <cerrno>
#include <memory>
#include <set>
#include <string>
#include <vector>
#include <sys/mman.h>
#include <unistd.h>
#define private public
#include "lib/roaringbitmap/ob_rb_utils.h"

#undef private

// CRoaring 内部头文件会取消 ROARING_CONTAINER_T，须在公开 API 头文件之后包含。
#include "roaring/roaring_array.h"


namespace oceanbase {
namespace common {

class TestRoaringBitmap : public ::testing::Test {
public:
  TestRoaringBitmap()
  {}
  ~TestRoaringBitmap()
  {}
  virtual void SetUp()
  {}
  virtual void TearDown()
  {}

  static void SetUpTestCase()
  {}

  static void TearDownTestCase()
  {}

private:
  // disallow copy
  DISALLOW_COPY_AND_ASSIGN(TestRoaringBitmap);

};


TEST_F(TestRoaringBitmap, serialize_deserialize)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ModulePageAllocator page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
  ObRoaringBitmap *rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator));
  ObRoaringBitmap *rb_64 = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator));

  // EMPTY Type
  ASSERT_EQ(rb->get_version(), BITMAP_VESION_1);
  ASSERT_EQ(rb->get_type(), ObRbType::EMPTY);
  ASSERT_EQ(rb->get_cardinality(), 0);
  ObString bin_empty;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_empty, rb));
  ASSERT_EQ(ObRbBinType::EMPTY, static_cast<ObRbBinType>(*(bin_empty.ptr() + 1)));
  ObRoaringBitmap *rb_empty;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, bin_empty, rb_empty));
  ASSERT_EQ(rb->get_version(), rb_empty->get_version());
  ASSERT_EQ(rb->get_type(), rb_empty->get_type());
  ASSERT_EQ(rb->get_cardinality(), rb_empty->get_cardinality());

  // SINGLE_32 Type
  ASSERT_EQ(OB_SUCCESS, rb->value_add(100));
  ASSERT_EQ(rb->get_type(), ObRbType::SINGLE);
  ASSERT_EQ(rb->get_cardinality(), 1);
  ASSERT_TRUE(rb->is_contains(100));
  ObString bin_single;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_single, rb));
  ASSERT_EQ(ObRbBinType::SINGLE_32, static_cast<ObRbBinType>(*(bin_single.ptr() + 1)));
  ObRoaringBitmap *rb_single;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, bin_single, rb_single));
  ASSERT_EQ(rb->get_version(), rb_single->get_version());
  ASSERT_EQ(rb->get_type(), rb_single->get_type());
  ASSERT_EQ(rb->get_cardinality(), rb_single->get_cardinality());
  ASSERT_TRUE(rb_single->is_contains(100));

  // SET_32 Type
  ASSERT_EQ(OB_SUCCESS, rb->value_add(200));
  ASSERT_EQ(rb->get_type(), ObRbType::SET);
  ASSERT_EQ(rb->get_cardinality(), 2);
  ASSERT_TRUE(rb->is_contains(100));
  ASSERT_TRUE(rb->is_contains(200));
  ObString bin_set;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_set, rb));
  ASSERT_EQ(ObRbBinType::SET_32, static_cast<ObRbBinType>(*(bin_set.ptr() + 1)));
  ObRoaringBitmap *rb_set;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, bin_set, rb_set));
  ASSERT_EQ(rb->get_version(), rb_set->get_version());
  ASSERT_EQ(rb->get_type(), rb_set->get_type());
  ASSERT_EQ(rb->get_cardinality(), rb_set->get_cardinality());
  ASSERT_TRUE(rb_set->is_contains(100));
  ASSERT_TRUE(rb_set->is_contains(200));

  // BITMAP_32 Type
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_EQ(OB_SUCCESS, rb->value_add(300 + i));
  }
  ASSERT_EQ(rb->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb->get_cardinality(), 34);
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_TRUE(rb->is_contains(300 + i));
  }
  ObString bin_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_bitmap, rb));
  ASSERT_EQ(ObRbBinType::BITMAP_64, static_cast<ObRbBinType>(*(bin_bitmap.ptr() + 1)));
  ObRoaringBitmap *rb_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, bin_bitmap, rb_bitmap));
  ASSERT_EQ(rb->get_version(), rb_bitmap->get_version());
  ASSERT_EQ(rb->get_type(), rb_bitmap->get_type());
  ASSERT_EQ(rb->get_cardinality(), rb_bitmap->get_cardinality());
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_TRUE(rb_bitmap->is_contains(300 + i));
  }

  // SINGLE_64 Type
  ASSERT_EQ(OB_SUCCESS, rb_64->value_add(4294967295 + 100));
  ASSERT_EQ(rb_64->get_type(), ObRbType::SINGLE);
  ASSERT_EQ(rb_64->get_cardinality(), 1);
  ASSERT_TRUE(rb_64->is_contains(4294967295 + 100));
  ObString bin_single_64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_single_64, rb_64));
  ASSERT_EQ(ObRbBinType::SINGLE_64, static_cast<ObRbBinType>(*(bin_single_64.ptr() + 1)));
  ObRoaringBitmap *rb_single_64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, bin_single_64, rb_single_64));
  ASSERT_EQ(rb_64->get_version(), rb_single_64->get_version());
  ASSERT_EQ(rb_64->get_type(), rb_single_64->get_type());
  ASSERT_EQ(rb_64->get_cardinality(), rb_single_64->get_cardinality());
  ASSERT_TRUE(rb_single_64->is_contains(4294967295 + 100));

  // SET_64 Type
  ASSERT_EQ(OB_SUCCESS, rb_64->value_add(200));
  ASSERT_EQ(rb_64->get_type(), ObRbType::SET);
  ASSERT_EQ(rb_64->get_cardinality(), 2);
  ASSERT_TRUE(rb_64->is_contains(4294967295 + 100));
  ASSERT_TRUE(rb_64->is_contains(200));
  ObString bin_set_64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_set_64, rb_64));
  ASSERT_EQ(ObRbBinType::SET_64, static_cast<ObRbBinType>(*(bin_set_64.ptr() + 1)));
  ObRoaringBitmap *rb_set_64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, bin_set_64, rb_set_64));
  ASSERT_EQ(rb_64->get_version(), rb_set_64->get_version());
  ASSERT_EQ(rb_64->get_type(), rb_set_64->get_type());
  ASSERT_EQ(rb_64->get_cardinality(), rb_set_64->get_cardinality());
  ASSERT_TRUE(rb_set_64->is_contains(4294967295 + 100));
  ASSERT_TRUE(rb_set_64->is_contains(200));

  // BITMAP_64 Type
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_EQ(OB_SUCCESS, rb_64->value_add(i * 4294967295 + i));
  }
  ASSERT_EQ(rb_64->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_64->get_cardinality(), 34);
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_TRUE(rb_64->is_contains(i * 4294967295 + i));
  }
  ObString bin_bitmap_64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_bitmap_64, rb_64));
  ASSERT_EQ(ObRbBinType::BITMAP_64, static_cast<ObRbBinType>(*(bin_bitmap_64.ptr() + 1)));
  ObRoaringBitmap *rb_bitmap_64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, bin_bitmap_64, rb_bitmap_64));
  ASSERT_EQ(rb_64->get_version(), rb_bitmap_64->get_version());
  ASSERT_EQ(rb_64->get_type(), rb_bitmap_64->get_type());
  ASSERT_EQ(rb_64->get_cardinality(), rb_bitmap_64->get_cardinality());
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_TRUE(rb_bitmap_64->is_contains(i * 4294967295 + i));
  }
}

TEST_F(TestRoaringBitmap, optimize)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ModulePageAllocator page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
  ObRoaringBitmap *rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator));

  // add 33 value, remain 33 value
  for (int i = 0; i < MAX_BITMAP_SET_VALUES + 1; i++) {
    ASSERT_EQ(OB_SUCCESS, rb->value_add(300 + i));
  }
  ASSERT_EQ(rb->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb->get_cardinality(), 33);
  // remove 32 value, remain 32 value
  ASSERT_EQ(rb->value_remove(300), OB_SUCCESS);
  ASSERT_EQ(rb->get_cardinality(), 32);
  ASSERT_FALSE(rb->is_contains(300));
  rb->optimize();
  ASSERT_EQ(rb->get_type(), ObRbType::SET);
  // remove 1 value, remain 33 value
  ASSERT_EQ(rb->value_add(300), OB_SUCCESS);
  ASSERT_EQ(rb->get_cardinality(), 33);
  ASSERT_EQ(rb->get_type(), ObRbType::BITMAP);
  // remove 32 value, remain 1 value
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_EQ(rb->value_remove(300 + i), OB_SUCCESS);
  }
  ASSERT_EQ(rb->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb->get_cardinality(), 1);
  rb->optimize();
  ASSERT_EQ(rb->get_type(), ObRbType::SINGLE);
  ASSERT_EQ(rb->get_cardinality(), 1);
  // remove 1 value, remain 0 value
  ASSERT_EQ(rb->value_remove(300 + 32), OB_SUCCESS);
  ASSERT_EQ(rb->get_type(), ObRbType::EMPTY);
  ASSERT_EQ(rb->get_cardinality(), 0);
  // add 32 value, remain 32 value
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_EQ(rb->value_add(i), OB_SUCCESS);
  }
  ASSERT_EQ(rb->get_type(), ObRbType::SET);
  ASSERT_EQ(rb->get_cardinality(), 32);
  // remove 32 value, remain 0 value
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_EQ(rb->value_remove(i), OB_SUCCESS);
  }
  ASSERT_EQ(rb->get_type(), ObRbType::SET);
  ASSERT_EQ(rb->get_cardinality(), 0);
  // add 33 value, remain 33 value
  for (int i = 0; i < MAX_BITMAP_SET_VALUES + 1; i++) {
    ASSERT_EQ(rb->value_add(300 + i), OB_SUCCESS);
  }
  ASSERT_EQ(rb->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb->get_cardinality(), 33);
  // remove 33 value, remain 0 value
  for (int i = 0; i < MAX_BITMAP_SET_VALUES + 1; i++) {
    ASSERT_EQ(rb->value_remove(300 + i), OB_SUCCESS);
  }
  ASSERT_EQ(rb->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb->get_cardinality(), 0);
  rb->optimize();
  ASSERT_EQ(rb->get_type(), ObRbType::EMPTY);
  ASSERT_EQ(rb->get_cardinality(), 0);
  // add 2 value, remain 2 value
  for (int i = 0; i < 2; i++) {
    ASSERT_EQ(rb->value_add(300 + i), OB_SUCCESS);
  }
  ASSERT_EQ(rb->get_type(), ObRbType::SET);
  ASSERT_EQ(rb->get_cardinality(), 2);
  // remove 1 value, remain 1 value
  ASSERT_EQ(rb->value_remove(300), OB_SUCCESS);
  ASSERT_EQ(rb->get_type(), ObRbType::SET);
  ASSERT_EQ(rb->get_cardinality(), 1);
  rb->optimize();
  ASSERT_EQ(rb->get_type(), ObRbType::SINGLE);
  ASSERT_EQ(rb->get_cardinality(), 1);
}

TEST_F(TestRoaringBitmap, to_roaring_bin)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ModulePageAllocator page_allocator_(allocator, common::ObModIds::OB_MODULE_PAGE_ALLOCATOR);
  ObRoaringBitmap *rb = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator));
  ObRoaringBitmap *rb64 = OB_NEWx(ObRoaringBitmap, &allocator, (&allocator));

  // EMPTY
  ObString bin_empty;
  ObString roaring_bin_empty;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_empty, rb));
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bin_empty, roaring_bin_empty));
  ObRbBinType bin_type_empty;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(roaring_bin_empty, bin_type_empty));
  ASSERT_EQ(ObRbBinType::BITMAP_32, bin_type_empty);
  ObRoaringBitmap *rb_roaring_empty;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, roaring_bin_empty, rb_roaring_empty));
  ASSERT_EQ(rb_roaring_empty->get_version(), 1);
  ASSERT_EQ(rb_roaring_empty->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_roaring_empty->get_cardinality(), 0);

  // SINGLE_32
  ASSERT_EQ(OB_SUCCESS, rb->value_add(100));
  ObString bin_single;
  ObString roaring_bin_single;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_single, rb));
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bin_single, roaring_bin_single));
  ObRbBinType bin_type_single;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(roaring_bin_single, bin_type_single));
  ASSERT_EQ(ObRbBinType::BITMAP_32, bin_type_single);
  ObRoaringBitmap *rb_roaring_single;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, roaring_bin_single, rb_roaring_single));
  ASSERT_EQ(rb_roaring_single->get_version(), 1);
  ASSERT_EQ(rb_roaring_single->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_roaring_single->get_cardinality(), 1);
  ASSERT_TRUE(rb_roaring_single->is_contains(100));

  // SET_32
  ASSERT_EQ(OB_SUCCESS, rb->value_add(101));
  ObString bin_set;
  ObString roaring_bin_set;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_set, rb));
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bin_set, roaring_bin_set));
  ObRbBinType bin_type_set;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(roaring_bin_set, bin_type_set));
  ASSERT_EQ(ObRbBinType::BITMAP_32, bin_type_set);
  ObRoaringBitmap *rb_roaring_set;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, roaring_bin_set, rb_roaring_set));
  ASSERT_EQ(rb_roaring_set->get_version(), 1);
  ASSERT_EQ(rb_roaring_set->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_roaring_set->get_cardinality(), 2);
  ASSERT_TRUE(rb_roaring_set->is_contains(100));
  ASSERT_TRUE(rb_roaring_set->is_contains(101));

  // BITMAP_32
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_EQ(OB_SUCCESS, rb->value_add(300 + i));
  }
  ObString bin_bitmap;
  ObString roaring_bin_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_bitmap, rb));
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bin_bitmap, roaring_bin_bitmap));
  ObRbBinType bin_type_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(roaring_bin_bitmap, bin_type_bitmap));
  ASSERT_EQ(ObRbBinType::BITMAP_32, bin_type_bitmap);
  ObRoaringBitmap *rb_roaring_bitmap;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, roaring_bin_bitmap, rb_roaring_bitmap));
  ASSERT_EQ(rb_roaring_bitmap->get_version(), 1);
  ASSERT_EQ(rb_roaring_bitmap->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_roaring_bitmap->get_cardinality(), 34);
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_TRUE(rb_roaring_bitmap->is_contains(300 + i));
  }

  // SINGLE_64
  ASSERT_EQ(OB_SUCCESS, rb64->value_add(4294967295 + 100));
  ObString bin_single64;
  ObString roaring_bin_single64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_single64, rb64));
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bin_single64, roaring_bin_single64));
  ObRbBinType bin_type_single64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(roaring_bin_single64, bin_type_single64));
  ASSERT_EQ(ObRbBinType::BITMAP_64, bin_type_single64);
  ObRoaringBitmap *rb_roaring_single64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, roaring_bin_single64, rb_roaring_single64));
  ASSERT_EQ(rb_roaring_single64->get_version(), 1);
  ASSERT_EQ(rb_roaring_single64->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_roaring_single64->get_cardinality(), 1);
  ASSERT_TRUE(rb_roaring_single64->is_contains(4294967295 + 100));

  // SET_64
  ASSERT_EQ(OB_SUCCESS, rb64->value_add(4294967295 + 101));
  ObString bin_set64;
  ObString roaring_bin_set64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_set64, rb64));
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bin_set64, roaring_bin_set64));
  ObRbBinType bin_type_set64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(roaring_bin_set64, bin_type_set64));
  ASSERT_EQ(ObRbBinType::BITMAP_64, bin_type_set64);
  ObRoaringBitmap *rb_roaring_set64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, roaring_bin_set64, rb_roaring_set64));
  ASSERT_EQ(rb_roaring_set64->get_version(), 1);
  ASSERT_EQ(rb_roaring_set64->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_roaring_set64->get_cardinality(), 2);
  ASSERT_TRUE(rb_roaring_set64->is_contains(4294967295 + 100));
  ASSERT_TRUE(rb_roaring_set64->is_contains(4294967295 + 101));

  // BITMAP_64
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_EQ(OB_SUCCESS, rb64->value_add(4294967295 + 300 + i));
  }
  ObString bin_bitmap64;
  ObString roaring_bin_bitmap64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_serialize(allocator, bin_bitmap64, rb64));
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bin_bitmap64, roaring_bin_bitmap64));
  ObRbBinType bin_type_bitmap64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(roaring_bin_bitmap64, bin_type_bitmap64));
  ASSERT_EQ(ObRbBinType::BITMAP_64, bin_type_bitmap64);
  ObRoaringBitmap *rb_roaring_bitmap64;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_deserialize(allocator, roaring_bin_bitmap64, rb_roaring_bitmap64));
  ASSERT_EQ(rb_roaring_bitmap64->get_version(), 1);
  ASSERT_EQ(rb_roaring_bitmap64->get_type(), ObRbType::BITMAP);
  ASSERT_EQ(rb_roaring_bitmap64->get_cardinality(), 34);
  for (int i = 0; i < MAX_BITMAP_SET_VALUES; i++) {
    ASSERT_TRUE(rb_roaring_bitmap64->is_contains(4294967295 + 300 + i));
  }

}

namespace {

using RbValues = std::vector<uint64_t>;
using RbValueSet = std::set<uint64_t>;

// ObRoaringBitmap requires explicit cleanup, including on fatal assertion exits.
class ScopedRoaringBitmap : public ObRoaringBitmap
{
public:
  explicit ScopedRoaringBitmap(ObIAllocator *allocator) : ObRoaringBitmap(allocator) {}
  ~ScopedRoaringBitmap() override { set_empty(); }
};

void make_set_erase_values(bool same_bucket, bool wide, RbValues &values)
{
  const uint64_t bucket_count = hash::cal_next_prime(MAX_BITMAP_SET_VALUES);
  const uint64_t base = wide ? (uint64_t(1) << 32) : 0;
  hash::hash_func<uint64_t> hash_func;
  RbValueSet buckets;
  values.clear();
  for (uint64_t i = 0; i < MAX_BITMAP_SET_VALUES; ++i) {
    const uint64_t value = base + i * (same_bucket ? bucket_count : 1);
    uint64_t hash_value = 0;
    ASSERT_EQ(OB_SUCCESS, hash_func(value, hash_value));
    buckets.insert(hash_value % bucket_count);
    values.push_back(value);
  }
  ASSERT_EQ(same_bucket ? 1U : values.size(), buckets.size());
}

void build_left_set(ObRoaringBitmap &rb, const RbValues &values, size_t count)
{
  ASSERT_LE(count, values.size());
  ASSERT_GE(values.size(), 2U);
  const size_t seed_count = count < 2 ? 2 : count;
  for (size_t i = 0; i < seed_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, rb.value_add(values[i]));
  }
  // Keep the SET representation for the zero- and one-element cases.
  for (size_t i = count; i < seed_count; ++i) {
    ASSERT_EQ(OB_SUCCESS, rb.value_remove(values[i]));
  }
  ASSERT_EQ(ObRbType::SET, rb.get_type());
  ASSERT_EQ(count, rb.get_cardinality());
}

RbValues set_iteration_order(ObRoaringBitmap &rb)
{
  RbValues order;
  for (auto iter = rb.get_set()->begin(); iter != rb.get_set()->end(); ++iter) {
    order.push_back(iter->first);
  }
  return order;
}

RbValueSet retained_values(const RbValues &order, int pattern)
{
  RbValueSet retained;
  for (size_t i = 0; i < order.size(); ++i) {
    // None, all, first, middle, last, and alternating nodes in traversal order.
    if (pattern == 1 || (pattern == 2 && i == 0)
        || (pattern == 3 && i == order.size() / 2)
        || (pattern == 4 && i + 1 == order.size())
        || (pattern == 5 && i % 2 == 0)) {
      retained.insert(order[i]);
    }
  }
  return retained;
}

RbValueSet expected_set_result(const RbValues &left, const RbValueSet &right, bool andnot)
{
  RbValueSet expected;
  for (uint64_t value : left) {
    if ((right.count(value) != 0) != andnot) {
      expected.insert(value);
    }
  }
  return expected;
}

void expect_members(ObRoaringBitmap &rb, const RbValues &probes, const RbValueSet &expected)
{
  EXPECT_EQ(expected.size(), rb.get_cardinality());
  for (uint64_t value : expected) {
    EXPECT_TRUE(rb.is_contains(value)) << value;
  }
  for (uint64_t value : probes) {
    EXPECT_EQ(expected.count(value) != 0, rb.is_contains(value)) << value;
  }
}

void check_object_set_erase(bool andnot)
{
  const size_t counts[] = {0, 1, 2, MAX_BITMAP_SET_VALUES};
  const ObRbType right_types[] = {ObRbType::EMPTY, ObRbType::SINGLE,
                                ObRbType::SET, ObRbType::BITMAP};
  for (int round = 0; round < 3; ++round) {
    for (bool same_bucket : {false, true}) {
      for (bool wide : {false, true}) {
        RbValues values;
        ASSERT_NO_FATAL_FAILURE(make_set_erase_values(same_bucket, wide, values));
        for (size_t count : counts) {
          for (int pattern = 0; pattern < 6; ++pattern) {
            for (ObRbType right_type : right_types) {
              if (andnot && right_type != ObRbType::BITMAP) {
                continue;
              }
              SCOPED_TRACE(::testing::Message() << "round=" << round << " same_bucket=" << same_bucket
                  << " wide=" << wide << " count=" << count << " pattern=" << pattern
                  << " right_type=" << static_cast<int>(right_type) << " andnot=" << andnot);
              ObArenaAllocator allocator(ObModIds::TEST);
              ScopedRoaringBitmap left(&allocator);
              ScopedRoaringBitmap right(&allocator);
              ASSERT_NO_FATAL_FAILURE(build_left_set(left, values, count));
              const RbValues original(values.begin(), values.begin() + count);
              const RbValueSet retained = retained_values(set_iteration_order(left), pattern);
              RbValueSet right_values = andnot ? expected_set_result(original, retained, true) : retained;
              if ((right_type == ObRbType::EMPTY && !right_values.empty())
                  || (right_type == ObRbType::SINGLE && right_values.size() > 1)) {
                continue;
              }
              const size_t min_count = right_type == ObRbType::EMPTY ? 0
                  : right_type == ObRbType::SINGLE ? 1 : right_type == ObRbType::SET ? 2
                  : MAX_BITMAP_SET_VALUES + 1;
              // Disjoint padding controls B's representation without changing A's result.
              for (uint64_t filler = values.back() + 1; right_values.size() < min_count; ++filler) {
                right_values.insert(filler);
              }
              for (uint64_t value : right_values) {
                ASSERT_EQ(OB_SUCCESS, right.value_add(value));
              }
              ASSERT_EQ(right_type, right.get_type());
              const RbValueSet expected = expected_set_result(original, right_values, andnot);
              ASSERT_EQ(retained, expected);
              if (andnot) {
                ASSERT_EQ(OB_SUCCESS, left.value_andnot(&right));
              } else {
                ASSERT_EQ(OB_SUCCESS, left.value_and(&right));
              }
              EXPECT_EQ(ObRbType::SET, left.get_type());
              expect_members(left, values, expected);
              EXPECT_EQ(right_type, right.get_type());
              expect_members(right, values, right_values);
            }
          }
        }
      }
    }
  }
}

void make_bitmap_binary(ObArenaAllocator &allocator, const RbValueSet &values,
                        bool wide, ObString &binary)
{
  ScopedRoaringBitmap right(&allocator);
  for (uint64_t value : values) {
    ASSERT_EQ(OB_SUCCESS, right.value_add(value));
  }
  // Serialize directly: rb_serialize would optimize small bitmaps back to SET.
  ASSERT_EQ(OB_SUCCESS, right.convert_to_bitmap());
  ObStringBuffer buffer(&allocator);
  ASSERT_EQ(OB_SUCCESS, right.serialize(buffer));
  ObString bitmap64;
  ASSERT_EQ(OB_SUCCESS, buffer.get_result_string(bitmap64));
  if (wide) {
    binary = bitmap64;
  } else {
    ASSERT_EQ(OB_SUCCESS, ObRbUtils::binary_format_convert(allocator, bitmap64, binary));
  }
  ObRbBinType type;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(binary, type));
  ASSERT_EQ(wide ? ObRbBinType::BITMAP_64 : ObRbBinType::BITMAP_32, type);
}

void check_binary_set_erase(const RbValues &values, size_t count,
                           const RbValueSet &right_values, bool wide)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ScopedRoaringBitmap left(&allocator);
  ASSERT_NO_FATAL_FAILURE(build_left_set(left, values, count));
  const RbValues original(values.begin(), values.begin() + count);
  const RbValueSet expected = expected_set_result(original, right_values, false);
  ObString binary;
  ASSERT_NO_FATAL_FAILURE(make_bitmap_binary(allocator, right_values, wide, binary));
  const std::string snapshot(binary.ptr(), binary.length());
  const uint32_t header_size = RB_VERSION_SIZE + RB_BIN_TYPE_SIZE;
  ObString payload(binary.length() - header_size, binary.ptr() + header_size);

  ObString left_binary;
  if (count >= 2) {
    ObStringBuffer buffer(&allocator);
    ASSERT_EQ(OB_SUCCESS, left.serialize(buffer));
    ASSERT_EQ(OB_SUCCESS, buffer.get_result_string(left_binary));
    ObRbBinType type;
    ASSERT_EQ(OB_SUCCESS, ObRbUtils::get_bin_type(left_binary, type));
    ASSERT_EQ(left.get_max() <= UINT32_MAX ? ObRbBinType::SET_32 : ObRbBinType::SET_64, type);
  }
  if (wide) {
    ObRoaring64Bin right(&allocator, payload);
    ASSERT_EQ(OB_SUCCESS, right.init());
    ASSERT_EQ(OB_SUCCESS, left.value_and(&right));
  } else {
    ObRoaringBin right(&allocator, payload);
    ASSERT_EQ(OB_SUCCESS, right.init());
    ASSERT_EQ(OB_SUCCESS, left.value_and(&right));
  }
  EXPECT_EQ(ObRbType::SET, left.get_type());
  expect_members(left, values, expected);
  ASSERT_EQ(snapshot, std::string(binary.ptr(), binary.length()));

  if (count >= 2) {
    // Deterministically enter the SET + BITMAP_32/64 aggregate fast path.
    ScopedRoaringBitmap aggregate_rb(&allocator);
    ObRbAggCell cell(&aggregate_rb, OB_SERVER_TENANT_ID);
    ASSERT_EQ(OB_SUCCESS, cell.value_calc(left_binary, T_FUN_SYS_RB_AND_AGG));
    ASSERT_FALSE(cell.is_new_);
    ASSERT_EQ(ObRbType::SET, aggregate_rb.get_type());
    expect_members(aggregate_rb, values, RbValueSet(original.begin(), original.end()));
    ASSERT_EQ(OB_SUCCESS, cell.value_calc(binary, T_FUN_SYS_RB_AND_AGG));
    expect_members(aggregate_rb, values, expected);
    ASSERT_EQ(OB_SUCCESS, cell.destroy());
    ASSERT_EQ(snapshot, std::string(binary.ptr(), binary.length()));
  }
  ScopedRoaringBitmap right_after(&allocator);
  ASSERT_EQ(OB_SUCCESS, right_after.deserialize(binary));
  expect_members(right_after, values, right_values);
}

void check_binary_set_erase_matrix(bool wide)
{
  const size_t counts[] = {0, 1, 2, MAX_BITMAP_SET_VALUES};
  for (int round = 0; round < 3; ++round) {
    for (bool same_bucket : {false, true}) {
      RbValues values;
      ASSERT_NO_FATAL_FAILURE(make_set_erase_values(same_bucket, wide, values));
      for (size_t count : counts) {
        ObArenaAllocator allocator(ObModIds::TEST);
        ScopedRoaringBitmap left(&allocator);
        ASSERT_NO_FATAL_FAILURE(build_left_set(left, values, count));
        const RbValues order = set_iteration_order(left);
        for (int pattern = 0; pattern < 6; ++pattern) {
          SCOPED_TRACE(::testing::Message() << "round=" << round << " same_bucket=" << same_bucket
              << " wide=" << wide << " count=" << count << " pattern=" << pattern);
          const RbValueSet right_values = retained_values(order, pattern);
          ASSERT_NO_FATAL_FAILURE(check_binary_set_erase(values, count, right_values, wide));
        }
      }
    }
  }
}

} // namespace

TEST_F(TestRoaringBitmap, set_and_object_erase)
{
  ASSERT_NO_FATAL_FAILURE(check_object_set_erase(false));
}

TEST_F(TestRoaringBitmap, set_andnot_bitmap_erase)
{
  ASSERT_NO_FATAL_FAILURE(check_object_set_erase(true));
}

TEST_F(TestRoaringBitmap, set_and_binary32_erase)
{
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase_matrix(false));
  const RbValues values = {1, 2, UINT32_MAX, uint64_t(1) << 32, UINT64_MAX};
  // Both high values would falsely match B if narrowed before the range check.
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase(values, values.size(), {0, 2, UINT32_MAX}, false));
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase(values, values.size(), {0, 1, 2, UINT32_MAX}, false));
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase(values, values.size(), {3, 4}, false));
}

TEST_F(TestRoaringBitmap, set_and_binary64_erase)
{
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase_matrix(true));
  const RbValues values = {1, 2, uint64_t(1) << 32, UINT64_MAX};
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase(values, values.size(), {2, UINT64_MAX}, true));
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase(values, values.size(),
                                               RbValueSet(values.begin(), values.end()), true));
  ASSERT_NO_FATAL_FAILURE(check_binary_set_erase(values, values.size(), {3, 4}, true));
}

TEST_F(TestRoaringBitmap, set_and_uninitialized_binary)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const RbValues values = {1, 2};
  ObString payload;
  ObRoaringBin bin32(&allocator, payload);
  ObRoaring64Bin bin64(&allocator, payload);
  ScopedRoaringBitmap left32(&allocator);
  ScopedRoaringBitmap left64(&allocator);
  ASSERT_NO_FATAL_FAILURE(build_left_set(left32, values, values.size()));
  ASSERT_NO_FATAL_FAILURE(build_left_set(left64, values, values.size()));
  ASSERT_FALSE(bin32.is_inited());
  ASSERT_FALSE(bin64.is_inited());
  ASSERT_EQ(OB_NOT_INIT, left32.value_and(&bin32));
  ASSERT_EQ(OB_NOT_INIT, left64.value_and(&bin64));
  EXPECT_EQ(ObRbType::SET, left32.get_type());
  EXPECT_EQ(ObRbType::SET, left64.get_type());
  expect_members(left32, values, {1, 2});
  expect_members(left64, values, {1, 2});
}

namespace {

// Keep malformed inputs raw: build_binary() would reject or normalize them
// before the reader under test sees them.
template <typename T>
void append_rb_value(std::string &binary, T value)
{
  binary.append(reinterpret_cast<const char *>(&value), sizeof(value));
}

std::string raw_rb_header(ObRbBinType type)
{
  std::string binary;
  append_rb_value(binary, BITMAP_VESION_1);
  append_rb_value(binary, static_cast<uint8_t>(type));
  return binary;
}

void expect_rb_to_string_error(const std::string &binary, int expected_error = OB_INVALID_ARGUMENT)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  ObString result;
  EXPECT_EQ(expected_error, ObRbUtils::rb_to_string(allocator, ObString(binary.size(), binary.data()), result));
  EXPECT_EQ(0, result.length());
  EXPECT_EQ(nullptr, result.ptr());
}

void expect_rb_to_string_value(const std::string &binary, const std::string &expected)
{
  ObArenaAllocator allocator(ObModIds::TEST);
  const std::string original(binary);
  ObString result;
  ASSERT_EQ(OB_SUCCESS, ObRbUtils::rb_to_string(allocator, ObString(binary.size(), binary.data()), result));
  ASSERT_EQ(expected.size(), static_cast<size_t>(result.length()));
  if (!expected.empty()) {
    ASSERT_NE(nullptr, result.ptr());
    EXPECT_EQ(0, MEMCMP(expected.data(), result.ptr(), expected.size()));
  }
  EXPECT_EQ(original, binary);
}

std::string rb_range_text(uint64_t first, uint32_t count)
{
  std::string text;
  for (uint32_t i = 0; i < count; ++i) {
    if (i > 0) {
      text += ',';
    }
    text += std::to_string(first + i);
  }
  return text;
}

// Serialize a real portable bitmap without build_binary() or optimization to
// SINGLE/SET. A roaring64 entry is [map size][high32][roaring32 portable data].
void make_raw_bitmap(bool is_64, uint32_t count, std::string &binary)
{
  std::unique_ptr<roaring::api::roaring_bitmap_t, decltype(&roaring::api::roaring_bitmap_free)> bitmap(
      roaring::api::roaring_bitmap_create_with_capacity(0), roaring::api::roaring_bitmap_free);
  ASSERT_NE(nullptr, bitmap.get());
  for (uint32_t i = 0; i < count; ++i) {
    roaring::api::roaring_bitmap_add(bitmap.get(), i);
  }
  ASSERT_EQ(count, roaring::api::roaring_bitmap_get_cardinality(bitmap.get()));
  binary = raw_rb_header(is_64 ? ObRbBinType::BITMAP_64 : ObRbBinType::BITMAP_32);
  if (is_64) {
    append_rb_value(binary, uint64_t(1)); // one high32 entry
    append_rb_value(binary, uint32_t(1)); // values start at 4294967296
  }
  const size_t offset = binary.size();
  const size_t size = roaring::api::roaring_bitmap_portable_size_in_bytes(bitmap.get());
  binary.resize(offset + size);
  ASSERT_EQ(size, roaring::api::roaring_bitmap_portable_serialize(bitmap.get(), &binary[offset]));
}

// Put the end of the input immediately before an inaccessible page. The first
// page is made read-only as well, so the reader cannot modify its source.
class GuardedRbBuffer
{
public:
  GuardedRbBuffer() : mapping_(MAP_FAILED), mapping_size_(0), data_(nullptr) {}
  ~GuardedRbBuffer()
  {
    if (MAP_FAILED != mapping_) {
      munmap(mapping_, mapping_size_);
    }
  }

  bool init(const std::string &binary)
  {
    const long page_size = sysconf(_SC_PAGESIZE);
    bool ready = false;
    if (page_size > 0 && binary.size() <= static_cast<size_t>(page_size)) {
      mapping_size_ = 2 * static_cast<size_t>(page_size);
      mapping_ = mmap(nullptr, mapping_size_, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
      if (MAP_FAILED != mapping_) {
        char *guard = static_cast<char *>(mapping_) + page_size;
        if (0 == mprotect(guard, page_size, PROT_NONE)) {
          char *data = guard - binary.size();
          MEMCPY(data, binary.data(), binary.size());
          if (0 == mprotect(mapping_, page_size, PROT_READ)) {
            data_ = data;
            ready = true;
          }
        }
      }
    }
    return ready;
  }

  const char *data() const { return data_; }

private:
  void *mapping_;
  size_t mapping_size_;
  const char *data_;
  DISALLOW_COPY_AND_ASSIGN(GuardedRbBuffer);
};

} // namespace

TEST_F(TestRoaringBitmap, to_string_invalid_headers)
{
  const std::string inputs[] = {
      std::string(), std::string("\x01", 1), std::string("\x00\x00", 2),
      std::string("\x02\x00", 2), std::string("\x01\x07", 2), std::string("\x01\x80", 2),
      std::string("\x01\xFF", 2), std::string("\x01\x00\x00", 3)};
  for (size_t i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
    SCOPED_TRACE(i);
    expect_rb_to_string_error(inputs[i]);
  }
}

TEST_F(TestRoaringBitmap, to_string_invalid_single_lengths)
{
  const ObRbBinType types[] = {ObRbBinType::SINGLE_32, ObRbBinType::SINGLE_64};
  for (ObRbBinType type : types) {
    SCOPED_TRACE(static_cast<int>(type));
    const size_t valid_length = 2 + (type == ObRbBinType::SINGLE_32 ? sizeof(uint32_t) : sizeof(uint64_t));
    for (size_t length = 2; length <= valid_length + 1; ++length) {
      SCOPED_TRACE(length);
      if (length != valid_length) {
        std::string binary = raw_rb_header(type);
        binary.resize(length, '\0');
        expect_rb_to_string_error(binary);
      }
    }
  }
}

TEST_F(TestRoaringBitmap, to_string_invalid_set_layouts)
{
  const ObRbBinType types[] = {ObRbBinType::SET_32, ObRbBinType::SET_64};
  const uint8_t valid_counts[] = {2, 32};
  const uint8_t invalid_counts[] = {0, 1, 33, 255};
  for (ObRbBinType type : types) {
    SCOPED_TRACE(static_cast<int>(type));
    const size_t width = type == ObRbBinType::SET_32 ? sizeof(uint32_t) : sizeof(uint64_t);
    expect_rb_to_string_error(raw_rb_header(type)); // missing count
    for (uint8_t count : valid_counts) {
      SCOPED_TRACE(static_cast<int>(count));
      const size_t lengths[] = {0, count * width - 1, count * width + 1};
      for (size_t length : lengths) {
        SCOPED_TRACE(length);
        std::string binary = raw_rb_header(type);
        append_rb_value(binary, count);
        binary.append(length, '\0');
        expect_rb_to_string_error(binary);
      }
    }
    for (uint8_t count : invalid_counts) {
      SCOPED_TRACE(static_cast<int>(count));
      std::string binary = raw_rb_header(type);
      append_rb_value(binary, count);
      // Matching payload length must not make an out-of-range count legal.
      binary.append(count * width, '\0');
      expect_rb_to_string_error(binary);
    }
  }
}

TEST_F(TestRoaringBitmap, to_string_truncated_inputs_at_guard_page)
{
  const std::string inputs[] = {
      std::string("\x01\x01", 2), std::string("\x01\x02", 2),
      std::string("\x01\x03", 2), std::string("\x01\x04", 2),
      std::string("\x01\x03\x02", 3), std::string("\x01\x04\x02", 3)};
  for (size_t i = 0; i < sizeof(inputs) / sizeof(inputs[0]); ++i) {
    SCOPED_TRACE(i);
    GuardedRbBuffer guarded;
    ASSERT_TRUE(guarded.init(inputs[i])) << "guard page setup failed: " << strerror(errno);
    ObArenaAllocator allocator(ObModIds::TEST);
    ObString result;
    EXPECT_EQ(OB_INVALID_ARGUMENT,
              ObRbUtils::rb_to_string(allocator, ObString(inputs[i].size(), guarded.data()), result));
    EXPECT_EQ(0, result.length());
    EXPECT_EQ(nullptr, result.ptr());
  }
}

TEST_F(TestRoaringBitmap, to_string_invalid_bitmap_layouts)
{
  for (bool is_64 : {false, true}) {
    SCOPED_TRACE(is_64);
    expect_rb_to_string_error(raw_rb_header(is_64 ? ObRbBinType::BITMAP_64 : ObRbBinType::BITMAP_32));
    std::string binary;
    ASSERT_NO_FATAL_FAILURE(make_raw_bitmap(is_64, 33, binary));
    expect_rb_to_string_error(binary.substr(0, binary.size() - 1));
    expect_rb_to_string_error(binary + '\0');

    // With no run optimization and values 0..32, there is one array container:
    // [cookie:4][container count:4][key/cardinality:4][offset:4][values].
    const size_t raw_offset = 2 + (is_64 ? sizeof(uint64_t) + sizeof(uint32_t) : 0);
    uint32_t cookie = 0;
    uint32_t container_count = 0;
    uint32_t container_offset = 0;
    ASSERT_GE(binary.size(), raw_offset + 16);
    MEMCPY(&cookie, binary.data() + raw_offset, sizeof(cookie));
    MEMCPY(&container_count, binary.data() + raw_offset + 4, sizeof(container_count));
    MEMCPY(&container_offset, binary.data() + raw_offset + 12, sizeof(container_offset));
    ASSERT_EQ(roaring::internal::SERIAL_COOKIE_NO_RUNCONTAINER, cookie);
    ASSERT_EQ(1, container_count);
    ASSERT_EQ(16, container_offset);
    const uint32_t bad_offset = static_cast<uint32_t>(binary.size() + 1);
    MEMCPY(&binary[raw_offset + 12], &bad_offset, sizeof(bad_offset));
    expect_rb_to_string_error(binary);
  }
}

TEST_F(TestRoaringBitmap, to_string_valid_raw_layouts_are_read_only)
{
  expect_rb_to_string_value(raw_rb_header(ObRbBinType::EMPTY), "");

  std::string binary = raw_rb_header(ObRbBinType::SINGLE_32);
  append_rb_value(binary, uint32_t(UINT32_MAX));
  expect_rb_to_string_value(binary, "4294967295");
  binary = raw_rb_header(ObRbBinType::SINGLE_64);
  append_rb_value(binary, uint64_t(UINT64_MAX));
  expect_rb_to_string_value(binary, "18446744073709551615");

  binary = raw_rb_header(ObRbBinType::SET_32);
  append_rb_value(binary, uint8_t(2));
  append_rb_value(binary, uint32_t(2));
  append_rb_value(binary, uint32_t(1));
  expect_rb_to_string_value(binary, "1,2");
  binary = raw_rb_header(ObRbBinType::SET_64);
  append_rb_value(binary, uint8_t(2));
  append_rb_value(binary, uint64_t(4294967296));
  append_rb_value(binary, uint64_t(1));
  expect_rb_to_string_value(binary, "1,4294967296");

  binary = raw_rb_header(ObRbBinType::SET_32);
  append_rb_value(binary, uint8_t(32));
  for (uint32_t i = 0; i < 32; ++i) {
    append_rb_value(binary, uint32_t(UINT32_MAX - i));
  }
  expect_rb_to_string_value(binary, rb_range_text(UINT32_MAX - 31, 32));
  binary = raw_rb_header(ObRbBinType::SET_64);
  append_rb_value(binary, uint8_t(32));
  for (uint32_t i = 0; i < 32; ++i) {
    append_rb_value(binary, uint64_t(UINT64_MAX - i));
  }
  expect_rb_to_string_value(binary, rb_range_text(UINT64_MAX - 31, 32));

  for (bool is_64 : {false, true}) {
    SCOPED_TRACE(is_64);
    ASSERT_NO_FATAL_FAILURE(make_raw_bitmap(is_64, 33, binary));
    expect_rb_to_string_value(binary, rb_range_text(is_64 ? uint64_t(4294967296) : 0, 33));
  }
}

TEST_F(TestRoaringBitmap, to_string_cardinality_limit)
{
  for (bool is_64 : {false, true}) {
    SCOPED_TRACE(is_64);
    std::string binary;
    ASSERT_NO_FATAL_FAILURE(make_raw_bitmap(is_64, 1000001, binary));
    expect_rb_to_string_error(binary, OB_NOT_SUPPORTED);
  }
}

} // namespace common
} // namespace oceanbase

int main(int argc, char** argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  // system("rm -f test_geo_tree.log");
  // OB_LOGGER.set_file_name("test_geo_tree.log");
  OB_LOGGER.set_log_level("DEBUG");
  return RUN_ALL_TESTS();
}
