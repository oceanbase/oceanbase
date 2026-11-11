/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SQL_DAS
#include <gtest/gtest.h>
#include <array>
#include <vector>
#define private public
#include "sql/das/ob_das_dml_ctx_define.h"
#undef private
#include "sql/das/ob_das_attach_define.h"
#include "sql/das/ob_das_ir_define.h"
#include "sql/das/ob_das_scan_op.h"
#include "share/domain_id/ob_domain_id.h"

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql;
namespace ser = oceanbase::common::serialization;

namespace {

class TestDASLookupDeserialize : public ::testing::TestWithParam<ObDASOpType>
{
protected:
  TestDASLookupDeserialize() : alloc_("DASDeserTest"), scan_(alloc_), aux_(alloc_), lookup_(alloc_) {}

  void TearDown() override
  {
    for (auto it = owned_.rbegin(); it != owned_.rend(); ++it) {
      (*it)->~ObDASBaseCtDef();
    }
  }

  ObDASBaseCtDef *make_merge()
  {
    ObDASBaseCtDef *merge = nullptr;
    EXPECT_EQ(OB_SUCCESS, ObDASTaskFactory::create_das_ctdef(GetParam(), alloc_, merge));
    if (merge != nullptr) {
      owned_.push_back(merge);
      if (GetParam() == DAS_OP_DOMAIN_ID_MERGE) {
        auto *domain = static_cast<ObDASDomainIdMergeCtDef *>(merge);
        EXPECT_EQ(OB_SUCCESS, domain->domain_types_.prepare_allocate(1));
        domain->domain_types_.at(0) = ObDomainIdUtils::ObDomainIDType::DOC_ID;
      }
    }
    return merge;
  }

  ObArenaAllocator alloc_;
  ObDASScanCtDef scan_;
  ObDASScanCtDef aux_;
  ObDASTableLookupCtDef lookup_;
  std::vector<ObDASBaseCtDef *> owned_;
};

TEST_P(TestDASLookupDeserialize, rejects_wrong_scan_type)
{
  ObDASBaseCtDef *merge = make_merge();
  ASSERT_NE(nullptr, merge);
  ObDASDocIdMergeCtDef wrong(alloc_);
  ObDASBaseCtDef *lookup_children[] = {&scan_, merge};
  ObDASBaseCtDef *merge_children[] = {&wrong, &aux_};
  lookup_.children_cnt_ = 2;
  lookup_.children_ = lookup_children;
  merge->children_cnt_ = 2;
  merge->children_ = merge_children;
  EXPECT_EQ(nullptr, lookup_.get_lookup_scan_ctdef());
}

TEST_P(TestDASLookupDeserialize, accepts_scan_child)
{
  ObDASBaseCtDef *merge = make_merge();
  ASSERT_NE(nullptr, merge);
  ObDASDocIdMergeCtDef unused(alloc_);
  ObDASBaseCtDef *lookup_children[] = {&aux_, merge};
  ObDASBaseCtDef *merge_children[] = {&scan_, &unused};
  lookup_.children_cnt_ = 2;
  lookup_.children_ = lookup_children;
  merge->children_cnt_ = 2;
  merge->children_ = merge_children;
  EXPECT_EQ(&scan_, lookup_.get_lookup_scan_ctdef());
  merge_children[1] = nullptr;
  EXPECT_EQ(&scan_, lookup_.get_lookup_scan_ctdef());
}

INSTANTIATE_TEST_CASE_P(MergeTypes, TestDASLookupDeserialize,
    ::testing::Values(DAS_OP_DOC_ID_MERGE, DAS_OP_VID_MERGE, DAS_OP_DOMAIN_ID_MERGE));

TEST(DASLookupGetter, runtime_rejects_wrong_scan_type)
{
  ObDASScanRtDef scan;
  ObDASDocIdMergeRtDef merge;
  ObDASDocIdMergeRtDef wrong;
  ObDASTableLookupRtDef lookup;
  ObDASBaseRtDef *lookup_children[] = {&scan, &merge};
  ObDASBaseRtDef *merge_children[] = {&wrong, &scan};
  lookup.children_cnt_ = merge.children_cnt_ = 2;
  lookup.children_ = lookup_children;
  merge.children_ = merge_children;
  EXPECT_EQ(nullptr, lookup.get_lookup_scan_rtdef());
  merge_children[0] = &scan;
  merge_children[1] = &wrong;
  EXPECT_EQ(&scan, lookup.get_lookup_scan_rtdef());
  merge_children[1] = nullptr;
  EXPECT_EQ(&scan, lookup.get_lookup_scan_rtdef());
}

class TestDASWriteBufferDeserialize : public ::testing::Test
{
protected:
  TestDASWriteBufferDeserialize() : alloc_("DASDeserTest"), buffer_() {}
  void SetUp() override { ASSERT_EQ(OB_SUCCESS, buffer_.init(alloc_)); }

  std::vector<char> row(uint32_t count, uint32_t inner_size, size_t bytes)
  {
    std::vector<char> result(bytes, 0);
    if (bytes >= sizeof(ObDASWriteBuffer::DmlRow)) {
      MEMCPY(result.data(), &count, sizeof(count));
      MEMCPY(result.data() + sizeof(count), &inner_size, sizeof(inner_size));
    }
    return result;
  }

  int64_t encode_row(char *out, int64_t capacity, int64_t wire_size, const std::vector<char> &data)
  {
    int64_t pos = 0;
    EXPECT_EQ(OB_SUCCESS, ser::encode_vi64(out, capacity, pos, 1));
    EXPECT_EQ(OB_SUCCESS, ser::encode_vi64(out, capacity, pos, wire_size));
    MEMCPY(out + pos, data.data(), data.size());
    return pos + data.size();
  }

  ObArenaAllocator alloc_;
  ObDASWriteBuffer buffer_;
};

TEST_F(TestDASWriteBufferDeserialize, rejects_row_larger_than_remaining_input)
{
  // Physical padding keeps the pre-fix test deterministic without reading other allocations.
  std::array<char, 1024> bytes{};
  const auto data = row(0, 512, 8);
  const int64_t len = encode_row(bytes.data(), bytes.size(), 512, data);
  int64_t pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR, buffer_.deserialize_buffer_list(bytes.data(), len, pos));
  EXPECT_EQ(0, buffer_.get_mem_used());
}

TEST_F(TestDASWriteBufferDeserialize, rejects_mismatching_inner_row_size)
{
  std::array<char, 1024> bytes{};
  const auto data = row(0, 8, 16);
  const int64_t len = encode_row(bytes.data(), bytes.size(), 16, data);
  int64_t pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR, buffer_.deserialize_buffer_list(bytes.data(), len, pos));
}

TEST_F(TestDASWriteBufferDeserialize, rejects_negative_list_size)
{
  std::array<char, 32> bytes{};
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, ser::encode_vi64(bytes.data(), bytes.size(), len, -1));
  int64_t pos = 0;
  EXPECT_EQ(OB_DESERIALIZE_ERROR, buffer_.deserialize_buffer_list(bytes.data(), len, pos));
  EXPECT_EQ(0, buffer_.get_mem_used());
}

TEST_F(TestDASWriteBufferDeserialize, accepts_empty_list)
{
  std::array<char, 32> bytes{};
  int64_t len = 0;
  ASSERT_EQ(OB_SUCCESS, ser::encode_vi64(bytes.data(), bytes.size(), len, 0));
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, buffer_.deserialize_buffer_list(bytes.data(), len, pos));
  EXPECT_EQ(len, pos);
  EXPECT_EQ(0, buffer_.get_row_cnt());
}

TEST_F(TestDASWriteBufferDeserialize, accepts_used_payload_and_unused_null_offset)
{
  const uint32_t size = sizeof(ObDASWriteBuffer::DmlRow) + 2 * sizeof(ObDatum) + 5;
  auto data = row(2, size, size);
  auto *stored = reinterpret_cast<ObDASWriteBuffer::DmlRow *>(data.data());
  new (&stored->cells()[0]) ObDatum();
  new (&stored->cells()[1]) ObDatum();
  stored->cells()[0].ptr_ = reinterpret_cast<const char *>(2 * sizeof(ObDatum));
  stored->cells()[0].pack_ = 5;
  stored->cells()[1].set_null();
  stored->cells()[1].ptr_ = reinterpret_cast<const char *>(-5);
  MEMCPY(data.data() + size - 5, "hello", 5);
  std::array<char, 1024> bytes{};
  const int64_t len = encode_row(bytes.data(), bytes.size(), size, data);
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, buffer_.deserialize_buffer_list(bytes.data(), len, pos));
  EXPECT_EQ(len, pos);
  ObDASWriteBuffer::Iterator iter;
  ASSERT_EQ(OB_SUCCESS, buffer_.begin(iter));
  const ObChunkDatumStore::StoredRow *decoded = nullptr;
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(decoded));
  ASSERT_NE(nullptr, decoded);
  ASSERT_EQ(2, decoded->cnt_);
  EXPECT_EQ(ObString::make_string("hello"), decoded->cells()[0].get_string());
  EXPECT_TRUE(decoded->cells()[1].is_null());
  EXPECT_EQ(OB_ITER_END, iter.get_next_row(decoded));

  pos = 0;
  ASSERT_EQ(OB_SUCCESS, buffer_.serialize(bytes.data(), bytes.size(), pos));
  const int64_t serialized_size = pos;
  ObDASWriteBuffer copy;
  ASSERT_EQ(OB_SUCCESS, copy.init(alloc_));
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, copy.deserialize(bytes.data(), serialized_size, pos));
  EXPECT_EQ(serialized_size, pos);
  ASSERT_EQ(OB_SUCCESS, copy.begin(iter));
  ASSERT_EQ(OB_SUCCESS, iter.get_next_row(decoded));
  EXPECT_EQ(ObString::make_string("hello"), decoded->cells()[0].get_string());
  EXPECT_TRUE(decoded->cells()[1].is_null());
}

} // namespace

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_das_deserialize.log", true);
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
