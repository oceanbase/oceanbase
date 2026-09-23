/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include <gtest/gtest.h>
#include <vector>
#include "sql/engine/px/p2p_datahub/ob_runtime_filter_vec_msg.h"

namespace oceanbase
{
namespace sql
{
using namespace common;

class TestRuntimeFilterVecMsg : public ::testing::TestWithParam<bool>
{
protected:
  void SetUp() override
  {
    ASSERT_EQ(OB_SUCCESS, source_.init(1, 1, 1, OB_SYS_TENANT_ID, 0, ObRegisterDmInfo()));
    source_.set_msg_type(ObP2PDatahubMsgBase::IN_FILTER_VEC_MSG);
    source_.max_in_num_ = 16;
    source_.build_send_opt_ = GetParam();
    ASSERT_EQ(OB_SUCCESS, source_.sm_hash_set_.init(source_.max_in_num_, OB_SYS_TENANT_ID));
    buffer_.resize(source_.get_serialize_size());
    data_len_ = 0;
    ASSERT_EQ(OB_SUCCESS, source_.serialize(buffer_.data(), buffer_.size(), data_len_));
  }

  ObRFInFilterVecMsg source_;
  std::vector<char> buffer_;
  int64_t data_len_ = 0;
};

TEST_P(TestRuntimeFilterVecMsg, valid_empty_row_store)
{
  ObRFInFilterVecMsg decoded;
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(buffer_.data(), data_len_, pos));
  EXPECT_EQ(data_len_, pos);
  EXPECT_TRUE(decoded.rows_set_.created());
  EXPECT_EQ(0, decoded.row_store_.get_row_cnt());
  EXPECT_TRUE(decoded.sm_hash_set_.inited());
  EXPECT_EQ(GetParam(), decoded.build_send_opt_);
}

TEST_P(TestRuntimeFilterVecMsg, preserve_row_store_deserialize_error)
{
  // Locate row_store_ inside the otherwise valid message, using the actual
  // encoded sizes of the fields preceding it (including the outer UNIS header).
  int64_t pos = 0;
  int64_t version = 0;
  int64_t payload_len = 0;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer_.data(), data_len_, pos, version));
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer_.data(), data_len_, pos, payload_len));
  pos += source_.ObP2PDatahubMsgBase::get_serialize_size()
      + serialization::encoded_length(source_.max_in_num_)
      + serialization::encoded_length(source_.need_null_cmp_flags_)
      + serialization::encoded_length(source_.build_row_cmp_info_)
      + serialization::encoded_length(source_.probe_row_cmp_info_)
      + serialization::encoded_length(source_.build_row_meta_);
  const int64_t row_store_pos = pos;
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer_.data(), data_len_, pos, version));
  ASSERT_EQ(OB_SUCCESS, serialization::decode(buffer_.data(), data_len_, pos, payload_len));
  // Empty row_store_ contains two zero counts. Make its first varint
  // unterminated, without touching the UNIS header or subsequent message fields.
  ASSERT_EQ(2, payload_len);
  ASSERT_LT(pos + payload_len, data_len_);
  buffer_[pos] = static_cast<char>(0x80);
  buffer_[pos + 1] = static_cast<char>(0x80);
  const int64_t row_store_end = pos + payload_len;

  // Establish that the real nested decoder fails and still advances past its
  // payload. Later fields remain valid, so they cannot mask this regression.
  ObArenaAllocator allocator;
  ObRFInFilterVecMsg::ObRFInFilterRowStore row_store(allocator);
  pos = row_store_pos;
  ASSERT_EQ(OB_DESERIALIZE_ERROR, row_store.deserialize(buffer_.data(), data_len_, pos));
  ASSERT_EQ(row_store_end, pos);

  ObRFInFilterVecMsg decoded;
  pos = 0;
  // Without the OB_SUCC guard on rows_set_.create(), this returns OB_SUCCESS.
  EXPECT_EQ(OB_DESERIALIZE_ERROR, decoded.deserialize(buffer_.data(), data_len_, pos));
  EXPECT_FALSE(decoded.rows_set_.created());
  EXPECT_FALSE(decoded.sm_hash_set_.inited());
}

INSTANTIATE_TEST_CASE_P(SendModes, TestRuntimeFilterVecMsg, ::testing::Bool());

} // namespace sql
} // namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("WARN");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
