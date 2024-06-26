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

#include <gtest/gtest.h>
#define protected public
#define private public


#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "storage/lob/ob_lob_seq.h"

namespace oceanbase
{
using namespace common;

namespace unittest
{

class TestLobSeqId  : public ::testing::Test
{
public:
  TestLobSeqId() {}
  virtual ~TestLobSeqId() {};
  
  virtual void SetUp()
  {
  }

  virtual void TearDown()
  {
  }
};

static void make_seq_id(ObIAllocator* alloc, uint32_t dig_size, char*& seq_buf, uint32_t*& digits ,uint32_t& seq_len)
{
  uint32_t seq_size = dig_size * 16;
  
  uint32_t ser_len = 0;
  uint32_t offset = 0;

  seq_buf = static_cast<char*>(alloc->alloc(seq_size));
  ASSERT_NE(seq_buf, nullptr);
  digits = static_cast<uint32_t*>(alloc->alloc(dig_size * sizeof(uint32_t)));
  ASSERT_NE(digits, nullptr);

  for (int i = 0; i < dig_size; i++) {
    digits[i] = random();
    (void)ObLobSeqId::store32be(seq_buf + offset, digits[i]);
    offset += sizeof(uint32_t);
  }

  seq_len = offset;
}

TEST_F(TestLobSeqId, parse)
{
  char* seq_buf;
  uint32_t seq_len;
  uint32_t* digits;
  uint32_t dig_size = 32;

  ObArenaAllocator allocator;
  make_seq_id(&allocator, dig_size, seq_buf, digits, seq_len);

  ObString seq_str;
  seq_str.assign_ptr(seq_buf, seq_len);
  ObLobSeqId seq_id(seq_str, &allocator);

  ASSERT_EQ(OB_SUCCESS, seq_id.parse());
  for (uint32_t i = 0; i < seq_id.dig_len_; ++i) {
    ASSERT_EQ(seq_id.digits_[i], digits[i]);
  }
}

TEST_F(TestLobSeqId, extend_internal)
{
  char* seq_buf;
  uint32_t seq_len;
  uint32_t* digits;
  uint32_t dig_size = ObLobSeqId::LOB_SEQ_DIGIT_DEFAULT_LEN;

  ObArenaAllocator allocator;
  make_seq_id(&allocator, dig_size, seq_buf, digits, seq_len);

  ObString seq_str;
  seq_str.assign_ptr(seq_buf, seq_len);
  ObLobSeqId seq_id(seq_str, &allocator);

  ASSERT_EQ(OB_SUCCESS, seq_id.parse());
  for (uint32_t i = 0; i < seq_id.dig_len_; ++i) {
    ASSERT_EQ(seq_id.digits_[i], digits[i]);
  }

  ASSERT_EQ(OB_SUCCESS, seq_id.add_digits(256));
  ASSERT_EQ(OB_SUCCESS, seq_id.append_seq_buf(256));

  ASSERT_EQ(dig_size * 2, seq_id.dig_cap_);
  ASSERT_EQ(dig_size + 1, seq_id.dig_len_);     
}

TEST_F(TestLobSeqId, next_seq_id)
{
  char* seq_buf;
  uint32_t seq_len;
  uint32_t* digits;
  uint32_t dig_size = ObLobSeqId::LOB_SEQ_DIGIT_DEFAULT_LEN;

  ObArenaAllocator allocator;
  ObString seq_str;
  ObLobSeqId seq_id(seq_str, &allocator);

  ASSERT_EQ(seq_id.dig_cap_, 0);
  ASSERT_EQ(seq_id.dig_len_, 0);
  ASSERT_EQ(seq_id.digits_, nullptr);

  ObString next_seq_id;
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  ASSERT_EQ(seq_id.dig_cap_, dig_size);
  ASSERT_EQ(seq_id.dig_len_, 1);
  ASSERT_NE(seq_id.digits_, nullptr);
  ASSERT_EQ(seq_id.digits_[0], 256);

  ASSERT_EQ(seq_id.cap_, 1024 * 8);
  ASSERT_EQ(seq_id.len_, 4);
  ASSERT_NE(seq_id.buf_, nullptr);
  
  seq_id.reset();
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  ASSERT_EQ(seq_id.dig_cap_, dig_size);
  ASSERT_EQ(seq_id.dig_len_, 1);
  ASSERT_NE(seq_id.digits_, nullptr);
  ASSERT_EQ(seq_id.digits_[0], 512);

  ASSERT_EQ(seq_id.len_, 4);
  ASSERT_NE(seq_id.buf_, nullptr);

  seq_id.reset();
  char buf[32] = {0};
  uint32_t* i_ptr = reinterpret_cast<uint32_t*>(buf);
  i_ptr[0] = UINT32_MAX;
  ObString overflow_str;
  overflow_str.assign_ptr(buf, sizeof(uint32_t));
  seq_id.set_seq_id(overflow_str);
  ASSERT_EQ(seq_id.get_next_seq_id(next_seq_id), OB_SUCCESS);
  
  ASSERT_EQ(seq_id.dig_cap_, dig_size);
  ASSERT_EQ(seq_id.dig_len_, 2);
  ASSERT_NE(seq_id.digits_, nullptr);
  ASSERT_EQ(seq_id.digits_[1], 256);

  ASSERT_NE(seq_id.buf_, nullptr);
}

struct ObLobDataOutRowCtxV0
{
  ObLobDataOutRowCtxV0()
    : is_full_(0), op_(0), offset_(0), check_sum_(0), seq_no_st_(0), seq_no_cnt_(0),
      del_seq_no_cnt_(0), modified_len_(0), first_meta_offset_(0)
  {}

  uint64_t is_full_ : 1;
  uint64_t op_ : 8;
  uint64_t offset_ : 55;
  uint64_t check_sum_;
  int64_t seq_no_st_;
  uint32_t seq_no_cnt_;
  uint32_t del_seq_no_cnt_; // for sql update
  uint64_t modified_len_;
  uint32_t first_meta_offset_;
};

TEST_F(TestLobSeqId, test_chunk_size_v0_to_v1)
{
  ObLobDataOutRowCtxV0 ctx_v0;
  ctx_v0.op_ = ObLobDataOutRowCtx::DIFF;
  ctx_v0.offset_ = 10000;
  ctx_v0.check_sum_ = 12345;
  ctx_v0.seq_no_st_ = 56778899;
  ctx_v0.seq_no_cnt_ = 5;
  ctx_v0.del_seq_no_cnt_ = 0;
  ctx_v0.modified_len_ = 111111;
  ctx_v0.first_meta_offset_ = 1234567;

  ObLobDataOutRowCtx ctx_v1;
  ASSERT_EQ(sizeof(ObLobDataOutRowCtx), sizeof(ObLobDataOutRowCtxV0));
  MEMCPY(&ctx_v1, &ctx_v0, sizeof(ObLobDataOutRowCtx));
  ASSERT_EQ(ctx_v0.is_full_, ctx_v1.is_full_);
  ASSERT_EQ(ctx_v0.offset_, ctx_v1.offset_);
  ASSERT_EQ(ctx_v0.check_sum_, ctx_v1.check_sum_);
  ASSERT_EQ(ctx_v0.seq_no_st_, ctx_v1.seq_no_st_);
  ASSERT_EQ(ctx_v0.seq_no_cnt_, ctx_v1.seq_no_cnt_);
  ASSERT_EQ(ctx_v0.del_seq_no_cnt_, ctx_v1.del_seq_no_cnt_);
  ASSERT_EQ(ctx_v0.modified_len_, ctx_v1.modified_len_);
  ASSERT_EQ(0, ctx_v1.chunk_size_);
  ASSERT_EQ(ctx_v0.first_meta_offset_, ctx_v1.first_meta_offset_);
}

TEST_F(TestLobSeqId, test_chunk_size_v1_to_v0)
{
  ObLobDataOutRowCtx ctx_v1;
  ctx_v1.op_ = ObLobDataOutRowCtx::DIFF;
  ctx_v1.offset_ = 10000;
  ctx_v1.check_sum_ = 12345;
  ctx_v1.seq_no_st_ = 56778899;
  ctx_v1.seq_no_cnt_ = 5;
  ctx_v1.del_seq_no_cnt_ = 0;
  ctx_v1.modified_len_ = 111111;
  ctx_v1.chunk_size_ = 4;
  ctx_v1.first_meta_offset_ = 1234567;

  ObLobDataOutRowCtxV0 ctx_v0;
  ASSERT_EQ(sizeof(ObLobDataOutRowCtx), sizeof(ObLobDataOutRowCtxV0));
  MEMCPY(&ctx_v0, &ctx_v1, sizeof(ObLobDataOutRowCtx));
  ASSERT_EQ(ctx_v0.is_full_, ctx_v1.is_full_);
  ASSERT_EQ(ctx_v0.offset_, ctx_v1.offset_);
  ASSERT_EQ(ctx_v0.check_sum_, ctx_v1.check_sum_);
  ASSERT_EQ(ctx_v0.seq_no_st_, ctx_v1.seq_no_st_);
  ASSERT_EQ(ctx_v0.seq_no_cnt_, ctx_v1.seq_no_cnt_);
  ASSERT_EQ(ctx_v0.del_seq_no_cnt_, ctx_v1.del_seq_no_cnt_);
  ASSERT_EQ(ctx_v0.modified_len_, ctx_v1.modified_len_);
  ASSERT_EQ(4, ctx_v1.chunk_size_);
  ASSERT_EQ(ctx_v0.first_meta_offset_ & 0xffffff, ctx_v1.first_meta_offset_ );
}

TEST_F(TestLobSeqId, test_lob_outrow_locator_size)
{
  ASSERT_EQ(84, sizeof(ObLobCommon) + sizeof(ObLobData)  + sizeof(ObLobDataOutRowCtx) + sizeof(uint64_t));
}


}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char **argv)
{
  system("rm -f test_lob_seq_id.log*");
  OB_LOGGER.set_file_name("test_lob_seq_id.log", true);
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
