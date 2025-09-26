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

#define USING_LOG_PREFIX SQL_DTL
#include <gtest/gtest.h>
#define private public
#include "sql/dtl/ob_dtl_linked_buffer.h"
#include "lib/utility/utility.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"
#include "sql/ob_sql_utils.h"

namespace oceanbase
{
using namespace common;
using namespace sql;
using namespace dtl;

namespace dtl
{

static const int64_t BUF_SIZE = 1024 * 1024; // 1MB buffer

struct ObDtlLinkedBuffer42x : public common::ObLink
{
  OB_UNIS_VERSION(2);

  char * buf_{nullptr};
  int64_t size_{0};
  mutable int64_t pos_{0};
  bool is_data_msg_{true};
  int64_t seq_no_{12345};
  uint64_t tenant_id_{1001};
  int64_t allocated_chid_{12345};
  bool is_eof_{false};
  int64_t timeout_ts_{60000000};
  ObDtlMsgType msg_type_{ObDtlMsgType::DH_DYNAMIC_SAMPLE_WHOLE_MSG};
  uint64_t flags_{0x12345678};
  ObDtlDfoKey dfo_key_{};
  bool use_interm_result_{false};
  int64_t batch_id_{98765};
  bool batch_info_valid_{true};
  int64_t rows_cnt_{1000};
  common::ObSArray<ObDtlBatchInfo> batch_info_{};
  int64_t dfo_id_{111111};
  int64_t sqc_id_{222222};
  bool enable_channel_sync_{true};
  common::ObRegisterDmInfo register_dm_info_{};
  ObDtlOpInfo op_info_{};
};

OB_DEF_SERIALIZE(ObDtlLinkedBuffer42x)
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
  OB_UNIS_ENCODE(size_);
  if (OB_SUCC(ret)) {
    if (buf_len - pos < size_) {
      ret = OB_SIZE_OVERFLOW;
    } else {
      MEMCPY(buf + pos, buf_, size_);
      pos += size_;
      LST_DO_CODE(OB_UNIS_ENCODE,
        is_data_msg_,
        seq_no_,
        tenant_id_,
        is_eof_,
        timeout_ts_,
        msg_type_,
        flags_,
        dfo_key_,
        use_interm_result_,
        batch_id_,
        batch_info_valid_);
      if (OB_SUCC(ret) && batch_info_valid_) {
        LST_DO_CODE(OB_UNIS_ENCODE, batch_info_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_ENCODE, dfo_id_, sqc_id_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_ENCODE, enable_channel_sync_);
      }
      if (OB_SUCC(ret)) {
        LST_DO_CODE(OB_UNIS_ENCODE, register_dm_info_);
      }
      if (OB_SUCC(ret) && seq_no_ == 1) {
        LST_DO_CODE(OB_UNIS_ENCODE, op_info_);
      }
    }
  }
  return ret;
}

int ObDtlLinkedBuffer42x::deserialize(const char *buf, const int64_t data_len, int64_t &pos)
{
  using namespace oceanbase::common;
  int ret = OB_SUCCESS;
  int64_t version = 0;
  int64_t len = 0;

  OB_UNIS_DECODE(version);
  OB_UNIS_DECODE(len);

  OB_UNIS_DECODE(size_);
  if (OB_SUCC(ret)) {
    buf_ = (char*)buf + pos;
    pos += size_;
    LST_DO_CODE(OB_UNIS_DECODE,
      is_data_msg_,
      seq_no_,
      tenant_id_,
      is_eof_,
      timeout_ts_,
      msg_type_,
      flags_,
      dfo_key_,
      use_interm_result_,
      batch_id_,
      batch_info_valid_);
    if (OB_SUCC(ret) && batch_info_valid_) {
      LST_DO_CODE(OB_UNIS_DECODE, batch_info_);
    }
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_DECODE, dfo_id_, sqc_id_);
    }
    if (OB_SUCC(ret)) {
      enable_channel_sync_ = false;
      LST_DO_CODE(OB_UNIS_DECODE, enable_channel_sync_);
    }
    if (OB_SUCC(ret)) {
      LST_DO_CODE(OB_UNIS_DECODE, register_dm_info_);
    }
    if (OB_SUCC(ret) && version == 3) {
      RowMeta row_meta;
      LST_DO_CODE(OB_UNIS_DECODE, row_meta);
    }
    if (OB_SUCC(ret) && seq_no_ == 1) {
      LST_DO_CODE(OB_UNIS_DECODE, op_info_);
    }
  }
  if (OB_SUCC(ret)) {
    (void)ObSQLUtils::adjust_time_by_ntp_offset(timeout_ts_);
  }
  return ret;
}

void fill_dtl_linked_buffer(ObDtlLinkedBuffer &buf)
{
  buf.buf_ = nullptr;
  buf.size_ = 0;
  buf.pos_ = 0;
  buf.is_data_msg_ = true;
  buf.seq_no_ = 12345;
  buf.tenant_id_ = 1001;
  buf.allocated_chid_ = 12345;
  buf.is_eof_ = false;
  buf.timeout_ts_ = 60000000;
  buf.msg_type_ = ObDtlMsgType::DH_DYNAMIC_SAMPLE_WHOLE_MSG;
  buf.flags_ = 0x12345678;
  buf.dfo_key_ = ObDtlDfoKey();
  buf.use_interm_result_ = false;
  buf.batch_id_ = 98765;
  buf.batch_info_valid_ = true;
  buf.rows_cnt_ = 1000;
  buf.dfo_id_ = 111111;
  buf.sqc_id_ = 222222;
  buf.enable_channel_sync_ = true;
  buf.register_dm_info_ = common::ObRegisterDmInfo();
  buf.op_info_ = ObDtlOpInfo();
}

class TestDtlLinkedBufferSerializeCompat : public ::testing::Test
{
public:
  TestDtlLinkedBufferSerializeCompat() {}
  virtual ~TestDtlLinkedBufferSerializeCompat() {}
  virtual void SetUp() {}
  virtual void TearDown() {}
};

void verify_basic_fields_equal(const ObDtlLinkedBuffer42x &buf1, const ObDtlLinkedBuffer &buf2)
{
  ASSERT_EQ(buf1.size_, buf2.size_);
  ASSERT_EQ(buf1.is_data_msg_, buf2.is_data_msg_);
  ASSERT_EQ(buf1.seq_no_, buf2.seq_no_);
  ASSERT_EQ(buf1.tenant_id_, buf2.tenant_id_);
  ASSERT_EQ(buf1.is_eof_, buf2.is_eof_);
  ASSERT_EQ(buf1.timeout_ts_, buf2.timeout_ts_);
  ASSERT_EQ(buf1.msg_type_, buf2.msg_type_);
  ASSERT_EQ(buf1.flags_, buf2.flags_);
  ASSERT_EQ(buf1.use_interm_result_, buf2.use_interm_result_);
  ASSERT_EQ(buf1.batch_id_, buf2.batch_id_);
  ASSERT_EQ(buf1.batch_info_valid_, buf2.batch_info_valid_);
  ASSERT_EQ(buf1.dfo_id_, buf2.dfo_id_);
  ASSERT_EQ(buf1.sqc_id_, buf2.sqc_id_);
  ASSERT_EQ(buf1.enable_channel_sync_, buf2.enable_channel_sync_);
}

TEST_F(TestDtlLinkedBufferSerializeCompat, test_42x_to_master)
{
  ObDtlLinkedBuffer42x buf_42x;
  ObDtlLinkedBuffer buf_master;

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, buf_42x.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, buf_master.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(buf_42x, buf_master);
}

TEST_F(TestDtlLinkedBufferSerializeCompat, test_master_to_42x)
{
  ObDtlLinkedBuffer buf_master;
  ObDtlLinkedBuffer42x buf_42x;
  fill_dtl_linked_buffer(buf_master);

  char buf[BUF_SIZE];
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, buf_master.serialize(buf, sizeof(buf), pos));

  int64_t deserialize_pos = 0;
  ASSERT_EQ(OB_SUCCESS, buf_42x.deserialize(buf, pos, deserialize_pos));

  verify_basic_fields_equal(buf_42x, buf_master);
}

} // namespace dtl
} // namespace oceanbase

int main(int argc, char **argv)
{
  oceanbase::common::ObLogger::get_logger().set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}