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

#include "clog/ob_log_entry.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "share/allocator/ob_tenant_mutil_allocator_mgr.h"
#include "share/ob_proposal_id.h"

#include <gtest/gtest.h>

using namespace oceanbase::common;
namespace oceanbase {
using namespace clog;
namespace unittest {
class TestObLogEntry : public ::testing::Test {
public:
  virtual void SetUp()
  {
    ltype = OB_LOG_SUBMIT;                                         // Test log type in normal case
    log_id = 4;                                                    // Set random log id
    gts = ::oceanbase::common::ObTimeUtility::current_time();      // log generation_timestamp
    tts = ::oceanbase::common::ObTimeUtility::current_time();      // log epoch_id
    rts.ts_ = ::oceanbase::common::ObTimeUtility::current_time();  // log propose_id
    mts = ::oceanbase::common::ObTimeUtility::current_time();      // log mc_timestamp
    sts = ::oceanbase::common::ObTimeUtility::current_time();      // log submit_timestamp
    data_len = BUFSIZE >> 1;
    tid = 2;
    pid = 3;
    p_count = 10;
    pos = 0;

    memset(data, 'A', data_len);
    partition_key.init(tid, pid, p_count);
  }
  virtual void TearDown()
  {}

public:
  static const int64_t BUFSIZE = 1 << 9;  // 256B
  ObLogEntryHeader header;
  clog::ObLogEntry entry;
  ObPartitionKey partition_key;
  ObLogType ltype;
  uint64_t log_id;
  int64_t gts;
  int64_t tts;
  common::ObProposalID rts;
  int64_t mts;
  int64_t sts;
  int64_t data_len;
  int64_t tid;
  int32_t pid;
  int32_t p_count;
  int64_t pos;
  char data[BUFSIZE];
  char buf[BUFSIZE];
};

TEST_F(TestObLogEntry, test_entry_copy)
{
  common::ObTenantMutilAllocator* alloc_mgr = NULL;
  const uint64_t tenant_id = 3001;
  ObMemAttr attr(tenant_id, ObModIds::OB_TENANT_MUTIL_ALLOCATOR);
  void* buf = ob_malloc(sizeof(common::ObTenantMutilAllocator), attr);
  if (NULL == buf) {
    CLOG_LOG(WARN, "alloc memory failed");
    OB_ASSERT(FALSE);
  }
  alloc_mgr = new (buf) common::ObTenantMutilAllocator(tenant_id);
  if (NULL == alloc_mgr) {
    CLOG_LOG(WARN, "alloc_mgr construct failed");
    OB_ASSERT(FALSE);
  }

  ObLogEntryHeader header2;
  clog::ObLogEntry entry2;

  // test_generate_entry
  EXPECT_EQ(OB_SUCCESS,
      header.generate_header(ltype, partition_key, log_id, data, data_len, gts, tts, rts, sts, ObVersion(0), true));
  EXPECT_EQ(OB_SUCCESS, entry.generate_entry(header, const_cast<char*>(data)));
  EXPECT_TRUE(entry.check_integrity());

  EXPECT_EQ(OB_SUCCESS, entry.deep_copy_to(entry2));
  EXPECT_TRUE(entry2.check_integrity());
  EXPECT_EQ(entry.get_header().get_header_checksum(), entry2.get_header().get_header_checksum());
  EXPECT_EQ(entry.get_header().get_data_checksum(), entry2.get_header().get_data_checksum());
  EXPECT_TRUE(entry == entry2);
  CLOG_LOG(INFO, "print entry2", "entry2", to_cstring(entry2));
  alloc_mgr->ge_free(const_cast<char*>(entry2.get_buf()));
  ob_free(alloc_mgr);

  entry.reset();
  EXPECT_FALSE(entry.check_integrity());
}

TEST_F(TestObLogEntry, test_serialize)
{
  EXPECT_EQ(OB_SUCCESS,
      header.generate_header(ltype, partition_key, log_id, data, data_len, gts, tts, rts, sts, ObVersion(0), true));
  EXPECT_EQ(OB_SUCCESS, entry.generate_entry(header, const_cast<char*>(data)));
  EXPECT_TRUE(entry.check_integrity());

  int64_t size = entry.get_serialize_size() - 1;
  // test_serialize_and_deserialize
  CLOG_LOG(INFO, "before serialize", "entry", to_cstring(entry));
  EXPECT_EQ(OB_INVALID_ARGUMENT, entry.serialize(buf, 0, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, entry.serialize(NULL, 0, pos));
  EXPECT_EQ(OB_SERIALIZE_ERROR, entry.serialize(buf, 1, pos));
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, entry.serialize(buf, size, pos));
  EXPECT_EQ(OB_SUCCESS, entry.serialize(buf, BUFSIZE, pos));

  pos = 0;
  EXPECT_EQ(OB_INVALID_ARGUMENT, entry.deserialize(NULL, data_len, pos));
  EXPECT_EQ(OB_INVALID_ARGUMENT, entry.deserialize(buf, -1, pos));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, entry.deserialize(buf, 1, pos));
  EXPECT_EQ(OB_DESERIALIZE_ERROR, entry.deserialize(buf, data_len, pos));
  CLOG_LOG(INFO, "before deserialize", "entry", to_cstring(entry));

  pos = 0;
  buf[size] = 'B';
  EXPECT_EQ(OB_DESERIALIZE_ERROR, entry.deserialize(buf, data_len, pos));
}

TEST_F(TestObLogEntry, test_ilog_entry)
{
  ObIndexEntry ilog1;
  ObIndexEntry ilog2;
  file_id_t file_id = 1;
  offset_t offset = 1;
  int32_t size = 1;
  int64_t submit_timestamp = 10000 + (1ull << 40);
  int64_t accum_checksum = 1000;
  bool batch_committed = true;
  EXPECT_EQ(OB_SUCCESS,
      ilog1.init(partition_key, log_id, file_id, offset, size, submit_timestamp, accum_checksum, batch_committed));
  EXPECT_EQ(submit_timestamp, ilog1.get_submit_timestamp());
  EXPECT_EQ(true, ilog1.is_batch_committed());
  batch_committed = false;
  EXPECT_EQ(OB_SUCCESS,
      ilog2.init(partition_key, log_id, file_id, offset, size, submit_timestamp, accum_checksum, batch_committed));
  EXPECT_EQ(submit_timestamp, ilog2.get_submit_timestamp());
  EXPECT_EQ(false, ilog2.is_batch_committed());
}

}  // end namespace unittest
}  // end namespace oceanbase

int main(int argc, char** argv)
{
  OB_LOGGER.set_file_name("test_ob_log_entry.log", true);
  OB_LOGGER.set_log_level("INFO");
  CLOG_LOG(INFO, "begin unittest:test_ob_log_entry");
  testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
