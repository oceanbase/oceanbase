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
#include "lib/ob_define.h"
#include "logservice/palf/palf_callback.h"
#include "logservice/palf/palf_env.h"
#include "logservice/palf/palf_options.h"
#include "logservice/palf/palf_handle.h"
#include "logservice/palf_handle_guard.h"
#include "rpc/frame/ob_req_transport.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"
#include "lib/file/file_directory_utils.h"

namespace oceanbase
{
namespace palf
{
using namespace common;
using namespace palf;

class MockLogCtx : public logservice::AppendCb
{
public:
  explicit MockLogCtx()
  {}
  ~MockLogCtx() {}
  int on_success() override {
    PALF_LOG(INFO, "on_success");
    return OB_SUCCESS;
  }
  // 日志未形成多数派时会调用此函数，调用此函数后对象不再使用
  int on_failure() override {
    PALF_LOG(INFO, "on_failure");
    return OB_SUCCESS;
  }
};

class TestCreatePalfEnv : public ::testing::Test
{
public:
	TestCreatePalfEnv() : self_(ObAddr::VER::IPV4, "127.0.0.1", 4096), palf_env_(), transport_(NULL, NULL), allocator_(500)
	{
	}
	~TestCreatePalfEnv() {palf::PalfEnv::destroy_palf_env(palf_env_);}
	int create_palf_handle(const int64_t id, palf::PalfRoleChangeCb *cb, palf::PalfHandle &handle)
	{
		return palf_env_->create(id, cb, handle);
	}
	void SetUp()
  {
    std::snprintf(log_dir_, OB_MAX_FILE_NAME_LENGTH, "%s_%ld", "unittest", ob_gettid());
    common::FileDirectoryUtils::delete_directory_rec(log_dir_);
    common::FileDirectoryUtils::create_directory(log_dir_);
		PalfDiskOptions opts;
		palf::PalfEnv::create_palf_env(opts, log_dir_, self_, &transport_, &allocator_, palf_env_);
  }
	void TearDown()
  {
    palf::PalfEnv::destroy_palf_env(palf_env_);
  }
protected:
	char log_dir_[OB_MAX_FILE_NAME_LENGTH];
	common::ObAddr self_;
	PalfEnv *palf_env_;
	rpc::frame::ObReqTransport transport_;
	ObTenantMutilAllocator allocator_;
};

class MockPalfRCCb : public PalfRoleChangeCb
{
  public:
  virtual int on_leader_revoke(int64_t id)
  { UNUSED(id); return OB_SUCCESS;}
  virtual int is_leader_revoke_done(int64_t id, bool &is_done) const
  { UNUSED(id); UNUSED(is_done); return OB_SUCCESS;}
  virtual int on_leader_takeover(int64_t id)
  { UNUSED(id); return OB_SUCCESS;}
  virtual int is_leader_takeover_done(int64_t id, bool &is_done) const
  { UNUSED(id); UNUSED(is_done); return OB_SUCCESS;}
  virtual int on_leader_active(int64_t id)
  { UNUSED(id); return OB_SUCCESS;}
};

TEST_F(TestCreatePalfEnv, creat)
{
	PalfHandle handle;
	int64_t base_ts = 1;
	PalfAppendOptions opts;
	const char *buffer = "123123123";
	int64_t buf_len = strlen(buffer);
	MockLogCtx cb;
  MockPalfRCCb cb1;
#include <string.h>
#include <gtest/gtest.h>
#include "lib/ob_errno.h"
#include "logservice/palf/log_writer_utils.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;

TEST(TestLogWriterUtils, test_log_write_buf)
{
  const char *buf1 = "hello,";
  const int64_t buf1_len = static_cast<int64_t>(strlen(buf1));
  const char *buf2 = "world!";
  const int64_t buf2_len = static_cast<int64_t>(strlen(buf2));
  const char *buf_check = "hello,world!";
  const int64_t buf_check_len = static_cast<int64_t>(strlen(buf_check));
  char buf[2048];
  int64_t buf_len = 2048;
  int64_t pos = 0;
  LogWriteBuf write_buf;
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf1, buf1_len));
  EXPECT_EQ(OB_SUCCESS, write_buf.push_back(buf2, buf2_len));
  EXPECT_EQ(OB_SIZE_OVERFLOW, write_buf.push_back(buf2, buf2_len));
  EXPECT_EQ(OB_SUCCESS, write_buf.serialize(buf, buf_len, pos));
  EXPECT_EQ(pos, write_buf.get_serialize_size());
  pos = 0;
  LogWriteBuf write_buf_check;
  EXPECT_EQ(OB_SUCCESS, write_buf_check.deserialize(buf, buf_len, pos));
  const char *data_buf = NULL;
  int64_t data_buf_len = 0;
  EXPECT_EQ(OB_SUCCESS, write_buf_check.get_write_buf(0, data_buf, data_buf_len));
  EXPECT_EQ(buf_check_len, write_buf.get_total_size());
  EXPECT_EQ(0, strncmp(buf_check, data_buf, buf_check_len));
  PALF_LOG(INFO, "runlin trace", K(data_buf), K(buf_check));

}

} // end namespace unittest
} // end namespace oceanbase

int main(int argc, char **argv)
{
  OB_LOGGER.set_file_name("test_log_writer_utils.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_log_writer_utils");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
