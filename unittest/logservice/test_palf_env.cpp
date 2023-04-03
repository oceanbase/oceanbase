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
	LSN lsn;
	int64_t ts;
	palf_env_->create(1, &cb1, handle);
  handle.register_role_change_cb(&cb1);
  PalfHandle handle1;
  handle.register_role_change_cb(&cb1);
  palf_env_->create(2, &cb1, handle1);
  handle1.register_role_change_cb(&cb1);
	EXPECT_EQ(OB_SUCCESS, handle.append(opts, buffer, buf_len, base_ts, &cb, lsn, ts));
	EXPECT_EQ(OB_SUCCESS, handle1.append(opts, buffer, buf_len, base_ts, &cb, lsn, ts));
  palf_env_->close(handle);
  palf_env_->close(handle1);
	sleep(1);
}
}
}

int main(int argc, char **argv)
{
  rmdir("unit_01");
  mkdir("unit_01", 0777);
  unlink("test_palf_env.log");
  OB_LOGGER.set_file_name("test_palf_env.log", true);
  OB_LOGGER.set_log_level("TRACE");
  PALF_LOG(INFO, "begin unittest::test_palf_env");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
