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
#include <stdint.h>
#include "storage/mock_tenant_module_env.h"
#include "lib/file/file_directory_utils.h"
#include "lib/ob_define.h"
#include "logservice/palf/palf_env.h"
#include "logservice/palf/palf_handle.h"
#include "rpc/frame/ob_req_transport.h"
#include "share/allocator/ob_tenant_mutil_allocator.h"

namespace oceanbase
{
namespace unittest
{
using namespace common;
using namespace palf;

int thread_num = 100;
int nbytes = 500;

class MockAppendCbWorker : public AppendCbWorker
{
public:
  int push_append_cb(const int64_t id, AppendCb *cb, const LSN &end_lsn) override final
  {
    int ret = OB_SUCCESS;
    UNUSED(id);
    UNUSED(end_lsn);
    cb->on_success();
    return ret;
  }
};

class MockAppendCb : public AppendCb
{
public:
  MockAppendCb()
  {
    is_called_ = true;
  }

  int on_success() override final
  {
    ATOMIC_STORE(&is_called_, true);
    return true;
  }

  int on_failure() override final
  {
    ATOMIC_STORE(&is_called_, true);
    return true;
  }

  void reset()
  {
    ATOMIC_STORE(&is_called_, false);
  }

  bool is_called() const
  {
    return ATOMIC_LOAD(&is_called_);
  }
private:
  bool is_called_;
};

class StandalonePalfEnv : public ::testing::Test
{
public:
  StandalonePalfEnv()
    : self_(ObAddr::VER::IPV4, "127.0.0.1", 2021),
      palf_env_(NULL),
      transport_(NULL, NULL),
      allocator_(500),
      total_num_(0)
  {
  }
public:
  void SetUp()
  {
    EXPECT_EQ(OB_SUCCESS, MockTenantModuleEnv::get_instance().init());

    int ret = OB_SUCCESS;

    snprintf(log_dir_, OB_MAX_FILE_NAME_LENGTH, "./%s", "standalone_palf_bench");

    FileDirectoryUtils::delete_directory_rec(log_dir_);
    FileDirectoryUtils::create_directory(log_dir_);

    PalfDiskOptions options;
    options.log_disk_usage_limit_size_ = 500 * 1024 * 1024 * 1024LL;

    ObMemberList member_list;
    EXPECT_EQ(OB_SUCCESS, member_list.add_server(self_));

    if (OB_FAIL(PalfEnv::create_palf_env(options, log_dir_, self_, &transport_,
            &allocator_, palf_env_))) {
      PALF_LOG(ERROR, "create_palf_env failed", K(ret));
    } else if (OB_FAIL(palf_env_->create(1, handle_))) {
      PALF_LOG(ERROR, "palf_env_ create failed", K(ret));
    }
    GlobalLearnerList learner_list;
    EXPECT_EQ(OB_SUCCESS, handle_.set_initial_member_list(member_list, 1, learner_list));

    while (true) {
      ObRole role;
      int64_t proposal_id = 0;
      bool is_pending_state = false;

      handle_.get_role(role, proposal_id, is_pending_state);

      if (LEADER == role) {
        break;
      } else {
        usleep(1000);
      }
    }
  }

  void TearDown()
  {
    palf_env_->close(handle_);
    PalfEnv::destroy_palf_env(palf_env_);
    palf_env_ = NULL;

    MockTenantModuleEnv::get_instance().destroy();
  }

  void smoke()
  {
    PalfAppendOptions options;
    options.need_nonblock = false;
    options.need_check_proposal_id = false;

    const int64_t NBYTES = 500;
    char BUFFER[NBYTES];
    memset(BUFFER, 'a', NBYTES);

    const int64_t CB_ARRAY_NUM = 1;
    MockAppendCb cb_array[CB_ARRAY_NUM];

    while(true)
    {
      const int64_t begin_ts = ObTimeUtility::current_time();
      for (int i = 0; i < CB_ARRAY_NUM; i++)
      {
        LSN lsn;
        int64_t ts_ns = 0;
        if (cb_array[i].is_called()) {
          cb_array[i].reset();
          EXPECT_EQ(OB_SUCCESS, handle_.append(options,
                                               BUFFER,
                                               nbytes,
                                               0,
                                               &cb_array[i],
                                               lsn,
                                               ts_ns));

          ATOMIC_INC(&total_num_);
        }
      }
      const int64_t end_ts = ObTimeUtility::current_time();

      if (end_ts - begin_ts < 200) {
        usleep(200 - (end_ts - begin_ts));
      }

      if (REACH_TIME_INTERVAL(1000 * 1000)) {
        PALF_LOG(ERROR, "total_num_", K(total_num_));
        ATOMIC_STORE(&total_num_, 0);
      }
    }
  }

public:
  char log_dir_[OB_MAX_FILE_NAME_LENGTH];
  ObAddr self_;
  PalfEnv *palf_env_;
  MockAppendCbWorker cb_worker_;
  rpc::frame::ObReqTransport transport_;
  ObTenantMutilAllocator allocator_;

  PalfHandle handle_;
  int64_t total_num_;
};

static void *append_thr_fn(void *arg)
{
  StandalonePalfEnv *p = reinterpret_cast<StandalonePalfEnv *>(arg);

  p->smoke();

  return (void *)0;
}

TEST_F(StandalonePalfEnv, TestAppend)
{
  const int64_t THREAD_NUM = 2000;
  pthread_t tids[THREAD_NUM];

  for (int64_t i = 0; i < thread_num; i++) {
    EXPECT_EQ(0, pthread_create(&tids[i], NULL, append_thr_fn, this));
  }

  for (int64_t i = 0; i < thread_num; i++) {
    pthread_join(tids[i], NULL);
  }
}
} // namespace unittest
} // namespace oceanbase

int main(int argc, char **argv)
{
  if (argc > 1) {
    oceanbase::unittest::thread_num = strtol(argv[1], NULL, 10);
    oceanbase::unittest::nbytes = strtol(argv[2], NULL, 10);
  }

  OB_LOGGER.set_file_name("test_palf_bench.log", true);
  OB_LOGGER.set_log_level("ERROR");

  PALF_LOG(INFO, "palf bench begin");
  ::testing::InitGoogleTest(&argc, argv);
  oceanbase::ObClusterVersion::get_instance().update_data_version(DATA_CURRENT_VERSION);
  return RUN_ALL_TESTS();
}
