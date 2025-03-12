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

#ifndef MOCK_OB_TMP_FILE_UTIL_H_
#define MOCK_OB_TMP_FILE_UTIL_H_

#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "mittest/mtlenv/mock_tenant_module_env.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase
{
using namespace tmp_file;

class MockTmpFileUtil
{
public:
  struct MOCK_SEND_IO_MODE
  {
  public:
    enum MODE
    {
      NORMAL = 0,
      MOCK_DISK_HUNG,
      MOCK_SERVER_OUTOF_DISK_SPACE, // all flush tasks write block return OB_SERVER_OUTOF_DISK_SPACE
      MOCK_SUCC_FOR_FIRST_X_TASK,   // write block succ for the first X flush task, return OB_SERVER_OUTOF_DISK_SPACE afterwards
      MOCK_FAIL_FOR_FIRST_X_TASK    // write block return OB_SERVER_OUTOF_DISK_SPACE error for the first X flush task, and succ for the rest
    };
  public:
    void reset();
    TO_STRING_KV(K(X_), K(io_cnt_), K(mode_));
  public:
    int64_t X_;      // let the first X task send IO succ/fail according to mode_
    int64_t io_cnt_; // total send io task cnt
    MODE mode_;
  };

  enum MOCK_WAIT_IO_MODE
  {
    NORMAL = 0,
    MOCK_ALL_IO_TIMEOUT,
    MOCK_META_IO_TIMEOUT // only meta task io timeout
  };

  struct MOCK_THREAD_RUNNING_CTRL
  {
  public:
    enum MODE
    {
      SYNC = 0,
      ASYNC = 1 // ATTENTION! need to set tenant_ctx_ pointer when using this mode
    };
  public:
    MOCK_THREAD_RUNNING_CTRL() : mode_(SYNC), mock_async_thread_(nullptr), tenant_ctx_(nullptr), mock_swap_tg_(nullptr) {}
    void reset();
  public:
    MODE mode_;
    std::thread *mock_async_thread_;
    ObTenantBase *tenant_ctx_;
    lib::TGRunnable *mock_swap_tg_;
  };

public:
  static MockTmpFileUtil &get_instance()
  {
    static MockTmpFileUtil instance;
    return instance;
  }
  void reset() {
    mock_send_io_mode.reset();
    ATOMIC_SET(&mock_wait_io_mode, NORMAL);
    mock_thread_running_ctrl.reset();
  }
  void check_wbp_free_list(ObTmpWriteBufferPool &wbp);
  // MOCK_SEND_IO_MODE
  void set_send_mode(MOCK_SEND_IO_MODE::MODE mode) { ATOMIC_SET(&mock_send_io_mode.mode_, mode); }
  MOCK_SEND_IO_MODE::MODE get_send_mode() { return ATOMIC_LOAD(&mock_send_io_mode.mode_); }
  void inc_io_cnt() { ATOMIC_INC(&mock_send_io_mode.io_cnt_); }
  void set_io_cnt(int64_t io_cnt) { ATOMIC_SET(&mock_send_io_mode.io_cnt_, io_cnt); }
  int64_t get_io_cnt() { return ATOMIC_LOAD(&mock_send_io_mode.io_cnt_); }
  void set_X(int64_t X) { ATOMIC_SET(&mock_send_io_mode.X_, X); }
  int64_t get_X() { return ATOMIC_LOAD(&mock_send_io_mode.X_); }
  // MOCK_WAIT_IO_MODE
  void set_wait_mode(MOCK_WAIT_IO_MODE mode) { ATOMIC_SET(&mock_wait_io_mode, mode); }
  MOCK_WAIT_IO_MODE get_wait_mode() { return ATOMIC_LOAD(&mock_wait_io_mode); }
  // MOCK_THREAD_RUNNING_CTRL
  void set_async_mode(ObTenantBase *tenant_ctx, lib::TGRunnable *mock_swap_tg);
  MOCK_THREAD_RUNNING_CTRL::MODE get_thread_running_mode() { return mock_thread_running_ctrl.mode_; }
  // common
  void save_generate_error_code(int ret) { ATOMIC_SET(&generate_ret, ret); }
  int get_generate_error_code() { return ATOMIC_LOAD(&generate_ret); }

public:
  MOCK_SEND_IO_MODE mock_send_io_mode;
  MOCK_WAIT_IO_MODE mock_wait_io_mode;
  MOCK_THREAD_RUNNING_CTRL mock_thread_running_ctrl;
  int generate_ret; // background thread will override error code,
                    // we save unexpected error code here to check flush status.
};

void MockTmpFileUtil::MOCK_SEND_IO_MODE::reset()
{
  ATOMIC_SET(&X_, 10);
  ATOMIC_SET(&io_cnt_, 0);
  ATOMIC_SET(&mode_, NORMAL);
}

void MockTmpFileUtil::MOCK_THREAD_RUNNING_CTRL::reset()
{
  if (MODE::ASYNC == mode_) {
    LOG_DEBUG("reset mock thread running ctrl in ASYNC mode", KP(mock_async_thread_), K(mock_swap_tg_));
    if (nullptr != mock_async_thread_ && nullptr != mock_swap_tg_) {
      LOG_DEBUG("stop mock_async_thread");
      ATOMIC_SET(&mock_swap_tg_->stop_, true);
      mock_async_thread_->join();
      delete mock_async_thread_;
      mock_async_thread_ = nullptr;
      LOG_DEBUG("stop mock_async_thread finish");
    }
  }
  mode_ = SYNC;
  mock_async_thread_ = nullptr;
  tenant_ctx_ = nullptr;
  mock_swap_tg_ = nullptr;
}

void MockTmpFileUtil::set_async_mode(ObTenantBase *tenant_ctx, lib::TGRunnable *mock_swap_tg)
{
  mock_thread_running_ctrl.mode_ = MOCK_THREAD_RUNNING_CTRL::ASYNC;
  mock_thread_running_ctrl.tenant_ctx_ = tenant_ctx;
  mock_thread_running_ctrl.mock_swap_tg_ = mock_swap_tg;
  if (nullptr == mock_thread_running_ctrl.mock_async_thread_) {
    mock_thread_running_ctrl.mock_async_thread_ = new std::thread([&]() {
      int ret = OB_SUCCESS;
      ASSERT_NE(nullptr, tenant_ctx);
      ASSERT_NE(nullptr, mock_swap_tg);
      ObTenantEnv::set_tenant(mock_thread_running_ctrl.tenant_ctx_);
      LOG_INFO("mock_async_thread start");
      mock_thread_running_ctrl.mock_swap_tg_->run1();
    });
  }
}

// Verify that the free_page_list accurately reflects all available free pages,
// ensuring that max_page_num - used_page_num equals free_page_size.
void MockTmpFileUtil::check_wbp_free_list(ObTmpWriteBufferPool &wbp)
{
  int ret = OB_SUCCESS;
  int64_t max_page_num = wbp.get_max_page_num();
  ObArray<uint32_t> data_page_id;
  ObArray<uint32_t> meta_page_id;

  int64_t free_page_num = max_page_num - wbp.used_page_num_;
  LOG_INFO("checking free page num", K(free_page_num), K(wbp.used_page_num_));
  wbp.print_statistics();
  EXPECT_GT(free_page_num, 0);

  // alloc data
  for (int32_t i = 0; OB_SUCC(ret) && i < free_page_num; ++i) {
    char *buf = nullptr;
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (OB_FAIL(wbp.alloc_page(0/*fd*/, ObTmpFilePageUniqKey(i), new_page_id, buf))) {
      LOG_WARN("wbp fail to alloc data page", K(wbp.shrink_ctx_));
    } else {
      ASSERT_EQ(OB_SUCCESS, data_page_id.push_back(new_page_id));
    }
  }
  ret = OB_SUCCESS;

  // alloc meta
  for (int32_t i = 0; OB_SUCC(ret) && i < free_page_num; ++i) {
    char *buf = nullptr;
    uint32_t new_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
    if (OB_FAIL(wbp.alloc_page(0/*fd*/, ObTmpFilePageUniqKey(0, i), new_page_id, buf))) {
      LOG_WARN("wbp fail to alloc meta page", K(wbp.shrink_ctx_));
    } else {
      ASSERT_EQ(OB_SUCCESS, meta_page_id.push_back(new_page_id));
    }
  }
  LOG_INFO("allocate all usable free pages",
           K(free_page_num), K(data_page_id.size()), K(meta_page_id.size()));
  wbp.print_statistics();
  EXPECT_EQ(free_page_num, data_page_id.size() + meta_page_id.size());

  // free all pages
  ret = OB_SUCCESS;
  uint32_t next_page_id = ObTmpFileGlobal::INVALID_PAGE_ID;
  for (int32_t i = 0; OB_SUCC(ret) && i < data_page_id.size(); ++i) {
    ret = wbp.free_page(0/*fd*/, data_page_id.at(i), ObTmpFilePageUniqKey(i), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
  for (int32_t i = 0; OB_SUCC(ret) && i < meta_page_id.size(); ++i) {
    ret = wbp.free_page(0/*fd*/, meta_page_id.at(i), ObTmpFilePageUniqKey(0, i), next_page_id);
    ASSERT_EQ(OB_SUCCESS, ret);
  }
}

#define MockIO (MockTmpFileUtil::get_instance())

} // namespace oceanbase
#endif