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

#ifndef OB_TMP_FILE_FLUSH_MGR_TEST_HELPER_
#define OB_TMP_FILE_FLUSH_MGR_TEST_HELPER_

#include "mittest/mtlenv/storage/tmp_file/mock_ob_tmp_file_util.h"
#include "mittest/mtlenv/mock_tenant_module_env.h"
#define USING_LOG_PREFIX STORAGE
#define protected public
#define private public
#include "storage/blocksstable/ob_data_file_prepare.h"
#include "share/ob_simple_mem_limit_getter.h"
#include "lib/alloc/ob_malloc_allocator.h"

namespace oceanbase
{
using namespace common;
using namespace blocksstable;
using namespace tmp_file;
using namespace storage;
using namespace share::schema;

/* ------------------------------ Mock tmp file classes ------------------------------ */
class MockTmpFileSwapTg : public ObTmpFileSwapTG
{
public:
  MockTmpFileSwapTg(ObTmpWriteBufferPool &wbp,
                    ObTmpFileEvictionManager &elimination_mgr,
                    ObTmpFileFlushTG &flush_tg,
                    ObTmpFilePageCacheController &pc_ctrl)
    : ObTmpFileSwapTG(wbp, elimination_mgr, flush_tg, pc_ctrl) {}
  virtual int init() override;
};

class MockTmpFileFlushTg : public ObTmpFileFlushTG // for mock_flush_mgr_;
{
public:
  MockTmpFileFlushTg(ObTmpWriteBufferPool &wbp,
                     ObTmpFileFlushManager &flush_mgr,
                     ObIAllocator &allocator,
                     ObTmpFileBlockManager &tmp_file_block_mgr)
  : ObTmpFileFlushTG(wbp, flush_mgr, allocator, tmp_file_block_mgr) {}
};

class MockTmpFileFlushTask : public ObTmpFileFlushTask
{
public:
  virtual int write_one_block() override;
  virtual int wait_macro_block_handle() override;
};

class MockTmpFileFlushManager : public ObTmpFileFlushManager
{
public:
  MockTmpFileFlushManager(ObTmpFilePageCacheController &pc_ctrl)
    : ObTmpFileFlushManager(pc_ctrl) {}
  virtual int alloc_flush_task(ObTmpFileFlushTask *&flush_task) override;
};

class MockTmpFilePageCacheController : public ObTmpFilePageCacheController
{
public:
  MockTmpFilePageCacheController(ObTmpFileBlockManager &tmp_file_block_mgr)
    : ObTmpFilePageCacheController(tmp_file_block_mgr),
      mock_flush_mgr_(*this),
      mock_flush_tg_(write_buffer_pool_, mock_flush_mgr_, task_allocator_, tmp_file_block_manager_),
      mock_swap_tg_(write_buffer_pool_, evict_mgr_, mock_flush_tg_, *this) {}
  virtual int init() override;
  virtual int invoke_swap_and_wait(int64_t expect_swap_size, int64_t timeout_ms) override;
  int inner_origin_invoke_swap_and_wait_(int64_t expect_swap_size, int64_t timeout_ms);
private:
  MockTmpFileFlushManager mock_flush_mgr_;
  MockTmpFileFlushTg mock_flush_tg_;
  MockTmpFileSwapTg mock_swap_tg_;
};

class MockSharedNothingTmpFile : public ObSharedNothingTmpFile
{
public:
  // TODO: generate_data_xxx
  // TODO: 在MockIO里存一份错误码
  int generate_meta_flush_info(
      ObTmpFileFlushTask &flush_task,
      ObTmpFileFlushInfo &info,
      ObTmpFileTreeFlushContext &meta_flush_context,
      const int64_t flush_sequence,
      const bool need_flush_tail) override;
};

class MockSNTenantTmpFileManager : public ObSNTenantTmpFileManager
{
public:
  MockSNTenantTmpFileManager() : mock_page_cache_controller_(tmp_file_block_manager_) {}
  virtual int init_sub_module_() override;
  virtual int open(int64_t &fd, const int64_t &dir_id, const char* const label) override;
private:
  MockTmpFilePageCacheController mock_page_cache_controller_;
};

class MockTenantTmpFileManager : public ObTenantTmpFileManager
{
public:
  virtual ObSNTenantTmpFileManager &get_sn_file_manager() override { return mock_sn_tmp_file_mgr_; }
  virtual int init() override;
private:
  MockSNTenantTmpFileManager mock_sn_tmp_file_mgr_;
};

// ------------------------- Mock functions implementation -----------------------//
int MockTenantTmpFileManager::init()
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(is_inited_)) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ObTenantTmpFileManager init twice", K(ret), K(is_inited_));
  } else {
    if (OB_FAIL(mock_sn_tmp_file_mgr_.init())) { /* replace sn_file_mgr_ with mock_sn_tmp_file_mgr_ */
      LOG_WARN("fail to init sn tmp file manager", KR(ret));
    } else {
      LOG_INFO("MockTenantTmpFileManager init sn file manager complete", KP(&get_sn_file_manager()), KP(&sn_file_manager_));
    }
  }

  if (OB_SUCC(ret)) {
    is_inited_ = true;
  }
  LOG_INFO("MockTenantTmpFileManager init success", KR(ret));
  return ret;
}

int MockTmpFileSwapTg::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFileSwapTG init twice");
  } else if (OB_FAIL(idle_cond_.init(ObWaitEventIds::NULL_EVENT))) {
    STORAGE_LOG(WARN, "failed to init condition variable", KR(ret));
  } else {
    is_inited_ = true;
    last_swap_timestamp_ = 0;
    swap_job_num_ = 0;
    working_list_size_ = 0;
    flush_io_finished_round_ = 0;
  }
  return ret;
}

int MockTmpFileFlushTask::write_one_block()
{
  int ret = OB_SUCCESS;
  int mock_ret = OB_SUCCESS;
  bool is_io_error = false;
  MockIO.inc_io_cnt();
  if (MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_DISK_HUNG == MockIO.get_send_mode()) {
    is_io_error = true;
  } else if (MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_SERVER_OUTOF_DISK_SPACE == MockIO.get_send_mode()) {
    is_io_error = true;
  } else if (MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_SUCC_FOR_FIRST_X_TASK == MockIO.get_send_mode()) {
    if (MockIO.get_io_cnt() > MockIO.get_X()) {
      is_io_error = true;
    }
  } else if (MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_FAIL_FOR_FIRST_X_TASK == MockIO.get_send_mode()) {
    if (MockIO.get_io_cnt() <= MockIO.get_X()) {
      is_io_error = true;
    }
  }

  ret = ObTmpFileFlushTask::write_one_block();
  if (is_io_error) {
    // override ret by mock_error_code
    if (MockTmpFileUtil::MOCK_SEND_IO_MODE::MOCK_DISK_HUNG == MockIO.get_send_mode()) {
      ret = OB_DISK_HUNG;
    } else {
      ret = OB_SERVER_OUTOF_DISK_SPACE;
    }
    write_block_ret_code_ = ret;
    LOG_DEBUG("MockTmpFileFlushTask use mock io error", KR(ret), KPC(this));
  } else {
    LOG_DEBUG("MockTmpFileFlushTask send io succ normally", KR(ret), KPC(this));
  }
  return ret;
}

int MockTmpFileFlushTask::wait_macro_block_handle()
{
  int ret = OB_SUCCESS;
  int64_t rand_num = 0;
  switch (MockIO.get_wait_mode()) {
    case MockTmpFileUtil::MOCK_WAIT_IO_MODE::NORMAL:
      ret = ObTmpFileFlushTask::wait_macro_block_handle();
      break;
    case MockTmpFileUtil::MOCK_WAIT_IO_MODE::MOCK_ALL_IO_TIMEOUT:
      ret = ObTmpFileFlushTask::wait_macro_block_handle();
      if (OB_SUCC(ret)) {
        if (ObTimeUtil::current_time() - create_ts_ < 1 * 1000 * 1000) {
          ret = OB_EAGAIN; // set ret code as if IO is not finished
        } else {
          io_result_ret_code_ = OB_TIMEOUT;
        }
      }
      break;
    case MockTmpFileUtil::MOCK_WAIT_IO_MODE::MOCK_META_IO_TIMEOUT:
      ret = ObTmpFileFlushTask::wait_macro_block_handle();
      if (OB_SUCC(ret) && get_type() == TaskType::META) {
        if (ObTimeUtil::current_time() - create_ts_ < 1 * 1000 * 1000) {
          ret = OB_EAGAIN;
        } else {
          io_result_ret_code_ = OB_TIMEOUT;
        }
      }
      break;
    default:
      LOG_WARN("unknown wait mode", KR(ret), K(io_result_ret_code_), K(MockIO.get_wait_mode()));
      break;
  }
  LOG_DEBUG("MockTmpFileFlushTask::wait_macro_block_handle", KR(ret), KPC(this));
  return ret;
}

int MockTmpFileFlushManager::alloc_flush_task(ObTmpFileFlushTask *&flush_task)
{
  int ret = OB_SUCCESS;
  flush_task = nullptr;

  void *task_buf = nullptr;
  if (OB_ISNULL(task_buf = task_allocator_.alloc(sizeof(MockTmpFileFlushTask)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for flush callback", KR(ret));
  } else {
    flush_task = new (task_buf) MockTmpFileFlushTask();
  }
  return ret;
}

int MockTmpFilePageCacheController::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    STORAGE_LOG(WARN, "ObTmpFilePageCacheController init twice");
  } else if (OB_FAIL(task_allocator_.init(lib::ObMallocAllocator::get_instance(),
                                          OB_MALLOC_MIDDLE_BLOCK_SIZE,
                                          ObMemAttr(MTL_ID(), "UTTmpFileCtl", ObCtxIds::DEFAULT_CTX_ID)))) {
    STORAGE_LOG(WARN, "fail to init task allocator", KR(ret));
  } else if (OB_FAIL(mock_flush_mgr_.init())) { /* init mock flush mgr */
    STORAGE_LOG(WARN, "fail to init mock flush task mgr", KR(ret));
  } else if (OB_FAIL(flush_priority_mgr_.init())) {
    STORAGE_LOG(WARN, "fail to init flush priority mgr", KR(ret));
  } else if (OB_FAIL(write_buffer_pool_.init())) {
    STORAGE_LOG(WARN, "fail to init write buffer pool", KR(ret));
  } else if (OB_FAIL(mock_flush_tg_.init())) { /* init mock flush tg and start timer threads */
    STORAGE_LOG(WARN, "fail to init mock flush thread", KR(ret));
  } else if (OB_FAIL(mock_flush_tg_.start())) {
    STORAGE_LOG(WARN, "fail to start mock flush thread", KR(ret));
  } else if (OB_FAIL(mock_swap_tg_.init())) { /* init mock swap tg */
    STORAGE_LOG(WARN, "fail to init mock swap thread", KR(ret));
  }  else {
    flush_all_data_ = false;
    is_inited_ = true;
    LOG_INFO("MockTmpFilePageCacheController init successful");
  }
  return ret;
}

int MockTmpFilePageCacheController::invoke_swap_and_wait(int64_t expect_swap_size, int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  switch (MockIO.get_thread_running_mode()) {
    case MockTmpFileUtil::MOCK_THREAD_RUNNING_CTRL::SYNC:
      // manually invoke swap and flush in front thread,
      // allowing tmp file write succ even if buffer pool is full
      mock_swap_tg_.swap();
      for (int64_t i = 0; i < 3; ++i) {
        mock_swap_tg_.flush_tg_ref_.try_work();
        usleep(5 * 1000);
      }
      mock_swap_tg_.swap();
      break;
    case MockTmpFileUtil::MOCK_THREAD_RUNNING_CTRL::ASYNC:
      ret = inner_origin_invoke_swap_and_wait_(expect_swap_size, timeout_ms);
      break;
  }
  return ret;
}

int MockTmpFilePageCacheController::inner_origin_invoke_swap_and_wait_(int64_t expect_swap_size, int64_t timeout_ms)
{
  int ret = OB_SUCCESS;
  void *task_buf = nullptr;
  ObTmpFileSwapJob *swap_job = nullptr;
  if (OB_ISNULL(task_buf = task_allocator_.alloc(sizeof(ObTmpFileSwapJob)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    STORAGE_LOG(WARN, "fail to allocate memory for swap job", KR(ret));
  } else if (FALSE_IT(swap_job = new (task_buf) ObTmpFileSwapJob())) {
  } else if (OB_FAIL(swap_job->init(expect_swap_size, timeout_ms))) {
    STORAGE_LOG(WARN, "fail to init sync swap job", KR(ret), KPC(swap_job));
  } else if (OB_FAIL(mock_swap_tg_.swap_job_enqueue(swap_job))) { // replace swap_tg_ with mock_swap_tg_ to inject IO error
    STORAGE_LOG(WARN, "fail to enqueue swap job", KR(ret), KPC(swap_job));
  } else {
    mock_swap_tg_.notify_doing_swap();
    if (OB_FAIL(swap_job->wait_swap_complete())) {
      STORAGE_LOG(WARN, "fail to wait for swap job complete timeout", KR(ret));
    }
  }

  if (OB_NOT_NULL(swap_job)) {
    if (OB_SUCCESS != swap_job->get_ret_code()) {
      ret = swap_job->get_ret_code();
    }
    swap_job->reset();
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(free_swap_job_(swap_job))) {
      STORAGE_LOG(ERROR, "fail to free swap job", KR(ret), KR(tmp_ret));
    }
  }
  return ret;
}

int MockSharedNothingTmpFile::generate_meta_flush_info(
    ObTmpFileFlushTask &flush_task,
    ObTmpFileFlushInfo &info,
    ObTmpFileTreeFlushContext &meta_flush_context,
    const int64_t flush_sequence,
    const bool need_flush_tail)
{
  int ret = OB_SUCCESS;
  MockIO.save_generate_error_code(OB_SUCCESS);
  ret = ObSharedNothingTmpFile::generate_meta_flush_info(flush_task, info, meta_flush_context, flush_sequence, need_flush_tail);
  if (OB_FAIL(ret) && OB_ITER_END != ret) {
    LOG_DEBUG("detect generate meta flush info failed, save error code", KR(ret));
    MockIO.save_generate_error_code(ret);

    if (MockTmpFileUtil::MOCK_THREAD_RUNNING_CTRL::ASYNC == MockIO.get_thread_running_mode()) {
      printf("mock generate meta flush info encounter unexpected error %d in ASYNC mode, abort\n", ret);
      LOG_ERROR("mock generate meta flush info encounter unexpected error in ASYNC mode, abort", K(ret), KPC(this));
      abort();
    }
  }
  return ret;
}

int MockSNTenantTmpFileManager::init_sub_module_()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(tmp_file_block_manager_.init(tenant_id_))) {
    LOG_WARN("fail to init tenant tmp file block manager", KR(ret));
  } else if (OB_FAIL(mock_page_cache_controller_.init())) {
    LOG_WARN("fail to init page cache controller", KR(ret));
  } else {
    mock_page_cache_controller_.mock_swap_tg_.set_stop(false); /* unset mock thread stop flag */
    is_running_ = true; /* manually set is_running_ flag */
    LOG_INFO("MockSNTenantTmpFileManager init successful", K(tenant_id_), KP(this));
  }
  return ret;
}

int MockSNTenantTmpFileManager::open(int64_t &fd, const int64_t &dir_id, const char* const label)
{
  int ret = OB_SUCCESS;
  fd = ObTmpFileGlobal::INVALID_TMP_FILE_FD;
  void *buf = nullptr;
  MockSharedNothingTmpFile *tmp_file = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("MockSNTenantTmpFileManager has not been inited", KR(ret), K(tenant_id_));
  } else if (OB_ISNULL(buf = tmp_file_allocator_.alloc(sizeof(MockSharedNothingTmpFile),
                                                       lib::ObMemAttr(tenant_id_, "UTSNTmpFile")))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("fail to allocate memory for tmp file",
             KR(ret), K(tenant_id_), K(sizeof(MockSharedNothingTmpFile)));
  } else if (FALSE_IT(tmp_file = new (buf) MockSharedNothingTmpFile())) {
  } else if (FALSE_IT(fd = ATOMIC_AAF(&current_fd_, 1))) {
  } else if (OB_FAIL(tmp_file->init(tenant_id_, fd, dir_id,
                                    &tmp_file_block_manager_, &callback_allocator_,
                                    &wbp_index_cache_allocator_, &wbp_index_cache_bucket_allocator_,
                                    &mock_page_cache_controller_, label))) { /* use mock page cache controller */
    LOG_WARN("fail to init tmp file", KR(ret), K(fd), K(dir_id));
  } else if (OB_FAIL(files_.insert(ObTmpFileKey(fd), static_cast<ObSharedNothingTmpFile *>(tmp_file)))) {
    LOG_WARN("fail to set refactored to tmp file map", KR(ret), K(fd), KP(tmp_file));
  }

  if (OB_FAIL(ret) && OB_NOT_NULL(tmp_file)) {
    tmp_file->~MockSharedNothingTmpFile();
    tmp_file_allocator_.free(tmp_file);
    tmp_file = nullptr;
  }

  LOG_INFO("open a tmp file over", KR(ret), K(fd), K(dir_id), KP(tmp_file), K(lbt()));
  return ret;
}

} // namespace oceanbase
#endif