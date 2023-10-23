/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "mds_table_handler.h"
#include "lib/lock/ob_small_spin_lock.h"
#include "lib/ob_errno.h"
#include "mds_table_mgr.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/ls/ob_ls.h"
#include "storage/meta_mem/ob_tablet_pointer.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
namespace mds
{

ObMdsTableHandler::~ObMdsTableHandler() { ATOMIC_STORE(&is_written_, false); }

int ObMdsTableHandler::get_mds_table_handle(mds::MdsTableHandle &handle,
                                            const ObTabletID &tablet_id,
                                            const share::ObLSID &ls_id,
                                            const bool not_exist_create,
                                            ObTabletPointer *pointer)
{
  #define PRINT_WRAPPER KR(ret), K(tablet_id), K(ls_id), K(not_exist_create), K(*this)
  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(storage::ObLSService *);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;
  mds::ObMdsTableMgr *mds_table_mgr;

  auto try_get_handle_directly = [this](mds::MdsTableHandle &handle) -> void {
    if (OB_LIKELY(mds_table_handle_.is_valid())) {
      handle = mds_table_handle_;
    }
  };

  handle.reset();
  MDS_TG(5_ms);
  {
    MdsRLockGuard guard(lock_);
    CLICK();
    try_get_handle_directly(handle);
  }

  if (OB_LIKELY(handle.is_valid())) {
    // do nothing
  } else if (OB_LIKELY(!handle.is_valid() && !not_exist_create)) {// mds_table is released and no need create new one
    ret = OB_ENTRY_NOT_EXIST;
  } else {// mds_table is released and need create new one
    MdsWLockGuard guard(lock_);
    try_get_handle_directly(handle);// try again(check if mds_table created between Rlock and Wlock)
    if (OB_UNLIKELY(handle.is_valid())) {
      // do nothing
    } else {
      if (OB_ISNULL(mds_table_mgr_handle_.get_mds_table_mgr())) {
        if (OB_ISNULL(ls_service)) {
          ret = OB_ERR_UNEXPECTED;
          MDS_LOG_INIT(WARN, "ls service should not be NULL");
        } else if (MDS_FAIL(ls_service->get_ls(ls_id,
                                               ls_handle,
                                               ObLSGetMod::TABLET_MOD))) {
          MDS_LOG_INIT(WARN, "failed to get ls");
        } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
          ret = OB_ERR_UNEXPECTED;
          MDS_LOG_INIT(WARN, "ls should not be NULL");
        } else if (MDS_FAIL(ls->get_tablet_svr()->get_mds_table_mgr(mds_table_mgr_handle_))) {
          MDS_LOG_INIT(WARN, "get mds table mgr failed");
        } else if (OB_ISNULL(mds_table_mgr = mds_table_mgr_handle_.get_mds_table_mgr())) {
          ret = OB_ERR_UNEXPECTED;
          MDS_LOG_INIT(ERROR, "mds table mgr is unexpected nullptr");
        }
      }
      if (OB_SUCC(ret)) {
        set_mds_mem_check_thread_local_info(ls_id, tablet_id, typeid(mds::NormalMdsTable).name());
        if (MDS_FAIL(mds_table_handle_.init<mds::NormalMdsTable>(mds::MdsAllocator::get_instance(),
                                                                 tablet_id,
                                                                 ls_id,
                                                                 pointer,
                                                                 mds_table_mgr_handle_.get_mds_table_mgr()))) {
          MDS_LOG_INIT(WARN, "fail to init mds table");
        } else {
          handle = mds_table_handle_;
        }
        reset_mds_mem_check_thread_local_info();
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObMdsTableHandler::try_gc_mds_table()
{
  #define PRINT_WRAPPER KR(ret), K(valid_node_cnt), K(*this)
  MDS_TG(100_ms);
  int ret = OB_SUCCESS;
  int64_t valid_node_cnt = 0;
  bool is_flushing = false;
  if (!mds_table_handle_.is_valid()) {
    // do nothing
  } else if (MDS_FAIL(mds_table_handle_.get_node_cnt(valid_node_cnt))) {// 1. check if count is 0
    MDS_LOG_GC(WARN, "fail to get total valid node cnt");
  } else if (0 != valid_node_cnt) {
    ret = OB_EAGAIN;
    MDS_LOG_GC(DEBUG, "there are valid nodes remains, maybe later");
  } else if (OB_FAIL(mds_table_handle_.is_flushing(is_flushing))) {// 2. check if is in flushing process
    MDS_LOG_GC(WARN, "fail to get mds_table flushing state");
  } else if (is_flushing) {
    ret = OB_EAGAIN;
    MDS_LOG_GC(INFO, "this mds_table is in flushing state, waiting for DAG on_flush() callback");
  } else {
    MdsWLockGuard guard(lock_);// 3. stop incoming incremental accessing to mds_table_handle(there are some stock accessing remain still)
    CLICK();
    if (mds_table_handle_.is_valid()) {
      int64_t handle_ref_cnt = 0;
      if (MDS_FAIL(mds_table_handle_.get_ref_cnt(handle_ref_cnt))) {// 4. check if this is the last ref
        MDS_LOG_GC(WARN, "fail to gc mds_table");
      } else if (handle_ref_cnt != 1) {
        MDS_LOG_GC(INFO, "althrough valid node cnt is 0, but ref cnt is not 0, wait next scan", K(handle_ref_cnt));
      } else if (MDS_FAIL(mds_table_handle_.get_node_cnt(valid_node_cnt))) {
        MDS_LOG_GC(WARN, "fail to get total valid node cnt");
      } else if (0 != valid_node_cnt) {// 5. double check to see if stock accessing write new nodes
        ret = OB_EAGAIN;
        MDS_LOG_GC(DEBUG, "there are valid nodes(by concurrent insert), maybe later");
      } else {
        MDS_LOG_GC(INFO, "success to gc mds_table");
        mds_table_handle_.reset();// 6. release the last ref, will do actual destruction and free here
      }
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

int ObMdsTableHandler::try_release_nodes_below(const share::SCN &scn)
{
  #define PRINT_WRAPPER KR(ret), K(scn), K(*this)
  int ret = OB_SUCCESS;
  MDS_TG(5_ms);
  MdsTableHandle mds_table_handle;
  {
    MdsRLockGuard guard(lock_);
    CLICK();
    mds_table_handle = mds_table_handle_;
  }
  if (mds_table_handle.is_valid()) {
    if (MDS_FAIL(mds_table_handle.try_recycle(scn))) {
      MDS_LOG_GC(WARN, "fail to try recycle");
    }
  }
  return ret;
  #undef PRINT_WRAPPER
}

ObMdsTableHandler &ObMdsTableHandler::operator=(const ObMdsTableHandler &rhs)// value sematic for tablet ponter deep copy
{
  #define PRINT_WRAPPER KR(ret), K(*this)
  int ret = OB_SUCCESS;

  MDS_TG(5_ms);
  MdsWLockGuard guard(lock_);
  CLICK();
  ATOMIC_STORE(&is_written_, ATOMIC_LOAD(&rhs.is_written_));
  mds_table_handle_ = rhs.mds_table_handle_;
  if (OB_NOT_NULL(rhs.mds_table_mgr_handle_.get_mds_table_mgr())) {
    mds_table_mgr_handle_.set_mds_table_mgr(const_cast<ObMdsTableMgr*>(rhs.mds_table_mgr_handle_.get_mds_table_mgr()));
  }
  return *this;
  #undef PRINT_WRAPPER
}

void ObMdsTableHandler::mark_removed_from_t3m(ObTabletPointer *pointer)
{
  int ret = OB_SUCCESS;
  MdsRLockGuard guard(lock_);
  if (mds_table_handle_.is_valid()) {
    if (OB_FAIL(mds_table_handle_.mark_removed_from_t3m(pointer))) {
      MDS_LOG(WARN, "fail to unregister_from_mds_table_mgr", K(*this));
    }
  }
}

}
}
}