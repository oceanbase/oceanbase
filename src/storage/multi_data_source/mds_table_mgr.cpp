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
#include "lib/ob_errno.h"
#include "ob_tablet_id.h"
#define USING_LOG_PREFIX MDS

#include "mds_table_mgr.h"
#include "mds_table_handle.h"
#include "storage/ls/ob_ls.h"
#include "storage/ls/ob_ls_tablet_service.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "storage/tablet/ob_tablet_iterator.h"
#include "storage/multi_data_source/mds_table_base.h"
#include "storage/meta_mem/ob_tablet_pointer.h"

namespace oceanbase {

using namespace share;

namespace storage {
namespace mds {

int ObMdsTableMgr::init(ObLS *ls)
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    MDS_LOG(WARN, "init mds table mgr twice", KR(ret), KPC(this));
  } else if (OB_ISNULL(ls) || OB_ISNULL(ls->get_tx_svr())) {
    ret = OB_ERR_UNEXPECTED;
    MDS_LOG(ERROR, "invalid ls when init mds table mgr", KR(ret), KPC(ls), KPC(this));
  } else if (OB_FAIL(ls->get_tx_svr()->register_common_checkpoint(checkpoint::MDS_TABLE_TYPE, this))) {
    MDS_LOG(WARN, "register common checkpoint failed", KR(ret), KPC(this));
  } else if (OB_FAIL(mds_table_map_.init("MdsTableMgr", MTL_ID()))) {
    MDS_LOG(ERROR, "fail to init mds table map", KR(ret), KPC(ls), KPC(this));
  } else {
    ls_ = ls;
    is_inited_ = true;
  }

  return ret;
}

int ObMdsTableMgr::reset()
{
  int ret = OB_SUCCESS;
  is_inited_ = false;
  is_freezing_ = false;
  rec_log_ts_ = 0;
  ref_cnt_ = 0;
  mds_table_cnt_ = 0;
  ls_ = nullptr;
  if (OB_FAIL(mds_table_map_.reset())) {
    MDS_LOG(WARN, "fail to reset mds_table_map", KR(ret));
  }
  return ret;
}

void ObMdsTableMgr::destroy() { reset(); }

int ObMdsTableMgr::register_to_mds_table_mgr(MdsTableBase *p_mds_table)
{
  int ret = OB_SUCCESS;
  common::ObTabletID tablet_id;
  if (OB_ISNULL(p_mds_table)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(ERROR, "invalid mdstable handle", KR(ret), KP(p_mds_table));
  } else if (FALSE_IT(tablet_id = p_mds_table->get_tablet_id())) {
  } else {
    int op_result = OB_SUCCESS;
    auto op = [p_mds_table, &op_result](const ObTabletID &id, List<MdsTableBase> &list) {// with map's bucket lock protected
      if (list.empty()) {
        list.insert_into_head(p_mds_table);
      } else {
        MdsTableBase *head_mds_table = static_cast<MdsTableBase *>(list.list_head_);
        if (!head_mds_table->is_removed_from_t3m()) {
          op_result = OB_ENTRY_EXIST;
          MDS_LOG_RET(INFO, op_result, "register meet conflct", K(id), K(list), KPC(p_mds_table));
        } else {
          list.insert_into_head(p_mds_table);
        }
      }
      return true;
    };
    bool need_break = false;
    List<MdsTableBase> tmp_list;
    tmp_list.append(p_mds_table);
    while (!need_break) {
      if (OB_FAIL(mds_table_map_.insert(tablet_id, tmp_list))) {
        if (OB_ENTRY_EXIST == ret) {
          ret = OB_SUCCESS;
          if (OB_FAIL(mds_table_map_.operate(tablet_id, op))) {
            if (OB_ENTRY_NOT_EXIST == ret) {// meet concurrent erase
              ret = OB_SUCCESS;
            } else {// meet errors can't handle, no need retry
              need_break = true;
              MDS_LOG(WARN, "operate list failed and ret is not ENTRY_NOT_EXIST", KR(ret), K(tablet_id));
            }
          } else {// insert to existing list success, no need retry
            need_break = true;
          }
        } else {
          need_break = true;// meet errors can't handle, no need retry
          MDS_LOG(WARN, "insert list failed and ret is not ENTRY_EXIST", KR(ret), K(tablet_id));
        }
      } else {
        need_break = true;// insert new list success, no need retry
      }
    };
    if (OB_FAIL(ret)) {
      // do nothing
    } else {
      ret = op_result;
      if (OB_SUCC(ret)) {
        MDS_LOG(INFO, "register success", KR(ret), KPC(p_mds_table));
      }
    }
  }
  return ret;
}

int ObMdsTableMgr::unregister_from_mds_table_mgr(MdsTableBase *p_mds_table)
{
  int ret = OB_SUCCESS;
  common::ObTabletID tablet_id;
  auto op = [p_mds_table](const ObTabletID &id, List<MdsTableBase> &list) {// with map's bucket lock protected
    if (list.check_node_exist(p_mds_table)) {
      list.del(p_mds_table);
    } else {
      MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "mds table not in list", KPC(p_mds_table), K(id), K(list));
      ob_abort();
    }
    return list.empty();
  };
  if (OB_ISNULL(p_mds_table)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(ERROR, "invalid mdstable handle", KR(ret), KP(p_mds_table));
  } else if (FALSE_IT(tablet_id = p_mds_table->get_tablet_id())) {
  } else if (OB_FAIL(mds_table_map_.erase_if(tablet_id, op))) {
    if (OB_EAGAIN == ret) {
      ret = OB_SUCCESS;
    } else {
      MDS_LOG(WARN, "fail to erase kv", KR(ret), K(tablet_id));
    }
  } else {
    MDS_LOG(INFO, "unregister success", KR(ret), KPC(p_mds_table));
  }
  return ret;
}

int ObMdsTableMgr::flush(SCN recycle_scn, bool need_freeze)
{
  int ret = OB_SUCCESS;
  MDS_LOG(DEBUG, "start flush", "ls_id", ls_->get_ls_id(), K(recycle_scn), K(need_freeze));
  common::ObTimeGuard tg("flush mds tables", 100 * 1000);
  MdsTableFreezeGuard freeze_guard;
  freeze_guard.init(this);
  int64_t flushed_table_cnt = 0;
  MdsTableHandle *flushing_mds_table = nullptr;
  ObLSID ls_of_flushing_tablet;
  ObTabletID flushing_tablet;
  auto flush_op =
  [&flushed_table_cnt, recycle_scn, need_freeze](const common::ObTabletID &tablet_id,
                                                 List<MdsTableBase> &mds_table_list) {
    int tmp_ret = OB_SUCCESS;
    if (mds_table_list.empty()) {
      MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "meet empty mds table list", K(tablet_id));
    } else {
      MdsTableBase *mds_table = static_cast<MdsTableBase *>(mds_table_list.list_head_);
      if (mds_table->is_removed_from_t3m()) {
        // jsut skip it
      } else if (OB_TMP_FAIL(mds_table->flush(recycle_scn, need_freeze))) {
        MDS_LOG_RET(WARN, ret, "flush mds table failed", KR(tmp_ret), K(tablet_id));
      } else {
        flushed_table_cnt++;
      }
    }
    // true means iterating the next mds table
    return true;
  };

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    MDS_LOG(ERROR, "regsiter mds table failed", KR(ret));
  } else if (!freeze_guard.can_freeze()) {
    if (REACH_TIME_INTERVAL(2 * 1000 * 1000 /*2 seconds*/)) {
      MDS_LOG(INFO, "mds table is flushing, skip flush once", K(recycle_scn), KPC(this));
    }
  } else if (has_flushing_mds_table_(ls_of_flushing_tablet, flushing_tablet)) {
    // if there are some mds tables flushing, skip flushing this time
    ret = OB_EAGAIN;
    if (REACH_TIME_INTERVAL(10 * 1000 * 1000 /*10 seconds*/)) {
      MDS_LOG(INFO, "skip trigger logstream mds table flush due to some mds tables are flushing",
              KR(ret), K(ls_of_flushing_tablet), K(flushing_tablet));
    }
  } else {
    if (OB_FAIL(mds_table_map_.for_each(flush_op))) {
      MDS_LOG(WARN, "fail to do map for_each", KR(ret), KPC(flushing_mds_table));
    }
  }
  MDS_LOG(DEBUG, "[MdsTableMgr] finish flush", "ls_id", ls_->get_ls_id(), K(flushed_table_cnt));
  return ret;
}

bool ObMdsTableMgr::has_flushing_mds_table_(ObLSID &ls_id_of_flushing_tablet,
                                            ObTabletID &flushing_tablet_id)
{
  int ret = OB_SUCCESS;
  bool has_flushing_mds_table_ = false;

  auto find_first_flushing_mds_table_and_retry =
  [&has_flushing_mds_table_,
   &ls_id_of_flushing_tablet,
   &flushing_tablet_id] (const common::ObTabletID &tablet_id, List<MdsTableBase> &mds_table_list) {
    int tmp_ret = OB_SUCCESS;
    bool is_flushing = false;
    if (mds_table_list.empty()) {
      MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "meet empty mds table list", K(tablet_id));
    } else {
      MdsTableBase *p_mds_table = static_cast<MdsTableBase *>(mds_table_list.list_head_);
      if (p_mds_table->is_removed_from_t3m()) {
        // jsut skip it
      } else if (FALSE_IT(is_flushing = p_mds_table->is_flushing())) {
      } else if (is_flushing) {
        if (OB_TMP_FAIL(p_mds_table->flush(SCN::max_scn() /*recycle_scn*/, false /*need_freeze*/))) {
          MDS_LOG_RET(WARN, tmp_ret, "flush mds table failed", KR(tmp_ret), KPC(p_mds_table));
        }
        if (!flushing_tablet_id.is_valid()) {
          ls_id_of_flushing_tablet = p_mds_table->get_ls_id();
          flushing_tablet_id = p_mds_table->get_tablet_id();
        }
        has_flushing_mds_table_ = true;
      }
    }
    // true means iterating the next mds table
    return true;
  };
  if (OB_FAIL(mds_table_map_.for_each(find_first_flushing_mds_table_and_retry))) {
    MDS_LOG(WARN, "flush mds table failed", KR(ret));
  }
  return has_flushing_mds_table_;
}

SCN ObMdsTableMgr::get_rec_scn()
{
  int ret = OB_SUCCESS;
  // TODO : @gengli FIX ME
  SCN min_rec_scn;
  SCN max_decided_scn = SCN::min_scn();
  common::ObTimeGuard tg("get mds table rec log scn", 100 * 1000);

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    MDS_LOG(ERROR, "regsiter mds table failed", KR(ret));
  } else if (OB_FAIL(ls_->get_max_decided_scn(max_decided_scn))) {
    MDS_LOG(WARN, "get max decided log ts failed", KR(ret));
  } else {
    min_rec_scn = max_decided_scn;
    auto get_scn_op = [&min_rec_scn, &ret](const common::ObTabletID &tablet_id,
                                           List<MdsTableBase> &mds_table_list) {
      SCN rec_scn;
      if (mds_table_list.empty()) {
        MDS_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "meet empty mds table list", K(tablet_id));
      } else {
        MdsTableBase *p_mds_table = static_cast<MdsTableBase *>(mds_table_list.list_head_);
        if (p_mds_table->is_removed_from_t3m()) {
          // jsut skip it
        } else if (FALSE_IT(rec_scn = p_mds_table->get_rec_scn())) {
        } else if (!rec_scn.is_valid()) {
          ret = OB_ERR_UNEXPECTED;
          MDS_LOG(ERROR, "get invalid rec scn", KR(ret));
        } else {
          min_rec_scn = std::min(min_rec_scn, rec_scn);
        }
      }
      // true means iterating the next mds table
      return OB_SUCCESS == ret;
    };
    if (OB_FAIL(mds_table_map_.for_each(get_scn_op))) {
      MDS_LOG(WARN, "fail to do for_each in map", KR(ret));
    }
    // todo zk: retry
    if (OB_FAIL(ret)) {
      min_rec_scn = SCN::min_scn();
    }
  }
  return min_rec_scn;
}

DEF_TO_STRING(ObMdsTableMgr)
{
  int64_t pos = 0;
  J_OBJ_START();
  J_KV(KP(this), K_(is_inited), K_(rec_log_ts), KPC_(ls));
  J_OBJ_END();
  return pos;
}

int MdsTableFreezeGuard::init(ObMdsTableMgr *mds_mgr)
{
  int ret = OB_SUCCESS;
  reset();
  if (OB_ISNULL(mds_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    MDS_LOG(WARN, "invalid tx data table", KR(ret));
  } else {
    can_freeze_ = (false == ATOMIC_CAS(&(mds_mgr->is_freezing_), false, true));
    if (can_freeze_) {
      mds_mgr_ = mds_mgr;
    }
  }
  return ret;
}

void MdsTableFreezeGuard::reset()
{
  can_freeze_ = false;
  if (OB_NOT_NULL(mds_mgr_)) {
    ATOMIC_STORE(&(mds_mgr_->is_freezing_), false);
    mds_mgr_ = nullptr;
  }
}

MdsTableMgrHandle::MdsTableMgrHandle()
  : mgr_(nullptr)
{
}

MdsTableMgrHandle::~MdsTableMgrHandle()
{
  reset();
}

int MdsTableMgrHandle::reset()
{
  if (OB_NOT_NULL(mgr_)) {
    mgr_->dec_ref();
  }
  mgr_ = nullptr;
  return OB_SUCCESS;
}

int MdsTableMgrHandle::set_mds_table_mgr(ObMdsTableMgr *mds_table_mgr)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(mds_table_mgr)) {
    ret = OB_INVALID_ARGUMENT;
    STORAGE_LOG(WARN, "invalid argument", K(ret), KP(mds_table_mgr), K(lbt()));
  } else {
    reset();
    mgr_ = mds_table_mgr;
    mgr_->inc_ref();
  }
  return ret;
}

}  // namespace mds
}  // namespace storage
}  // namespace oceanbase
