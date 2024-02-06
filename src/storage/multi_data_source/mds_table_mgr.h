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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_MGR_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_MDS_TABLE_MGR_H

#include "lib/lock/ob_small_spin_lock.h"
#include "lib/lock/ob_tc_rwlock.h"
#include "meta_programming/ob_type_traits.h"
#include "ob_tablet_id.h"
#include "storage/checkpoint/ob_common_checkpoint.h"
#include "storage/multi_data_source/runtime_utility/list_helper.h"
#include "lib/hash/ob_linear_hash_map.h"

namespace oceanbase {
namespace storage {
class ObLS;
class ObTabletHandle;
namespace mds {
class MdsTableFreezeGuard;
class MdsTableBase;

class RemovedMdsTableRecorder// tablet leak will resulting mds table leak, we must prove it
{
public:
  RemovedMdsTableRecorder() = default;
  void record(MdsTableBase *mds_table);
  void del(MdsTableBase *mds_table);
  bool check_is_list_head(MdsTableBase *mds_table) {
    SpinRLockGuard guard(lock_);
    return removed_mds_table_list_.list_head_ == (ListNodeBase *)mds_table;
  }
  template <typename OP>
  void for_each(OP &&op) {
    SpinRLockGuard guard(lock_);
    removed_mds_table_list_.for_each_node_from_head_to_tail_until_true(op);
  }
private:
  SpinRWLock lock_;
  mds::List<MdsTableBase> removed_mds_table_list_;// this list is for record those removed mds tables from t3m, but not destroed yet
};

class ObMdsTableMgr final : public checkpoint::ObCommonCheckpoint
{
  using MdsTableMap = common::ObLinearHashMap<common::ObTabletID, MdsTableBase*>;
  friend MdsTableFreezeGuard;
public:
  ObMdsTableMgr()
      : is_inited_(false),
        is_freezing_(false),
        freezing_scn_(share::SCN::min_scn()),
        ref_cnt_(0),
        ls_(nullptr),
        mds_table_map_(),
        removed_mds_table_recorder_() {}
  ~ObMdsTableMgr() { destroy(); }

  int init(ObLS *ls);
  int reset();
  void offline();
  void destroy();
  int register_to_mds_table_mgr(MdsTableBase *p_mds_table);// call when create new mds table
  int unregister_from_mds_table_mgr(MdsTableBase *p_mds_table);// call when remove tablet pointer from t3m map, record mds table in removed_mds_table_recorder
  void unregister_from_removed_mds_table_recorder(MdsTableBase *p_mds_table);// call when mds table released(tablet pointer released), del from removed_mds_table_recorder
  template <typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(MdsTableBase &))>// if op return FAIL, break for-each
  int for_each_removed_mds_table(OP &&op) {
    int ret = OB_SUCCESS;
    auto op_wrapper = [&op, &ret](const MdsTableBase &mds_table) -> bool {
      bool break_flag = false;// means keep iterating next mds_table
      if (OB_FAIL(op(const_cast<MdsTableBase &>(mds_table)))) {
        break_flag = true;
      }
      return break_flag;
    };
    removed_mds_table_recorder_.for_each(op_wrapper);
    return ret;
  }
  template <typename OP, ENABLE_IF_LIKE_FUNCTION(OP, int(MdsTableBase &))>// if op return FAIL, break for-each
  int for_each_in_t3m_mds_table(OP &&op) {
    auto op_wrapper = [&op](const common::ObTabletID &k, MdsTableBase* &v) -> bool {
      bool keep_iterating = true;// means keep iterating next mds_table
      int ret = OB_SUCCESS;
      if (OB_FAIL(op(*v))) {
        keep_iterating = false;
      }
      return keep_iterating;
    };
    return mds_table_map_.for_each(op_wrapper);
  }
  DECLARE_TO_STRING;

public: // derived from ObCommonCheckpoint
  share::SCN get_freezing_scn() const;
  virtual share::SCN get_rec_scn() override;
  virtual share::SCN get_rec_scn(ObTabletID &tablet_id) override;
  virtual int flush(share::SCN recycle_scn, bool need_freeze = true) override;
  virtual ObTabletID get_tablet_id() const override { return ObTabletID(0); }
  virtual bool is_flushing() const override { return false; }

public: // getter and setter
  void inc_ref() { ATOMIC_INC(&ref_cnt_); };
  void dec_ref() { ATOMIC_DEC(&ref_cnt_); };
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }

private:
  int first_scan_to_get_min_rec_scn_(share::SCN &min_rec_scn, ObIArray<ObTabletID> &min_rec_scn_ids);
  int second_scan_to_do_flush_(share::SCN min_rec_scn);

private:
  bool is_inited_;
  bool is_freezing_;
  share::SCN freezing_scn_;
  int64_t ref_cnt_;
  ObLS *ls_;
  MdsTableMap mds_table_map_;
  RemovedMdsTableRecorder removed_mds_table_recorder_;
};

class MdsTableFreezeGuard
{
public:
  MdsTableFreezeGuard() : can_freeze_(false), mds_mgr_(nullptr) {}
  ~MdsTableFreezeGuard() { reset(); }

  int init(ObMdsTableMgr *mds_mgr);
  void reset();
  bool can_freeze() { return can_freeze_; }

public:
  bool can_freeze_;
  ObMdsTableMgr *mds_mgr_;
};

class MdsTableMgrHandle
{
public:
  MdsTableMgrHandle();
  ~MdsTableMgrHandle();
  MdsTableMgrHandle(const MdsTableMgrHandle &other) = delete;
  MdsTableMgrHandle &operator= (const MdsTableMgrHandle &other) = delete;

  int reset();
  int set_mds_table_mgr(ObMdsTableMgr *mds_table_mgr);
  bool is_valid() const { return OB_NOT_NULL(mgr_); }

  OB_INLINE ObMdsTableMgr *get_mds_table_mgr() { return mgr_; }
  OB_INLINE const ObMdsTableMgr *get_mds_table_mgr() const { return mgr_; }

  TO_STRING_KV(KPC_(mgr));
private:
  ObMdsTableMgr *mgr_;
};


} // namespace mds
} // namespace storage
} // namespace oceanbase

#endif