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

#include "lib/lock/ob_tc_rwlock.h"
#include "meta_programming/ob_type_traits.h"
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
class ObMdsTableMgr final : public checkpoint::ObCommonCheckpoint
{
  using MdsTableMap = common::ObLinearHashMap<common::ObTabletID, List<MdsTableBase>>;
  friend MdsTableFreezeGuard;
public:
  ObMdsTableMgr()
      : is_inited_(false),
        is_freezing_(false),
        rec_log_ts_(0),
        ref_cnt_(0),
        mds_table_cnt_(0),
        ls_(nullptr),
        mds_table_map_() {}
  ~ObMdsTableMgr() { destroy(); }

  int init(ObLS *ls);
  int reset();
  void destroy();
  int register_to_mds_table_mgr(MdsTableBase *p_mds_table);
  int unregister_from_mds_table_mgr(MdsTableBase *p_mds_table);
  template <typename OP, ENABLE_IF_LIKE_FUNCTION(OP, bool(const ObTabletID &, List<MdsTableBase> &))>
  int for_each_mds_table_list(OP &&op) {
    return mds_table_map_.for_each(op);
  }

  DECLARE_TO_STRING;


public: // derived from ObCommonCheckpoint
  virtual share::SCN get_rec_scn() override;
  virtual int flush(share::SCN recycle_scn, bool need_freeze = true) override;
  virtual ObTabletID get_tablet_id() const override { return ObTabletID(0); }
  virtual bool is_flushing() const override { return false; }

public: // getter and setter
  void inc_ref() { ATOMIC_INC(&ref_cnt_); };
  void dec_ref() { ATOMIC_DEC(&ref_cnt_); };
  int64_t get_ref() { return ATOMIC_LOAD(&ref_cnt_); }
  int64_t get_mds_table_cnt() { return ATOMIC_LOAD(&mds_table_cnt_); }

private:
  bool has_flushing_mds_table_(share::ObLSID &ls_id, ObTabletID &tablet_id);

private:
  bool is_inited_;
  bool is_freezing_;
  int64_t rec_log_ts_;
  int64_t ref_cnt_;
  int64_t mds_table_cnt_;
  ObLS *ls_;
  MdsTableMap mds_table_map_;
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