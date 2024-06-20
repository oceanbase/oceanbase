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

#ifndef OCEANBASE_STORAGE_OB_LS_SNAPSHOT_MGR_
#define OCEANBASE_STORAGE_OB_LS_SNAPSHOT_MGR_

#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"
#include "storage/tenant_snapshot/ob_ls_snapshot_defs.h"

namespace oceanbase
{
namespace storage
{

typedef common::LinkHashNode<ObLSSnapshotMapKey> ObLSSnapshotNode;
class ObLSSnapshotHashAlloc
{
public:
  ObLSSnapshotHashAlloc() : value_cnt_(0) {}
  ~ObLSSnapshotHashAlloc() { value_cnt_ = 0; }

  ObLSSnapshotHashAlloc(const ObLSSnapshotHashAlloc* other) :
    value_cnt_(other->value_cnt_) { }

  ObLSSnapshot* alloc_value()
  {
    ObMemAttr memattr(MTL_ID(), "LSSnapshot");
    ObLSSnapshot* ls_snapshot = OB_NEW(ObLSSnapshot, memattr);
    if (OB_NOT_NULL(ls_snapshot)) {
      ATOMIC_INC(&value_cnt_);
    }
    return ls_snapshot;
  }
  void free_value(ObLSSnapshot* snapshot)
  {
    if (NULL != snapshot) {
      ObMemAttr memattr(MTL_ID(), "LSSnapshot");
      OB_DELETE(ObLSSnapshot, memattr, snapshot);
      ATOMIC_DEC(&value_cnt_);
    }
  }
  ObLSSnapshotNode* alloc_node(ObLSSnapshot* snapshot)
  {
    UNUSED(snapshot);
    ObMemAttr memattr(MTL_ID(), "LSSnapshotNode");
    return OB_NEW(ObLSSnapshotNode, memattr);
  }
  void free_node(ObLSSnapshotNode* node)
  {
    if (NULL != node) {
      ObMemAttr memattr(MTL_ID(), "LSSnapshotNode");
      OB_DELETE(ObLSSnapshotNode, memattr, node);
    }
  }

  int64_t get_value_cnt() const { return ATOMIC_LOAD(&value_cnt_); }
private:
  int64_t value_cnt_;
};

typedef common::ObLinkHashMap<ObLSSnapshotMapKey, ObLSSnapshot,
      ObLSSnapshotHashAlloc, common::RefHandle> ObLSSnapshotMap;

class ObLSSnapshotMgr
{
public:
  ObLSSnapshotMgr()
    : is_inited_(false),
      ls_snapshot_map_(),
      build_ctx_allocator_(),
      meta_handler_(nullptr) {}

  virtual ~ObLSSnapshotMgr() {}
  int init(ObTenantMetaSnapshotHandler* meta_handler);
  void destroy();

public:
  int get_ls_snapshot(const share::ObTenantSnapshotID &tenant_snapshot_id,
                      const share::ObLSID &ls_id,
                      ObLSSnapshot *&ls_snapshot);

  int acquire_ls_snapshot(const share::ObTenantSnapshotID &tenant_snapshot_id,
                          const share::ObLSID &ls_id,
                          ObLSSnapshot *&ls_snapshot);

  int revert_ls_snapshot(ObLSSnapshot *ls_snapshot);
  int del_ls_snapshot(const share::ObTenantSnapshotID& tenant_snapshot_id,
                      const share::ObLSID& ls_id);

  template <typename Fn> int for_each(Fn &fn) { return ls_snapshot_map_.for_each(fn); }
  template <typename Fn> int remove_if(Fn &fn) { return ls_snapshot_map_.remove_if(fn); }
private:
  int create_ls_snapshot_(const share::ObTenantSnapshotID &tenant_snapshot_id,
                          const share::ObLSID &ls_id,
                          ObLSSnapshot *&ls_snapshot);

private:
  bool is_inited_;
  ObLSSnapshotMap ls_snapshot_map_;
  common::ObConcurrentFIFOAllocator build_ctx_allocator_;
  ObTenantMetaSnapshotHandler* meta_handler_;

public:
  TO_STRING_KV(K(is_inited_));
};

}
}

#endif
