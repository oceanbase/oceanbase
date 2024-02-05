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

#ifndef OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_MGR_
#define OCEANBASE_STORAGE_OB_TENANT_SNAPSHOT_MGR_

#include "share/tenant_snapshot/ob_tenant_snapshot_id.h"
#include "storage/tenant_snapshot/ob_tenant_snapshot_defs.h"

namespace oceanbase
{
namespace storage
{

class ObLSSnapshotMgr;

typedef common::LinkHashNode<share::ObTenantSnapshotID> ObTenantSnapshotNode;
class ObTenantSnapshotHashAlloc
{
public:
  ObTenantSnapshotHashAlloc() : value_cnt_(0) {}
  ~ObTenantSnapshotHashAlloc() { value_cnt_ = 0; }

  ObTenantSnapshotHashAlloc(const ObTenantSnapshotHashAlloc* other) :
    value_cnt_(other->value_cnt_) { }

  ObTenantSnapshot* alloc_value()
  {
    ObMemAttr memattr(MTL_ID(), "TenantSnapshot");
    ObTenantSnapshot* tenant_snapshot = OB_NEW(ObTenantSnapshot, memattr);
    if (OB_NOT_NULL(tenant_snapshot)) {
      ATOMIC_INC(&value_cnt_);
    }
    return tenant_snapshot;
  }
  void free_value(ObTenantSnapshot* snapshot)
  {
    if (NULL != snapshot) {
      ObMemAttr memattr(MTL_ID(), "TenantSnapshot");
      OB_DELETE(ObTenantSnapshot, memattr, snapshot);
      ATOMIC_DEC(&value_cnt_);
    }
  }
  ObTenantSnapshotNode* alloc_node(ObTenantSnapshot* snapshot)
  {
    UNUSED(snapshot);
    ObMemAttr memattr(MTL_ID(), "TSNode");
    return OB_NEW(ObTenantSnapshotNode, memattr);
  }
  void free_node(ObTenantSnapshotNode* node)
  {
    if (NULL != node) {
      ObMemAttr memattr(MTL_ID(), "TSNode");
      OB_DELETE(ObTenantSnapshotNode, memattr, node);
      node = NULL;
    }
  }

  int64_t get_value_cnt() const { return ATOMIC_LOAD(&value_cnt_); }

private:
  int64_t value_cnt_;
};

typedef common::ObLinkHashMap<share::ObTenantSnapshotID, ObTenantSnapshot,
      ObTenantSnapshotHashAlloc, common::RefHandle> ObTenantSnapshotMap;

class ObTenantSnapshotMgr
{
public:
  ObTenantSnapshotMgr()
    : is_inited_(false),
      ls_snapshot_mgr_(nullptr),
      meta_handler_(nullptr),
      tenant_snapshot_map_() {}

  virtual ~ObTenantSnapshotMgr() {}

  int init(ObLSSnapshotMgr* ls_snapshot_mgr, ObTenantMetaSnapshotHandler* meta_handler);
  void stop();
  void destroy();

public:
  int get_tenant_snapshot(const share::ObTenantSnapshotID &tenant_snapshot_id,
                          ObTenantSnapshot *&tenant_snapshot);

  int acquire_tenant_snapshot(const share::ObTenantSnapshotID &tenant_snapshot_id,
                              ObTenantSnapshot *&tenant_snapshot);

  int has_tenant_snapshot_stopped(bool& has_tenant_snapshot_stopped);

  int revert_tenant_snapshot(ObTenantSnapshot *tenant_snapshot);

  int del_tenant_snapshot(const share::ObTenantSnapshotID& tenant_snapshot_id);

  template <typename Fn> int for_each(Fn &fn) { return tenant_snapshot_map_.for_each(fn); }
  template <typename Fn> int remove_if(Fn &fn) { return tenant_snapshot_map_.remove_if(fn); }

  int get_tenant_snapshot_cnt(int64_t& cnt);
private:
  int create_tenant_snapshot_(const share::ObTenantSnapshotID &tenant_snapshot_id,
                              ObTenantSnapshot *&tenant_snapshot);

private:
  bool is_inited_;
  ObLSSnapshotMgr *ls_snapshot_mgr_;
  ObTenantMetaSnapshotHandler* meta_handler_;
  ObTenantSnapshotMap tenant_snapshot_map_;
public:
  TO_STRING_KV(K(is_inited_));
};

}
}

#endif
