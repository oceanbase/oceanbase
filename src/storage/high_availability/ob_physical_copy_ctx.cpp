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

#define USING_LOG_PREFIX STORAGE
#include "storage/high_availability/ob_physical_copy_ctx.h"

namespace oceanbase
{
namespace storage
{

/******************ObPhysicalCopyCtx*********************/
ObPhysicalCopyCtx::ObPhysicalCopyCtx()
  : lock_(),
    tenant_id_(OB_INVALID_ID),
    ls_id_(),
    tablet_id_(),
    src_info_(),
    bandwidth_throttle_(nullptr),
    svr_rpc_proxy_(nullptr),
    is_leader_restore_(false),
    restore_action_(ObTabletRestoreAction::RESTORE_NONE),
    restore_base_info_(nullptr),
    meta_index_store_(nullptr),
    second_meta_index_store_(nullptr),
    ha_dag_(nullptr),
    sstable_index_builder_(nullptr),
    restore_macro_block_id_mgr_(nullptr),
    need_sort_macro_meta_(true),
    need_check_seq_(false),
    ls_rebuild_seq_(-1),
    table_key_()
{
}

ObPhysicalCopyCtx::~ObPhysicalCopyCtx()
{
}

bool ObPhysicalCopyCtx::is_valid() const
{
  bool bool_ret = false;
  bool_ret = tenant_id_ != OB_INVALID_ID
             && ls_id_.is_valid()
             && tablet_id_.is_valid()
             && OB_NOT_NULL(bandwidth_throttle_)
             && OB_NOT_NULL(svr_rpc_proxy_)
             && OB_NOT_NULL(ha_dag_)
             && OB_NOT_NULL(sstable_index_builder_)
             && ((need_check_seq_ && ls_rebuild_seq_ >= 0) || !need_check_seq_)
             && table_key_.is_valid();

  if (bool_ret) {
    if (!is_leader_restore_) {
      bool_ret = src_info_.is_valid();
    } else if (OB_ISNULL(restore_base_info_)
               || OB_ISNULL(second_meta_index_store_)) {
      bool_ret = false;
    } else if (!ObTabletRestoreAction::is_restore_replace_remote_sstable(restore_action_) && OB_ISNULL(restore_macro_block_id_mgr_)) {
      bool_ret = false;
      LOG_WARN_RET(OB_INVALID_ARGUMENT, "restore_macro_block_id_mgr_ is null", K_(restore_action), KP_(restore_macro_block_id_mgr));
    }
  }

  return bool_ret;
}

void ObPhysicalCopyCtx::reset()
{
  tenant_id_ = OB_INVALID_ID;
  ls_id_.reset();
  tablet_id_.reset();
  src_info_.reset();
  bandwidth_throttle_ = nullptr;
  svr_rpc_proxy_ = nullptr;
  is_leader_restore_ = false;
  restore_action_= ObTabletRestoreAction::RESTORE_NONE;
  restore_base_info_ = nullptr;
  meta_index_store_ = nullptr;
  second_meta_index_store_ = nullptr;
  ha_dag_ = nullptr;
  sstable_index_builder_ = nullptr;
  restore_macro_block_id_mgr_ = nullptr;
  need_sort_macro_meta_ = true;
  need_check_seq_ = false;
  ls_rebuild_seq_ = -1;
  table_key_.reset();
}

}
}