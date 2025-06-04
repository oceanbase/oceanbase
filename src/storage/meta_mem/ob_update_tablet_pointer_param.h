/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef OCEANBASE_STORAGE_OB_UPDATE_TABLET_POINTER_PARAM_H_
#define OCEANBASE_STORAGE_OB_UPDATE_TABLET_POINTER_PARAM_H_

#include "storage/meta_mem/ob_tablet_attr.h"
#include "storage/meta_store/ob_startup_accelerate_info.h"
#include "storage/slog_ckpt/ob_tablet_replay_create_handler.h"

namespace oceanbase
{
namespace storage
{

struct ObUpdateTabletPointerParam final
{
public:
  ObUpdateTabletPointerParam() = default;
  ObUpdateTabletPointerParam(const ObTabletReplayItem &item)
    : resident_info_(item.info_),
      accelerate_info_(item.accelerate_info_)
  {}
  ~ObUpdateTabletPointerParam() = default;
  bool is_valid() const { return resident_info_.addr_.is_valid(); }
  bool is_empty_shell() const { return resident_info_.attr_.is_empty_shell(); }
  const lib::Worker::CompatMode &compat_mode() const { return accelerate_info_.compat_mode_; }
  const ObTabletTransferInfo &transfer_info() const { return accelerate_info_.transfer_info_; }
  const ObMetaDiskAddr &tablet_addr() const { return resident_info_.addr_; }
  void refresh_tablet_cache()
  {
    resident_info_.attr_.refresh_cache(accelerate_info_.clog_checkpoint_scn_,
                                       accelerate_info_.ddl_checkpoint_scn_,
                                       accelerate_info_.mds_checkpoint_scn_);
  }
  TO_STRING_KV(K_(resident_info), K_(accelerate_info));
public:
  ObTabletResidentInfo resident_info_;
  ObStartupTabletAccelerateInfo accelerate_info_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_UPDATE_TABLET_POINTER_PARAM_H_
