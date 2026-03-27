/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
  ObUpdateTabletPointerParam()
    : resident_info_(),
      accelerate_info_(),
      update_last_match_meta_version_(false)
  {}
  ObUpdateTabletPointerParam(const ObTabletReplayItem &item)
    : resident_info_(item.info_),
      accelerate_info_(item.accelerate_info_),
      update_last_match_meta_version_(false)
  {}
  ~ObUpdateTabletPointerParam() = default;
  bool is_valid() const { return resident_info_.addr_.is_valid(); }
  bool is_empty_shell() const { return resident_info_.attr_.is_empty_shell(); }
  const lib::Worker::CompatMode &compat_mode() const { return accelerate_info_.compat_mode_; }
  const ObTabletTransferInfo &transfer_info() const { return accelerate_info_.transfer_info_; }
  const ObMetaDiskAddr &tablet_addr() const { return resident_info_.addr_; }
  int refresh_tablet_cache();
  void set_update_last_match_meta_version()
  {
    update_last_match_meta_version_ = true;
  }
  bool update_last_match_meta_version() const
  {
    return update_last_match_meta_version_;
  }

  TO_STRING_KV(K_(resident_info), K_(accelerate_info), K_(update_last_match_meta_version));
public:
  ObTabletResidentInfo resident_info_;
  ObStartupTabletAccelerateInfo accelerate_info_;
  bool update_last_match_meta_version_;
};

} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_UPDATE_TABLET_POINTER_PARAM_H_
