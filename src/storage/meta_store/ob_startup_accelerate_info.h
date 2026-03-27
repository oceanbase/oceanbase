/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_STARTUP_ACCELERATE_INFO_H_
#define OCEANBASE_STORAGE_OB_STARTUP_ACCELERATE_INFO_H_

#include "storage/high_availability/ob_tablet_transfer_info.h"

namespace oceanbase
{
namespace storage
{
class ObStartupTabletAccelerateInfo final
{
public:
  ObStartupTabletAccelerateInfo() = default;
  ~ObStartupTabletAccelerateInfo() = default;
  TO_STRING_KV(K_(clog_checkpoint_scn),
               K_(ddl_checkpoint_scn),
               K_(mds_checkpoint_scn),
               K_(transfer_info),
               K_(compat_mode));
  OB_UNIS_VERSION(1);
public:
  share::SCN clog_checkpoint_scn_;
  share::SCN ddl_checkpoint_scn_;
  share::SCN mds_checkpoint_scn_;
  ObTabletTransferInfo transfer_info_;
  lib::Worker::CompatMode compat_mode_;
};
} // end namespace storage
} // end namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_STARTUP_ACCELERATE_INFO_H_
