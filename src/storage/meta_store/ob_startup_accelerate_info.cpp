/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "storage/meta_store/ob_startup_accelerate_info.h"

namespace oceanbase
{
namespace storage
{

OB_SERIALIZE_MEMBER(ObStartupTabletAccelerateInfo,
                    clog_checkpoint_scn_,
                    ddl_checkpoint_scn_,
                    mds_checkpoint_scn_,
                    transfer_info_,
                    compat_mode_);

} // end namespace storage
} // end namespace oceanbase
