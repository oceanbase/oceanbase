/**
 * Copyright (c) 2025 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "ob_update_tablet_pointer_param.h"

namespace oceanbase
{
namespace storage
{

int ObUpdateTabletPointerParam::refresh_tablet_cache()
{
  int ret = OB_SUCCESS;
  int64_t current_version = 0;
  MacroBlockId block_id;
  if (resident_info_.addr_.is_sslog()) {
    current_version = 0;
  } else if (OB_FAIL(resident_info_.addr_.get_macro_block_id(block_id))) {
    LOG_WARN("failed to get macro block id", K(ret), K(resident_info_));
  } else {
    current_version = static_cast<int64_t>(block_id.meta_version_id());
  }

  if (OB_SUCC(ret)) {
    resident_info_.attr_.refresh_cache(accelerate_info_.clog_checkpoint_scn_,
                                       accelerate_info_.ddl_checkpoint_scn_,
                                       accelerate_info_.mds_checkpoint_scn_,
                                       current_version);
  }
  return ret;
}

} //storage
} //oceanbase
