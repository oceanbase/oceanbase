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
