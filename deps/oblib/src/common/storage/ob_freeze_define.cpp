/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#include "common/storage/ob_freeze_define.h"

namespace oceanbase
{
namespace storage
{
OB_SERIALIZE_MEMBER(ObFrozenStatus, frozen_version_,
                    frozen_timestamp_, status_, schema_version_, cluster_version_);
} // end namespace storage
} // end namespace oceanbase
