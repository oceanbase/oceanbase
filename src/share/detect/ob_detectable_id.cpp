/**
 * Copyright (c) 2023 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */
#define USING_LOG_PREFIX SHARE

#include "share/detect/ob_detectable_id.h"

namespace oceanbase {
namespace common {

OB_SERIALIZE_MEMBER(ObDetectableId, first_, second_, tenant_id_);
OB_SERIALIZE_MEMBER(ObRegisterDmInfo, detectable_id_, addr_);

} // end namespace common
} // end namespace oceanbase
