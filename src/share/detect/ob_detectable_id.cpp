/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#include "share/detect/ob_detectable_id.h"

namespace oceanbase {
namespace common {

OB_SERIALIZE_MEMBER(ObDetectableId, first_, second_, tenant_id_);
OB_SERIALIZE_MEMBER(ObRegisterDmInfo, detectable_id_, addr_);

} // end namespace common
} // end namespace oceanbase
