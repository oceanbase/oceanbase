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

#include "ob_remote_log_source_allocator.h"
#include "ob_remote_log_source.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace logservice
{
ObRemoteLogParent *ObResSrcAlloctor::alloc(const share::ObLogRestoreSourceType &type,
    const share::ObLSID &ls_id)
{
  ObRemoteLogParent *source = NULL;
  if (! is_valid_log_source_type(type)) {
    // just skip
  } else {
    switch (type) {
      case share::ObLogRestoreSourceType::SERVICE:
        source = OB_NEW(ObRemoteSerivceParent, "SerSource", ls_id);
        break;
      case share::ObLogRestoreSourceType::RAWPATH:
        source = OB_NEW(ObRemoteRawPathParent, "DestSource", ls_id);
        break;
      case share::ObLogRestoreSourceType::LOCATION:
        source = OB_NEW(ObRemoteLocationParent, "LocSource", ls_id);
        break;
      default:
        CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cannot allocate ObRemoteLogParent for invalid type", K(type), K(ls_id));
        break;
    }
  }
  return source;
}

void ObResSrcAlloctor::free(ObRemoteLogParent *source)
{
  if (NULL != source) {
    const share::ObLogRestoreSourceType type = source->get_source_type();
    switch (type) {
      case share::ObLogRestoreSourceType::SERVICE:
        MTL_DELETE(ObRemoteLogParent, "SerSource", source);
        break;
      case share::ObLogRestoreSourceType::RAWPATH:
        MTL_DELETE(ObRemoteLogParent, "DestSource", source);
        break;
      case share::ObLogRestoreSourceType::LOCATION:
        MTL_DELETE(ObRemoteLogParent, "LocSource", source);
        break;
      default:
        CLOG_LOG_RET(ERROR, OB_ERR_UNEXPECTED, "cannot free ObRemoteLogParent for a invalid type", K(type), KP(source));
        break;
    }
    source = NULL;
  }
}
} // namespace logservice
} // namespace oceanbase
