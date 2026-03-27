/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_SERVICE_OB_REMOTE_LOG_SOURCE_ALLOCATOR_H_
#define OCEANBASE_LOG_SERVICE_OB_REMOTE_LOG_SOURCE_ALLOCATOR_H_

#include "share/restore/ob_log_restore_source.h"
#include "ob_remote_log_source.h"
namespace oceanbase
{
namespace logservice
{
class ObResSrcAlloctor
{
public:
    static ObRemoteLogParent *alloc(const share::ObLogRestoreSourceType &type, const share::ObLSID &ls_id);
    static void free(ObRemoteLogParent *source);
};
} // namespace logservice
} // namespace oceanbase


#endif /* OCEANBASE_LOG_SERVICE_OB_REMOTE_LOG_SOURCE_ALLOCATOR_H_ */
