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
