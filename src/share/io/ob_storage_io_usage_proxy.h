/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LIB_OB_STORAGE_IO_USAGE_PROXY_H
#define OCEANBASE_LIB_OB_STORAGE_IO_USAGE_PROXY_H

#include "lib/ob_define.h"
namespace oceanbase
{
namespace common
{
class ObMySQLTransaction;
class ObString;
class ObMySQLProxy;
}
namespace share
{
class ObStorageIOUsageProxy
{
public:
    ObStorageIOUsageProxy() {}
    ~ObStorageIOUsageProxy() {}
    int update_storage_io_usage(common::ObMySQLTransaction &trans,
                                const uint64_t tenant_id,
                                const int64_t storage_id,
                                const int64_t dest_id,
                                const ObString &storage_mod,
                                const ObString &type,
                                const int64_t total);
private:
    DISALLOW_COPY_AND_ASSIGN(ObStorageIOUsageProxy);
};

}
}
#endif