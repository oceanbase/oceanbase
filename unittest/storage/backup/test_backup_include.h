/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE
#include "gtest/gtest.h"
#include "storage/backup/ob_backup_index_merger.h"
#include "storage/blocksstable/ob_logic_macro_id.h"
#define private public
#define protected public

namespace oceanbase
{
namespace backup
{

int64_t max_tablet_id = 0;

}
}
