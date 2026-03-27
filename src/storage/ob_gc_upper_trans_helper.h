/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

 #ifndef OCEANBASE_STORAGE_OB_GC_UPPER_TRANS_HELPER
 #define OCEANBASE_STORAGE_OB_GC_UPPER_TRANS_HELPER

#include "storage/ls/ob_ls.h"
#include "storage/tablet/ob_tablet.h"

namespace oceanbase
{
namespace storage
{

class ObGCUpperTransHelper
{
public:
static int try_get_sstable_upper_trans_version(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    int64_t &new_upper_trans_version);

static int check_need_gc_or_update_upper_trans_version(
    ObLS &ls,
    ObTablet &tablet,
    int64_t &multi_version_start,
    UpdateUpperTransParam &upper_trans_param,
    bool &need_update);

static int check_uncommit_tx_info_state(
    ObLS &ls,
    const blocksstable::ObSSTable &sstable,
    int64_t &new_upper_trans_version);
};

} // namespace storage
} // namespace oceanbase

#endif