/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
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
    const ObTablet &tablet,
    int64_t &multi_version_start,
    UpdateUpperTransParam &upper_trans_param,
    bool &need_update);
};

} // namespace storage
} // namespace oceanbase

 #endif