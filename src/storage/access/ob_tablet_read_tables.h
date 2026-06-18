/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_STORAGE_TABLET_READ_TABLES_H
#define OB_STORAGE_TABLET_READ_TABLES_H

#include "lib/utility/ob_print_utils.h"
#include "share/ob_i_tablet_scan.h"
#include "storage/meta_mem/ob_tablet_handle.h"

namespace oceanbase
{
namespace storage
{
struct ObTableAccessParam;
struct ObTableAccessContext;

struct ObTabletReadTables final
{
public:
    ObTabletReadTables() : frozen_version_(-1), sample_info_(), tablet_iter_(), refreshed_merge_(nullptr), need_split_dst_table_(true) {}
    ~ObTabletReadTables() { reset(); }
    bool is_valid() const { return tablet_iter_.is_valid(); }
    void reset()
    {
    frozen_version_ = -1;
    sample_info_.reset();
    tablet_iter_.reset();
    refreshed_merge_ = nullptr;
    need_split_dst_table_ = true;
    }
    int prepare_candidate_read_tables(const ObTableAccessParam &access_param, const ObTableAccessContext &access_context);
    TO_STRING_KV(K_(frozen_version), K_(sample_info), K_(tablet_iter), K_(need_split_dst_table));
public:
    int64_t frozen_version_;
    common::SampleInfo sample_info_;
    ObTabletTableIterator tablet_iter_;

    // when tablet has been refreshed, to notify other ObMultipleMerge in ObTableScanIterator re-inited
    // before rescan.
    void *refreshed_merge_;

    // true means maybe need split dst table, which is always safe because get_read_tables will check by mds again;
    // false means no need split dst table, which is for optimization and UNSAFE
    bool need_split_dst_table_;
    DISALLOW_COPY_AND_ASSIGN(ObTabletReadTables);
};

} // namespace storage
} // namespace oceanbase

#endif