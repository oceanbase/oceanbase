/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX STORAGE

#include "ob_tablet_read_tables.h"
#include "ob_table_access_context.h"
#include "ob_table_access_param.h"

namespace oceanbase
{
namespace storage
{

int ObTabletReadTables::prepare_candidate_read_tables(const ObTableAccessParam &access_param, const ObTableAccessContext &access_context)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!tablet_iter_.is_valid() || nullptr == tablet_iter_.table_iter())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected tablet iter", K(ret), K(tablet_iter_));
  } else if (FALSE_IT(tablet_iter_.table_iter()->reset())) {
  } else if (access_param.iter_param_.is_mds_query_) {
    int64_t snapshot_version = -1 == frozen_version_ ?
      access_context.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx() : frozen_version_;
    if (OB_FAIL(tablet_iter_.get_mds_sstables_from_tablet(snapshot_version))) {
      LOG_WARN("fail to get mds sstables", K(ret), K(snapshot_version), KPC(this));
    } else {
      LOG_DEBUG("succeed to get mds sstables from tablet", K(ret), K_(tablet_iter));
    }
  } else {
    const bool need_split_src_table = access_param.iter_param_.is_tablet_spliting();
    int64_t read_tables_version = 0;
    if (access_context.is_mview_query()) {
      read_tables_version = access_context.trans_version_range_.base_version_ > 0 ?
        access_context.trans_version_range_.base_version_ :
        access_context.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx();
    } else {
      read_tables_version = sample_info_.is_no_sample() ?
        access_context.store_ctx_->mvcc_acc_ctx_.get_snapshot_version().get_val_for_tx() : INT64_MAX;
    }
    if (OB_UNLIKELY(frozen_version_ != -1)) {
      if (!sample_info_.is_no_sample()) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("sample query does not support frozen_version", K(ret), KPC(this), K(access_param));
      } else if (OB_FAIL(tablet_iter_.refresh_read_tables_from_tablet(
          frozen_version_,
          false/*allow_not_ready*/,
          true/*major_sstable_only*/,
          need_split_src_table,
          need_split_dst_table_))) {
        LOG_WARN("get table iterator fail", K(ret), KPC(this), K(access_param));
      }
    } else if (OB_FAIL(tablet_iter_.refresh_read_tables_from_tablet(
        read_tables_version,
        false/*allow_not_ready*/,
        false/*major_sstable_only*/,
        need_split_src_table,
        need_split_dst_table_))) {
      LOG_WARN("get table iterator fail", K(ret), KPC(this), K(access_param));
    }
  }
  return ret;
}

} // namespace storage
} // namespace oceanbase