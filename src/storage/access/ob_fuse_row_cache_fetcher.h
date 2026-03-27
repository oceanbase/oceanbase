/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OB_FUSE_ROW_CACHE_FETCHER_H_
#define OB_FUSE_ROW_CACHE_FETCHER_H_

#include "storage/blocksstable/ob_fuse_row_cache.h"
#include "ob_table_access_param.h"
#include "ob_table_access_context.h"

namespace oceanbase
{
namespace storage
{

class ObTableAccessContext;

class ObFuseRowCacheFetcher final
{
public:
  ObFuseRowCacheFetcher();
  ~ObFuseRowCacheFetcher() = default;
  int init(const StorageScanType type, const ObTabletID &tablet_id, const ObITableReadInfo *read_info,
           const int64_t read_start_version, const int64_t read_snapshot_version);
  int get_fuse_row_cache(const blocksstable::ObDatumRowkey &rowkey, blocksstable::ObFuseRowValueHandle &handle);
  int put_fuse_row_cache(const blocksstable::ObDatumRowkey &rowkey, blocksstable::ObDatumRow &row);
  TO_STRING_KV(K(is_inited_), K(type_), K(tablet_id_), KPC(read_info_),
               K(read_start_version_), K(read_snapshot_version_));
private:
  bool is_inited_;
  StorageScanType type_;
  ObTabletID tablet_id_;
  const ObITableReadInfo *read_info_;
  int64_t read_start_version_;
  int64_t read_snapshot_version_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OB_FUSE_ROW_CACHE_FETCHER_H_
