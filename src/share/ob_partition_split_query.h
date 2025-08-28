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

#ifndef OCEANBASE_SHARE_OB_PARTITION_SPLIT_QUERY_H
#define OCEANBASE_SHARE_OB_PARTITION_SPLIT_QUERY_H

#include "lib/allocator/page_arena.h"
#include "share/schema/ob_table_schema.h"
#include "share/schema/ob_schema_service.h"
#include "share/location_cache/ob_location_struct.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/ob_storage_struct.h"
#include "storage/meta_mem/ob_tablet_handle.h"
#include "sql/engine/expr/ob_expr.h"

namespace oceanbase
{
namespace sql
{
class ObPushdownOperator;
}
namespace storage
{
class ObTabletHandle;
class ObLSHandle;
struct ObTabletSplitTscInfo;
}
namespace share
{
class ObPartitionSplitQuery final
{
public:
  ObPartitionSplitQuery() : 
    split_info_() 
  {}
  ~ObPartitionSplitQuery() {
    split_info_.reset();
  }
public:
  // FIXME:remove it
  int get_tablet_handle(
      const ObTabletID &tablet_id,
      const ObLSID &ls_id,
      storage::ObTabletHandle &tablet_handle);

  int get_tablet_split_range(
      const ObTablet &tablet,
      const blocksstable::ObStorageDatumUtils &datum_utils,
      const storage::ObTabletSplitTscInfo &split_info,
      ObIAllocator &allocator,
      blocksstable::ObDatumRange &src_range,
      bool &is_empty_range);

  int get_tablet_split_ranges(
      const ObTablet &tablet,
      const common::ObIArray<common::ObStoreRange> &ori_ranges,
      common::ObIArray<common::ObStoreRange> &new_ranges,
      ObIAllocator &allocator);

  int get_split_datum_range(
      const ObTablet &tablet,
      const blocksstable::ObStorageDatumUtils *datum_utils,
      ObIAllocator &allocator,
      blocksstable::ObDatumRange &datum_range,
      bool &is_empty_range);
      
  int get_tablet_split_info(
      const ObTablet &tablet,
      ObIAllocator &allocator);

  int split_multi_ranges_if_need(
      const ObIArray<ObStoreRange> &src_ranges,
      ObIArray<ObStoreRange> &new_ranges,
      ObIAllocator &allocator,
      const storage::ObTabletHandle &tablet_handle,
      bool &is_splited_range);

  int fill_auto_split_params(
      const ObTablet &tablet,
      const bool is_split_dst,
      sql::ObPushdownOperator *op,
      const uint64_t filter_type,
      sql::ExprFixedArray *filter_params,
      ObIAllocator &allocator);

  int check_rowkey_is_included(
      const ObTablet &tablet,
      const blocksstable::ObDatumRowkey &target_rowkey, 
      const blocksstable::ObStorageDatumUtils *datum_utils,
      bool &is_included);

private:
  int fill_range_filter_param(
      const storage::ObTabletSplitTscInfo &split_info,
      sql::ObEvalCtx &eval_ctx,
      sql::ExprFixedArray *filter_params);

  int set_split_info(const storage::ObTabletSplitTscInfo &split_info);
  
  int copy_split_key(
      const blocksstable::ObDatumRowkey &split_key,
      const int64_t src_datum_cnt,
      blocksstable::ObDatumRowkey &new_key,
      ObIAllocator &allocator);

private:
  ObTabletHandle tablet_handle_;
  ObTabletSplitTscInfo split_info_;
};

}  // end namespace share
}  // end namespace oceanbase

#endif // end OCEANBASE_SHARE_OB_PARTITION_SPLIT_QUERY_H
