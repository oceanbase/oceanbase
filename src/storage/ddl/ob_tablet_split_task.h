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

#ifndef OCEANBASE_STORAGE_OB_TABLET_SPLIT_TASK_H
#define OCEANBASE_STORAGE_OB_TABLET_SPLIT_TASK_H

#include "share/ob_ddl_common.h"

namespace oceanbase
{
namespace storage
{
struct ObTabletSplitUtil final
{
public:
  static int get_participants(
      const share::ObSplitSSTableType &split_sstable_type,
      const ObTableStoreIterator &table_store_iterator,
      const bool is_table_restore,
      ObIArray<ObITable *> &participants);
  static int split_task_ranges(
      ObIAllocator &allocator,
      const share::ObDDLType ddl_type,
      const share::ObLSID &ls_id,
      const ObTabletID &tablet_id,
      const int64_t user_parallelism,
      const int64_t schema_tablet_size,
      ObIArray<blocksstable::ObDatumRowkey> &parallel_datum_rowkey_list);
  static int convert_datum_rowkey_to_range(
      ObIAllocator &allocator,
      const ObIArray<blocksstable::ObDatumRowkey> & datum_rowkey_list,
      ObIArray<blocksstable::ObDatumRange> &datum_ranges_array);
private:
  static const int64_t RECOVER_TABLE_PARALLEL_MIN_TASK_SIZE = 2 * 1024 * 1024L; /*2MB*/
};

}  // end namespace storage
}  // end namespace oceanbase
#endif  // OCEANBASE_STORAGE_OB_TABLET_SPLIT_TASK_H
