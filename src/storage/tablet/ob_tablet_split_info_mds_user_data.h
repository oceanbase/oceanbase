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

#ifndef OCEANBASE_STORAGE_OB_TABLET_SPLIT_INFO_MDS_DATA
#define OCEANBASE_STORAGE_OB_TABLET_SPLIT_INFO_MDS_DATA

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/ob_i_table.h"
#include "storage/ddl/ob_ddl_struct.h"
#include "share/ob_ddl_common.h"

namespace oceanbase
{
namespace storage
{

class ObTabletSplitInfoMdsUserData
{
  OB_UNIS_VERSION(1);
public:
  ObTabletSplitInfoMdsUserData();
  void reset();
  bool is_valid() const;
  int assign(const ObTabletSplitInfoMdsUserData &other);
  int assign_datum_rowkey_list(const common::ObSArray<blocksstable::ObDatumRowkey> &other_datum_rowkey_list); //deep copy
  int get_storage_schema(const ObStorageSchema *&storage_schema) const
  {
    storage_schema = &storage_schema_;
    return OB_SUCCESS;
  }
  TO_STRING_KV(K_(table_id), K_(lob_table_id), K_(schema_version), K(dest_schema_version_), K(tablet_task_id_),
      K_(task_id), K_(task_type), K_(parallelism), K_(source_tablet_id), K_(dest_tablets_id),
      K_(data_format_version), K_(consumer_group_id), K_(can_reuse_macro_block), K(is_no_logging_),
      K_(split_sstable_type), K_(lob_col_idxs), K_(parallel_datum_rowkey_list), K_(storage_schema));
public:
  ObArenaAllocator allocator_;
  uint64_t table_id_; // scan rows needed, index table id or main table id.
  uint64_t lob_table_id_; // scan rows needed, valid when split lob tablet.
  int64_t schema_version_; // report replica build status needed.
  int64_t dest_schema_version_;// report replica build status needed.
  int64_t tablet_task_id_;
  int64_t task_id_; // report replica build status needed.
  share::ObDDLType task_type_;
  int64_t parallelism_;
  common::ObTabletID source_tablet_id_;
  common::ObSArray<common::ObTabletID> dest_tablets_id_;
  int64_t data_format_version_;
  uint64_t consumer_group_id_;
  bool can_reuse_macro_block_;
  bool is_no_logging_;
  share::ObSplitSSTableType split_sstable_type_;
  common::ObSEArray<uint64_t, 16> lob_col_idxs_;
  common::ObSArray<blocksstable::ObDatumRowkey> parallel_datum_rowkey_list_;
  ObStorageSchema storage_schema_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_SPLIT_INFO_MDS_DATA