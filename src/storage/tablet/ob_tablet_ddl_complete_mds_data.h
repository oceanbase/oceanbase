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

#ifndef OCEANBASE_STORAGE_OB_TABLET_DDL_COMPLETE_MDS_DATA
#define OCEANBASE_STORAGE_OB_TABLET_DDL_COMPLETE_MDS_DATA

#include <stdint.h>
#include "lib/utility/ob_print_utils.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"
#include "common/ob_tablet_id.h"
#include "storage/tablet/ob_tablet_common.h"
#include "storage/ob_i_table.h"
#include "storage/ddl/ob_ddl_struct.h"

namespace oceanbase
{
namespace storage
{
class ObDDLTableMergeDagParam;
class ObTabletDDLCompleteArg;
class ObTabletDDLCompleteMdsUserData
{
  OB_UNIS_VERSION(1);
public:
  ObTabletDDLCompleteMdsUserData();
  void reset();
  bool is_valid() const ;
  int assign(const ObTabletDDLCompleteMdsUserData &other);
  TO_STRING_KV(K_(has_complete), K_(direct_load_type), K_(has_complete),
               K_(data_format_version), K_(snapshot_version),
               K_(table_key));
public:
  bool has_complete_;
  /* for merge param */
  ObDirectLoadType direct_load_type_;
  uint64_t data_format_version_;
  int64_t snapshot_version_;
  ObITable::TableKey table_key_;
};
} // namespace storage
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_OB_TABLET_CREATE_DELETE_MDS_USER_DATA
