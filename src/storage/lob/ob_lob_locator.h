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

#ifndef OB_STORAGE_LOB_LOCATOR_H
#define OB_STORAGE_LOB_LOCATOR_H

#include "storage/access/ob_table_access_param.h"

namespace oceanbase
{
namespace storage
{

class ObLobLocatorHelper
{
public:
  ObLobLocatorHelper();
  virtual ~ObLobLocatorHelper();
  void reset();
  void reuse();
  int init(const share::schema::ObTableParam &table_param, const int64_t snapshot_version);
  int fill_lob_locator(blocksstable::ObDatumRow &row, bool is_projected_row,
                        const ObTableAccessParam &access_param);
  OB_INLINE bool is_valid() const { return is_inited_; }
  TO_STRING_KV(K_(table_id), K_(snapshot_version), K_(rowid_version), KPC(rowid_project_), K_(rowid_objs), K_(is_inited));
private:
  static const int64_t DEFAULT_LOCATOR_OBJ_ARRAY_SIZE = 8;
  int init_rowid_version(const share::schema::ObTableSchema &table_schema);
  int build_rowid_obj(blocksstable::ObDatumRow &row,
                      common::ObString &rowid_str,
                      bool is_projected_row,
                      const ObColDescIArray &col_descs,
                      const common::ObIArray<int32_t> &out_project,
                      const common::ObTabletID &tablet_id);
  int build_lob_locator(common::ObString payload, const uint64_t column_id,
                        const common::ObString &rowid_str, ObLobLocator *&locator);
private:
  uint64_t table_id_;
  int64_t snapshot_version_;
  int64_t rowid_version_;
  const common::ObIArray<int32_t> *rowid_project_; //map to projected row
  common::ObSEArray<common::ObObj, DEFAULT_LOCATOR_OBJ_ARRAY_SIZE> rowid_objs_;
  common::ObArenaAllocator locator_allocator_;
  bool is_inited_;
};

} // namespace storage
} // namespace oceanbase
#endif
