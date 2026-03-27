/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_STORAGE_OB_LOB_DATA_READER_H_
#define OCEANBASE_STORAGE_OB_LOB_DATA_READER_H_

#include "common/ob_tablet_id.h"
#include "storage/access/ob_table_access_context.h"
#include "lib/allocator/page_arena.h"
#include "storage/access/ob_table_access_param.h"
#include "storage/blocksstable/ob_datum_row.h"

namespace oceanbase
{
namespace storage
{

class ObLobDataReader
{
public:
  ObLobDataReader();
  virtual ~ObLobDataReader();
  int init(const ObTableIterParam &iter_param, storage::ObTableAccessContext &context);
  // for temporary lob
  int read_lob_data(blocksstable::ObStorageDatum &datum, ObCollationType coll_type);
  void reuse();
  void reset();
  bool is_init() const { return is_inited_; }
  int fuse_disk_lob_header(common::ObObj &obj);
  ObIAllocator &get_allocator() { return allocator_; }
private:
  int read_lob_data_impl(blocksstable::ObStorageDatum &datum, ObCollationType coll_type);
private:
  bool is_inited_;
  common::ObTabletID tablet_id_;
  storage::ObTableAccessContext* access_ctx_;
  common::ObArenaAllocator allocator_;
};

}  // end namespace storage
}  // end namespace oceanbase

#endif  // OCEANBASE_STORAGE_OB_LOB_DATA_READER_H_
