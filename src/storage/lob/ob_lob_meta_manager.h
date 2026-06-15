/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_OB_LOB_META_MANAGER_
#define OCEABASE_STORAGE_OB_LOB_META_MANAGER_

#include <cstdint>
#include "lib/ob_errno.h"
#include "storage/lob/ob_lob_access_param.h"
#include "storage/blocksstable/ob_datum_row_iterator.h"
#include "storage/lob/ob_lob_persistent_adaptor.h"
#include "storage/compaction_ttl/ob_ttl_filter_info.h"

namespace oceanbase
{
namespace storage
{
class ObLobMetaSingleGetter;
class ObLobMetaInfo;
class ObLobMetaWriteIter;
class ObLobMetaScanIter;
class ObLobMetaManager {
public:
  explicit ObLobMetaManager(const uint64_t tenant_id) :
    persistent_lob_adapter_(tenant_id)
  {}
  ~ObLobMetaManager() {}
  // write one lob meta row
  int write(ObLobAccessParam& param, ObLobMetaInfo& in_row);
  int batch_insert(ObLobAccessParam& param, blocksstable::ObDatumRowIterator &iter);
  int batch_delete(ObLobAccessParam& param, blocksstable::ObDatumRowIterator &iter);
  // append
  int append(ObLobAccessParam& param, ObLobMetaWriteIter& iter);
  // return ObLobMetaWriteResult
  int insert(ObLobAccessParam& param, ObLobMetaWriteIter& iter);
  // specified range rebuild
  int rebuild(ObLobAccessParam& param);
  // specified range LobMeta scan
  int scan(ObLobAccessParam& param, ObLobMetaScanIter &iter);
  // scan raw lob meta
  int scan(ObLobAccessParam& param, ObLobMetaIterator *&iter);
  int revert_scan_iter(ObLobMetaIterator *iter);
  // specified range erase
  int erase(ObLobAccessParam& param, ObLobMetaInfo& in_row);
  // specified range update
  int update(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row);
  // fetch lob id
  int fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id);

  int open(ObLobAccessParam &param, ObLobMetaSingleGetter* getter);
  int getlength(ObLobAccessParam &param, uint64_t &char_len);
  int get_table_param(ObTTLFilterColType ttl_type, const ObTableParam *&table_param);
  int get_table_dml_param(ObTTLFilterColType ttl_type, const ObTableDMLParam *&table_dml_param);
  TO_STRING_KV("[LOB]", "meta mngr");

private:
  int local_scan(ObLobAccessParam& param, ObLobMetaScanIter &iter);
  int remote_scan(ObLobAccessParam& param, ObLobMetaScanIter &iter);

  int getlength_local(ObLobAccessParam &param, uint64_t &char_len);
  int getlength_remote(ObLobAccessParam &param, uint64_t &char_len);

private:
  // lob adaptor
  ObPersistentLobApator persistent_lob_adapter_;
};


} // storage
} // oceanbase

#endif