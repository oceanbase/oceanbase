/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEABASE_STORAGE_OB_LOB_ADAPTOR_
#define OCEABASE_STORAGE_OB_LOB_ADAPTOR_
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "storage/access/ob_dml_param.h"
#include "ob_lob_util.h"

namespace oceanbase
{
namespace storage
{

class ObLobMetaIterator;
// TODO interface define
class ObILobApator {
public:
  virtual int write_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) = 0;
  virtual int update_lob_meta(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row) = 0;
  virtual int erase_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) = 0;
  virtual int scan_lob_meta(ObLobAccessParam &param, ObLobMetaIterator *&meta_iter) = 0;
  virtual int get_lob_data(ObLobAccessParam &param, uint64_t piece_id, ObLobPieceInfo& info) = 0;
  virtual int revert_scan_iter(ObLobMetaIterator *iter) = 0;
  virtual int fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id) = 0;
};

} // storage
} // oceanbase

#endif


