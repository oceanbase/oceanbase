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

typedef struct ObLobPieceInfo ObLobPieceInfo;

// TODO interface define
class ObILobApator {
public:
  virtual int write_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) = 0;
  virtual int update_lob_meta(ObLobAccessParam& param, ObLobMetaInfo& old_row, ObLobMetaInfo& new_row) = 0;
  virtual int erase_lob_meta(ObLobAccessParam &param, ObLobMetaInfo& row_info) = 0;
  virtual int scan_lob_meta(ObLobAccessParam &param, ObTableScanParam &scan_param, common::ObNewRowIterator *&meta_iter_) = 0;
  virtual int get_lob_data(ObLobAccessParam &param, uint64_t piece_id, ObLobPieceInfo& info) = 0;
  virtual int revert_scan_iter(common::ObNewRowIterator *iter) = 0;
  virtual int fetch_lob_id(ObLobAccessParam& param, uint64_t &lob_id) = 0;
  virtual int prepare_single_get(
      ObLobAccessParam &param,
      ObTableScanParam &scan_param,
      uint64_t &table_id) = 0;
};

} // storage
} // oceanbase

#endif


