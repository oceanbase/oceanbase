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

#ifndef OCEABASE_STORAGE_OB_LOB_PIECE_
#define OCEABASE_STORAGE_OB_LOB_PIECE_
#include "lib/lock/ob_spin_lock.h"
#include "lib/task/ob_timer.h"
#include "storage/blocksstable/ob_macro_block_id.h"
#include "ob_lob_util.h"
#include "ob_lob_persistent_adaptor.h"

namespace oceanbase
{
namespace storage
{

class ObLobPieceUtil {
public:
  static const uint64_t LOB_PIECE_COLUMN_CNT = 3;
  static const uint64_t PIECE_ID_COL_ID = 0;
  static const uint64_t LEN_COL_ID = 1;
  static const uint64_t MACRO_ID_COL_ID = 2;
public:
  static int transform(common::ObNewRow* row, ObLobPieceInfo &info);
private:
  static int transform_piece_id(common::ObNewRow* row, ObLobPieceInfo &info);
  static int transform_len(common::ObNewRow* row, ObLobPieceInfo &info);
  static int transform_macro_id(common::ObNewRow* row, ObLobPieceInfo &info);
};

class ObLobPieceManager
{
public:
  ObLobPieceManager() {}
  ~ObLobPieceManager() {}
  int get(ObLobAccessParam& param, uint64_t piece_id, ObLobPieceInfo& info);
  int write(ObLobAccessParam& param, ObLobPieceInfo& in_row);
  int erase(ObLobAccessParam& param, ObLobPieceInfo& in_row);
  int update(ObLobAccessParam& param, ObLobPieceInfo& in_row);
  TO_STRING_KV("[LOB]", "piece mngr");
private:
  ObPersistentLobApator persistent_lob_adapter_;
};


} // storage
} // oceanbase

#endif


