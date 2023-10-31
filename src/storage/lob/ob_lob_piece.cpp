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

#define USING_LOG_PREFIX STORAGE

#include "lib/oblog/ob_log.h"
#include "ob_lob_piece.h"
#include "storage/tx_storage/ob_access_service.h"
#include "share/schema/ob_table_dml_param.h"
#include "storage/tx_storage/ob_ls_map.h"     // ObLSIterator


namespace oceanbase
{
namespace storage
{


int ObLobPieceUtil::transform_piece_id(blocksstable::ObDatumRow* row, ObLobPieceInfo &info)
{
  info.piece_id_ = row->storage_datums_[0].get_uint64();
  return OB_SUCCESS;
}

int ObLobPieceUtil::transform_len(blocksstable::ObDatumRow* row, ObLobPieceInfo &info)
{
  info.len_ = row->storage_datums_[1].get_uint32();
  return OB_SUCCESS;
}

int ObLobPieceUtil::transform_macro_id(blocksstable::ObDatumRow* row, ObLobPieceInfo &info)
{
  int ret = OB_SUCCESS;
  ObString ser_macro_id = row->storage_datums_[2].get_string();;
  int64_t pos = 0;
  if (OB_FAIL(info.macro_id_.deserialize(ser_macro_id.ptr(), ser_macro_id.length(), pos))) {
    LOG_WARN("deserialize macro id from buffer failed.", K(ret), K(ser_macro_id));
  }
  return ret;
}

int ObLobPieceUtil::transform(blocksstable::ObDatumRow* row, ObLobPieceInfo &info)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(row)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("row is null.", K(ret));
  } else if (!row->is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), KPC(row));
  } else if (row->get_column_count() != 3) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid lob meta row.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_piece_id(row, info))) {
    LOG_WARN("get lob id from row failed.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_len(row, info))) {
    LOG_WARN("get seq id from row failed.", K(ret), KPC(row));
  } else if (OB_FAIL(transform_macro_id(row, info))) {
    LOG_WARN("get macro id from row failed.", K(ret), KPC(row));
  }
  return ret;
}

int ObLobPieceManager::get(
    ObLobAccessParam& param,
    uint64_t piece_id,
    ObLobPieceInfo& info)
{
  int ret = OB_SUCCESS;
  ObILobApator *adapter = &persistent_lob_adapter_;
  if (OB_FAIL(adapter->get_lob_data(param, piece_id, info))) {
    LOG_WARN("open lob scan iter failed.");
  }
  return ret;
}


int ObLobPieceManager::write(ObLobAccessParam& param, ObLobPieceInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.write_lob_piece_tablet(param, in_row))) {
    LOG_WARN("write lob piece failed.");
  }
  return ret;
}

int ObLobPieceManager::erase(ObLobAccessParam& param, ObLobPieceInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.erase_lob_piece_tablet(param, in_row))) {
    LOG_WARN("write lob piece failed.");
  }
  return ret;
}

int ObLobPieceManager::update(ObLobAccessParam& param, ObLobPieceInfo& in_row)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(persistent_lob_adapter_.update_lob_piece_tablet(param, in_row))) {
    LOG_WARN("write lob piece failed.");
  }
  return ret;
}

} // storage
} // oceanbase
