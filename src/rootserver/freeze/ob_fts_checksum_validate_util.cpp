// Copyright (c) 2024 OceanBase
// SPDX-License-Identifier: Apache-2.0
#include "rootserver/freeze/ob_fts_checksum_validate_util.h"
#include "share/rc/ob_tenant_base.h"

namespace oceanbase
{
namespace rootserver
{
ObFTSGroup::ObFTSGroup()
    : data_table_id_(0),
      rowkey_doc_index_id_(0),
      doc_rowkey_index_id_(0),
      index_info_()
{
  index_info_.set_attr(ObMemAttr(MTL_ID(), "FTS_GROUP"));
}

ObFTSGroupArray::ObFTSGroupArray()
  : fts_groups_()
{
  fts_groups_.set_attr(ObMemAttr(MTL_ID(), "FTS_INFO_ARR"));
}

bool ObFTSGroupArray::need_check_fts() const
{
  return count() > 0;
}

} // namespace rootserver
} // namespace oceanbase
