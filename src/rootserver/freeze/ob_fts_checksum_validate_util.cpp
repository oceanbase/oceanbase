//Copyright (c) 2024 OceanBase
// OceanBase is licensed under Mulan PubL v2.
// You can use this software according to the terms and conditions of the Mulan PubL v2.
// You may obtain a copy of Mulan PubL v2 at:
//          http://license.coscl.org.cn/MulanPubL-2.0
// THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
// EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
// MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
// See the Mulan PubL v2 for more details.
#include "rootserver/freeze/ob_fts_checksum_validate_util.h"
#include "share/rc/ob_tenant_base.h"
#include "storage/compaction/ob_compaction_util.h"

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
  return VERIFY_FTS_CHECKSUM && count() > 0;
}

} // namespace rootserver
} // namespace oceanbase
