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

#include "ob_all_virtual_server_compaction_event_history.h"
#include "storage/compaction/ob_compaction_util.h"

namespace oceanbase
{
using namespace storage;
using namespace common;
namespace observer
{
ObAllVirtualServerCompactionEventHistory::ObAllVirtualServerCompactionEventHistory()
    : event_(),
      event_iter_(),
      is_inited_(false)
{
}

ObAllVirtualServerCompactionEventHistory::~ObAllVirtualServerCompactionEventHistory()
{
  reset();
}

int ObAllVirtualServerCompactionEventHistory::init()
{
  int ret = OB_SUCCESS;
  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    SERVER_LOG(WARN, "ObAllVirtualServerCompactionEventHistory has been inited", K(ret));
  } else if (OB_FAIL(event_iter_.open(effective_tenant_id_))) {
    SERVER_LOG(WARN, "Fail to open suggestion iter", K(ret));
  } else {
    is_inited_ = true;
  }
  return ret;
}

int ObAllVirtualServerCompactionEventHistory::inner_get_next_row(common::ObNewRow *&row)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    SERVER_LOG(WARN, "ObAllVirtualServerCompactionEventHistory has been inited", K(ret));
  } else if (OB_FAIL(event_iter_.get_next_info(event_))) {
    if (OB_ITER_END != ret) {
      STORAGE_LOG(WARN, "Fail to get next suggestion info", K(ret));
    }
  } else if (OB_FAIL(fill_cells())) {
    STORAGE_LOG(WARN, "Fail to fill cells", K(ret), K(event_));
  } else {
    row = &cur_row_;
  }
  return ret;
}

int ObAllVirtualServerCompactionEventHistory::fill_cells()
{
  int ret = OB_SUCCESS;
  int tmp_ret = OB_SUCCESS;
  const int64_t col_count = output_column_ids_.count();
  ObObj *cells = cur_row_.cells_;
  int64_t compression_ratio = 0;
  for (int64_t i = 0; OB_SUCC(ret) && i < col_count; ++i) {
    uint64_t col_id = output_column_ids_.at(i);
    switch (col_id) {
    case SVR_IP:
      //svr_ip
      if (ObServerConfig::get_instance().self_addr_.ip_to_string(ip_buf_, sizeof(ip_buf_))) {
        cells[i].set_varchar(ip_buf_);
        cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      }
      break;
    case SVR_PORT:
      //svr_port
      cells[i].set_int(ObServerConfig::get_instance().self_addr_.get_port());
      break;
    case ZONE:
      //zone
      cells[i].set_varchar(GCONF.zone);
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case TENANT_ID:
      cells[i].set_int(event_.tenant_id_);
      break;
    case MERGE_TYPE:
      cells[i].set_varchar(merge_type_to_str(event_.merge_type_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case COMPACTION_SCN:
      cells[i].set_uint64(event_.compaction_scn_ < 0 ? 0 : event_.compaction_scn_);
      break;
    case EVENT_TIMESTAMP:
      cells[i].set_timestamp(event_.timestamp_);
      break;
    case EVENT:
      if (OB_TMP_FAIL(event_.generate_event_str(event_buf_, sizeof(event_buf_)))) {
        SERVER_LOG(WARN, "failed to generate event str", K(tmp_ret), K(event_));
        cells[i].set_varchar("");
      } else {
        cells[i].set_varchar(event_buf_);
      }
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    case ROLE:
      cells[i].set_varchar(compaction::ObServerCompactionEvent::get_comp_role_str(event_.role_));
      cells[i].set_collation_type(ObCharset::get_default_collation(ObCharset::get_default_charset()));
      break;
    default:
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "invalid column id", K(ret), K(col_id));
    }
  }

  return ret;
}

void ObAllVirtualServerCompactionEventHistory::reset()
{
  ObVirtualTableScannerIterator::reset();
  event_iter_.reset();
  memset(ip_buf_, 0, sizeof(ip_buf_));
  memset(event_buf_, 0, sizeof(event_buf_));
  is_inited_ = false;
}

} /* namespace observer */
} /* namespace oceanbase */
