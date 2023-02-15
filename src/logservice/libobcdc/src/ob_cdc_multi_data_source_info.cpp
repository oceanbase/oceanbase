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
 *
 * MultiDataSourceNode and MultiDataSourceInfo
 */

#define USING_LOG_PREFIX OBLOG_PARSER

#include "lib/ob_errno.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_macro_utils.h"

#include "ob_cdc_multi_data_source_info.h"

namespace oceanbase
{
namespace libobcdc
{

MultiDataSourceNode::MultiDataSourceNode() :
    lsn_(),
    tx_buf_node_()
{}

int MultiDataSourceNode::init(
    const palf::LSN &lsn,
    const transaction::ObTxDataSourceType &type,
    const char *buf,
    const int64_t buf_size)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(0 >= buf_size)
      || OB_UNLIKELY(! lsn.is_valid())
      || OB_UNLIKELY(transaction::ObTxDataSourceType::UNKNOWN >= type
      || transaction::ObTxDataSourceType::MAX_TYPE <= type)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(lsn), K(type), K(buf), K(buf_size));
  } else {
    lsn_ = lsn;
    common::ObString data(buf_size, buf);

    if (OB_FAIL(tx_buf_node_.init(type, data))) {
      LOG_ERROR("init tx_buf_node failed", KR(ret), K(lsn), K(type), K(data), K(buf_size));
    }
  }

  return ret;
}

void MultiDataSourceNode::reset()
{
  lsn_.reset();
  tx_buf_node_.reset();
}

MultiDataSourceInfo::MultiDataSourceInfo() :
    has_ls_table_op_(false),
    ls_attr_(),
    tablet_change_info_arr_(),
    has_ddl_trans_op_(false)
{}

void MultiDataSourceInfo::reset()
{
  has_ls_table_op_ = false;
  ls_attr_.reset();
  tablet_change_info_arr_.reset();
  has_ddl_trans_op_ = false;
}

int MultiDataSourceInfo::push_back_tablet_change_info(const ObCDCTabletChangeInfo &tablet_change_info)
{
  int ret = OB_SUCCESS;

  if (OB_UNLIKELY(! tablet_change_info.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_ERROR("invalid argument", KR(ret), K(tablet_change_info));
  } else if (OB_FAIL(tablet_change_info_arr_.push_back(tablet_change_info))) {
    LOG_ERROR("push_back tablet_change_info failed", KR(ret), K(tablet_change_info), KPC(this));
  }

  return ret;
}

int64_t MultiDataSourceInfo::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (NULL != buf && buf_len > 0) {
    if (has_ls_table_op_) {
      (void)common::databuff_printf(buf, buf_len, pos, "{ls_table_op: %s", to_cstring(ls_attr_));
    } else {
      (void)common::databuff_printf(buf, buf_len, pos, "has_ls_table_op: false");
    }

    (void)common::databuff_printf(buf, buf_len, pos, ", is_ddl_trans: %d", has_ddl_trans_op_);
    if (has_tablet_change_op()) {
      (void)common::databuff_printf(buf, buf_len, pos, ", tablet_change_info: %s}", to_cstring(tablet_change_info_arr_));
    } else {
      (void)common::databuff_printf(buf, buf_len, pos, ", tablet_change_info: None}");
    }
  }

  return pos;
}

} // namespace libobcdc
} // namespace oceanbase
