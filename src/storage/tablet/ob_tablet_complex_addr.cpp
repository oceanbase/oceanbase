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

#include "storage/tablet/ob_tablet_complex_addr.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace storage
{
template <>
int64_t ObTabletComplexAddr<mds::MdsDumpKV>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    databuff_printf(buf, buf_len, pos, "{ptr:%p", static_cast<void*>(ptr_));
    if (nullptr != ptr_) {
      databuff_printf(buf, buf_len, pos, ", dump_node:");
      const mds::MdsDumpNode &dump_node = ptr_->v_;
      dump_node.simple_to_string(buf, buf_len, pos);
    }
    databuff_print_json_kv_comma(buf, buf_len, pos, "addr", addr_);
    databuff_printf(buf, buf_len, pos, "}");
  }
  return pos;
}

template <>
int64_t ObTabletComplexAddr<share::ObTabletAutoincSeq>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    databuff_printf(buf, buf_len, pos, "{ptr:%p", static_cast<void*>(ptr_));
    if (nullptr != ptr_) {
      databuff_printf(buf, buf_len, pos, ", auto_inc_seq:");
      databuff_print_obj(buf, buf_len, pos, *ptr_);
    }
    databuff_print_json_kv_comma(buf, buf_len, pos, "addr", addr_);
    databuff_printf(buf, buf_len, pos, "}");
  }
  return pos;
}

template <>
int64_t ObTabletComplexAddr<ObTabletDumpedMediumInfo>::to_string(char *buf, const int64_t buf_len) const
{
  int64_t pos = 0;

  if (OB_ISNULL(buf) || buf_len <= 0) {
    // do nothing
  } else {
    databuff_printf(buf, buf_len, pos, "{ptr:%p", static_cast<void*>(ptr_));
    if (nullptr != ptr_) {
      databuff_printf(buf, buf_len, pos, ", dumped_medium_info:");
      ptr_->simple_to_string(buf, buf_len, pos);
    }
    databuff_print_json_kv_comma(buf, buf_len, pos, "addr", addr_);
    databuff_printf(buf, buf_len, pos, "}");
  }
  return pos;
}
} // namespace storage
} // namespace oceanbase