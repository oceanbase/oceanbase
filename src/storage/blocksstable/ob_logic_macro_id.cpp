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

#include "storage/blocksstable/ob_logic_macro_id.h"

namespace oceanbase
{
namespace blocksstable
{
int64_t ObLogicMacroBlockId::hash() const
{
  int64_t hash_val = 0;
  hash_val = common::murmurhash(&data_seq_, sizeof(data_seq_), hash_val);
  hash_val = common::murmurhash(&logic_version_, sizeof(logic_version_), hash_val);
  hash_val = common::murmurhash(&tablet_id_, sizeof(tablet_id_), hash_val);

  return hash_val;
}

bool ObLogicMacroBlockId::operator==(const ObLogicMacroBlockId &other) const
{
  return data_seq_     == other.data_seq_
      && logic_version_ == other.logic_version_
      && tablet_id_    == other.tablet_id_;
}

bool ObLogicMacroBlockId::operator!=(const ObLogicMacroBlockId &other) const
{
  return !(operator==(other));
}

bool ObLogicMacroBlockId::operator<(const ObLogicMacroBlockId &other) const
{
  bool bool_ret = false;
  if (tablet_id_ < other.tablet_id_) {
    bool_ret = true;
  } else if (tablet_id_ > other.tablet_id_) {
    bool_ret= false;
  } else if (logic_version_ < other.logic_version_) {
    bool_ret = true;
  } else if (logic_version_ > other.logic_version_) {
    bool_ret = false;
  } else if (data_seq_ < other.data_seq_) {
    bool_ret = true;
  } else if (data_seq_ > other.data_seq_) {
    bool_ret = false;
  }
  return bool_ret;
}

bool ObLogicMacroBlockId::operator>(const ObLogicMacroBlockId &other) const
{
  bool bool_ret = false;
  if (tablet_id_ < other.tablet_id_) {
    bool_ret = false;
  } else if (tablet_id_ > other.tablet_id_) {
    bool_ret= true;
  } else if (logic_version_ < other.logic_version_) {
    bool_ret = false;
  } else if (logic_version_ > other.logic_version_) {
    bool_ret = true;
  } else if (data_seq_ < other.data_seq_) {
    bool_ret = false;
  } else if (data_seq_ > other.data_seq_) {
    bool_ret = true;
  }
  return bool_ret;
}

void ObLogicMacroBlockId::reset() {
  logic_version_ = 0;
  data_seq_ = 0;
  tablet_id_ = 0;
}

OB_SERIALIZE_MEMBER(ObLogicMacroBlockId, data_seq_, logic_version_, tablet_id_);
} // blocksstable
} // oceanbase