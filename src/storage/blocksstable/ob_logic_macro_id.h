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

#ifndef OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOGIC_MACRO_BLOCK_ID_H_
#define OCEANBASE_STORAGE_BLOCKSSTABLE_OB_LOGIC_MACRO_BLOCK_ID_H_

#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "common/ob_action_flag.h"
#include "common/object/ob_obj_type.h"
#include "common/ob_accuracy.h"
#include "lib/checksum/ob_crc64.h"

namespace oceanbase
{
namespace blocksstable
{

struct ObLogicMacroBlockId
{
private:
  static const int64_t LOGIC_BLOCK_ID_VERSION = 1;

public:
  ObLogicMacroBlockId()
    : data_seq_(0), logic_version_(0), tablet_id_(0 /* ObTabletID::INVALID_TABLET_ID */)
  {}
  ObLogicMacroBlockId(const int64_t data_seq, const uint64_t logic_version, const int64_t tablet_id)
    : data_seq_(data_seq), logic_version_(logic_version), tablet_id_(tablet_id)
  {}

  int64_t hash() const;
  int hash(uint64_t &hash_val) const { hash_val = hash(); return OB_SUCCESS; }
  bool operator ==(const ObLogicMacroBlockId &other) const;
  bool operator !=(const ObLogicMacroBlockId &other) const;
  bool operator <(const ObLogicMacroBlockId &other) const;
  bool operator >(const ObLogicMacroBlockId &other) const;
  void reset();
  OB_INLINE bool is_valid() const
  {
    return data_seq_ >= 0 && logic_version_ > 0 && tablet_id_ > 0;
  }
  TO_STRING_KV(K_(data_seq), K_(logic_version), K_(tablet_id));

public:
  int64_t data_seq_;
  uint64_t logic_version_;
  int64_t tablet_id_;
  OB_UNIS_VERSION(LOGIC_BLOCK_ID_VERSION);
};

} // blocksstable
} // oceanbase

#endif
