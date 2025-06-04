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

#pragma once

#include "lib/oblog/ob_log_print_kv.h"
#include "common/ob_tablet_id.h"
#include "storage/ddl/ob_direct_insert_define.h"

namespace oceanbase
{
namespace storage
{
class ObDDLIncLogBasic final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncLogBasic();
  ~ObDDLIncLogBasic() = default;
  int init(const ObTabletID &tablet_id,
           const ObTabletID &lob_meta_tablet_id,
           const ObDirectLoadType direct_load_type = ObDirectLoadType::DIRECT_LOAD_INCREMENTAL);
  uint64_t hash() const;
  int hash(uint64_t &hash_val) const;
  void reset()
  {
    tablet_id_.reset();
    lob_meta_tablet_id_.reset();
    direct_load_type_ = ObDirectLoadType::DIRECT_LOAD_INVALID;
  }
  bool operator ==(const ObDDLIncLogBasic &other) const
  {
    return tablet_id_ == other.get_tablet_id()
              && lob_meta_tablet_id_ == other.get_lob_meta_tablet_id()
              && direct_load_type_ == other.get_direct_load_type();
  }
  bool is_valid() const
  {
    return tablet_id_.is_valid();
  }
  const ObTabletID &get_tablet_id() const { return tablet_id_; }
  const ObTabletID &get_lob_meta_tablet_id() const { return lob_meta_tablet_id_; }
  const ObDirectLoadType &get_direct_load_type() const { return direct_load_type_; }
  TO_STRING_KV(K_(tablet_id), K_(lob_meta_tablet_id), K_(direct_load_type));
private:
  ObTabletID tablet_id_;
  ObTabletID lob_meta_tablet_id_;
  ObDirectLoadType direct_load_type_;
};

class ObDDLIncStartLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncStartLog();
  ~ObDDLIncStartLog() = default;
  int init(const ObDDLIncLogBasic &log_basic);
  bool is_valid() const { return log_basic_.is_valid(); }
  const ObDDLIncLogBasic &get_log_basic() const { return log_basic_; }
  TO_STRING_KV(K_(log_basic));
private:
  ObDDLIncLogBasic log_basic_;
};

class ObDDLIncCommitLog final
{
  OB_UNIS_VERSION_V(1);
public:
  ObDDLIncCommitLog();
  ~ObDDLIncCommitLog() = default;
  int init(const ObDDLIncLogBasic &log_basic);
  bool is_valid() const { return log_basic_.is_valid(); }
  const ObDDLIncLogBasic &get_log_basic() const { return log_basic_; }
  TO_STRING_KV(K_(log_basic));
private:
  ObDDLIncLogBasic log_basic_;
};

} // namespace storage
} // namespace oceanbase
