/**
 * Copyright (c) 2024 OceanBase
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

#include "storage/tx/ob_trans_define.h"

namespace oceanbase
{
namespace storage
{

struct ObDirectLoadTransParam
{
public:
  ObDirectLoadTransParam() : tx_desc_(nullptr) {}
  ~ObDirectLoadTransParam() {}
  void reset()
  {
    tx_desc_ = nullptr;
    tx_id_.reset();
    tx_seq_.reset();
  }
  bool is_valid() const { return nullptr != tx_desc_ && tx_id_.is_valid() && tx_seq_.is_valid(); }
  TO_STRING_KV(KPC_(tx_desc), K_(tx_id), K_(tx_seq));

public:
  transaction::ObTxDesc *tx_desc_;
  transaction::ObTransID tx_id_;
  transaction::ObTxSEQ tx_seq_;
};

} // namespace storage
} // namespace oceanbase
