/**
 * Copyright (c) 2024 OceanBase
 * SPDX-License-Identifier: Apache-2.0
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
