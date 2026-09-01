/**
 * Copyright (c) 2021 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 */

#ifndef OCEANBASE_LOG_EXTERNAL_ADDR_CONFIG_H_
#define OCEANBASE_LOG_EXTERNAL_ADDR_CONFIG_H_

#include "lib/container/ob_iarray.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase
{
namespace logservice
{

enum class ObLogExternalAddrSource : int8_t
{
  NONE = 0,
  CDC_RS_LIST,
  CDC_TENANT_ENDPOINT,
  RESTORE_SOURCE
};

enum class ObLogExternalAddrState : int8_t
{
  // The source intentionally supplies no usable mapping candidate.
  NOT_PROVIDED = 0,
  // All endpoints share one IP. Their ports may differ because route mapping
  // keeps the observer-reported RPC port.
  UNIQUE,
  // Multiple external IPs exist; choosing one would be unsafe.
  AMBIGUOUS,
  // The source exists but contains an empty or invalid address.
  INVALID
};

struct ObLogExternalAddrConfig
{
  ObLogExternalAddrSource source_;
  ObLogExternalAddrState state_;
  common::ObAddr external_addr_;
  int64_t version_;

  ObLogExternalAddrConfig() { reset(); }

  void reset()
  {
    source_ = ObLogExternalAddrSource::NONE;
    state_ = ObLogExternalAddrState::NOT_PROVIDED;
    external_addr_.reset();
    version_ = 0;
  }

  bool is_unique() const
  {
    return ObLogExternalAddrState::UNIQUE == state_ && external_addr_.is_valid();
  }

  bool is_provided() const
  {
    return ObLogExternalAddrState::NOT_PROVIDED != state_;
  }

  int assign(const ObLogExternalAddrSource source,
      const common::ObIArray<common::ObAddr> &addr_list,
      const int64_t version = 1)
  {
    int ret = OB_SUCCESS;
    reset();
    source_ = source;
    version_ = version;
    if (addr_list.empty() || !addr_list.at(0).is_valid()) {
      state_ = ObLogExternalAddrState::INVALID;
    } else {
      state_ = ObLogExternalAddrState::UNIQUE;
      external_addr_ = addr_list.at(0);
      for (int64_t idx = 1; idx < addr_list.count(); ++idx) {
        if (!addr_list.at(idx).is_valid()) {
          state_ = ObLogExternalAddrState::INVALID;
          external_addr_.reset();
          break;
        } else if (!external_addr_.is_equal_except_port(addr_list.at(idx))) {
          // A distributed RS list is valid input. Preserve ambiguity and let
          // LRS reject it only when a loopback route actually needs mapping.
          state_ = ObLogExternalAddrState::AMBIGUOUS;
          external_addr_.reset();
          break;
        }
      }
    }
    return ret;
  }

  TO_STRING_KV("source", static_cast<int8_t>(source_),
      "state", static_cast<int8_t>(state_), K_(external_addr), K_(version));
};

} // namespace logservice
} // namespace oceanbase

#endif
