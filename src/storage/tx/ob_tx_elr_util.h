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

#ifndef OCEANBASE_TX_ELR_UTIL_
#define OCEANBASE_TX_ELR_UTIL_

#include "ob_trans_define.h"

namespace oceanbase
{

namespace transaction
{

class ObTxDesc;

class ObTxELRUtil
{
public:
  ObTxELRUtil() : last_refresh_ts_(0),
                  can_tenant_elr_(false) {}
  int check_and_update_tx_elr_info(ObTxDesc &tx);
  bool is_can_tenant_elr() const { return can_tenant_elr_; }
  void reset()
  {
    last_refresh_ts_ = 0;
    can_tenant_elr_ = false;
  }
  TO_STRING_KV(K_(last_refresh_ts), K_(can_tenant_elr));
private:
  void refresh_elr_tenant_config_();
private:
  static const int64_t REFRESH_INTERVAL = 5000000;
private:
  int64_t last_refresh_ts_;
  bool can_tenant_elr_;
};

} // transaction
} // oceanbase

#endif
