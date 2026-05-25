/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_LOG_ACTIVE_KEEP_ALIVE_H_
#define OCEANBASE_LOG_ACTIVE_KEEP_ALIVE_H_

#include "lib/container/ob_se_array.h"
#include "lib/hash/ob_hashmap.h"
#include "lib/net/ob_addr.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_macro_utils.h"

namespace oceanbase
{

namespace palf
{
class IPalfHandleImpl;
} // namespace palf

#ifdef OB_BUILD_ARBITRATION
namespace palflite
{
class PalfEnvKey;
class PalfEnvLite;
};
#endif // OB_BUILD_ARBITRATION

namespace logservice
{

// Active keepalive worker on arbserver side:
// - periodically refreshes probe targets from current palf membership
// - probes connectivity by actively establishing new connections (ObNetKeepAlive::probe_connectivity)
// - prints ERROR logs when probe fails or dst is blacklisted for new connections
class ObLogActiveKeepAlive
{
public:
  ObLogActiveKeepAlive();
  ~ObLogActiveKeepAlive() { destroy(); }

  int init(const common::ObAddr &self_addr);
  bool is_inited() const;
  void destroy();

  // One tick work, expected to be called periodically (e.g. every 10s).
  void do_work();
  TO_STRING_KV(K_(is_inited),
               K_(self_addr),
               "addr_set_size", addr_set_.size());
private:
  typedef hash::ObHashMap<common::ObAddr, int64_t> AddrSet;
  struct CollectAddrFunctor
  {
  public:
    CollectAddrFunctor(ObLogActiveKeepAlive *active_keep_alive, const int64_t cluster_id, const int64_t tenant_id);
    ~CollectAddrFunctor();
    int operator()(palf::IPalfHandleImpl *handle);
  private:
    ObLogActiveKeepAlive *active_keep_alive_;
    int64_t cluster_id_;
    int64_t tenant_id_;
  };
#ifdef OB_BUILD_ARBITRATION
  struct ArbEnvForEacher
  {
  public:
    ArbEnvForEacher(ObLogActiveKeepAlive *active_keep_alive);
    ~ArbEnvForEacher();
    bool operator()(const palflite::PalfEnvKey &key, palflite::PalfEnvLite *env);
  private:
    ObLogActiveKeepAlive *active_keep_alive_;
  };
#endif // OB_BUILD_ARBITRATION
private:
  void probe_once_();
  int refresh_addr_set_();
  int refresh_addr_set_in_arb_mode_();
  int refresh_addr_set_in_normal_observer_();
  int collect_addr_from_handle_(const int64_t cluster_id, palf::IPalfHandleImpl *handle);

private:
  bool is_inited_;
  common::ObAddr self_addr_;
  AddrSet addr_set_;

  DISABLE_COPY_ASSIGN(ObLogActiveKeepAlive);
};

} // namespace logservice
} // namespace oceanbase

#endif // OCEANBASE_LOG_ACTIVE_KEEP_ALIVE_H_
