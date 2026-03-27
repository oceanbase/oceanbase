/**
 * Copyright (c) 2021 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#ifndef OCEANBASE_TRANSACTION_OB_TRANS_VERSION_MGR_
#define OCEANBASE_TRANSACTION_OB_TRANS_VERSION_MGR_

#include "ob_trans_define.h"
#include "lib/lock/ob_bucket_lock.h"
#include "lib/hash/ob_linear_hash_map.h"
#include "share/config/ob_server_config.h"
#include "ob_trans_factory.h"
#include "ob_trans_define.h"

namespace oceanbase
{
namespace transaction
{

class ObITransVersionMgr
{
public:
  ObITransVersionMgr() {}
  virtual ~ObITransVersionMgr() {}
  virtual void destroy() = 0;
  virtual void reset() = 0;
public:
  virtual int get_and_update_local_trans_version(int64_t &local_trans_version) = 0;
  virtual int get_local_trans_version(int64_t &local_trans_version) = 0;
  virtual int update_local_trans_version(const int64_t local_trans_version) = 0;
  virtual int get_publish_version(int64_t &publish_version) = 0;
  virtual int update_publish_version(const int64_t publish_version) = 0;
};

class ObTransVersionMgr : public ObITransVersionMgr
{
public:
  ObTransVersionMgr() { reset(); }
  ~ObTransVersionMgr() { destroy(); }
  void destroy();
  void reset();
public:
  int get_and_update_local_trans_version(int64_t &local_trans_version);
  int get_local_trans_version(int64_t &local_trans_version);
  int update_local_trans_version(const int64_t local_trans_version);
  int get_publish_version(int64_t &publish_version);
  int update_publish_version(const int64_t publish_version);
public:
  static ObTransVersionMgr &get_instance();
private:
  int update_local_trans_version_(const int64_t local_trans_version);
  int update_publish_version_(const int64_t publish_version);
private:
  DISALLOW_COPY_AND_ASSIGN(ObTransVersionMgr);
private:
  int64_t publish_version_;
  int64_t local_trans_version_;
};

} // transaction
} // oceanbase

#endif  // OCEANBASE_TRANSACTION_OB_TRANS_VERSION_MGR_
