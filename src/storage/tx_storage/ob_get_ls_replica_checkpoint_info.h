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

#ifndef OCEANBASE_STORAGE_TX_STORAGE_OB_GET_LS_REPLICA_CHECKPOINT_INFO_H_
#define OCEANBASE_STORAGE_TX_STORAGE_OB_GET_LS_REPLICA_CHECKPOINT_INFO_H_

#include "lib/ob_define.h"
#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/utility/ob_unify_serialize.h"
#include "share/ob_ls_id.h"
#include "share/scn.h"

namespace oceanbase
{
namespace obrpc
{

struct ObGetLSReplicaCheckpointInfoArg
{
  OB_UNIS_VERSION(1);
public:
  ObGetLSReplicaCheckpointInfoArg(): tenant_id_(common::OB_INVALID_TENANT_ID), ls_id_() {}
  ~ObGetLSReplicaCheckpointInfoArg() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id);
  int assign(const ObGetLSReplicaCheckpointInfoArg &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLSReplicaCheckpointInfoArg);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
};

struct ObGetLSReplicaCheckpointInfoRes
{
  OB_UNIS_VERSION(1);
public:
  ObGetLSReplicaCheckpointInfoRes(): tenant_id_(common::OB_INVALID_TENANT_ID),
                                     ls_id_(),
                                     checkpoint_scn_(share::SCN::min_scn()) {}
  ~ObGetLSReplicaCheckpointInfoRes() {}
  bool is_valid() const;
  int init(const uint64_t tenant_id, const share::ObLSID &ls_id,
      const share::SCN &checkpoint_scn);
  int assign(const ObGetLSReplicaCheckpointInfoRes &other);
  TO_STRING_KV(K_(tenant_id), K_(ls_id), K_(checkpoint_scn));

  uint64_t get_tenant_id() const
  {
    return tenant_id_;
  }
  share::ObLSID get_ls_id() const
  {
    return ls_id_;
  }
  share::SCN get_checkpoint_scn() const
  {
    return checkpoint_scn_;
  }
private:
  DISALLOW_COPY_AND_ASSIGN(ObGetLSReplicaCheckpointInfoRes);
private:
  uint64_t tenant_id_;
  share::ObLSID ls_id_;
  share::SCN checkpoint_scn_;
};

} // namespace obrpc
} // namespace oceanbase

#endif // OCEANBASE_STORAGE_TX_STORAGE_OB_GET_LS_REPLICA_CHECKPOINT_INFO_H_
