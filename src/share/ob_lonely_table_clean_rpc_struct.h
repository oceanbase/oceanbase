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

#ifndef OCEANBASE_RPC_OB_LONELY_TABLE_CLEAN_RPC_STRUCT_H_
#define OCEANBASE_RPC_OB_LONELY_TABLE_CLEAN_RPC_STRUCT_H_

#include "share/ob_rpc_struct.h"

namespace oceanbase
{
namespace obrpc
{

struct ObForceDropLonelyLobAuxTableArg: public ObDDLArg
{
  OB_UNIS_VERSION(1);
public:
  ObForceDropLonelyLobAuxTableArg():
      ObDDLArg(),
      tenant_id_(common::OB_INVALID_ID),
      data_table_id_(common::OB_INVALID_ID),
      aux_lob_meta_table_id_(common::OB_INVALID_ID),
      aux_lob_piece_table_id_(common::OB_INVALID_ID)
  {}
  virtual ~ObForceDropLonelyLobAuxTableArg() {}

  int init(const uint64_t tenant_id,
           const uint64_t data_table_id,
           const uint64_t aux_lob_meta_table_id,
           const uint64_t aux_lob_piece_table_id)
  {
    int ret = OB_SUCCESS;
    if (common::OB_INVALID_ID == tenant_id
        || common::OB_INVALID_ID == data_table_id
        || common::OB_INVALID_ID == aux_lob_meta_table_id
        || common::OB_INVALID_ID == aux_lob_piece_table_id) {
      ret = OB_INVALID_ARGUMENT;
      SHARE_LOG(WARN, "invalid arg", KR(ret), K(tenant_id), K(data_table_id), K(aux_lob_meta_table_id), K(aux_lob_piece_table_id));
    } else {
      exec_tenant_id_ = tenant_id;
      tenant_id_ = tenant_id;
      data_table_id_ = data_table_id;
      aux_lob_meta_table_id_ = aux_lob_meta_table_id;
      aux_lob_piece_table_id_ = aux_lob_piece_table_id;
    }
    return ret;
  }

  void reset()
  {
    ObDDLArg::reset();
    tenant_id_ = common::OB_INVALID_ID;
    data_table_id_ = common::OB_INVALID_ID;
    aux_lob_meta_table_id_ = common::OB_INVALID_ID;
    aux_lob_piece_table_id_ = common::OB_INVALID_ID;
  }
  bool is_valid() const
  {
    return common::OB_INVALID_ID != tenant_id_
      && common::OB_INVALID_ID != data_table_id_
      && common::OB_INVALID_ID != aux_lob_meta_table_id_
      && common::OB_INVALID_ID != aux_lob_piece_table_id_;
  }
  int assign(const ObForceDropLonelyLobAuxTableArg &other)
  {
    int ret = OB_SUCCESS;
    if (OB_FAIL(ObDDLArg::assign(other))) {
      SHARE_LOG(WARN, "fail to assign ddl arg", KR(ret));
    } else {
      tenant_id_ = other.tenant_id_;
      data_table_id_ = other.data_table_id_;
      aux_lob_meta_table_id_ = other.aux_lob_meta_table_id_;
      aux_lob_piece_table_id_ = other.aux_lob_piece_table_id_;
    }
    return ret;
  }

  uint64_t get_tenant_id() const { return tenant_id_; }
  uint64_t get_data_table_id() const { return data_table_id_; }
  uint64_t get_aux_lob_meta_table_id() const { return aux_lob_meta_table_id_; }
  uint64_t get_aux_lob_piece_table_id() const { return aux_lob_piece_table_id_; }

private:
  uint64_t tenant_id_;
  uint64_t data_table_id_;
  uint64_t aux_lob_meta_table_id_;
  uint64_t aux_lob_piece_table_id_;
public:
  INHERIT_TO_STRING_KV("ObDDLArg", ObDDLArg, K_(tenant_id), K_(data_table_id), K_(aux_lob_meta_table_id), K_(aux_lob_piece_table_id));
};

}//end namespace obrpc
}//end namespace oceanbase
#endif
