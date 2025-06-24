/**
 * Copyright (c) 2025 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#ifndef _OB_TABLE_META_PROCESSOR_H
#define _OB_TABLE_META_PROCESSOR_H

#include "ob_table_meta_handler.h"

namespace oceanbase
{
namespace observer
{

/// @see RPC_S(PR5 table_api_meta_info_execute, obrpc::OB_TABLE_API_META_INFO_EXECUTE, (ObTableMetaRequest),
/// ObTableMetaResponse);
class ObTableMetaP: public oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_META_INFO_EXECUTE> >
{
  typedef oceanbase::observer::ObTableRpcProcessor<ObTableRpcProxy::ObRpc<OB_TABLE_API_META_INFO_EXECUTE> > ParentType;
public:
  explicit ObTableMetaP(const ObGlobalContext &gctx);
  virtual ~ObTableMetaP() = default;

  int try_process();
protected:
  virtual int check_arg() override { return OB_SUCCESS; }
  virtual void reset_ctx() override { return; }
  virtual uint64_t get_request_checksum() override {
    uint64_t checksum = 0;
    checksum = ob_crc64(checksum, &arg_.meta_type_, sizeof(arg_.meta_type_));
    checksum = ob_crc64(checksum, &arg_.data_, sizeof(arg_.data_));
    return checksum;
  }
  virtual table::ObTableEntityType get_entity_type() override { return table::ObTableEntityType::ET_HKV; }
  virtual bool is_kv_processor() override { return true; }
};


}
}

#endif
