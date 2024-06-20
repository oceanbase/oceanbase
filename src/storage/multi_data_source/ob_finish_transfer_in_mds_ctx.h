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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_OB_FINISH_TRANSFER_IN_MDS_CTX_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_OB_FINISH_TRANSFER_IN_MDS_CTX_H

#include "mds_ctx.h"
#include "lib/container/ob_array.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"
#include "lib/container/ob_array_serialization.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{
class MdsTableHandle;
struct ObFinishTransferInMdsCtxVersion
{
  enum VERSION {
    FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1 = 1,
    FINISH_TRANSFER_IN_MDS_CTX_VERSION_V2 = 2,
    MAX
  };
  static const VERSION CURRENT_CTX_VERSION = FINISH_TRANSFER_IN_MDS_CTX_VERSION_V2;
  static bool is_valid(const ObFinishTransferInMdsCtxVersion::VERSION &version) {
    return version >= FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1
        && version < MAX;
  }
};

class ObFinishTransferInMdsCtx : public MdsCtx
{
public:
  ObFinishTransferInMdsCtx();
  ObFinishTransferInMdsCtx(const MdsWriter &writer);
  virtual ~ObFinishTransferInMdsCtx();
  virtual int serialize(char *buf, const int64_t len, int64_t &pos) const;
  virtual int deserialize(const char *buf, const int64_t len, int64_t &pos);
  virtual int64_t get_serialize_size(void) const;

  virtual void on_redo(const share::SCN &redo_scn) override;
  share::SCN &get_redo_scn() { return redo_scn_; }

  INHERIT_TO_STRING_KV("MdsCtx", MdsCtx, K_(version), K_(redo_scn));
private:
  ObFinishTransferInMdsCtxVersion::VERSION version_;
  share::SCN redo_scn_;
private:
  DISALLOW_COPY_AND_ASSIGN(ObFinishTransferInMdsCtx);
};

} //mds
} //storage
} //oceanbase

#endif //SHARE_STORAGE_MULTI_DATA_SOURCE_OB_FINISH_TRANSFER_IN_MDS_CTX_H
