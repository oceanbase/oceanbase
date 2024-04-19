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

#ifndef SHARE_STORAGE_MULTI_DATA_SOURCE_OB_START_TRANSFER_IN_MDS_CTX_H
#define SHARE_STORAGE_MULTI_DATA_SOURCE_OB_START_TRANSFER_IN_MDS_CTX_H

#include "mds_ctx.h"
#include "lib/container/ob_array.h"
#include "lib/container/ob_array_serialization.h"
#include "share/ob_ls_id.h"
#include "storage/multi_data_source/runtime_utility/mds_tenant_service.h"

namespace oceanbase
{
namespace storage
{
namespace mds
{

class MdsTableHandle;

struct ObStartTransferInMdsCtxVersion
{
  enum VERSION {
    START_TRANSFER_IN_MDS_CTX_VERSION_V1 = 1,
    START_TRANSFER_IN_MDS_CTX_VERSION_V2 = 2,
    START_TRANSFER_IN_MDS_CTX_VERSION_V3 = 3,
    MAX
  };
  static const VERSION CURRENT_CTX_VERSION = START_TRANSFER_IN_MDS_CTX_VERSION_V3;
  static bool is_valid(const ObStartTransferInMdsCtxVersion::VERSION &version) {
    return version >= START_TRANSFER_IN_MDS_CTX_VERSION_V1
        && version < MAX;
  }
};

class ObStartTransferInMdsCtx : public MdsCtx
{
public:
  ObStartTransferInMdsCtx();
  explicit ObStartTransferInMdsCtx(const MdsWriter &writer);
  virtual ~ObStartTransferInMdsCtx();
public:
  virtual void on_prepare(const share::SCN &prepare_version) override;
  virtual void on_abort(const share::SCN &abort_scn) override;

  virtual int serialize(char *buf, const int64_t len, int64_t &pos) const override;
  virtual int deserialize(const char *buf, const int64_t len, int64_t &pos) override;
  virtual int64_t get_serialize_size(void) const override;
public:
  void set_ls_id(const share::ObLSID &ls_id) { ls_id_ = ls_id; }
private:
  ObStartTransferInMdsCtxVersion::VERSION version_;
  share::ObLSID ls_id_;
  DISALLOW_COPY_AND_ASSIGN(ObStartTransferInMdsCtx);
};

} //mds
} //storage
} //oceanbase

#endif //SHARE_STORAGE_MULTI_DATA_SOURCE_OB_TRANSFER_IN_MDS_CTX_H
