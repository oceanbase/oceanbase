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
#include "ob_start_transfer_in_mds_ctx.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "mds_table_handle.h"
#include "share/ob_errno.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/high_availability/ob_ls_transfer_info.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{
namespace mds
{

//errsim def
ERRSIM_POINT_DEF(EN_START_TRANSFER_IN_ON_PREPARE);

ObStartTransferInMdsCtx::ObStartTransferInMdsCtx()
    : MdsCtx(),
      version_(ObStartTransferInMdsCtxVersion::CURRENT_CTX_VERSION),
      ls_id_()
{
}

ObStartTransferInMdsCtx::ObStartTransferInMdsCtx(const MdsWriter &writer)
    : MdsCtx(writer),
      version_(ObStartTransferInMdsCtxVersion::CURRENT_CTX_VERSION),
      ls_id_()
{
}

ObStartTransferInMdsCtx::~ObStartTransferInMdsCtx()
{
}

void ObStartTransferInMdsCtx::on_prepare(const share::SCN &prepare_version)
{
  // TODO(@muwei.ym): add ERRSIM code later
  MdsCtx::on_prepare(prepare_version);
}

void ObStartTransferInMdsCtx::on_abort(const share::SCN &abort_scn)
{
  mds::MdsCtx::on_abort(abort_scn);

  // TODO(@bowen.gbw): feature branch transfer_dml_ctrl_42x patch to master,
  // then add more logic, currently this code is just for compat
}

int ObStartTransferInMdsCtx::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t saved_pos = pos;
  const int64_t length = get_serialize_size();

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (!ObStartTransferInMdsCtxVersion::is_valid(version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid version", K(ret), K_(version));
  } else if (OB_UNLIKELY(length > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - pos));
  } else if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1 == version_) {
    if (OB_FAIL(MdsCtx::serialize(buf, len, pos))) {
      LOG_WARN("failed to serialize mds ctx", K(ret), K(len), K(pos));
    }
  } else if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V2 == version_
      || ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3 == version_) {
    if (OB_FAIL(serialization::encode(buf, len, pos, static_cast<int64_t>(version_)))) {
      LOG_WARN("failed to serialize start transfer in mds ctx version", K(ret), K(len), K(pos), K_(version));
    } else if (OB_FAIL(serialization::encode_i32(buf, len, pos, length))) {
      LOG_WARN("failed to serialize start transfer in mds ctx length", K(ret), K(len), K(pos), K(length));
    } else if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3 == version_
        && OB_FAIL(ls_id_.serialize(buf, len, pos))) {
      LOG_WARN("failed to serialize ls id", K(ret), K(len), K(pos), K_(ls_id));
    } else if (OB_FAIL(MdsCtx::serialize(buf, len, pos))) {
      LOG_WARN("failed to serialize mds ctx", K(ret), K(len), K(pos));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(length != pos - saved_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("start transfer in mds ctx is not match standard length", K(ret), K(saved_pos), K(pos), K(length), K(len));
  }
  return ret;
}

int ObStartTransferInMdsCtx::deserialize(const char *buf, const int64_t len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t saved_pos = pos;
  int64_t version = -1;
  int32_t length = 0;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)
      || OB_UNLIKELY(len <= pos)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (OB_FAIL(serialization::decode(buf, len, pos, version))) {
    LOG_WARN("failed to deserialize start transfer in mds ctx's version", K(ret), K(len), K(pos));
  } else if (FALSE_IT(version_ = static_cast<ObStartTransferInMdsCtxVersion::VERSION>(version))) {
  } else if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1 == version_) {
    pos = saved_pos;
    if (OB_FAIL(MdsCtx::deserialize(buf, len, pos))) {
      LOG_WARN("failed to deserialize mds ctx", K(ret), K(len), K(pos));
    }
  } else if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V2 == version_
      || ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3 == version_) {
    if (OB_FAIL(serialization::decode_i32(buf, len, pos, &length))) {
      LOG_WARN("failed to deserialize start transfer in mds ctx's length", K(ret), K(len), K(pos));
    } else if (OB_UNLIKELY(length > len - saved_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - pos));
    } else if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3 == version_
        && OB_FAIL(ls_id_.deserialize(buf, len, pos))) {
      LOG_WARN("fail to deserialize ls id", K(ret), K(len), K(pos));
    } else if (OB_FAIL(MdsCtx::deserialize(buf, len, pos))) {
      LOG_WARN("failed to deserialize mds ctx", K(ret), K(len), K(pos));
    } else if (OB_UNLIKELY(length != pos - saved_pos)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("start transfer in ctx length doesn't match standard length", K(ret), K(saved_pos), K(pos), K(length), K(len));
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid version", K(ret), K_(version));
  }
  return ret;
}

int64_t ObStartTransferInMdsCtx::get_serialize_size(void) const
{
  int64_t size = 0;
  const int32_t placeholder_length = 0;
  if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V1 == version_) {
    size += MdsCtx::get_serialize_size();
  } else if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V2 == version_
      || ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3 == version_) {
    const int64_t version = static_cast<int64_t>(version_);
    size += serialization::encoded_length(version);
    size += serialization::encoded_length_i32(placeholder_length);
    if (ObStartTransferInMdsCtxVersion::START_TRANSFER_IN_MDS_CTX_VERSION_V3 == version_) {
      size += ls_id_.get_serialize_size();
    }

    size += MdsCtx::get_serialize_size();
  }
  return size;
}

}
}
}
