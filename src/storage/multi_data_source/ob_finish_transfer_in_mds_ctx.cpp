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
#include "ob_finish_transfer_in_mds_ctx.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "mds_table_handle.h"
#include "share/ob_errno.h"
#include "storage/tx_storage/ob_ls_handle.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{
namespace mds
{

ObFinishTransferInMdsCtx::ObFinishTransferInMdsCtx()
    : MdsCtx(),
      version_(ObFinishTransferInMdsCtxVersion::CURRENT_CTX_VERSION),
      redo_scn_(share::SCN::base_scn())
{
}

ObFinishTransferInMdsCtx::ObFinishTransferInMdsCtx(const MdsWriter &writer)
    : MdsCtx(writer),
      version_(ObFinishTransferInMdsCtxVersion::CURRENT_CTX_VERSION),
      redo_scn_(share::SCN::base_scn())

{
}

ObFinishTransferInMdsCtx::~ObFinishTransferInMdsCtx()
{
  version_ = ObFinishTransferInMdsCtxVersion::MAX;
  redo_scn_.reset();
}

int ObFinishTransferInMdsCtx::serialize(char *buf, const int64_t len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  int64_t saved_pos = pos;
  const int64_t length = get_serialize_size();

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(len), K(pos));
  } else if (!ObFinishTransferInMdsCtxVersion::is_valid(version_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("invalid version", K(ret), K_(version));
  } else if (OB_UNLIKELY(length > len - pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - pos));
  } else if (ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1 == version_) {
    if (OB_FAIL(MdsCtx::serialize(buf, len, pos))) {
      LOG_WARN("failed to serialize mds ctx serialize", K(ret), K(len), K(pos));
    }
  } else {
    if (OB_FAIL(serialization::encode(buf, len, pos, static_cast<int64_t>(version_)))) {
      LOG_WARN("failed to serialize tablet meta's version", K(ret), K(len), K(pos), K_(version));
    } else if (OB_FAIL(serialization::encode_i32(buf, len, pos, length))) {
      LOG_WARN("failed to serialize tablet meta's length", K(ret), K(len), K(pos), K(length));
    } else if (OB_FAIL(MdsCtx::serialize(buf, len, pos))) {
      LOG_WARN("failed to serialize mds ctx serialize", K(ret), K(len), K(pos));
    } else if (OB_FAIL(redo_scn_.serialize(buf, len, pos))) {
      LOG_WARN("failed to serialize redo scn", K(ret), K(len), K(pos));
    }
  }

  if (OB_FAIL(ret)) {
  } else if (OB_UNLIKELY(length != pos - saved_pos)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("finish transfer in mds ctx is not match standard length", K(ret), K(saved_pos), K(pos), K(length), K(len));
  }

  return ret;
}

int ObFinishTransferInMdsCtx::deserialize(const char *buf, const int64_t len, int64_t &pos)
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
  } else if (FALSE_IT(version_ = static_cast<ObFinishTransferInMdsCtxVersion::VERSION>(version))) {
  } else if (ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1 == version_) {
    pos = saved_pos;
    if (OB_FAIL(MdsCtx::deserialize(buf, len, pos))) {
      LOG_WARN("failed to deserialize mds ctx", K(ret), K(len), K(pos));
    }
  } else {
    if (OB_FAIL(serialization::decode_i32(buf, len, pos, &length))) {
      LOG_WARN("failed to deserialize start transfer in mds ctx's length", K(ret), K(len), K(pos));
    } else if (ObFinishTransferInMdsCtxVersion::CURRENT_CTX_VERSION != version_) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("invalid version", K(ret), K_(version));
    } else {
      if (OB_UNLIKELY(length > len - saved_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("buffer's length is not enough", K(ret), K(length), K(len - saved_pos));
      } else if (OB_FAIL(MdsCtx::deserialize(buf, len, pos))) {
        LOG_WARN("failed to deserialize mds ctx", K(ret), K(len), K(pos));
      } else if (OB_FAIL(redo_scn_.deserialize(buf, len, pos))) {
        LOG_WARN("failed to deserialize redo scn", K(ret), K(len), K(pos));
      } else if (OB_UNLIKELY(length != pos - saved_pos)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("tablet's length doesn't match standard length", K(ret), K(saved_pos), K(pos), K(length), K(len));
      }
    }
  }
  return ret;
}

int64_t ObFinishTransferInMdsCtx::get_serialize_size(void) const
{
  int64_t size = 0;
  const int32_t placeholder_length = 0;
  if (ObFinishTransferInMdsCtxVersion::FINISH_TRANSFER_IN_MDS_CTX_VERSION_V1 == version_) {
    size += MdsCtx::get_serialize_size();
  } else {
    const int64_t version = static_cast<int64_t>(version_);
    size += serialization::encoded_length(version);
    size += serialization::encoded_length_i32(placeholder_length);
    size += MdsCtx::get_serialize_size();
    size += redo_scn_.get_serialize_size();
  }
  return size;
}

void ObFinishTransferInMdsCtx::on_redo(const share::SCN &redo_scn)
{
  redo_scn_ = redo_scn;
  MdsCtx::on_redo(redo_scn);
  LOG_INFO("[TRANSFER] finish transfer in mds ctx on_redo", K(redo_scn));
}

}
}
}
