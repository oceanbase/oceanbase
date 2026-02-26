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

#include "storage/multi_data_source/ob_tablet_create_mds_ctx.h"
#include "src/storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{
namespace mds
{
ObTabletCreateMdsCtx::ObTabletCreateMdsCtx()
  : MdsCtx(),
    magic_(MAGIC),
    version_(VERSION),
    ls_id_()
{
}

ObTabletCreateMdsCtx::ObTabletCreateMdsCtx(const MdsWriter &writer)
  : MdsCtx(writer),
    magic_(MAGIC),
    version_(VERSION),
    ls_id_()
{
}

void ObTabletCreateMdsCtx::on_abort(const share::SCN &abort_scn)
{
  mds::MdsCtx::on_abort(abort_scn);

  int ret = OB_SUCCESS;
  ObLSService *ls_service = MTL(ObLSService*);
  ObLSHandle ls_handle;
  ObLS *ls = nullptr;

  if (OB_UNLIKELY(!ls_id_.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("ls id is invalid", K(ret), K_(ls_id));
  } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
    LOG_WARN("fail to get ls", K(ret), K_(ls_id));
  } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("ls is null", K(ret), K_(ls_id), KP(ls));
  } else {
    checkpoint::ObTabletEmptyShellHandler *handler = ls->get_tablet_empty_shell_handler();
    handler->set_empty_shell_trigger(true/*is_trigger*/);

    LOG_INFO("tablet create tx aborted", K(ret), K_(ls_id), K(abort_scn));
  }
}

int ObTabletCreateMdsCtx::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
{
  int ret = OB_SUCCESS;
  const int64_t serialize_size = get_serialize_size();
  int64_t tmp_pos = pos;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_UNLIKELY(buf_len - pos < serialize_size)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("buffer len is not enough to serialize", K(ret), K(buf_len), K(pos), K(serialize_size));
  } else if (VERSION == version_) {
    if (OB_FAIL(MdsCtx::serialize(buf, buf_len, tmp_pos))) {
      LOG_WARN("failed to serialize mds ctx", K(ret), K(buf_len), K(tmp_pos));
    } else if (OB_FAIL(serialization::encode(buf, buf_len, tmp_pos, magic_))) {
      LOG_WARN("fail to serialize magic", K(ret), K(buf_len), K(tmp_pos), K_(magic));
    } else if (OB_FAIL(serialization::encode(buf, buf_len, tmp_pos, version_))) {
      LOG_WARN("fail to serialize version", K(ret), K(buf_len), K(tmp_pos), K_(version));
    } else if (OB_FAIL(serialization::encode_i64(buf, buf_len, tmp_pos, serialize_size))) {
      LOG_WARN("fail to serialize length", K(ret), K(buf_len), K(tmp_pos), K(serialize_size));
    } else if (OB_FAIL(ls_id_.serialize(buf, buf_len, tmp_pos))) {
      LOG_WARN("fail to serialize ls id", K(ret), K(buf_len), K(tmp_pos), K_(ls_id));
    } else {
      pos = tmp_pos;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected version", K(ret), K_(version));
  }

  return ret;
}

int ObTabletCreateMdsCtx::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
{
  int ret = OB_SUCCESS;
  int64_t origin_pos = pos;
  int64_t tmp_pos = pos;
  int32_t magic = -1;
  int32_t version = -1;
  int64_t serialize_size = 0;

  if (OB_ISNULL(buf)
      || OB_UNLIKELY(buf_len <= 0)
      || OB_UNLIKELY(pos < 0)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid args", K(ret), K(buf), K(buf_len), K(pos));
  } else if (OB_FAIL(MdsCtx::deserialize(buf, buf_len, tmp_pos))) {
    LOG_WARN("fail to deserialize mds ctx", K(ret), K(buf_len), K(tmp_pos));
  } else {
    int tmp_ret = OB_SUCCESS;
    pos = tmp_pos;
    bool is_old_data = false;
    if (tmp_pos == buf_len) {
      LOG_WARN("buffer is not enough for magic deserialize", K(ret), K(buf_len), K(tmp_pos));
      is_old_data = true;
    } else if (OB_TMP_FAIL(serialization::decode(buf, buf_len, tmp_pos, magic))) {
      LOG_WARN("decode magic from buffer failed", K(tmp_ret), K(ret), K(buf_len), K(tmp_pos));
      is_old_data = true;
    } else if (magic != MAGIC) {
      LOG_WARN("magic not match", K(tmp_ret), K(ret), K(buf_len), K(tmp_pos));
      is_old_data = true;
    } else if (OB_FAIL(serialization::decode(buf, buf_len, tmp_pos, version))) {
      LOG_WARN("failed to deserialize version", K(ret), K(buf_len), K(tmp_pos));
    } else if (OB_UNLIKELY(VERSION != version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("version does not match", K(ret), K(version));
    } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, tmp_pos, &serialize_size))) {
      LOG_WARN("failed to deserialize serialize size", K(ret), K(buf_len), K(tmp_pos));
    } else if (tmp_pos - origin_pos < serialize_size && OB_FAIL(ls_id_.deserialize(buf, buf_len, tmp_pos))) {
      LOG_WARN("failed to deserialize ls id", K(ret), K(buf_len), K(tmp_pos));
    } else if (OB_UNLIKELY(tmp_pos - origin_pos != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialize length does not match", K(ret), K(buf_len), K(pos), K(tmp_pos), K(serialize_size));
    } else {
      version_ = version;
      pos = tmp_pos;
    }

    if (is_old_data) {
      FLOG_INFO("maybe meet old version data", K(ret), K(magic), LITERAL_K(MAGIC));
      version_ = VERSION;
      ls_id_ = ObLSID::INVALID_LS_ID;
    }
  }

  return ret;
}

int64_t ObTabletCreateMdsCtx::get_serialize_size() const
{
  int64_t size = 0;
  int64_t serialize_size = 0; // dummy
  size += MdsCtx::get_serialize_size();
  size += serialization::encoded_length(magic_);
  size += serialization::encoded_length(version_);
  size += serialization::encoded_length_i64(serialize_size);
  size += ls_id_.get_serialize_size();
  return size;
}
} // namespace mds
} // namespace storage
} // namespace oceanbase
