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

#include "storage/multi_data_source/ob_tablet_truncate_mds_ctx.h"
#include "storage/ls/ob_ls.h"
#include "storage/tx_storage/ob_ls_service.h"
#include "storage/meta_mem/ob_tenant_meta_mem_mgr.h"
#include "storage/tablet/ob_tablet_create_delete_helper.h"
#include "storage/meta_mem/ob_tablet_mds_truncate_lock.h"
#include "share/ob_debug_sync_point.h"

#define USING_LOG_PREFIX MDS

namespace oceanbase
{
namespace storage
{
namespace mds
{

ObTabletTruncateMdsCtx::ObTabletTruncateMdsCtx()
  : MdsCtx(),
    magic_(MAGIC),
    version_(VERSION),
    ls_id_(),
    tablet_id_(),
    nop_(true)
{
}

ObTabletTruncateMdsCtx::ObTabletTruncateMdsCtx(const MdsWriter &writer)
  : MdsCtx(writer),
    magic_(MAGIC),
    version_(VERSION),
    ls_id_(),
    tablet_id_(),
    nop_(true)
{
}

ObTabletTruncateMdsCtx::~ObTabletTruncateMdsCtx()
{
  if (nop_) {
    // do nothing
  } else {
    MDS_ASSERT(is_valid_());
    MDS_LOG_RET(INFO, OB_SUCCESS, "truncate mds is not commit or abort when ctx is destroyed"
      , KP(this), KPC(this));
    on_abort(share::SCN::max_scn());
  }
}

void ObTabletTruncateMdsCtx::on_commit(
    const share::SCN &commit_version,
    const share::SCN &commit_scn)
{
  int64_t timecost_us = ObTimeUtility::current_time();
  MdsCtx::on_commit(commit_version, commit_scn);
  DEBUG_SYNC(BEFORE_TRUNCATE_CAS_TABLET);

  int ret = OB_SUCCESS;
  if (nop_) {
    LOG_INFO("[TRUNCATE TABLET] nothing to do", K(ret), KP(this), KPC(this));
  } else if (!is_valid_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ctx is invalid", K(ret), KPC(this));
  } else {
    ObLSService *ls_service = MTL(ObLSService*);
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    ObLSTabletService *ls_tablet_svr = nullptr;
    // try until success
    while (true) {
      ret = OB_SUCCESS;
      if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
        LOG_WARN("fail to get ls", K(ret), K_(ls_id));
      } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls is null", K(ret), K_(ls_id), KP(ls));
      } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("ls tablet svr is null", K(ret), K_(ls_id), KP(ls_tablet_svr));
      } else if (OB_FAIL(ls_tablet_svr->commit_tablet_truncate_mds(tablet_id_, commit_scn))) {
        LOG_WARN("failed to commit tablet truncate mds", K(ret),
          K_(ls_id), K_(tablet_id));
      }
      if (OB_SUCC(ret)
         || OB_HASH_NOT_EXIST == ret) {
        break;
      } else {
        ob_usleep(10_ms);
        if (REACH_TIME_INTERVAL(10_s)) {
          LOG_ERROR("failed to commit tablet truncate mds and reach 10s intervals "
            "since last time failure", K(ret), K_(ls_id), K_(tablet_id));
        }
      }
    }
    inner_end_(true/*is_commit*/);
    timecost_us = ObTimeUtility::current_time() - timecost_us;
    LOG_INFO("[TRUNCATE TABLET] on_commit", K(ret), K_(ls_id), K_(tablet_id),
        K(commit_version), K(commit_scn), KP(this), K(timecost_us));
  }
}

void ObTabletTruncateMdsCtx::on_abort(const share::SCN &abort_scn)
{
  int64_t timecost_us = ObTimeUtility::current_time();
  MdsCtx::on_abort(abort_scn);
  int ret = OB_SUCCESS;
  if (nop_) {
    LOG_INFO("[TRUNCATE TABLET] nothing to do", K(ret), KP(this), KPC(this));
  } else if (!is_valid_()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_ERROR("ctx is invalid", K(ret), KPC(this));
  } else {
    inner_end_(false/*is_commit*/);
    timecost_us = ObTimeUtility::current_time() - timecost_us;
    LOG_INFO("[TRUNCATE TABLET] on_abort, discard truncated tablet",
      K(ret), K_(ls_id), K_(tablet_id), K(abort_scn), KP(this), K(timecost_us));
  }
}

OB_INLINE bool ObTabletTruncateMdsCtx::is_valid_() const
{
  return ls_id_.is_valid() && tablet_id_.is_valid();
}

void ObTabletTruncateMdsCtx::inner_end_(const bool is_commit)
{
  int ret = OB_SUCCESS;
  while(true) {
    ret = OB_SUCCESS;
    ObLSHandle ls_handle;
    ObLS *ls = nullptr;
    ObLSTabletService *ls_tablet_svr = nullptr;
    ObLSService *ls_service = MTL(ObLSService *);
    if (OB_ISNULL(ls_service)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ls service", K(ret), KP(ls_service));
    } else if (OB_FAIL(ls_service->get_ls(ls_id_, ls_handle, ObLSGetMod::MDS_TABLE_MOD))) {
      if (OB_LS_NOT_EXIST != ret) {
        LOG_WARN("fail to get ls", K(ret), K_(ls_id));
      } else {
        LOG_INFO("ls is not exist", K(ret), K_(ls_id));
        ret = OB_SUCCESS;
      }
    } else if (OB_ISNULL(ls = ls_handle.get_ls())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("ls is null", K(ret), K_(ls_id), KP(ls));
    } else if (OB_ISNULL(ls_tablet_svr = ls->get_tablet_svr())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null ls_tablet_svr", K(ret), KPC(ls), KP(ls_tablet_svr));
    } else if (OB_FAIL(ls_tablet_svr->end_tablet_truncate_mds(tablet_id_, is_commit))) {
      LOG_WARN("failed to end tablet truncate mds", K(ret), K_(ls_id), K_(tablet_id));
    }

    if (OB_SUCC(ret)) {
      set_nop(true);
      break;
    } else {
      ob_usleep(10_ms);
      if (REACH_TIME_INTERVAL(10_s)) {
        LOG_ERROR("failed to commit tablet truncate mds and reach 10s intervals "
          "since last time failure", K(ret), K_(ls_id), K_(tablet_id));
      }
    }
  }
}


int ObTabletTruncateMdsCtx::assign(const ObTabletTruncateMdsCtx &other)
{
  int ret = OB_SUCCESS;
  const MdsCtx &mds_ctx = static_cast<const MdsCtx &>(other);
  if (OB_FAIL(MdsCtx::assign(mds_ctx))) {
    LOG_WARN("tablet truncate mds ctx assign failed", KR(ret), K(other));
  } else {
    ls_id_ = other.ls_id_;
    tablet_id_ = other.tablet_id_;
    // nop_ is runtime cleanup ownership and must not be transferred to a copied ctx.
  }
  return ret;
}

int ObTabletTruncateMdsCtx::serialize(char *buf, const int64_t buf_len, int64_t &pos) const
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
    } else if (OB_FAIL(tablet_id_.serialize(buf, buf_len, tmp_pos))) {
      LOG_WARN("fail to serialize tablet id", K(ret), K(buf_len), K(tmp_pos), K_(tablet_id));
    } else {
      pos = tmp_pos;
    }
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected version", K(ret), K_(version));
  }

  return ret;
}

int ObTabletTruncateMdsCtx::deserialize(const char *buf, const int64_t buf_len, int64_t &pos)
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
    pos = tmp_pos;
    if (tmp_pos == buf_len) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("buffer is not enough for magic deserialize", K(ret), K(buf_len), K(tmp_pos));
    } else if (OB_FAIL(serialization::decode(buf, buf_len, tmp_pos, magic))) {
      LOG_WARN("decode magic from buffer failed", K(ret), K(buf_len), K(tmp_pos));
    } else if (magic != MAGIC) {
      ret = OB_DESERIALIZE_ERROR;
      LOG_WARN("magic not match", K(ret), K(magic), LITERAL_K(MAGIC), K(buf_len), K(tmp_pos));
    } else if (OB_FAIL(serialization::decode(buf, buf_len, tmp_pos, version))) {
      LOG_WARN("failed to deserialize version", K(ret), K(buf_len), K(tmp_pos));
    } else if (OB_UNLIKELY(VERSION != version)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("version does not match", K(ret), K(version));
    } else if (OB_FAIL(serialization::decode_i64(buf, buf_len, tmp_pos, &serialize_size))) {
      LOG_WARN("failed to deserialize serialize size", K(ret), K(buf_len), K(tmp_pos));
    } else if (tmp_pos - origin_pos < serialize_size && OB_FAIL(ls_id_.deserialize(buf, buf_len, tmp_pos))) {
      LOG_WARN("failed to deserialize ls id", K(ret), K(buf_len), K(tmp_pos));
    } else if (tmp_pos - origin_pos < serialize_size && OB_FAIL(tablet_id_.deserialize(buf, buf_len, tmp_pos))) {
      LOG_WARN("failed to deserialize tablet id", K(ret), K(buf_len), K(tmp_pos));
    } else if (OB_UNLIKELY(tmp_pos - origin_pos != serialize_size)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("deserialize length does not match", K(ret), K(buf_len), K(pos), K(tmp_pos), K(serialize_size));
    } else {
      version_ = version;
      pos = tmp_pos;
    }
  }

  return ret;
}

int64_t ObTabletTruncateMdsCtx::get_serialize_size() const
{
  int64_t size = 0;
  int64_t serialize_size = 0; // dummy
  size += MdsCtx::get_serialize_size();
  size += serialization::encoded_length(magic_);
  size += serialization::encoded_length(version_);
  size += serialization::encoded_length_i64(serialize_size);
  size += ls_id_.get_serialize_size();
  size += tablet_id_.get_serialize_size();
  return size;
}

} // namespace mds
} // namespace storage
} // namespace oceanbase
