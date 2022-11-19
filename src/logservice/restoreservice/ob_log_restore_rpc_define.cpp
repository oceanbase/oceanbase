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

#include "ob_log_restore_rpc_define.h"
#include "lib/allocator/ob_allocator.h"     // ObMemAttr
#include "lib/ob_define.h"
#include "lib/ob_errno.h"
#include "lib/utility/ob_macro_utils.h"
#include "logservice/palf/palf_iterator.h"  // PalfGroupBufferIterator
#include "logservice/palf_handle_guard.h"   // PalfHandleGuard
#include "share/rc/ob_tenant_base.h"        // MTL*
#include "logservice/ob_log_service.h"      // ObLogService

namespace oceanbase
{
namespace obrpc
{

// ========================== fetch log ================================== //
OB_SERIALIZE_MEMBER(ObRemoteFetchLogRequest, tenant_id_, id_, start_lsn_, end_lsn_);

bool ObRemoteFetchLogRequest::is_valid() const
{
  return OB_INVALID_TENANT_ID != tenant_id_
    && id_.is_valid()
    && start_lsn_.is_valid()
    && end_lsn_.is_valid()
    && end_lsn_ > start_lsn_;
}

DEFINE_SERIALIZE(ObRemoteFetchLogResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_ENCODE, err_code_, id_, start_lsn_, end_lsn_, data_len_);
  if (OB_SUCC(ret) && data_len_ > 0) {
    if (buf_len - pos < data_len_) {
      ret = OB_BUF_NOT_ENOUGH;
    } else if (0 == data_len_) {
      // do nothing
    } else{
      MEMCPY(buf + pos, data_, data_len_);
      pos += data_len_;
    }
  }
  return ret;
}

DEFINE_DESERIALIZE(ObRemoteFetchLogResponse)
{
  int ret = OB_SUCCESS;
  LST_DO_CODE(OB_UNIS_DECODE, err_code_, id_, start_lsn_, end_lsn_, data_len_);
  if (data_len_ < 0 || data_len_ > logservice::MAX_FETCH_LOG_BUF_LEN) {
    ret = OB_ERR_UNEXPECTED;
  } else if (data_len_ == 0) {
    // do nothing
  } else {
    MEMCPY(data_, buf + pos, data_len_);
    pos += data_len_;
  }
  return ret;
}

DEFINE_GET_SERIALIZE_SIZE(ObRemoteFetchLogResponse)
{
  int64_t len = 0;
  LST_DO_CODE(OB_UNIS_ADD_LEN, err_code_, id_, start_lsn_, end_lsn_, data_len_);
  len += data_len_;
  return len;
}

bool ObRemoteFetchLogResponse::is_valid() const
{
  return 0 == err_code_
    && id_.is_valid()
    && start_lsn_.is_valid()
    && end_lsn_.is_valid()
    && data_len_ >= 0;
}

bool ObRemoteFetchLogResponse::is_empty() const
{
  return 0 == data_len_;
}

void ObRemoteFetchLogResponse::reset()
{
  err_code_ = 0;
  id_.reset();
  start_lsn_.reset();
  end_lsn_.reset();
  data_len_ = 0;
}

int ObRemoteFetchLogP::process()
{
  int ret = OB_SUCCESS;
  char *data = NULL;
  int64_t data_len = 0;
  ObRemoteFetchLogRequest &req = arg_;
  ObRemoteFetchLogResponse &res = result_;
  if (OB_UNLIKELY(! req.is_valid())) {
    ret = OB_INVALID_ARGUMENT;
    CLOG_LOG(WARN, "invalid argument", K(ret), K(req));
  } else if (NULL == log_service_) {
    ret = get_log_service_(req.tenant_id_, log_service_);
  }

  if (OB_SUCC(ret) && NULL != log_service_) {
    if (OB_FAIL(fetch_log_(req.id_, req.start_lsn_, req.end_lsn_, res.data_, res.data_len_))) {
      CLOG_LOG(WARN, "fetch_log_ failed", K(ret), K(req), K(res));
    } else {
      // TODO res.set();
      res.err_code_ = OB_SUCCESS;
      res.id_ = req.id_;
      res.start_lsn_ = req.start_lsn_;
      res.end_lsn_ = req.start_lsn_ + res.data_len_;
      CLOG_LOG(INFO, "fetch log succ", K(req), K(res));
    }
  }
  return ret;
}

int ObRemoteFetchLogP::get_log_service_(const uint64_t tenant_id, logservice::ObLogService *&log_service)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(tenant_id != MTL_ID())) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "mtl_id not match", K(ret), K(tenant_id), "mtl_id", MTL_ID());
  } else if (OB_ISNULL(log_service = MTL(logservice::ObLogService*))) {
    ret = OB_ERR_UNEXPECTED;
    CLOG_LOG(ERROR, "log_service is NULL", K(ret), K(tenant_id));
  }
  return ret;
}

//TODO fetch log from Archive Service
int ObRemoteFetchLogP::fetch_log_(const share::ObLSID &id,
    const palf::LSN &start_lsn,
    const palf::LSN &end_lsn,
    char *data,
    int64_t &data_len)
{
  int ret = OB_SUCCESS;
  palf::PalfHandleGuard guard;
  palf::PalfGroupBufferIterator iter;
  uint64_t buf_len = end_lsn - start_lsn;
  const uint64_t tenant_id = MTL_ID();
  const int64_t buf_size = logservice::MAX_FETCH_LOG_BUF_LEN;
  data_len = 0;
  if (OB_FAIL(log_service_->open_palf(id, guard))) {
    CLOG_LOG(WARN, "open palf failed", K(ret), K(id));
  } else if (OB_FAIL(guard.seek(start_lsn, iter))) {
    CLOG_LOG(WARN, "seek iter failed", K(ret), K(id), K(start_lsn));
  } else {
    int64_t pos = 0;
    int64_t append_data_len = 0;
    palf::LogGroupEntry entry;
    palf::LSN offset;
    bool is_full = false;
    while (OB_SUCC(ret) && ! is_full) {
      if (OB_FAIL(iter.next())) {
        if (OB_ITER_END != ret) {
          CLOG_LOG(WARN, "iterate log failed", K(ret), K(id), K(tenant_id));
        } else {
          CLOG_LOG(TRACE, "iterate log to end", K(ret), K(id), K(tenant_id));
        }
      } else if (OB_FAIL(iter.get_entry(entry, offset))) {
        CLOG_LOG(WARN, "get entry failed", K(ret), K(id), K(tenant_id));
      } else if (OB_UNLIKELY(! entry.check_integrity())) {
        ret = OB_ERR_UNEXPECTED;
        CLOG_LOG(ERROR, "check integrity failed", K(ret), K(id), K(tenant_id), K(offset), K(entry));
      } else if (true == (is_full = pos + entry.get_serialize_size() > buf_size)) {
        // full just skip
      } else if (OB_FAIL(entry.serialize(data, buf_size, pos))) {
        CLOG_LOG(WARN, "entry serialize failed", K(ret), K(id), K(tenant_id), K(pos), K(entry));
      } else {
        is_full = pos > end_lsn - start_lsn;
        append_data_len = pos;
      }
    }
    if (OB_ITER_END == ret) {
      ret = OB_SUCCESS;
    }
    if (OB_SUCC(ret)) {
      data_len = append_data_len;
    }
  }
  return ret;
}

} // namespace obrpc
} // namespace oceanbase
