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

#include "ob_mysql_request_utils.h"
#include "lib/allocator/ob_malloc.h"
#include "lib/compress/zlib/ob_zlib_compressor.h"
#include "lib/stat/ob_diagnose_info.h"
#include "rpc/ob_request.h"
#include "rpc/obmysql/ob_mysql_util.h"
#include "rpc/obmysql/ob_mysql_packet.h"
#include "rpc/obmysql/ob_packet_record.h"
#include "rpc/obmysql/obsm_struct.h"

using namespace oceanbase::common;

namespace oceanbase
{
namespace obmysql
{
void __attribute__((weak)) request_finish_callback() {}

ObMySQLRequestUtils::ObMySQLRequestUtils(){}

ObMySQLRequestUtils::~ObMySQLRequestUtils(){}

static int64_t get_max_comp_pkt_size(const int64_t uncomp_pkt_size)
{
  int64_t ret_size = 0;
  if (uncomp_pkt_size > MAX_COMPRESSED_BUF_SIZE) {
    //limit max comp_buf_size is 2M-1k
    ret_size = MAX_COMPRESSED_BUF_SIZE;
  } else {
    ret_size = (common::OB_MYSQL_COMPRESSED_HEADER_SIZE
                + uncomp_pkt_size
                + 13
                + (uncomp_pkt_size >> 12)
                + (uncomp_pkt_size >> 14)
                + (uncomp_pkt_size >> 25));
    if (ret_size > MAX_COMPRESSED_BUF_SIZE) {
      ret_size = MAX_COMPRESSED_BUF_SIZE;
    }
  }
  return ret_size;
}

/*
 * when use compress, packet header looks like:
 *  3B  length of compressed payload
 *  1B  compressed sequence id
 *  3B  length of payload before compression
 *
 *  the body is compressed packet(orig header + orig body)
 *
 * NOTE: In standard mysql compress protocol, if src_pktlen < 50B, or compr_pktlen >= src_pktlen
 *       mysql will do not compress it and set pktlen_before_compression = 0,
 *       it can not ensure checksum.
 * NOTE: In OB, we need always checksum ensured first!
 */
static int build_compressed_packet(ObEasyBuffer &src_buf,
    const int64_t next_compress_size, ObCompressionContext &context)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(context.send_buf_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "send_buf_ is null", K(context), K(ret));
  } else if (OB_ISNULL(context.conn_)) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "conn_ is null", K(context), K(ret));
  } else {
    ObEasyBuffer dst_buf(*context.send_buf_);
    const int64_t comp_buf_size = dst_buf.write_avail_size() - OB_MYSQL_COMPRESSED_HEADER_SIZE;
    ObZlibCompressor compressor;
    bool use_real_compress = true;
    if (context.use_checksum()) {
      compressor.set_compress_level(0);
      use_real_compress = !context.is_checksum_off_;
    }
    int64_t dst_data_size = 0;
    int64_t pos = 0;
    int64_t len_before_compress = 0;
    if (use_real_compress) {
      if (OB_FAIL(compressor.compress(src_buf.read_pos(), next_compress_size,
                                      dst_buf.last() + OB_MYSQL_COMPRESSED_HEADER_SIZE,
                                      comp_buf_size, dst_data_size))) {
        SERVER_LOG(WARN, "compress packet failed", K(ret));
      } else if (OB_UNLIKELY(dst_data_size > comp_buf_size)) {
        ret = OB_SIZE_OVERFLOW;
        SERVER_LOG(WARN, "dst_data_size is overflow, it should not happened",
                   K(dst_data_size), K(comp_buf_size), K(ret));
      } else {
        len_before_compress = next_compress_size;
      }
    } else if (next_compress_size > comp_buf_size) {
      ret = OB_BUF_NOT_ENOUGH;
      SERVER_LOG(WARN, "do not use real compress, dst buffer is not enough", K(ret),
                 K(next_compress_size), K(comp_buf_size), K(lbt()));
    } else {
      //if compress off, just copy date to output buf
      MEMCPY(dst_buf.last() + OB_MYSQL_COMPRESSED_HEADER_SIZE, src_buf.read_pos(), next_compress_size);
      dst_data_size = next_compress_size;
      len_before_compress = 0;
    }

    if (FAILEDx(ObMySQLUtil::store_int3(dst_buf.last(), OB_MYSQL_COMPRESSED_HEADER_SIZE,
                                               static_cast<int32_t>(dst_data_size), pos))) {
      SERVER_LOG(WARN, "failed to store_int3", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int1(dst_buf.last(), OB_MYSQL_COMPRESSED_HEADER_SIZE,
                                               context.seq_, pos))) {
      SERVER_LOG(WARN, "failed to store_int1", K(ret));
    } else if (OB_FAIL(ObMySQLUtil::store_int3(dst_buf.last(), OB_MYSQL_COMPRESSED_HEADER_SIZE,
                                               static_cast<int32_t>(len_before_compress), pos))) {
      SERVER_LOG(WARN, "failed to store_int3", K(ret));
    } else {
      if (context.conn_->pkt_rec_wrapper_.enable_proto_dia()) {
        context.conn_->pkt_rec_wrapper_.end_seal_comp_pkt(
                          static_cast<uint32_t>(dst_data_size), context.seq_);
      }
      SERVER_LOG(DEBUG, "succ to build compressed pkt", "comp_len", dst_data_size,
                 "comp_seq", context.seq_, K(len_before_compress), K(next_compress_size),
                 K(src_buf), K(dst_buf), K(context));
      src_buf.read(next_compress_size);
      dst_buf.write(dst_data_size + OB_MYSQL_COMPRESSED_HEADER_SIZE);
      ++context.seq_;
    }
  }
  return ret;
}

static int build_compressed_buffer(ObEasyBuffer &orig_send_buf,
    ObCompressionContext &context)
{
  int ret = OB_SUCCESS;
  if (NULL != context.send_buf_) {
    ObEasyBuffer comp_send_buf(*context.send_buf_);
    if (OB_UNLIKELY(!orig_send_buf.is_valid()) || OB_UNLIKELY(!comp_send_buf.is_valid())) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "orig_send_buf or comp_send_buf is invalid", K(orig_send_buf), K(comp_send_buf), K(ret));
    } else {
      const int64_t max_read_step = context.get_max_read_step();
      int64_t next_read_size = orig_send_buf.get_next_read_size(context.last_pkt_pos_, max_read_step);
      int64_t last_read_size = 0;
      if (next_read_size > (comp_send_buf.write_avail_size() - OB_MYSQL_COMPRESSED_HEADER_SIZE)) {
        next_read_size = max_read_step;
      }
      int64_t max_comp_pkt_size = get_max_comp_pkt_size(next_read_size);
      while (OB_SUCC(ret)
             && next_read_size > 0
             && max_comp_pkt_size <= comp_send_buf.write_avail_size()) {
        //error+ok/ok packet should use last seq
        if (context.last_pkt_pos_ == orig_send_buf.read_pos() && context.is_proxy_compress_based()) {
          --context.seq_;
        }

        if (OB_FAIL(build_compressed_packet(orig_send_buf, next_read_size, context))) {
          SERVER_LOG(WARN, "fail to build_compressed_packet", K(ret));
        } else {
          //optimize for multi packet
          last_read_size = next_read_size;
          next_read_size = orig_send_buf.get_next_read_size(context.last_pkt_pos_, max_read_step);
          if (next_read_size > (comp_send_buf.write_avail_size() - OB_MYSQL_COMPRESSED_HEADER_SIZE)) {
            next_read_size = max_read_step;
          }
          if (last_read_size != next_read_size) {
            max_comp_pkt_size = get_max_comp_pkt_size(next_read_size);
          }
        }
      }
    }
  }
  return ret;
}

static int reuse_compress_buffer(ObCompressionContext &comp_context, ObEasyBuffer &orig_send_buf,
                                                  int64_t comp_buf_size, rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  bool need_alloc = false;
  if (NULL == comp_context.send_buf_) {
    need_alloc = true;
    //use buf_size to avoid alloc again next time
    comp_buf_size = get_max_comp_pkt_size(orig_send_buf.orig_buf_size());
  } else {
    const int64_t new_size = get_max_comp_pkt_size(orig_send_buf.read_avail_size());
    if (new_size <= comp_buf_size) {
      //reusing last size is enough
    } else {
      SERVER_LOG(DEBUG, "need resize compressed buf", "old_size", comp_buf_size, K(new_size),
                 "orig_send_buf_", orig_send_buf);
      //realloc
      comp_buf_size = new_size;
      need_alloc = true;
    }
  }

  //alloc if necessary
  if (need_alloc) {
    const uint32_t size = static_cast<uint32_t>(comp_buf_size + sizeof(easy_buf_t));
    if (OB_ISNULL(comp_context.send_buf_ =
        reinterpret_cast<easy_buf_t *>(SQL_REQ_OP.alloc_sql_response_buffer(&req, size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "allocate memory failed", K(size), K(ret));
    }
  }

  //reuse
  if (OB_SUCC(ret)) {
    init_easy_buf(comp_context.send_buf_,
                  reinterpret_cast<char *>(comp_context.send_buf_ + 1),
                  NULL, comp_buf_size);
  }

  return ret;
}

static int send_compressed_buffer(bool pkt_has_completed, ObCompressionContext &comp_context, 
                                                            ObEasyBuffer &orig_send_buf, rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  const bool is_last_flush = pkt_has_completed && !orig_send_buf.is_read_avail();
  int64_t buf_size = 0;
  int64_t read_avail_size = 0;

  if (NULL != comp_context.send_buf_) {
    ObEasyBuffer comp_send_buf(*comp_context.send_buf_);
    buf_size = comp_send_buf.orig_buf_size();
    read_avail_size = comp_send_buf.read_avail_size();
  }

  if (read_avail_size > 0) {
    if (is_last_flush) {
        if (OB_FAIL(SQL_REQ_OP.async_write_response(&req, comp_context.send_buf_->pos, read_avail_size))) {
          SERVER_LOG(WARN, "failed to flush buffer", K(ret));
        }
    } else {
      if (OB_FAIL(SQL_REQ_OP.write_response(&req, comp_context.send_buf_->pos, read_avail_size))) {
        SERVER_LOG(WARN, "failed to flush buffer", K(ret));
      }
    }
  }

  if (OB_SUCC(ret) && !is_last_flush) {
    if (OB_FAIL(reuse_compress_buffer(comp_context, orig_send_buf, buf_size, req))) {
      SERVER_LOG(WARN, "faild to reuse_compressed_buffer_sql_nio", K(ret));
    }
  }

  return ret;
}

int ObMySQLRequestUtils::flush_buffer_internal(easy_buf_t *send_buf,
    ObFlushBufferParam &param, const bool is_last_flush)
{
  int ret = OB_SUCCESS;
  if (OB_UNLIKELY(!param.conn_valid_)) {
    // force core dump in debug
    ret = OB_CONNECT_ERROR;
    SERVER_LOG(ERROR, "connection in error, maybe has disconnected", K(ret));
  } else if (param.req_has_wokenup_) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "req_has_wokenup, resource maybe has destroy", K(ret));
  } else if (OB_ISNULL(param.ez_req_.ms) ||  OB_ISNULL(param.ez_req_.ms->c)
      || OB_UNLIKELY(param.ez_req_.ms->c->conn_has_error)) {
    ret = OB_CONNECT_ERROR;
    SERVER_LOG(WARN, "connection has error", KP(param.ez_req_.ms), K(ret));
    wakeup_easy_request(param.ez_req_, param.req_has_wokenup_);
    param.conn_valid_ = false;
  } else {
    //flush it only if data contained
    if (NULL != send_buf && send_buf->last > send_buf->pos) {
      param.ez_req_.opacket = send_buf;
    } else {
      param.ez_req_.opacket = NULL;
    }

    if (is_last_flush) {
      param.ez_req_.retcode = EASY_OK;
      wakeup_easy_request(param.ez_req_, param.req_has_wokenup_);

    } else {
      easy_client_wait_t wait_obj;
      easy_client_wait_init(&wait_obj);

      // The order is important
      param.ez_req_.client_wait = &wait_obj;
      param.ez_req_.retcode = EASY_AGAIN;
      param.ez_req_.waiting = 1;
      wakeup_easy_request(param.ez_req_, param.req_has_wokenup_);

      // wait for wake up by IO thread in `process' callback
      {
        ObWaitEventGuard guard(ObWaitEventIds::MYSQL_RESPONSE_WAIT_CLIENT, 0, 0, 0);
        easy_client_wait(&wait_obj, 1);
      }

      // return OB_CONNECT_ERROR if status equals EASY_CONN_CLOSE
      if (EASY_CONN_CLOSE == wait_obj.status) {
        ret = OB_CONNECT_ERROR;
        param.conn_valid_ = false;
        SERVER_LOG(WARN, "send error happen, quit current query", K(ret));
      }
      easy_client_wait_cleanup(&wait_obj);
    }
  }
  return ret;
}

int ObMySQLRequestUtils::check_flush_param(ObFlushBufferParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(param.ez_req_.ms) || OB_ISNULL(param.ez_req_.ms->pool)
      || OB_ISNULL(param.ez_req_.ms->c) || OB_UNLIKELY(param.ez_req_.ms->c->conn_has_error)) {
    ret = OB_CONNECT_ERROR;
    SERVER_LOG(WARN, "connection has error", KP(param.ez_req_.ms), K(ret));
    wakeup_easy_request(param.ez_req_, param.req_has_wokenup_);
    param.conn_valid_ = false;
  } else if (OB_UNLIKELY(!param.orig_send_buf_.is_valid())) {
    ret = OB_ERR_UNEXPECTED;
    SERVER_LOG(ERROR, "orig_send_buf_ is invalid", K(param.orig_send_buf_), K(ret));
  } else if (!param.comp_context_.use_compress()) {
    if (OB_NOT_NULL(param.comp_context_.last_pkt_pos_)
        || OB_NOT_NULL(param.comp_context_.send_buf_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "last_pkt_pos_ or send_buf is not null", K(param.comp_context_), K(ret));
    }
  } else if (param.comp_context_.is_default_compress()) {
    if (OB_NOT_NULL(param.comp_context_.last_pkt_pos_)) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(ERROR, "last_pkt_pos_ is not null", K(param.comp_context_), K(ret));
    }
  } else if (param.comp_context_.is_proxy_compress_based()) {
    if (NULL == param.comp_context_.last_pkt_pos_ && param.pkt_has_completed_ && param.orig_send_buf_.is_read_avail()) {
      ret = OB_ERR_UNEXPECTED;
      SERVER_LOG(WARN, "this is last flush, but without proxy last pkt pos",
                 K(param.comp_context_), K(param.orig_send_buf_), K(ret));
    } else if (NULL != param.comp_context_.last_pkt_pos_) {
      /*
       *    pos          ppos       last     end
       *    |-------------|----------|--------|
       *
       * 1) ppos=null, send_buf's data is normal packet, we can treat it as idx=size
       * 2) ppos>last, oversize
       * 3) ppos=pos, send_buf's data is proxy last packet
       * 4) ppos=last, send_buf's data is normal packet
       * 5) pos<ppos<last, send_buf's data is normal[pos, ppos) + proxy last packet[idx, last)
       * */
      if (OB_UNLIKELY(param.comp_context_.last_pkt_pos_ > param.orig_send_buf_.last())
          || OB_UNLIKELY(param.comp_context_.last_pkt_pos_ < param.orig_send_buf_.read_pos())) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "last_pkt_pos_ is out of range", KP_(param.comp_context_.last_pkt_pos),
                   KP(param.orig_send_buf_.read_pos()), KP(param.orig_send_buf_.last()), K(ret));
      } else if (param.orig_send_buf_.proxy_read_avail_size(param.comp_context_.last_pkt_pos_) >
                 MAX_COMPRESSED_BUF_SIZE) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "proxy last pkt size is too large", K(ret),
                   K(MAX_COMPRESSED_BUF_SIZE),
                   "size", param.orig_send_buf_.proxy_read_avail_size(param.comp_context_.last_pkt_pos_));
      }
    }
  } else {
    //do nothing
  }
  return ret;
}

int ObMySQLRequestUtils::consume_compressed_buffer(ObFlushBufferParam &param,
    const bool flush_immediately/*false*/)
{
  int ret = OB_SUCCESS;
  const bool is_last_flush = param.pkt_has_completed_ && !param.orig_send_buf_.is_read_avail();
  int64_t buf_size = 0;
  int64_t read_avail_size = 0;
  if (NULL != param.comp_context_.send_buf_) {
    ObEasyBuffer comp_send_buf(*param.comp_context_.send_buf_);
    buf_size = comp_send_buf.orig_buf_size();
    read_avail_size = comp_send_buf.read_avail_size();
  }

  //1. flush it
  if (read_avail_size > 0 || flush_immediately) {
    if (OB_FAIL(flush_buffer_internal(param.comp_context_.send_buf_, param, is_last_flush))) {
      SERVER_LOG(WARN, "failed to flush_buffer", K(ret));
    } else {
      SERVER_LOG(DEBUG, "finish flush_buffer_internal", K(read_avail_size), K(flush_immediately), K(buf_size));
    }
  }

  //2. reuse it
  if (OB_SUCC(ret) && !is_last_flush) {
    if (OB_FAIL(reuse_compressed_buffer(param, buf_size, is_last_flush))) {
      SERVER_LOG(WARN, "fail to reuse_compressed_buffer", K(ret));
    }
  } else {
    SERVER_LOG(DEBUG, "finish flush buffer", K(is_last_flush), "orig_buf_size", buf_size, K(ret));
  }
  return ret;
}

int ObMySQLRequestUtils::reuse_compressed_buffer(ObFlushBufferParam &param,
    int64_t comp_buf_size, const bool is_last_flush)
{
  int ret = OB_SUCCESS;
  bool need_alloc = false;
  if (NULL == param.comp_context_.send_buf_) {
    need_alloc = true;
    if (is_last_flush) {
      //use data size is enough
      comp_buf_size = get_max_comp_pkt_size(param.orig_send_buf_.orig_data_size());
    } else {
      //use buf_size to avoid alloc again next time
      comp_buf_size = get_max_comp_pkt_size(param.orig_send_buf_.orig_buf_size());
    }
  } else {
    const int64_t new_size = get_max_comp_pkt_size(param.orig_send_buf_.read_avail_size());
    if (new_size <= comp_buf_size) {
      //reusing last size is enough
    } else {
      SERVER_LOG(DEBUG, "need resize compressed buf", "old_size", comp_buf_size, K(new_size),
                 "orig_send_buf_", param.orig_send_buf_);
      //realloc
      comp_buf_size = new_size;
      need_alloc = true;
    }
  }

  //alloc if necessary
  if (need_alloc) {
    const uint32_t size = static_cast<uint32_t>(comp_buf_size + sizeof(easy_buf_t));
    if (OB_ISNULL(param.comp_context_.send_buf_ =
        reinterpret_cast<easy_buf_t *>(easy_pool_alloc(param.ez_req_.ms->pool, size)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      SERVER_LOG(ERROR, "allocate memory failed", K(size), K(ret));
    }
  }

  //reuse
  if (OB_SUCC(ret)) {
    init_easy_buf(param.comp_context_.send_buf_,
                  reinterpret_cast<char *>(param.comp_context_.send_buf_ + 1),
                  &param.ez_req_, comp_buf_size);
  }
  return ret;
}

int ObMySQLRequestUtils::flush_buffer(ObFlushBufferParam &param)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(check_flush_param(param))) {
    SERVER_LOG(WARN, "failed to check_flush_param", K(ret));
  } else if (!param.comp_context_.use_compress()) {
    //use no compress
    // easy_wakeup will move buf.pos to buf.last, so orig_buf_size() after flush is not the original buf length
    const int64_t buf_size = param.orig_send_buf_.orig_buf_size();
    if (OB_FAIL(flush_buffer_internal(&param.orig_send_buf_.buf_, param, param.pkt_has_completed_))) {
      SERVER_LOG(WARN, "failed to flush_buffer", K(ret));
    } else if (!param.pkt_has_completed_) {
      init_easy_buf(&param.orig_send_buf_.buf_, reinterpret_cast<char *>(&param.orig_send_buf_.buf_ + 1),
                    &param.ez_req_, buf_size);
    }
  }

  //use compress
  if (OB_SUCC(ret) && param.comp_context_.use_compress()) {
    int64_t need_hold_size = 0;

    //1. save orig_send_buf hold_pos and size
    if (param.comp_context_.need_hold_last_pkt(param.pkt_has_completed_)) {
      need_hold_size = param.orig_send_buf_.proxy_read_avail_size(param.comp_context_.last_pkt_pos_);
      //NOTE::data still leave at orig_send_buf, we can still get it before reuse orig_send_buf
      param.orig_send_buf_.write(0 - need_hold_size);
      SERVER_LOG(DEBUG, "need hold uncompleted proxy pkt", K(need_hold_size),
                 "orig_send_buf", param.orig_send_buf_);
    }

    //2. compress --> flush --> reuse
    if (!param.orig_send_buf_.is_read_avail()) {
      //empty orig_send_buf, just flush immediately
      const bool immediately = true;
      if (OB_FAIL(consume_compressed_buffer(param, immediately))) {
        SERVER_LOG(WARN, "fail to consume_compressed_buffer", K(ret));
      } else {
        //do nothing
      }
    } else {
      //flush until orig_send_buf become empty
      while (OB_SUCC(ret)
             && !param.req_has_wokenup_
             && param.orig_send_buf_.is_read_avail()) {
        if (OB_FAIL(build_compressed_buffer(param.orig_send_buf_, param.comp_context_))) {
          SERVER_LOG(WARN, "fail to build_compressed_buffer", K(ret));
        } else if (OB_FAIL(consume_compressed_buffer(param))) {
          SERVER_LOG(WARN, "fail to consume_compressed_buffer", K(ret));
        } else {
          //do nothing
        }
      }
    }

    //3. reuse orig_send_buf and restore hold data
    if (OB_SUCC(ret) && !param.pkt_has_completed_) {
      const bool need_reset_last_pkt_pos = (param.comp_context_.last_pkt_pos_ == param.orig_send_buf_.last());
      //reuse send_buf
      init_easy_buf(&param.orig_send_buf_.buf_, reinterpret_cast<char *>(&param.orig_send_buf_.buf_ + 1),
                    &param.ez_req_, param.orig_send_buf_.orig_buf_size());

      //NOTE::data still leave at orig_send_buf, we can still get it before reuse orig_send_buf
      if (need_reset_last_pkt_pos) {
        if (need_hold_size > 0) {
          MEMMOVE(param.orig_send_buf_.last(), param.comp_context_.last_pkt_pos_, need_hold_size);
          param.orig_send_buf_.write(need_hold_size);
        }

        //if last_pkt_pos_, we need reset to pos after reuse send_buf
        param.comp_context_.last_pkt_pos_ = param.orig_send_buf_.begin();
        SERVER_LOG(DEBUG, "need reset last_pkt_pos", K(need_hold_size),
                   "orig_send_buf_", param.orig_send_buf_,
                   "comp_context", param.comp_context_);
      }
    }
  }

  //if error had happened, libeasy must wakeup
  if (OB_FAIL(ret) && param.conn_valid_) {
    // Here is other potential bug that connection may be shut down twice and easy request may be
    // waken up twice if response is sent synchronously. We will not change it this time. It needs
    // to be redesigned in higher level.
    SERVER_LOG(WARN, "failed to flush buffer, and need close connection and wake up easy request.", K(ret));
    disconnect(param.ez_req_);
    wakeup_easy_request(param.ez_req_, param.req_has_wokenup_);
    param.conn_valid_ = false;
  }
  return ret;
}

void ObMySQLRequestUtils::wakeup_easy_request(easy_request_t &ez_req, bool &req_has_wokenup)
{
  if (!req_has_wokenup) {
    if (ez_req.retcode != EASY_AGAIN) {
      req_has_wokenup = true;
      request_finish_callback();
    }
    easy_request_wakeup(&ez_req);
  }
}

void ObMySQLRequestUtils::disconnect(easy_request_t &ez_req)
{
  if (OB_ISNULL(ez_req.ms)) {
    SERVER_LOG_RET(WARN, common::OB_INVALID_ARGUMENT, "null req input", KP(ez_req.ms));
  } else {
    easy_connection_destroy_dispatch(ez_req.ms->c);
  }
}

int ObMysqlPktContext::save_fragment_mysql_packet(const char *start, const int64_t len)
{
  int ret = OB_SUCCESS;
  if (OB_ISNULL(start) || len <= 0) {
    ret = OB_INVALID_ARGUMENT;
    SERVER_LOG(WARN, "invalid input value", KP(start), K(len), K(ret));
  } else {
    if (payload_buf_alloc_len_ >= (len + payload_buffered_total_len_)) {
      // buffer is enough
    } else {
      int64_t alloc_size = std::max(len, payload_len_);
      if (alloc_size > OB_MYSQL_MAX_PAYLOAD_LENGTH) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "invalid alloc size", K(alloc_size), K(ret));
      } else {
        alloc_size += payload_buffered_total_len_;
        char *tmp_buffer = reinterpret_cast<char *>(arena_.alloc(alloc_size));
        if (OB_ISNULL(tmp_buffer)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          SERVER_LOG(ERROR, "fail to alloc memory", K(alloc_size), K(ret));
        } else {
          if (payload_buffered_total_len_ > 0) { // skip the first alloc
            MEMCPY(tmp_buffer, payload_buf_, payload_buffered_total_len_);
          }
          payload_buf_alloc_len_ = alloc_size;
          payload_buf_ = tmp_buffer;
        }
      }
    }

    if (OB_SUCC(ret)) {
      if (OB_UNLIKELY(payload_buf_alloc_len_ < (payload_buffered_total_len_ + len))) {
        ret = OB_ERR_UNEXPECTED;
        SERVER_LOG(ERROR, "invalid buf len", K_(payload_buf_alloc_len),
                  K_(payload_buffered_total_len), K(len), K(ret));
      } else {
        MEMCPY(payload_buf_ + payload_buffered_total_len_, start, len);
        payload_buffered_total_len_ += len;
        payload_buffered_len_ += len;
      }
    }
  }

  return ret;
}

int ObMySQLRequestUtils::flush_compressed_buffer(bool pkt_has_completed, ObCompressionContext &comp_context, 
                                                              ObEasyBuffer &orig_send_buf, rpc::ObRequest &req)
{
  int ret = OB_SUCCESS;
  int64_t need_hold_size = 0;

  if (comp_context.need_hold_last_pkt(pkt_has_completed)) {
    need_hold_size = orig_send_buf.proxy_read_avail_size(comp_context.last_pkt_pos_);
    orig_send_buf.write(0 - need_hold_size);
    SERVER_LOG(DEBUG, "need hold uncompleted proxy pkt", K(need_hold_size),
            "orig_send_buf", orig_send_buf);
  }

  if (false == orig_send_buf.is_read_avail()) {

  } else {
    while (OB_SUCC(ret)
            && orig_send_buf.is_read_avail()) {
      if (OB_FAIL(build_compressed_buffer(orig_send_buf, comp_context))) {
        SERVER_LOG(WARN, "failed to build_compressed_buffer", K(ret));
        break;
      } else if (OB_FAIL(send_compressed_buffer(pkt_has_completed, comp_context, orig_send_buf, req))) {
        SERVER_LOG(WARN, "faild to send compressed response", K(ret));
        break;
      }
    }
  }

  if (OB_SUCC(ret) && false == pkt_has_completed) {
    const bool need_reset_last_pkt_pos = (comp_context.last_pkt_pos_ == orig_send_buf.last());
    init_easy_buf(&orig_send_buf.buf_, reinterpret_cast<char *>(&orig_send_buf.buf_ + 1),
                  NULL, orig_send_buf.orig_buf_size());

    if (need_reset_last_pkt_pos) {
      if (need_hold_size > 0) {
          MEMMOVE(orig_send_buf.last(), comp_context.last_pkt_pos_, need_hold_size);
          orig_send_buf.write(need_hold_size);
      }
      comp_context.last_pkt_pos_ = orig_send_buf.begin();
      SERVER_LOG(DEBUG, "need reset last_pkt_pos", K(need_hold_size),
            "orig_send_buf_", orig_send_buf,
            "comp_context", comp_context);
    }
  }
  
  return ret;
}

} //end of namespace obmysql
} //end of namespace oceanbase
