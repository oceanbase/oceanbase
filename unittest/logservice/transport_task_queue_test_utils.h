#ifndef UNITTEST_LOGSERVICE_TRANSPORT_TASK_QUEUE_TEST_UTILS_H_
#define UNITTEST_LOGSERVICE_TRANSPORT_TASK_QUEUE_TEST_UTILS_H_

#include <cstdint>
#include <cstring>
#include <mutex>
#include <condition_variable>

#include "lib/utility/ob_macro_utils.h"
#include "share/rc/ob_tenant_base.h"
#include "logservice/palf/log_group_entry_header.h"
#include "logservice/palf/log_group_entry.h"
#include "logservice/palf/log_entry_header.h"
#include "logservice/palf/log_writer_utils.h"
#include "logservice/transportservice/ob_log_transport_rpc_define.h"

namespace oceanbase
{
namespace logservice
{
namespace unittest
{

struct GroupEntryBuffer
{
  GroupEntryBuffer() : buf_(nullptr), size_(0) {}
  ~GroupEntryBuffer() { reset(); }
  void reset()
  {
    if (nullptr != buf_) {
      share::mtl_free(buf_);
      buf_ = nullptr;
      size_ = 0;
    }
  }
  char *release()
  {
    char *tmp = buf_;
    buf_ = nullptr;
    size_ = 0;
    return tmp;
  }

  char *buf_;
  int64_t size_;
  DISALLOW_COPY_AND_ASSIGN(GroupEntryBuffer);
};

inline int build_group_entry_buffer(const int64_t log_id,
                                    const share::SCN &scn,
                                    const int64_t payload_len,
                                    GroupEntryBuffer &out,
                                    const bool corrupt_checksum = false,
                                    const int64_t truncate_size = -1)
{
  int ret = OB_SUCCESS;
  if (payload_len <= 0 || !scn.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    palf::LogGroupEntryHeader group_header;
    palf::LogEntryHeader entry_header;
    const int64_t group_header_size = group_header.get_serialize_size();
    const int64_t entry_header_size = entry_header.get_serialize_size();
    const int64_t total_size = group_header_size + entry_header_size + payload_len;
    ObMemAttr attr(MTL_ID(), "TpQueueTest");
    char *buf = static_cast<char *>(share::mtl_malloc(total_size, attr));
    if (OB_ISNULL(buf)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      const int64_t data_offset = group_header_size + entry_header_size;
      memset(buf + data_offset, 'a', payload_len);

      if (OB_FAIL(entry_header.generate_header(buf + data_offset,
                                               payload_len,
                                               scn))) {
        // do nothing
      } else {
        int64_t pos = 0;
        if (OB_FAIL(entry_header.serialize(buf + group_header_size,
                                           total_size - group_header_size,
                                           pos))) {
          // do nothing
        } else {
          palf::LogWriteBuf write_buf;
          const int64_t data_len = total_size - group_header_size;
          if (OB_FAIL(write_buf.push_back(buf, total_size))) {
            // do nothing
          } else {
            int64_t data_checksum = 0;
            const palf::LSN committed_end_lsn(0);
            const int64_t proposal_id = 1;
            if (OB_FAIL(group_header.generate(false,
                                              false,
                                              write_buf,
                                              data_len,
                                              scn,
                                              log_id,
                                              committed_end_lsn,
                                              proposal_id,
                                              data_checksum))) {
              // do nothing
            } else {
              group_header.update_accumulated_checksum(data_checksum);
              group_header.update_header_checksum();
              pos = 0;
              if (OB_FAIL(group_header.serialize(buf, total_size, pos))) {
                // do nothing
              } else {
                if (corrupt_checksum) {
                  buf[data_offset] ^= 0x1;
                }
                out.reset();
                out.buf_ = buf;
                out.size_ = total_size;
                if (truncate_size > 0 && truncate_size < total_size) {
                  out.size_ = truncate_size;
                }
                buf = nullptr;
              }
            }
          }
        }
      }
    }
    if (nullptr != buf) {
      share::mtl_free(buf);
      buf = nullptr;
    }
  }
  return ret;
}

inline int build_transport_req(const palf::LSN &start_lsn,
                               const palf::LSN &end_lsn,
                               const share::SCN &scn,
                               char *log_data,
                               const int64_t log_size,
                               ObLogTransportReq *&out_req)
{
  int ret = OB_SUCCESS;
  out_req = nullptr;
  if (!start_lsn.is_valid() || !end_lsn.is_valid() || !scn.is_valid() || log_size <= 0 || nullptr == log_data) {
    ret = OB_INVALID_ARGUMENT;
  } else {
    ObMemAttr attr(MTL_ID(), "TpQueueReq");
    ObLogTransportReq *req = static_cast<ObLogTransportReq *>(share::mtl_malloc(sizeof(ObLogTransportReq), attr));
    if (OB_ISNULL(req)) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
    } else {
      req->standby_cluster_id_ = 1;
      req->standby_tenant_id_ = 1;
      req->ls_id_ = share::ObLSID(1);
      req->start_lsn_ = start_lsn;
      req->end_lsn_ = end_lsn;
      req->scn_ = scn;
      req->log_data_ = log_data;
      req->log_size_ = log_size;
      out_req = req;
    }
  }
  return ret;
}

inline void free_transport_req(ObLogTransportReq *req)
{
  if (OB_NOT_NULL(req)) {
    if (OB_NOT_NULL(req->log_data_) && req->log_size_ > 0) {
      share::mtl_free(const_cast<char *>(req->log_data_));
      req->log_data_ = nullptr;
    }
    share::mtl_free(req);
  }
}

class TransportReqGuard
{
public:
  explicit TransportReqGuard(ObLogTransportReq *req) : req_(req), released_(false) {}
  ~TransportReqGuard()
  {
    if (!released_) {
      free_transport_req(req_);
    }
  }
  ObLogTransportReq *get() const { return req_; }
  ObLogTransportReq *release()
  {
    released_ = true;
    return req_;
  }
private:
  ObLogTransportReq *req_;
  bool released_;
  DISALLOW_COPY_AND_ASSIGN(TransportReqGuard);
};

class SimpleBarrier
{
public:
  explicit SimpleBarrier(const int64_t total) : total_(total), arrived_(0) {}
  void wait()
  {
    std::unique_lock<std::mutex> lock(mutex_);
    ++arrived_;
    if (arrived_ >= total_) {
      cv_.notify_all();
    } else {
      cv_.wait(lock, [&]() { return arrived_ >= total_; });
    }
  }
private:
  int64_t total_;
  int64_t arrived_;
  std::mutex mutex_;
  std::condition_variable cv_;
  DISALLOW_COPY_AND_ASSIGN(SimpleBarrier);
};

} // namespace unittest
} // namespace logservice
} // namespace oceanbase

#endif // UNITTEST_LOGSERVICE_TRANSPORT_TASK_QUEUE_TEST_UTILS_H_
