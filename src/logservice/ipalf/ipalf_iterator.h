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

#ifndef OCEANBASE_LOGSERVICE_IPALF_ITERATOR_
#define OCEANBASE_LOGSERVICE_IPALF_ITERATOR_

#include "share/scn.h"
#include "logservice/palf/log_iterator_impl.h"
#include "logservice/palf/log_iterator_storage.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/palf_iterator.h"
#include "interface_structs.h"
#include "ipalf_log_entry.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "palf_ffi.h"
#include "logservice/libpalf/libpalf_iterator.h"
#endif

namespace oceanbase
{
namespace palf
{
class ILogStorage;
class LSN;
class ILogStorage;
class LogIOContext;
class LogEntryHeader;
class LogEntry;
class IPalfHandleImpl;
}
namespace ipalf
{
class IPalfLogIterator
{
public:
  IPalfLogIterator();
  ~IPalfLogIterator();

  int init(const palf::LSN &lsn, palf::IPalfHandleImpl *palf_handle_impl);

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int init(const palf::LSN &lsn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi);
#endif

  int set_io_context(const palf::LogIOContext &io_ctx);
  int reuse(const palf::LSN &start_lsn);
  void destroy();

  int next();
  int next(const share::SCN &replayable_point_scn);
  int next(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point);

  int get_entry(ILogEntry &entry, palf::LSN &lsn);
  int get_entry(const char *&buffer, ILogEntry &entry, palf::LSN& lsn);
  int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, bool &is_raw_write);
  int get_entry(const char *&buffer, int64_t &nbytes, palf::LSN &lsn, int64_t &log_proposal_id);
  int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, int64_t &log_proposal_id, bool &is_raw_write);
  int get_consumed_info(palf::LSN &end_lsn, share::SCN &end_scn);
  bool is_inited() const;
  bool is_valid() const;
  bool check_is_the_last_entry();
  // @brief cleanup some resource when calling 'destroy'.
  int set_destroy_iterator_storage_functor(const palf::DestroyStorageFunctor &destroy_func);

  palf::PalfBufferIterator &get_palf_iterator_() {
    return palf_iterator_;
  }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  TO_STRING_KV(K(is_inited_),
               K(enable_logservice_),
               K(palf_iterator_),
               K(libpalf_iterator_));
#else
  TO_STRING_KV(K(is_inited_),
               K(palf_iterator_));
#endif
private:
    bool is_inited_;

    palf::PalfBufferIterator palf_iterator_;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    bool enable_logservice_;
    libpalf::LibPalfLogEntryIterator libpalf_iterator_;
#endif
    DISALLOW_COPY_AND_ASSIGN(IPalfLogIterator);
};
} // end namespace ipalf
} // end namespace oceanbase

#endif