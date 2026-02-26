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
#include "ipalf_log_group_entry.h"
#include "share/config/ob_server_config.h"
#include "logservice/archiveservice/ob_archive_define.h"
#include "logservice/palf/palf_handle_impl.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/ob_log_compression.h"
#include "logservice/libpalf/libpalf_common_define.h"
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
class LogGroupEntry;
class IPalfHandleImpl;
}
namespace ipalf
{
template<typename EntryType>
struct EntryTypeMapper;
class ILogEntry;
class IGroupEntry;
template<>
struct EntryTypeMapper<ILogEntry> {
  using EntryType = palf::LogEntry;
};

template<>
struct EntryTypeMapper<IGroupEntry> {
  using EntryType = palf::LogGroupEntry;
};

template<>
struct EntryTypeMapper<palf::LogEntry> {
  using EntryType = palf::LogEntry;
};

template<>
struct EntryTypeMapper<palf::LogGroupEntry> {
  using EntryType = palf::LogEntry;
};

template <class LogEntryType>
class IPalfIterator
{
public:
IPalfIterator() :
    is_inited_(false),
    palf_iterator_(),
    enable_logservice_(GCONF.enable_logservice),
    destroy_storage_functor_()
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    ,libpalf_log_iterator_(),
    libpalf_group_iterator_()
#endif
{}
  ~IPalfIterator()
  {
    destroy();
  }

  int init(const palf::LSN &lsn, palf::IPalfHandleImpl *palf_handle_impl);

  IPalfIterator(bool enable_logservice) :
    is_inited_(false),
    palf_iterator_(),
    enable_logservice_(enable_logservice),
    destroy_storage_functor_()
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    ,libpalf_log_iterator_(),
    libpalf_group_iterator_()
#endif
{}
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int init(const palf::LSN &lsn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi);
  int init(const share::SCN &scn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi);
#endif
  inline int init(const palf::LSN &lsn,
          const palf::GetFileEndLSN &get_file_end_lsn,
          palf::ILogStorage *log_storage)
  {
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfIterator is inited twice", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        if (OB_FAIL(ret = init_libpalf_iterator_(lsn, get_file_end_lsn, log_storage))) {
            PALF_LOG(WARN, "init_libpalf_iterator_ failed", K(ret), K(log_storage));
        } else {
            is_inited_ = true;
        }
#endif
    } else {
        if (OB_FAIL(ret = palf_iterator_.init(lsn, get_file_end_lsn, log_storage))) {
            PALF_LOG(WARN, "palf_iterator_.init failed", K(ret));
        } else {
            is_inited_ = true;
            PALF_LOG(TRACE, "palf_iterator_.init success", K(ret));
        }
    }
    return ret;
  }

  inline int init_libpalf_iterator_(const palf::LSN &lsn,
                      const palf::GetFileEndLSN &get_file_end_lsn,
                      palf::ILogStorage *log_storage);

  inline int set_io_context(const palf::LogIOContext &io_ctx)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // do nothing
#endif
    } else {
        ret = palf_iterator_.set_io_context(io_ctx);
    }
    return ret;
  }
  inline int reuse(const palf::LSN &start_lsn);

  inline void destroy()
  {
    int ret = OB_SUCCESS;
    is_inited_ = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    if (OB_NOT_NULL(libpalf_log_iterator_)) {
        if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_free_log_iterator(libpalf_log_iterator_, nullptr)))) {
            PALF_LOG(WARN, "libpalf_free_log_iterator failed", K(ret));
        } else {
            libpalf_log_iterator_ = nullptr;
            PALF_LOG(TRACE, "libpalf_free_log_iterator success", K(ret));
        }
        if (destroy_storage_functor_.is_valid()) {
            destroy_storage_functor_();
            destroy_storage_functor_.reset();
        }
    } else if (OB_NOT_NULL(libpalf_group_iterator_)) {
        if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_free_group_log_iterator(libpalf_group_iterator_, nullptr)))) {
            PALF_LOG(WARN, "libpalf_free_group_iterator failed", K(ret));
        } else {
            libpalf_group_iterator_ = nullptr;
            PALF_LOG(TRACE, "libpalf_free_group_iterator success", K(ret));
        }
        if (destroy_storage_functor_.is_valid()) {
            destroy_storage_functor_();
            destroy_storage_functor_.reset();
        }
    } else
#endif
    {
        palf_iterator_.destroy();
    }
  }

  inline int next()
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        const share::SCN upper_bound_scn = SCN::max_scn();
        ret = next_libpalf_iterator_(upper_bound_scn);
#endif
    } else {
        ret = palf_iterator_.next();
    }
    return ret;
  }
  inline int next(const share::SCN &replayable_point_scn)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = next_libpalf_iterator_(replayable_point_scn);
#endif
    } else {
        ret = palf_iterator_.next(replayable_point_scn);
    }
    return ret;
  }
  inline int next(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = next_libpalf_iterator_temp(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
#endif
    } else {
        ret = palf_iterator_.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
    }
    return ret;
  }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int next_libpalf_iterator_(const share::SCN &upper_bound_scn);
  int next_libpalf_iterator_temp(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point);
#endif

  inline int get_entry(LogEntryType &entry, palf::LSN &lsn)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = get_entry_libpalf_iterator_(entry, lsn);
#endif
    } else {
        ret = palf_iterator_.get_entry(entry.palf_log_entry_, lsn);
    }
    return ret;
  }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int get_entry_libpalf_iterator_(LogEntryType &entry, palf::LSN &lsn);
  int get_entry_libpalf_iterator_(const char *&buffer, LogEntryType &entry, palf::LSN &lsn);
#endif

  inline int get_entry(const char *&buffer, LogEntryType &entry, palf::LSN& lsn)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        if (OB_FAIL(get_entry_libpalf_iterator_(entry, lsn))) {
            PALF_LOG(WARN, "get_entry_libpalf_iterator_ failed", K(ret));
        } else {
            buffer = entry.get_data_buf() - entry.get_header_size();
            PALF_LOG(TRACE, "IPalfIterator get_entry success", K(ret), KPC(this), K(entry));
        }
#endif
    } else {
        ret = palf_iterator_.get_entry(buffer, entry.palf_log_entry_, lsn);
    }
    return ret;
  }

  inline int get_entry(const char *&buffer,
    int64_t &nbytes,
    share::SCN &scn,
    palf::LSN &lsn,
    bool &is_raw_write)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
      ret = get_entry_libpalf_iterator_data_(buffer, nbytes, scn, lsn, is_raw_write);
#endif
    } else {
      ret = palf_iterator_.get_entry(buffer, nbytes, scn, lsn, is_raw_write);
    }
    return ret;
  }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int get_entry_libpalf_iterator_data_(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, bool &is_raw_write);
#endif

  inline int get_entry(const char *&buffer, int64_t &nbytes, palf::LSN &lsn, int64_t &log_proposal_id)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else {
        ret = OB_NOT_SUPPORTED;
        PALF_LOG(WARN, "get_entry with proposal id not support with LogEntry Iterator", K(ret));
    }
    return ret;
  }
  inline int get_entry(const char *&buffer, int64_t &nbytes, share::SCN &scn, palf::LSN &lsn, int64_t &log_proposal_id, bool &is_raw_write)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else {
        ret = OB_NOT_SUPPORTED;
        PALF_LOG(WARN, "get_entry with proposal id not support with LogEntry Iterator", K(ret));
    }
    return ret;
  }
  inline int get_consumed_info(palf::LSN &end_lsn, share::SCN &end_scn)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = get_libpalf_iterator_consumed_info(end_lsn, end_scn);
#endif
    } else {
        ret = OB_NOT_SUPPORTED;
    }
    return ret;
  }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int get_libpalf_iterator_consumed_info(palf::LSN &lsn, share::SCN &scn);
#endif

  inline bool is_inited() const
  {
    return is_inited_;
  }
  inline void set_is_inited() {
    is_inited_ = true;
  }
  inline bool is_valid() const
  {
    bool valid = false;
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        // do nothing
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        if (OB_FAIL(libpalf_iterator_is_valid(valid))) {
            PALF_LOG(WARN, "libpalf_iterator_is_valid failed", K(ret));
        } else {
            PALF_LOG(TRACE, "libpalf_iterator_is_valid success", K(ret), K(valid));
        }
#endif
    } else {
        valid = palf_iterator_.is_valid();
    }
    return valid;
  }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  inline int libpalf_iterator_is_valid(bool &is_valid) const;
#endif

  inline void set_logservice_mode(const bool enable_logservice) {
    enable_logservice_ = enable_logservice;
  }
  bool check_is_the_last_entry()
  {
    bool is_last_entry = false;
    if (IS_NOT_INIT) {
        // do nothing
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // is_last_entry = libpalf_iterator_.check_is_the_last_entry();
        is_last_entry = false;
#endif
    } else {
        is_last_entry = palf_iterator_.check_is_the_last_entry();
    }
    return is_last_entry;
  }
  // @brief cleanup some resource when calling 'destroy'.
  inline int set_destroy_iterator_storage_functor(const palf::DestroyStorageFunctor &destroy_func)
  {
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // TODO by xiyu.
        // ret = libpalf_iterator_.set_destroy_iterator_storage_functor(destroy_func);
        if (!destroy_func.is_valid()) {
            ret = OB_INVALID_ARGUMENT;
            PALF_LOG(WARN, "destroy_func is invalid", K(ret));
        } else if (OB_FALSE_IT(destroy_storage_functor_ = destroy_func)) {
        } else if (OB_UNLIKELY(!destroy_storage_functor_.is_valid())) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            PALF_LOG(WARN, "destroy_storage_functor_ is invalid", K(ret));
        }
#endif
    } else {
        ret = palf_iterator_.set_destroy_iterator_storage_functor(destroy_func);
    }
    return ret;
  }

  inline void set_need_print_error(const bool need_print_error) {
    need_print_error_ = need_print_error;
  }
  palf::PalfBufferIterator &get_palf_iterator_() {
    return palf_iterator_;
  }

#ifdef OB_BUILD_SHARED_LOG_SERVICE
  TO_STRING_KV(K(is_inited_),
               K(enable_logservice_),
               K(palf_iterator_),
               K(libpalf_log_iterator_),
               K(libpalf_group_iterator_));
#else
  TO_STRING_KV(K(is_inited_),
               K(palf_iterator_));
#endif

public:
    bool is_inited_;

    palf::PalfIterator<typename EntryTypeMapper<LogEntryType>::EntryType> palf_iterator_;
    bool enable_logservice_;
    bool need_print_error_;
    palf::DestroyStorageFunctor destroy_storage_functor_;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    const libpalf::LibPalfLogEntryIteratorFFI *libpalf_log_iterator_;
    const libpalf::LibPalfGroupEntryIteratorFFI *libpalf_group_iterator_;
#endif
    DISALLOW_COPY_AND_ASSIGN(IPalfIterator);
};

template <>
inline int IPalfIterator<ILogEntry>::init(const palf::LSN &lsn, palf::IPalfHandleImpl *palf_handle_impl)
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfIterator has been inited", K(ret));
    } else if (GCONF.enable_logservice) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "PalfBufferIterator is not compatible with logservice", K(ret));
    } else if (!lsn.is_valid() || OB_ISNULL(palf_handle_impl)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), KP(palf_handle_impl));
    } else if (OB_FAIL(palf_handle_impl->alloc_palf_buffer_iterator(lsn, palf_iterator_))) {
        PALF_LOG(WARN, "alloc_palf_buffer_iterator failed", K(ret), K(lsn));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "IPalfIterator init success", K(lsn), K(this));
    }
    return ret;
}

template <>
inline int IPalfIterator<IGroupEntry>::init(const palf::LSN &lsn, palf::IPalfHandleImpl *palf_handle_impl)
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfIterator has been inited", K(ret));
    } else if (GCONF.enable_logservice) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "PalfBufferIterator is not compatible with logservice", K(ret));
    } else if (!lsn.is_valid() || OB_ISNULL(palf_handle_impl)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "invalid argument", K(ret), K(lsn), KP(palf_handle_impl));
    } else if (OB_FAIL(palf_handle_impl->alloc_palf_group_buffer_iterator(lsn, palf_iterator_))) {
        PALF_LOG(WARN, "alloc_palf_group_buffer_iterator failed", K(ret), K(lsn));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "IPalfIterator init success", K(lsn), K(this));
    }
    return ret;
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<ILogEntry>::init(const palf::LSN &lsn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfIterator has been inited", K(ret));
    } else if (!GCONF.enable_logservice) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not compatible with logservice", K(ret));
    } else if (!lsn.is_valid() || OB_ISNULL(libpalf_handle_ffi)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not valid", K(ret), KP(libpalf_handle_ffi));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_seek_log_iterator(
                lsn.val_, libpalf_handle_ffi, &libpalf_log_iterator_)))) {
        PALF_LOG(WARN, "libpalf_seek_log_iterator failed",
                K(ret), K(lsn), KP(libpalf_handle_ffi), KP(libpalf_log_iterator_));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator init success", K(lsn), KP(libpalf_handle_ffi), K(this));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<IGroupEntry>::init(const palf::LSN &lsn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    bool is_read_committed = true;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfIterator has been inited", K(ret));
    } else if (!GCONF.enable_logservice) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not compatible with logservice", K(ret));
    } else if (!lsn.is_valid() || OB_ISNULL(libpalf_handle_ffi)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not valid", K(ret), KP(libpalf_handle_ffi));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_seek_group_log_iterator(
                lsn.val_, is_read_committed, libpalf_handle_ffi, &libpalf_group_iterator_)))) {
        PALF_LOG(WARN, "libpalf_seek_group_log_iterator failed",
                K(ret), K(lsn), KP(libpalf_handle_ffi), KP(libpalf_group_iterator_));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator init success", K(lsn), KP(libpalf_handle_ffi), K(this));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<ILogEntry>::init(const share::SCN &scn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    uint64_t ret_scn = scn.get_val_for_logservice();
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfIterator has been inited", K(ret));
    } else if (!scn.is_valid() || OB_ISNULL(libpalf_handle_ffi)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not valid", K(ret), KP(libpalf_handle_ffi));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_seek_log_iterator_with_scn(
                ret_scn, libpalf_handle_ffi, &libpalf_log_iterator_)))) {
        PALF_LOG(WARN, "libpalf_seek_log_iterator failed",
                K(ret), K(scn), KP(libpalf_handle_ffi), KP(libpalf_log_iterator_));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator init success", K(scn), KP(libpalf_handle_ffi), K(this));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<IGroupEntry>::init(const share::SCN &scn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    bool is_read_committed = true;
    uint64_t ret_scn = scn.get_val_for_logservice();
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfIterator has been inited", K(ret));
    } else if (!scn.is_valid() || OB_ISNULL(libpalf_handle_ffi)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not valid", K(ret), KP(libpalf_handle_ffi));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_seek_group_log_iterator_with_scn(
                ret_scn, is_read_committed, libpalf_handle_ffi, &libpalf_group_iterator_)))) {
        PALF_LOG(WARN, "libpalf_seek_group_log_iterator failed",
                K(ret), K(scn), KP(libpalf_handle_ffi), KP(libpalf_group_iterator_));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator init success", K(scn), KP(libpalf_handle_ffi), K(this));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<IGroupEntry>::init_libpalf_iterator_(const palf::LSN &lsn,
                                                    const palf::GetFileEndLSN &get_file_end_lsn,
                                                    palf::ILogStorage *log_storage)
{
    int ret = OB_SUCCESS;

    const libpalf::LibPalfIteratorMemoryStorageFFI *memory_storage = log_storage->get_memory_storage();
    if (OB_ISNULL(log_storage)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "log_storage is NULL", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_seek_mem_group_log_iterator(lsn.val_, &libpalf_group_iterator_, memory_storage, get_file_end_lsn().val_)))) {
        PALF_LOG(WARN, "MemoryGroupIteratorStorage seek failed", K(ret), K(lsn));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "libpalf_group_iterator_ init success");
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<ILogEntry>::init_libpalf_iterator_(const palf::LSN &lsn,
                                                    const palf::GetFileEndLSN &get_file_end_lsn,
                                                    palf::ILogStorage *log_storage)
{
    int ret = OB_SUCCESS;

    const libpalf::LibPalfIteratorMemoryStorageFFI *memory_storage = log_storage->get_memory_storage();
    if (OB_ISNULL(log_storage)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "log_storage is NULL", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_seek_mem_log_iterator(lsn.val_, &libpalf_log_iterator_, memory_storage, get_file_end_lsn().val_)))) {
        PALF_LOG(WARN, "MemoryLogIteratorStorage seek failed", K(ret), K(lsn));
    } else {
        is_inited_ = true;
        PALF_LOG(TRACE, "libpalf_log_iterator_ init success");
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<IGroupEntry>::next_libpalf_iterator_(const share::SCN &upper_bound_scn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_next(libpalf_group_iterator_, upper_bound_scn.get_val_for_logservice())))) {
        PALF_LOG(WARN, "libpalf_group_iterator_next failed", K(ret));
    } else {
        PALF_LOG(TRACE, "libpalf_group_iterator_next success", K(ret));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<ILogEntry>::next_libpalf_iterator_(const share::SCN &upper_bound_scn)
{
    int ret = OB_SUCCESS;
    uint64_t next_min_scn_val = 0;
    bool iterate_end_by_upper_bound = false;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_next(libpalf_log_iterator_,
            upper_bound_scn.get_val_for_logservice(), &next_min_scn_val, &iterate_end_by_upper_bound)))) {
        PALF_LOG(WARN, "libpalf_log_iterator_next failed", K(ret));
    } else {
        PALF_LOG(TRACE, "libpalf_log_iterator_next success", K(ret));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<ILogEntry>::next_libpalf_iterator_temp(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point)
{
    int ret = OB_SUCCESS;
    uint64_t next_min_scn_ffi_outer_param = 0;
    int tmp_ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_next(
        libpalf_log_iterator_,
        replayable_point_scn.get_val_for_logservice(),
        &next_min_scn_ffi_outer_param,
        &iterate_end_by_replayable_point)))
        && OB_FAIL(tmp_ret = ret)
        && ret != OB_ITER_END) {
        PALF_LOG(WARN, "libpalf_log_iterator_next failed", K(ret), KP(libpalf_log_iterator_),
                 K(replayable_point_scn), K(next_min_scn_ffi_outer_param),
                 K(iterate_end_by_replayable_point));
    } else if (OB_FAIL(next_min_scn.convert_for_logservice(next_min_scn_ffi_outer_param))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K(next_min_scn_ffi_outer_param));
    } else if (OB_ITER_END == tmp_ret) {
        ret = OB_ITER_END;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator next end", K(ret), KP(libpalf_log_iterator_), KP(this),
                 K(replayable_point_scn), K(next_min_scn), K(iterate_end_by_replayable_point));
    } else {
        PALF_LOG(TRACE, "LibPalfLogEntryIterator next succ", K(ret), KP(libpalf_log_iterator_), KP(this),
                 K(replayable_point_scn), K(next_min_scn), K(iterate_end_by_replayable_point));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<IGroupEntry>::next_libpalf_iterator_temp(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point)
{
    int ret = OB_SUCCESS;
    uint64_t next_min_scn_ffi_outer_param = 0;
    int tmp_ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_next(
        libpalf_group_iterator_,
        replayable_point_scn.get_val_for_logservice())))
        && OB_FAIL(tmp_ret = ret)
        && ret != OB_ITER_END) {
        PALF_LOG(WARN, "libpalf_group_iterator_next failed", K(ret), KP(libpalf_group_iterator_),
                 K(replayable_point_scn), K(next_min_scn_ffi_outer_param),
                 K(iterate_end_by_replayable_point));
    } else if (OB_FAIL(next_min_scn.convert_for_logservice(next_min_scn_ffi_outer_param))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K(next_min_scn_ffi_outer_param));
    } else if (OB_ITER_END == tmp_ret) {
        ret = OB_ITER_END;
        PALF_LOG(TRACE, "LibPalfLogEntryIterator next end", K(ret), KP(libpalf_group_iterator_), KP(this),
                 K(replayable_point_scn), K(next_min_scn), K(iterate_end_by_replayable_point));
    } else {
        PALF_LOG(TRACE, "LibPalfLogEntryIterator next succ", K(ret), KP(libpalf_group_iterator_), KP(this),
                 K(replayable_point_scn), K(next_min_scn), K(iterate_end_by_replayable_point));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<IGroupEntry>::get_entry_libpalf_iterator_(IGroupEntry &entry, palf::LSN &lsn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_get_entry(libpalf_group_iterator_, &lsn.val_, &entry.libpalf_log_entry_)))) {
        PALF_LOG(WARN, "libpalf_group_iterator_get_entry failed", K(ret), K(lsn));
    } else {
        PALF_LOG(TRACE, "get_entry_libpalf_iterator_ success", K(lsn));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<ILogEntry>::get_entry_libpalf_iterator_(ILogEntry &entry, palf::LSN &lsn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_get_entry(libpalf_log_iterator_, &lsn.val_, &entry.libpalf_log_entry_)))) {
        PALF_LOG(WARN, "libpalf_log_iterator_get_entry failed", K(ret), K(lsn));
    } else {
        PALF_LOG(TRACE, "get_entry_libpalf_iterator_ success", K(lsn));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<IGroupEntry>::get_entry_libpalf_iterator_(const char *&buffer, IGroupEntry &entry, palf::LSN &lsn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_get_entry(libpalf_group_iterator_, &lsn.val_, &entry.libpalf_log_entry_)))) {
        PALF_LOG(WARN, "libpalf_group_iterator_get_entry failed", K(ret), K(lsn));
    } else {
        buffer = reinterpret_cast<const char*>(entry.libpalf_log_entry_.body) - sizeof(libpalf::LibPalfLogGroupEntryHeader);
        PALF_LOG(TRACE, "get_entry_libpalf_iterator_ success", K(ret), K(lsn), KP(buffer));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<ILogEntry>::get_entry_libpalf_iterator_(const char *&buffer, ILogEntry &entry, palf::LSN &lsn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_get_entry(libpalf_group_iterator_, &lsn.val_, &entry.libpalf_log_entry_)))) {
        PALF_LOG(WARN, "libpalf_group_iterator_get_entry failed", K(ret), K(lsn));
    } else {
        buffer = reinterpret_cast<const char*>(entry.libpalf_log_entry_.data_buf) - sizeof(libpalf::LibPalfLogEntryHeader);
        PALF_LOG(TRACE, "get_entry_libpalf_iterator_ success", K(ret), K(lsn), KP(buffer));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<ILogEntry>::get_entry_libpalf_iterator_data_(const char *&buffer,
    int64_t &nbytes,
    share::SCN &scn,
    palf::LSN &lsn,
    bool &is_raw_write)
{
    int ret = OB_SUCCESS;
    libpalf::LibPalfLogEntry entry(libpalf::LibPalfLogEntryHeader(0, 0, 0, 0, 0, 0), NULL);
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_get_entry(libpalf_log_iterator_, &lsn.val_, &entry)))) {
        PALF_LOG(WARN, "libpalf_log_iterator_get_entry failed", K(ret), K(lsn));
    } else if (OB_FAIL(scn.convert_for_logservice(entry.header.scn))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K(entry.header.scn));
    } else {
        buffer = reinterpret_cast<const char*>(entry.data_buf);
        nbytes = entry.header.data_size;
        is_raw_write = false;
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<IGroupEntry>::get_entry_libpalf_iterator_data_(const char *&buffer,
    int64_t &nbytes,
    share::SCN &scn,
    palf::LSN &lsn,
    bool &is_raw_write)
{
    int ret = OB_SUCCESS;
    libpalf::LibPalfLogGroupEntry entry(libpalf::LibPalfLogGroupEntryHeader(0, 0, 0, 0, 0, 0, 0, 0, 0), NULL);
    if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_get_entry(libpalf_group_iterator_, &lsn.val_, &entry)))) {
        PALF_LOG(WARN, "libpalf_group_iterator_get_entry failed", K(ret), K(lsn));
    } else if (OB_FAIL(scn.convert_for_logservice(entry.header.max_scn))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret), K(entry.header.max_scn));
    } else {
        buffer = reinterpret_cast<const char*>(entry.body);
        nbytes = entry.header.data_size;
        is_raw_write = false;
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<ILogEntry>::get_libpalf_iterator_consumed_info(palf::LSN &lsn, share::SCN &scn)
{
    int ret = OB_SUCCESS;
    uint64_t scn_val = 0;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_get_consumed_info(libpalf_log_iterator_, &lsn.val_, &scn_val)))) {
        PALF_LOG(WARN, "libpalf_log_iterator_get_consumed_info failed", K(ret));
    } else if (OB_FAIL(scn.convert_for_logservice(scn_val))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template <>
inline int IPalfIterator<IGroupEntry>::get_libpalf_iterator_consumed_info(palf::LSN &lsn, share::SCN &scn)
{
    int ret = OB_SUCCESS;
    uint64_t scn_val = 0;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_get_consumed_info(libpalf_group_iterator_, &lsn.val_, &scn_val)))) {
        PALF_LOG(WARN, "libpalf_group_iterator_get_consumed_info failed", K(ret));
    } else if (OB_FAIL(scn.convert_for_logservice(scn_val))) {
        PALF_LOG(WARN, "convert_for_logservice failed", K(ret));
    }
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<ILogEntry>::libpalf_iterator_is_valid(bool &is_valid) const
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_is_valid(libpalf_log_iterator_, &is_valid)))) {
        PALF_LOG(WARN, "libpalf_log_iterator_is_valid failed", K(ret), K(is_valid));
    } else {}
    return ret;
}
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
template<>
inline int IPalfIterator<IGroupEntry>::libpalf_iterator_is_valid(bool &is_valid) const
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_is_valid(libpalf_group_iterator_, &is_valid)))) {
        PALF_LOG(WARN, "libpalf_log_iterator_is_valid failed", K(ret), K(is_valid));
    } else {}
    return ret;
}
#endif

template<>
inline int IPalfIterator<IGroupEntry>::reuse(const palf::LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  } else if (enable_logservice_) {
      if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_group_iterator_reset(libpalf_group_iterator_, start_lsn.val_)))) {
          PALF_LOG(ERROR, "reuse log iterator failed", K(ret), K(start_lsn));
      } else {
          PALF_LOG(TRACE, "reuse log iterator success", K(ret),K(start_lsn));
      }
#endif
  } else {
      ret = palf_iterator_.reuse(start_lsn);
  }
  return ret;
}

template<>
inline int IPalfIterator<ILogEntry>::reuse(const palf::LSN &start_lsn)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
      ret = OB_NOT_INIT;
      PALF_LOG(WARN, "IPalfIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  } else if (enable_logservice_) {
      if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_log_iterator_reset(libpalf_log_iterator_, start_lsn.val_)))) {
          PALF_LOG(ERROR, "reuse log iterator failed", K(ret), K(start_lsn));
      } else {
          PALF_LOG(TRACE, "reuse log iterator success", K(ret),K(start_lsn));
      }
#endif
  } else {
      ret = palf_iterator_.reuse(start_lsn);
  }
  return ret;
}


} // end namespace ipalf
} // end namespace oceanbase

#endif