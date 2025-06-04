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

#include "share/config/ob_server_config.h"
#include "ipalf_iterator.h"
#include "logservice/palf/palf_handle_impl.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/libpalf/libpalf_common_define.h"
#endif

namespace oceanbase
{
namespace ipalf
{

IPalfLogIterator::IPalfLogIterator() :
    is_inited_(false),
    palf_iterator_()
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    ,enable_logservice_(false),
    libpalf_iterator_()
#endif
{ }


IPalfLogIterator::~IPalfLogIterator()
{
    destroy();
}

void IPalfLogIterator::destroy()
{
    is_inited_ = false;
    palf_iterator_.destroy();
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    libpalf_iterator_.destroy();
#endif
}

int IPalfLogIterator::init(const palf::LSN &lsn, palf::IPalfHandleImpl *palf_handle_impl)
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfLogIterator has been inited", K(ret));
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
        PALF_LOG(INFO, "IPalfLogIterator init success", K(lsn), K(this));
    }
    return ret;
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
int IPalfLogIterator::init(const palf::LSN &lsn, const libpalf::LibPalfHandleFFI *libpalf_handle_ffi)
{
    int ret = OB_SUCCESS;
    const libpalf::LibPalfLogEntryIteratorFFI *iterator_ffi = NULL;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
        PALF_LOG(WARN, "IPalfLogIterator has been inited", K(ret));
    } else if (!GCONF.enable_logservice) {
        ret = OB_ERR_UNEXPECTED;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not compatible with logservice", K(ret));
    } else if (!lsn.is_valid() || OB_ISNULL(libpalf_handle_ffi)) {
        ret = OB_INVALID_ARGUMENT;
        PALF_LOG(WARN, "LibPalfLogEntryIterator is not valid", K(ret), KP(libpalf_handle_ffi));
    } else if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf::libpalf_seek_log_iterator(
                lsn.val_, libpalf_handle_ffi, &iterator_ffi)))) {
        PALF_LOG(WARN, "libpalf_seek_log_iterator failed",
                K(ret), K(lsn), KP(libpalf_handle_ffi), KP(iterator_ffi));
    } else if (OB_FAIL(libpalf_iterator_.init(iterator_ffi))) {
        PALF_LOG(WARN, "LibPalfLogEntryIterator init failed", K(ret));
    } else {
        enable_logservice_ = GCONF.enable_logservice;
        is_inited_ = true;
        PALF_LOG(INFO, "LibPalfLogEntryIterator init success", K(lsn), KP(libpalf_handle_ffi), K(this));
    }
    return ret;
}
#endif

int IPalfLogIterator::set_io_context(const palf::LogIOContext &io_ctx)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // do nothing
#endif
    } else {
        ret = palf_iterator_.set_io_context(io_ctx);
    }
    return ret;
}

int IPalfLogIterator::reuse(const palf::LSN &start_lsn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = libpalf_iterator_.reuse(start_lsn);
#endif
    } else {
        ret = palf_iterator_.reuse(start_lsn);
    }
    return ret;
}

int IPalfLogIterator::next()
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = libpalf_iterator_.next();
#endif
    } else {
        ret = palf_iterator_.next();
    }
    return ret;
}

int IPalfLogIterator::next(const share::SCN &replayable_point_scn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = libpalf_iterator_.next(replayable_point_scn);
#endif
    } else {
        ret = palf_iterator_.next(replayable_point_scn);
    }
    return ret;
}

int IPalfLogIterator::next(const share::SCN &replayable_point_scn, share::SCN &next_min_scn, bool &iterate_end_by_replayable_point)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = libpalf_iterator_.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
#endif
    } else {
        ret = palf_iterator_.next(replayable_point_scn, next_min_scn, iterate_end_by_replayable_point);
    }
    return ret;
}


int IPalfLogIterator::get_entry(ILogEntry &entry, palf::LSN &lsn)
{
    int ret = OB_SUCCESS;
    ILogEntry tmp_entry;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret), K(lsn));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        if (OB_FAIL(libpalf_iterator_.get_entry(tmp_entry.libpalf_log_entry_, lsn))) {
            PALF_LOG(WARN, "libpalf_iterator_ get_entry failed", K(ret), K(lsn));
        } else if (OB_FAIL(entry.init(tmp_entry.libpalf_log_entry_))) {
            PALF_LOG(WARN, "ipalf log entry init failed", K(ret), K(lsn));
        } else {
            // do nothing
        }
#endif
    } else {
        if (OB_FAIL(palf_iterator_.get_entry(tmp_entry.palf_log_entry_, lsn))) {
            PALF_LOG(WARN, "palf_iterator_ get_entry failed", K(ret), K(lsn));
        } else if (OB_FAIL(entry.init(tmp_entry.palf_log_entry_))) {
            PALF_LOG(WARN, "ipalf log entry init failed", K(ret), K(lsn));
        } else {
            // do nothing
        }
    }
    return ret;
}

int IPalfLogIterator::get_entry(const char *&buffer, ILogEntry &entry, palf::LSN& lsn)
{
    int ret = OB_SUCCESS;
    ILogEntry tmp_entry;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        if (OB_FAIL(libpalf_iterator_.get_entry(buffer, tmp_entry.libpalf_log_entry_, lsn))) {
            PALF_LOG(WARN, "libpalf_iterator_ get_entry failed", K(ret), K(lsn));
        } else if (OB_FAIL(entry.init(tmp_entry.libpalf_log_entry_))) {
            PALF_LOG(WARN, "ipalf log entry init failed", K(ret), K(lsn));
        } else {
            // do nothing
        }
#endif
    } else {
        if (OB_FAIL(palf_iterator_.get_entry(buffer, tmp_entry.palf_log_entry_, lsn))) {
            PALF_LOG(WARN, "palf_iterator_ get_entry failed", K(ret), K(lsn));
        } else if (OB_FAIL(entry.init(tmp_entry.palf_log_entry_))) {
            PALF_LOG(WARN, "ipalf log entry init failed", K(ret), K(lsn));
        } else {
            // do nothing
        }
    }
    return ret;
}

int IPalfLogIterator::get_entry(const char *&buffer,
                                int64_t &nbytes,
                                share::SCN &scn,
                                palf::LSN &lsn,
                                bool &is_raw_write)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = libpalf_iterator_.get_entry(buffer, nbytes, scn, lsn, is_raw_write);
#endif
    } else {
        ret = palf_iterator_.get_entry(buffer, nbytes, scn, lsn, is_raw_write);
    }
    return ret;
}

int IPalfLogIterator::get_entry(const char *&buffer,
                                int64_t &nbytes, palf::LSN &lsn,
                                int64_t &log_proposal_id)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
    } else {
        ret = OB_NOT_SUPPORTED;
        PALF_LOG(WARN, "get_entry with proposal id not support with LogEntry Iterator", K(ret));
    }
    return ret;
}

int IPalfLogIterator::get_entry(const char *&buffer,
                                int64_t &nbytes,
                                share::SCN &scn,
                                palf::LSN &lsn,
                                int64_t &log_proposal_id,
                                bool &is_raw_write)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
    } else {
        ret = OB_NOT_SUPPORTED;
        PALF_LOG(WARN, "get_entry with proposal id not support with LogEntry Iterator", K(ret));
    }
    return ret;
}

int IPalfLogIterator::get_consumed_info(palf::LSN &end_lsn, share::SCN &end_scn)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = libpalf_iterator_.get_consumed_info(end_lsn, end_scn);
#endif
    } else {
        ret = OB_NOT_SUPPORTED;
    }
    return ret;
}


bool IPalfLogIterator::is_inited() const
{
    return is_inited_;
}

bool IPalfLogIterator::is_valid() const
{
    bool valid = false;
    if (IS_NOT_INIT) {
        // do nothing
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        valid = libpalf_iterator_.is_valid();
#endif
    } else {
        valid = palf_iterator_.is_valid();
    }
    return valid;
}

bool IPalfLogIterator::check_is_the_last_entry()
{
    bool is_last_entry = false;
    if (IS_NOT_INIT) {
        // do nothing
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        is_last_entry = libpalf_iterator_.check_is_the_last_entry();
#endif
    } else {
        is_last_entry = palf_iterator_.check_is_the_last_entry();
    }
    return is_last_entry;
}

int IPalfLogIterator::set_destroy_iterator_storage_functor(
    const palf::DestroyStorageFunctor &destroy_func)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
        PALF_LOG(WARN, "IPalfLogIterator is not inited", K(ret));
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        ret = libpalf_iterator_.set_destroy_iterator_storage_functor(destroy_func);
#endif
    } else {
        ret = palf_iterator_.set_destroy_iterator_storage_functor(destroy_func);
    }
    return ret;
}

} // end namespace ipalf
} // end namspace oceanbase