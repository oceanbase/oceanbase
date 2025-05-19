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
#include "ipalf_log_entry.h"

namespace oceanbase
{
namespace ipalf
{

ILogEntryHeader::ILogEntryHeader()
    : is_inited_(false),
      palf_log_header_(NULL)
#ifdef OB_BUILD_SHARED_LOG_SERVICE
      ,enable_logservice_(GCONF.enable_logservice),
      libpalf_log_header_(NULL)
#endif
{}

ILogEntryHeader::~ILogEntryHeader()
{
    reset();
}

void ILogEntryHeader::reset()
{
    is_inited_ = false;
    palf_log_header_ = NULL;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    enable_logservice_ = false;
    libpalf_log_header_ = NULL;
#endif
}

bool ILogEntryHeader::is_valid() const
{
    bool valid = false;
    if (IS_NOT_INIT) {
        valid = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // TODO by qingxia: is_valid() should be implemented in libpalf
        valid = true;
#endif
    } else {
        valid = palf_log_header_->is_valid();
    }
    return valid;
}

bool ILogEntryHeader::check_integrity(const char *buf, const int64_t buf_len) const
{
    bool integrity = false;
    if (!is_valid()) {
        integrity = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // TODO by qingxia: check_integrity() should be implemented in libpalf
        integrity = true;
#endif
    } else {
        integrity = palf_log_header_->check_integrity(buf, buf_len);
    }
    return integrity;
}

int32_t ILogEntryHeader::get_data_len() const
{
    int len = 0;
    if (!is_valid()) {
        len = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        len = libpalf_log_header_->data_size;
#endif
    } else {
        len = palf_log_header_->get_data_len();
    }
    return len;
}

const share::SCN ILogEntryHeader::get_scn() const
{
    share::SCN scn;
    if (!is_valid()) {
        scn.reset();
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(scn.convert_for_logservice(libpalf_log_header_->scn))) {
            PALF_LOG(ERROR, "convert_for_logservice failed",
                    K(ret), K(libpalf_log_header_->scn));
        } else {
            // do nothing
        }
#endif
    } else {
        scn = palf_log_header_->get_scn();
    }
    return scn;
}

int64_t ILogEntryHeader::get_data_checksum() const
{
    int64_t check_sum = 0;
    if (!is_valid()) {
        check_sum = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        check_sum = libpalf_log_header_->data_crc;
#endif
    } else {
        check_sum = palf_log_header_->get_data_checksum();
    }
    return check_sum;
}

bool ILogEntryHeader::check_header_integrity() const
{
    bool integrity = false;
    if (!is_valid()) {
        integrity = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    // TODO by qingxia: check header integrity should be implemented in libpalf
    } else if (enable_logservice_) {
        integrity = true;
#endif
    } else {
        integrity = palf_log_header_->check_header_integrity();
    }
    return integrity;
}

int64_t ILogEntryHeader::to_string(char *buf, const int64_t buf_len) const
{
    int64_t pos = 0;
    J_OBJ_START();
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    if (enable_logservice_) {
        if (NULL == libpalf_log_header_) {
            J_KV(K_(is_inited), K_(enable_logservice), KP_(libpalf_log_header));
        } else {
            J_KV(K_(is_inited), K_(enable_logservice), KP_(libpalf_log_header),
                 K(libpalf_log_header_->magic),
                 K(libpalf_log_header_->version),
                 K(libpalf_log_header_->data_size),
                 K(libpalf_log_header_->scn),
                 K(libpalf_log_header_->data_crc),
                 K(libpalf_log_header_->flag));
        }
    } else {
        J_KV(K_(is_inited), K_(enable_logservice), KP_(palf_log_header), KP_(palf_log_header));
    }
#else
    J_KV(K_(is_inited), KP_(palf_log_header), KP_(palf_log_header));
#endif
    J_OBJ_END();
    return pos;
}


ILogEntry::ILogEntry()
    : is_inited_(false),
      header_(),
      palf_log_entry_()
#ifdef OB_BUILD_SHARED_LOG_SERVICE
      ,enable_logservice_(GCONF.enable_logservice),
      libpalf_log_entry_(libpalf::LibPalfLogEntryHeader(0, 0, 0, 0, 0, 0), NULL)
#endif
{}

ILogEntry::~ILogEntry()
{
    reset();
}

int ILogEntry::shallow_copy(const ILogEntry &input)
{
    int ret = OB_SUCCESS;
    if (!input.is_valid()) {
        ret = OB_INVALID_ARGUMENT;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (input.enable_logservice_ != enable_logservice_) {
        ret = OB_ERR_UNEXPECTED;
    } else if (enable_logservice_) {
        libpalf_log_entry_ = input.libpalf_log_entry_;
        header_.libpalf_log_header_ = &libpalf_log_entry_.header;
        header_.is_inited_ = true;
        is_inited_ = true;
#endif
    } else {
        (void) palf_log_entry_.shallow_copy(input.palf_log_entry_);
        header_.palf_log_header_ = &palf_log_entry_.get_header();
        header_.is_inited_ = true;
        is_inited_ = true;
    }
    return ret;
}

bool ILogEntry::is_valid() const
{
    bool valid = false;
    if (IS_NOT_INIT) {
        valid = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        valid = header_.is_valid() && (NULL != libpalf_log_entry_.data_buf);
#endif
    } else {
        valid = palf_log_entry_.is_valid();
    }
    return valid;
}

void ILogEntry::reset()
{
    is_inited_ = false;
    header_.reset();
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    if (enable_logservice_) {
        libpalf_log_entry_.~LibPalfLogEntry();
    } else {
        palf_log_entry_.~LogEntry();
    }
#endif
    palf_log_entry_.~LogEntry();
}

bool ILogEntry::check_integrity() const
{
    bool integrity = false;
    if (IS_NOT_INIT) {
        integrity = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // TODO by qingxia: finish
        integrity = true;
#endif
    } else {
        integrity = palf_log_entry_.check_integrity();
    }
    return integrity;
}

int64_t ILogEntry::get_header_size() const
{
    int size = 0;
    if (IS_NOT_INIT) {
        size = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // TODO by qingxia: get header size after serialize is implemented
#endif
    } else {
        size = palf_log_entry_.get_header_size();
    }
    return size;
}

int64_t ILogEntry::get_payload_offset() const
{
    int offset = 0;
    if (IS_NOT_INIT) {
        offset = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // TODO by qingxia: get payload offset after serialize is implemented
#endif
    } else {
        offset = palf_log_entry_.get_payload_offset();
    }
    return offset;
}

int64_t ILogEntry::get_data_len() const
{
    int len = 0;
    if (IS_NOT_INIT) {
        len = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        len = libpalf_log_entry_.header.data_size;
#endif
    } else {
        len = palf_log_entry_.get_data_len();
    }
    return len;
}

const share::SCN ILogEntry::get_scn() const
{
    share::SCN scn;
    if (IS_NOT_INIT) {
        scn.reset();
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(scn.convert_for_logservice(libpalf_log_entry_.header.scn))) {
            PALF_LOG(WARN, "convert_for_logservice failed",
                    K(ret), K(libpalf_log_entry_.header.scn));
        } else {
            // do nothing
        }
#endif
    } else {
        scn = palf_log_entry_.get_scn();
    }
    return scn;
}

const char *ILogEntry::get_data_buf() const
{
    const char *buf = NULL;
    if (IS_NOT_INIT) {
        buf = NULL;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        buf = reinterpret_cast<const char*>(libpalf_log_entry_.data_buf);
#endif
    } else {
        buf = palf_log_entry_.get_data_buf();
    }
    return buf;
}

const ILogEntryHeader &ILogEntry::get_header()
{
    return header_;
}

int ILogEntry::init(palf::LogEntry &palf_log_entry)
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
    } else {
        palf_log_entry_.shallow_copy(palf_log_entry);
        header_.palf_log_header_ = &palf_log_entry_.get_header();
        header_.is_inited_ = true;
        is_inited_ = true;
    }
    return ret;
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
int ILogEntry::init(libpalf::LibPalfLogEntry &libpalf_log_entry)
{
    int ret = OB_SUCCESS;
    if (IS_INIT) {
        ret = OB_INIT_TWICE;
    } else if (!enable_logservice_) {
        ret = OB_ERR_UNEXPECTED;
    } else {
        libpalf_log_entry_ = libpalf_log_entry;
        header_.libpalf_log_header_ = &libpalf_log_entry_.header;
        header_.is_inited_ = true;
        is_inited_ = true;
    }
    return ret;
}
#endif

} // end namespace ipalf
} // end namspace oceanbase