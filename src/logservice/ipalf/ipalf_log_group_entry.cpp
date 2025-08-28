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
#include "ipalf_log_group_entry.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "palf_ffi.h"
#endif

namespace oceanbase
{
namespace ipalf
{
IGroupEntryHeader::IGroupEntryHeader()
    : is_inited_(false),
      palf_log_header_(nullptr),
      enable_logservice_(GCONF.enable_logservice)
#ifdef OB_BUILD_SHARED_LOG_SERVICE
      ,libpalf_log_header_(nullptr)
#endif
{}
IGroupEntryHeader::IGroupEntryHeader(bool enable_logservice)
    : is_inited_(false),
      palf_log_header_(nullptr),
      enable_logservice_(enable_logservice)
#ifdef OB_BUILD_SHARED_LOG_SERVICE
      ,libpalf_log_header_(nullptr)
#endif
{}

IGroupEntryHeader::~IGroupEntryHeader()
{
    reset();
}

void IGroupEntryHeader::reset()
{
    is_inited_ = false;
    palf_log_header_ = nullptr;
    enable_logservice_ = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    libpalf_log_header_ = nullptr;
#endif
}

bool IGroupEntryHeader::is_valid() const
{
    bool valid = false;
    if (IS_NOT_INIT) {
        valid = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        // TODO by xiyu: is_valid() should be implemented in libpalf
        valid = true;
#endif
    } else {
        valid = palf_log_header_->is_valid();
    }
    return valid;
}

bool IGroupEntryHeader::is_padding_log() const
{
    bool is_padding = false;
    int64_t PADDING_TYPE_MASK = 1ll << 63;
    if (IS_NOT_INIT) {
        is_padding = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        is_padding = libpalf_log_header_->flag & PADDING_TYPE_MASK;
#endif
    } else {
        is_padding = palf_log_header_->is_padding_log();
    }
    return is_padding;
}

int64_t IGroupEntryHeader::to_string(char *buf, const int64_t buf_len) const
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
                 K(libpalf_log_header_->proposal_id),
                 K(libpalf_log_header_->committed_end_lsn),
                 K(libpalf_log_header_->max_scn),
                 K(libpalf_log_header_->acc_crc),
                 K(libpalf_log_header_->log_id),
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

bool IGroupEntry::is_valid() const
{
    bool valid = false;
    if (IS_NOT_INIT) {
        valid = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        valid = header_.is_valid() && (NULL != libpalf_log_entry_.body);
#endif
    } else {
        valid = palf_log_entry_.is_valid();
    }
    return valid;
}

void IGroupEntry::reset()
{
    is_inited_ = true;
    header_.reset();
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    if (enable_logservice_) {
        // TODO by xiyu
        libpalf_log_entry_.~LibPalfLogGroupEntry();
    }
#endif
    palf_log_entry_.~LogGroupEntry();
}

const share::SCN IGroupEntry::get_scn() const
{
    share::SCN scn;
    if (IS_NOT_INIT) {
        scn.reset();
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        int ret = OB_SUCCESS;
        if (OB_FAIL(scn.convert_for_logservice(libpalf_log_entry_.header.max_scn))) {
            PALF_LOG(WARN, "convert_for_logservice failed",
                    K(ret), K(libpalf_log_entry_.header.max_scn));
        } else {
            // do nothing
        }
#endif
    } else {
        scn = palf_log_entry_.get_scn();
    }
    return scn;
}

const IGroupEntryHeader &IGroupEntry::get_header()
{
    return header_;
}

int64_t IGroupEntry::get_header_size() const
{
    int size = 0;
    if (IS_NOT_INIT) {
        size = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        size = sizeof(libpalf::LibPalfLogGroupEntryHeader);
#endif
    } else {
        size = palf_log_entry_.get_header_size();
    }
    return size;
}

int64_t IGroupEntry::get_data_len() const
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

const char *IGroupEntry::get_data_buf() const
{
    const char *buf = NULL;
    if (IS_NOT_INIT) {
        buf = NULL;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        buf = reinterpret_cast<const char*>(libpalf_log_entry_.body);
#endif
    } else {
        buf = palf_log_entry_.get_data_buf();
    }
    return buf;
}

int64_t IGroupEntry::get_serialize_size(const palf::LSN &lsn) const
{
    int ret = OB_SUCCESS;
    int64_t size = 0;
    if (IS_NOT_INIT) {
        size = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        size_t body_size = 0;
        if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_group_log_entry_get_body_size(&libpalf_log_entry_.header, lsn.val_, &body_size)))) {
            PALF_LOG(WARN, "libpalf_group_log_entry_get_body_size failed", K(ret), K(lsn));
        } else {
            size = static_cast<int64_t>(body_size) + sizeof(libpalf_log_entry_.header);
        }
#endif
    } else {
        size = palf_log_entry_.get_serialize_size();
    }
    return size;
}

int64_t IGroupEntry::get_group_entry_size(const palf::LSN &lsn) const
{
    int ret = OB_SUCCESS;
    int64_t size = 0;
    if (IS_NOT_INIT) {
        size = 0;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        int64_t group_entry_header_size = header_.get_serialize_size();
        size_t group_entry_body_size = 0;
        if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_group_log_entry_get_body_size(&libpalf_log_entry_.header,
          lsn.val_, &group_entry_body_size)))) {
            PALF_LOG(WARN, "libpalf_group_log_entry_get_body_size failed", K(ret), K(lsn));
        } else {
            size = group_entry_header_size + static_cast<int64_t>(group_entry_body_size);
        }
#endif
    } else {
        size = palf_log_entry_.get_serialize_size();
    }
    return size;
}

int IGroupEntry::serialize(const palf::LSN &lsn, char * buf, int64_t size, int64_t &pos) const
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_NOT_INIT;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        size_t c_pos = static_cast<size_t>(pos);
        ret = LIBPALF_ERRNO_CAST(libpalf_group_entry_serialize(&libpalf_log_entry_, lsn.val_, buf, size, &c_pos));
        pos = static_cast<int64_t>(c_pos);
#endif
    } else {
        ret = palf_log_entry_.serialize(buf, size, pos);
    }
    return ret;
}

int IGroupEntry::deserialize(palf::LSN &lsn, const char *buf, int64_t size, int64_t &pos)
{
    int ret = OB_SUCCESS;
    if (IS_NOT_INIT) {
        ret = OB_INVALID_ARGUMENT;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        size_t c_pos = static_cast<size_t>(pos);
        ret = LIBPALF_ERRNO_CAST(libpalf_log_group_entry_deserialize(&libpalf_log_entry_, lsn.val_, buf, size, &c_pos));
        pos = static_cast<int64_t>(c_pos);
#endif
    } else {
        ret = palf_log_entry_.deserialize(buf, size, pos);
    }
    return ret;
}

bool IGroupEntry::check_integrity(const palf::LSN &lsn) const
{
    int ret = OB_SUCCESS;
    bool bool_ret = false;
    if (IS_NOT_INIT) {
        bool_ret = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        if (OB_FAIL(LIBPALF_ERRNO_CAST(libpalf_group_entry_check_integrity(&libpalf_log_entry_, lsn.val_)))) {
            PALF_LOG(WARN, "libpalf_group_entry_check_integrity failed", K(ret), K(lsn));
            bool_ret = false;
        } else {
            bool_ret = true;
        }
#endif
    } else {
        bool_ret = palf_log_entry_.check_integrity();
    }
    return bool_ret;
}

bool IGroupEntry::check_compatibility() const
{
    bool bool_ret = false;
    if (IS_NOT_INIT) {
        bool_ret = false;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    } else if (enable_logservice_) {
        bool_ret = true;
#endif
    } else {
        bool_ret = palf_log_entry_.check_compatibility();
    }
    return bool_ret;
}
int IGroupEntry::init(palf::LogGroupEntry &palf_log_group_entry)
{
    int ret = OB_SUCCESS;
    palf_log_entry_.shallow_copy(palf_log_group_entry);
    header_.palf_log_header_ = &palf_log_entry_.get_header();
    header_.is_inited_ = true;
    is_inited_ = true;
    return ret;
}

#ifdef OB_BUILD_SHARED_LOG_SERVICE
int IGroupEntry::init(libpalf::LibPalfLogGroupEntry &libpalf_log_group_entry)
{
    int ret = OB_SUCCESS;
    if (!enable_logservice_) {
        ret = OB_ERR_UNEXPECTED;
    } else {
        // TODO by xiyu : need to implement the shallow copy for libpalf::LibPalfLogGroupEntry
        libpalf_log_entry_.header = libpalf_log_group_entry.header;
        libpalf_log_entry_.body = libpalf_log_group_entry.body;
        header_.libpalf_log_header_ = &libpalf_log_entry_.header;
        header_.is_inited_ = true;
        is_inited_ = true;
    }
    return ret;
}
#endif
}
}