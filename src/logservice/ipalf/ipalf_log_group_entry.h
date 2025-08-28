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

#ifndef OCEANBASE_LOGSERVICE_IPALF_LOG_GROUP_ENTRY_
#define OCEANBASE_LOGSERVICE_IPALF_LOG_GROUP_ENTRY_

#include "share/scn.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_group_entry.h"
#include "share/config/ob_server_config.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/libpalf/libpalf_common_define.h"
#include "palf_ffi.h"
#endif

namespace oceanbase
{
namespace palf
{
class LSN;
class LogGroupEntryHeader;
class LogGroupEntry;
}
namespace ipalf
{
template <class LogEntryType> class IPalfIterator;
class IGroupEntry;
class IGroupEntryHeader
{
public:
  friend class IGroupEntry;
public:
  IGroupEntryHeader();
  IGroupEntryHeader(bool enable_logservice);
  ~IGroupEntryHeader();
public:
  void reset();
  bool is_valid() const;
  int32_t get_data_len() const;
  int64_t get_serialize_size() const {
#ifdef OB_BUILD_SHARED_LOG_SERVICE
    if (enable_logservice_) {
      return sizeof(libpalf::LibPalfLogGroupEntryHeader);
    } else
#endif
    {
      return palf_log_header_->get_serialize_size();
    }
  }
  bool is_padding_log() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  DISALLOW_COPY_AND_ASSIGN(IGroupEntryHeader);
private:
  bool is_inited_;
  const palf::LogGroupEntryHeader *palf_log_header_;
  bool enable_logservice_;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  const libpalf::LibPalfLogGroupEntryHeader *libpalf_log_header_;
#endif
};

class IGroupEntry
{
public:
  template <class LogEntryType>
  friend class IPalfIterator;
public:
  IGroupEntry()
  :
#ifdef OB_BUILD_SHARED_LOG_SERVICE
      libpalf_log_entry_(libpalf::LibPalfLogGroupEntryHeader(0, 0, 0, 0, 0, 0, 0, 0, 0), NULL),
#endif
      is_inited_(true),
      header_(),
      palf_log_entry_(),
      enable_logservice_(GCONF.enable_logservice)
  {}
  IGroupEntry(bool enable_logservice)
  :
#ifdef OB_BUILD_SHARED_LOG_SERVICE
      libpalf_log_entry_(libpalf::LibPalfLogGroupEntryHeader(0, 0, 0, 0, 0, 0, 0, 0, 0), NULL),
#endif
      is_inited_(true),
      header_(enable_logservice),
      palf_log_entry_(),
      enable_logservice_(enable_logservice)
  {}
  ~IGroupEntry()
  {
    reset();
  }

private:

public:
  bool is_valid() const;
  void reset();
  int64_t get_header_size() const;
  int64_t get_data_len() const;
  const share::SCN get_scn() const;
  const char *get_data_buf() const;
  const IGroupEntryHeader &get_header();
  int64_t get_serialize_size(const palf::LSN &lsn) const;
  int64_t get_group_entry_size(const palf::LSN &lsn) const;
  int serialize(const palf::LSN &lsn, char * buf, int64_t size, int64_t &pos) const;
  int deserialize(palf::LSN &lsn, const char *buf, int64_t size, int64_t &pos);
  bool check_integrity(const palf::LSN &lsn) const;
  bool check_compatibility() const;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  TO_STRING_KV(K(enable_logservice_),
               K(header_),
               K(palf_log_entry_),
               KP(libpalf_log_entry_.body),
               K(libpalf_log_entry_.header.magic),
               K(libpalf_log_entry_.header.version),
               K(libpalf_log_entry_.header.data_size),
               K(libpalf_log_entry_.header.proposal_id),
               K(libpalf_log_entry_.header.committed_end_lsn),
               K(libpalf_log_entry_.header.max_scn),
               K(libpalf_log_entry_.header.acc_crc),
               K(libpalf_log_entry_.header.log_id),
               K(libpalf_log_entry_.header.flag));
#else
  TO_STRING_KV(K(header_),
               K(palf_log_entry_));
#endif
private:
  DISALLOW_COPY_AND_ASSIGN(IGroupEntry);
  int init(palf::LogGroupEntry &palf_log_group_entry);
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int init(libpalf::LibPalfLogGroupEntry &libpalf_log_group_entry);
#endif
public:
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  libpalf::LibPalfLogGroupEntry libpalf_log_entry_;
#endif
private:
  bool is_inited_;
  IGroupEntryHeader header_;
  palf::LogGroupEntry palf_log_entry_;
  bool enable_logservice_;
};
}
}
#endif