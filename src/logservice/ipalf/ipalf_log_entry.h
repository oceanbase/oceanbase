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

#ifndef OCEANBASE_LOGSERVICE_IPALF_LOG_ENTRY_
#define OCEANBASE_LOGSERVICE_IPALF_LOG_ENTRY_

#include "share/scn.h"
#include "logservice/palf/log_define.h"
#include "logservice/palf/log_entry.h"
#ifdef OB_BUILD_SHARED_LOG_SERVICE
#include "logservice/libpalf/libpalf_common_define.h"
#include "palf_ffi.h"
#endif

namespace oceanbase
{
namespace palf
{
class LSN;
class LogEntryHeader;
class LogEntry;
}
namespace ipalf
{
template <class LogEntryType> class IPalfIterator;
class ILogEntry;
class ILogEntryHeader
{
public:
  friend class ILogEntry;
public:
  ILogEntryHeader();
  ILogEntryHeader(bool enable_logservice);
  ~ILogEntryHeader();
public:
  // TODO by qingxia: finish
  // ILogEntryHeader& operator=(const ILogEntryHeader &header);
  void reset();
  bool is_valid() const;
  bool check_integrity(const char *buf, const int64_t buf_len) const;
  int32_t get_data_len() const;
  const share::SCN get_scn() const;
  int64_t get_data_checksum() const;
  bool check_header_integrity() const;
  int64_t to_string(char *buf, const int64_t buf_len) const;
private:
  DISALLOW_COPY_AND_ASSIGN(ILogEntryHeader);
private:
  bool is_inited_;
  const palf::LogEntryHeader *palf_log_header_;
  bool enable_logservice_;
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  const libpalf::LibPalfLogEntryHeader *libpalf_log_header_;
#endif
};

class ILogEntry
{
public:
  template <class LogEntryType>
  friend class IPalfIterator;
public:
  ILogEntry();
  ILogEntry(bool enable_logservice);
  ~ILogEntry();

public:
  int shallow_copy(const ILogEntry &input);
  bool is_valid() const;
  void reset();
  bool check_integrity() const;
  int64_t get_header_size() const;
  int64_t get_payload_offset() const;
  int64_t get_data_len() const;
  const share::SCN get_scn() const;
  const char *get_data_buf() const;
  const ILogEntryHeader &get_header();
  int64_t get_serialize_size(const palf::LSN &lsn) const;
  int serialize(const palf::LSN &lsn, char * buf, int64_t size, int64_t &pos) const;
  int deserialize(const palf::LSN &lsn, const char *buf, int64_t size, int64_t &pos);
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  TO_STRING_KV(K(enable_logservice_),
               K(header_),
               K(palf_log_entry_),
               KP(libpalf_log_entry_.data_buf),
               K(libpalf_log_entry_.header.magic),
               K(libpalf_log_entry_.header.version),
               K(libpalf_log_entry_.header.data_size),
               K(libpalf_log_entry_.header.scn),
               K(libpalf_log_entry_.header.data_crc),
               K(libpalf_log_entry_.header.flag));
#else
  TO_STRING_KV(K(header_),
               K(palf_log_entry_));
#endif
  // NEED_SERIALIZE_AND_DESERIALIZE;
  static const int64_t BLOCK_SIZE = palf::PALF_BLOCK_SIZE;
  using LogEntryHeaderType=ILogEntryHeader;
private:
  DISALLOW_COPY_AND_ASSIGN(ILogEntry);
  int init(palf::LogEntry &palf_log_entry);
#ifdef OB_BUILD_SHARED_LOG_SERVICE
  int init(libpalf::LibPalfLogEntry &libpalf_log_entry);
#endif

#ifdef OB_BUILD_SHARED_LOG_SERVICE
public:
  libpalf::LibPalfLogEntry libpalf_log_entry_;
#endif
private:
  bool is_inited_;
  ILogEntryHeader header_;
  palf::LogEntry palf_log_entry_;
  bool enable_logservice_;
};

} // end namespace ipalf
} // end namespace oceanbase

#endif