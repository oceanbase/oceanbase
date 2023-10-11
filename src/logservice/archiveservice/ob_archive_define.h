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

#ifndef OCEANBASE_ARCHIVE_OB_ARCHIVE_DEFINE_H_
#define OCEANBASE_ARCHIVE_OB_ARCHIVE_DEFINE_H_

#include "lib/ob_define.h"                  // int64_t
#include "lib/utility/ob_print_utils.h"     // print
#include "logservice/palf/log_define.h"     // PALF_BLOCK_SIZE
#include "share/backup/ob_archive_piece.h"  // ObArchivePiece
#include "share/backup/ob_backup_struct.h"  // ObBackupPathString
#include "logservice/palf/lsn.h"            // LSN
#include "share/scn.h"            // share::SCN

namespace oceanbase
{
namespace share
{
class SCN;
}
namespace archive
{
using oceanbase::palf::LSN;
using oceanbase::share::ObArchivePiece;

// =================== const define ================= //
const int64_t OB_INVALID_ARCHIVE_INCARNATION_ID = -1;
const int64_t OB_INVALID_ARCHIVE_DEST_ID = -1;
const int64_t OB_INVALID_ARCHIVE_ROUND_ID = -1;
const int64_t OB_INVALID_ARCHIVE_PIECE_ID = -1;
const int64_t OB_INVALID_ARCHIVE_LEASE_ID = -1;
// 归档文件大小是CLOG虚拟文件大小的N倍, N至少为1
const int64_t ARCHIVE_N = 1;
const int64_t MAX_LOG_FILE_SIZE = palf::PALF_BLOCK_SIZE;  // 64M - 16K
//归档log文件大小, 是ob日志文件整数倍, 默认情况下等于ob日志文件
const int64_t MAX_ARCHIVE_FILE_SIZE = ARCHIVE_N * MAX_LOG_FILE_SIZE;
const int64_t OB_INVALID_ARCHIVE_FILE_ID = 0;
const int64_t OB_INVALID_ARCHIVE_FILE_OFFSET = -1;
const int64_t COMMON_HEADER_SIZE = 4 * 1024L;    // 4K
const int64_t ARCHIVE_FILE_HEADER_SIZE = COMMON_HEADER_SIZE;
const int64_t DEFAULT_ARCHIVE_UNIT_SIZE = 16 * 1024L;   // 归档压缩加密单元大小
const int64_t ARCHIVE_FILE_DATA_BUF_SIZE = MAX_ARCHIVE_FILE_SIZE + ARCHIVE_FILE_HEADER_SIZE;

const int64_t DEFAULT_MAX_LOG_SIZE = palf::MAX_LOG_BUFFER_SIZE;
const int64_t MAX_FETCH_TASK_NUM = 4;

const int64_t MIN_FETCHER_THREAD_COUNT = 1;
const int64_t MAX_FETCHER_THREAD_COUNT = 3;

const int64_t DEFAULT_LS_RECORD_INTERVAL = 30 * 60 * 1000 * 1000L;   // 30min
const int64_t MAX_META_RECORD_DATA_SIZE = 2 * 1024 * 1024L;
const int64_t MAX_META_RECORD_FILE_SIZE = COMMON_HEADER_SIZE + MAX_META_RECORD_DATA_SIZE;  // 2M + 4K

const int64_t MAX_LS_ARCHIVE_MEMORY_LIMIT = 4 * MAX_LOG_FILE_SIZE;
const int64_t MAX_LS_SEND_TASK_COUNT_LIMIT = 6;
// ================================================= //

// 日志流leader授权备份zone内server归档, leader通过lease机制将授权下发给server
// 其中有日志流副本server具有更高选择优先级
//
// lease超时时间为30s, leader每隔5s向server续约lease, server收到授权之后30s内为该日志流归档服务
// 如果30s内未收到续约授权, server将不再为该日志流服务并丢弃已有任务
//
// lease_id与lease_start_ts共同标示连续生效的lease范围, 只有lease_id或lease_start_ts改变, 表示不同的lease在本server生效
//
// 当leader长时间收不到授权应答响应或者被server拒绝为该日志流归档服务, leader会等lease超时并等待足够长安全时间后,
// 选择其他server为该日志流归档服务
class ObArchiveLease
{
public:
  ObArchiveLease();
  ObArchiveLease(const int64_t lease_id, const int64_t start_ts, const int64_t end_ts);
  ~ObArchiveLease();

public:
  bool is_valid() const;
  void reset();
  ObArchiveLease &operator=(const ObArchiveLease &other);
  bool operator==(const ObArchiveLease &other) const;
  TO_STRING_KV(K_(lease_id), K_(lease_start_ts), K_(lease_end_ts));

public:
  // TODO 考虑使用选举epoch + server内递增序列号
  int64_t         lease_id_;        // 唯一标示lease
  int64_t         lease_start_ts_;  // 标示lease开始生效时间
  int64_t         lease_end_ts_;    // 标示生效lease超时时间
};

// 以LSN, 对应日志提交时间戳以及所属piece表示归档内部进度
class LogFileTuple
{
public:
  LogFileTuple();
  LogFileTuple(const LSN &lsn, const share::SCN &scn, const ObArchivePiece &piece);
  ~LogFileTuple();

public:
  bool is_valid() const;
  void reset();
  bool operator < (const LogFileTuple &other) const;
  LogFileTuple &operator=(const LogFileTuple &other);

  const LSN &get_lsn() const { return offset_; }
  const share::SCN &get_scn() const { return scn_; }
  const ObArchivePiece &get_piece() const { return piece_; };
  void compensate_piece();
  TO_STRING_KV(K_(offset), K_(scn), K_(piece));

private:
  LSN offset_;            // 文件内偏移
  share::SCN scn_;        // 最大log scn
  ObArchivePiece piece_;  // 所属piece
};

struct ArchiveKey
{
  int64_t incarnation_;
  int64_t dest_id_;
  int64_t round_;
  ArchiveKey();
  ArchiveKey(const int64_t incarnation, const int64_t dest_id, const int64_t round);
  ~ArchiveKey();
  void reset();
  bool is_valid() const;
  bool operator==(const ArchiveKey &other) const;
  bool operator!=(const ArchiveKey &other) const;
  ArchiveKey &operator=(const ArchiveKey &other);
  TO_STRING_KV(K_(incarnation), K_(dest_id), K_(round));
};
// 以incarnation/round/Lease唯一标示某个归档Task, 过期的Task是无效任务
class ArchiveWorkStation
{
public:
  ArchiveWorkStation();
  ArchiveWorkStation(const ArchiveKey &key, const ObArchiveLease &lease);
  ~ArchiveWorkStation();

public:
  bool is_valid() const;
  void reset();
  const ArchiveKey &get_round() const { return key_; }
  const ObArchiveLease &get_lease() const { return lease_; }
  ArchiveWorkStation &operator=(const ArchiveWorkStation &other);
  bool operator==(const ArchiveWorkStation &other) const;
  bool operator!=(const ArchiveWorkStation &other) const;
  TO_STRING_KV(K_(key), K_(lease));

private:
  ArchiveKey        key_;
  ObArchiveLease    lease_;
};

struct ObArchiveSendDestArg
{
  int64_t cur_file_id_;
  int64_t cur_file_offset_;
  LogFileTuple tuple_;
  bool piece_dir_exist_;
};

// file header for common archive log file
struct ObArchiveFileHeader
{
  int16_t magic_;                    // FH
  int16_t version_;
  int32_t flag_;//for compression and encrytion and so on
  int64_t unit_size_;
  int64_t start_lsn_;
  int64_t checksum_;

  bool is_valid() const;
  int generate_header(const LSN &lsn);
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(magic),
               K_(version),
               K_(flag),
               K_(unit_size),
               K_(start_lsn),
               K_(checksum));

private:
  static const int16_t ARCHIVE_FILE_HEADER_MAGIC = 0x4648; // FH means archive file header
};

// file header for ls meta file
struct ObLSMetaFileHeader
{
  int16_t magic_;
  int16_t version_;
  int32_t place_holder_;
  share::SCN timestamp_;
  int64_t data_checksum_;
  int64_t header_checksum_;

  bool is_valid() const;
  int generate_header(const share::SCN &timestamp, const int64_t data_checksum);
  NEED_SERIALIZE_AND_DESERIALIZE;
  TO_STRING_KV(K_(magic),
               K_(version),
               K_(place_holder),
               K_(timestamp),
               K_(data_checksum),
               K_(header_checksum));

private:
  static const int64_t LS_META_FILE_HEADER_MAGIC = 0x5348; // MH means ls meta file header
};

class ObArchiveInterruptReason
{
public:
  enum Factor
  {
    UNKONWN = 0,
    SEND_ERROR = 1,
    LOG_RECYCLE = 2,
    NOT_CONTINUOUS = 3,
    GC = 4,
    MAX,
  };
  ObArchiveInterruptReason() {}
  ObArchiveInterruptReason(Factor factor, char *lbt_trace, const int ret_code)
    : factor_(factor),
    lbt_trace_(lbt_trace),
    ret_code_(ret_code) {}
  const char *get_str() const;
  const char *get_lbt() const { return lbt_trace_; }
  int get_code() const { return ret_code_; }
  void set(Factor factor, char *lbt_trace, const int ret_code);
  TO_STRING_KV("reason", get_str(), K_(ret_code));
private:
  Factor factor_;
  char *lbt_trace_;
  int ret_code_;
};

} // namespace archive
} // namespace oceanbase
#endif /* OCEANBASE_ARCHIVE_OB_ARCHIVE_DEFINE_H_ */
