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

#ifndef oceanbase_share_ob_admin_dump_helper_
#define oceanbase_share_ob_admin_dump_helper_

#include <cstdint>
#include "common/ob_tablet_id.h"
#include "lib/utility/ob_print_utils.h"
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

namespace oceanbase
{
namespace share
{
enum class LogFormatFlag
{
  NO_FORMAT = 0,
  TX_FORMAT = 1,
  FILTER_FORMAT = 2,
  STAT_FORMAT = 3,
  META_FORMAT = 4,
  DECOMPRESS_FORMAT = 5,
};

class ObAdminLogDumperInterface
{
public:
  ObAdminLogDumperInterface() {}
  virtual ~ObAdminLogDumperInterface() {}
  virtual int start_object() = 0;
  virtual int end_object() = 0;
  virtual int dump_key(const char *arg) = 0;
  virtual int dump_int64(int64_t arg) = 0;
  virtual int dump_uint64(uint64_t arg) = 0;
  virtual int dump_string(const char *str) = 0;
};

class ObAdminLogStatDumper : public ObAdminLogDumperInterface
{
public:
  ObAdminLogStatDumper(){}
  ~ObAdminLogStatDumper(){}
public:
  int start_object();
  int end_object();
  int dump_key(const char *arg);
  int dump_int64(int64_t arg);
  int dump_uint64(uint64_t arg);
  int dump_string(const char *str);
};


class ObAdminLogNormalDumper : public ObAdminLogDumperInterface
{
public:
  ObAdminLogNormalDumper(){}
  ~ObAdminLogNormalDumper(){}
public:
  int start_object();
  int end_object();
  int dump_key(const char *arg);
  int dump_int64(int64_t arg);
  int dump_uint64(uint64_t arg);
  int dump_string(const char *str);
};

class ObAdminLogJsonDumper : public ObAdminLogDumperInterface, public rapidjson::PrettyWriter<rapidjson::StringBuffer>
{
public :
  ObAdminLogJsonDumper(rapidjson::StringBuffer &str_buf) : rapidjson::PrettyWriter<rapidjson::StringBuffer>::PrettyWriter(str_buf)
  {}
  ~ObAdminLogJsonDumper(){}
public:
  int start_object();
  int end_object();
  int dump_key(const char *arg);
  int dump_int64(int64_t arg);
  int dump_uint64(uint64_t arg);
  int dump_string(const char *str);
};

class ObAdminLogDumpFilter
{
public:
  ObAdminLogDumpFilter() : tx_id_(INVALID_TX_ID), tablet_id_() { }
  ~ObAdminLogDumpFilter() {}
  void reset()
  {
    tx_id_ = INVALID_TX_ID;
    tablet_id_.reset();
  }

  ObAdminLogDumpFilter &operator= (const ObAdminLogDumpFilter &rhs);
  bool is_tx_id_valid() const {return INVALID_TX_ID != tx_id_;}
  bool is_tablet_id_valid() const {return tablet_id_.is_valid(); }
  bool is_valid() const;
  int parse(const char *str);
  int64_t get_tx_id() const {return tx_id_;}
  common::ObTabletID get_tablet_id() const {return tablet_id_;}
public:
  TO_STRING_KV(K_(tx_id), K_(tablet_id));
private:
  static const int64_t INVALID_TX_ID = 0;
  int64_t tx_id_;
  common::ObTabletID tablet_id_;
};

struct ObLogStat
{
  ObLogStat() {reset();}
  ~ObLogStat() {}
  void reset();
  int64_t total_size() const;
  int64_t group_entry_header_size_;
  int64_t log_entry_header_size_;
  int64_t log_base_header_size_;
  int64_t tx_block_header_size_;
  int64_t tx_log_header_size_;
  int64_t tx_redo_log_size_;
  int64_t mutator_size_;
  int64_t new_row_size_;
  int64_t old_row_size_;
  int64_t total_group_entry_count_;
  int64_t total_log_entry_count_;
  int64_t total_tx_log_count_;
  int64_t total_tx_redo_log_count_;
  int64_t normal_row_count_;
  int64_t table_lock_count_;
  int64_t ext_info_log_count_;


  TO_STRING_KV(
  K(group_entry_header_size_),
  K(log_entry_header_size_),
  K(log_base_header_size_),
  K(tx_block_header_size_),
  K(tx_log_header_size_),
  K(tx_redo_log_size_),
  K(mutator_size_),
  K(new_row_size_),
  K(old_row_size_),
  K(total_group_entry_count_),
  K(total_log_entry_count_),
  K(total_tx_log_count_),
  K(total_tx_redo_log_count_),
  K(normal_row_count_),
  K(table_lock_count_),
  K(ext_info_log_count_));
};

struct ObAdminMutatorStringArg
{
public:
  ObAdminMutatorStringArg()
  {
    reset();
  }
  ~ObAdminMutatorStringArg() {reset();}

public:
  void reset();
  void reset_buf();
  ObAdminMutatorStringArg &operator= (const ObAdminMutatorStringArg &rhs);
  TO_STRING_KV(KP_(buf), K_(buf_len), KP(decompress_buf_), K(decompress_buf_len_), K_(pos), K(flag_), K(filter_),
               KPC(log_stat_));
public:
  char *buf_;
  int64_t buf_len_;

  char *decompress_buf_;
  int64_t decompress_buf_len_;
  int64_t pos_;
  LogFormatFlag flag_;
//  int64_t tx_id_;
  ObAdminLogDumperInterface *writer_ptr_;
  ObAdminLogDumpFilter filter_;
  ObLogStat *log_stat_;
};

} // namespace share
} // namespace oceanbase
#endif
