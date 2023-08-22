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

#ifndef OCEANBASE_SHARE_BACKUP_OB_BACKUP_CONFIG_H_
#define OCEANBASE_SHARE_BACKUP_OB_BACKUP_CONFIG_H_

#include <utility>
#include "lib/allocator/page_arena.h"
#include "lib/string/ob_sql_string.h"
#include "lib/container/ob_array_serialization.h"
#include "lib/mysqlclient/ob_mysql_proxy.h"
#include "ob_backup_struct.h"
#include "share/restore/ob_log_restore_source.h"
#include "share/backup/ob_backup_store.h"
#include "ob_log_restore_struct.h"

namespace oceanbase
{
namespace share
{

class ObBackupDest;
class ObBackupHelper;
class ObBackupFormatDesc;
class ObBackupConfigType final
{
public:
  enum Type {
    // if type >= DATA_BACKUP_DEST and type < LOG_ARCHIVE_DEST, config in __all_backup_parameters_table;
    // if type >= LOG_ARCHIVE_DEST and type < MAX_CONFIG_NAME, config in __all_log_archive_parameters_table;
    DATA_BACKUP_DEST = 0,
    LOG_ARCHIVE_DEST = 1,
    LOG_ARCHIVE_DEST_STATE = 2,
    LOG_ARCHIVE_DEST_1 = 3,
    LOG_ARCHIVE_DEST_STATE_1 = 4,
    LOG_ARCHIVE_DEST_2 = 5,
    LOG_ARCHIVE_DEST_STATE_2 = 6,
    LOG_ARCHIVE_DEST_3 = 7,
    LOG_ARCHIVE_DEST_STATE_3 = 8,
    LOG_ARCHIVE_DEST_4 = 9,
    LOG_ARCHIVE_DEST_STATE_4 = 10,
    LOG_ARCHIVE_DEST_5 = 11,
    LOG_ARCHIVE_DEST_STATE_5 = 12,
    LOG_ARCHIVE_DEST_6 = 13,
    LOG_ARCHIVE_DEST_STATE_6 = 14,
    LOG_ARCHIVE_DEST_7 = 15,
    LOG_ARCHIVE_DEST_STATE_7 = 16,
    LOG_ARCHIVE_DEST_8 = 17,
    LOG_ARCHIVE_DEST_STATE_8 = 18,
    LOG_RESTORE_SOURCE = 19,
    MAX_CONFIG_NAME
  };
  static const char *const type_str[Type::MAX_CONFIG_NAME];
public:
  ObBackupConfigType(): type_(Type::MAX_CONFIG_NAME) {}
  ObBackupConfigType(const ObBackupConfigType::Type &type): type_(type) {}
  ~ObBackupConfigType() {}
  ObBackupConfigType &operator = (const ObBackupConfigType &that) { if (this != &that) { type_ = that.type_; } return *this; }
  ObBackupConfigType &operator = (const ObBackupConfigType::Type &type) { type_ = type; return *this; }
  bool operator == (const ObBackupConfigType &other) const { return type_ == other.type_; }
  bool operator == (const ObBackupConfigType::Type &other) const { return type_ == other; }
  bool operator != (const ObBackupConfigType &other) const { return type_ != other.type_; }
  bool operator != (const ObBackupConfigType::Type &other) const { return type_ != other; }
  bool is_log_archive_dest_config() { return type_ >= Type::LOG_ARCHIVE_DEST && type_ < Type::MAX_CONFIG_NAME; }
  bool is_valid() const { return type_ >= Type::DATA_BACKUP_DEST && type_ < Type::MAX_CONFIG_NAME; }
  Type get_type() const { return type_; }
  int set_backup_config_type(const common::ObString& str);
  const char * get_backup_config_type_str();
  void reset() { type_ = Type::MAX_CONFIG_NAME; }
  TO_STRING_KV(K_(type));
private:
  Type type_;
};

struct BackupConfigItemPair final
{
  BackupConfigItemPair() : key_(), value_() {}
  ~BackupConfigItemPair() {}
  int assign(const BackupConfigItemPair &that);
  bool is_valid() const;
  void reset();
  int set_value(const int64_t &value);
  TO_STRING_KV(K_(key), K_(value));
  common::ObSqlString key_;
  common::ObSqlString value_;  
};

class ObIBackupConfigItemParser
{
public:
  ObIBackupConfigItemParser(const ObBackupConfigType::Type &type,  const uint64_t tenant_id) 
    : tenant_id_(tenant_id), type_(type), config_items_() {}
  virtual ~ObIBackupConfigItemParser() {}

  virtual int parse_from(const common::ObSqlString &value) = 0;
  virtual int update_inner_config_table(common::ObISQLClient &trans) = 0;
  virtual int check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans) = 0;
  virtual int check_before_update_inner_config(
    const bool for_verify,
    ObCompatibilityMode &compat_mode) { return OB_NOT_SUPPORTED; }
  virtual int get_compatibility_mode(common::ObCompatibilityMode &compatibility_mode) { return OB_NOT_SUPPORTED; }
  TO_STRING_KV(K_(tenant_id), K_(type), K_(config_items));
protected:
  uint64_t tenant_id_;
  ObBackupConfigType type_;
  common::ObSArray<BackupConfigItemPair> config_items_;
  DISALLOW_COPY_AND_ASSIGN(ObIBackupConfigItemParser);
};

class ObBackupConfigParserGenerator final
{
public:
  ObBackupConfigParserGenerator(): is_setted_(false), restore_source_type_(share::ObLogRestoreSourceType::INVALID),
  config_parser_(nullptr), allocator_() {}
  ~ObBackupConfigParserGenerator() { reset(); }
  int set(const ObBackupConfigType &type, const uint64_t tenant_id, const common::ObSqlString &value);
  void reset();
  ObIBackupConfigItemParser *&get_parser() { return config_parser_; }
private:
  int generate_parser_(const ObBackupConfigType &type, const uint64_t tenant_id);
  int set_restore_source_type_(const common::ObSqlString &value);
private:
  bool is_setted_;
  share::ObLogRestoreSourceType restore_source_type_;
  ObIBackupConfigItemParser *config_parser_;
  common::ObArenaAllocator allocator_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupConfigParserGenerator);
};

class ObBackupConfigChecker final
{ 
public:
  ObBackupConfigChecker() : type_() {}
  ~ObBackupConfigChecker() {}
  int check_config_name(const common::ObString &name, bool &is_backup_config);
private:
  ObBackupConfigType type_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupConfigChecker);
};

class ObBackupConfigParserMgr final
{
public:
  ObBackupConfigParserMgr();
  ~ObBackupConfigParserMgr() {}
  int init(const common::ObSqlString &name, const common::ObSqlString &value, const uint64_t tenant_id);
  int update_inner_config_table(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans);
  void reset();
  int only_check_before_update(ObCompatibilityMode &compat_mode);
private:
  bool is_inited_;
  ObBackupConfigParserGenerator parser_generator_; 
  DISALLOW_COPY_AND_ASSIGN(ObBackupConfigParserMgr);
};

class ObDataBackupDestConfigParser final : public ObIBackupConfigItemParser
{
public:
  ObDataBackupDestConfigParser(const uint64_t tenant_id): ObIBackupConfigItemParser(ObBackupConfigType::DATA_BACKUP_DEST, tenant_id) {}
  virtual ~ObDataBackupDestConfigParser() {}
  virtual int parse_from(const common::ObSqlString &value) override;
  virtual int update_inner_config_table(common::ObISQLClient &trans) override;
  virtual int check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans) override;
private:
  int check_doing_backup_(common::ObISQLClient &trans, bool &is_doing);
  int check_backup_dest_has_been_used_(bool &is_used);
  int update_data_backup_dest_config_(common::ObISQLClient &trans);
private:
  DISALLOW_COPY_AND_ASSIGN(ObDataBackupDestConfigParser);
};

class ObLogArchiveDestConfigParser : public ObIBackupConfigItemParser
{
public:
  ObLogArchiveDestConfigParser(const ObBackupConfigType::Type &type, const uint64_t tenant_id,  const int64_t dest_no)
    : ObIBackupConfigItemParser(type, tenant_id), dest_no_(dest_no), archive_dest_(), backup_dest_(), is_empty_(false) {}
  virtual ~ObLogArchiveDestConfigParser() {}
  virtual int parse_from(const common::ObSqlString &value) override;
  virtual int update_inner_config_table(common::ObISQLClient &trans) override;
  virtual int check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans) override;
  TO_STRING_KV(K_(dest_no), K_(archive_dest), K_(backup_dest), K_(is_empty));
protected:
  virtual int do_parse_sub_config_(const common::ObString &config_str);
  int do_parse_log_archive_dest_(const common::ObString &dest_type_str, const common::ObString &url);
  int do_parse_piece_switch_interval_(const common::ObString &name, const common::ObString &value);
  int do_parse_compression_(const common::ObString &name, const common::ObString &value);
  int do_parse_log_archive_mode_(const common::ObString &name, const common::ObString &value);
  int update_archive_dest_config_(common::ObISQLClient &trans);
protected:
  int64_t dest_no_;
  ObLogArchiveDestAtrr archive_dest_;
  share::ObBackupPathString backup_dest_;
  bool is_empty_;
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveDestConfigParser);
};

class ObLogArchiveDestStateConfigParser final : public ObLogArchiveDestConfigParser
{
public:
  ObLogArchiveDestStateConfigParser(const ObBackupConfigType::Type &type, const uint64_t tenant_id, const int64_t dest_no)
    : ObLogArchiveDestConfigParser(type, tenant_id, dest_no) {}
  virtual ~ObLogArchiveDestStateConfigParser() {}
  virtual int parse_from(const common::ObSqlString &value) override;
  virtual int update_inner_config_table(common::ObISQLClient &trans) override;
  virtual int check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans) override;
private:
  DISALLOW_COPY_AND_ASSIGN(ObLogArchiveDestStateConfigParser);
};

}
}

#endif /* OCEANBASE_SHARE_BACKUP_OB_BACKUP_CONFIG_H_ */