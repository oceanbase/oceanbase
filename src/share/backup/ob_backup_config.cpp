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

#define USING_LOG_PREFIX SHARE
#include "ob_backup_config.h"
#include "ob_log_restore_config.h"
#include "ob_backup_data_table_operator.h"
#include "ob_backup_helper.h"
#include "ob_archive_persist_helper.h"
#include "ob_backup_connectivity.h"
#include "share/backup/ob_backup_connectivity.h"
#include "share/backup/ob_tenant_archive_mgr.h"
#include "share/backup/ob_backup_clean_util.h"
#include "share/ob_rpc_struct.h"
#include "share/ob_license_utils.h"


using namespace oceanbase;
using namespace share;
using namespace common;

const char *const ObBackupConfigType::type_str[ObBackupConfigType::Type::MAX_CONFIG_NAME] = {
  "data_backup_dest",
  "log_archive_dest",
  "log_archive_dest_state",
  "log_archive_dest_1",
  "log_archive_dest_state_1",
  "log_archive_dest_2",
  "log_archive_dest_state_2",
  "log_archive_dest_3",
  "log_archive_dest_state_3",
  "log_archive_dest_4",
  "log_archive_dest_state_4",
  "log_archive_dest_5",
  "log_archive_dest_state_5",
  "log_archive_dest_6",
  "log_archive_dest_state_6",
  "log_archive_dest_7",
  "log_archive_dest_state_7",
  "log_archive_dest_8",
  "log_archive_dest_state_8",
  "log_restore_source",
};

int ObBackupConfigType::set_backup_config_type(const common::ObString& str) {
  int ret = OB_SUCCESS;
  char tmp_str[common::OB_MAX_CONFIG_NAME_LEN] = { 0 };
  if (str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(str));
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", str.length() ,str.ptr()))) {
    LOG_WARN("fail to set config value", K(ret), K(str));
  } else if (OB_FALSE_IT(str_tolower(tmp_str, strlen(tmp_str)))) {
  } else {
    Type tmp_type = Type::MAX_CONFIG_NAME;
    STATIC_ASSERT(Type::MAX_CONFIG_NAME == ARRAYSIZEOF(type_str), "types count mismatch");
    for (int64_t i = 0; i < ARRAYSIZEOF(type_str); i++) {
      if (0 == STRCMP(type_str[i], tmp_str)) {
        tmp_type = static_cast<Type>(i);
      }
    }
    if (Type::MAX_CONFIG_NAME == tmp_type) {
      ret = OB_ENTRY_NOT_EXIST;
    } else {
      type_ = tmp_type;
    }
  }
  return ret;
}

const char *ObBackupConfigType::get_backup_config_type_str()
{
  const char *str = "UNKNOWN";
  STATIC_ASSERT(Type::MAX_CONFIG_NAME == ARRAYSIZEOF(type_str), "types count mismatch");
  if (type_ < Type::DATA_BACKUP_DEST || type_ >= Type::MAX_CONFIG_NAME) {
    LOG_ERROR_RET(OB_INVALID_ARGUMENT, "invalid backup config type", K(type_));
  } else {
    str = type_str[type_];
  }
  return str;
}

int BackupConfigItemPair::assign(const BackupConfigItemPair &that)
{
  int ret = OB_SUCCESS;
  if (!that.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid backup config item", K(ret));
  } else if (OB_FAIL(key_.assign(that.key_))) {
    LOG_WARN("fail to assign key", K(ret), K(that.key_));
  } else if (OB_FAIL(value_.assign(that.value_))) {
    LOG_WARN("fail to assign key", K(ret), K(that.value_));
  }
  return ret;
}

bool BackupConfigItemPair::is_valid() const
{
  return key_.is_valid() && value_.is_valid();
}

void BackupConfigItemPair::reset()
{
  key_.reset();
  value_.reset();
}

int BackupConfigItemPair::set_value(const int64_t &value)
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(value_.assign_fmt("%ld", value))) {
    LOG_WARN("failed to set value", K(ret), K(value));
  }
  return ret;
}

int ObBackupConfigParserGenerator::set(const ObBackupConfigType &type, const uint64_t tenant_id, const common::ObSqlString &value)
{
  int ret = OB_SUCCESS;

  if (is_setted_) {
    ret = OB_INIT_TWICE;
    LOG_WARN("config parser generator has been setted", K(ret));
  } else if (!type.is_valid() || !is_valid_tenant_id(tenant_id)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(type), K(tenant_id));
  } else if (type.get_type() == ObBackupConfigType::LOG_RESTORE_SOURCE && OB_FAIL(ObLicenseUtils::check_standby_allowed())) {
    LOG_WARN("fail to check standby allowed, set log restore source is not allowed", KR(ret));
  } else if (nullptr != config_parser_) {
    config_parser_->~ObIBackupConfigItemParser();
    allocator_.free(config_parser_);
    config_parser_ = nullptr;
  }
  if (OB_SUCC(ret)) {
    if ((ObBackupConfigType::Type::LOG_RESTORE_SOURCE == type.get_type()) && OB_FAIL(set_restore_source_type_(value))) {
      LOG_WARN("fail to get restore source type");
    } else if (OB_FAIL(generate_parser_(type, tenant_id))) {
      LOG_WARN("fail to generate_parser", K(ret));
    }
  }
  return ret;
}

/*
As LOG_RESTORE_SOURCE supports two kind of type now, this function is to figure out
whether the LOG_RESTORE_SOURCE is SERVICE or LOCATION.
If the value is "SERVICE=127.0.0.1:1000;127.0.0.1:1001;127.0.0.1:1002 USER=ziqi_user@ziqi_tenant PASSWORD=123",
the restore_source_type_ will be parsed into SERVICE type.
If the value is "LOCATION=file:///data/1/zhaoyongheng.zyh/archivelog";
the restore_source_type_ will be parsed into LOCATION type.
*/
int ObBackupConfigParserGenerator::set_restore_source_type_(const common::ObSqlString &value)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  char *token = nullptr;
  char *tmp_token = nullptr;
  char *saveptr = nullptr;
  char *tmp_saveptr = nullptr;
  bool is_location = false;
  bool is_service = false;

  if (value.empty()) {
    restore_source_type_ = share::ObLogRestoreSourceType::LOCATION;
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", static_cast<int>(value.length()), value.ptr()))) {
    LOG_WARN("fail to set config value", K(value));
  } else {
    saveptr = tmp_str;
    for (char *str = saveptr; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, " ", &saveptr);
      if (nullptr == token) {
        break;
      } else if (OB_ISNULL(tmp_token = ::STRTOK_R(token, "=", &tmp_saveptr))) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("fail to split config str", K(ret), KP(tmp_token), K(tmp_saveptr));
      } else if (OB_FALSE_IT(str_tolower(tmp_token, strlen(tmp_token)))) {
      } else if (0 == STRCASECMP(tmp_token, OB_STR_LOCATION)) {
        is_location = true;
      } else if (0 == STRCASECMP(tmp_token, OB_STR_SERVICE)) {
        is_service = true;
      }
    }
    if (OB_FAIL(ret)) {
      LOG_WARN("fail to parse value", K(value));
    } else if ((is_location && is_service) || (!is_location && !is_service)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_WARN("fail to parse restore source type", K(ret), K(value));
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "log restore source type");
    } else if (is_location) {
      restore_source_type_ = share::ObLogRestoreSourceType::LOCATION;
    } else if (is_service) {
      restore_source_type_ = share::ObLogRestoreSourceType::SERVICE;
    }
    LOG_DEBUG("log restore source type", K(is_location), K(is_service));
  }
  return ret;
}

#define GENERATE_LOG_ARCHIVE_PARSER(parserClassName, channelNum, configType, tenantId)  \
    case ObBackupConfigType::Type::configType: {                                        \
      int64_t size = sizeof(ObLogArchive##parserClassName##ConfigParser);               \
      void *tmp_ptr = nullptr;                                                          \
      if (OB_ISNULL(tmp_ptr = (allocator_.alloc(size)))) {                              \
        ret = OB_ALLOCATE_MEMORY_FAILED;                                                \
        LOG_WARN("parser generator alloc memory failed", K(ret));                       \
      } else {                                                                          \
        config_parser_ = new(tmp_ptr) ObLogArchive##parserClassName##ConfigParser(ObBackupConfigType::Type::configType, tenantId, channelNum); \
      }                                                                                 \
      break;                                                                            \
    }

int ObBackupConfigParserGenerator::generate_parser_(const ObBackupConfigType &type, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  switch (type.get_type()) {
    case ObBackupConfigType::Type::DATA_BACKUP_DEST: {
      int64_t size = sizeof(ObDataBackupDestConfigParser);
      void *tmp_ptr = nullptr;
      if (OB_ISNULL(tmp_ptr = (allocator_.alloc(size)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("parser generator alloc memory failed", K(ret));
      } else {
        config_parser_ = new(tmp_ptr) ObDataBackupDestConfigParser(tenant_id);
      }
      break;
    }
    case ObBackupConfigType::Type::LOG_RESTORE_SOURCE: {
      if (share::ObLogRestoreSourceType::SERVICE == restore_source_type_) {
        int64_t size = sizeof(ObLogRestoreSourceServiceConfigParser);
        void *tmp_ptr = nullptr;
        if (OB_ISNULL(tmp_ptr = (allocator_.alloc(size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("parser generator alloc memory failed", K(ret));
        } else {
          config_parser_ = new(tmp_ptr) ObLogRestoreSourceServiceConfigParser(ObBackupConfigType::LOG_RESTORE_SOURCE, tenant_id);
        }
        break;
      } else if (share::ObLogRestoreSourceType::LOCATION == restore_source_type_) {
        int64_t size = sizeof(ObLogRestoreSourceLocationConfigParser);
        void *tmp_ptr = nullptr;
        if (OB_ISNULL(tmp_ptr = (allocator_.alloc(size)))) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("parser generator alloc memory failed", K(ret));
        } else {
          config_parser_ = new(tmp_ptr) ObLogRestoreSourceLocationConfigParser(ObBackupConfigType::LOG_RESTORE_SOURCE, tenant_id, 0);
        }
        break;
      }
    }
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 0, LOG_ARCHIVE_DEST, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 1, LOG_ARCHIVE_DEST_1, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 2, LOG_ARCHIVE_DEST_2, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 3, LOG_ARCHIVE_DEST_3, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 4, LOG_ARCHIVE_DEST_4, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 5, LOG_ARCHIVE_DEST_5, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 6, LOG_ARCHIVE_DEST_6, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 7, LOG_ARCHIVE_DEST_7, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(Dest, 8, LOG_ARCHIVE_DEST_8, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 0, LOG_ARCHIVE_DEST_STATE, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 1, LOG_ARCHIVE_DEST_STATE_1, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 2, LOG_ARCHIVE_DEST_STATE_2, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 3, LOG_ARCHIVE_DEST_STATE_3, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 4, LOG_ARCHIVE_DEST_STATE_4, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 5, LOG_ARCHIVE_DEST_STATE_5, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 6, LOG_ARCHIVE_DEST_STATE_6, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 7, LOG_ARCHIVE_DEST_STATE_7, tenant_id);
    GENERATE_LOG_ARCHIVE_PARSER(DestState, 8, LOG_ARCHIVE_DEST_STATE_8, tenant_id);
    default: {
      ret = OB_ERR_SYS;
      LOG_ERROR("invalid config type", K(ret), K(type));
    }
  }
  return ret;
}

void ObBackupConfigParserGenerator::reset()
{
  if (nullptr != config_parser_) {
    config_parser_->~ObIBackupConfigItemParser();
    allocator_.free(config_parser_);
    config_parser_ = nullptr;
  }
  is_setted_ = false;
}

int ObBackupConfigChecker::check_config_name(const common::ObString & name, bool &is_backup_config)
{
  int ret = OB_SUCCESS;
  type_.reset();
  is_backup_config = true;
  if (name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid argument", K(ret), K(name));
  } else if (OB_FAIL(type_.set_backup_config_type(name.ptr()))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      is_backup_config = false;
      ret = OB_SUCCESS;
    } else {
      LOG_WARN("fail to set backup config type", K(ret), K(name.ptr()));
    }
  }
  return ret;
}

ObBackupConfigParserMgr::ObBackupConfigParserMgr()
  : is_inited_(false),
    parser_generator_()
{
}

int ObBackupConfigParserMgr::init(const common::ObSqlString &name, const common::ObSqlString &value, const uint64_t tenant_id)
{
  int ret = OB_SUCCESS;
  parser_generator_.reset();
  share::ObBackupConfigChecker checker;
  share::ObBackupConfigType type;
  share::ObIBackupConfigItemParser *config_parser = nullptr;
  bool is_valid = false;
  if (name.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup config argumnet", K(ret), K(name));
  } else if (OB_FAIL(checker.check_config_name(name.ptr(), is_valid))) {
    LOG_WARN("fail to check backup config is valid", K(ret));
  } else if (!is_valid) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup config argumnet", K(ret), K(name), K(value));
  } else if (OB_FAIL(type.set_backup_config_type(name.ptr()))) {
    LOG_WARN("fail to set backup config type", K(ret), K(name));
#ifdef OB_BUILD_SHARED_STORAGE
  } else if (GCTX.is_shared_storage_mode()
             && (ObBackupConfigType::Type::DATA_BACKUP_DEST == type.get_type()
                 || ObBackupConfigType::Type::LOG_ARCHIVE_DEST == type.get_type())) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("set log_archive_dest/data_backup_dest in Shared-Storage mode is not supported", K(name), K(tenant_id));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set log_archive_dest/data_backup_dest in Shared-Storage mode is ");
#endif
  } else if (OB_FAIL(parser_generator_.set(type, tenant_id, value))) {
    LOG_WARN("fail to set backup parser generator", K(ret), K(type));
  } else if (OB_ISNULL(config_parser = parser_generator_.get_parser())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("config parser must not be nullptr", K(ret));
  } else if (OB_FAIL(config_parser->parse_from(value))) {
    LOG_WARN("fail to parse value", K(ret), K(type), K(value));
  } else {
    is_inited_ = true;
  }

  if (OB_FAIL(ret)) {
    reset();
  }
  return ret;
}

int ObBackupConfigParserMgr::update_inner_config_table(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObIBackupConfigItemParser *parser = nullptr;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup parser mgr not init", K(ret));
  } else if (OB_ISNULL(parser = parser_generator_.get_parser())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser must not be nullptr", K(ret));
  } else if (OB_FAIL(parser->check_before_update_inner_config(rpc_proxy, trans))) {
    LOG_WARN("fail to check before update inner config", K(ret));
  } else if (OB_FAIL(parser->update_inner_config_table(trans))) {
    LOG_WARN("fail to update inner config table", K(ret));
  }
  return ret;
}

int ObBackupConfigParserMgr::only_check_before_update(ObCompatibilityMode &compat_mode)
{
  int ret = OB_SUCCESS;
  ObIBackupConfigItemParser *parser = nullptr;
  compat_mode = ObCompatibilityMode::OCEANBASE_MODE;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("backup parser mgr not init", KR(ret));
  } else if (OB_ISNULL(parser = parser_generator_.get_parser())) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parser must not be nullptr", KR(ret));
  } else if (OB_FAIL(parser->check_before_update_inner_config(true /* for_verify */, compat_mode))) {
    LOG_WARN("fail to check_before_update_inner_config", KR(ret));
  }
  return ret;
}

void ObBackupConfigParserMgr::reset()
{
  parser_generator_.reset();
  is_inited_ = false;
}

int ObDataBackupDestConfigParser::parse_from(const common::ObSqlString &value)
{
  int ret = OB_SUCCESS;
  share::ObBackupDest backup_dest;
  share::BackupConfigItemPair pair;
  ObBackupPathString path;
  if (value.empty()) { // allow user to set backup_data_dest = "";
  } else if (OB_FAIL(backup_dest.set_without_decryption(value.string()))) {
    LOG_WARN("fail to set backup dest", K(ret));
  } else if (OB_FAIL(backup_dest.get_backup_dest_str(path.ptr(), path.capacity()))) {
    LOG_WARN("fail to get path", K(ret));
  } 
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(pair.key_.assign(type_.get_backup_config_type_str()))) {
    LOG_WARN("fail to assign backup config type str", K(ret));
  } else if (OB_FAIL(pair.value_.assign(value.ptr()))) {
    LOG_WARN("fail to assign backup dest root path", K(ret));
  } else if (OB_FAIL(config_items_.push_back(pair))) {
    LOG_WARN("fail to push backup item", K(ret));
  } else {
    LOG_INFO("parse from", K(value), K(backup_dest), K(path), K(config_items_));
  }
  return ret;
}


int ObDataBackupDestConfigParser::check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  bool is_doing = false;
  ObBackupDest dest;
  share::ObBackupPathString backup_dest;
  share::ObBackupStore store;
  int64_t dest_id = 0;
  ObBackupDestMgr dest_mgr;
  bool is_empty = true;
  bool is_cleaning = false;
  ObBackupDestType::TYPE dest_type = ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA;
  if (!type_.is_valid() || 1 != config_items_.count()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", K(ret), KPC(this));
  } else if (OB_FAIL(check_doing_backup_(trans, is_doing))) {
    LOG_WARN("fail to check doing backup", K(ret));
  } else if (is_doing) {
    ret = OB_BACKUP_IN_PROGRESS;
    LOG_WARN("backup is in progress, can't change backup dest", K(ret), KPC(this));
  } else if (!config_items_.at(0).value_.empty()) {
    ObBackupDest backup_dest_tmp;
    if (OB_FAIL(backup_dest_tmp.set(config_items_.at(0).value_.ptr()))) {
      LOG_WARN("fail to set backup dest", K(ret), K_(tenant_id), K_(config_items));
    } else if (OB_FAIL(ObBackupChangeExternalStorageDestUtil::get_extension_cleaning_status(
                       trans, tenant_id_, backup_dest_tmp, is_cleaning))) {
      LOG_WARN("fail to check backup dest exist", K(ret), K_(tenant_id), K_(config_items));
    } else if (is_cleaning) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("A backup cleaning is in progress, set it again is not allowed", K(ret), K_(tenant_id), K_(config_items));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "cleaning of this backup dest is in progress, set it again ");
    } else if (OB_FAIL(backup_dest.assign(config_items_.at(0).value_.ptr()))) {
      LOG_WARN("fail to assign backup dest", K(ret), K_(tenant_id), K_(config_items));
    } else if (OB_FAIL(ObIBackupConfigItemParser::set_default_checksum_type(backup_dest))) {
      LOG_WARN("fail to check dest checksum type", K(ret), K(backup_dest));
    } else if (OB_FAIL(dest.set(backup_dest))) {
      LOG_WARN("fail to set backup dest", K(ret));
    } else {
      const char *extension = dest.get_storage_info()->get_extension();
      char src_locality[OB_MAX_BACKUP_SRC_INFO_LENGTH] = { 0 };
      ObBackupSrcType src_type = ObBackupSrcType::EMPTY;
      if (OB_FAIL(ObBackupDestIOPermissionMgr::get_src_info_from_extension(ObString(extension),
                                                    src_locality, sizeof(src_locality), src_type))) {
        LOG_WARN("failed to get src info from extension", K(ret), K(extension));
      } else if (ObBackupSrcType::EMPTY != src_type
                    && OB_FAIL(ObBackupDestIOPermissionMgr::check_backup_src_info_valid(src_locality, src_type))) {
        LOG_WARN("please check backup src info valid", K(src_locality), K(src_type));
      } else if (OB_FAIL(dest_mgr.init(tenant_id_, dest_type, backup_dest, trans))) {
        LOG_WARN("fail to init dest manager", K(ret), K_(tenant_id), K(backup_dest));
      } else if (OB_FAIL(dest_mgr.check_dest_validity(rpc_proxy, false/*need_format_file*/))) {
        if (OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM == ret) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                          "set backup dest: parameter enable_worm=true is required for bucket with worm.");
      }
      LOG_WARN("fail to check dest validity", K(ret), K_(tenant_id), K(backup_dest));
      } else {
        LOG_INFO("succ to check data dest config", K_(tenant_id), K(backup_dest));
      }
    }
  }
  return ret;
}

int ObDataBackupDestConfigParser::update_data_backup_dest_config_(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  share::ObBackupHelper helper;
  share::ObBackupDest dest;
  ObBackupDestIOPermissionMgr *dest_io_permission_mgr = nullptr;
  char backup_dest_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  if (!type_.is_valid() || 1 != config_items_.count() ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", K(ret), KPC(this));
  } else if (!config_items_.at(0).value_.empty()) {
    if (OB_FAIL(dest.set(config_items_.at(0).value_.ptr()))) {
      LOG_WARN("fail to set backup dest", K(ret), K_(tenant_id), K_(config_items));
    } else if (OB_FAIL(dest.get_backup_dest_str(backup_dest_str, sizeof(backup_dest_str)))) {
      LOG_WARN("fail to get_backup_dest_str", K(ret), K(dest));
    }
  }
  //Locality info is only displayed in the __all_backup_storage_info table.
  //Delete locality info in backup dest str before updating the __all_backup_dest_parameter table.
  if (OB_FAIL(ret)) {
  } else if (OB_ISNULL(dest_io_permission_mgr = MTL(ObBackupDestIOPermissionMgr*))) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("MTL ObBackupDestIOPermissionMgr is null", K(ret));
  } else if (OB_FAIL(dest_io_permission_mgr->delete_locality_info_in_backup_dest_str(backup_dest_str))) {
    LOG_WARN("failed to delete locality info in backup dest str", K(ret), K(backup_dest_str));
  } else if (OB_FAIL(helper.init(tenant_id_, trans))) {
    LOG_WARN("fail to init backup help", K(ret), K(tenant_id_), K(dest));
  } else if (OB_FAIL(helper.set_backup_dest(backup_dest_str))) {
    LOG_WARN("fail to set backup dest", K(ret), K(backup_dest_str));
  }
  return ret;
} 

int ObDataBackupDestConfigParser::update_inner_config_table(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObBackupDestMgr dest_mgr;
  share::ObBackupPathString backup_dest;
  ObBackupDestType::TYPE dest_type = ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA;

  // TODO(wangxiaohui.wxh):4.3, handle trans failed after write format file in 4.1.
  if (!type_.is_valid() || 1 != config_items_.count() ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", K(ret), KPC(this));
  } else if (!config_items_.at(0).value_.empty()) {
    // allow set empty data backup dest
    if (OB_FAIL(backup_dest.assign(config_items_.at(0).value_.ptr()))) {
      LOG_WARN("fail to assign backup dest", K(ret), K_(tenant_id), K_(config_items));
    } else if (OB_FAIL(ObIBackupConfigItemParser::set_default_checksum_type(backup_dest))) {
      LOG_WARN("fail to check dest checksum type", K(ret), K(backup_dest));
    } else if (OB_FAIL(dest_mgr.init(tenant_id_, dest_type, backup_dest, trans))) {
      LOG_WARN("fail to init dest manager", K(ret), K_(tenant_id));
    } else if (OB_FAIL(dest_mgr.write_format_file())) {
      LOG_WARN("fail to assign backup dest", K(ret), K_(tenant_id));
    }
  }
  if (FAILEDx(update_data_backup_dest_config_(trans))) {
    LOG_WARN("fail to update data backup dest config", K(ret), K_(tenant_id));
  } else {
    LOG_INFO("succeed to set backup dest", K(ret), KPC(this));
  }
  return ret;
}

int ObDataBackupDestConfigParser::check_doing_backup_(common::ObISQLClient &trans, bool &is_doing)
{
  int ret = OB_SUCCESS;
  common::ObSArray<share::ObBackupJobAttr> job_attrs;
  is_doing = false;
  if (is_sys_tenant(tenant_id_)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("sys tenant no need to set backup dest", K(ret));
  } else if (OB_FAIL(share::ObBackupJobOperator::get_jobs(trans, tenant_id_, false/*no update*/, job_attrs))) {
    LOG_WARN("fail to get job_attrs", K(ret), K(tenant_id_));
  } else if (!job_attrs.empty()) {
    is_doing = true;
  }
  return ret;
}

int ObDataBackupDestConfigParser::check_backup_dest_has_been_used_(bool &is_used)
{
  int ret = OB_SUCCESS;
  return ret;
}

int ObLogArchiveDestConfigParser::parse_from(const common::ObSqlString &value)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
  char *token = nullptr;
  char *saveptr = nullptr;
  char *p_end = nullptr;
  is_empty_ = false;
  if (value.empty()) {
    // allow set empty archive dest
    is_empty_ = true;
  } else if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", static_cast<int>(value.length()), value.ptr()))) {
    LOG_WARN("fail to set config value", K(ret), K(value));
  } else {
    token = tmp_str;
    for (char *str = token; OB_SUCC(ret); str = nullptr) {
      token = ::STRTOK_R(str, " ", &saveptr);
      if (nullptr == token) {
        break;
      } else if (OB_FAIL(do_parse_sub_config_(token))) {
        LOG_WARN("fail to do parse log archve dest sub config", K(ret));
      }
    }
  }
  return ret;
}

int ObLogArchiveDestConfigParser::update_archive_dest_config_(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper helper;
  share::BackupConfigItemPair dest_id_pair;
  ObBackupDest dest;
  int64_t dest_id = 0;
  bool is_exist = false;
  bool is_running = false;
  if (OB_FAIL(helper.init(tenant_id_))) {
    LOG_WARN("fail to init backup help", K(ret), K(tenant_id_));
  } else if (OB_FAIL(helper.lock_archive_dest(trans, dest_no_, is_exist))) {
    LOG_WARN("fail to lock archive dest", K(ret), K_(dest_no));
  } else if (OB_FAIL(ObTenantArchiveMgr::is_archive_running(trans, tenant_id_, dest_no_, is_running))) {
    LOG_WARN("failed to check archive running.", K(ret), K_(backup_dest));
  } else if (is_running) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot change archive dest when archive is running.", K(ret), K_(backup_dest));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "change archive dest when archive is running is");
  } else if (is_empty_) {
    if (OB_FAIL(helper.del_dest(trans, dest_no_))) {
      LOG_WARN("fail to del dest", K(ret), K(dest_no_));
    }
  } else {
    if (OB_FAIL(dest.set(backup_dest_.ptr()))) {
      LOG_WARN("fail to set dest", K(ret));
    } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(trans, tenant_id_, dest, dest_id))) {
      LOG_WARN("fail to get dest id", K(ret)); 
    } else if (OB_FALSE_IT(archive_dest_.dest_id_ = dest_id)) {
    } else if (OB_FAIL(ObIBackupConfigItemParser::set_default_checksum_type(archive_dest_.dest_))) {
      LOG_WARN("fail to set default checksum type", K(ret), "backup_dest", archive_dest_.dest_);
    } else if (OB_FAIL(archive_dest_.gen_config_items(config_items_))) {
      LOG_WARN("fail to gen archive config items", K(ret)); 
    }


    // TODO(wangxiaohui.wxh):4.3, handle trans failed after write format file in 4.1.
    ARRAY_FOREACH_X(config_items_, i, cnt, OB_SUCC(ret)) {
      const BackupConfigItemPair &config_item = config_items_.at(i);
      if (OB_FAIL(helper.set_kv_item(trans, dest_no_, config_item.key_, config_item.value_))) {
        LOG_WARN("fail to set log archive dest state", K(ret), K(*this));
      }
    }
  }
  return ret;
}


int ObLogArchiveDestConfigParser::update_inner_config_table(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObBackupDestMgr dest_mgr;
  ObBackupDestType::TYPE dest_type = ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG; 
  if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", K(ret), KPC(this));
  } else if (!is_empty_) {
    if (OB_FAIL(dest_mgr.init(tenant_id_, dest_type, backup_dest_, trans))) {
      LOG_WARN("fail to init dest manager", K(ret), K_(tenant_id));
    } else if (OB_FAIL(dest_mgr.write_format_file())) {
      LOG_WARN("fail to write formate file", K(ret), K_(tenant_id));
    }
  } 
  
  if (OB_FAIL(ret)) {
  } else if (OB_FAIL(update_archive_dest_config_(trans))) {
    LOG_WARN("fail to update archive dest config", K(ret), K_(tenant_id)); 
  }
  return ret;
}

int ObLogArchiveDestConfigParser::check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObBackupDestType::TYPE dest_type = ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG;
  ObBackupDestMgr dest_mgr;
  ObBackupDest backup_dest;
  bool is_running = false;
  bool is_cleaning = false;
  if (is_empty_) {
  } else if (!type_.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", K(ret), KPC(this));
  } else if (backup_dest_.is_empty()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot set archive dest without location.", K(ret), K_(backup_dest));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "set archive dest without location is");
  } else if (OB_FAIL(ObTenantArchiveMgr::is_archive_running(trans, tenant_id_, dest_no_, is_running))) {
    LOG_WARN("failed to check archive running.", K(ret), K_(backup_dest));
  } else if (is_running) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("cannot change archive dest when archive is running.", K(ret), K_(backup_dest));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "change archive dest when archive is running is");
  } else if (OB_FAIL(ObIBackupConfigItemParser::set_default_checksum_type(backup_dest_))) {
    LOG_WARN("fail to check dest checksum type", K(ret));
  } else if (OB_FAIL(backup_dest.set(backup_dest_))) {
    LOG_WARN("fail to set backup dest", K(ret));
  } else if (OB_FAIL(ObBackupChangeExternalStorageDestUtil::get_extension_cleaning_status(
                      trans, tenant_id_, backup_dest, is_cleaning))) {
    LOG_WARN("fail to check backup dest exist", K(ret), K_(tenant_id), K_(backup_dest));
  } else if (is_cleaning) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("A backup cleaning is in progress, set it again is not allowed", K(ret), K_(tenant_id), K_(backup_dest));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "cleaning of this dest is in progress, set it again ");
  } else {
    omt::ObTenantConfigGuard tenant_config(TENANT_CONF(tenant_id_));
    const int64_t lag_target = tenant_config.is_valid() ? tenant_config->archive_lag_target : 0L;
    const char *extension = backup_dest.get_storage_info()->get_extension();
    char src_locality[OB_MAX_BACKUP_SRC_INFO_LENGTH] = { 0 };
    ObBackupSrcType src_type = ObBackupSrcType::EMPTY;
    if (OB_FAIL(ObBackupDestIOPermissionMgr::get_src_info_from_extension(ObString(extension),
                                                    src_locality, sizeof(src_locality), src_type))) {
      LOG_WARN("failed to get src info from extension", K(ret), K(extension));
    } else if (ObBackupSrcType::EMPTY != src_type
                  && OB_FAIL(ObBackupDestIOPermissionMgr::check_backup_src_info_valid(src_locality, src_type))) {
      LOG_WARN("please check backup src info valid", K(src_locality), K(src_type));
    } else if (backup_dest.is_storage_type_s3() && MIN_LAG_TARGET_FOR_S3 > lag_target) {
      ret = OB_OP_NOT_ALLOW;
      LOG_USER_ERROR(OB_OP_NOT_ALLOW, "archive_lag_target is smaller than 60s, set log_archive_dest to S3 is");
    } else if (OB_FAIL(dest_mgr.init(tenant_id_, dest_type, backup_dest_, trans))) {
      LOG_WARN("fail to update archive dest config", K(ret), K_(tenant_id));
    } else if (OB_FAIL(dest_mgr.check_dest_validity(rpc_proxy, false/*need_format_file*/))) {
      if (OB_OBJECT_STORAGE_OBJECT_LOCKED_BY_WORM == ret) {
        LOG_USER_ERROR(OB_INVALID_ARGUMENT,
                          "set backup dest: parameter enable_worm=true is required for bucket with worm.");
      }
      LOG_WARN("fail to update archive dest config", K(ret), K_(tenant_id));
    }
  }
  return ret;
}

int ObLogArchiveDestConfigParser::do_parse_sub_config_(const common::ObString &config_str)
{
  int ret = OB_SUCCESS;
  const char *target= nullptr;
  if (config_str.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty log archve dest sub config is not allowed", K(ret), K(config_str));
  } else {
    char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    char *token = nullptr;
    char *saveptr = nullptr;
    char *p_end = nullptr;
    if (OB_FAIL(databuff_printf(tmp_str, sizeof(tmp_str), "%.*s", config_str.length(), config_str.ptr()))) {
      LOG_WARN("fail to set config value", K(ret), K(config_str));
    } else if (OB_ISNULL(token = ::STRTOK_R(tmp_str, "=", &saveptr))) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("fail to split config str", K(ret), KP(token));
    } else if (OB_FALSE_IT(str_tolower(token, strlen(token)))) {
    } else if (0 == STRCASECMP(token, OB_STR_BINDING)) {
      if (OB_FAIL(do_parse_log_archive_mode_(token, saveptr))) {
        LOG_WARN("fail to do parse log archive mode", K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_LOCATION)) {
      if (OB_FAIL(do_parse_log_archive_dest_(token, saveptr))) {
        LOG_WARN("fail to do parse log archive dest", K(ret), K(token), K(saveptr));
      }
    } else if (0 == STRCASECMP(token, OB_STR_PIECE_SWITCH_INTERVAL)) {
      if (OB_FAIL(do_parse_piece_switch_interval_(token, saveptr))) {
        LOG_WARN("fail to do parse piece switch interval", K(ret), K(token), K(saveptr));
      }
    } else {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("log archive dest does not has this config", K(ret), K(token));
    }
  }
  return ret;
}

int ObLogArchiveDestConfigParser::do_parse_log_archive_mode_(const common::ObString &name, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log archve binding config", K(ret), K(name), K(value));
  } else if (OB_FAIL(archive_dest_.set_binding(value.ptr()))) {
    LOG_WARN("failed to set binding", K(ret), K(name), K(value));
  }
  return ret;
}

int ObLogArchiveDestConfigParser::do_parse_log_archive_dest_(const common::ObString &name, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log archve dest config", K(ret), K(value));
  } else if (OB_FAIL(backup_dest_.assign(value.ptr()))) {
    LOG_WARN("fail to set log archive dest", K(ret));
  } else if (OB_FAIL(archive_dest_.set_log_archive_dest(value))) {
    LOG_WARN("fail to set log archive dest", K(ret));
  }
  return ret;
}

int ObLogArchiveDestConfigParser::do_parse_piece_switch_interval_(const common::ObString &name, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  const int64_t MIN_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL = 24 * 3600 * 1000LL * 1000LL; //1d
  const int64_t MAX_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL = 7 * 24 * 3600 * 1000LL * 1000LL; //7d
  if (name.empty() || value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log archve dest config", K(ret), K(name), K(value));
  } else if (OB_FAIL(archive_dest_.set_piece_switch_interval(value.ptr()))) {
    LOG_WARN("fail to set piece switch interval", K(ret), K(value));
  } else if (!archive_dest_.is_piece_switch_interval_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("piece switch interval is not valid", K(ret), K(value));
  } else if (archive_dest_.piece_switch_interval_ > MAX_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL
            || archive_dest_.piece_switch_interval_ < MIN_LOG_ARCHIVE_PIECE_SWITCH_INTERVAL) {
#ifndef ERRSIM
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("invalid piece_switch_interval", K(ret), K(value));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "invalid piece_switch_interval out of range [1d,7d] is");
#endif
  }
  return ret;
}

int ObLogArchiveDestConfigParser::do_parse_compression_(const common::ObString &name, const common::ObString &value)
{
  int ret = OB_SUCCESS;
  if (name.empty() || value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log archve dest config", K(ret), K(name), K(value));
  } else if (0 == STRCASECMP(value.ptr(), OB_STR_ENABLE)
      || 0 == STRCASECMP(value.ptr(), OB_STR_DISABLE)) {
    share::BackupConfigItemPair pair;
    if (OB_FAIL(pair.key_.assign(name))) {
      LOG_WARN("fail to assign compression", K(ret), K(name));
    } else if (OB_FAIL(pair.value_.assign(value))) {
      LOG_WARN("fail to assign value", K(ret), K(value));
    } else if (OB_FAIL(config_items_.push_back(pair))) {
      LOG_WARN("fail to push back pair", K(ret), K(pair));
    }
  } else {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("compression not support value", K(ret), K(value));
  }
  return ret;
}

int ObLogArchiveDestStateConfigParser::parse_from(const common::ObSqlString &value)
{
  int ret = OB_SUCCESS;
  ObLogArchiveDestState dest_state;
  share::BackupConfigItemPair pair;
  if (value.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid log archve dest config", K(ret), K(value));
  } else if (OB_FAIL(dest_state.set_state(value.ptr()))) {
    LOG_WARN("fail to set piece switch interval", K(ret), K(value));
  } else if (!dest_state.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("dest state is not valid", K(ret), K(value));
  } else if (OB_FAIL(pair.key_.assign(OB_STR_STATE))) {
    LOG_WARN("fail to assign check point", K(ret), K(value));
  } else if (OB_FAIL(pair.value_.assign(value))) {
    LOG_WARN("fail to assign value", K(ret), K(value));
  } else if (OB_FAIL(config_items_.push_back(pair))) {
    LOG_WARN("fail to push back pair", K(ret), K(pair));
  }
  return ret;
}

int ObLogArchiveDestStateConfigParser::update_inner_config_table(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  share::ObArchivePersistHelper helper;
  if (!type_.is_valid() || 1 != config_items_.count() ) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid parser", K(ret), KPC(this));
  } else if (OB_FAIL(helper.init(tenant_id_))) {
    LOG_WARN("fail to init backup help", K(ret), K(tenant_id_));
  } else {
    const BackupConfigItemPair &config_item = config_items_.at(0);
    if (OB_FAIL(helper.set_kv_item(trans, dest_no_, config_item.key_, config_item.value_))) {
      LOG_WARN("fail to set log archive dest state", K(ret), K(*this));
    }
  }
  return ret;
}


int ObLogArchiveDestStateConfigParser::check_before_update_inner_config(obrpc::ObSrvRpcProxy &rpc_proxy, common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  // do nothing
  return ret;
}

int ObIBackupConfigItemParser::set_default_checksum_type(share::ObBackupPathString &backup_dest)
{
  int ret = OB_SUCCESS;
  char tmp_str[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };

  if (backup_dest.is_empty() || OB_MAX_BACKUP_DEST_LENGTH < backup_dest.size()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("backup dest is empty or too long", K(ret), "str_size", backup_dest.size());
  } else {
    if ( NULL != strstr(backup_dest.ptr(), CHECKSUM_TYPE)) { // user has specified checksum type
    } else {
      ObBackupDest tmp_dest;
      if (OB_FAIL(tmp_dest.set(backup_dest))) {
        LOG_WARN("fail to set tmp backup dest", K(ret));
      } else if (is_object_storage_type(tmp_dest.get_storage_info()->get_type())) {
        int64_t pos = 0;
        if (OB_FAIL(databuff_printf(tmp_str, OB_MAX_BACKUP_DEST_LENGTH, pos, "%s%s%s%s",
                                    backup_dest.ptr(), "&", CHECKSUM_TYPE, CHECKSUM_TYPE_MD5))) {
          LOG_WARN("fail to databuff printf", K(ret));
        } else if (OB_FAIL(backup_dest.assign(tmp_str))) {
          LOG_WARN("fail to assign backup dest", K(ret));
        }
      }
    }
  }
  return ret;
}

int ObIBackupConfigItemParser::set_default_checksum_type(ObBackupDest &backup_dest)
{
  int ret = OB_SUCCESS;
  if (!backup_dest.is_valid()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid backup dest", K(ret), K(backup_dest));
  } else {
    char buf[OB_MAX_BACKUP_DEST_LENGTH] = { 0 };
    ObBackupPathString backup_dest_str;
    if (OB_FAIL(backup_dest.get_backup_dest_str(buf, OB_MAX_BACKUP_DEST_LENGTH))) {
      LOG_WARN("fail to get backup dest str", K(ret), K(backup_dest));
    } else if (OB_FAIL(backup_dest_str.assign(buf))) {
      LOG_WARN("fail to assign backup dest str", K(ret), K(backup_dest));
    } else if (OB_FAIL(set_default_checksum_type(backup_dest_str))) {
      LOG_WARN("fail to set default checksum type", K(ret), K(backup_dest));
    } else if (OB_FALSE_IT(backup_dest.reset())) {
    } else if (OB_FAIL(backup_dest.set(backup_dest_str))) {
      LOG_WARN("fail to set backup dest", K(ret), K(backup_dest_str));
    }
  }
  return ret;
}

ObChangeExternalStorageDestMgr::ObChangeExternalStorageDestMgr()
  : is_inited_(false),
    tenant_id_(OB_INVALID_TENANT_ID),
    dest_id_(OB_INVALID_DEST_ID),
    dest_type_(ObBackupDestType::TYPE::DEST_TYPE_MAX),
    sql_proxy_(NULL),
    backup_dest_(),
    change_option_(),
    change_access_info_(false)
{
}

void ObChangeExternalStorageDestMgr::reset()
{
  is_inited_ = false;
  tenant_id_ = OB_INVALID_TENANT_ID;
  dest_id_ = OB_INVALID_DEST_ID;
  dest_type_ = ObBackupDestType::TYPE::DEST_TYPE_MAX;
  sql_proxy_ = NULL;
  backup_dest_.reset();
  change_option_.reset();
  change_access_info_ = false;
}

int ObChangeExternalStorageDestMgr::init(
    const uint64_t tenant_id,
    const common::ObFixedLengthString<common::OB_MAX_CONFIG_VALUE_LEN> &path,
    common::ObISQLClient &sql_proxy)
{
  int ret = OB_SUCCESS;
  ObBackupPathString backup_path;

  if (IS_INIT) {
    ret = OB_INIT_TWICE;
    LOG_WARN("ChangeExternalStorageDest init twice.", K(ret));
  } else if (OB_INVALID_TENANT_ID == tenant_id || path.is_empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid dest", K(ret), K(path));
  } else if (OB_FAIL(backup_dest_.set_storage_path(path.ptr()))) {
    LOG_WARN("failed to set backup dest", K(ret), K(path));
  } else if (OB_FAIL(backup_dest_.get_backup_path_str(backup_path.ptr(), backup_path.capacity()))) {
    LOG_WARN("fail to get backup path str", K(ret), K(tenant_id));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_backup_dest(sql_proxy, tenant_id, backup_path, backup_dest_))) {
    if (OB_ENTRY_NOT_EXIST == ret) {
      LOG_USER_ERROR(OB_ENTRY_NOT_EXIST, "the path does not exist");
    }
    LOG_WARN("failed to get backup dest", K(ret), K(tenant_id), K(backup_path));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_type(sql_proxy, tenant_id, backup_dest_, dest_type_))) {
    LOG_WARN("failed to get dest type", K(ret), K(tenant_id), K(backup_dest_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::get_dest_id(sql_proxy, tenant_id, backup_dest_, dest_id_))) {
    LOG_WARN("failed to get dest id", K(ret), K(tenant_id), K(backup_dest_));
  } else {
    tenant_id_ = tenant_id;
    sql_proxy_ = &sql_proxy;
    change_option_.reset();
    change_access_info_ = false;
    is_inited_ = true;
  }

  return ret;
}

//Updates backup_dest_ with new access_id and access_key, and validates the access permissions of new ak&sk.
int ObChangeExternalStorageDestMgr::update_and_validate_authorization(const char *access_id, const char *access_key)
{
  int ret = OB_SUCCESS;
  ObBackupDestMgr dest_mgr;
  ObBackupPathString backup_dest_str;
  obrpc::ObSrvRpcProxy *rpc_proxy = nullptr;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChangeExternalStorageDestMgr not init", K(ret));
  } else if (OB_ISNULL(rpc_proxy = GCTX.srv_rpc_proxy_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("rpc_proxy should not be NULL", K(ret), KP(rpc_proxy));
  } else if (OB_FAIL(update_backup_dest_authorization_(access_id, access_key))) {
    LOG_WARN("failed to update backup dest authorization", K(ret), KCSTRING(access_id));
  } else if (OB_FAIL(backup_dest_.get_backup_dest_str(backup_dest_str.ptr(), backup_dest_str.capacity()))) {
    LOG_WARN("fail to get backup dest str", K(ret));
  } else if (OB_FAIL(dest_mgr.init(tenant_id_, dest_type_, backup_dest_str, *sql_proxy_))) {
    LOG_WARN("failed to init dest mgr", K(ret), K(tenant_id_), K(backup_dest_str));
  } else if (OB_FAIL(dest_mgr.check_dest_validity(*rpc_proxy, true/*need_format_file*/))) {
    LOG_WARN("fail to check archive dest validity", K(ret), K(tenant_id_), K(backup_dest_str));
  } else {
    LOG_INFO("succeed to check archive dest validity", K(tenant_id_), K(backup_dest_str));
  }

  return ret;
}

//updates dest attr in __all_backup_parameter_table, __all_log_archive_dest_parameter and __all_backup_storage_info.
int ObChangeExternalStorageDestMgr::update_backup_dest_authorization_(const char *access_id, const char *access_key)
{
  int ret = OB_SUCCESS;

  if (OB_ISNULL(access_id) || OB_ISNULL(access_key) || 0 >= strlen(access_id) || 0 >= strlen(access_key)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("access_id or access_key is null", K(ret), KP(access_id));
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "reset ak/sk, access_id or access_key is null");
  } else if (!backup_dest_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup dest is not valid", K(ret), K(backup_dest_));
  } else if (backup_dest_.is_storage_type_file()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("nfs path not support reset ak/sk", K(ret), K(backup_dest_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "reset ak/sk of nfs path");
  } else if (backup_dest_.is_assume_role_mode()) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("assume role mode not support reset ak/sk", K(ret), K(backup_dest_));
    LOG_USER_ERROR(OB_NOT_SUPPORTED, "reset ak/sk of path with assume role mode ");
  } else if (OB_FAIL(backup_dest_.reset_access_id_and_access_key(access_id, access_key))) {
    LOG_WARN("failed to reset access id and access key", K(ret), KCSTRING(access_id));
  }

  return ret;
}

int ObChangeExternalStorageDestMgr::update_backup_parameter_(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObBackupHelper backup_helper;
  ObBackupPathString backup_dest_str;
  ObBackupDest dest;
  bool is_equal = false;

  if (OB_FAIL(backup_helper.init(tenant_id_, trans))) {
    LOG_WARN("fail to init backup helper", K(ret), K(tenant_id_));
  } else if (OB_FAIL(backup_helper.get_backup_dest(backup_dest_str))) {
    LOG_WARN("fail to get backup dest", K(ret), K(tenant_id_));
  } else if (backup_dest_str.is_empty()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup dest is empty", K(ret), K(tenant_id_));
  } else if (OB_FAIL(dest.set(backup_dest_str.ptr()))) {
    LOG_WARN("fail to set backup dest", K(ret), K(tenant_id_));
  } else if (OB_FAIL(dest.is_backup_path_equal(backup_dest_, is_equal))) {
    LOG_WARN("fail to check backup dest equal", K(ret));
  } else if (is_equal) {
    backup_dest_str.reset();
    if (OB_FAIL(backup_dest_.get_backup_dest_str(backup_dest_str.ptr(), backup_dest_str.capacity()))) {
      LOG_WARN("fail to get backup dest str", K(ret), K(tenant_id_));
    } else if (OB_FAIL(backup_helper.set_backup_dest(backup_dest_str))) {
      LOG_WARN("fail to set backup dest", K(ret), K(backup_dest_str));
    }
  }

  return ret;
}

int ObChangeExternalStorageDestMgr::update_archive_parameter_(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  ObArchivePersistHelper archive_helper;
  // Only one dest is supported.
  const int64_t dest_no = 0;
  const bool need_lock = true;
  int64_t dest_id = OB_INVALID_DEST_ID;
  ObBackupPathString archive_dest_str;
  ObSqlString key;
  ObSqlString value;

  if (OB_FAIL(archive_helper.init(tenant_id_))) {
    LOG_WARN("fail to init archive helper", K(ret), K(tenant_id_));
  } else if (OB_FAIL(archive_helper.get_dest_id(trans, need_lock, dest_no, dest_id))) {
    LOG_WARN("fail to get archive dest id", K(ret), K(tenant_id_));
  } else if (dest_id == dest_id_) {
    if (OB_FAIL(backup_dest_.get_backup_dest_str(archive_dest_str.ptr(), archive_dest_str.capacity()))) {
      LOG_WARN("fail to get backup dest str", K(ret), K(tenant_id_));
    } else if (OB_FAIL(key.assign(OB_STR_PATH))) {
      LOG_WARN("failed to assign key", K(ret));
    } else if (OB_FAIL(value.assign(archive_dest_str.ptr()))) {
      LOG_WARN("failed to assign value", K(ret));
    } else if (OB_FAIL(archive_helper.set_kv_item(trans, dest_no, key, value))) {
      LOG_WARN("fail to reset log archive dest", K(ret));
    }
  }

  return ret;
}

int ObChangeExternalStorageDestMgr::update_inner_table_authorization(common::ObISQLClient &trans)
{
  int ret = OB_SUCCESS;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChangeExternalStorageDestMgr not init", K(ret));
  } else if (!backup_dest_.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("backup dest is not valid", K(ret), K(backup_dest_));
  } else if (OB_FAIL(ObBackupStorageInfoOperator::update_backup_authorization(trans, tenant_id_, backup_dest_))) {
    LOG_WARN("failed to update backup authorization", K(ret), K(tenant_id_), K(backup_dest_));
  } else if (ObBackupDestType::TYPE::DEST_TYPE_BACKUP_DATA == dest_type_) {
    if (OB_FAIL(update_backup_parameter_(trans))) {
      LOG_WARN("failed to update backup parameter table", K(ret), K(tenant_id_), K(backup_dest_));
    }
  } else if (ObBackupDestType::TYPE::DEST_TYPE_ARCHIVE_LOG == dest_type_) {
    if (OB_FAIL(update_archive_parameter_(trans))) {
      LOG_WARN("failed to update log archive dest table", K(ret), K(tenant_id_), K(backup_dest_));
    }
  }
  return ret;
}

int ObChangeExternalStorageDestMgr::set_authorization(const common::ObString &access_info)
{
  int ret = OB_SUCCESS;
  ObBackupDestAttribute access_info_option;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChangeExternalStorageDestMgr not init", K(ret));
  } else if (access_info.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("access_info is empty", K(ret));
  } else if (OB_FAIL(ObBackupDestAttributeParser::parse_access_info(access_info, access_info_option))) {
    LOG_WARN("failed to parse access_info", K(ret), K(access_info));
  } else if (OB_FAIL(update_and_validate_authorization(
                    access_info_option.access_id_, access_info_option.access_key_))) {
    LOG_WARN("failed to reset access id and access key", K(ret), K(access_info_option));
  } else {
    change_access_info_ = true;
  }

  return ret;
}

int ObChangeExternalStorageDestMgr::change_external_storage_dest()
{
  int ret = OB_SUCCESS;
  ObMySQLTransaction trans;
  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChangeExternalStorageDestMgr not init", KR(ret));
  } else if (OB_FAIL(trans.start(sql_proxy_, gen_meta_tenant_id(tenant_id_)))) {
    LOG_WARN("failed to start trans", KR(ret), K_(tenant_id));
  } else if (change_access_info_ && OB_FAIL(update_inner_table_authorization(trans))) {
    LOG_WARN("failed to update inner table authorization", KR(ret), K_(tenant_id));
  } else if (change_option_.change_qosattr()) {
    if (OB_FAIL(ObBackupStorageInfoOperator::update_backup_dest_attribute(trans, tenant_id_, backup_dest_,
                                                    change_option_.max_iops_, change_option_.max_bandwidth_))) {
      LOG_WARN("failed to update backup dest attribute", KR(ret), K_(tenant_id));
    }
  }

  if (OB_SUCC(ret) && change_option_.change_src_info()) {
    if (OB_FAIL(ObBackupChangeExternalStorageDestUtil::change_src_info(trans, gen_user_tenant_id(tenant_id_),
                                                                          change_option_, backup_dest_))) {
      LOG_WARN("failed to change src info", KR(ret), K_(tenant_id));
    }
  }

  if (trans.is_started()) {
    int tmp_ret = OB_SUCCESS;
    if (OB_TMP_FAIL(trans.end(OB_SUCC(ret)))) {
      LOG_WARN("trans end failed", "is_commit", OB_SUCCESS == ret, K(tmp_ret));
      ret = COVER_SUCC(tmp_ret);
    }
  }
  return ret;
}

int ObChangeExternalStorageDestMgr::set_attribute(const common::ObString &attribute)
{
  int ret = OB_SUCCESS;

  if (IS_NOT_INIT) {
    ret = OB_NOT_INIT;
    LOG_WARN("ObChangeExternalStorageDestMgr not init", K(ret));
  } else if (attribute.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("attribute is empty", K(ret));
  } else if (OB_FAIL(ObBackupDestAttributeParser::parse(attribute, change_option_))) {
    LOG_WARN("failed to parse attribute", K(ret), K(attribute));
  }

  return ret;
}

int ObBackupConfigUtil::admin_set_backup_config(
    common::ObMySQLProxy &sql_proxy,
    obrpc::ObSrvRpcProxy &rpc_proxy,
    share::schema::ObMultiVersionSchemaService &schema_service,
    const obrpc::ObAdminSetConfigArg &arg)
{
  int ret = OB_SUCCESS;
  if (!arg.is_valid()) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid backup config arg", K(ret));
  } else if (!arg.is_backup_config_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("admin set config type not backup config", K(ret), K(arg));
  }
  share::BackupConfigItemPair config_item;
  share::ObBackupConfigParserMgr config_parser_mgr;
  ARRAY_FOREACH_X(arg.items_, i , cnt, OB_SUCC(ret)) {
    const obrpc::ObAdminSetConfigItem &item = arg.items_.at(i);
    uint64_t exec_tenant_id = OB_INVALID_TENANT_ID;
    ObMySQLTransaction trans;
    config_parser_mgr.reset();
    if ((common::is_sys_tenant(item.exec_tenant_id_) && item.tenant_name_.is_empty())
        || (common::is_user_tenant(item.exec_tenant_id_) && !item.tenant_name_.is_empty())
        || common::is_meta_tenant(item.exec_tenant_id_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("backup config only support user tenant", K(ret));
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "backup config only support user tenant");
    } else if (!item.tenant_name_.is_empty()) {
      schema::ObSchemaGetterGuard guard;
      if (OB_FAIL(schema_service.get_tenant_schema_guard(OB_SYS_TENANT_ID, guard))) {
        LOG_WARN("fail to get tenant schema guard", K(ret));
      } else if (OB_FAIL(guard.get_tenant_id(ObString(item.tenant_name_.ptr()), exec_tenant_id))) {
        LOG_WARN("fail to get tenant id", K(ret));
      }
    } else {
      exec_tenant_id = item.exec_tenant_id_;
    }

    if (OB_FAIL(ret)) {
    } else if (OB_FAIL(trans.start(&sql_proxy, gen_meta_tenant_id(exec_tenant_id)))) {
      LOG_WARN("fail to start trans", K(ret));
    } else {
      // Only lock policy table and check log_only policy when setting backup_dest
      bool need_lock_and_check = (0 == strcmp(item.name_.ptr(), share::OB_STR_DATA_BACKUP_DEST));
      if (need_lock_and_check) {
        bool exists = false;
        if (OB_FAIL(ObBackupCleanUtil::lock_policy_table_then_check(trans, exec_tenant_id, exists, true/*log_only*/))) {
          LOG_WARN("fail to check log only policy exist", K(ret));
        } else if (exists) {
          ret = OB_BACKUP_DEST_NOT_ALLOWED_TO_SET;
          LOG_WARN("log_only policy exists, cannot set backup_dest", K(ret), K(exec_tenant_id));
        }
      }

      if (OB_SUCC(ret)) {
        common::ObSqlString name;
        common::ObSqlString value;
        if (OB_FAIL(name.assign(item.name_.ptr()))) {
          LOG_WARN("fail to assign name", K(ret));
        } else if (OB_FAIL(value.assign(item.value_.ptr()))) {
          LOG_WARN("fail to assign value", K(ret));
        } else if (OB_FAIL(config_parser_mgr.init(name, value, exec_tenant_id))) {
          LOG_WARN("fail to init backup config parser mgr", K(ret), K(item));
        } else if (OB_FAIL(config_parser_mgr.update_inner_config_table(rpc_proxy, trans))) {
          LOG_WARN("fail to update inner config table", K(ret));
        }
      }

      if (OB_SUCC(ret)) {
        if (OB_FAIL(trans.end(true))) {
          LOG_WARN("fail to commit trans", K(ret));
        }
      } else {
        int tmp_ret = OB_SUCCESS;
        if (OB_SUCCESS != (tmp_ret = trans.end(false))) {
          LOG_WARN("fail to rollback trans", K(tmp_ret));
        }
      }
    }
  }
  return ret;
}