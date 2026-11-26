/**
 * Copyright (c) 2026 OceanBase
 * SPDX-License-Identifier: Apache-2.0
 */

#define USING_LOG_PREFIX SHARE
#include <gtest/gtest.h>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>
#include <type_traits>
#include <memory>
#include <cstdlib>
#include <unistd.h>
#include "share/config/ob_server_config.h"
#include "observer/omt/ob_tenant_config.h"
#define private public
#include "lib/restore/ob_storage_info.h"
#include "rootserver/backup/ob_backup_param_operator.h"
#undef private
#include "lib/container/ob_fixed_array.h"
#include "lib/json/ob_json.h"
#include "storage/blocksstable/ob_data_buffer.h"
#define private public
#include "storage/backup/ob_backup_file_writer_ctx.h"
#include "rootserver/restore/ob_restore_util.h"
#undef private
#include "share/ob_dml_sql_splicer.h"
#include "share/ob_rpc_struct.h"
#include "share/restore/ob_physical_restore_info.h"
#include "share/restore/ob_restore_persist_helper.h"
#include "share/backup/ob_backup_config.h"
#include "share/backup/ob_backup_helper.h"
#include "share/backup/ob_archive_persist_helper.h"
#include "storage/backup/ob_backup_data_store.h"
#include "sql/resolver/cmd/ob_alter_system_stmt.h"
#include "sql/plan_cache/ob_prepare_stmt_struct.h"
#include "sql/ob_sql_context.h"
#include "storage/backup/ob_backup_index_cache.h"
#include "storage/backup/ob_backup_meta_cache.h"
#include "share/restore/ob_import_table_struct.h"
#include "share/backup/ob_log_restore_struct.h"
#include "share/restore/ob_log_restore_source_mgr.h"
#include "logservice/restoreservice/ob_log_archive_piece_mgr.h"
#include "sql/resolver/cmd/ob_alter_system_resolver.h"
#include "share/backup/ob_log_restore_config.h"
#include "share/backup/ob_backup_connectivity.h"
#include "logservice/restoreservice/ob_log_restore_define.h"
#include "observer/virtual_table/ob_all_virtual_restore_job.h"
#include "share/ob_i_tablet_scan.h"
#include "share/inner_table/ob_inner_table_schema.h"
#include "share/schema/ob_schema_getter_guard.h"
#include "share/restore/ob_restore_uri_parser.h"
#ifdef OB_BUILD_TDE_SECURITY
#include "share/ob_encrypt_kms.h"
#include "share/ob_master_key_getter.h"
#endif

using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::share;
using namespace oceanbase::backup;
using namespace oceanbase::storage;
using namespace oceanbase::obrpc;
using namespace oceanbase::sql;

namespace
{
const char *const SECRET = "F65_FAKE_SECRET_20261030";

template <typename T>
std::string format(const T &value)
{
  char buf[32768] = {};
  const int64_t size = value.to_string(buf, sizeof(buf));
  std::string result(buf, size);
  return result;
}

std::string read_log()
{
  std::ifstream file("test_backup_sensitive_logging.log");
  std::string result((std::istreambuf_iterator<char>(file)), std::istreambuf_iterator<char>());
  std::ifstream rs_file("test_backup_sensitive_logging.rs.log");
  result.append(std::istreambuf_iterator<char>(rs_file), std::istreambuf_iterator<char>());
  return result;
}

struct SensitiveHolder
{
  ObSensitiveFixedLengthString<OB_MAX_PASSWORD_LENGTH> passwd_;
  TO_STRING_KV(K_(passwd));
};

TEST(BackupSensitiveLogging, sensitive_string_storage_and_logging)
{
  SensitiveHolder holder;
  ASSERT_EQ(OB_SUCCESS, holder.passwd_.assign(SECRET));
  EXPECT_STREQ(SECRET, holder.passwd_.ptr());
  EXPECT_EQ("<redacted>", format(holder.passwd_));
  EXPECT_EQ(std::string::npos, format(holder).find(SECRET));
  EXPECT_NE(std::string::npos, format(holder).find("<redacted>"));
  SensitiveHolder copied = holder;
  EXPECT_EQ(holder.passwd_, copied.passwd_);
  copied.passwd_.reset();
  EXPECT_TRUE(copied.passwd_.is_empty());
  EXPECT_STREQ(SECRET, holder.passwd_.ptr());
  copied = holder;
  EXPECT_EQ(holder.passwd_, copied.passwd_);
  LOG_INFO("sensitive object logging", K(holder.passwd_), K(holder), KPC(&holder));
  const std::string oversized = std::string(SECRET) + std::string(OB_MAX_PASSWORD_LENGTH, 'x');
  ObSensitiveFixedLengthString<OB_MAX_PASSWORD_LENGTH> truncated(oversized.c_str());
  EXPECT_EQ(OB_MAX_PASSWORD_LENGTH - 1, truncated.size());
  EXPECT_EQ(OB_BUF_NOT_ENOUGH, truncated.assign(ObString::make_string(oversized.c_str())));
  EXPECT_EQ("<redacted>", format(truncated));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("sensitive object logging"));
  EXPECT_NE(std::string::npos, log.find("<redacted>"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

TEST(BackupSensitiveLogging, sensitive_string_wire_compatibility)
{
  const std::string values[] = {"", SECRET, std::string(OB_MAX_PASSWORD_LENGTH - 1, 'x')};
  for (const std::string &value : values) {
    ObFixedLengthString<OB_MAX_PASSWORD_LENGTH> ordinary;
    ObSensitiveFixedLengthString<OB_MAX_PASSWORD_LENGTH> sensitive;
    ASSERT_EQ(OB_SUCCESS, ordinary.assign(value.c_str()));
    ASSERT_EQ(OB_SUCCESS, sensitive.assign(ordinary));
    ASSERT_EQ(ordinary.get_serialize_size(), sensitive.get_serialize_size());
    std::vector<char> old_wire(ordinary.get_serialize_size());
    std::vector<char> new_wire(sensitive.get_serialize_size());
    int64_t old_pos = 0;
    int64_t new_pos = 0;
    ASSERT_EQ(OB_SUCCESS, ordinary.serialize(old_wire.data(), old_wire.size(), old_pos));
    ASSERT_EQ(OB_SUCCESS, sensitive.serialize(new_wire.data(), new_wire.size(), new_pos));
    EXPECT_EQ(old_pos, new_pos);
    EXPECT_EQ(old_wire, new_wire);
    ordinary.reset();
    sensitive.reset();
    old_pos = 0;
    new_pos = 0;
    ASSERT_EQ(OB_SUCCESS, ordinary.deserialize(new_wire.data(), new_wire.size(), old_pos));
    ASSERT_EQ(OB_SUCCESS, sensitive.deserialize(old_wire.data(), old_wire.size(), new_pos));
    EXPECT_STREQ(value.c_str(), ordinary.ptr());
    EXPECT_STREQ(value.c_str(), sensitive.ptr());
  }
}

TEST(BackupSensitiveLogging, sensitive_view_wire_compatibility)
{
  static_assert(!std::is_convertible<ObSensitiveStringView, ObString>::value, "raw values must be explicit");
  const std::string values[] = {"", SECRET, std::string(OB_MAX_PASSWORD_LENGTH, 'x'),
      std::string("pool_list=pool_a&method=") + SECRET};
  for (const std::string &value : values) {
    ObString ordinary = ObString::make_string(value.c_str());
    ObSensitiveStringView sensitive(ordinary);
    EXPECT_EQ("<redacted>", format(sensitive));
    EXPECT_EQ(ordinary.ptr(), sensitive.ptr());
    ASSERT_EQ(ordinary.get_serialize_size(), sensitive.get_serialize_size());
    std::vector<char> old_wire(ordinary.get_serialize_size());
    std::vector<char> new_wire(sensitive.get_serialize_size());
    int64_t old_pos = 0;
    int64_t new_pos = 0;
    ASSERT_EQ(OB_SUCCESS, ordinary.serialize(old_wire.data(), old_wire.size(), old_pos));
    ASSERT_EQ(OB_SUCCESS, sensitive.serialize(new_wire.data(), new_wire.size(), new_pos));
    EXPECT_EQ(old_wire, new_wire);
    EXPECT_EQ(serialization::encoded_length(ordinary), serialization::encoded_length(sensitive));
    old_pos = 0;
    new_pos = 0;
    ASSERT_EQ(OB_SUCCESS, serialization::encode(old_wire.data(), old_wire.size(), old_pos, ordinary));
    ASSERT_EQ(OB_SUCCESS, serialization::encode(new_wire.data(), new_wire.size(), new_pos, sensitive));
    EXPECT_EQ(old_pos, new_pos);
    EXPECT_EQ(old_wire, new_wire);
    ordinary.reset();
    sensitive.reset();
    old_pos = 0;
    new_pos = 0;
    ASSERT_EQ(OB_SUCCESS, ordinary.deserialize(new_wire.data(), new_wire.size(), old_pos));
    ASSERT_EQ(OB_SUCCESS, sensitive.deserialize(old_wire.data(), old_wire.size(), new_pos));
    EXPECT_EQ(0, ordinary.compare(ObString::make_string(value.c_str())));
    EXPECT_EQ(0, sensitive.str().compare(ordinary));
  }
}

TEST(BackupSensitiveLogging, restore_job_preserves_context_and_owns_copied_secrets)
{
  ObPhysicalRestoreJob job;
  job.set_tenant_id(1002);
  ASSERT_EQ(OB_SUCCESS, job.set_tenant_name(ObString::make_string("restore_diagnostic_tenant")));
  std::string input(SECRET);
  ASSERT_EQ(OB_SUCCESS, job.set_restore_option(ObString::make_string(input.c_str())));
  ASSERT_EQ(OB_SUCCESS, job.set_kms_info(ObString::make_string(input.c_str())));
  ASSERT_EQ(OB_SUCCESS, job.set_passwd_array(ObString::make_string(input.c_str())));
  ASSERT_EQ(OB_SUCCESS, job.set_sts_credential(ObString::make_string(input.c_str())));
  ASSERT_EQ(OB_SUCCESS, job.set_encrypt_key(ObString::make_string(input.c_str())));
  ASSERT_EQ(OB_SUCCESS, job.set_kms_encrypt_key(ObString::make_string(input.c_str())));
  ASSERT_EQ(OB_SUCCESS, job.set_backup_dest(ObString::make_string(input.c_str())));
  ASSERT_EQ(OB_SUCCESS, job.set_kms_dest(ObString::make_string(input.c_str())));
  input.assign(input.size(), 'x');
  EXPECT_EQ(0, job.get_restore_option().compare(SECRET));
  EXPECT_EQ(0, job.get_passwd_array().compare(SECRET));
  ObPhysicalRestoreJob copied;
  ASSERT_EQ(OB_SUCCESS, copied.assign(job));
  job.reset();
  EXPECT_TRUE(job.get_restore_option().empty());
  EXPECT_EQ(0, copied.get_restore_option().compare(SECRET));
  EXPECT_EQ(0, copied.get_kms_info().compare(SECRET));
  EXPECT_EQ(0, copied.get_passwd_array().compare(SECRET));
  EXPECT_EQ(0, copied.get_sts_credential().compare(SECRET));
  EXPECT_EQ(0, copied.get_encrypt_key().compare(SECRET));
  EXPECT_EQ(0, copied.get_kms_encrypt_key().compare(SECRET));
  EXPECT_EQ(0, copied.get_backup_dest().compare(SECRET));
  EXPECT_EQ(0, copied.get_kms_dest().compare(SECRET));
  const std::string printed = format(copied);
  EXPECT_EQ(std::string::npos, printed.find(SECRET));
  EXPECT_NE(std::string::npos, printed.find("restore_diagnostic_tenant"));
  EXPECT_NE(std::string::npos, printed.find("1002"));
  EXPECT_NE(std::string::npos, printed.find("<redacted>"));
  const int ret = OB_INVALID_ARGUMENT;
  LOG_WARN("restore job diagnostics", K(ret), K(copied));
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
}

class ObTestServerConfig : public ObServerConfig
{
public:
  ObTestServerConfig() = default;
};

TEST(BackupSensitiveLogging, config_values_and_nested_rpc)
{
  const char *names[] = {"DATA_BACKUP_DEST", "log_archive_dest_3", "log_restore_source", "access_info",
                        "attribute", "path", "sts_credential", "ssl_external_kms_info"};
  ObAdminSetConfigArg arg;
  for (const char *name : names) {
    ObAdminSetConfigItem item;
    ASSERT_EQ(OB_SUCCESS, item.name_.assign(name));
    // Deliberately malformed and repeated secrets must still be hidden in full.
    ASSERT_EQ(OB_SUCCESS, item.value_.assign("access_key=F65_FAKE_SECRET_20261030&access_key=F65_FAKE_SECRET_20261030"));
    EXPECT_EQ(std::string::npos, format(item).find(SECRET));
    ASSERT_EQ(OB_SUCCESS, arg.items_.push_back(item));
    EXPECT_NE(nullptr, strstr(item.value_.ptr(), SECRET));
  }
  EXPECT_EQ(std::string::npos, format(arg).find(SECRET));
  ObAdminSetConfigArg storage_arg;
  for (const char *name : {"path", "access_info", "attribute"}) {
    ObAdminSetConfigItem item;
    ASSERT_EQ(OB_SUCCESS, item.name_.assign(name));
    ASSERT_EQ(OB_SUCCESS, item.value_.assign(SECRET));
    ASSERT_EQ(OB_SUCCESS, item.tenant_name_.assign("diagnostic_tenant"));
    item.exec_tenant_id_ = 1002;
    ASSERT_EQ(OB_SUCCESS, storage_arg.items_.push_back(item));
  }
  const int ret = OB_INVALID_ARGUMENT;
  LOG_WARN("external storage argument diagnostics", K(ret), "item_count", storage_arg.items_.count(), K(storage_arg));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("external storage argument diagnostics"));
  EXPECT_NE(std::string::npos, log.find("diagnostic_tenant"));
  EXPECT_NE(std::string::npos, log.find("1002"));
  EXPECT_NE(std::string::npos, log.find("access_info"));
  EXPECT_NE(std::string::npos, log.find("attribute"));
  EXPECT_NE(std::string::npos, log.find("<redacted>"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
  ObDMLSqlSplicer dml;
  ObSqlString event_sql;
  ASSERT_EQ(OB_SUCCESS, dml.add_column("value2", arg));
  ASSERT_EQ(OB_SUCCESS, dml.add_column("old_value",
      share::get_config_value_for_log(ObString::make_string("sts_credential"), ObString::make_string(SECRET))));
  ASSERT_EQ(OB_SUCCESS, dml.splice_insert_sql("__all_rootservice_event_history", event_sql));
  EXPECT_EQ(nullptr, strstr(event_sql.ptr(), SECRET));
  std::vector<char> wire(arg.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, arg.serialize(wire.data(), wire.size(), pos));
  ObAdminSetConfigArg decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(wire.data(), wire.size(), pos));
  ASSERT_EQ(arg.items_.count(), decoded.items_.count());
  EXPECT_STREQ(arg.items_.at(0).value_.ptr(), decoded.items_.at(0).value_.ptr());
  ObAdminSetConfigItem ordinary;
  ASSERT_EQ(OB_SUCCESS, ordinary.name_.assign("backup_recovery_window"));
  ASSERT_EQ(OB_SUCCESS, ordinary.value_.assign("7d"));
  EXPECT_NE(std::string::npos, format(ordinary).find("7d"));
  std::unique_ptr<ObTestServerConfig> server(new ObTestServerConfig());
  std::unique_ptr<omt::ObTenantConfig> tenant(new omt::ObTenantConfig(1002));
  ASSERT_TRUE(server->ssl_external_kms_info.set_value(SECRET));
  ASSERT_TRUE(tenant->sts_credential.set_value(SECRET));
  ASSERT_TRUE(tenant->external_kms_info.set_value(SECRET));
  server->print();
  tenant->print();
  EXPECT_STREQ(SECRET, server->ssl_external_kms_info.str());
  EXPECT_STREQ(SECRET, tenant->sts_credential.str());
  EXPECT_STREQ(SECRET, tenant->external_kms_info.str());
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
}

TEST(BackupSensitiveLogging, backup_parameter_formatters)
{
  BackupConfigItemPair pair;
  ASSERT_EQ(OB_SUCCESS, pair.key_.assign("data_backup_dest"));
  ASSERT_EQ(OB_SUCCESS, pair.value_.assign(SECRET));
  EXPECT_EQ(std::string::npos, format(pair).find(SECRET));
  ObBackupParam param;
  ASSERT_EQ(OB_SUCCESS, param.name_.assign("sts_credential"));
  param.value_ = ObString::make_string(SECRET);
  EXPECT_EQ(std::string::npos, format(param).find(SECRET));
  EXPECT_EQ(0, param.value_.compare(SECRET));
  ASSERT_EQ(OB_SUCCESS, param.name_.assign("backup_recovery_window"));
  param.value_ = ObString::make_string("7d");
  EXPECT_NE(std::string::npos, format(param).find("7d"));
}

TEST(BackupSensitiveLogging, restore_and_backup_arguments)
{
  ObPhysicalRestoreTenantArg restore;
  restore.tenant_name_ = ObString::make_string("restore_target");
  restore.restore_option_ = ObString::make_string(SECRET);
  restore.encrypt_key_ = ObString::make_string(SECRET);
  restore.kms_encrypt_key_ = ObString::make_string(SECRET);
  restore.kms_uri_ = ObString::make_string(SECRET);
  restore.kms_info_ = ObString::make_string(SECRET);
  restore.passwd_array_ = ObString::make_string(SECRET);
  restore.uri_ = ObString::make_string(SECRET);
  restore.multi_uri_ = ObString::make_string(SECRET);
  restore.sts_credential_ = ObString::make_string(SECRET);
  std::vector<char> restore_wire(restore.get_serialize_size());
  int64_t restore_pos = 0;
  ASSERT_EQ(OB_SUCCESS, restore.serialize(restore_wire.data(), restore_wire.size(), restore_pos));
  ObPhysicalRestoreTenantArg decoded_restore;
  restore_pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded_restore.deserialize(restore_wire.data(), restore_wire.size(), restore_pos));
  EXPECT_EQ(0, decoded_restore.restore_option_.str().compare(SECRET));
  EXPECT_EQ(0, decoded_restore.passwd_array_.str().compare(SECRET));
  EXPECT_EQ(0, decoded_restore.sts_credential_.str().compare(SECRET));
  EXPECT_EQ(0, decoded_restore.kms_encrypt_key_.str().compare(SECRET));
  EXPECT_EQ(0, decoded_restore.multi_uri_.str().compare(SECRET));
  EXPECT_EQ(std::string::npos, format(decoded_restore).find(SECRET));
  EXPECT_EQ(std::string::npos, format(restore).find(SECRET));
  EXPECT_NE(std::string::npos, format(restore).find("restore_target"));
#ifdef OB_BUILD_TDE_SECURITY
  ObRestoreKeyArg key;
  key.encrypt_key_ = ObString::make_string(SECRET);
  key.backup_dest_ = ObString::make_string(SECRET);
  EXPECT_EQ(std::string::npos, format(key).find(SECRET));
#endif
  ObBackupDatabaseArg backup;
  ASSERT_EQ(OB_SUCCESS, backup.passwd_.assign(SECRET));
  EXPECT_EQ(std::string::npos, format(backup).find(SECRET));
  std::vector<char> wire(backup.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, backup.serialize(wire.data(), wire.size(), pos));
  ObBackupDatabaseArg decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(wire.data(), wire.size(), pos));
  EXPECT_STREQ(SECRET, decoded.passwd_.ptr());
  EXPECT_EQ(std::string::npos, format(decoded).find(SECRET));
}

TEST(BackupSensitiveLogging, password_and_restore_source_statements)
{
  ObBackupSetEncryptionStmt encryption;
  ASSERT_EQ(OB_SUCCESS, encryption.set_param(ObBackupEncryptionMode::PASSWORD, ObString::make_string(SECRET)));
  const std::string digest(encryption.get_passwd().ptr(), encryption.get_passwd().length());
  EXPECT_FALSE(digest.empty());
  EXPECT_EQ(std::string::npos, format(encryption).find(SECRET));
  EXPECT_NE(std::string::npos, format(encryption).find("<redacted>"));
  EXPECT_EQ(std::string::npos, format(encryption).find(digest));
  ObBackupPathString backup_dest;
  ASSERT_EQ(OB_SUCCESS, backup_dest.assign(SECRET));
  ObBackupDatabaseStmt database;
  ObBackupDescription description;
  ObSArray<uint64_t> tenant_ids;
  ASSERT_EQ(OB_SUCCESS, database.set_param(1002, 0, 0, backup_dest, description, tenant_ids));
  EXPECT_EQ(std::string::npos, format(database).find(SECRET));
  EXPECT_STREQ(SECRET, database.get_backup_dest().ptr());
  ObBackupClusterParamStmt cluster;
  ASSERT_EQ(OB_SUCCESS, cluster.set_param(backup_dest));
  ObBackupKeyStmt backup_key;
  ASSERT_EQ(OB_SUCCESS, backup_key.set_param(1002, backup_dest, ObString::make_string(SECRET)));
  EXPECT_EQ(std::string::npos, format(cluster).find(SECRET));
  EXPECT_EQ(std::string::npos, format(backup_key).find(SECRET));
  EXPECT_STREQ(SECRET, cluster.get_backup_dest().ptr());
  EXPECT_STREQ(SECRET, backup_key.get_backup_dest().ptr());
  LOG_INFO("backup statements", K(database), K(cluster), K(backup_key));
  ObBackupSetDecryptionStmt decryption;
  ASSERT_EQ(OB_SUCCESS, decryption.add_passwd(ObString::make_string(SECRET)));
  EXPECT_EQ(std::string::npos, format(decryption).find(SECRET));
  EXPECT_NE(std::string::npos, format(decryption).find("<redacted>"));
  EXPECT_EQ(std::string::npos, format(decryption).find(digest));
  ObAddRestoreSourceStmt source;
  ASSERT_EQ(OB_SUCCESS, source.add_restore_source(ObString::make_string(SECRET)));
  ASSERT_EQ(OB_SUCCESS, source.add_restore_source(ObString::make_string(SECRET)));
  EXPECT_EQ(std::string::npos, format(source).find(SECRET));
  EXPECT_NE(std::string::npos, format(source).find("<redacted>"));
  EXPECT_NE(nullptr, strstr(source.get_restore_source_array().ptr(), SECRET));
  ObBackupJobAttr job;
  ASSERT_EQ(OB_SUCCESS, job.passwd_.assign(digest.c_str()));
  EXPECT_EQ(std::string::npos, format(job).find(digest));
  EXPECT_NE(std::string::npos, format(job).find("<redacted>"));
  ObBackupSetTaskAttr task;
  ASSERT_EQ(OB_SUCCESS, task.passwd_.assign(digest.c_str()));
  EXPECT_EQ(std::string::npos, format(task).find(digest));
  EXPECT_NE(std::string::npos, format(task).find("<redacted>"));
  ObBackupSetFileDesc file;
  ASSERT_EQ(OB_SUCCESS, file.passwd_.assign(digest.c_str()));
  EXPECT_EQ(std::string::npos, format(file).find(digest));
  EXPECT_NE(std::string::npos, format(file).find("<redacted>"));
  EXPECT_EQ(OB_BACKUP_INVALID_PASSWORD, file.check_passwd("wrong_password"));
  std::vector<char> wire(file.get_serialize_size());
  int64_t pos = 0;
  ASSERT_EQ(OB_SUCCESS, file.serialize(wire.data(), wire.size(), pos));
  ObBackupSetFileDesc decoded;
  pos = 0;
  ASSERT_EQ(OB_SUCCESS, decoded.deserialize(wire.data(), wire.size(), pos));
  EXPECT_STREQ(digest.c_str(), decoded.passwd_.ptr());
  EXPECT_EQ(OB_SUCCESS, decoded.check_passwd(digest.c_str()));
  EXPECT_EQ(std::string::npos, format(decoded).find(digest));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("add passwd"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
  EXPECT_EQ(std::string::npos, log.find(digest));
}

TEST(BackupSensitiveLogging, oversized_restore_source_is_omitted_from_resolver_warning)
{
  ObArenaAllocator allocator;
  ObSQLSessionInfo session;
  ASSERT_EQ(OB_SUCCESS, session.set_tenant(ObString::make_string("sys"), OB_SYS_TENANT_ID));
  ObStmtFactory factory(allocator);
  ObResolverParams params;
  params.allocator_ = &allocator;
  params.session_info_ = &session;
  params.stmt_factory_ = &factory;
  params.query_ctx_ = factory.get_query_ctx();
  ASSERT_NE(nullptr, params.query_ctx_);
  ObAddRestoreSourceResolver resolver(params);
  std::string uri = std::string("oss://bucket/piece?access_key=") + SECRET;
  uri.append(8 * 1024 * 1024, 'x');
  ParseNode child = {};
  child.type_ = T_VARCHAR;
  child.str_value_ = uri.c_str();
  child.str_len_ = uri.size();
  ParseNode *children[] = { &child };
  ParseNode tree = {};
  tree.type_ = T_ADD_RESTORE_SOURCE;
  tree.num_child_ = 1;
  tree.children_ = children;
  EXPECT_EQ(OB_SIZE_OVERFLOW, resolver.resolve(tree));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("source_length"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

TEST(BackupSensitiveLogging, attribute_success_and_parse_failure_logs)
{
  ObBackupDestAttribute attribute;
  const std::string access = std::string("access_id=fake_id&access_key=") + SECRET;
  ASSERT_EQ(OB_SUCCESS, ObBackupDestAttributeParser::parse_access_info(ObString::make_string(access.c_str()), attribute));
  EXPECT_STREQ(SECRET, attribute.access_key_);
  ObBackupDest destination;
  const std::string uri = std::string("oss://diagnostic_bucket/backup?host=example.invalid&access_id=fake_id&access_key=") + SECRET;
  ASSERT_EQ(OB_SUCCESS, destination.set(uri.c_str()));
  const std::string printed = format(destination);
  EXPECT_NE(std::string::npos, printed.find("diagnostic_bucket/backup"));
  EXPECT_NE(std::string::npos, printed.find("example.invalid"));
  EXPECT_EQ(std::string::npos, printed.find("fake_id"));
  EXPECT_EQ(std::string::npos, printed.find(SECRET));
  const std::string oversized_key = access + std::string(OB_MAX_BACKUP_ACCESSKEY_LENGTH, 'x');
  EXPECT_NE(OB_SUCCESS, ObBackupDestAttributeParser::parse_access_info(ObString::make_string(oversized_key.c_str()), attribute));
  const std::string oversized = access + std::string(OB_MAX_URI_LENGTH, 'x');
  EXPECT_NE(OB_SUCCESS, ObBackupDestAttributeParser::parse(ObString::make_string(oversized.c_str()), attribute));
  EXPECT_NE(OB_SUCCESS, ObBackupDestAttributeParser::parse_access_info(ObString::make_string(oversized.c_str()), attribute));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("fail to set access key"));
  EXPECT_NE(std::string::npos, log.find("the str too long"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

TEST(BackupSensitiveLogging, invalid_cache_buffer_and_password_output_are_not_strings)
{
  char buf[64];
  MEMSET(buf, 'x', sizeof(buf));
  MEMCPY(buf, SECRET, strlen(SECRET));
  ObBackupIndexCacheKey source;
  ObIKVCacheKey *output = nullptr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, source.deep_copy(buf, 1, output));
  EXPECT_EQ(nullptr, output);
  ObBackupMetaCacheKey meta_source;
  EXPECT_EQ(OB_INVALID_ARGUMENT, meta_source.deep_copy(buf, 1, output));
  EXPECT_EQ(nullptr, output);
  ObBackupFileWriteCtx writer;
  EXPECT_EQ(OB_INVALID_ARGUMENT, writer.write_buffer_(buf, 0, false));
  ObBackupDest uninitialized_dest;
  EXPECT_EQ(OB_NOT_INIT, uninitialized_dest.get_backup_path_str(buf, sizeof(buf)));
  ObRestoreSourceServiceAttr service;
  EXPECT_EQ(OB_INVALID_ARGUMENT, service.get_password(nullptr, 0));
  EXPECT_EQ(OB_INVALID_ARGUMENT, service.get_password(buf, 1));
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
}

TEST(BackupSensitiveLogging, sts_debug_callback_does_not_print_payload)
{
  char payload[64];
  MEMSET(payload, 'x', sizeof(payload));
  MEMCPY(payload, SECRET, strlen(SECRET));
  for (curl_infotype type : {CURLINFO_HEADER_OUT, CURLINFO_DATA_OUT, CURLINFO_HEADER_IN, CURLINFO_DATA_IN}) {
    EXPECT_EQ(OB_SUCCESS, ObDeviceCredentialMgr::debug_callback(nullptr, type, payload, sizeof(payload), nullptr));
  }
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("Send data"));
  EXPECT_NE(std::string::npos, log.find("Recv data"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

TEST(BackupSensitiveLogging, parameter_backup_queries_exclude_secrets)
{
  ObSqlString sql;
  ASSERT_EQ(OB_SUCCESS, ObBackupParamOperator::construct_query_sql_(1002, sql));
  EXPECT_NE(nullptr, strstr(sql.ptr(), "name not in"));
  EXPECT_NE(nullptr, strstr(sql.ptr(), "'sts_credential'"));
  EXPECT_NE(nullptr, strstr(sql.ptr(), "'external_kms_info'"));
  sql.reset();
  ASSERT_EQ(OB_SUCCESS, ObBackupParamOperator::construct_query_sql_(OB_SYS_TENANT_ID, sql));
  EXPECT_NE(nullptr, strstr(sql.ptr(), "name not in"));
  EXPECT_NE(nullptr, strstr(sql.ptr(), "'ssl_external_kms_info'"));
}

#ifdef OB_BUILD_TDE_SECURITY
class ObTestBkmiClient : public ObBkmiClient
{
public:
  using ObBkmiClient::parse_decrypted_data;
};

TEST(BackupSensitiveLogging, malformed_kms_configs_do_not_print_secrets)
{
  for (const std::string &config : {std::string("{\"PRIVATE_KEY\":\"") + SECRET,
      std::string("{\"PRIVATE_KEY\":") + SECRET + "}"}) {
    ObBkmiClient bkmi;
    ObSSLClient ssl;
    ObCeairClient ceair;
    ObAliyunClient aliyun;
    ObMultiCloudClient multi_cloud;
    for (ObKmsClient *client : std::vector<ObKmsClient *>{&bkmi, &ssl, &ceair, &aliyun, &multi_cloud}) {
      EXPECT_NE(OB_SUCCESS, client->init(config.c_str(), config.length()));
    }
  }
  const std::string config = std::string("{\"ACCESS_KEY_SECRET\":\"") + SECRET + "\"}";
  ObAliyunClient aliyun;
  ObMultiCloudClient multi_cloud;
  EXPECT_NE(OB_SUCCESS, aliyun.init(config.c_str(), config.length()));
  EXPECT_NE(OB_SUCCESS, multi_cloud.init(config.c_str(), config.length()));

  ObKmsRequest request;
  request.url_ = ObString::make_string(SECRET);
  request.post_str_ = ObString::make_string(SECRET);
  request.cert_ = ObString::make_string(SECRET);
  ASSERT_EQ(OB_SUCCESS, request.header_strs_.push_back(ObString::make_string(SECRET)));
  EXPECT_EQ(std::string::npos, format(request).find(SECRET));
  LOG_INFO("kms request metadata", K(request));

  ObArenaAllocator allocator;
  ObKmsResult result;
  for (const std::string &response : {std::string("invalid ") + SECRET,
      std::string("{\"Plaintext\":\"") + SECRET,
      std::string("{\"Other\":\"") + SECRET + "\"}"}) {
    EXPECT_NE(OB_SUCCESS, ObKmsClientUtil::get_data_key_from_response(
        allocator, ObString::make_string(response.c_str()), result));
    EXPECT_NE(OB_SUCCESS, ObKmsClientUtil::get_ekey_from_response(
        allocator, ObString::make_string(response.c_str()), result));
  }
  ObTestBkmiClient bkmi;
  const std::string malformed = std::string("{\"content\":\"") + SECRET;
  ObString decrypted_data = ObString::make_string(malformed.c_str());
  EXPECT_NE(OB_SUCCESS, bkmi.parse_decrypted_data(GET_KEY, decrypted_data, result));
  const std::string response = std::string("{\"Plaintext\":\"") + SECRET + "\"}";
  ASSERT_EQ(OB_SUCCESS, ObKmsClientUtil::get_data_key_from_response(
      allocator, ObString::make_string(response.c_str()), result));
  EXPECT_EQ(0, result.content_.compare(SECRET));
  EXPECT_EQ(std::string::npos, format(result).find(SECRET));
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
}
#endif

TEST(BackupSensitiveLogging, json_parse_failure_does_not_print_remaining_input)
{
  ObArenaAllocator allocator;
  json::Parser parser;
  json::Value *value = nullptr;
  const std::string config = std::string("{\"PRIVATE_KEY\":") + SECRET + "}";
  ASSERT_EQ(OB_SUCCESS, parser.init(&allocator));
  EXPECT_NE(OB_SUCCESS, parser.parse(config.data(), config.length(), value));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("lack of member value"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

class ObFailingRestoreDisplayAllocator : public ObIAllocator
{
public:
  void *alloc(const int64_t) override { return nullptr; }
  void *alloc(
      const int64_t,
      const ObMemAttr &) override
  {
    return nullptr;
  }
  void free(void *) override {}
};

TEST(BackupSensitiveLogging, restore_display_failure_does_not_fall_back_to_raw_value)
{
  ObArenaAllocator allocator;
  ObString shown;
  for (const std::string &malformed : {std::string("oss://bucket/a?access_key=") + SECRET + ",invalid",
      std::string("invalid://bucket/a?encrypt_key=") + SECRET,
      std::string("invalid://bucket/a?access_key=first&access_key=") + SECRET}) {
    ASSERT_EQ(OB_SUCCESS, ObPhysicalRestoreJob::get_display_value(ObString::make_string("backup_dest"),
        ObString::make_string(malformed.c_str()), allocator, shown));
    EXPECT_EQ(0, shown.compare("<redacted>"));
  }
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
  ASSERT_EQ(OB_SUCCESS, ObPhysicalRestoreJob::get_display_value(ObString::make_string("backup_dest"),
      ObString(), allocator, shown));
  EXPECT_TRUE(shown.empty());
  ObFailingRestoreDisplayAllocator failing_allocator;
  ObArenaAllocator failing_arena(failing_allocator);
  ASSERT_EQ(OB_ALLOCATE_MEMORY_FAILED, ObPhysicalRestoreJob::get_display_value(
      ObString::make_string("backup_dest"), ObString::make_string("oss://bucket/a"), failing_arena, shown));
  EXPECT_EQ(0, shown.compare("<redacted>"));
}

class RestoreDisplayIterator : public observer::ObAllVirtualRestoreJob
{
public:
  int build_query(const schema::ObTableSchema &table, const schema::ObTableSchema &index,
      schema::ObSchemaGetterGuard &guard, ObSqlString &sql)
  {
    base_table_ = &table;
    table_schema_ = &table;
    index_table_ = &index;
    schema_guard_ = &guard;
    base_tenant_id_ = OB_SYS_TENANT_ID;
    int ret = construct_sql(OB_SYS_TENANT_ID, sql);
    return ret;
  }

  int set_column_name(const char *name)
  {
    const std::string quoted = std::string("`") + name + "`";
    int ret = ob_write_string(*allocator_, ObString::make_string(quoted.c_str()), mapping_[0].base_col_name_);
    return ret;
  }

  int prepare_table_columns(
      const schema::ObTableSchema &base,
      const schema::ObTableSchema &view,
      const uint64_t tenant_id)
  {
    table_schema_ = &view;
    base_table_ = &base;
    effective_tenant_id_ = tenant_id;
    mapping_.reuse();
    return build_base_table_mapping();
  }

  int prepare(const uint64_t table_id, const ObVTableScanParam &param, ObObj &output)
  {
    int ret = OB_SUCCESS;
    base_table_id_ = table_id;
    scan_param_ = &param;
    allocator_ = &inner_allocator_;
    mapping_.set_block_allocator(ObWrapperAllocator(allocator_));
    if (OB_FAIL(mapping_.prepare_allocate(1))) {
    } else if (OB_FAIL(ob_write_string(*allocator_, ObString::make_string("`value`"), mapping_[0].base_col_name_))) {
    } else {
      cur_row_.cells_ = &output;
      cur_row_.count_ = 1;
    }
    return ret;
  }
};

TEST(BackupSensitiveLogging, restore_monitoring_redacts_value_only_projection)
{
  const std::string uri = std::string("oss://bucket/a?host=example.invalid&access_key=") + SECRET
      + ",s3://bucket/b?host=example.invalid&encrypt_key=" + SECRET;
  for (uint64_t table_id : {OB_ALL_RESTORE_JOB_TID, OB_ALL_RESTORE_JOB_HISTORY_TID,
      OB_ALL_RECOVER_TABLE_JOB_TID, OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID}) {
    ObVTableScanParam param;
    ASSERT_EQ(OB_SUCCESS, param.column_ids_.push_back(0));
    ObObj output;
    RestoreDisplayIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.prepare(table_id, param, output));
    ObObj cells[2];
    ObNewRow input;
    input.cells_ = cells;
    const bool history = OB_ALL_RESTORE_JOB_TID != table_id;
    const bool recover_table = OB_ALL_RECOVER_TABLE_JOB_TID == table_id
        || OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID == table_id;
    schema::ObTableSchema table;
    schema::ObTableSchema index;
    schema::ObSchemaGetterGuard guard;
    ObSqlString query;
    if (recover_table) {
      ASSERT_EQ(OB_SUCCESS, OB_ALL_RECOVER_TABLE_JOB_TID == table_id
          ? ObInnerTableSchema::all_recover_table_job_schema(table)
          : ObInnerTableSchema::all_recover_table_job_history_schema(table));
    } else {
      ASSERT_EQ(OB_SUCCESS, history ? ObInnerTableSchema::all_restore_job_history_schema(table)
          : ObInnerTableSchema::all_restore_job_schema(table));
    }
    ASSERT_EQ(OB_SUCCESS, iter.set_column_name(history ? "backup_dest" : "value"));
    ASSERT_EQ(OB_SUCCESS, iter.build_query(table, index, guard, query));
    EXPECT_NE(nullptr, strstr(query.ptr(), history ? "`backup_dest` FROM" : "`value`, `name` FROM"));
    input.count_ = history ? 1 : 2;
    ObNewRow *row = nullptr;
    for (const char *name : {"backup_dest", "kms_dest", "backup_set_list", "backup_piece_list", "log_path_list",
        "kms_info", "encrypt_key", "kms_encrypt_key", "passwd_array", "sts_credential",
        "backup_passwd", "external_kms_info", "restore_option"}) {
      if (history) {
        if (0 != strcmp(name, "backup_dest") && 0 != strcmp(name, "backup_set_list")
            && 0 != strcmp(name, "backup_piece_list") && 0 != strcmp(name, "restore_option") && !(recover_table
                && (0 == strcmp(name, "backup_passwd") || 0 == strcmp(name, "external_kms_info")))) {
          continue;
        }
        ASSERT_EQ(OB_SUCCESS, iter.set_column_name(name));
      }
      cells[0].reset();
      const bool varchar_column = (history && 0 == strcmp(name, "restore_option"))
          || (recover_table && (0 == strcmp(name, "backup_dest")
          || 0 == strcmp(name, "backup_passwd") || 0 == strcmp(name, "external_kms_info")));
      if (varchar_column) {
        cells[0].set_varchar(ObString::make_string(uri.c_str()));
      } else {
        cells[0].set_lob_value(ObLongTextType, uri.data(), uri.length());
      }
      cells[0].set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
      cells[1].set_varchar(name);
      ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
      ASSERT_NE(nullptr, row);
      ASSERT_EQ(1, row->count_);
      EXPECT_EQ(varchar_column ? ObVarcharType : ObLongTextType, row->cells_[0].get_type());
      const ObString shown = row->cells_[0].get_string();
      const char *expected = (0 == strcmp(name, "backup_dest") || 0 == strcmp(name, "kms_dest")
          || 0 == strcmp(name, "backup_set_list") || 0 == strcmp(name, "backup_piece_list") || 0 == strcmp(name, "log_path_list"))
          ? "oss://bucket/a,s3://bucket/b" : "<redacted>";
      EXPECT_EQ(0, shown.compare(expected));
      EXPECT_EQ(0, cells[0].get_string().compare(uri.c_str()));
      if (varchar_column) {
        continue;
      }
      std::vector<char> lob_buf(sizeof(ObLobCommon) + uri.length());
      ObLobCommon *lob = new (lob_buf.data()) ObLobCommon();
      MEMCPY(lob->buffer_, uri.data(), uri.length());
      cells[0].set_lob_value(ObLongTextType, lob, lob_buf.size());
      cells[0].set_has_lob_header();
      ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
      EXPECT_EQ(0, row->cells_[0].get_string().compare(expected));
      EXPECT_FALSE(row->cells_[0].has_lob_header());
    }
    cells[0].reset();
    cells[0].set_lob_value(ObLongTextType, "diagnostic_tenant", 17);
    cells[1].set_varchar("tenant_name");
    if (history) {
      ASSERT_EQ(OB_SUCCESS, iter.set_column_name(recover_table ? "aux_tenant_name" : "restore_tenant_name"));
    }
    ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
    EXPECT_EQ(0, row->cells_[0].get_string().compare("diagnostic_tenant"));
    cells[0].set_null();
    cells[1].set_varchar("backup_dest");
    if (history) {
      ASSERT_EQ(OB_SUCCESS, iter.set_column_name("backup_dest"));
    }
    ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
    EXPECT_TRUE(row->cells_[0].is_null());
    const std::string malformed = std::string("invalid://bucket/a?encrypt_key=") + SECRET;
    if (recover_table) {
      cells[0].set_varchar(ObString::make_string(malformed.c_str()));
    } else {
      cells[0].set_lob_value(ObLongTextType, malformed.data(), malformed.length());
    }
    ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
    ASSERT_NE(nullptr, row);
    EXPECT_EQ(0, row->cells_[0].get_string().compare("<redacted>"));
    EXPECT_EQ(0, cells[0].get_string().compare(malformed.c_str()));
    if (recover_table) {
      cells[0].set_varchar(ObString::make_string(uri.c_str()));
    } else {
      cells[0].set_lob_value(ObLongTextType, uri.data(), uri.length());
    }
    ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
    ASSERT_NE(nullptr, row);
    EXPECT_EQ(0, row->cells_[0].get_string().compare("oss://bucket/a,s3://bucket/b"));
    EXPECT_EQ(0, cells[0].get_string().compare(uri.c_str()));
  }
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
}

TEST(BackupSensitiveLogging, restore_option_parse_errors_do_not_print_input)
{
  ObPhysicalRestoreJob job;
  for (const std::string &option : {std::string("=") + SECRET,
      std::string("pool_list=") + SECRET + std::string(1024, 'x'),
      std::string(OB_MAX_URI_LENGTH, 'x') + SECRET}) {
    EXPECT_NE(OB_SUCCESS, ObPhysicalRestoreOptionParser::parse(ObString::make_string(option.c_str()), job));
  }
  // Exercise recognized-option validation and the real caller's K(arg)/K(job) logs.
  const std::string rejected_options[] = {
      std::string("pool_list=pool_a&method=") + SECRET + "&concurrency=invalid",
      std::string("pool_list=pool_a&concurrency=") + SECRET,
      std::string("pool_list=pool_a&concurrency=999999999999999999999999") + SECRET};
  const int expected_results[] = {OB_INVALID_ARGUMENT, OB_INVALID_ARGUMENT, OB_SIZE_OVERFLOW};
  ObPhysicalRestoreTenantArg arg;
  arg.tenant_name_ = ObString::make_string("restore_option_diagnostic_tenant");
  arg.uri_ = ObString::make_string("file:///restore_option_test");
  OB_LOGGER.set_log_level("TRACE");
  for (int64_t i = 0; i < ARRAYSIZEOF(rejected_options); ++i) {
    arg.restore_option_ = ObString::make_string(rejected_options[i].c_str());
    EXPECT_TRUE(arg.is_valid());
    EXPECT_EQ(expected_results[i], rootserver::ObRestoreUtil::fill_physical_restore_job(1, arg, job));
    EXPECT_EQ(0, arg.restore_option_.str().compare(rejected_options[i].c_str()));
  }
  OB_LOGGER.set_log_level("DEBUG");
  ASSERT_EQ(OB_SUCCESS, ObPhysicalRestoreOptionParser::parse(
      ObString::make_string("pool_list=pool_a&concurrency=2"), job));
  EXPECT_EQ(0, job.get_pool_list().compare("pool_a"));
  EXPECT_EQ(2, job.get_concurrency());
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("fail parse arg"));
  EXPECT_NE(std::string::npos, log.find("fail parse uri buf"));
  EXPECT_NE(std::string::npos, log.find("fail set value"));
  EXPECT_NE(std::string::npos, log.find("fail to parse restore_option"));
  EXPECT_NE(std::string::npos, log.find("finish fill_physical_restore_job"));
  EXPECT_NE(std::string::npos, log.find("KV pair ignored by common restore uri parser."));
  EXPECT_EQ(std::string::npos, log.find("extra_args="));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

TEST(BackupSensitiveLogging, recover_table_views_hide_credentials)
{
  const std::string uri = std::string("oss://bucket/path?access_key=") + SECRET;
  for (uint64_t table_id : {OB_ALL_RECOVER_TABLE_JOB_TID, OB_ALL_RECOVER_TABLE_JOB_HISTORY_TID}) {
    schema::ObTableSchema schema;
    ASSERT_EQ(OB_SUCCESS, OB_ALL_RECOVER_TABLE_JOB_TID == table_id
        ? ObInnerTableSchema::all_recover_table_job_schema(schema)
        : ObInnerTableSchema::all_recover_table_job_history_schema(schema));
    schema::ObTableSchema view;
    ASSERT_EQ(OB_SUCCESS, OB_ALL_RECOVER_TABLE_JOB_TID == table_id
        ? ObInnerTableSchema::all_virtual_recover_table_job_schema(view)
        : ObInnerTableSchema::all_virtual_recover_table_job_history_schema(view));
    ObVTableScanParam param;
    ASSERT_EQ(OB_SUCCESS, param.column_ids_.push_back(0));
    ObObj input_cell;
    ObObj output_cell;
    ObNewRow input;
    input.cells_ = &input_cell;
    input.count_ = 1;
    ObNewRow *row = nullptr;
    RestoreDisplayIterator iter;
    ASSERT_EQ(OB_SUCCESS, iter.prepare(table_id, param, output_cell));
    const uint64_t tenants[] = {OB_SYS_TENANT_ID, 1002};
    for (uint64_t tenant : tenants) {
      ASSERT_EQ(OB_SUCCESS, iter.prepare_table_columns(schema, view, tenant));
      for (const char *name : {"backup_passwd", "external_kms_info", "restore_option", "backup_dest", "target_tenant_name"}) {
        SCOPED_TRACE(name);
        const schema::ObColumnSchemaV2 *column = view.get_column_schema(name);
        ASSERT_NE(nullptr, column);
        param.column_ids_.at(0) = column->get_column_id();
        const bool ordinary = 0 == strcmp(name, "target_tenant_name");
        input_cell.set_varchar(ordinary ? "recover_tenant" : uri.c_str());
        input_cell.set_collation_type(CS_TYPE_UTF8MB4_GENERAL_CI);
        ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
        ASSERT_NE(nullptr, row);
        EXPECT_EQ(ObVarcharType, output_cell.get_type());
        EXPECT_EQ(input_cell.get_collation_type(), output_cell.get_collation_type());
        const char *expected = "<redacted>";
        if (ordinary) {
          expected = "recover_tenant";
        } else if (0 == strcmp(name, "backup_dest")) {
          expected = "oss://bucket/path";
        }
        EXPECT_EQ(0, output_cell.get_string().compare(expected));
        EXPECT_EQ(0, input_cell.get_string().compare(ordinary ? "recover_tenant" : uri.c_str()));
        input_cell.set_null();
        ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
        EXPECT_TRUE(output_cell.is_null());
      }
      for (const char *name : {"backup_set_list", "backup_piece_list"}) {
        const schema::ObColumnSchemaV2 *column = view.get_column_schema(name);
        ASSERT_NE(nullptr, column);
        param.column_ids_.at(0) = column->get_column_id();
        std::vector<char> buf(sizeof(ObLobCommon) + uri.size());
        ObLobCommon *lob = new (buf.data()) ObLobCommon();
        MEMCPY(lob->buffer_, uri.data(), uri.size());
        input_cell.set_lob_value(ObLongTextType, lob, buf.size());
        input_cell.set_has_lob_header();
        ASSERT_EQ(OB_SUCCESS, iter.try_convert_row(&input, row));
        EXPECT_EQ(ObLongTextType, output_cell.get_type());
        EXPECT_FALSE(output_cell.has_lob_header());
        EXPECT_EQ(0, output_cell.get_string().compare("oss://bucket/path"));
      }
    }
  }
}

TEST(BackupSensitiveLogging, full_restore_log_path_array_keeps_uri_out_of_warning)
{
  ObArenaAllocator allocator;
  ObFixedArray<ObBackupPathString, ObArenaAllocator> paths(allocator, 1);
  ObBackupDest dest;
  const char *uri = "file:///restore/private-log-path-marker";
  ASSERT_EQ(OB_SUCCESS, dest.set(uri));
  ASSERT_EQ(OB_SUCCESS, rootserver::ObRestoreUtil::get_restore_log_path_list_(dest, paths));
  ASSERT_EQ(1, paths.count());
  EXPECT_STREQ(uri, paths.at(0).ptr());
  EXPECT_EQ(OB_SIZE_OVERFLOW, rootserver::ObRestoreUtil::get_restore_log_path_list_(dest, paths));
  const std::string log = read_log();
  const size_t warning = log.rfind("fail to push backup log path");
  ASSERT_NE(std::string::npos, warning);
  const std::string line = log.substr(warning, log.find('\n', warning) - warning);
  EXPECT_NE(std::string::npos, line.find("log_path_count"));
  EXPECT_EQ(std::string::npos, line.find(uri));
}

TEST(BackupSensitiveLogging, log_restore_source_diagnostics_preserve_only_metadata)
{
  const std::string uri = std::string("oss://bucket/piece?access_key=") + SECRET;
  const ObString source = ObString::make_string(uri.c_str());
  ObLogRestoreSourceItem item(1002, 1, ObLogRestoreSourceType::RAWPATH, source, SCN::min_scn());
  EXPECT_EQ(0, source.compare(item.value_));
  EXPECT_EQ(std::string::npos, format(item).find(SECRET));
  EXPECT_NE(std::string::npos, format(item).find("1002"));

  ObMySQLProxy proxy;
  ObLogRestoreSourceMgr mgr;
  ASSERT_EQ(OB_SUCCESS, mgr.init(1002, &proxy));
  share::DirArray paths;
  ASSERT_EQ(OB_SUCCESS, paths.push_back(ObBackupPathString(uri.c_str())));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.add_location_source(SCN(), source));
  EXPECT_EQ(OB_INVALID_ARGUMENT, mgr.add_rawpath_source(SCN(), paths));

  logservice::DirArray directories;
  logservice::DirInfo dir;
  ASSERT_EQ(OB_SUCCESS, dir.first.assign("oss://bucket/piece"));
  ASSERT_EQ(OB_SUCCESS, dir.second.assign(SECRET));
  ASSERT_EQ(OB_SUCCESS, directories.push_back(dir));
  logservice::ObLogRawPathPieceContext ctx;
  ASSERT_EQ(OB_SUCCESS, ctx.init(ObLSID(1), directories));
  char storage_info[128] = {};
  ASSERT_EQ(OB_SUCCESS, ctx.get_cur_storage_info(storage_info, sizeof(storage_info)));
  EXPECT_STREQ(SECRET, storage_info);
  EXPECT_NE(std::string::npos, format(ctx).find("path_count"));
  EXPECT_EQ(std::string::npos, format(ctx).find(SECRET));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("invalid argument to add rawpath source"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

TEST(BackupSensitiveLogging, log_restore_source_tenant_omits_password)
{
  logservice::ObLogRestoreSourceTenant source;
  ASSERT_EQ(OB_SUCCESS, source.user_name_.assign("diagnostic_restore_user"));
  ASSERT_EQ(OB_SUCCESS, source.user_passwd_.assign(SECRET));
  logservice::ObLogRestoreSourceTenant copy;
  ASSERT_EQ(OB_SUCCESS, copy.set(source));
  EXPECT_STREQ(SECRET, copy.user_passwd_.ptr());
  const std::string printed = format(copy);
  EXPECT_NE(std::string::npos, printed.find("diagnostic_restore_user"));
  EXPECT_EQ(std::string::npos, printed.find(SECRET));
}

TEST(BackupSensitiveLogging, restore_source_config_failures_hide_input)
{
  const std::string password = std::string("PASSWORD=") + SECRET + std::string(OB_MAX_PASSWORD_LENGTH, 'x');
  ObSqlString config;
  ASSERT_EQ(OB_SUCCESS, config.assign(password.c_str()));
  ObLogRestoreSourceServiceConfigParser service(ObBackupConfigType::LOG_RESTORE_SOURCE, 1002);
  EXPECT_EQ(OB_INVALID_ARGUMENT, service.parse_from(config));
  ObRestoreSourceServiceAttr attr;
  EXPECT_EQ(OB_INVALID_ARGUMENT, attr.parse_service_attr_from_str(config));

  ObLogRestoreSourceLocationConfigParser location(ObBackupConfigType::LOG_RESTORE_SOURCE, 1002, 1);
  const std::string invalid_location = std::string("LOCATION=unknown://bucket?access_key=") + SECRET;
  ASSERT_EQ(OB_SUCCESS, config.assign(invalid_location.c_str()));
  EXPECT_NE(OB_SUCCESS, location.parse_from(config));

  ObRestoreSourceLocationPrimaryAttr primary;
  ASSERT_EQ(OB_SUCCESS, primary.location_.assign(invalid_location.c_str()));
  EXPECT_EQ(std::string::npos, format(primary).find(SECRET));

  ObCreateTenantArg arg;
  arg.log_restore_source_ = ObString::make_string(password.c_str());
  EXPECT_EQ(std::string::npos, format(arg).find(SECRET));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("parse restore source service password failed"));
  EXPECT_NE(std::string::npos, log.find("fail to do parse log archive dest"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

class BackupRestoreWriteProxy : public ObMySQLProxy
{
public:
  int write(
      const uint64_t tenant_id,
      const char *sql,
      const int32_t group_id,
      int64_t &affected_rows) override
  {
    saw_marker_ = nullptr != sql && nullptr != strstr(sql, SECRET);
    affected_rows = OB_SUCCESS == result_ ? 1 : 0;
    return result_;
  }

  bool saw_marker_ = false;
  int result_ = OB_SUCCESS;
};

class BackupDestReadTimeoutProxy : public ObMySQLProxy
{
public:
  int read(
      ReadResult &result,
      const uint64_t tenant_id,
      const char *sql) override
  {
    return OB_TIMEOUT;
  }
};

TEST(BackupSensitiveLogging, query_diagnostics_hide_statement_text)
{
  const ObString stmt = ObString::make_string(SECRET);
  ObDMLSqlSplicer dml;
  EXPECT_EQ(OB_INVALID_ARGUMENT, dml.add_column(nullptr, stmt));
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
  ObPsSqlKey key(1002, stmt);
  EXPECT_EQ(std::string::npos, format(key).find(SECRET));
  ObSEArray<ObString, 2> batch;
  ASSERT_EQ(OB_SUCCESS, batch.push_back(stmt));
  ObMultiStmtItem item(true, 1, stmt);
  item.set_batched_queries(&batch);
  EXPECT_EQ(std::string::npos, format(item).find(SECRET));
  EXPECT_EQ(1, item.get_batched_stmt_cnt());
}

TEST(BackupSensitiveLogging, archive_and_backup_destination_persistence)
{
  const std::string uri = std::string("oss://bucket/archive?host=example.invalid&access_id=") + SECRET;
  BackupRestoreWriteProxy proxy;
  ObBackupHelper backup;
  ObArchivePersistHelper archive;
  ObSqlString name;
  ObSqlString value;
  ASSERT_EQ(OB_SUCCESS, backup.init(1002, proxy));
  ASSERT_EQ(OB_SUCCESS, archive.init(1002));
  ASSERT_EQ(OB_SUCCESS, name.assign(OB_STR_PATH));
  ASSERT_EQ(OB_SUCCESS, value.assign(uri.c_str()));
  const int results[] = {OB_SUCCESS, OB_TIMEOUT};
  for (const int expected : results) {
    proxy.result_ = expected;
    proxy.saw_marker_ = false;
    EXPECT_EQ(expected, backup.set_backup_dest(ObBackupPathString(uri.c_str())));
    EXPECT_TRUE(proxy.saw_marker_);
    proxy.saw_marker_ = false;
    EXPECT_EQ(expected, archive.set_kv_item(proxy, 1, name, value));
    EXPECT_TRUE(proxy.saw_marker_);
  }
  ObBackupPieceInfo piece;
  ASSERT_EQ(OB_SUCCESS, piece.backup_dest_.assign(uri.c_str()));
  EXPECT_EQ(std::string::npos, format(piece).find(SECRET));
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("insert/update one row"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

TEST(BackupSensitiveLogging, backup_destination_failures_hide_credentials)
{
  const std::string uri = std::string("oss://bucket/archive?host=example.invalid&access_id=") + SECRET + "&access_key=" + SECRET;
  BackupDestReadTimeoutProxy proxy;
  ObBackupDest dest;
  EXPECT_EQ(OB_TIMEOUT, ObBackupStorageInfoOperator::get_backup_dest(proxy, 1002, ObBackupPathString(uri.c_str()), dest));
  ASSERT_EQ(OB_SUCCESS, dest.set(uri.c_str()));
#ifdef OB_BUILD_TDE_SECURITY
  ASSERT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().init(nullptr));
  ASSERT_EQ(OB_SUCCESS, ObMasterKeyGetter::instance().set_root_key(OB_SYS_TENANT_ID, RootKeyType::DEFAULT, ObString()));
#endif
  BackupRestoreWriteProxy writer;
  EXPECT_EQ(OB_SUCCESS, ObBackupStorageInfoOperator::update_backup_authorization(writer, 1002, dest));
  writer.result_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, ObBackupStorageInfoOperator::update_backup_authorization(writer, 1002, dest));
#ifdef OB_BUILD_TDE_SECURITY
  ObMasterKeyGetter::instance().destroy();
#endif
  const std::string log = read_log();
  EXPECT_NE(std::string::npos, log.find("fail to execute sql"));
  EXPECT_EQ(std::string::npos, log.find(SECRET));
}

#ifdef OB_BUILD_TDE_SECURITY
TEST(BackupSensitiveLogging, root_key_rpc_diagnostics)
{
  ObString key = ObString::make_string(SECRET);
  ObGetMasterKeyResultArg rpc_result;
  ASSERT_EQ(key.length(), rpc_result.str_.write(key.ptr(), key.length()));
  EXPECT_EQ(0, rpc_result.str_.compare(key));
  EXPECT_EQ(std::string::npos, format(rpc_result).find(SECRET));
  ObMasterKey master_key;
  MEMCPY(master_key.key_, key.ptr(), key.length());
  master_key.len_ = key.length();
  EXPECT_EQ(std::string::npos, format(master_key).find("4636355"));
  ObRootKeyArg arg;
  EXPECT_EQ(OB_INVALID_ARGUMENT, arg.init(OB_INVALID_TENANT_ID, RootKeyType::NORMAL, key));
  ASSERT_EQ(OB_SUCCESS, arg.init(1002, RootKeyType::NORMAL, key));
  EXPECT_EQ(0, arg.root_key_.compare(key));
  EXPECT_NE(std::string::npos, format(arg).find("1002"));
  EXPECT_EQ(std::string::npos, format(arg).find(SECRET));
  ObRootKeyResult result;
  result.key_type_ = RootKeyType::NORMAL;
  result.root_key_ = key;
  EXPECT_EQ(std::string::npos, format(result).find(SECRET));
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
}
#endif

TEST(BackupSensitiveLogging, restore_history_object_hides_credentials)
{
  ObHisRestoreJobPersistInfo info;
  ASSERT_EQ(OB_SUCCESS, info.restore_option_.assign(SECRET));
  ASSERT_EQ(OB_SUCCESS, info.backup_dest_.assign(SECRET));
  ASSERT_EQ(OB_SUCCESS, info.backup_set_list_.assign(SECRET));
  ASSERT_EQ(OB_SUCCESS, info.backup_piece_list_.assign(SECRET));
  ASSERT_EQ(OB_SUCCESS, info.restore_tenant_name_.assign("diagnostic_tenant"));
  const std::string printed = format(info);
  EXPECT_EQ(std::string::npos, printed.find(SECRET));
  EXPECT_NE(std::string::npos, printed.find("diagnostic_tenant"));
  EXPECT_STREQ(SECRET, info.backup_dest_.ptr());
  info.key_.tenant_id_ = OB_SYS_TENANT_ID;
  info.key_.job_id_ = 123;
  info.restore_scn_ = SCN::min_scn();
  ObRestorePersistHelper helper;
  BackupRestoreWriteProxy proxy;
  ASSERT_EQ(OB_SUCCESS, helper.init(OB_SYS_TENANT_ID, 0));
  ASSERT_EQ(OB_SUCCESS, helper.insert_restore_job_history(proxy, info));
  proxy.result_ = OB_TIMEOUT;
  EXPECT_EQ(OB_TIMEOUT, helper.insert_restore_job_history(proxy, info));
  EXPECT_NE(std::string::npos, read_log().find("insert one row"));
  LOG_INFO("restore history object", K(info));
  ObRecoverTableJob recover;
  ASSERT_EQ(OB_SUCCESS, recover.set_restore_option(ObString::make_string(SECRET)));
  ASSERT_EQ(OB_SUCCESS, recover.set_backup_dest(ObString::make_string(SECRET)));
  ASSERT_EQ(OB_SUCCESS, recover.set_backup_passwd(ObString::make_string(SECRET)));
  ASSERT_EQ(OB_SUCCESS, recover.set_external_kms_info(ObString::make_string(SECRET)));
  ASSERT_EQ(OB_SUCCESS, recover.set_aux_tenant_name(ObString::make_string("diagnostic_tenant")));
  EXPECT_EQ(std::string::npos, format(recover).find(SECRET));
  EXPECT_NE(std::string::npos, format(recover).find("diagnostic_tenant"));
  EXPECT_EQ(0, recover.get_backup_dest().compare(SECRET));
  EXPECT_EQ(0, recover.get_backup_passwd().compare(SECRET));
  EXPECT_EQ(0, recover.get_external_kms_info().compare(SECRET));
  LOG_INFO("recover table job object", K(recover));
  EXPECT_EQ(std::string::npos, read_log().find(SECRET));
}
} // namespace

int main(int argc, char **argv)
{
  std::remove("test_backup_sensitive_logging.log");
  std::remove("test_backup_sensitive_logging.rs.log");
  OB_LOGGER.set_enable_async_log(false);
  OB_LOGGER.set_file_name("test_backup_sensitive_logging.log", true, false, "test_backup_sensitive_logging.rs.log");
  OB_LOGGER.set_log_level("DEBUG");
  ::testing::InitGoogleTest(&argc, argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
