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

#ifndef OCEANBASE_SHARE_OB_BACKUP_STORE_H_
#define OCEANBASE_SHARE_OB_BACKUP_STORE_H_

#include "share/backup/ob_backup_struct.h"
#include "share/backup/ob_backup_serialize_provider.h"
#include "share/ob_srv_rpc_proxy.h"

namespace oceanbase
{
namespace share
{
struct ObBackupFormatDesc final : public ObIBackupSerializeProvider
{
  OB_UNIS_VERSION_V(1); // virtual
  static const uint8_t FILE_VERSION = 1;
public:
  common::ObFixedLengthString<common::OB_MAX_CLUSTER_NAME_LENGTH> cluster_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> tenant_name_;
  ObBackupPathString path_;

  int64_t cluster_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  int64_t dest_id_;
  int64_t dest_type_;
  int64_t ts_; // format generated timestamp.

  ObBackupFormatDesc();
  
  bool is_valid() const override;

  // Get file data type
  uint16_t get_data_type() const override;
  
  // Get file data version
  uint16_t get_data_version() const override;

  // Get file data compress algorithm type, default none.
  uint16_t get_compressor_type() const override
  {
    return ObCompressorType::NONE_COMPRESSOR;
  }
  bool is_format_equal(const ObBackupFormatDesc &desc) const;

  TO_STRING_KV(K_(cluster_name), K_(tenant_name), K_(path), K_(cluster_id), 
    K_(tenant_id), K_(incarnation), K_(dest_id), K_(dest_type));
};

struct ObBackupCheckDesc final : public ObIBackupSerializeProvider
{
  OB_UNIS_VERSION_V(1); // virtual
  static const uint8_t FILE_VERSION = 1;
public:
  common::ObFixedLengthString<common::OB_MAX_CLUSTER_NAME_LENGTH> cluster_name_;
  common::ObFixedLengthString<common::OB_MAX_TENANT_NAME_LENGTH> tenant_name_;
  ObBackupPathString path_;
  int64_t cluster_id_;
  uint64_t tenant_id_;
  int64_t incarnation_;
  int64_t ts_; // format generated timestamp.

  ObBackupCheckDesc();
  
  bool is_valid() const override;
  uint16_t get_data_type() const override;
  uint16_t get_data_version() const override;
  uint16_t get_compressor_type() const override
  {
    return ObCompressorType::NONE_COMPRESSOR;
  }

  TO_STRING_KV(K_(cluster_name), K_(tenant_name), K_(path), K_(cluster_id), 
    K_(tenant_id), K_(incarnation));
};

struct ObBackupConsistencyCheckDesc final : public ObIBackupSerializeProvider
{
  OB_UNIS_VERSION_V(1); // virtual
  static const uint8_t FILE_VERSION = 1;
public:
  ObBackupConsistencyCheckDesc();
  bool is_valid() const override;
  uint16_t get_data_type() const override;
  uint16_t get_data_version() const override;
  uint16_t get_compressor_type() const override
  {
    return ObCompressorType::NONE_COMPRESSOR;
  }
  int init(const uint64_t tenant_id);
  void reset();
  uint64_t get_tenant_id() const { return tenant_id_; }
  TO_STRING_KV(K_(tenant_id));
private:
  static const int64_t RANDOM_CONTENT_LENGTH = 256;
  int generate_random_content_();
private:
  uint64_t tenant_id_;
  char random_content_[RANDOM_CONTENT_LENGTH];
};

class ObExternBackupDataDesc : public share::ObIBackupSerializeProvider
{
public:
  explicit ObExternBackupDataDesc(uint16_t type, uint16_t version)
    : type_(type), version_(version) {}
  virtual ~ObExternBackupDataDesc() {}

  uint16_t get_data_type() const override { return type_; }
  uint16_t get_data_version() const override { return version_; }
  uint16_t get_compressor_type() const override { return ObCompressorType::NONE_COMPRESSOR; }

  VIRTUAL_TO_STRING_KV(K_(type), K_(version));
private:
  uint16_t type_;
  uint16_t version_;
};

class ObBackupStore
{
public:
  ObBackupStore();

  int init(const char *backup_dest);
  int init(const share::ObBackupDest &backup_dest);

  bool is_init() const;
  void reset();
  const ObBackupDest &get_backup_dest() const;
  const ObBackupStorageInfo *get_storage_info() const;

  // oss://backup_dest/format
  int get_format_file_path(ObBackupPathString &path) const;
  int is_format_file_exist(bool &is_exist) const;
  int dest_is_empty_directory(bool &is_empty) const;
  int read_format_file(ObBackupFormatDesc &desc) const;
  int write_format_file(const ObBackupFormatDesc &desc) const;
  int write_check_file(const ObBackupPathString &full_path, const ObBackupCheckDesc &desc) const;
  int read_check_file(const ObBackupPathString &full_path, ObBackupCheckDesc &desc) const;
  int write_rw_consistency_check_file(const ObBackupPathString &full_path, const ObBackupConsistencyCheckDesc &desc) const;
  int read_rw_consistency_check_file(const ObBackupPathString &full_path, ObBackupConsistencyCheckDesc &desc);
  TO_STRING_KV(K_(is_inited), K_(backup_dest));

protected:
  int write_single_file(const ObBackupPathString &full_path, const ObIBackupSerializeProvider &serializer) const;
  int read_single_file(const ObBackupPathString &full_path, ObIBackupSerializeProvider &serializer) const;

private:
  bool is_inited_;
  // backup dest with storage info
  ObBackupDest backup_dest_;

  DISALLOW_COPY_AND_ASSIGN(ObBackupStore);
};

class ObBackupDestMgr final
{
public:
  enum RemoteExecuteType {
    CHECK_DEST_VALIDITY = 0,
    WRITE_FORMAT_FILE = 1,
    MAX
  };
  ObBackupDestMgr();
  ~ObBackupDestMgr() {}

  int init(
      const uint64_t tenant_id,
      const ObBackupDestType::TYPE &dest_type,
      const share::ObBackupPathString &backup_dest_str,
      common::ObISQLClient &sql_proxy);
  int init_for_rpc(
      const uint64_t tenant_id,
      const ObBackupDestType::TYPE &dest_type,
      const share::ObBackupPathString &backup_dest_str,
      common::ObISQLClient &sql_proxy);
  int check_dest_connectivity(obrpc::ObSrvRpcProxy &rpc_proxy);
  int check_dest_validity(obrpc::ObSrvRpcProxy &rpc_proxy, const bool need_format_file);
  int write_format_file();
  void reset();
  bool is_valid_type(const RemoteExecuteType type) const { return CHECK_DEST_VALIDITY <= type && type < MAX; }
private:
  int generate_format_desc_(
      const int64_t dest_id,
      const ObBackupDestType::TYPE &dest_type,
      share::ObBackupFormatDesc &format_desc);
  int updata_backup_file_status_();
  int remote_execute_if_need_(obrpc::ObSrvRpcProxy &rpc_proxy,
                              const bool need_format_file,
                              const RemoteExecuteType type,
                              bool &need_remote_execute);
private:
  bool is_inited_;
  uint64_t tenant_id_;
  int64_t max_iops_;
  int64_t max_bandwidth_;
  ObBackupDestType::TYPE dest_type_;
  share::ObBackupDest backup_dest_;
  common::ObISQLClient *sql_proxy_;
  bool is_remote_execute_;
  DISALLOW_COPY_AND_ASSIGN(ObBackupDestMgr);
};


}
}

#endif