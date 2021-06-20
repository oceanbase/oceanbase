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

#ifndef __OCEANBASE_SHARE_RESTORE_URI_PARSER_H__
#define __OCEANBASE_SHARE_RESTORE_URI_PARSER_H__

#include "share/backup/ob_physical_restore_info.h"
#include "share/ob_kv_parser.h"
#include "share/ob_list_parser.h"
#include "share/restore/ob_restore_args.h"
#include "lib/hash/ob_hashmap.h"
#include "common/ob_zone.h"

namespace oceanbase {
namespace share {
// backup_info example:
// 0_8_1516844707416396_0_8
// 0_9_1516886087756333_1_9
// 0_10_1516887298293162_0_10
// 1_17_1516895507194456_0_10
// 1_18_1516895591162037_0_10
// 0_19_1516895657029050_0_19
struct ObAgentBackupInfo {
  ObAgentBackupInfo();
  int64_t backup_type_;
  int64_t curr_data_version_;
  int64_t timestamp_;
  int64_t backup_result_;
  int64_t base_data_version_;
  int64_t base_schema_version_;
  uint64_t cluster_version_;
  static bool info_compare(const ObAgentBackupInfo& a, const ObAgentBackupInfo& b)
  {
    return a.timestamp_ < b.timestamp_;
  }
  TO_STRING_KV(K_(backup_type), K_(base_data_version), K_(curr_data_version), K_(timestamp), K_(backup_result),
      K_(base_schema_version), K_(cluster_version));
};

class ObRestoreURIParserHelper {
public:
  static int set_data_version(share::ObRestoreArgs& arg);
};

class ObRestoreURIParser {
public:
  static int parse(const common::ObString& uri, ObRestoreArgs& arg);

private:
  static int parse(const char* uri, share::ObRestoreArgs& arg);
  static int split_uri(const char* full_uri, share::ObRestoreArgs& arg, char* dir_path, char* extra_args);

public:
  class DirParser {
  public:
    DirParser(share::ObRestoreArgs& arg) : arg_(arg){};
    int parse(const char* val);

  private:
    /* functions */
    share::ObRestoreArgs& arg_;
  };

  class ExtraArgsCb : public share::ObKVMatchCb {
  public:
    ExtraArgsCb(share::ObRestoreArgs& arg);  // : arg_(arg) {};
    int match(const char* key, const char* value);
    bool check() const;

  private:
    /* functions */
    typedef int (*Setter)(share::ObRestoreArgs& arg, const char* val);
    static int set_data_timestamp(share::ObRestoreArgs& arg, const char* val);
    static int set_base_data_version(share::ObRestoreArgs& arg, const char* val);
    static int set_curr_data_version(share::ObRestoreArgs& arg, const char* val);
    static int set_locality(share::ObRestoreArgs& arg, const char* val);
    static int set_pool_list(share::ObRestoreArgs& arg, const char* val);
    static int set_primary_zone(share::ObRestoreArgs& arg, const char* val);
    static int set_restore_user(share::ObRestoreArgs& arg, const char* val);
    static int set_restore_pass(share::ObRestoreArgs& arg, const char* val);
    static int set_tcp_invited_nodes(share::ObRestoreArgs& arg, const char* val);

  private:
    share::ObRestoreArgs& arg_;
    struct Action {
      const char* key;
      Setter setter;
      bool required;
    };
    const static int ACTION_CNT = 9;
    static Action actions_[ACTION_CNT];
    bool is_set_[ACTION_CNT];
  };

  class ZoneMapCb : public share::ObKVMatchCb {
  public:
    typedef common::hash::ObHashMap<common::ObZone, common::ObZone> ZoneMap;

  public:
    ZoneMapCb(ZoneMap& zone_map) : zone_map_(zone_map){};
    int match(const char* key, const char* val);

  private:
    ZoneMap& zone_map_;
  };

private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObRestoreURIParser);
};

class ObPhysicalRestoreOptionParser {
public:
  static int parse(const common::ObString& uri, ObPhysicalRestoreJob& job);

private:
  static int parse(const char* uri, ObPhysicalRestoreJob& job);

public:
  class ExtraArgsCb : public share::ObKVMatchCb {
  public:
    ExtraArgsCb(ObPhysicalRestoreJob& job);
    int match(const char* key, const char* value);
    bool check() const;

  private:
    /* functions */
    typedef int (*Setter)(ObPhysicalRestoreJob& job, const char* val);
    static int set_restore_job_id(ObPhysicalRestoreJob& job, const char* val);
    static int set_backup_cluster_id(ObPhysicalRestoreJob& job, const char* val);
    static int set_backup_cluster_name(ObPhysicalRestoreJob& job, const char* val);
    static int set_pool_list(ObPhysicalRestoreJob& job, const char* val);
    static int set_locality(ObPhysicalRestoreJob& job, const char* val);
    static int set_primary_zone(ObPhysicalRestoreJob& job, const char* val);

  private:
    ObPhysicalRestoreJob& job_;
    struct Action {
      const char* key;
      Setter setter;
      bool required;
    };
    const static int ACTION_CNT = 7;
    static Action actions_[ACTION_CNT];
    bool is_set_[ACTION_CNT];
  };

private:
  /* variables */
  DISALLOW_COPY_AND_ASSIGN(ObPhysicalRestoreOptionParser);
};
}  // namespace share
}  // namespace oceanbase
#endif /* __OCEANBASE_SHARE_RESTORE_URI_PARSER_H__ */
//// end of header file
