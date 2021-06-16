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

#ifndef OCEANBASE_OBSERVER_ALL_VIRTUAL_ZONE_STAT_H
#define OCEANBASE_OBSERVER_ALL_VIRTUAL_ZONE_STAT_H
#include "share/ob_virtual_table_projector.h"
#include "share/ob_zone_info.h"
#include "lib/string/ob_string.h"
#include "lib/container/ob_array.h"
namespace oceanbase {
namespace common {
class ObMySQLProxy;
class ObSqlString;
class ObString;
}  // namespace common
namespace share {
namespace schema {
class ObTableSchema;
class ObMultiVersionSchemaService;
}  // namespace schema
}  // namespace share
namespace observer {
class ObAllVirtualZoneStat : public common::ObVirtualTableProjector {
public:
  struct ZoneStat {
    ZoneStat();
    ~ZoneStat()
    {}
    bool is_valid() const;
    void reset();
    TO_STRING_KV(K_(zone_info), K_(server_count), K_(resource_pool_count), K_(unit_count));

    share::ObZoneInfo zone_info_;
    int64_t server_count_;
    int64_t resource_pool_count_;
    int64_t unit_count_;
  };

  ObAllVirtualZoneStat();
  virtual ~ObAllVirtualZoneStat();
  int init(share::schema::ObMultiVersionSchemaService& schema_service, common::ObMySQLProxy* sql_proxy);
  virtual int inner_get_next_row(common::ObNewRow*& row);

private:
  int get_all_stat();
  int get_full_row(
      const share::schema::ObTableSchema* table, const ZoneStat& zone_stat, common::ObIArray<Column>& columns);

  DISALLOW_COPY_AND_ASSIGN(ObAllVirtualZoneStat);

private:
  bool inited_;
  common::ObArray<ZoneStat> zone_stats_;
  common::ObFixedLengthString<common::MAX_ZONE_INFO_LENGTH> cluster_name_;
  share::schema::ObMultiVersionSchemaService* schema_service_;
  common::ObMySQLProxy* sql_proxy_;
};
}  // namespace observer
}  // namespace oceanbase
#endif
