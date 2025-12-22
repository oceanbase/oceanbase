/**
 * Copyright (c) 2024 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */

#define UNITTEST_DEBUG
#define USING_LOG_PREFIX SHARE
#include "lib/oblog/ob_log_module.h"
#include "lib/string/ob_string.h"
#include "lib/utility/ob_defer.h"
#include "sql/table_format/iceberg/spec/table_metadata.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <stdio.h>

using namespace oceanbase;
using namespace oceanbase::share;
namespace iceberg = oceanbase::sql::iceberg;

class TestTableMetadata : public ::testing::Test
{
public:
  TestTableMetadata();
  ~TestTableMetadata();
  virtual void SetUp();
  virtual void TearDown();
};

TestTableMetadata::TestTableMetadata() {}

TestTableMetadata::~TestTableMetadata() {}

void TestTableMetadata::SetUp() {}

void TestTableMetadata::TearDown() {}

TEST_F(TestTableMetadata, parse_from_json)
{
  ObString json_metadata = R"(
  {
    "format-version": 2,
    "table-uuid": "84565e81-1858-4a43-b97d-92944b139eb9",
    "location": "/Users/smith/Software/iceberg-warehouse/test/partition_transform_pos_delete",
    "last-sequence-number": 2,
    "last-updated-ms": 1752137591262,
    "last-column-id": 3,
    "current-schema-id": 0,
    "schemas": [
      {
        "type": "struct",
        "schema-id": 0,
        "fields": [
          {
            "id": 1,
            "name": "name",
            "required": false,
            "type": "string"
          },
          {
            "id": 2,
            "name": "age",
            "required": false,
            "type": "long"
          },
          {
            "id": 3,
            "name": "dt",
            "required": false,
            "type": "timestamptz"
          }
        ]
      }
    ],
    "default-spec-id": 0,
    "partition-specs": [
      {
        "spec-id": 0,
        "fields": [
          {
            "name": "dt_day",
            "transform": "day",
            "source-id": 3,
            "field-id": 1000
          }
        ]
      }
    ],
    "last-partition-id": 1000,
    "default-sort-order-id": 0,
    "sort-orders": [
      {
        "order-id": 0,
        "fields": []
      }
    ],
    "properties": {
      "owner": "smith",
      "write.update.mode": "merge-on-read",
      "write.delete.mode": "merge-on-read",
      "write.parquet.compression-codec": "zstd",
      "write.format.default": "orc"
    },
    "current-snapshot-id": 4351244898079697879,
    "refs": {
      "main": {
        "snapshot-id": 4351244898079697879,
        "type": "branch"
      }
    },
    "snapshots": [
      {
        "sequence-number": 1,
        "snapshot-id": 4967612336156730087,
        "timestamp-ms": 1752137541342,
        "summary": {
          "operation": "append",
          "spark.app.id": "local-1752137498909",
          "added-data-files": "3",
          "added-records": "5",
          "added-files-size": "2968",
          "changed-partition-count": "3",
          "total-records": "5",
          "total-files-size": "2968",
          "total-data-files": "3",
          "total-delete-files": "0",
          "total-position-deletes": "0",
          "total-equality-deletes": "0",
          "engine-version": "3.5.5",
          "app-id": "local-1752137498909",
          "engine-name": "spark",
          "iceberg-version": "Apache Iceberg unspecified"
        },
        "manifest-list": "/Users/smith/Software/iceberg-warehouse/test/partition_transform_pos_delete/metadata/snap-4967612336156730087-1-a2cbc449-4a2f-4dba-9064-211d1d3bb42f.avro",
        "schema-id": 0
      },
      {
        "sequence-number": 2,
        "snapshot-id": 4351244898079697879,
        "parent-snapshot-id": 4967612336156730087,
        "timestamp-ms": 1752137544538,
        "summary": {
          "operation": "overwrite",
          "spark.app.id": "local-1752137498909",
          "added-data-files": "1",
          "added-position-delete-files": "1",
          "added-delete-files": "1",
          "added-records": "1",
          "added-files-size": "2786",
          "added-position-deletes": "1",
          "changed-partition-count": "1",
          "total-records": "6",
          "total-files-size": "5754",
          "total-data-files": "4",
          "total-delete-files": "1",
          "total-position-deletes": "1",
          "total-equality-deletes": "0",
          "engine-version": "3.5.5",
          "app-id": "local-1752137498909",
          "engine-name": "spark",
          "iceberg-version": "Apache Iceberg unspecified"
        },
        "manifest-list": "/Users/smith/Software/iceberg-warehouse/test/partition_transform_pos_delete/metadata/snap-4351244898079697879-1-4b056f6a-47c2-4146-89bb-97721542c031.avro",
        "schema-id": 0
      }
    ],
    "statistics": [
      {
        "snapshot-id": 4351244898079697879,
        "statistics-path": "/Users/smith/Software/iceberg-warehouse/test/partition_transform_pos_delete/metadata/4351244898079697879-141a66ef-243c-46c6-b669-b4aebf79a3a3.stats",
        "file-size-in-bytes": 906,
        "file-footer-size-in-bytes": 703,
        "blob-metadata": [
          {
            "type": "apache-datasketches-theta-v1",
            "snapshot-id": 4351244898079697879,
            "sequence-number": 2,
            "fields": [
              1
            ],
            "properties": {
              "ndv": "5"
            }
          },
          {
            "type": "apache-datasketches-theta-v1",
            "snapshot-id": 4351244898079697879,
            "sequence-number": 2,
            "fields": [
              2
            ],
            "properties": {
              "ndv": "5"
            }
          },
          {
            "type": "apache-datasketches-theta-v1",
            "snapshot-id": 4351244898079697879,
            "sequence-number": 2,
            "fields": [
              3
            ],
            "properties": {
              "ndv": "4"
            }
          }
        ]
      }
    ],
    "partition-statistics": [],
    "snapshot-log": [
      {
        "timestamp-ms": 1752137541342,
        "snapshot-id": 4967612336156730087
      },
      {
        "timestamp-ms": 1752137544538,
        "snapshot-id": 4351244898079697879
      }
    ],
    "metadata-log": [
      {
        "timestamp-ms": 1752137536823,
        "metadata-file": "/Users/smith/Software/iceberg-warehouse/test/partition_transform_pos_delete/metadata/v1.metadata.json"
      },
      {
        "timestamp-ms": 1752137541342,
        "metadata-file": "/Users/smith/Software/iceberg-warehouse/test/partition_transform_pos_delete/metadata/v2.metadata.json"
      },
      {
        "timestamp-ms": 1752137544538,
        "metadata-file": "/Users/smith/Software/iceberg-warehouse/test/partition_transform_pos_delete/metadata/v3.metadata.json"
      }
    ]
  })";

  ObArenaAllocator allocator;
  iceberg::TableMetadata table_metadata(allocator);

  ObJsonNode *json_node = NULL;
  ObJsonParser::get_tree(&allocator, json_metadata, json_node);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, json_node->json_type());
  ASSERT_EQ(OB_SUCCESS, table_metadata.init_from_json(*down_cast<ObJsonObject *>(json_node)));
  iceberg::DataFileFormat data_file_format;
  ASSERT_EQ(OB_SUCCESS, table_metadata.get_table_default_write_format(data_file_format));
  ASSERT_EQ(iceberg::DataFileFormat::ORC, data_file_format);
}

TEST_F(TestTableMetadata, parse_from_json_with_ref)
{
  ObString json_metadata = R"(
  {
      "format-version": 2,
      "table-uuid": "09df4302-b6ab-4d60-b205-c6bc816bc2e7",
      "location": "oss://antsys-ob-sql-bucket-prod/zhengjin/default/test_time_travel",
      "last-sequence-number": 2,
      "last-updated-ms": 1759132297897,
      "last-column-id": 3,
      "current-schema-id": 0,
      "schemas":
      [
          {
              "type": "struct",
              "schema-id": 0,
              "fields":
              [
                  {
                      "id": 1,
                      "name": "dt",
                      "required": false,
                      "type": "date"
                  },
                  {
                      "id": 2,
                      "name": "number",
                      "required": false,
                      "type": "int"
                  },
                  {
                      "id": 3,
                      "name": "letter",
                      "required": false,
                      "type": "string"
                  }
              ]
          }
      ],
      "default-spec-id": 0,
      "partition-specs":
      [
          {
              "spec-id": 0,
              "fields":
              []
          }
      ],
      "last-partition-id": 999,
      "default-sort-order-id": 0,
      "sort-orders":
      [
          {
              "order-id": 0,
              "fields":
              []
          }
      ],
      "properties":
      {
          "owner": "root",
          "write.parquet.compression-codec": "zstd"
      },
      "current-snapshot-id": 6811305257257310519,
      "refs":
      {
          "main":
          {
              "snapshot-id": 6811305257257310519,
              "type": "branch"
          },
          "before_tag":
          {
              "snapshot-id": 4208412434246510602,
              "type": "tag"
          }
      },
      "snapshots":
      [
          {
              "sequence-number": 1,
              "snapshot-id": 4208412434246510602,
              "timestamp-ms": 1759132170812,
              "summary":
              {
                  "operation": "append",
                  "spark.app.id": "local-1759131857600",
                  "added-data-files": "6",
                  "added-records": "6",
                  "added-files-size": "5153",
                  "changed-partition-count": "1",
                  "total-records": "6",
                  "total-files-size": "5153",
                  "total-data-files": "6",
                  "total-delete-files": "0",
                  "total-position-deletes": "0",
                  "total-equality-deletes": "0",
                  "engine-version": "3.5.5",
                  "app-id": "local-1759131857600",
                  "engine-name": "spark",
                  "iceberg-version": "Apache Iceberg 1.10.0"
              },
              "manifest-list": "oss://antsys-ob-sql-bucket-prod/zhengjin/default/test_time_travel/metadata/snap-4208412434246510602-1-a0f0d0fb-c5ec-491b-a0a8-5877667626d1.avro",
              "schema-id": 0
          },
          {
              "sequence-number": 2,
              "snapshot-id": 6811305257257310519,
              "parent-snapshot-id": 4208412434246510602,
              "timestamp-ms": 1759132208513,
              "summary":
              {
                  "operation": "append",
                  "spark.app.id": "local-1759131857600",
                  "added-data-files": "6",
                  "added-records": "6",
                  "added-files-size": "5154",
                  "changed-partition-count": "1",
                  "total-records": "12",
                  "total-files-size": "10307",
                  "total-data-files": "12",
                  "total-delete-files": "0",
                  "total-position-deletes": "0",
                  "total-equality-deletes": "0",
                  "engine-version": "3.5.5",
                  "app-id": "local-1759131857600",
                  "engine-name": "spark",
                  "iceberg-version": "Apache Iceberg 1.10.0"
              },
              "manifest-list": "oss://antsys-ob-sql-bucket-prod/zhengjin/default/test_time_travel/metadata/snap-6811305257257310519-1-f82846c4-a6ea-41c2-8b68-1c9a32a4c699.avro",
              "schema-id": 0
          }
      ],
      "statistics":
      [],
      "partition-statistics":
      [],
      "snapshot-log":
      [
          {
              "timestamp-ms": 1759132170812,
              "snapshot-id": 4208412434246510602
          },
          {
              "timestamp-ms": 1759132208513,
              "snapshot-id": 6811305257257310519
          }
      ],
      "metadata-log":
      [
          {
              "timestamp-ms": 1759132149744,
              "metadata-file": "oss://antsys-ob-sql-bucket-prod/zhengjin/default/test_time_travel/metadata/v1.metadata.json"
          },
          {
              "timestamp-ms": 1759132170812,
              "metadata-file": "oss://antsys-ob-sql-bucket-prod/zhengjin/default/test_time_travel/metadata/v2.metadata.json"
          },
          {
              "timestamp-ms": 1759132208513,
              "metadata-file": "oss://antsys-ob-sql-bucket-prod/zhengjin/default/test_time_travel/metadata/v3.metadata.json"
          }
      ]
  }
  )";

  ObArenaAllocator allocator;
  iceberg::TableMetadata table_metadata(allocator);

  ObJsonNode *json_node = NULL;
  ObJsonParser::get_tree(&allocator, json_metadata, json_node);
  ASSERT_EQ(ObJsonNodeType::J_OBJECT, json_node->json_type());
  ASSERT_EQ(OB_SUCCESS, table_metadata.init_from_json(*down_cast<ObJsonObject *>(json_node)));
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}