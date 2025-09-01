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
#include "sql/table_format/iceberg/scan/conversions.h"
#include "sql/table_format/iceberg/scan/delete_file_index.h"
#include "sql/table_format/iceberg/spec/manifest.h"
#include "sql/table_format/iceberg/spec/schema.h"

#include <gmock/gmock.h>
#include <gtest/gtest.h>

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace sql::iceberg;
class TestDeleteFileIndex : public ::testing::Test
{
public:
  TestDeleteFileIndex() = default;
  ~TestDeleteFileIndex() = default;

protected:
  ManifestEntry *create_data_file(int64_t seq,
                                  int32_t partition_spec_id,
                                  int64_t partition_value,
                                  ObString path)
  {
    PartitionSpec *partition_spec = create_partition_spec(partition_spec_id);
    ObObj partition_value_obj;
    partition_value_obj.set_int(partition_value);
    ObArray<ObObj> partition_values;
    partition_values.push_back(partition_value_obj);
    ManifestEntry *entry = create_manifest_entry(seq,
                                                 path,
                                                 DataFileContent::DATA,
                                                 DataFileFormat::PARQUET,
                                                 std::nullopt,
                                                 partition_spec->spec_id,
                                                 partition_spec,
                                                 &partition_values);
    return entry;
  }

  PartitionSpec *create_partition_spec(int32_t spec_id)
  {
    PartitionSpec *partition_spec = OB_NEWx(PartitionSpec, &allocator_, allocator_);
    partition_spec->spec_id = spec_id;
    PartitionField *partition_field = OB_NEWx(PartitionField, &allocator_, allocator_);
    partition_field->name = "dt";
    partition_field->transform.transform_type = TransformType::Identity;
    partition_spec->fields.push_back(partition_field);
    return partition_spec;
  }

  ManifestEntry *create_dv(int64_t seq, ObString path, ObString referenced_data_file)
  {
    return create_manifest_entry(seq,
                                 path,
                                 DataFileContent::POSITION_DELETES,
                                 DataFileFormat::PUFFIN,
                                 referenced_data_file);
  }

  ManifestEntry *create_pos_path(int64_t seq, ObString path, ObString referenced_data_file)
  {
    return create_manifest_entry(seq,
                                 path,
                                 DataFileContent::POSITION_DELETES,
                                 DataFileFormat::PARQUET,
                                 referenced_data_file);
  }

  ManifestEntry *create_global_eq(int64_t seq, ObString path)
  {
    PartitionSpec *unpartitioned = OB_NEWx(PartitionSpec, &allocator_, allocator_);
    unpartitioned->convert_to_unpartitioned();
    return create_manifest_entry(seq,
                                 path,
                                 DataFileContent::EQUALITY_DELETES,
                                 DataFileFormat::PARQUET,
                                 std::nullopt,
                                 unpartitioned->spec_id,
                                 unpartitioned);
  }

  ManifestEntry *create_par_delete(int64_t seq,
                                   ObString path,
                                   DataFileContent content,
                                   int32_t partition_spec_id,
                                   int64_t partition_value)
  {
    PartitionSpec *partition_spec = create_partition_spec(partition_spec_id);
    ObArray<ObObj> partition_values;
    ObObj partition_value_obj;
    partition_value_obj.set_int(partition_value);
    partition_values.push_back(partition_value_obj);
    ManifestEntry *entry = create_manifest_entry(seq,
                                                 path,
                                                 content,
                                                 DataFileFormat::PARQUET,
                                                 std::nullopt,
                                                 partition_spec_id,
                                                 partition_spec,
                                                 &partition_values);
    return entry;
  }

  ManifestEntry *create_manifest_entry(int64_t seq,
                                       ObString path,
                                       DataFileContent content,
                                       DataFileFormat file_format = DataFileFormat::PARQUET,
                                       std::optional<ObString> referenced_data_file = std::nullopt,
                                       std::optional<int32_t> partition_spec_id = std::nullopt,
                                       PartitionSpec *partition_spec = NULL,
                                       ObIArray<ObObj> *partition_values = NULL)
  {
    ManifestEntry *entry = OB_NEWx(ManifestEntry, &allocator_, allocator_);
    entry->sequence_number = seq;
    entry->data_file.content = content;
    entry->data_file.file_path = path;

    entry->data_file.file_format = file_format;

    if (referenced_data_file.has_value()) {
      entry->data_file.referenced_data_file = referenced_data_file.value();
    }

    if (partition_spec_id.has_value()) {
      entry->partition_spec_id = partition_spec_id.value();
    }

    if (partition_spec != NULL) {
      entry->partition_spec.assign(*partition_spec);
    }

    if (partition_values != NULL) {
      entry->data_file.partition.assign(*partition_values);
    }

    return entry;
  }

  ObArenaAllocator allocator_;
};

TEST_F(TestDeleteFileIndex, find_pos_deletes)
{
  ObArray<const ManifestEntry *> pos_deletes;
  pos_deletes.push_back(create_manifest_entry(5, "5", DataFileContent::POSITION_DELETES));
  pos_deletes.push_back(create_manifest_entry(3, "3", DataFileContent::POSITION_DELETES));
  pos_deletes.push_back(create_manifest_entry(4, "4-2", DataFileContent::POSITION_DELETES));
  pos_deletes.push_back(create_manifest_entry(4, "4-1", DataFileContent::POSITION_DELETES));
  pos_deletes.push_back(create_manifest_entry(2, "2", DataFileContent::POSITION_DELETES));
  pos_deletes.push_back(create_manifest_entry(1, "1", DataFileContent::POSITION_DELETES));

  DeleteFileIndex::sort_delete_files(pos_deletes);

  ManifestEntry *data_file = create_manifest_entry(4, "data", DataFileContent::DATA);

  ObArray<const ManifestEntry *> result;
  ASSERT_EQ(OB_SUCCESS, DeleteFileIndex::find_pos_deletes(*data_file, pos_deletes, result));
  EXPECT_EQ(3, result.size());
  for (int32_t i = 0; i < result.size(); i++) {
    if (0 == result.at(i)->data_file.file_path.case_compare("4-1")) {
      // nothing
    } else if (0 == result.at(i)->data_file.file_path.case_compare("4-2")) {
      // nothing
    } else if (0 == result.at(i)->data_file.file_path.case_compare("5")) {
      // nothing
    } else {
      EXPECT_TRUE(false);
    }
  }
}

TEST_F(TestDeleteFileIndex, find_eq_deletes)
{
  ObArray<const ManifestEntry *> pos_deletes;
  pos_deletes.push_back(create_manifest_entry(5, "5", DataFileContent::EQUALITY_DELETES));
  pos_deletes.push_back(create_manifest_entry(3, "3", DataFileContent::EQUALITY_DELETES));
  pos_deletes.push_back(create_manifest_entry(4, "4-2", DataFileContent::EQUALITY_DELETES));
  pos_deletes.push_back(create_manifest_entry(4, "4-1", DataFileContent::EQUALITY_DELETES));
  pos_deletes.push_back(create_manifest_entry(2, "2", DataFileContent::EQUALITY_DELETES));
  pos_deletes.push_back(create_manifest_entry(1, "1", DataFileContent::EQUALITY_DELETES));

  DeleteFileIndex::sort_delete_files(pos_deletes);

  ManifestEntry *data_file = create_manifest_entry(4, "data", DataFileContent::DATA);

  ObArray<const ManifestEntry *> result;
  ASSERT_EQ(OB_SUCCESS, DeleteFileIndex::find_eq_deletes(*data_file, pos_deletes, result));
  EXPECT_EQ(1, result.size());
  for (int32_t i = 0; i < result.size(); i++) {
    if (0 == result.at(i)->data_file.file_path.case_compare("5")) {
      // nothing
    } else {
      EXPECT_TRUE(false);
    }
  }
}

// test dv + global eq + partition eq
TEST_F(TestDeleteFileIndex, dv_global_eq_partition_eq)
{
  ObArray<const ManifestEntry *> delete_files;
  {
    // create dv
    ManifestEntry *entry = create_dv(5, "dv", "datafile");
    delete_files.push_back(entry);
  }

  {
    // create global
    ManifestEntry *entry = create_global_eq(5, "global_eq-5");
    delete_files.push_back(entry);
    entry = create_global_eq(6, "global_eq-6");
    delete_files.push_back(entry);
  }

  {
    // seq=5, eq + part(1)
    ManifestEntry *entry
        = create_par_delete(5, "eq_par-5", DataFileContent::EQUALITY_DELETES, 0, 1);
    delete_files.push_back(entry);
  }

  {
    // seq=6, eq + part(1)
    ManifestEntry *entry
        = create_par_delete(6, "eq_par-6-1", DataFileContent::EQUALITY_DELETES, 0, 1);
    delete_files.push_back(entry);
  }

  {
    // seq=6, eq + part(2)
    ManifestEntry *entry
        = create_par_delete(6, "eq_par-6-2", DataFileContent::EQUALITY_DELETES, 0, 2);
    delete_files.push_back(entry);
  }

  ManifestEntry *datafile = create_data_file(5, 0, 1, "datafile");

  DeleteFileIndex delete_file_index;
  delete_file_index.init(delete_files);
  ObArray<const ManifestEntry *> dv_result;
  ObArray<const ManifestEntry *> eq_result;
  ObArray<const ManifestEntry *> pos_result;
  delete_file_index.match_delete_files(*datafile, pos_result, eq_result, dv_result);

  ASSERT_EQ(1, dv_result.count());
  ASSERT_EQ(ObString("dv"), dv_result.at(0)->data_file.file_path);
  ASSERT_EQ(0, pos_result.count());
  ASSERT_EQ(2, eq_result.count());
  for (int32_t i = 0; i < eq_result.size(); i++) {
    if (eq_result.at(i)->data_file.file_path == ObString("global_eq-6")
        || eq_result.at(i)->data_file.file_path == ObString("eq_par-6-1")) {
    } else {
      ASSERT_TRUE(false);
    }
  }
}

// test pos path + pos partition + global eq + partition eq
TEST_F(TestDeleteFileIndex, pos_path_pos_partition_global_eq_partition_eq)
{
  ObArray<const ManifestEntry *> delete_files;
  {
    // create pos + path
    ManifestEntry *entry = create_pos_path(5, "pos_path", "datafile");
    delete_files.push_back(entry);
  }

  {
    // seq=5, pos + part(1)
    ManifestEntry *entry
        = create_par_delete(5, "pos_par-5", DataFileContent::POSITION_DELETES, 0, 1);
    delete_files.push_back(entry);
  }

  {
    // seq=6, pos + part(1)
    ManifestEntry *entry
        = create_par_delete(6, "pos_par-6-1", DataFileContent::POSITION_DELETES, 0, 1);
    delete_files.push_back(entry);
  }

  {
    // seq=6, pos + part(2)
    ManifestEntry *entry
        = create_par_delete(6, "pos_par-6-2", DataFileContent::POSITION_DELETES, 0, 2);
    delete_files.push_back(entry);
  }

  {
    // create global
    ManifestEntry *entry = create_global_eq(5, "global_eq-5");
    delete_files.push_back(entry);
    entry = create_global_eq(6, "global_eq-6");
    delete_files.push_back(entry);
  }

  {
    // seq=5, eq + part(1)
    ManifestEntry *entry
        = create_par_delete(5, "eq_par-5", DataFileContent::EQUALITY_DELETES, 0, 1);
    delete_files.push_back(entry);
  }

  {
    // seq=6, eq + part(1)
    ManifestEntry *entry
        = create_par_delete(6, "eq_par-6-1", DataFileContent::EQUALITY_DELETES, 0, 1);
    delete_files.push_back(entry);
  }

  {
    // seq=6, eq + part(2)
    ManifestEntry *entry
        = create_par_delete(6, "eq_par-6-2", DataFileContent::EQUALITY_DELETES, 0, 2);
    delete_files.push_back(entry);
  }

  ManifestEntry *datafile = create_data_file(5, 0, 1, "datafile");

  DeleteFileIndex delete_file_index;
  delete_file_index.init(delete_files);
  ObArray<const ManifestEntry *> dv_result;
  ObArray<const ManifestEntry *> eq_result;
  ObArray<const ManifestEntry *> pos_result;
  delete_file_index.match_delete_files(*datafile, pos_result, eq_result, dv_result);

  ASSERT_EQ(0, dv_result.count());
  ASSERT_EQ(3, pos_result.count());
  for (int32_t i = 0; i < pos_result.size(); i++) {
    if (pos_result.at(i)->data_file.file_path == ObString("pos_path")
        || pos_result.at(i)->data_file.file_path == ObString("pos_par-5")
        || pos_result.at(i)->data_file.file_path == ObString("pos_par-6-1")) {
    } else {
      ASSERT_TRUE(false);
    }
  }
  ASSERT_EQ(2, eq_result.count());
  for (int32_t i = 0; i < eq_result.size(); i++) {
    if (eq_result.at(i)->data_file.file_path == ObString("global_eq-6")
        || eq_result.at(i)->data_file.file_path == ObString("eq_par-6-1")) {
    } else {
      ASSERT_TRUE(false);
    }
  }
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  OB_LOGGER.set_log_level("INFO");
  return RUN_ALL_TESTS();
}