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

#include <avro/Compiler.hh>
#include <avro/ValidSchema.hh>
#include <avro/DataFile.hh>
#include <avro/Stream.hh>
#include <avro/Generic.hh>
#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "lib/oblog/ob_log_module.h"
#include "lib/utility/ob_print_utils.h"
#include "lib/string/ob_string.h"
#include "src/sql/table_format/iceberg/spec/schema_field.h"
#include "src/sql/table_format/iceberg/spec/manifest.h"
#include "src/sql/table_format/iceberg/spec/table_metadata.h"
#include "src/sql/table_format/iceberg/spec/type.h"
#include "src/sql/table_format/iceberg/avro_schema_util.h"
#include "src/sql/table_format/iceberg/write/avro_data_file_writer.h"
#include "src/sql/table_format/iceberg/write/ob_snapshot_producer.h"
#include "src/sql/table_format/iceberg/write/metadata_helper.h"

using namespace std;
using namespace oceanbase;
using namespace oceanbase::common;
using namespace oceanbase::sql::iceberg;
class TestAvroWrite : public ::testing::Test
{
public:
  TestAvroWrite() = default;
  ~TestAvroWrite() = default;
  ObArenaAllocator allocator_;
};
struct Person {
  long ID;
  std::string First;
  Person() : ID(0), First("") {}
  Person(long id, const char* first) : ID(id), First(first) {}
};
namespace avro {
template<>
struct codec_traits<Person> {
    static void encode(Encoder &e, const Person &v) {
        avro::encode(e, v.ID);
        avro::encode(e, v.First);
    }
    static void decode(Decoder &d, Person &v) {
        avro::decode(d, v.ID);
        avro::decode(d, v.First);
    }
};

}
TEST_F(TestAvroWrite, test_simple)
{
  const char PERSON_SCHEMA[]
      = R"({"type":"record", "name":"Person", "fields":[{"name": "ID", "type": "long"}, {"name": "First", "type": "string"}]})";
  ::avro::ValidSchema schema = ::avro::compileJsonSchemaFromString(PERSON_SCHEMA);
  ASSERT_EQ(avro::Type::AVRO_RECORD, schema.root().get()->type());


  sql::iceberg::DataFileWriter<Person> writer("test_avro1", schema);
  Person p1(100, "jack");
  writer.write(p1);
  writer.close();

  avro::DataFileReader<Person> reader("test_avro1", schema);
  Person p2;
  while (reader.read(p2)) {
    ASSERT_EQ(p2.ID, 100);
    ASSERT_EQ(p2.First, "jack");
  }

}

TEST_F(TestAvroWrite, test_visitor)
{
  ::avro::NodePtr node;
  ObString root_name = "test_visitor";
  ToAvroNodeVisitor visitor(root_name);
  visitor.visit(IntType{}, node);
  ASSERT_EQ(avro::Type::AVRO_INT, node->type());

  ObArray<const SchemaField*> array;
  const ObString name1 = "bool_field";
  const ObString name2 = "int_field";
  const ObString doc1 = "doc1";
  const ObString doc2 = "doc2";
  const char* json_schema = R"({
    "type": "record",
    "name": "test_visitor",
    "fields": [
        {
            "name": "bool_field",
            "type": "boolean",
            "doc": "doc1",
            "field-id": 1
        },
        {
            "name": "int_field",
            "type": [
                "null",
                "int"
            ],
            "default": null,
            "doc": "doc2",
            "field-id": 2
        }
    ]
}
)";
  SchemaField bool_field{1, name1, BooleanType::get_instance(), false, doc1};
  SchemaField int_field{2, name2, IntType::get_instance(), true, doc2};
  StructType struct_type({&bool_field, &int_field});
  int ret = OB_SUCCESS;
  try {
    if (OB_FAIL(visitor.visit(struct_type, node))) {
      LOG_WARN("failed to visit", K(ret));
    } else {
      ASSERT_EQ(node->type(), ::avro::AVRO_RECORD);

      ASSERT_EQ(node->names(), 2);
      ASSERT_EQ(node->nameAt(0), "bool_field");
      ASSERT_EQ(node->nameAt(1), "int_field");

      ASSERT_EQ(node->customAttributes(), 2);

      ASSERT_EQ(node->leaves(), 2);
      ASSERT_EQ(node->leafAt(0)->type(), ::avro::AVRO_BOOL);
      ASSERT_EQ(node->leafAt(1)->type(), ::avro::AVRO_UNION);
      ASSERT_EQ(node->leafAt(1)->leaves(), 2);
      ASSERT_EQ(node->leafAt(1)->leafAt(0)->type(), ::avro::AVRO_NULL);
      ASSERT_EQ(node->leafAt(1)->leafAt(1)->type(), ::avro::AVRO_INT);

      avro::ValidSchema schema(node);
      std::ostringstream actual;
      schema.toJson(actual);
      ASSERT_EQ(actual.str(), json_schema);

      sql::iceberg::DataFileWriter<avro::GenericDatum> writer("test_visitor", schema);
      avro::GenericDatum datum(schema);
      ASSERT_EQ(datum.type(), avro::Type::AVRO_RECORD);
      auto &r = datum.value<avro::GenericRecord>();
      ASSERT_EQ(r.fieldAt(0).type(), avro::Type::AVRO_BOOL);
      ASSERT_EQ(r.fieldAt(1).type(), avro::Type::AVRO_NULL);
      r.fieldAt(1).selectBranch(1);
      ASSERT_EQ(r.fieldAt(1).type(), avro::Type::AVRO_INT);

      r.fieldAt(0) = true;
      r.fieldAt(1).value<int32_t>() = 1;
      writer.write(datum);
      writer.close();

      avro::DataFileReader<avro::GenericDatum> reader("test_visitor", schema);

      while (reader.read(datum)) {
        auto &r = datum.value<avro::GenericRecord>();
        r.fieldAt(1).selectBranch(1);
        ASSERT_EQ(r.fieldAt(0).value<bool>(), true);
        ASSERT_EQ(r.fieldAt(1).value<int32_t>(), 1);
      }
    }
  } catch (const std::exception& ex) {
    LOG_WARN("exeception", K(ex.what()));
  }
}

TEST_F(TestAvroWrite, test_manifest_entry_visitor)
{
  int ret = OB_SUCCESS;
  ManifestEntry manifest_entry(allocator_);
  manifest_entry.status = ManifestEntryStatus::ADDED;
  manifest_entry.snapshot_id = 5836641602751894588;
  manifest_entry.sequence_number = 1;
  manifest_entry.file_sequence_number = 1;
  manifest_entry.data_file.content = DataFileContent::DATA;
  manifest_entry.data_file.file_path = "test_datafile";
  manifest_entry.data_file.file_format = DataFileFormat::PARQUET;
  manifest_entry.data_file.record_count = 1;
  manifest_entry.data_file.file_size_in_bytes = 100;
  char *buf = NULL;
  int64_t buf_len = DEFAULT_BUF_LENGTH;
  int64_t pos = 0;
  ObString iceberg_schema;
  V2Metadata* metadata_helper = nullptr;
  try {
    if (OB_ISNULL(metadata_helper = OB_NEWx(V2Metadata, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc metadata_helper", K(ret));
    } else if (OB_FAIL(metadata_helper->init_manifest_entry_node())) {
      LOG_WARN("failed to init manifest entry node", K(ret));
    } else if (OB_ISNULL(buf = static_cast<char *>(allocator_.alloc(buf_len)))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc buf", K(ret), K(buf_len));
    } else if (OB_ISNULL(metadata_helper->get_manifest_entry_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("manifest_entry_type is NULL", K(ret));
    } else if (OB_FALSE_IT(metadata_helper->get_manifest_entry_type()->set_schema_id(0))) {
    } else if (OB_FAIL(metadata_helper->get_manifest_entry_type()->to_json_kv_string(buf, buf_len, pos))) {
      LOG_WARN("failed to convert manifest entry struct to json", K(ret));
    } else {
      iceberg_schema.assign_ptr(buf, pos);
      avro::ValidSchema schema(metadata_helper->get_manifest_entry_node());
      std::map<std::string, std::string> metadata;
      metadata["schema-id"] = "0";
      metadata["format-version"] = "2";
      metadata["iceberg.schema"] = std::string(iceberg_schema.ptr(), iceberg_schema.length());
      metadata["partition-spec-id"] = "0";
      metadata["partition-spec"] = "[]";
      metadata["content"] = "data";
      sql::iceberg::DataFileWriter<avro::GenericDatum> writer("test_manifest_entry_visitor",
                                                              schema,
                                                              metadata,
                                                              16 * 1024,
                                                              avro::Codec::DEFLATE_CODEC);
      avro::GenericDatum* datum = nullptr;
      if (OB_FAIL(metadata_helper->convert_to_avro(manifest_entry, datum))) {
        LOG_WARN("failed to convert_to_avro", K(ret));
      } else {
        writer.write(*datum);
        writer.close();

        avro::DataFileReader<avro::GenericDatum> reader("test_manifest_entry_visitor", schema);
        while (reader.read(*datum)) {
          ASSERT_EQ(datum->type(), avro::Type::AVRO_RECORD);
          auto &manifest_entry_record = datum->value<avro::GenericRecord>();
          ASSERT_EQ(manifest_entry_record.fieldAt(0).type(), avro::Type::AVRO_INT);
          ASSERT_EQ(manifest_entry_record.fieldAt(0).value<int32_t>(), 1);

          ASSERT_EQ(manifest_entry_record.fieldAt(1).isUnion(), true);
          manifest_entry_record.fieldAt(1).selectBranch(1);
          ASSERT_EQ(manifest_entry_record.fieldAt(1).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_entry_record.fieldAt(1).value<int64_t>(), 5836641602751894588);

          ASSERT_EQ(manifest_entry_record.fieldAt(2).isUnion(), true);
          manifest_entry_record.fieldAt(2).selectBranch(1);
          ASSERT_EQ(manifest_entry_record.fieldAt(2).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_entry_record.fieldAt(2).value<int64_t>(), 1);

          ASSERT_EQ(manifest_entry_record.fieldAt(3).isUnion(), true);
          manifest_entry_record.fieldAt(3).selectBranch(1);
          ASSERT_EQ(manifest_entry_record.fieldAt(3).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_entry_record.fieldAt(3).value<int64_t>(), 1);

          ASSERT_EQ(manifest_entry_record.fieldAt(4).type(), avro::Type::AVRO_RECORD);
          auto &r = manifest_entry_record.fieldAt(4).value<avro::GenericRecord>();
          ASSERT_EQ(r.fieldAt(0).type(), avro::Type::AVRO_INT);
          ASSERT_EQ(r.fieldAt(0).value<int32_t>(), 0);
          ASSERT_EQ(r.fieldAt(1).type(), avro::Type::AVRO_STRING);
          ASSERT_EQ(r.fieldAt(1).value<std::string>(), "test_datafile");
          ASSERT_EQ(r.fieldAt(2).type(), avro::Type::AVRO_STRING);
          ASSERT_EQ(r.fieldAt(2).value<std::string>(), "PARQUET");
          ASSERT_EQ(r.fieldAt(3).type(), avro::Type::AVRO_RECORD);
          ASSERT_EQ(r.fieldAt(4).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(r.fieldAt(4).value<int64_t>(), 1);
          ASSERT_EQ(r.fieldAt(5).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(r.fieldAt(5).value<int64_t>(), 100);
          for (int i = 6; i <= 16; ++i) {
            ASSERT_EQ(r.fieldAt(i).type(), avro::Type::AVRO_NULL);
          }
        }
      }
    }
  } catch (const std::exception& ex) {
    LOG_WARN("exeception", K(ex.what()));
  }
}

TEST_F(TestAvroWrite, test_manifest_file_visitor)
{
  int ret = OB_SUCCESS;
  V2Metadata* metadata_helper = nullptr;
  ManifestFile manifest_file(allocator_);
  manifest_file.manifest_path = "test_manifest_file";
  manifest_file.manifest_length = 7053;
  manifest_file.partition_spec_id = 0;
  manifest_file.content = ManifestContent::DATA;
  manifest_file.sequence_number = 1;
  manifest_file.min_sequence_number = 1;
  manifest_file.added_snapshot_id = 5836641602751894588;
  manifest_file.added_files_count = 1;
  manifest_file.existing_files_count = 0;
  manifest_file.deleted_files_count = 0;
  manifest_file.added_rows_count = 1;
  manifest_file.existing_rows_count = 0;
  manifest_file.deleted_rows_count = 0;
  try {
    if (OB_ISNULL(metadata_helper = OB_NEWx(V2Metadata, &allocator_, allocator_))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc metadata", K(ret));
    } else if (OB_FAIL(metadata_helper->init_manifest_file_node())) {
      LOG_WARN("failed to init manifest file node", K(ret));
    } else if (OB_ISNULL(metadata_helper->get_manifest_file_type())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("manifest file type is null", K(ret));
    } else {
      avro::ValidSchema schema(metadata_helper->get_manifest_file_node());
      sql::iceberg::DataFileWriter<avro::GenericDatum> writer("test_manifest_file_visitor", schema);
      avro::GenericDatum* datum = nullptr;
      if (OB_FAIL(metadata_helper->convert_to_avro(manifest_file, datum))) {
        LOG_WARN("failed to convert_to_avro", K(ret));
      } else {
        writer.write(*datum);
        writer.close();
        avro::DataFileReader<avro::GenericDatum> reader("test_manifest_file_visitor", schema);
        while (reader.read(*datum)) {
          ASSERT_EQ(datum->type(), avro::Type::AVRO_RECORD);
          auto &manifest_file_record = datum->value<avro::GenericRecord>();
          ASSERT_EQ(manifest_file_record.fieldAt(0).type(), avro::Type::AVRO_STRING);
          ASSERT_EQ(manifest_file_record.fieldAt(0).value<std::string>(), "test_manifest_file");

          ASSERT_EQ(manifest_file_record.fieldAt(1).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_file_record.fieldAt(1).value<int64_t>(), 7053);

          ASSERT_EQ(manifest_file_record.fieldAt(2).type(), avro::Type::AVRO_INT);
          ASSERT_EQ(manifest_file_record.fieldAt(2).value<int32_t>(), 0);

          ASSERT_EQ(manifest_file_record.fieldAt(3).type(), avro::Type::AVRO_INT);
          ASSERT_EQ(manifest_file_record.fieldAt(3).value<int32_t>(), 0);

          ASSERT_EQ(manifest_file_record.fieldAt(4).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_file_record.fieldAt(4).value<int64_t>(), 1);

          ASSERT_EQ(manifest_file_record.fieldAt(5).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_file_record.fieldAt(5).value<int64_t>(), 1);

          ASSERT_EQ(manifest_file_record.fieldAt(6).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_file_record.fieldAt(6).value<int64_t>(), 5836641602751894588);

          ASSERT_EQ(manifest_file_record.fieldAt(7).type(), avro::Type::AVRO_INT);
          ASSERT_EQ(manifest_file_record.fieldAt(7).value<int32_t>(), 1);

          ASSERT_EQ(manifest_file_record.fieldAt(8).type(), avro::Type::AVRO_INT);
          ASSERT_EQ(manifest_file_record.fieldAt(8).value<int32_t>(), 0);

          ASSERT_EQ(manifest_file_record.fieldAt(9).type(), avro::Type::AVRO_INT);
          ASSERT_EQ(manifest_file_record.fieldAt(9).value<int32_t>(), 0);

          ASSERT_EQ(manifest_file_record.fieldAt(10).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_file_record.fieldAt(10).value<int64_t>(), 1);

          ASSERT_EQ(manifest_file_record.fieldAt(11).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_file_record.fieldAt(11).value<int64_t>(), 0);

          ASSERT_EQ(manifest_file_record.fieldAt(12).type(), avro::Type::AVRO_LONG);
          ASSERT_EQ(manifest_file_record.fieldAt(12).value<int64_t>(), 0);

          for (int i = 13; i <= 14; ++i) {
            ASSERT_EQ(manifest_file_record.fieldAt(i).type(), avro::Type::AVRO_NULL);
          }
        }
      }
    }
  } catch (const std::exception& ex) {
    LOG_WARN("exeception", K(ex.what()));
  }
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
