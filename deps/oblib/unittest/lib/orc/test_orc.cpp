/**
 * Copyright (c) 2023 OceanBase
 * OceanBase CE is licensed under Mulan PubL v2.
 * You can use this software according to the terms and conditions of the Mulan PubL v2.
 * You may obtain a copy of Mulan PubL v2 at:
 *          http://license.coscl.org.cn/MulanPubL-2.0
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PubL v2 for more details.
 */
#define USING_LOG_PREFIX SQL

#include "gtest/gtest.h"
#include "lib/oblog/ob_log.h"
#include "lib/oblog/ob_log_module.h"

#include <orc/OrcFile.hh>
#include <orc/MemoryPool.hh>
#include <orc/Writer.hh>
#include <orc/Reader.hh>
#include <orc/Vector.hh>

#include <iostream>
#include <cstdlib>


#include "lib/allocator/page_arena.h"
#include "lib/file/ob_file.h"
#include "lib/file/file_directory_utils.h"
#include "lib/charset/ob_template_helper.h"
#include "lib/net/ob_net_util.h"

#define USING_LOG_PREFIX SQL

using namespace oceanbase::common;

class TestOrc: public ::testing::Test
{
public:
  TestOrc() {};
  virtual ~TestOrc() {};
  virtual void SetUp();
  virtual void TearDown();
};



void TestOrc::SetUp()
{
}

void TestOrc::TearDown()
{
}

class OrcMemoryPool : public ::orc::MemoryPool
{
  public:
      virtual char* malloc(uint64_t size) override {
        return (char* )alloc_.alloc(size);
      }
      virtual void free(char* p) override {
        //do nothing
      }
  private:
    oceanbase::common::ObArenaAllocator alloc_;
};


// Result<std::unique_ptr<liborc::Type>> GetOrcType(const DataType& type) {
//   Type::type kind = type.id();
//   switch (kind) {
//     case Type::type::BOOL:
//       return liborc::createPrimitiveType(liborc::TypeKind::BOOLEAN);
//     case Type::type::INT8:
//       return liborc::createPrimitiveType(liborc::TypeKind::BYTE);
//     case Type::type::INT16:
//       return liborc::createPrimitiveType(liborc::TypeKind::SHORT);
//     case Type::type::INT32:
//       return liborc::createPrimitiveType(liborc::TypeKind::INT);
//     case Type::type::INT64:
//       return liborc::createPrimitiveType(liborc::TypeKind::LONG);
//     case Type::type::FLOAT:
//       return liborc::createPrimitiveType(liborc::TypeKind::FLOAT);
//     case Type::type::DOUBLE:
//       return liborc::createPrimitiveType(liborc::TypeKind::DOUBLE);
//     // Use STRING instead of VARCHAR for now, both use UTF-8
//     case Type::type::STRING:
//     case Type::type::LARGE_STRING:
//       return liborc::createPrimitiveType(liborc::TypeKind::STRING);
//     case Type::type::BINARY:
//     case Type::type::LARGE_BINARY:
//     case Type::type::FIXED_SIZE_BINARY:
//       return liborc::createPrimitiveType(liborc::TypeKind::BINARY);
//     case Type::type::DATE32:
//       return liborc::createPrimitiveType(liborc::TypeKind::DATE);
//     case Type::type::DATE64:
//       return liborc::createPrimitiveType(liborc::TypeKind::TIMESTAMP);
//     case Type::type::TIMESTAMP: {
//       const auto& timestamp_type = checked_cast<const TimestampType&>(type);
//       if (!timestamp_type.timezone().empty()) {
//         // The timestamp values stored in the arrow array are normalized to UTC.
//         // TIMESTAMP_INSTANT type is always preferred over TIMESTAMP type.
//         return liborc::createPrimitiveType(liborc::TypeKind::TIMESTAMP_INSTANT);
//       }
//       // The timestamp values stored in the arrow array can be in any timezone.
//       return liborc::createPrimitiveType(liborc::TypeKind::TIMESTAMP);
//     }
//     case Type::type::DECIMAL128: {
//       const uint64_t precision =
//           static_cast<uint64_t>(checked_cast<const Decimal128Type&>(type).precision());
//       const uint64_t scale =
//           static_cast<uint64_t>(checked_cast<const Decimal128Type&>(type).scale());
//       return liborc::createDecimalType(precision, scale);
//     }
//     case Type::type::LIST:
//     case Type::type::FIXED_SIZE_LIST:
//     case Type::type::LARGE_LIST: {
//       const auto& value_field = checked_cast<const BaseListType&>(type).value_field();
//       ARROW_ASSIGN_OR_RAISE(auto orc_subtype, GetOrcType(*value_field->type()));
//       SetAttributes(value_field, orc_subtype.get());
//       return liborc::createListType(std::move(orc_subtype));
//     }
//     case Type::type::STRUCT: {
//       std::unique_ptr<liborc::Type> out_type = liborc::createStructType();
//       std::vector<std::shared_ptr<Field>> arrow_fields =
//           checked_cast<const StructType&>(type).fields();
//       for (auto it = arrow_fields.begin(); it != arrow_fields.end(); ++it) {
//         std::string field_name = (*it)->name();
//         ARROW_ASSIGN_OR_RAISE(auto orc_subtype, GetOrcType(*(*it)->type()));
//         SetAttributes(*it, orc_subtype.get());
//         out_type->addStructField(field_name, std::move(orc_subtype));
//       }
//       return out_type;
//     }
//     case Type::type::MAP: {
//       const auto& key_field = checked_cast<const MapType&>(type).key_field();
//       const auto& item_field = checked_cast<const MapType&>(type).item_field();
//       ARROW_ASSIGN_OR_RAISE(auto key_orc_type, GetOrcType(*key_field->type()));
//       ARROW_ASSIGN_OR_RAISE(auto item_orc_type, GetOrcType(*item_field->type()));
//       SetAttributes(key_field, key_orc_type.get());
//       SetAttributes(item_field, item_orc_type.get());
//       return liborc::createMapType(std::move(key_orc_type), std::move(item_orc_type));
//     }
//     case Type::type::DENSE_UNION:
//     case Type::type::SPARSE_UNION: {
//       std::unique_ptr<liborc::Type> out_type = liborc::createUnionType();
//       std::vector<std::shared_ptr<Field>> arrow_fields =
//           checked_cast<const UnionType&>(type).fields();
//       for (const auto& arrow_field : arrow_fields) {
//         std::shared_ptr<DataType> arrow_child_type = arrow_field->type();
//         ARROW_ASSIGN_OR_RAISE(auto orc_subtype, GetOrcType(*arrow_child_type));
//         SetAttributes(arrow_field, orc_subtype.get());
//         out_type->addUnionChild(std::move(orc_subtype));
//       }
//       return out_type;
//     }
//     default: {
//       return Status::NotImplemented("Unknown or unsupported Arrow type: ",
//                                     type.ToString());
//     }
//   }
// }

  // class ObOrcRandomAccess : public :orc::InputStream {
  //   public:
  //     ObOrcRandomAccess(oceanbase::sql::ObExternalDataAccessDriver &file_reader, const char* file_name, orc::MemoryPool *pool) 
  //     : file_reader_(file_reader), file_name_(file_name), pool_(pool) {
      
  //     }

  //     uint64_t getLength() const override {
  //       return totalLength;
  //     }

  //     uint64_t getNaturalReadSize() const override {
  //       return 128 * 1024;
  //     }

  //     void read(void* buf,
  //               uint64_t length,
  //               uint64_t offset) override {
  //       int64_t bytesRead = 0;
  //       int ret = file_reader_.pread(buf, length, offset, bytesRead);
  //       totalLength += bytesRead;
  //       if (ret != OB_SUCCESS) {
  //         throw orc::ParseError("Bad read of " + std::string(file_name_));
  //       }
  //     }

  //     const std::string& getName() const override {
  //       return file_name_;
  //     }
  //   private:
  //     oceanbase::sql::ObExternalDataAccessDriver &file_reader_;
  //     const std::string file_name_;
  //     orc::MemoryPool *pool_;
  //     uint64_t totalLength;
  // };


void wirte_orc_file() {
  std::unique_ptr<orc::OutputStream> outStream = orc::writeLocalFile("my-file.orc");
  //std::unique_ptr<orc::Type> schema(orc::Type::buildTypeFromString("struct<x:int,y:int>"));
  std::unique_ptr<orc::Type> schema = orc::createStructType();
  schema->addStructField("x", orc::createPrimitiveType(orc::TypeKind::INT));
  schema->addStructField("y", orc::createPrimitiveType(orc::TypeKind::BOOLEAN));
  std::unique_ptr<orc::Type> sub_schema = orc::createStructType();
  sub_schema->addStructField("z", orc::createPrimitiveType(orc::TypeKind::FLOAT));
  sub_schema->addStructField("d", orc::createPrimitiveType(orc::TypeKind::DATE));
  schema->addStructField("S2", std::move(sub_schema));
  orc::WriterOptions options;
  OrcMemoryPool pool;
  options.setMemoryPool(&pool);
  options.setCompression(orc::CompressionKind::CompressionKind_ZLIB);
  std::unique_ptr<orc::Writer> writer = orc::createWriter(*schema, outStream.get(), options);

  uint64_t batchSize = 8, rowCount = 100;
  std::unique_ptr<orc::ColumnVectorBatch> batch =
    writer->createRowBatch(batchSize);
  orc::StructVectorBatch *root =
    dynamic_cast<orc::StructVectorBatch *>(batch.get());
  orc::LongVectorBatch *x =
    dynamic_cast<orc::LongVectorBatch *>(root->fields[0]);
  orc::LongVectorBatch *y =
    dynamic_cast<orc::LongVectorBatch *>(root->fields[1]);
  orc::DoubleVectorBatch *z =
    dynamic_cast<orc::DoubleVectorBatch *>(dynamic_cast<orc::StructVectorBatch *>(root->fields[2])->fields[0]);

  uint64_t rows = 0;
  for (uint64_t i = 0; i < rowCount; ++i) {
    if (i % 5 == 0) {
      x->notNull[rows] = 0;
      y->notNull[rows] = 0;
      z->notNull[rows] = 0;
      x->hasNulls = true;
      y->hasNulls = true;
      z->hasNulls = true;
      x->data[rows] = 0;
      y->data[rows] = 0;
      z->data[rows] = i * 1.1 + 0.01;
      rows++;
    } else {
      x->notNull[rows] = true;
      y->notNull[rows] = true;
      z->notNull[rows] = true;
      x->data[rows] = i + 1;
      y->data[rows] = i * 3 + 1;
      z->data[rows] = i * 1.1 + 0.01;
      rows++;
    }


    if (rows == batchSize) {
      root->numElements = rows;
      x->numElements = rows;
      y->numElements = rows;
      z->numElements = rows;

      writer->add(*batch);
      rows = 0;
    }
  }

  if (rows != 0) {
    root->numElements = rows;
    x->numElements = rows;
    y->numElements = rows;
    //z->numElements = rows;

    writer->add(*batch);
    rows = 0;
  }

  writer->close();
}

void read_orc_file() {
  std::unique_ptr<orc::InputStream> inStream = orc::readLocalFile("my-file.orc");
  orc::ReaderOptions options;
  OrcMemoryPool pool;
  options.setMemoryPool(pool);
  std::unique_ptr<orc::Reader> reader = orc::createReader(std::move(inStream), options);

  orc::RowReaderOptions rowReaderOptions;
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOptions);
  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(2);

  //std::cout <<"root field size: " << root->fields.size() << std::endl;
  while (rowReader->next(*batch)) {
    std::cout<<"column batch:" << batch->toString() <<"\n";
    for (uint64_t r = 0; r < batch->numElements; ++r) {
      orc::StructVectorBatch *root =
        dynamic_cast<orc::StructVectorBatch *>(batch.get());
      orc::ColumnVectorBatch *col[10] = {NULL};
      int k = 0;
      std::cout<< "row:" << r ;
      for (int i = 0; i < reader->getType().getSubtypeCount(); i++) {
        const uint8_t* valid_bytes = NULL;
        switch (reader->getType().getSubtype(i)->getKind()) {
          case orc::TypeKind::BOOLEAN:
          case orc::TypeKind::BYTE:
          case orc::TypeKind::SHORT:
          case orc::TypeKind::INT:
          case orc::TypeKind::LONG:
          case orc::TypeKind::DATE:
            //valid_bytes = reinterpret_cast<const uint8_t*>( dynamic_cast<orc::LongVectorBatch *>(root->fields[i])->notNull.data()) + r;
            std::cout<<" col" << i <<":" << dynamic_cast<orc::LongVectorBatch *>(root->fields[i])->data[r] << " is not null:"<< bool(root->fields[i]->notNull[r]);
            break;
          case orc::TypeKind::FLOAT:
          case orc::TypeKind::DOUBLE:
            std::cout<< " col" << i <<":" << dynamic_cast<orc::DoubleVectorBatch *>(root->fields[i])->data[r] ;
            std::cout<<" has NULL:"<<(root->fields[i])->hasNulls<<" is not null:"<< bool(root->fields[i]->notNull[r]);
            break;
          case orc::TypeKind::STRING:
          case orc::TypeKind::VARCHAR:
          case orc::TypeKind::CHAR:
          case orc::TypeKind::BINARY:
            std::cout<< " col" << i <<":" << dynamic_cast<orc::StringVectorBatch *>(root->fields[i])->data[r] <<" is not null:"<< bool(root->fields[i]->notNull[r]);
            break;
          case orc::TypeKind::LIST:
          case orc::TypeKind::MAP:
          case orc::TypeKind::UNION:
            //not supported
            break;
          case orc::TypeKind::DECIMAL:
            //std::cout<< "row:" << r << " col" << i <<":" << dynamic_cast<orc::Decimal128VectorBatch *>(root->fields[i])->data[r] <<" is not null:"<< (bool)dynamic_cast<orc::LongVectorBatch *>(root->fields[i])->notNull[r];
            break;
          case orc::TypeKind::TIMESTAMP:
          case orc::TypeKind::TIMESTAMP_INSTANT:
            //std::cout<< "row:" << r << " col" << i <<":" << dynamic_cast<orc::TimestampVectorBatch *>(root->fields[i])->data[r] <<" is not null:"<< (bool)dynamic_cast<orc::LongVectorBatch *>(root->fields[i])->notNull[r];
            break;
          case orc::TypeKind::STRUCT:
            std::cout<< " col" << i <<":" <<  dynamic_cast<orc::DoubleVectorBatch *>(dynamic_cast<orc::StructVectorBatch *>(root->fields[i])->fields[0])->data[r];
            break;
          default:
          //error
            break;
        }
        // if (reader->getType().getColumnId() == reader->getType().getMaximumColumnId()) {//is primitive
        //   col[k++] = x;
        // }
      }
      std::cout<<"\n";
      
    }
  }
}

void printType(const orc::Type &type) {
  std::cout << " type:" << type.toString() <<" type subTypeCount:" << type.getSubtypeCount()<< " typeKind: " << type.getKind() << " ColumnId:" << type.getColumnId() << " maxColumnId:" << type.getMaximumColumnId() << "\n";
  for (int i = 0; i < type.getSubtypeCount(); i++) {
    printType(*type.getSubtype(i));
  }
}


bool is_primitive_Type(const orc::Type &type) {
  if (type.getColumnId() == type.getMaximumColumnId()) {
    return true;
  }
  return false;
}
void read_file_footer() {
  std::unique_ptr<orc::InputStream> inStream = orc::readLocalFile("my-file.orc");
  orc::ReaderOptions options;
  OrcMemoryPool pool;
  //options.setMemoryPool(pool);
  std::unique_ptr<orc::Reader> reader = orc::createReader(std::move(inStream), options);
  const orc::Type& type = reader->getType();
  printType(type);
  for (int i = 0; i < reader->getType().getSubtypeCount(); i++) {
    std::cout << "Subfield" << i << ": " << type.getFieldName(i);
    printType(*type.getSubtype(i));
  }
  for (int i = 0; i <= reader->getType().getMaximumColumnId(); i++) {
    std::cout<<"column" <<i<<": " << reader->getColumnStatistics(i)->toString() <<"\n";
  }
}

// Result<std::shared_ptr<Schema>> GetArrowSchema(const liborc::Type& type) {
//   if (type.getKind() != liborc::STRUCT) {
//     return Status::NotImplemented(
//         "Only ORC files with a top-level struct "
//         "can be handled");
//   }
//   int size = static_cast<int>(type.getSubtypeCount());
//   std::vector<std::shared_ptr<Field>> fields;
//   fields.reserve(size);
//   for (int child = 0; child < size; ++child) {
//     const std::string& name = type.getFieldName(child);
//     ARROW_ASSIGN_OR_RAISE(auto elem_field, GetArrowField(name, type.getSubtype(child)));
//     fields.push_back(std::move(elem_field));
//   }
//   ARROW_ASSIGN_OR_RAISE(auto metadata, ReadMetadata());
//   return std::make_shared<Schema>(std::move(fields), std::move(metadata));
// }

// Result<std::shared_ptr<const KeyValueMetadata>> ReadMetadata() {
//   const std::list<std::string> keys = reader_->getMetadataKeys();
//   auto metadata = std::make_shared<KeyValueMetadata>();
//   for (const auto& key : keys) {
//     metadata->Append(key, reader_->getMetadataValue(key));
//   }
//   return std::const_pointer_cast<const KeyValueMetadata>(metadata);
// }

void read_column() {
  std::cout<<"=================== test read column ===================\n";
  std::unique_ptr<orc::InputStream> inStream = orc::readLocalFile("my-file.orc");
  orc::ReaderOptions options;
  // OrcMemoryPool pool;
  // options.setMemoryPool(pool);
  std::list<std::string> include_names_list;
  include_names_list.push_front(std::string("y"));
  include_names_list.push_front(std::string("S2.z"));
  include_names_list.push_front(std::string("x"));
  std::unique_ptr<orc::Reader> reader = orc::createReader(std::move(inStream), options);
  orc::RowReaderOptions rowReaderOptions;
  rowReaderOptions.include(include_names_list);
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOptions);
  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(1024);
  std::cout<<"column batch:" << batch->toString() <<"\n";
  printType(rowReader->getSelectedType());
  const orc::Type &type = rowReader->getSelectedType();
  int size = static_cast<int>(type.getSubtypeCount());
  for (int child = 0; child < size; ++child) {
    const std::string& name = type.getFieldName(child);
    std::cout<<"field "<< child << " name:" << name <<std::endl;
    printType(*type.getSubtype(child));
  }
  // const std::list<std::string> keys = reader->getMetadataKeys();
  // auto metadata = std::make_shared<KeyValueMetadata>();
  // for (const auto& key : keys) {
  //   metadata->Append(key, reader_->getMetadataValue(key));
  // }
  // const orc::Type& type = reader->getType();
  // printType(type);
  // for (int i = 0; i < reader->getType().getSubtypeCount(); i++) {
  //   std::cout << "Subfield" << i << ": " << type.getFieldName(i);
  //   printType(*type.getSubtype(i));
  // }
  // for (int i = 0; i <= reader->getType().getMaximumColumnId(); i++) {
  //   std::cout<<"column" <<i<<": " << reader->getColumnStatistics(i)->toString() <<"\n";
  // }

}

void read_schema(std::unique_ptr<orc::RowReader> &rowReader) {
  const orc::Type &type = rowReader->getSelectedType();
  if (type.getKind() != orc::STRUCT) {
    throw std::runtime_error("Only ORC files with a top-level struct can be handled");
  } else {

  }
}

void read_stripe() {
  //  int64_t nstripes = reader_->getNumberOfStripes();
  //   stripes_.resize(static_cast<size_t>(nstripes));
  //   std::unique_ptr<liborc::StripeInformation> stripe;
  //   uint64_t first_row_of_stripe = 0;
  //   for (int i = 0; i < nstripes; ++i) {
  //     stripe = reader_->getStripe(i);
  //     stripes_[i] = StripeInformation({static_cast<int64_t>(stripe->getOffset()),
  //                                      static_cast<int64_t>(stripe->getLength()),
  //                                      static_cast<int64_t>(stripe->getNumberOfRows()),
  //                                      static_cast<int64_t>(first_row_of_stripe)});
  //     first_row_of_stripe += stripe->getNumberOfRows();
  //   }
}

void generate_orc_file(const bool output_null) {
  std::unique_ptr<orc::OutputStream> outStream = orc::writeLocalFile("test1.orc");
  //std::unique_ptr<orc::Type> schema(orc::Type::buildTypeFromString("struct<x:int,y:int>"));
  //std::unique_ptr<orc::Type> schema = orc::createStructType();
  // schema->addStructField("age", orc::createPrimitiveType(orc::TypeKind::INT))
  // ->addStructField("is_male", orc::createPrimitiveType(orc::TypeKind::BOOLEAN))
  // ->addStructField("name", orc::createPrimitiveType(orc::TypeKind::Varchar))
  // ->addStructField("comment", orc::createPrimitiveType(orc::TypeKind::String));
  // std::unique_ptr<Type> schema(Type::buildTypeFromString("
  // struct<name:varchar(100), age:int, 
  // sex:char(1), comment:string, in_date:date, job_time:timestamp, 
  // language:binary, has_house:boolean, money:bigint,
  // height:decimal(5, 2), weight:double, width:float>"));
  // std::unique_ptr<orc::Type> sub_schema = orc::createStructType();
  // sub_schema->addStructField("z", orc::createPrimitiveType(orc::TypeKind::FLOAT));
  // schema->addStructField("S2", std::move(sub_schema));
  // orc::WriterOptions options;
  // OrcMemoryPool pool;
  // options.setCompression(orc::CompressionKind::CompressionKind_SNAPPY);
  // //options.setStripeSize(1024);
  // options.setCompressionBlockSize(1024);
  // options.setMemoryPool(&pool);
  // std::unique_ptr<orc::Writer> writer = orc::createWriter(*schema, outStream.get(), options);

  // uint64_t batchSize = 128, rowCount = 12800;
  // std::unique_ptr<orc::ColumnVectorBatch> batch = writer->createRowBatch(batchSize);
  // orc::StructVectorBatch *root = dynamic_cast<orc::StructVectorBatch *>(batch.get());

  // uint64_t rows = 0;
  // for (uint64_t i = 0; i < rowCount; ++i) {
  //   if (output_null) {
  //     int put_null = std::rand() % 10;
  //     if (put_null == 1) {

  //     }
  //   }

  //   if (i % 5 == 0) {
  //     x->notNull[rows] = 0;
  //     y->notNull[rows] = 0;
  //     z->notNull[rows] = 0;
  //     x->hasNulls = true;
  //     y->hasNulls = true;
  //     z->hasNulls = true;
  //     x->data[rows] = 0;
  //     y->data[rows] = 0;
  //     z->data[rows] = i * 1.1 + 0.01;
  //     rows++;
  //   } else {
  //     x->notNull[rows] = true;
  //     y->notNull[rows] = true;
  //     z->notNull[rows] = true;
  //     x->data[rows] = i + 1;
  //     y->data[rows] = i * 3 + 1;
  //     z->data[rows] = i * 1.1 + 0.01;
  //     rows++;
  //   }


  //   if (rows == batchSize) {
  //     root->numElements = rows;
  //     x->numElements = rows;
  //     y->numElements = rows;
  //     z->numElements = rows;

  //     writer->add(*batch);
  //     rows = 0;
  //   }
  // }

  // if (rows != 0) {
  //   root->numElements = rows;
  //   x->numElements = rows;
  //   y->numElements = rows;
  //   //z->numElements = rows;

  //   writer->add(*batch);
  //   rows = 0;
  // }

  // writer->close();
}

void read_test_orc_file()
{
  std::cout<<"=================== test read orc file column ===================\n";
  std::unique_ptr<orc::InputStream> inStream = orc::readLocalFile("/data/1/mingye.swj/work/support_master_orc/data/t.orc");
  orc::ReaderOptions options;
  // OrcMemoryPool pool;
  // options.setMemoryPool(pool);
  // std::list<std::string> include_names_list;
  // include_names_list.push_front(std::string("S2.z"));
  // include_names_list.push_front(std::string("x"));
  std::unique_ptr<orc::Reader> reader = orc::createReader(std::move(inStream), options);
  orc::RowReaderOptions rowReaderOptions;
  //rowReaderOptions.include(include_names_list);
  std::unique_ptr<orc::RowReader> rowReader = reader->createRowReader(rowReaderOptions);
  std::unique_ptr<orc::ColumnVectorBatch> batch = rowReader->createRowBatch(256);
  while (rowReader->next(*batch)) {
    std::cout<<"column batch:" << batch->toString() <<"\n";
    for (uint64_t r = 0; r < batch->numElements; ++r) {
      orc::StructVectorBatch *root =
        dynamic_cast<orc::StructVectorBatch *>(batch.get());
        std::cout<<" row" << r <<":" << dynamic_cast<orc::LongVectorBatch *>(root->fields[0])->data[r] << " is not null:"<< bool(root->fields[0]->notNull[r]);
    }
  }
  printType(rowReader->getSelectedType());
  const orc::Type &type = rowReader->getSelectedType();
  int size = static_cast<int>(type.getSubtypeCount());
  for (int child = 0; child < size; ++child) {
    const std::string& name = type.getFieldName(child);
    printType(*type.getSubtype(child));
  }
}

TEST_F(TestOrc, read_write_orc_file_test)
{
  wirte_orc_file();
  read_orc_file();
  read_file_footer();
  read_column();
  //read_test_orc_file();
}

int main(int argc, char **argv)
{
  OB_LOGGER.set_log_level("INFO");
  testing::InitGoogleTest(&argc,argv);
  return RUN_ALL_TESTS();
}