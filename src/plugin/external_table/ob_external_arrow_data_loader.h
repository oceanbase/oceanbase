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

#pragma once

#include "plugin/external_table/ob_external_arrow_status.h"
#include "lib/utility/ob_print_utils.h"

namespace oceanbase {

namespace common {
struct ObDatum;
class ObIVector;
}

namespace sql {
class ObEvalCtx;
class ObExpr;
struct ObDatumMeta;
}

namespace plugin {
namespace external {

/**
 * A loader loads data from arrow::Array to oceanbase::ObExpr
 */
class ObArrowDataLoader
{
public:
  virtual ~ObArrowDataLoader() { this->destroy(); }

  virtual int init(const DataType &arrow_type, const sql::ObDatumMeta &datum_type) { return OB_SUCCESS; }
  virtual void destroy() {}
  virtual int load(const Array &, sql::ObEvalCtx&, sql::ObExpr*) = 0;

  //DEFINE_VIRTUAL_TO_STRING(K(1));
  virtual int64_t to_string(char buf[], int64_t buf_len) const { return 0; }
};

/**
 * Create a loader by arrow type information and oceanbase expr meta information
 */
class ObArrowDataLoaderFactory final
{
public:
  // we can add some config later
  ObArrowDataLoaderFactory() = default;

  int select_loader(ObIAllocator &allocator,
                    const DataType &arrow_type,
                    const sql::ObDatumMeta &datum_type,
                    ObArrowDataLoader *&loader);
};

/**
 * Load int64/double/float into oceanbase
 * @details ArrowType can be Int64Type, DoubleType, FloatType
 */
template <typename ArrowType>
class ObCopyableArrowDataLoader : public ObArrowDataLoader
{
public:
  ~ObCopyableArrowDataLoader() override { destroy(); }
  int load(const Array &, sql::ObEvalCtx&, sql::ObExpr*) override;
};

/**
 * Load int32/int16/int8 into oceanbase
 */
template <typename ArrowType>
class ObIntToInt64ArrowDataLoader : public ObArrowDataLoader
{
public:
  ~ObIntToInt64ArrowDataLoader() override { destroy(); }

  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

/**
 * load arrow string(utf8)/binary to char/varchar/binary/text/blob type
 */
template <typename ArrowType>
class ObStringToStringArrowDataLoader : public ObArrowDataLoader
{
public:
  ~ObStringToStringArrowDataLoader() override { destroy(); }
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
private:
  using DatumSetter =
      int (*)(sql::ObExpr *expr, sql::ObEvalCtx &eval_ctx, const ObString &in_str, common::ObDatum &datum);

  DatumSetter datum_setter_ = nullptr;
};

/**
 * Load string to TIME field
 * @details JDBC only handle hours in [0, 24] but MySQL supports more values.
 */
template <typename ArrowType>
class ObStringToTimeArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

/**
 * Load string to MySQLDateTime or DateTime
 */
template <typename ArrowType>
class ObStringToDateTimeArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
private:
  using DateTimeHandler = int (*)(const common::ObString &, ObIVector *, int64_t);
  DateTimeHandler datetime_handler_ = nullptr;
};

/**
 * Load string to MySQLDate
 */
template <typename ArrowType>
class ObStringToMysqlDateArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

/**
 * Load string to Date
 */
template <typename ArrowType>
class ObStringToDateArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

/**
 * Load string to Year
 */
template <typename ArrowType>
class ObStringToYearArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

/**
 * Load string to Timestamp
 */
template <typename ArrowType>
class ObStringToTimestampArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

/**
 * Load Geometry type from Binary
 */
template <typename ArrowType>
class ObBinaryToGisArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

class ObBoolToIntArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

class ObDecimalArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

class ObDecimalToIntArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
private:
  int (*get_int64_func_)(const shared_ptr<DataType> &, const uint8_t *, int64_t &) = nullptr;
};

/**
 * Load from arrow::Date32Type to oceanbase ObMySQLDate
 */
class ObDate32ToMysqlDateArrowDataLoader : public ObArrowDataLoader
{
public:
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;
};

/**
 * load Time32/Time64/Timestamp into OceanBase
 * @note The timezone is ignored and use UTC as the default timezone.
 */
template <typename ArrowType>
class ObTimeArrowDataLoader : public ObArrowDataLoader
{
public:
  int init(const DataType &arrow_type, const sql::ObDatumMeta &ob_type) override;
  int load(const Array &, sql::ObEvalCtx &, sql::ObExpr *) override;

private:
  // OceanBase use microseconds and Arrow use others(default milliseconds)
  // oceanbase.time = arrow.time * muliples_
  int64_t muliples_ = 0;
};

} // namespace external
} // namespace plugin
} // namespace oceanbase
