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

#ifndef OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
#define OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
#include "lib/utility/ob_print_utils.h"
#include "lib/charset/ob_charset.h"

namespace oceanbase
{
namespace common
{
class ObObjMeta;
class ObAccuracy;
template <class T>
class ObIArray;

// @note Obj类型只能增加，不能删除，顺序也不能变，见ob_obj_cast.h
// @note 增加类型时需要同步修改ObObjTypeClass和ob_obj_type_class().
enum ObObjType
{
  ObNullType        = 0,  // 空类型

  ObTinyIntType     = 1,  // int8, aka mysql boolean type
  ObSmallIntType    = 2,  // int16
  ObMediumIntType   = 3,  // int24
  ObInt32Type       = 4,  // int32
  ObIntType         = 5,  // int64, aka bigint

  ObUTinyIntType    = 6,  // uint8
  ObUSmallIntType   = 7,  // uint16
  ObUMediumIntType  = 8,  // uint24
  ObUInt32Type      = 9,  // uint32
  ObUInt64Type      = 10, // uint64

  ObFloatType       = 11, // single-precision floating point
  ObDoubleType      = 12, // double-precision floating point

  ObUFloatType      = 13, // unsigned single-precision floating point
  ObUDoubleType     = 14, // unsigned double-precision floating point

  ObNumberType      = 15, // aka decimal/numeric
  ObUNumberType     = 16,

  ObDateTimeType    = 17,
  ObTimestampType   = 18,
  ObDateType        = 19,
  ObTimeType        = 20,
  ObYearType        = 21,

  ObVarcharType     = 22, // charset: utf8mb4 or binary
  ObCharType        = 23, // charset: utf8mb4 or binary

  ObHexStringType   = 24, // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001

  ObExtendType      = 25, // Min, Max, NOP etc.
  ObUnknownType     = 26, // For question mark(?) in prepared statement, no need to serialize
  // @note future new types to be defined here !!!
  ObTinyTextType    = 27,
  ObTextType        = 28,
  ObMediumTextType  = 29,
  ObLongTextType    = 30,

  ObBitType         = 31,
  ObEnumType        = 32,
  ObSetType         = 33,
  ObEnumInnerType   = 34,
  ObSetInnerType    = 35,

  ObTimestampTZType   = 36, // timestamp with time zone for oracle
  ObTimestampLTZType  = 37, // timestamp with local time zone for oracle
  ObTimestampNanoType = 38, // timestamp nanosecond for oracle
  ObRawType           = 39, // raw type for oracle
  ObIntervalYMType    = 40, // interval year to month
  ObIntervalDSType    = 41, // interval day to second
  ObNumberFloatType   = 42, // oracle float, subtype of NUMBER
  ObNVarchar2Type     = 43, // nvarchar2
  ObNCharType         = 44, // nchar
  ObURowIDType        = 45, // UROWID
  ObLobType           = 46, // Oracle Lob
  ObJsonType          = 47, // Json Type
  ObGeometryType      = 48, // Geometry type

  ObUserDefinedSQLType = 49, // User defined type in SQL
  ObDecimalIntType     = 50, // decimal int type
  ObCollectionSQLType  = 51, // collection(varray and nested table) in SQL
  ObMySQLDateType      = 52, // date type which is compatible with MySQL.
  ObMySQLDateTimeType  = 53, // datetime type which is compatible with MySQL.
  ObRoaringBitmapType  = 54, // Roaring Bitmap Type
  ObMaxType                 // invalid type, or count of obj type
};

//Oracle type
enum ObObjOType
{
  ObONotSupport       = 0,
  ObONullType         = 1,   // 空类型
  ObOSmallIntType     = 2,
  ObOIntType          = 3,
  ObONumberFloatType  = 4,  //float
  ObOBinFloatType     = 5,  //binary float
  ObOBinDoubleType    = 6,  //binary double
  ObONumberType       = 7,
  ObOCharType         = 8,
  ObOVarcharType      = 9,
  ObODateType         = 10,
  ObOTimestampTZType  = 11, //timestamp with time zone
  ObOTimestampLTZType = 12, //timestamp with local time zone
  ObOTimestampType    = 13, //timestamp
  ObOIntervalYMType   = 14, //TODO
  ObOIntervalDSType   = 15, //TODO
  ObOLobType          = 16, //blob
  ObOExtendType       = 17,      //not sure
  ObOUnknownType      = 18,      //not sure
  ObORawType          = 19,
  ObONVarchar2Type    = 20,
  ObONCharType        = 21,
  ObOURowIDType       = 22,
  ObOLobLocatorType   = 23,
  ObOJsonType         = 24,
  ObOGeometryType     = 25,
  ObOUDTSqlType       = 26,
  ObOCollectionSqlType  = 27,
  ObORoaringBitmapType  = 28,
  ObOMaxType          //invalid type, or count of ObObjOType
};

enum class ObGeoType
{
  GEOMETRY = 0,
  POINT = 1,
  LINESTRING = 2,
  POLYGON = 3,
  MULTIPOINT = 4,
  MULTILINESTRING = 5,
  MULTIPOLYGON = 6,
  GEOMETRYCOLLECTION = 7,
  GEOTYPEMAX = 31, // 5 bit for geometry type in column schema,set max 31
  // 3d geotype is not supported to define as subtype yet,
  // only use for inner type
  POINTZ = 1001,
  LINESTRINGZ = 1002,
  POLYGONZ = 1003,
  MULTIPOINTZ = 1004,
  MULTILINESTRINGZ = 1005,
  MULTIPOLYGONZ = 1006,
  GEOMETRYCOLLECTIONZ = 1007,
  GEO3DTYPEMAX = 1024,
};

//for cast/cmp map
static ObObjOType OBJ_TYPE_TO_O_TYPE[ObMaxType+1] = {
  ObONullType,               // 空类型
  ObOSmallIntType,           // int8, aka mysql boolean type
  ObOSmallIntType,           // int16
  ObONotSupport,             // int24
  ObOIntType,                // int32
  ObOIntType,                // int64, aka bigint
  ObONotSupport,             // uint8
  ObONotSupport,             // uint16
  ObONotSupport,             // uint24
  ObONotSupport,             // uint32
  ObONotSupport,             // uint64

  ObOBinFloatType,           // single-precision floating point
  ObOBinDoubleType,          // double-precision floating point

  ObONotSupport,             // unsigned single-precision floating point
  ObONotSupport,             // unsigned double-precision floating point

  ObONumberType,             // aka decimal/numeric
  ObONotSupport,             //ObUNumberType

  ObODateType,               //ObDateTimeType
  ObONotSupport,             //ObTimestampType
  ObODateType,               //ObDateType
  ObONotSupport,             //ObTimeType
  ObONotSupport,             //ObYearType, //TODO

  ObOVarcharType,           //ObVarcharType=22,
  ObOCharType,              //ObCharType=23,     // charset: utf8mb4 or binary

  ObONotSupport,            //ObHexStringType=24, //TODO // hexadecimal literal, e.g. X'42', 0x42, b'1001', 0b1001

  ObOExtendType,             //ObExtendType=25,  //?TODO               // Min, Max, NOP etc.
  ObOUnknownType,            //ObUnknownType=26,  //?TODO             // For question mark(?) in prepared statement, no need to serialize
  ObONotSupport,             //ObTinyTextType = 27,
  ObONotSupport,             //ObTextType = 28,
  ObONotSupport,             //ObMediumTextType = 29,
  ObOLobType,                //ObLongTextType = 30,
  ObONotSupport,             //ObBitType=31,
  ObONotSupport,             //ObEnumType=32,
  ObONotSupport,             //ObSetType=33,
  ObONotSupport,             //ObEnumÅInnerType=34,
  ObONotSupport,             //ObSetInnerType=35,
  ObOTimestampTZType,        //ObTimeStampTZType=36,
  ObOTimestampLTZType,       //ObTimeStampLTZType=37,
  ObOTimestampType,          //ObTimeStamp=38,
  ObORawType,                //ObRawType=39,
  ObOIntervalYMType,         //ObIntervalYMType=40,
  ObOIntervalDSType,         //ObIntervalDSType=41,
  ObONumberFloatType,        //ObNumberFloatType=42,
  ObONVarchar2Type,          //ObNVarchar2=43,
  ObONCharType,              //ObNChar=44,
  ObOURowIDType,             //ObURowID=45
  ObOLobLocatorType,         //ObLobType = 46,
  ObOJsonType,               //ObJsonType = 47,
  ObOGeometryType,           //ObGeometryType = 48,
  ObOUDTSqlType,             //ObUserDefinedSQLType = 49,
  ObONumberType,             //ObDecimalIntType = 50,
  ObOCollectionSqlType,      //ObCollectionSQLType = 51,
  ObONotSupport,             //ObMySQLDateType = 52,
  ObONotSupport,             //ObMySQLDateTimeType = 53,
  ObORoaringBitmapType,      //Roaring Bitmap Type = 54，
  ObONotSupport              //ObMaxType,
};


enum ObObjTypeClass
{
  ObNullTC      = 0,    // null
  ObIntTC       = 1,    // int8, int16, int24, int32, int64.
  ObUIntTC      = 2,    // uint8, uint16, uint24, uint32, uint64.
  ObFloatTC     = 3,    // float, ufloat.
  ObDoubleTC    = 4,    // double, udouble.
  ObNumberTC    = 5,    // number, unumber.
  ObDateTimeTC  = 6,    // datetime, timestamp.
  ObDateTC      = 7,    // date
  ObTimeTC      = 8,    // time
  ObYearTC      = 9,    // year
  ObStringTC    = 10,   // varchar, char, varbinary, binary.
  ObExtendTC    = 11,   // extend
  ObUnknownTC   = 12,   // unknown
  ObTextTC      = 13,   // TinyText,MediumText, Text ,LongText, TinyBLOB,MediumBLOB, BLOB ,LongBLOB
  ObBitTC       = 14,   // bit
  ObEnumSetTC   = 15,   // enum, set
  ObEnumSetInnerTC  = 16,
  ObOTimestampTC    = 17, //timestamp with time zone
  ObRawTC       = 18,   // raw
  ObIntervalTC      = 19, //oracle interval type class include interval year to month and interval day to second
  ObRowIDTC         = 20, // oracle rowid typeclass, includes urowid and rowid
  ObLobTC           = 21, //oracle lob typeclass
  ObJsonTC          = 22, // json type class 
  ObGeometryTC      = 23, // geometry type class
  ObUserDefinedSQLTC = 24, // user defined type class in SQL
  ObDecimalIntTC     = 25, // decimal int class
  ObCollectionSQLTC = 26, // collection type class in SQL
  ObMySQLDateTC     = 27, // mysql date type class
  ObMySQLDateTimeTC = 28, // mysql date time type class
  ObRoaringBitmapTC = 29, // Roaringbitmap type class
  ObMaxTC,
  // invalid type classes are below, only used as the result of XXXX_type_promotion()
  // to indicate that the two obj can't be promoted to the same type.
  ObIntUintTC,
  ObLeftTypeTC,
  ObRightTypeTC,
  ObActualMaxTC
};

// Define obj type to obj type class mapping pairs.
#define OBJ_TYPE_TC_PAIRS                      \
		(ObNullType, ObNullTC),                    \
		(ObTinyIntType, ObIntTC),                  \
		(ObSmallIntType, ObIntTC),                 \
		(ObMediumIntType, ObIntTC),                \
		(ObInt32Type, ObIntTC),                    \
		(ObIntType, ObIntTC),                      \
		(ObUTinyIntType, ObUIntTC),                \
		(ObUSmallIntType, ObUIntTC),               \
		(ObUMediumIntType, ObUIntTC),              \
		(ObUInt32Type, ObUIntTC),                  \
		(ObUInt64Type, ObUIntTC),                  \
		(ObFloatType, ObFloatTC),                  \
		(ObDoubleType, ObDoubleTC),                \
		(ObUFloatType, ObFloatTC),                 \
		(ObUDoubleType, ObDoubleTC),               \
		(ObNumberType, ObNumberTC),                \
		(ObUNumberType, ObNumberTC),               \
		(ObDateTimeType, ObDateTimeTC),            \
		(ObTimestampType, ObDateTimeTC),           \
		(ObDateType, ObDateTC),                    \
		(ObTimeType, ObTimeTC),                    \
		(ObYearType, ObYearTC),                    \
		(ObVarcharType, ObStringTC),               \
		(ObCharType, ObStringTC),                  \
		(ObHexStringType, ObStringTC),             \
		(ObExtendType, ObExtendTC),                \
		(ObUnknownType, ObUnknownTC),              \
		(ObTinyTextType, ObTextTC),                \
		(ObTextType, ObTextTC),                    \
		(ObMediumTextType, ObTextTC),              \
		(ObLongTextType, ObTextTC),                \
		(ObBitType, ObBitTC),                      \
		(ObEnumType, ObEnumSetTC),                 \
		(ObSetType, ObEnumSetTC),                  \
		(ObEnumInnerType, ObEnumSetInnerTC),       \
		(ObSetInnerType, ObEnumSetInnerTC),        \
		(ObTimestampTZType, ObOTimestampTC),       \
		(ObTimestampLTZType, ObOTimestampTC),      \
		(ObTimestampNanoType, ObOTimestampTC),     \
		(ObRawType, ObRawTC),                      \
		(ObIntervalYMType, ObIntervalTC),          \
		(ObIntervalDSType, ObIntervalTC),          \
		(ObNumberFloatType, ObNumberTC),           \
		(ObNVarchar2Type, ObStringTC),             \
		(ObNCharType, ObStringTC),                 \
    (ObURowIDType, ObRowIDTC),                 \
    (ObLobType, ObLobTC),                      \
    (ObJsonType, ObJsonTC),                    \
    (ObGeometryType, ObGeometryTC),            \
    (ObUserDefinedSQLType, ObUserDefinedSQLTC),\
    (ObDecimalIntType, ObDecimalIntTC),        \
    (ObCollectionSQLType, ObCollectionSQLTC),  \
    (ObMySQLDateType, ObMySQLDateTC),          \
    (ObMySQLDateTimeType, ObMySQLDateTimeTC),  \
    (ObRoaringBitmapType, ObRoaringBitmapTC)

#define SELECT_SECOND(x, y) y
#define SELECT_TC(arg) SELECT_SECOND arg

const ObObjTypeClass OBJ_TYPE_TO_CLASS[ObMaxType] =
{
	LST_DO(SELECT_TC, (,), OBJ_TYPE_TC_PAIRS)
};

#undef SELECT_TC
#undef SELECT_SECOND

const ObObjType OBJ_DEFAULT_TYPE[ObActualMaxTC] =
{
  ObNullType,       // null
  ObIntType,        // int
  ObUInt64Type,     // uint
  ObFloatType,      // float
  ObDoubleType,     // double
  ObNumberType,     // number
  ObDateTimeType,   // datetime
  ObDateType,       // date
  ObTimeType,       // time
  ObYearType,       // year
  ObVarcharType,    // varchar
  ObExtendType,     // extend
  ObUnknownType,    // unknown
  ObLongTextType,   // text
  ObBitType,        // bit
  ObUInt64Type,     // enumset
  ObMaxType,        // enumsetInner
  ObTimestampNanoType,// timestamp nano
  ObRawType,        // raw
  ObMaxType,        // no default type for interval type class
  ObMaxType,        // no default type for rowid type class
  ObLobType,        // lob
  ObJsonType,       // json
  ObGeometryType,   // geometry
  ObUserDefinedSQLType, // user defined type in sql
  ObDecimalIntType, // decimal int
  ObCollectionSQLType,  // collection type in sql
  ObMySQLDateType,     // mysql date
  ObMySQLDateTimeType, // mysql datetime
  ObRoaringBitmapType,  // roaringbitmap
  ObMaxType,        // maxtype
  ObUInt64Type,     // int&uint
  ObMaxType,        // lefttype
  ObMaxType,        // righttype
};

//plz check if OB_OBJ_IMPLICIT_CAST_DIRECTION_FOR_ORACLE is also need to be modified after you modify this one
static ObObjTypeClass OBJ_O_TYPE_TO_CLASS[ObOMaxType + 1] =
{
  ObMaxTC,        //ObONotSupport = 1
  ObNullTC,       // ObONullType = 2,   // 空类型
  ObIntTC,        // ObOSmallIntType=3,
  ObIntTC,        // ObOIntType=4,
  ObNumberTC,     // ObOBinFloatType=5,          //float
  ObFloatTC,      // ObOBinFloatType=6,           //binary float
  ObDoubleTC,     // ObOBinDoubleType=7,          //binary double
  ObNumberTC,     // ObONumbertype=8,
  ObStringTC,     // ObOCharType=9,
  ObStringTC,     // ObOVarcharType=10,
  ObDateTimeTC,   // ObODateType11,
  ObOTimestampTC, // ObOTimestampTZType=12,
  ObOTimestampTC, // ObOTimestampLTZType=13,
  ObOTimestampTC, // ObOTimestampType=14,
  ObIntervalTC,   // ObOIntervalYMType=15,
  ObIntervalTC,   // ObOIntervalDSType=16,
  ObTextTC,       // ObOTextType=17,
  ObExtendTC,     // ObOExtendType=18,       //not sure
  ObUnknownTC,    // ObOUnknownType=19,      //not sure
  ObRawTC,        // ObORawType
  ObStringTC,     // ObONVarchar2
  ObStringTC,     // ObONChar
  ObRowIDTC,      // ObOURowID
  ObLobTC,        // ObOLobLocator
  ObJsonTC,       // ObOJsonType
  ObGeometryTC,   // ObOGeometryType
  ObUserDefinedSQLTC, // ObOUDTSqlType
  ObCollectionSQLTC, // ObCollectionSqlType
  ObRoaringBitmapTC, // ObRoaringBitmapType
  ObMaxTC
};

enum ImplicitCastDirection {
  IC_NOT_SUPPORT = -1,
  IC_NO_CAST = 0,
  IC_A_TO_B = 1,
  IC_B_TO_A = 2,
  IC_TO_MIDDLE_TYPE = 3,
  IC_A_TO_C = 4,
  IC_B_TO_C = 5,
};

/*
 * Datetime > double > float > number > int/smallint > char/varchar > others
 */
static ImplicitCastDirection OB_OBJ_IMPLICIT_CAST_DIRECTION_FOR_ORACLE[ObObjOType::ObOMaxType][ObObjOType::ObOMaxType] =
{   /*A->B*/
  {/*ObONotSupport->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NOT_SUPPORT,  /*Null*/
    IC_NOT_SUPPORT,  /*SmallInt*/
    IC_NOT_SUPPORT,  /*Int*/
    IC_NOT_SUPPORT,  /*Float*/
    IC_NOT_SUPPORT,  /*BinaryFloat*/
    IC_NOT_SUPPORT,  /*BinaryDouble*/
    IC_NOT_SUPPORT,  /*Number*/
    IC_NOT_SUPPORT,  /*Char*/
    IC_NOT_SUPPORT,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_NOT_SUPPORT,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_NOT_SUPPORT,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*Null->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NO_CAST,  /*SmallInt*/
    IC_NO_CAST,  /*Int*/
    IC_NO_CAST,  /*Float*/
    IC_NO_CAST,  /*BinaryFloat*/
    IC_NO_CAST,  /*BinaryDouble*/
    IC_NO_CAST,  /*Number*/
    IC_NO_CAST,  /*Char*/
    IC_NO_CAST,  /*Varchar*/
    IC_NO_CAST,  /*Date*/
    IC_NO_CAST,  /*TimestampTZ*/
    IC_NO_CAST,  /*TimestampLTZ*/
    IC_NO_CAST,  /*Timestamp*/
    IC_NO_CAST,  /*IntervalYM*/
    IC_NO_CAST,  /*IntervalDS*/
    IC_NO_CAST,  /*Lob*/
    IC_NO_CAST,  /*Extend*/
    IC_NO_CAST,  /*Unknown*/
    IC_NO_CAST,  /*Raw*/
    IC_NO_CAST,  /*NVarchar2*/
    IC_NO_CAST,  /*NChar*/
    IC_NO_CAST,  /*URowID*/
    IC_NO_CAST,  /*LobLocator*/
    IC_A_TO_B,  /*JsonType*/
  },
  {/*SmallInt->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NO_CAST,  /*SmallInt*/
    IC_NO_CAST,  /*Int*/
    IC_TO_MIDDLE_TYPE,   /*Float*/
    IC_A_TO_B,  /*BinaryFloat*/
    IC_A_TO_B,  /*BinaryDouble*/
    IC_NO_CAST, //IC_A_TO_B,  /*Number*/
    IC_TO_MIDDLE_TYPE,  /*Char*/
    IC_TO_MIDDLE_TYPE,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_TO_MIDDLE_TYPE,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_NOT_SUPPORT,  /*NVarchar2*/
    IC_NOT_SUPPORT,  /*NChar*/
    IC_NOT_SUPPORT,  /*URowID*/
    IC_TO_MIDDLE_TYPE,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*Int->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NO_CAST,  /*SmallInt*/
    IC_NO_CAST,  /*Int*/
    IC_TO_MIDDLE_TYPE,   /*Float*/
    IC_A_TO_B,  /*BinaryFloat*/
    IC_A_TO_B,  /*BinaryDouble*/
    IC_NO_CAST, //IC_A_TO_B,  /*Number*/
    IC_TO_MIDDLE_TYPE,  /*Char*/
    IC_TO_MIDDLE_TYPE,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_TO_MIDDLE_TYPE,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_NOT_SUPPORT,  /*NVarchar2*/
    IC_NOT_SUPPORT,  /*NChar*/
    IC_NOT_SUPPORT,  /*URowID*/
    IC_TO_MIDDLE_TYPE,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*Float->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_TO_MIDDLE_TYPE,  /*SmallInt*/
    IC_TO_MIDDLE_TYPE,  /*Int*/
    IC_NO_CAST, /*Float*/
    IC_A_TO_B,  /*BinaryFloat*/
    IC_A_TO_B,  /*BinaryDouble*/
    IC_A_TO_B,  /*Number*/
    IC_TO_MIDDLE_TYPE,  /*Char*/
    IC_TO_MIDDLE_TYPE,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_TO_MIDDLE_TYPE,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_TO_MIDDLE_TYPE,  /*NVarchar2*/
    IC_TO_MIDDLE_TYPE,  /*NChar*/
    IC_NOT_SUPPORT,  /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*BinaryFloat->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_B_TO_A,  /*SmallInt*/
    IC_B_TO_A,  /*Int*/
    IC_B_TO_A,  /*Float*/
    IC_NO_CAST,  /*BinaryFloat*/
    IC_A_TO_B,  /*BinaryDouble*/
    IC_B_TO_A,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT,  /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*BinaryDouble->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_B_TO_A,  /*SmallInt*/
    IC_B_TO_A,  /*Int*/
    IC_B_TO_A, /*Float*/
    IC_B_TO_A, /*BinaryFloat*/
    IC_NO_CAST, /*BinaryDouble*/
    IC_B_TO_A,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT,  /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*Number->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NO_CAST,  //IC_B_TO_A,  /*SmallInt*/
    IC_NO_CAST,  //IC_B_TO_A,  /*Int*/
    IC_B_TO_A,   /*Float*/
    IC_A_TO_B,   /*BinaryFloat*/
    IC_A_TO_B,   /*BinaryDouble*/
    IC_NO_CAST,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*Char->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_TO_MIDDLE_TYPE,  /*SmallInt*/
    IC_TO_MIDDLE_TYPE,  /*Int*/
    IC_TO_MIDDLE_TYPE, /*Float*/
    IC_A_TO_B, /*BinaryFloat*/
    IC_A_TO_B, /*BinaryDouble*/
    IC_A_TO_B,  /*Number*/
    IC_NO_CAST,  /*Char*/
    IC_NO_CAST,  /*Varchar*/
    IC_A_TO_B,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_A_TO_B,  /*Timestamp*/
    IC_A_TO_B,  /*IntervalYM*/
    IC_A_TO_B,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_B_TO_A,  /*Raw*/
    IC_A_TO_B,  /*NVarchar2*/
    IC_A_TO_B,  /*NChar*/
    IC_A_TO_B,  /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_A_TO_B,  /*JsonType*/
  },
  {/*Varchar->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_TO_MIDDLE_TYPE,  /*SmallInt*/
    IC_TO_MIDDLE_TYPE,  /*Int*/
    IC_TO_MIDDLE_TYPE, /*Float*/
    IC_A_TO_B, /*BinaryFloat*/
    IC_A_TO_B, /*BinaryDouble*/
    IC_A_TO_B,  /*Number*/
    IC_NO_CAST,  /*Char*/
    IC_NO_CAST,  /*Varchar*/
    IC_A_TO_B,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_A_TO_B,  /*Timestamp*/
    IC_A_TO_B,  /*IntervalYM*/
    IC_A_TO_B,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_B_TO_A,  /*Raw*/
    IC_A_TO_B,  /*NVarchar2*/
    IC_A_TO_C,  /*NChar*/
    IC_A_TO_B,  /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_A_TO_B,  /*JsonType*/
  },
  {/*Date->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NOT_SUPPORT,  /*SmallInt*/
    IC_NOT_SUPPORT,  /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_NO_CAST,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_A_TO_B,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*TimestampTZ->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NOT_SUPPORT,  /*SmallInt*/
    IC_NOT_SUPPORT,  /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_B_TO_A,  /*Date*/
    IC_NO_CAST,  /*TimestampTZ*/
    IC_B_TO_A,  /*TimestampLTZ*/  //oracle called internal_function(TimestampLTZ) when cmp with TimestampTZ
    IC_B_TO_A,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*TimestampLTZ->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST, /*Null*/
    IC_NOT_SUPPORT, /*SmallInt*/
    IC_NOT_SUPPORT, /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT, /*Number*/
    IC_B_TO_A, /*Char*/
    IC_B_TO_A, /*Varchar*/
    IC_B_TO_A, /*Date*/
    IC_A_TO_B, /*TimestampTZ*/ //oracle called internal_function(TimestampLTZ) when cmp with TimestampTZ
    IC_NO_CAST, /*TimestampLTZ*/
    IC_B_TO_A, /*Timestamp*/
    IC_NOT_SUPPORT, /*IntervalYM*/
    IC_NOT_SUPPORT, /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT, /*Extend*/
    IC_NOT_SUPPORT, /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*Timestamp->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NOT_SUPPORT,  /*SmallInt*/
    IC_NOT_SUPPORT,  /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_B_TO_A,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_NO_CAST,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*ObRowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*IntervalYM->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST, /*Null*/
    IC_NOT_SUPPORT, /*SmallInt*/
    IC_NOT_SUPPORT, /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT, /*Number*/
    IC_B_TO_A, /*Char*/
    IC_B_TO_A, /*Varchar*/
    IC_NO_CAST, /*Date*/
    IC_NO_CAST, /*TimestampTZ*/
    IC_NO_CAST, /*TimestampLTZ*/
    IC_NO_CAST, /*Timestamp*/
    IC_NO_CAST, /*IntervalYM*/
    IC_NOT_SUPPORT, /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT, /*Extend*/
    IC_NOT_SUPPORT, /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*ObRowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*IntervalDS->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST, /*Null*/
    IC_NOT_SUPPORT, /*SmallInt*/
    IC_NOT_SUPPORT, /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT, /*Number*/
    IC_B_TO_A, /*Char*/
    IC_B_TO_A, /*Varchar*/
    IC_NO_CAST, /*Date*/
    IC_NO_CAST, /*TimestampTZ*/
    IC_NO_CAST, /*TimestampLTZ*/
    IC_NO_CAST, /*Timestamp*/
    IC_NOT_SUPPORT, /*IntervalYM*/
    IC_NO_CAST, /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT, /*Extend*/
    IC_NOT_SUPPORT, /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*ObRowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*Lob->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_TO_MIDDLE_TYPE,  /*SmallInt*/
    IC_TO_MIDDLE_TYPE,  /*Int*/
    IC_TO_MIDDLE_TYPE, /*Float*/
    IC_A_TO_B, /*BinaryFloat*/
    IC_A_TO_B, /*BinaryDouble*/
    IC_A_TO_B,  /*Number*/
    IC_A_TO_B,  /*Char*/
    IC_A_TO_B,  /*Varchar*/
    IC_A_TO_B,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_A_TO_B,  /*Timestamp*/
    IC_A_TO_B,  /*IntervalYM*/
    IC_A_TO_B,  /*IntervalDS*/
    IC_NO_CAST,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/ // todo: @hanhui
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_A_TO_B, /*JsonType*/
  },
  {/*Extend->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST, /*Null*/
    IC_NOT_SUPPORT, /*SmallInt*/
    IC_NOT_SUPPORT, /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT, /*Number*/
    IC_NOT_SUPPORT, /*Char*/
    IC_NOT_SUPPORT, /*Varchar*/
    IC_NOT_SUPPORT, /*Date*/
    IC_NOT_SUPPORT, /*TimestampTZ*/
    IC_NOT_SUPPORT, /*TimestampLTZ*/
    IC_NOT_SUPPORT, /*Timestamp*/
    IC_NOT_SUPPORT, /*IntervalYM*/
    IC_NOT_SUPPORT, /*IntervalDS*/
    IC_NOT_SUPPORT, /*Lob*/
    IC_NO_CAST, /*Extend*/
    IC_NOT_SUPPORT, /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_NOT_SUPPORT,  /*NVarchar2*/
    IC_NOT_SUPPORT,  /*NChar*/
    IC_NOT_SUPPORT,  /*URowID*/
    IC_NOT_SUPPORT,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*UnKnown->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST, /*Null*/
    IC_NOT_SUPPORT, /*SmallInt*/
    IC_NOT_SUPPORT, /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT, /*Number*/
    IC_NOT_SUPPORT, /*Char*/
    IC_NOT_SUPPORT, /*Varchar*/
    IC_NOT_SUPPORT, /*Date*/
    IC_NOT_SUPPORT, /*TimestampTZ*/
    IC_NOT_SUPPORT, /*TimestampLTZ*/
    IC_NOT_SUPPORT, /*Timestamp*/
    IC_NOT_SUPPORT, /*IntervalYM*/
    IC_NOT_SUPPORT, /*IntervalDS*/
    IC_NOT_SUPPORT, /*Lob*/
    IC_NOT_SUPPORT, /*Extend*/
    IC_NO_CAST, /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_NOT_SUPPORT,  /*NVarchar2*/
    IC_NOT_SUPPORT,  /*NChar*/
    IC_NOT_SUPPORT,  /*URowID*/
    IC_NOT_SUPPORT,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*Raw->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST, /*Null*/
    IC_NO_CAST, /*SmallInt*/
    IC_NO_CAST, /*Int*/
    IC_NO_CAST, /*Float*/
    IC_NO_CAST, /*BinaryFloat*/
    IC_NO_CAST, /*BinaryDouble*/
    IC_NO_CAST, /*Number*/
    IC_A_TO_B, /*Char*/
    IC_A_TO_B, /*Varchar*/
    IC_NO_CAST, /*Date*/
    IC_NO_CAST, /*TimestampTZ*/
    IC_NO_CAST, /*TimestampLTZ*/
    IC_NO_CAST, /*Timestamp*/
    IC_NOT_SUPPORT, /*IntervalYear*/
    IC_NOT_SUPPORT, /*IntervalDay*/
    IC_NOT_SUPPORT, /*Lob*/ // todo: @hanhui
    IC_NO_CAST, /*Extend*/
    IC_NO_CAST, /*Unknown*/
    IC_NO_CAST,  /*Raw*/
    IC_A_TO_B,  /*NVarchar2*/
    IC_A_TO_B,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_NOT_SUPPORT,  /*LobLocator*/
    IC_NOT_SUPPORT,  /*JsonType*/
  },
  {/*NVarchar->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_TO_MIDDLE_TYPE,  /*SmallInt*/
    IC_TO_MIDDLE_TYPE,  /*Int*/
    IC_TO_MIDDLE_TYPE, /*Float*/
    IC_A_TO_B, /*BinaryFloat*/
    IC_A_TO_B, /*BinaryDouble*/
    IC_A_TO_B,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_A_TO_B,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_A_TO_B,  /*Timestamp*/
    IC_A_TO_B,  /*IntervalYM*/
    IC_A_TO_B,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_B_TO_A,  /*Raw*/
    IC_NO_CAST,  /*NVarchar2*/
    IC_NO_CAST,   /*NChar*/
    IC_A_TO_B,  /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_A_TO_B, /*JsonType*/
  },
  {/*NChar->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_TO_MIDDLE_TYPE,  /*SmallInt*/
    IC_TO_MIDDLE_TYPE,  /*Int*/
    IC_TO_MIDDLE_TYPE, /*Float*/
    IC_A_TO_B, /*BinaryFloat*/
    IC_A_TO_B, /*BinaryDouble*/
    IC_A_TO_B,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_C,  /*Varchar*/
    IC_A_TO_B,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_A_TO_B,  /*Timestamp*/
    IC_A_TO_B,  /*IntervalYM*/
    IC_A_TO_B,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_B_TO_A,  /*Raw*/
    IC_NO_CAST,  /*NVarchar2*/
    IC_NO_CAST,  /*NChar*/
    IC_A_TO_B, /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_A_TO_B, /*JsonType*/
  },
  {/*URowID->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_NOT_SUPPORT,  /*SmallInt*/
    IC_NOT_SUPPORT,  /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_NOT_SUPPORT,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NO_CAST, /*URowID*/
    IC_NOT_SUPPORT,  /*LobLocator*/
    IC_NOT_SUPPORT, /*JsonType*/
  },
  {/*LobLocator->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_NO_CAST,  /*Null*/
    IC_TO_MIDDLE_TYPE,  /*SmallInt*/
    IC_TO_MIDDLE_TYPE,  /*Int*/
    IC_A_TO_B, /*Float*/
    IC_A_TO_B, /*BinaryFloat*/
    IC_A_TO_B, /*BinaryDouble*/
    IC_A_TO_B,  /*Number*/
    IC_A_TO_B,  /*Char*/
    IC_A_TO_B,  /*Varchar*/
    IC_A_TO_B,  /*Date*/
    IC_A_TO_B,  /*TimestampTZ*/
    IC_A_TO_B,  /*TimestampLTZ*/
    IC_A_TO_B,  /*Timestamp*/
    IC_A_TO_B,  /*IntervalYM*/
    IC_A_TO_B,  /*IntervalDS*/
    IC_A_TO_B,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_A_TO_B,  /*NVarchar2*/
    IC_A_TO_B,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_NO_CAST,  /*LobLocator*/
    IC_A_TO_B, /*JsonType*/
  },
    {/*JsonType->XXX*/
    IC_NOT_SUPPORT,  /*ObONotSupport*/
    IC_B_TO_A,  /*Null*/
    IC_NOT_SUPPORT,  /*SmallInt*/
    IC_NOT_SUPPORT,  /*Int*/
    IC_NOT_SUPPORT, /*Float*/
    IC_NOT_SUPPORT, /*BinaryFloat*/
    IC_NOT_SUPPORT, /*BinaryDouble*/
    IC_NOT_SUPPORT,  /*Number*/
    IC_B_TO_A,  /*Char*/
    IC_B_TO_A,  /*Varchar*/
    IC_NOT_SUPPORT,  /*Date*/
    IC_NOT_SUPPORT,  /*TimestampTZ*/
    IC_NOT_SUPPORT,  /*TimestampLTZ*/
    IC_NOT_SUPPORT,  /*Timestamp*/
    IC_NOT_SUPPORT,  /*IntervalYM*/
    IC_NOT_SUPPORT,  /*IntervalDS*/
    IC_B_TO_A,  /*Lob*/
    IC_NOT_SUPPORT,  /*Extend*/
    IC_NOT_SUPPORT,  /*Unknown*/
    IC_NOT_SUPPORT,  /*Raw*/
    IC_B_TO_A,  /*NVarchar2*/
    IC_B_TO_A,  /*NChar*/
    IC_NOT_SUPPORT, /*URowID*/
    IC_B_TO_A,  /*LobLocator*/
    IC_B_TO_A, /*JsonType*/
  },
};

enum ObExtObjType
{
  //1~99 used for PL extended type
  T_EXT_SQL_ARRAY = 100,
  //100~199 used for SQL extended type
  T_EXT_SQL_END = 200
};

// systemd defined udt type (fixed udt id)
enum ObUDTType
{
  T_OBJ_NOT_SUPPORTED = 0,
  T_OBJ_XML = 300001,
  T_OBJ_SDO_POINT = 300027,
  T_OBJ_SDO_GEOMETRY = 300028,
  T_OBJ_SDO_ELEMINFO_ARRAY = 300029,
  T_OBJ_SDO_ORDINATE_ARRAY = 300030,
};

// reserved sub schema id for system defined types
enum ObSystemUDTSqlType
{
  ObXMLSqlType = 0,
  ObMaxSystemUDTSqlType = 16, // used not supported cases;
  ObInvalidSqlType = 17 // only used when subschema id not set, like the creation of col ref rawexpr
};

OB_INLINE bool is_valid_obj_type(const ObObjType type)
{
  return ObNullType <= type && type < ObMaxType;
}

OB_INLINE bool is_valid_oracle_type(const ObObjOType otype)
{
  //not include ObONotSupport
  return ObONotSupport < otype && otype < ObOMaxType;
}

OB_INLINE ObObjOType ob_obj_type_to_oracle_type(const ObObjType type)
{
  return type < ObMaxType ? OBJ_TYPE_TO_O_TYPE[type] : ObOMaxType;
}

OB_INLINE ObObjTypeClass ob_oracle_type_class(const ObObjType type)
{
  return type < ObMaxType ? OBJ_O_TYPE_TO_CLASS[OBJ_TYPE_TO_O_TYPE[type]] : ObMaxTC;
}

OB_INLINE ObObjTypeClass ob_oracle_type_class(const ObObjOType oType)
{
  return oType < ObOMaxType ? OBJ_O_TYPE_TO_CLASS[oType] : ObMaxTC;
}

OB_INLINE ImplicitCastDirection ob_oracle_implict_cast_direction(const ObObjOType aType, const ObObjOType bType)
{
  if (aType < ObOMaxType && bType < ObOMaxType) {
    return OB_OBJ_IMPLICIT_CAST_DIRECTION_FOR_ORACLE[aType][bType];
  } else {
    return ImplicitCastDirection::IC_NOT_SUPPORT;
  }
}

OB_INLINE ObObjTypeClass ob_obj_type_class(const ObObjType type)
{
  return OB_LIKELY(type < ObMaxType) ? OBJ_TYPE_TO_CLASS[type] : ObMaxTC;
}

OB_INLINE ObObjType ob_obj_default_type(const ObObjTypeClass tc)
{
  return OB_LIKELY(tc < ObActualMaxTC) ? OBJ_DEFAULT_TYPE[tc] : ObMaxType;
}

enum VecValueTypeClass: uint16_t {
  VEC_TC_NULL = 0,
  VEC_TC_INTEGER,
  VEC_TC_UINTEGER,
  VEC_TC_FLOAT,
  VEC_TC_DOUBLE,
  VEC_TC_FIXED_DOUBLE,
  VEC_TC_NUMBER,
  VEC_TC_DATETIME,
  VEC_TC_DATE,
  VEC_TC_TIME,
  VEC_TC_YEAR,
  VEC_TC_EXTEND,
  VEC_TC_UNKNOWN,
  VEC_TC_STRING,
  VEC_TC_BIT,
  VEC_TC_ENUM_SET,
  VEC_TC_ENUM_SET_INNER,
  VEC_TC_TIMESTAMP_TZ,   // ObTimestampTZType
  VEC_TC_TIMESTAMP_TINY, // ObTimestampNanoType, ObTimestampLTZType
  VEC_TC_RAW,
  VEC_TC_INTERVAL_YM,
  VEC_TC_INTERVAL_DS,
  VEC_TC_ROWID,
  VEC_TC_LOB,
  VEC_TC_JSON,
  VEC_TC_GEO,
  //TODO shengle give a doc for how to add a new type
  VEC_TC_UDT,
  VEC_TC_DEC_INT32,
  VEC_TC_DEC_INT64,
  VEC_TC_DEC_INT128,
  VEC_TC_DEC_INT256,
  VEC_TC_DEC_INT512,
  VEC_TC_COLLECTION,
  VEC_TC_ROARINGBITMAP,
  MAX_VEC_TC
};

inline bool ob_is_double_type(ObObjType);
inline bool ob_is_decimal_int(ObObjType);

OB_INLINE VecValueTypeClass get_vec_value_tc(const ObObjType type, const int16_t scale,
                                             const int16_t precision)
{
  UNUSED(precision);
  const static VecValueTypeClass maps[] = {
    VEC_TC_NULL,              // ObNullType
    VEC_TC_INTEGER,           // ObTinyIntType
    VEC_TC_INTEGER,           // ObSmallIntType
    VEC_TC_INTEGER,           // ObMediumIntType
    VEC_TC_INTEGER,           // ObInt32Type
    VEC_TC_INTEGER,           // ObIntType
    VEC_TC_UINTEGER,          // ObUTinyIntType
    VEC_TC_UINTEGER,          // ObUSmallIntType
    VEC_TC_UINTEGER,          // ObUMediumIntType
    VEC_TC_UINTEGER,          // ObUInt32Type
    VEC_TC_UINTEGER,          // ObUInt64Type
    VEC_TC_FLOAT,             // ObFloatType
    VEC_TC_DOUBLE,            // ObDoubleType
    VEC_TC_FLOAT,             // ObUFloatType
    VEC_TC_DOUBLE,            // ObUDoubleType
    VEC_TC_NUMBER,            // ObNumberType
    VEC_TC_NUMBER,            // ObUNumberType
    VEC_TC_DATETIME,          // ObDateTimeType
    VEC_TC_DATETIME,          // ObTimestampType
    VEC_TC_DATE,              // ObDateType
    VEC_TC_TIME,              // ObTimeType
    VEC_TC_YEAR,              // ObYearType
    VEC_TC_STRING,            // ObVarcharType
    VEC_TC_STRING,            // ObCharType
    VEC_TC_STRING,            // ObHexStringType
    VEC_TC_EXTEND,            // ObExtendType
    VEC_TC_UNKNOWN,           // ObUnknownType
    VEC_TC_STRING,            // ObTinyTextType
    VEC_TC_LOB,               // ObTextType
    VEC_TC_LOB,               // ObMediumTextType
    VEC_TC_LOB,               // ObLongTextType
    VEC_TC_BIT,               // ObBitType
    VEC_TC_ENUM_SET,          // ObEnumType
    VEC_TC_ENUM_SET,          // ObSetType
    VEC_TC_ENUM_SET_INNER,    // ObEnumInnerType
    VEC_TC_ENUM_SET_INNER,    // ObSetInnerType
    VEC_TC_TIMESTAMP_TZ,      // ObTimestampTZType
    VEC_TC_TIMESTAMP_TINY,    // ObTimestampLTZType
    VEC_TC_TIMESTAMP_TINY,    // ObTimestampNanoType
    VEC_TC_RAW,               // ObRawType
    VEC_TC_INTERVAL_YM,       // ObIntervalYMType
    VEC_TC_INTERVAL_DS,       // ObIntervalDSType
    VEC_TC_NUMBER,            // ObNumberFloatType
    VEC_TC_STRING,            // ObNVarchar2Type
    VEC_TC_STRING,            // ObNCharType
    VEC_TC_ROWID,             // ObURowID
    VEC_TC_LOB,               // ObLobType
    VEC_TC_JSON,              // ObJsonType
    VEC_TC_GEO,               // ObGeometryType
    VEC_TC_UDT,               // ObUserDefinedSQLType
    MAX_VEC_TC,               // invalid for ObDecimalIntType
    VEC_TC_COLLECTION,        // ObCollectionSQLType
    MAX_VEC_TC,               // reserved for ObMySQLDateType
    MAX_VEC_TC,               // reserved for ObMySQLDateTimeType
    VEC_TC_ROARINGBITMAP      // ObRoaringBitmapType
  };
  VecValueTypeClass t = MAX_VEC_TC;
  if (type < 0 || type >= ObMaxType) {
    OB_ASSERT(false);
  } else {
    if (ob_is_double_type(type) && scale > SCALE_UNKNOWN_YET
        && scale <= OB_MAX_DOUBLE_FLOAT_SCALE) {
      t = VEC_TC_FIXED_DOUBLE;
    } else if (ob_is_decimal_int(type)) {
      if (precision <= MAX_PRECISION_DECIMAL_INT_32) {
        t = VEC_TC_DEC_INT32;
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_64) {
        t = VEC_TC_DEC_INT64;
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_128) {
        t = VEC_TC_DEC_INT128;
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_256) {
        t = VEC_TC_DEC_INT256;
      } else if (precision <= MAX_PRECISION_DECIMAL_INT_512) {
        t = VEC_TC_DEC_INT512;
      }
    } else {
      t = maps[type];
    }
  }
  return t;
}

extern const int64_t INT_MIN_VAL[ObMaxType];
extern const int64_t INT_MAX_VAL[ObMaxType];
extern const uint64_t UINT_MAX_VAL[ObMaxType];
extern const double REAL_MIN_VAL[ObMaxType];
extern const double REAL_MAX_VAL[ObMaxType];


OB_INLINE bool ob_is_castable_type_class(ObObjTypeClass tc)
{
  return (ObIntTC <= tc && tc <= ObStringTC) || ObLeftTypeTC == tc || ObRightTypeTC == tc
      || ObBitTC == tc || ObEnumSetTC == tc || ObEnumSetInnerTC == tc || ObTextTC == tc
      || ObOTimestampTC == tc || ObRawTC == tc || ObIntervalTC == tc
      || ObRowIDTC == tc || ObLobTC == tc || ObJsonTC == tc || ObGeometryTC == tc
      || ObUserDefinedSQLTC == tc || ObDecimalIntTC == tc || ObCollectionSQLTC == tc
      || ObRoaringBitmapTC == tc;
}

//used for arithmetic
OB_INLINE bool ob_is_int_uint(ObObjTypeClass left_tc, ObObjTypeClass right_tc)
{
  return (ObIntTC == left_tc && ObUIntTC == right_tc) || (ObIntTC == right_tc && ObUIntTC == left_tc);
}

OB_INLINE bool ob_is_int_less_than_64(ObObjType type)
{
  return (ObTinyIntType <= type && type <= ObInt32Type)
         || (ObUTinyIntType <= type && type <= ObUInt32Type);
}

// print obj type string
const char *ob_obj_type_str(ObObjType type);

const char *inner_obj_type_str(ObObjType type);
const char *ob_sql_type_str(ObObjType type);

//such as "double(10,7)". with accuracy
int ob_sql_type_str(char *buff,
                    int64_t buff_length,
                    int64_t &pos,
                    ObObjType type,
                    int64_t length,
                    int64_t precision,
                    int64_t scale,
                    ObCollationType coll_type,
                    const uint64_t sub_type = static_cast<uint64_t>(common::ObGeoType::GEOTYPEMAX));

int ob_sql_type_str(const common::ObObjMeta &obj_meta,
                    const common::ObAccuracy &accuracy,
                    const common::ObIArray<ObString> &type_info,
                    const int16_t default_length_semantics,
                    char *buff, int64_t buff_length, int64_t &pos,
                    const uint64_t sub_type = static_cast<uint64_t>(common::ObGeoType::GEOTYPEMAX));


//such as "double". without any accuracy.
int ob_sql_type_str(char *buff,
                    int64_t buff_length,
                    ObObjType type,
                    ObCollationType coll_type,
                    const common::ObGeoType geo_type = common::ObGeoType::GEOTYPEMAX);

// print obj type class string
const char *ob_obj_tc_str(ObObjTypeClass tc);
const char *ob_sql_tc_str(ObObjTypeClass tc);

// get obj type size for fixed length type
int32_t ob_obj_type_size(ObObjType type);

inline bool ob_is_valid_obj_type(ObObjType type) { return ObNullType <= type && type < ObMaxType; }
inline bool ob_is_invalid_obj_type(ObObjType type) { return !ob_is_valid_obj_type(type); }
inline bool ob_is_valid_obj_tc(ObObjTypeClass tc) { return ObNullTC <= tc && tc < ObMaxTC; }
inline bool ob_is_invalid_obj_tc(ObObjTypeClass tc) { return !ob_is_valid_obj_tc(tc); }

inline bool ob_is_valid_obj_o_type(ObObjType type) { return ObNullType <= type && type < ObMaxType ? (OBJ_TYPE_TO_O_TYPE[type] != ObONotSupport) : false; }
inline bool ob_is_invalid_obj_o_type(ObObjType type) { return !ob_is_valid_obj_o_type(type); }
inline bool ob_is_valid_obj_o_tc(ObObjTypeClass tc) { return ObNullTC <= tc && tc < ObMaxTC; }
inline bool ob_is_invalid_obj_o_tc(ObObjTypeClass tc) { return !ob_is_valid_obj_tc(tc); }
inline bool ob_is_numeric_tc(ObObjTypeClass tc)
{
  return (tc >= ObIntTC && tc <= ObNumberTC) || tc == ObBitTC || ObDecimalIntTC == tc;
}

inline bool ob_is_int_tc(ObObjType type) { return ObIntTC == ob_obj_type_class(type); }
inline bool ob_is_uint_tc(ObObjType type) { return ObUIntTC == ob_obj_type_class(type); }
inline bool ob_is_int_uint_tc(ObObjType type) { return ob_is_int_tc(type) || ob_is_uint_tc(type); }
inline bool ob_is_float_tc(ObObjType type) { return ObFloatTC == ob_obj_type_class(type); }
inline bool ob_is_double_tc(ObObjType type) { return ObDoubleTC == ob_obj_type_class(type); }
inline bool ob_is_number_tc(ObObjType type) { return ObNumberTC == ob_obj_type_class(type); }
inline bool ob_is_datetime_tc(ObObjType type) { return ObDateTimeTC == ob_obj_type_class(type); }
inline bool ob_is_date_tc(ObObjType type) { return ObDateTC == ob_obj_type_class(type); }
inline bool ob_is_otimestampe_tc(ObObjType type) { return ObOTimestampTC == ob_obj_type_class(type); }
inline bool ob_is_time_tc(ObObjType type) { return ObTimeTC == ob_obj_type_class(type); }
inline bool ob_is_year_tc(ObObjType type) { return ObYearTC == ob_obj_type_class(type); }
inline bool ob_is_string_tc(ObObjType type) { return ObStringTC == ob_obj_type_class(type); }
inline bool ob_is_text_tc(ObObjType type) { return ObTextTC == ob_obj_type_class(type); }
inline bool ob_is_bit_tc(ObObjType type) { return ObBitTC == ob_obj_type_class(type); }
inline bool ob_is_raw_tc(ObObjType type) { return ObRawTC == ob_obj_type_class(type); }
inline bool ob_is_interval_tc(ObObjType type) { return ObIntervalTC == ob_obj_type_class(type); }
inline bool ob_is_lob_tc(ObObjType type) { return ObLobTC == ob_obj_type_class(type); }
inline bool ob_is_json_tc(ObObjType type) { return ObJsonTC == ob_obj_type_class(type); }
inline bool ob_is_geometry_tc(ObObjType type) { return ObGeometryTC == ob_obj_type_class(type); }
inline bool ob_is_decimal_int_tc(ObObjType type) { return ObDecimalIntTC == ob_obj_type_class(type); }
inline bool ob_is_roaringbitmap_tc(ObObjType type) { return ObRoaringBitmapTC == ob_obj_type_class(type); }

inline bool is_lob(ObObjType type) { return ob_is_text_tc(type); }
inline bool is_lob_locator(ObObjType type) { return ObLobType == type; }

// test type catalog
inline bool ob_is_integer_type(ObObjType type) { return ObTinyIntType <= type && type <= ObUInt64Type; }
inline bool ob_is_numeric_type(ObObjType type)
{
  return (ObTinyIntType <= type && type <= ObUNumberType) || type == ObBitType
         || type == ObNumberFloatType || type == ObDecimalIntType;
}
inline bool ob_is_real_type(ObObjType type) { return ObFloatType <= type && type <= ObUDoubleType; }
inline bool ob_is_float_type(ObObjType type) { return ObFloatType == type || ObUFloatType == type; }
inline bool ob_is_double_type(ObObjType type) { return ObDoubleType == type || ObUDoubleType == type; }
inline bool ob_is_accurate_numeric_type(ObObjType type)
{
  return (ObTinyIntType <= type && type <= ObUInt64Type)
         || (ob_is_number_tc(type)) || ObDecimalIntType == type;
}
inline bool ob_is_unsigned_type(ObObjType type)
{
  return (ObUTinyIntType <= type && type <= ObUInt64Type)
          || ObYearType == type
          || ObUFloatType == type
          || ObUDoubleType == type
          || ObUNumberType == type
          || ObBitType == type;
}
bool is_match_alter_integer_column_online_ddl_rules(const common::ObObjMeta& src_meta,
                                                    const common::ObObjMeta& dst_meta);
bool is_match_alter_string_column_online_ddl_rules(const common::ObObjMeta& src_meta,
                                                   const common::ObObjMeta& dst_meta,
                                                   const int32_t src_len,
                                                   const int32_t dst_len);
inline void convert_unsigned_type_to_signed(ObObjType &type)
{
  switch(type) {
    case(ObUTinyIntType):
      type = ObTinyIntType;
      break;
    case(ObUSmallIntType):
      type = ObSmallIntType;
      break;
    case(ObUMediumIntType):
      type = ObMediumIntType;
      break;
    case(ObUInt32Type):
      type = ObInt32Type;
      break;
    case(ObUInt64Type):
      type = ObIntType;
      break;
    case(ObUFloatType):
      type = ObFloatType;
      break;
    case(ObUDoubleType):
      type = ObDoubleType;
      break;
    case(ObUNumberType):
      type = ObNumberType;
      break;
    default:
      break;
  }
}

inline bool ob_is_oracle_numeric_type(ObObjType type)
{
  return ObIntType == type || ob_is_number_tc(type) || ObFloatType == type || ObDoubleType == type
         || ObDecimalIntType == type;
}
inline bool ob_is_number_or_decimal_int_tc(ObObjType type)
{
  return ob_is_number_tc(type) || ob_is_decimal_int_tc(type);
}
inline bool ob_is_oracle_temporal_type(ObObjType type)
{
  return ObDateTimeType == type || ob_is_otimestampe_tc(type);
}

//this func means when eumset cast to this type, use numberic value
inline bool ob_is_enumset_numeric_type(ObObjType type) { return (ob_is_numeric_type(type) || ObYearType == type); }

inline bool ob_is_enum_or_set_type(ObObjType type) { return ObEnumType == type || ObSetType == type; }
inline bool ob_is_temporal_type(ObObjType type) { return type >= ObDateTimeType && type <= ObYearType; }
inline bool ob_is_string_or_enumset_type(ObObjType type)
{ return (type >= ObVarcharType && type <= ObHexStringType)
      || ObEnumType == type || ObSetType == type
      || ObNVarchar2Type == type || ObNCharType == type; }
inline bool ob_is_otimestamp_type(const ObObjType type) { return (ObTimestampTZType <= type && type <= ObTimestampNanoType); }
inline bool ob_is_varbinary_type(const ObObjType type, const ObCollationType cs_type)
{ return (ObVarcharType == type &&  CS_TYPE_BINARY == cs_type); }
inline bool ob_is_varchar_char_type(const ObObjType type, const ObCollationType cs_type)
{ return (ObVarcharType == type || ObCharType == type) &&  CS_TYPE_BINARY != cs_type; }
inline bool ob_is_varchar_type(const ObObjType type, const ObCollationType cs_type)
{ return ObVarcharType == type && CS_TYPE_BINARY != cs_type; }


inline bool ob_is_binary(ObObjType type, ObCollationType cs_type)
{
  return (ObCharType == type && CS_TYPE_BINARY == cs_type);
}
inline bool ob_is_varbinary_or_binary(ObObjType type, ObCollationType cs_type)
{
  return ob_is_varbinary_type(type, cs_type) || ob_is_binary(type, cs_type);
}
inline bool ob_is_varchar(ObObjType type, ObCollationType cs_type)
{
  return ((type == static_cast<uint8_t>(ObVarcharType)) && (CS_TYPE_BINARY != cs_type));
}
inline bool ob_is_char(ObObjType type, ObCollationType cs_type)
{
  return ((type == static_cast<uint8_t>(ObCharType)) && (CS_TYPE_BINARY != cs_type));
}
inline bool ob_is_nstring(ObObjType type)
{
  return type == static_cast<uint8_t>(ObNVarchar2Type) ||
    type == static_cast<uint8_t>(ObNCharType);
}
inline bool ob_is_varchar_or_char(ObObjType type, ObCollationType cs_type)
{
  return ob_is_varchar(type, cs_type) || ob_is_char(type, cs_type);
}
inline bool ob_is_character_type(ObObjType type, ObCollationType cs_type)
{
  return ob_is_nstring(type) || ob_is_varchar_or_char(type, cs_type);
}
inline bool ob_is_string_type(ObObjType type) { return ob_is_string_tc(type) || ob_is_text_tc(type); }
inline bool ob_is_json(const ObObjType type) { return ObJsonType == type; }
inline bool ob_is_blob_locator(const ObObjType type, const ObCollationType cs_type) { return ObLobType == type && CS_TYPE_BINARY == cs_type; }
inline bool ob_is_clob_locator(const ObObjType type, const ObCollationType cs_type) { return ObLobType == type && CS_TYPE_BINARY != cs_type; }
inline bool ob_is_lob_locator(const ObObjType type) { return ObLobType == type; }
inline bool ob_is_string_or_lob_type(ObObjType type) {
  return ob_is_string_tc(type) || ob_is_text_tc(type) || ob_is_lob_locator(type);
}
inline bool ob_is_clob(const ObObjType type, const ObCollationType cs_type) { return ObLongTextType == type && CS_TYPE_BINARY != cs_type; }
inline bool ob_is_blob(const ObObjType type, const ObCollationType cs_type) { return ObTextTC == ob_obj_type_class(type) && CS_TYPE_BINARY == cs_type; }
inline bool ob_is_text(const ObObjType type, const ObCollationType cs_type) { return ObTextTC == ob_obj_type_class(type) && CS_TYPE_BINARY != cs_type; }
inline bool ob_is_null(const ObObjType type) { return ObNullType == type; }
inline bool ob_is_oracle_datetime_tc(ObObjType type) { return ob_is_otimestampe_tc(type) || ob_is_datetime_tc(type); }

//inline bool ob_is_enum_set_type(ObObjType type) {return (ObEnumType == type || ObSetType == type); }
inline bool ob_is_enumset_tc(ObObjType type) { return ObEnumSetTC == ob_obj_type_class(type); }
inline bool ob_is_enumset_inner_tc(ObObjType type) { return ObEnumSetInnerTC == ob_obj_type_class(type); }
inline bool ob_is_raw(const ObObjType type) { return ObRawType == type; }
inline bool ob_is_extend(const ObObjType type) { return ObExtendType == type; }
inline bool ob_is_urowid(const ObObjType type) { return ObURowIDType == type; }
inline bool ob_is_rowid_tc(const ObObjType type) { return ob_is_urowid(type); }
inline bool ob_is_accuracy_length_valid_tc(ObObjType type) { return ob_is_string_type(type) ||
                                                             ob_is_raw(type) ||
                                                             ob_is_enumset_tc(type) ||
                                                             ob_is_rowid_tc(type) ||
                                                             ob_is_lob_tc(type) ||
                                                             ob_is_json_tc(type) ||
                                                             ob_is_geometry_tc(type) ||
                                                             ob_is_roaringbitmap_tc(type); }
inline bool ob_is_string_or_enumset_tc(ObObjType type) { return ObStringTC == ob_obj_type_class(type) || ob_is_enumset_tc(type); }
inline bool ob_is_large_text(ObObjType type) { return ObTextType <= type && ObLongTextType >= type; }
inline bool ob_is_datetime(const ObObjType type) { return ObDateTimeType == type; }
inline bool ob_is_timestamp_tz(const ObObjType type) { return ObTimestampTZType == type; }
inline bool ob_is_timestamp_ltz(const ObObjType type) { return ObTimestampLTZType == type; }
inline bool ob_is_timestamp_nano(const ObObjType type) { return ObTimestampNanoType == type; }
inline bool ob_is_interval_ym(const ObObjType type) { return ObIntervalYMType == type; }
inline bool ob_is_interval_ds(const ObObjType type) { return ObIntervalDSType == type; }
inline bool ob_is_nvarchar2(const ObObjType type) { return ObNVarchar2Type == type; }
inline bool ob_is_nchar(const ObObjType type) { return ObNCharType == type; }
inline bool ob_is_nstring_type(const ObObjType type)
{
  return ob_is_nchar(type) || ob_is_nvarchar2(type);
}
inline bool ob_is_var_len_type(const ObObjType type) {
  return ob_is_string_type(type)
      || ob_is_raw(type)
      || ob_is_rowid_tc(type)
      || ob_is_lob_locator(type);
}
inline bool ob_is_collection_sql_type(const ObObjType type) { return ObCollectionSQLType == type; }
inline bool is_lob_storage(const ObObjType type) { return ob_is_large_text(type)
                                                          || ob_is_json_tc(type)
                                                          || ob_is_geometry_tc(type)
                                                          || ob_is_collection_sql_type(type)
                                                          || ob_is_roaringbitmap_tc(type); }
inline bool ob_is_geometry(const ObObjType type) { return ObGeometryType == type; }
inline bool ob_is_roaringbitmap(const ObObjType type) { return ObRoaringBitmapType == type; }

inline bool ob_is_decimal_int(const ObObjType type) { return ObDecimalIntType == type; }
inline bool is_decimal_int_accuracy_valid(const int16_t precision, const int16_t scale)
{
  return scale >= 0 && precision >= scale;
}

inline bool ob_is_user_defined_sql_type(const ObObjType type) { return ObUserDefinedSQLType == type; }
inline bool ob_is_user_defined_pl_type(const ObObjType type) { return ObExtendType == type; }
inline bool ob_is_user_defined_type(const ObObjType type) {
  return ob_is_user_defined_sql_type(type) || ob_is_user_defined_pl_type(type);
}
// xml type without schema
inline bool ob_is_xml_sql_type(const ObObjType type, const uint16_t sub_schema_id) {
  return (ObUserDefinedSQLType == type) && (sub_schema_id == ObXMLSqlType);
}

inline bool ob_is_xml_pl_type(const ObObjType type, const uint64_t udt_id) {
  return (ObExtendType == type) && (udt_id == static_cast<uint64_t>(T_OBJ_XML));
}

// to_string adapter
template<>
inline int databuff_print_obj(char *buf, const int64_t buf_len, int64_t &pos, const ObObjType &t)
{
  return databuff_printf(buf, buf_len, pos, "\"%s\"", inner_obj_type_str(t));
}
template<>
inline int databuff_print_key_obj(char *buf, const int64_t buf_len, int64_t &pos, const char *key,
                                  const bool with_comma, const ObObjType &t)
{
  return databuff_printf(buf, buf_len, pos, WITH_COMMA("%s:\"%s\""), key, inner_obj_type_str(t));
}

bool ob_can_static_cast(const ObObjType src, const ObObjType dst);
int find_type(const common::ObIArray<common::ObString> &type_infos, common::ObCollationType cs_type, const common::ObString &val, int32_t &pos);

enum ObOTimestampMetaAttrType
{
  OTMAT_TIMESTAMP_TZ = 0,
  OTMAT_TIMESTAMP_LTZ,
  OTMAT_TIMESTAMP_NANO
};

enum ObDecimalIntWideType
{
  DECIMAL_INT_32 = 0, // precision from 1 to 9
  DECIMAL_INT_64,     // precision from 1 to 18
  DECIMAL_INT_128,    // precision from 1 to 38
  DECIMAL_INT_256,    // precision from 1 to 76
  DECIMAL_INT_512,    // precision from 1 to 154
  DECIMAL_INT_MAX
};

ObDecimalIntWideType get_decimalint_type(const int16_t precision);
int16_t get_max_decimalint_precision(const int16_t precision);

}  // end namespace common
}  // end namespace oceanbase

#endif //OCEANBASE_COMMON_OB_OBJECT_TYPE_H_
