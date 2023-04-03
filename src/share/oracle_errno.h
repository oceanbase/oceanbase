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

#ifndef _ORACLE_ERRNO_H
#define _ORACLE_ERRNO_H 1

//ORA-00000 normal, successful completion
//Cause: An operation has completed normally, having met no exceptions.
//Action: No action required.
#define OER_SUCCESS 0

//ORA-00001: unique constraint (string.string) violated
//Cause: An UPDATE or INSERT statement attempted to insert a duplicate key. For Trusted Oracle configured in DBMS MAC mode, you may see this message if a duplicate entry exists at a different level.
//Action: Either remove the unique restriction or do not insert the key.
#define OER_UNIQUE_CONSTRAINT_VIOLATED   1

//ORA-00036: maximum number of recursive SQL levels (string) exceeded
//Cause: An attempt was made to go more than the specified number of recursive SQL levels.
//Action: Remove the recursive SQL, possibly a recursive trigger.
#define OER_RECURSIVE_SQL_LEVELS_EXCEEDED 36

//ORA-00054: resource busy and acquire with NOWAIT specified or timeout expired
//Cause: Interested resource is busy.
//Action: Retry if necessary or increase timeout.
#define OER_RESOURCE_BUSY_AND_ACQUIRE_WITH_NOWAIT  54

//ORA-00060: deadlock detected while waiting for resource
//Cause: Transactions deadlocked one another while waiting for resources.
//Action: Look at the trace file to see the transactions and resources involved. Retry if necessary.
#define OER_DEADLOCK_DETECTED_WHILE_WAITING    60

//ORA-00600: internal error code, arguments: [string], [string], [string], [string], [string],
//           [string], [string], [string], [string], [string], [string], [string]
/* Cause: This is the generic internal error number for Oracle program exceptions.
 * It indicates that a process has encountered a low-level, unexpected condition.
 * The first argument is the internal message number. This argument and the database version
 * number are critical in identifying the root cause and the potential impact to your system.
 * */
/* Action: Visit My Oracle Support to access the ORA-00600 Lookup tool (reference Note 600.1)
 * for more information regarding the specific ORA-00600 error encountered. An Incident has been
 * created for this error in the Automatic Diagnostic Repository (ADR). When logging a service
 * request, use the Incident Packaging Service (IPS) from the Support Workbench or the ADR Command
 * Interpreter (ADRCI) to automatically package the relevant trace information (reference My
 * Oracle Support Note 411.1). The following information should also be gathered to help determine
 * the root cause: - changes leading up to the error - events or unusual circumstances leading up
 * to the error - operations attempted prior to the error - conditions of the operating system and
 * databases at the time of the error Note: The cause of this message may manifest itself as
 * different errors at different times. Be aware of the history of errors that occurred
 * before this internal error.
 * */
#define OER_INTERNAL_ERROR_CODE    600

/*
* ORA-00603 ORACLE server session terminated by fatal error
* Cause: An Oracle Server session is in an unrecoverable state.
*
* Action: Log in to Oracle again so a new server session will be created automatically. Examine the session trace file for more information.
*/
#define OER_SESSION_TERMINATED_ERROR_CODE 603

//ORA-00608: testing error [string] [string] [string] [string] [string]
//Cause: Internal error reserved for testing.
//Action: call Oracle Support
#define OER_INTERNAL_ERROR_RESERVED_FOR_TESTING  608

//ORA-00900: invalid SQL statement
#define OER_INVALID_SQL_STATEMENT   900

//ORA-00903: invalid table name
#define OER_INVALID_TABLE_NAME   903

//ORA-00904: string: invalid identifier
#define OER_INVALID_IDENTIFIER   904

//ORA-00909: invalid number of arguments
#define OER_INVALID_ARGUMENTS_NUMBER   909

//ORA-00910: specified length too long for its datatype
//Cause: for datatypes CHAR and RAW, the length specified was > 2000; otherwise, the length specified was > 4000.
//Action: use a shorter length or switch to a datatype permitting a longer length such as a VARCHAR2, LONG CHAR, or LONG RAW
#define OER_TOO_LONG_DATATYPE_LENGTH   910

//ORA-00913: too many values
#define OER_TOO_MANY_VALUES 913

//ORA-00917: missing comma
#define OER_MISSING_COMMA   917

//ORA-00918:column ambiguously defined
#define OER_COLUMN_AMBIGUOUS 918

//ORA-00926: missing VALUES keyword
#define OER_MISSING_VALUES 926

//ORA-00932: inconsistent datatypes: expected %s UNIT got %s
#define OER_INCONSISTENT_DATATYPES 932

//ORA-00933: SQL command not properly ended
#define OER_CMD_NOT_PROPERLY_ENDED 933

//ORA-00934: group function is not allowed here
#define OER_INVALID_GROUP_FUNC_USE 934

//ORA-00936: missing expression
#define OER_MISSING_EXPRESSION   936

//ORA-00938: not enough arguments for function
#define OER_NOT_ENOUGH_ARGS_FOR_FUN 938

//ORA-00939: too many arguments for function
#define OER_TOO_MANY_ARGS_FOR_FUN 939

//ORA-00942: table or view does not exist
#define OER_TABLE_OR_VIEW_NOT_EXIST   942

//ORA-00947: not enough values
#define OER_NOT_ENOUGH_VALUES 947

//ORA-00955: name is already used by an existing object
#define OER_DUPLICATE_OBJECT_NAME 955

//ORA-00957: duplicate column name
#define OER_DUPLICATE_COLUMN_NAME 957

//ORA-00959: tablespace 'string' does not exist
#define OER_TABLESPACE_NOT_EXIST 959

//ORA-00969: missing ON keyword
#define OER_MISSING_ON_KEYWORD 969

//ORA-00979: not a GROUP BY expression
#define OER_WRONG_FIELD_WITH_GROUP 979

// ORA-00980: synonym translation is no longer valid
// Cause: A synonym did not translate to a legal target object. This could happen for one of the following reasons:
// 1. The target schema does not exist.
// 2. The target object does not exist.
// 3. The synonym specifies an incorrect database link.
// 4. The synonym is not versioned but specifies a versioned target object.
// Action: Change the synonym definition so that the synonym points at a legal target object.
#define OER_SYNONYM_TRANSLATION_INVALID 980

// ORA-00998: must name this expression with a column alias
// Cause:
// An expression or function was used in a CREATE VIEW statement, but no corresponding column name was specified.
//
// Solution:
// Enter a column name for each column in the view in parentheses after the view name.
#define OER_NO_COLUMN_ALIAS 998

//ORA-01031: insufficient privileges
//Cause: An attempt was made to perform a database operation without the necessary privileges.
//Action: Ask your database administrator or designated security administrator to grant you the necessary privileges
#define OER_INSUFFICIENT_PRIVILEGES			1031

//ORA-01045: user string lacks CREATE SESSION privilege; logon denied
//Cause: A connect was attempted to a userid which does not have create session privilege.
//Action: Grant the user CREATE SESSION privilege.
#define OER_LACKS_CREATE_SESSION_PRIVILEGE			1045

//ORA-01086
#define OER_SAVE_POINT_NOT_EXIST 1086

//ORA-01092: ORACLE instance terminated. Disconnection forced
//Cause: The instance this process was connected to was terminated abnormally, probably via a shutdown abort. This process was forced to disconnect from the instance.
//Action: Examine the alert log for more details. When the instance has been restarted, retry action.
#define OER_DISCONNECTION_FORCED  1092

//ORA-01400: cannot insert NULL into (string)
//Cause: An attempt was made to insert NULL into previously listed objects.
//Action: These objects cannot accept NULL values.
#define OER_CANNOT_INSERT_NULL   1400

// ORA-01408: such column list already indexed
#define OER_COLUMN_LIST_ALREADY_INDEXED 1408

// ORA-01410: invalid ROWID
#define OER_INVALID_ROWID 1410

// ORA-01416: 两表无法彼此外部连接
// 01416. 00000 -  "two tables cannot be outer-joined to each other"
#define OER_OUTER_JOIN_NESTED 1416

// ORA-01418: specified index does not exist
#define OER_INDEX_NOT_EXISTS 1418

//ORA-01426: numeric overflow
#define OER_NUMERIC_OVERFLOW 1426

//ORA-01427: single-row subquery returns more than one row
#define OER_MORE_THAN_ONE_ROW 1427

//ORA-01428: argument '%s' is out of range
#define OER_ARGUMENT_OUT_OF_RANGE 1428

//ORA-01422 exact fetch returns more than requested number of rows
//Cause: The number specified in exact fetch is less than the rows returned.
//Action: Rewrite the query or change number of rows requested.
#define OER_EXACT_FETCH_RETURN_MORE_ROWS  1422

//ORA-01424: missing or illegal character following the escape character
//Cause: Only support '_' and '%' following escape character in the pattern string of like expression
//Action: Correct the pattern of like expression.
#define OER_INVALID_CHAR_FOLLOWING_ESCAPE_CHAR 1424

//ORA-01425: escape character must be character string of length 1
//Cause: escape character must be length 1
//Action: Correct the escape character of like expression.
#define OER_INVALID_ESCAPE_CHAR_LENGTH 1425

//ORA-01442: column to be modified to NOT NULL is already NOT NULL
#define OER_COLUMN_CANT_CHANGE_TO_NOT_NULL 1442

//ORA-01450: maximum key length (string) exceeded
#define OER_KEY_LENGTH_TOO_LONG 1450

// ORA-01451: column to be modified to NULL cannot be modified to NULL
#define OER_COLUMN_CANT_CHANGE_TO_NULLALE 1451

//ORA-01453: SET TRANSACTION must be first statement of transaction
//Cause: self-evident
//Action: commit (or rollback) transaction, and re-execute
#define OER_CANT_CHANGE_TX_CHARACTERISTICS 1453

//ORA-01465: invalid hex number
#define OER_INVALID_HEX_NUMBER   1465

// ORA-01468: 一个谓词只能引用一个外部联接的表
// 01468. 00000 -  "a predicate may reference only one outer-joined table"
#define OER_MULTI_OUTER_JOIN_TABLE 1468

//ORA-01476 divisor is equal to zero
//Cause: An expression attempted to divide by zero.
//Action: Correct the expression, then retry the operation.
#define OER_DIVISOR_IS_ZERO 1476

//ORA-01481 invalid number format model
//Cause: An invalid format parameter was used with the TO_CHAR or TO_NUMBER function.
//Action: Correct the syntax, then retry the operation.
#define OER_INVALID_NUMBER_FORMAT_MODEL 1481

//ORA-01489: result of string concatenation is too long
//Cause: String concatenation result is more than the maximum size.
//Action: Make sure that the result is less than the maximum size.
#define OER_TOO_LONG_STRING_IN_CONCAT 1489

//ORA-01543: tablespace 'string' already exists
//Cause: Tried to create a tablespace which already exists
//Action: Use a different name for the new tablespace
#define OER_TABLESPACE_EXIST 1543

//ORA-01555: snapshot too old: rollback segment number string with name "string" too small
//Cause: rollback records needed by a reader for consistent read are overwritten by other writers
//Action: If in Automatic Undo Management mode, increase undo_retention setting. Otherwise, use larger rollback segments
#define OER_SNAPSHOT_TOO_OLD 1555

// ORA-01705: 无法在关联列中指定外部联接
// 01705. 00000 -  "an outer join cannot be specified on a correlation column"
#define OER_OUTER_JOIN_ON_CORRELATION_COLUMN 1705

// ORA-01711: duplicate privilege listed
#define OER_PRIV_DUP 1711

//ORA-01718: BY ACCESS | SESSION clause not allowed for NOAUDIT
//Cause: Attempt to specify BY ACCESS | SESSION in a NOAUDIT statement.
//Action: Remove BY ACCESS | SESSION.
#define OER_BY_ACCESS_OR_SESSION_CLAUSE_NOT_ALLOWED_FOR_NOAUDIT      1718

// ORA-01719: OR 或 IN 操作数中不允许外部联接运算符 (+)
// 01719. 00000 -  "outer join operator (+) not allowed in operand of OR or IN"
// *Cause:    An outer join appears in an or clause.
// *Action:   If A and B are predicates, to get the effect of (A(+) or B),
//            try (select where (A(+) and not B)) union all (select where (B)).
#define OER_OUTER_JOIN_AMBIGUOUS 1719

//ORA-01720: grant option does not exist for 'string.string'
//Cause: A grant was being performed on a view or a view was being replaced and the grant option was not present for an underlying object.
//Action: Obtain the grant option on all underlying objects of the view or revoke existing grants on the view.
#define OER_GRANT_OPTION_DOES_NOT_EXIST_FOR			1720

//ORA-01722: invalid number
//Cause: The specified number was invalid.
//Action: Specify a valid number.
#define OER_INVALID_NUMBER   1722

//ORA-01723 zero-length columns are not allowed
//Cause: During CREATE TABLE, a zero-length column was specified, for example, CHAR(0).
//Action: Correct the column declaration so that the length is at least 1 and try the operation again.
#define OER_ZERO_LEN_COL 1723

//ORA-01724: floating point precision is out of range (1 to 126)
//Cause: The specified number had an invalid range.
//Action: Use a floating point precision that is in the correct precision.
#define OER_FLOAT_PRECISION_OUT_RANGE 1724

//ORA-01727: numeric precision specifier is out of range (1 to 38)
#define OER_NUMERIC_PRECISION_OUT_RANGE 1727

//ORA-01728: numeric scale specifier is out of range (-84 to 127)
#define OER_NUMERIC_SCALE_OUT_RANGE 1728

// ORA-01732: data manipulation operation not legal on this view
#define OER_ILLEGAL_VIEW_UPDATE 1732
// ORA-01733: virtual column not allowed here
#define OER_VIRTUAL_COL_NOT_ALLOWED 1733


//ORA-01744 inappropriate INTO
//Cause: The INTO clause may not be used in a subquery.
//Action: Check the syntax, place the INTO clause in the top-level query, and retry the statement.
#define OER_INAPPROPRIATE_INTO	1744

// ORA-01752: cannot delete from view without exactly one key-preserved table
// Cause: The deleted table had - no key-preserved tables, - more than one key-preserved table, or - the key-preserved table was an unmerged view.
// Action: Redefine the view or delete it from the underlying base tables.
#define OER_DELETE_VIEW_NON_KEY_PRESERVED 1752

//ORA-01760: illegal argument for function
#define OER_ILLEGAL_ARGUMENT_FOR_FUNCTION      1760

// ORA-01765: specifying owner's name of the table is not allowed
// Cause: in some case (e.g. RENAME t1 to t2), owner name cannot be specified.
// Action: remove specified owner name
#define OER_OWNER_NAME_NOT_ALLOWED 1765

//ORA-01767: UPDATE ... SET expression must be a subquery
#define OER_NOT_SUBQUERY 1767

// ORA-01776: cannot modify more than one base table through a join view
// Cause: Columns belonging to more than one underlying table were either inserted into or updated.
// Action: Phrase the statement as two or more separate statements.
#define OER_VIEW_MULTIUPDATE 1776

// ORA-01779: cannot modify a column which maps to a non key-preserved table
// Cause: An attempt was made to insert or update columns of a join view which map to a non-key-preserved table.
// Action: Modify the underlying base tables directly.
#define OER_UPDATE_VIEW_NON_KEY_PRESERVED 1779

//ORA-01785: ORDER BY item must be the number of a SELECT-list expression
// *Cause:    the literal order by item must be the number of select list expression
// *Action:   specify the right number of order by
#define OER_ORDER_BY_ITEM_NOT_IN_SELECT_LIST 1785

//ORA-01786: FOR UPDATE of this query expression is not allowed
#define OER_FOR_UPDATE_EXPR_NOT_ALLOWED 1786

//ORA-01790: expression must have same datatype as corresponding expression
#define OER_EXPRESSION_MUST_HAVE_SAME_DATATYPE_AS_CORRESPONDING_EXPRESSION      1790

//ORA-01791: not a SELECTed expression
//Cause: Order by expr is not refer select item expr
//Action: Check the order by expr and use the select item expr
#define OER_NOT_SELECTED_EXPR 1791

//ORA-01796: this operator cannot be used with lists
#define OPERATOR_CANNOT_BE_USED_WITH_LISTS 1796

// ORA-01799: 列不能外部联接到子查询
// 01799. 00000 -  "a column may not be outer-joined to a subquery"
// *Cause:    <expression>(+) <relop> (<subquery>) is not allowed.
// *Action:   Either remove the (+) or make a view out of the subquery.
//            In V6 and before, the (+) was just ignored in this case.
#define OER_OUTER_JOIN_WITH_SUBQUERY 1799

//ORA-01801: date format is too long for internal buffer
#define OER_DATE_FORMAT_IS_TOO_LONG_FOR_INTERNAL_BUFFER      1801

//ORA-01810 format code appears twice
//Cause: A format code was listed twice in a date specification. Each format code may be specified only once in the function TO_DATE.
//Action: Remove the duplicate format code from the date specification, then retry the operation.
#define OER_FORMAT_CODE_APPEARS_TWICE                             1810

//ORA-01811 Julian date precludes use of day of year
//Cause: Both a Julian date and a day of the year were specified in the TO_DATE function. If a Julian date is specified, the day of the year (DDD) may not be specified, as it is contained in the Julian date.
//Action: Remove the day of the year or the Julian date from the specification, then retry the operation.
#define OER_JULIAN_DATE_PRECLUDES_USE_OF_DAY_OF_YEAR              1811

//ORA-01812 year may only be specified once
//Cause: More than one year format code was listed in a date specification. Only one of the following year format codes may be specified in a date: YYYY, YYY, YY, Y.
//Action: Remove all but one year format from the date specification.
#define OER_YEAR_MAY_ONLY_BE_SPECIFIED_ONCE                       1812

//ORA-01813 hour may only be specified once
//Cause: More than one hour format code was listed in a date specification. Only one of the following hour format codes may be specified in a date: HH, HH12, HH24.
//Action: Remove all but one hour format from the date specification.
#define OER_HOUR_MAY_ONLY_BE_SPECIFIED_ONCE                       1813

//ORA-01814 AM/PM conflicts with use of A.M./P.M.
//Cause: Both types of meridian indicators, AM and PM, were listed in a date specification. If one of the meridian indicator format masks, such as AM or A.M., is specified in the date, the other may not be specified.
//Action: Use one meridian indicator format, with or without periods, consistently.
#define OER_AM_PM_CONFLICTS_WITH_USE_OF_AM_DOT_PM_DOT             1814

//ORA-01815 BC/AD conflicts with use of B.C./A.D.
//Cause: Both types of BC/AD indicators were listed in a date specification. If one of the BC/AD indicator format masks, such as BC or B.C., is specified in the date, the other may not be specified.
//Action: Use one BC/AD indicator format, with or without periods, consistently.
#define OER_BC_AD_CONFLICT_WITH_USE_OF_BC_DOT_AD_DOT              1815

//ORA-01816 month may only be specified once
//Cause: More than one month format code was listed in a date specification. Only one of the following month format codes may be specified in a date: MM, MON, MONTH.
//Action: Remove all but one month format from the date specification.
#define OER_MONTH_MAY_ONLY_BE_SPECIFIED_ONCE                      1816

//ORA-01817 day of week may only be specified once
//Cause: More than one day-of-the-week format code was listed in a date specification. Only one of the following day-of-the-week format codes may be specified in a date: D, DY, DAY.
//Action: Remove all but one day-of-the-week format from the date specification.
#define OER_DAY_OF_WEEK_MAY_ONLY_BE_SPECIFIED_ONCE                1817

//ORA-01818 'HH24' precludes use of meridian indicator
//Cause: A date specification contained both a 24-hour time format code and a meridian indicator code. If hours are specified using the 24-hour time format (HH24), a meridian indicator, AM or PM, may not be specified.
//Action: Remove the meridian indicator format code, AM/PM or A.M./P.M., or the 24-hour time format code, HH24, from the date specification.
#define OER_INVALID_MERIDIAN_INDICATOR_USE                        1818

//ORA-01819 signed year precludes use of BC/AD
//Cause: A date specification contained both a signed year and a B.C./A.D. indicator. If the year is specified with a sign, such as SYYYY, then B.C. or A.D. is implicit in the date and must not be entered.
//Action: Remove the B.C./A.D. indicator from the date specification.
#define OER_SIGNED_YEAR_PRECLUDES_USE_OF_BC_AD                    1819

//ORA-01820 format code cannot appear in date input format
//Cause: A date specification contained an invalid format code. Only the following may be specified when entering a date: year, month, day, hours, minutes, seconds, Julian day, A.M./P.M. and B.C./A.D.
//Action: Remove the invalid format code from the date specification.
#define OER_FORMAT_CODE_CANNOT_APPEAR                             1820

//ORA-01821: date format not recognized
#define OER_DATE_FORMAT_NOT_RECOGNIZED                            1821

//ORA-01830: date format picture ends before converting entire input string
#define OER_INVALID_DATE_FORMAT_END                               1830

//ORA-01831 year conflicts with Julian date
//Cause: The wrong year was specified with a Julian day. If a year is specified with a Julian date, it must be the year in which the Julian date occurs.
//Action: Remove the year value from the date specification or enter the correct year for the Julian date.
#define OER_YEAR_CONFLICTS_WITH_JULIAN_DATE                       1831

//ORA-01832 day of year conflicts with Julian date
//Cause: A Julian date was specified with the day of the year but the day did not correspond to the Julian date. If the day of the year is specified with a Julian date, it must be the same day as the Julian date.
//Action: Remove the day of the year value from the date specification or enter the correct day for the Julian date.
#define OER_DAY_OF_YEAR_CONFLICTS_WITH_JULIAN_DATE                1832

//ORA-01833 month conflicts with Julian date
//Cause: The wrong month was specified with a Julian date. If a month is specified with a Julian date, it must be the month in which the Julian date occurs.
//Action: Remove the month value from the date specification or enter the correct month for the Julian date.
#define OER_MONTH_CONFLICTS_WITH_JULIAN_DATE                      1833

//ORA-01834 day of month conflicts with Julian date
//Cause: A Julian date was specified with the day of the month, but the month day did not correspond to the Julian date. If the day of the month is specified with a Julian date, it must be the same day of the month as the Julian date.
//Action: Remove the day of the month value from the date specification or enter the correct day of the month for the Julian date.
#define OER_DAY_OF_MONTH_CONFLICTS_WITH_JULIAN_DATE               1834

//ORA-01835 day of week conflicts with Julian date
//Cause: A Julian date was specified with the day of the week, but the weekday did not correspond to the Julian date. If the day of the week is specified with a Julian date, it must be the same day of the week as the Julian date.
//Action: Remove the day of the week value from the date specification or enter the correct day of the week for the Julian date.
#define OER_DAY_OF_WEEK_CONFLICTS_WITH_JULIAN_DATE                1835

//ORA-01836 hour conflicts with seconds in day
//Cause: The wrong hour was specified with seconds in the day. If an hour is specified with seconds past midnight (SSSSS), it must be the hour in which the seconds value falls.
//Action: Remove the hour value from the date specification or specify the correct hour for the seconds past midnight.
#define OER_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY                    1836

//ORA-01837 minutes of hour conflicts with seconds in day
//Cause: A date specification contained both minutes of the hour and seconds in the day but the values did not correspond. If both minutes in the hour (MI) and seconds past midnight (SSSSS) are specified, the minutes value must be the minute in which the seconds value will occur.
//Action: Remove the minutes value from the date specification or enter the correct minute value for the specified seconds value.
#define OER_MINUTES_OF_HOUR_CONFLICTS_WITH_SECONDS_IN_DAY         1837

//ORA-01838 seconds of minute conflicts with seconds in day
//Cause: A date specification contained both seconds of the minute and seconds in the day but the values did not correspond. If both types of seconds are specified, the seconds of the minute value (SS) must be the second in which the seconds past midnight value (SSSSS) will fall.
//Action: Remove the seconds of the minute value from the date specification or enter a value that corresponds to the given seconds in the day.
#define OER_SECONDS_OF_MINUTE_CONFLICTS_WITH_SECONDS_IN_DAY       1838

//ORA-01839 date not valid for month specified
//Cause: The day of the month specified in the date is invalid for the given month. The day of the month (DD) must be between 1 and the number of days in the month.
//Action: Enter a valid day of the month for the specified month.
#define OER_DATE_NOT_VALID_FOR_MONTH_SPECIFIED                    1839

//ORA-01840 input value not long enough for date format
//Cause: The data to be converted to date format was incomplete; the date format picture was longer than the input data.
//Action: Either add more input or shorten the date picture format, then retry the operation.
#define OER_INPUT_VALUE_NOT_LONG_ENOUGH                           1840

//ORA-01841 (full) year must be between -4713 and +9999, and not be 0
//Cause: A date specified a year that is not in the valid date range. A valid date is any date between January 1, 4712 B.C. and December 31, 9999 A.D.
//Action: Enter a valid date value between 4712 B.C. and 9999 A.D.
#define OER_INVALID_YEAR_VALUE                                    1841

//ORA-01842 quarter must be between 1 and 4
//Cause: An invalid value was specified for the quarter of the year in a date. The quarter (Q) must be between 1 and 4.
//Action: Enter a value for quarter between 1 and 4.
#define OER_INVALID_QUARTER_VALUE                                 1842

//ORA-01843 not a valid month
//Cause: A date specified an invalid month. Valid months are: January-December, for format code MONTH, and Jan-Dec, for format code MON.
//Action: Enter a valid month value in the correct format.
#define OER_INVALID_MONTH_NAME                                    1843

//ORA-01846 not a valid day of the week
//Cause: A date specified an invalid day of the week. Valid days are:
//  Monday-Sunday, for format code DAY
//  Mon-Sun, for format code DY
//  1-7, for format code D
//Action: Enter a valid day of the week value in the correct format.
#define OER_INVALID_DAY_OF_THE_WEEK                               1846

//ORA-01847 day of month must be between 1 and last day of month
//Cause: The day of the month listed in a date is invalid for the specified month. The day of the month (DD) must be between 1 and the number of days in that month.
//Action: Enter a valid day value for the specified month.
#define OER_DAY_OF_MONTH_SHOULD_BETWEEN_ONE_AND_LAST_DAY_OF_MONTH 1847

//ORA-01848 day of year must be between 1 and 365 (366 for leap year)
//Cause: An invalid day of the year was specified in a date. Day of the year (DDD) must be between 1 and 365 for a non-leap year or 1 and 366 for a leap year.
//Action: Enter a day of the year value between 1 and 365 (or 366).
#define OER_INVALID_DAY_OF_YEAR_VALUE                             1848

//ORA-01849 hour must be between 1 and 12
//Cause: An invalid hour was specified for a date using the 12-hour time format. If a 12-hour format code (HH or HH12) is used, the specified hour must be between 1 and 12.
//Action: Enter an hour value between 1 and 12.
#define OER_INVALID_HOUR12_VALUE                                  1849

//ORA-01850 hour must be between 0 and 23
//Cause: An invalid hour was specified for a date using the 24-hour time format. If the 24-hour format code (HH24) is listed, the specified hour must be between 0 and 23.
//Action: Enter an hour value between 0 and 23.
#define OER_INVALID_HOUR24_VALUE                                  1850

//ORA-01851 minutes must be between 0 and 59
//Cause: An invalid minute value was specified in a date. Minutes must be between 0 and 59.
//Action: Enter a minute value between 0 and 59.
#define OER_INVALID_MINUTES_VALUE                                 1851

//ORA-01852 seconds must be between 0 and 59
//Cause: An invalid seconds value was specified in a date. Seconds must be between 0 and 59 if the seconds format code (SS) is used.
//Action: Enter a seconds value between 0 and 59.
#define OER_INVALID_SECONDS_VALUE                                 1852

//ORA-01853 seconds in day must be between 0 and 86399
//Cause: An invalid value for seconds in the day was specified in a date. Seconds must be between 0 and 86399 if the seconds past midnight format code (SSSSS) is used.
//Action: Specify a seconds value between 0 and 86399.
#define OER_INVALID_SECONDS_IN_DAY_VALUE                          1853

//ORA-01854 julian date must be between 1 and 5373484
//Cause: An invalid Julian date was entered.
//Action: Enter a valid Julian date between 1 and 5373484.
#define OER_INVALID_JULIAN_DATE_VALUE                             1854

//ORA-01855 AM/A.M. or PM/P.M. required
//Cause: A date specification contained a meridian indicator format code but a valid meridian indicator was not specified. If a meridian indicator code, A.M./P.M. or AM/PM, is included in a date format, the date must include a valid meridian indicator.
//Action: Specify the meridian indicator in the correct format, A.M./AM or P.M./PM.
#define OER_AM_OR_PM_REQUIRED                                     1855

//ORA-01856 BC/B.C. or AD/A.D. required
//Cause: A date specification contained a BC/AD format code but a valid BC/AD indicator was not specified. If one of the BC/AD format codes, BC/AD or B.C./A.D., is specified in a date format, the date must include BC/B.C. or AD/A.D.
//Action: Specify the BC/AD indicator in the date using the correct format.
#define OER_BC_OR_AD_REQUIRED                                     1856

//ORA-01857: not a valid time zone
#define OER_NOT_A_VALID_TIME_ZONE      1857

//ORA-01858 a non-numeric character was found where a numeric was expected
//Cause: The input data to be converted using a date format model was incorrect. The input data did not contain a number where a number was required by the format model.
//Action: Fix the input data or the date format model to make sure the elements match in number and type. Then retry the operation.
#define OER_NON_NUMERIC_CHARACTER_VALUE                           1858

//ORA-01861: literal does not match format string
//Cause: Literals in the input must be the same length as literals in the
//       format string (with the exception of leading whitespace). If the "FX"
//       modifier has been toggled on, the literal must match exactly, with no extra whitespace.
//Action: Correct the format string to match the literal.
#define OER_LITERAL_NOT_MATCH_FORMAT_STRING  1861

//ORA-01867: the interval is invalid
//Cause: The character string you specified is not a valid interval.
//Action: Please specify a valid interval.
#define OER_INTERVAL_INVALID  1867

//ORA-01873: the leading precision of the interval is too small
//Cause: The leading precision of the interval is too small to store the specified interval.
//Action: Increase the leading precision of the interval or specify an interval with a smaller leading precision.
#define OER_THE_LEADING_PRECISION_OF_THE_INTERVAL_IS_TOO_SMALL      1873

//ORA-01874: time zone hour must be between -12 and 14
//Cause: The time zone hour specified was not in the valid range.
//Action: Specify a time zone hour between -12 and 14.
#define OER_INVALID_TIME_ZONE_HOUR      1874

//ORA-01875: time zone minute must be between -59 and 59
//Cause: The time zone minute specified was not in the valid range.
//Action: Specify a time zone minute between -59 and 59.
#define OER_INVALID_TIME_ZONE_MINUTE      1875

//ORA-01878: specified field not found in datetime or interval
//Cause: The specified field was not found in the datetime or interval.
//Action: Make sure that the specified field is in the datetime or interval.
#define OER_FIELD_NOT_FOUND_IN_DATETIME_OR_INTERVAL  1878

//ORA-01881: timezone region ID is invalid
//Cause: The region ID referenced an invalid region.
//Action: Contact Oracle Support Services.
#define OER_INVALID_TIMEZONE_REGION_ID  1881

//ORA-01882 timezone region string not found
//Cause: The specified region name was not found.
//Action: Please contact Oracle Support Services.
#define OER_TIMEZONE_REGION_STRING_NOT_FOUND           1882

//ORA-01917: user or role 'string' does not exist
//Cause: There is not a user or role by that name.
//Action: Re-specify the name.
#define OER_USER_OR_ROLE_DOES_NOT_EXIST			1917

//ORA-01918: user 'string' does not exist
//Cause: User does not exist in the system.
//Action: Verify the user name is correct.
#define OER_USER_DOES_NOT_EXIST			1918

//ORA-01917: user or role 'string' does not exist
//ORA-01919: role 'string' does not exist
//ORA-01921: role name 'string' conflicts with another user or role name
#define OER_ROLE_NOT_EXIST  1919

//ORA-01920: user name 'string' conflicts with another user or role name
//Cause: There is already a user or role with that name.
//Action: Specify a different user name.
#define OER_USER_EXISTS  1920

#define OER_ROLE_EXIST  1921

//ORA-01927: cannot REVOKE privileges you did not grant
//Cause: You can only revoke privileges you granted.
//Action: Don't revoke these privileges.
#define OER_CANNOT_REVOKE_PRIVILEGES_YOU_DID_NOT_GRANT			1927

//ORA-01930: auditing the object is not supported
//Cause: AUDIT or NOAUDIT was specified for an object that cannot be audited.
//Action: Don't attempt to AUDIT the object.
#define OER_AUDITING_THE_OBJECT_IS_NOT_SUPPORTED      1930

//ORA-01932: ADMIN option not granted for role 'string'
//Cause: The operation requires the admin option on the role.
//Action: Obtain the grant option and re-try.
#define OER_ADMIN_OPTION_NOT_GRANTED_FOR_ROLE			1932

//ORA-01934: circular role grant detected
//Cause: Roles cannot be granted circularly. Also, a role cannot be granted to itself.
//Action: Do not perform the grant.
#define OER_CIRCULAR_ROLE_GRANT_DETECTED			1934

//ORA-01952: system privileges not granted to 'string'
//Cause: A system privilege you tried to revoke was not granted to the user.
//Action: Make sure the privileges you are trying to revoke are granted.
#define OER_SYSTEM_PRIVILEGES_NOT_GRANTED_TO			1952

//ORA-01982: invalid auditing option for tables
//Cause: AUDIT or NOAUDIT on a table specifies an auditing option that is not legal for tables.
//Action: The following options may not be used for tables and should be removed: REFERENCES, EXECUTE
#define OER_INVALID_AUDITING_OPTION_FOR_TABLES      1982

//ORA-02014: cannot select FOR UPDATE from view with DISTINCT, GROUP BY, etc.
//Cause: if the generate table deal with data, FOR UPDATE can't lock the Temporary data
//Action: make sure you use select FOR UPDATE in generate table correctly
#define OER_FOR_UPDATE_SELECT_VIEW_CANNOT  2014

//ORA-02019 connection description for remote database not found
//Cause: An attempt was made to connect or log in to a remote database using a connection
//       description that could not be found.
//Action: Specify an existing database link. Query the data dictionary to see all existing
//        database links. See your operating system-specific Net8 documentation for valid
//        connection descriptors.
#define OER_DBLINK_NOT_EXIST_TO_ACCESS 2019

//ORA-02024: database link not found
//Cause: Database link to be dropped is not found in dictionary
//Action: Correct the database link name
#define OER_DBLINK_NOT_EXIST_TO_DROP 2024

//ORA-02049: timeout: distributed transaction waiting for lock
//Cause: "The number of seconds specified in the distributed_lock_timeout initialization parameter "
//       "were exceeded while waiting for a lock or for a begin transaction hash collision to end."
//Action: Treat this timeout as a deadlock. Roll back the transaction and try again.
#define OER_TIMEOUT_BY_DISTRIBUTED_TRANS_LOCK_WAITING  2049

// ORA-02149: Specified partition does not exist
// Cause: Partition not found for the object.
// Action: Retry with correct partition name.
#define OER_ERR_PARTITION_NOT_EXIST  2149

//ORA-02204: ALTER, INDEX and EXECUTE not allowed for views
//Cause: An attempt was made to grant or revoke an invalid privilege on a view.
//Action: Do not attempt to grant or revoke any of ALTER, INDEX, or EXECUTE privileges on views.
#define OER_ALTER_INDEX_AND_EXECUTE_NOT_ALLOWED_FOR_VIEWS			2204

//ORA-02205: only SELECT and ALTER privileges are valid for sequences
//Cause: An attempt was made to grant or revoke an invalid privilege on a sequence.
//Action: Do not attempt to grant or revoke DELETE, INDEX, INSERT, UPDATE, REFERENCES or EXECUTE privilege on sequences.
#define OER_ONLY_SELECT_AND_ALTER_PRIVILEGES_ARE_VALID_FOR_SEQUENCES			2205

//ORA-02207: invalid INITRANS option value
//Cause: The INITRANS value is not an integer between 1 and 255 and less than or equal to the MAXTRANS value.
//Action: Choose a valid INITRANS value.
#define ORE_INVALID_INITRANS_VALUE 2207

//ORA-02209: invalid MAXTRANS option value
//Cause: The MAXTRANS value is not an integer between 1 and 255 and greater than or equal to the INITRANS value.
//Action: Choose a valid MAXTRANS value.
#define ORE_INVALID_MAXTRANS_VALUE 2209

//ORA-02211: invalid value for PCTFREE or PCTUSED
//Cause: The specified value for PCTFREE or PCTUSED is not an integer between 0 and 100.
//Action: Choose an appropriate value for the option.
#define ORE_INVALID_PCTFREE_OR_PCTUSED_VALUE 2211

//ORA-02224: EXECUTE privilege not allowed for tables
//Cause: An attempt was made to grant or revoke an invalid privilege on a table.
//Action: Do not attempt to grant or revoke EXECUTE privilege on tables.
#define OER_EXECUTE_PRIVILEGE_NOT_ALLOWED_FOR_TABLES			2224

//ORA-02225: only EXECUTE and DEBUG privileges are valid for procedures
//Cause: An attempt was made to grant or revoke an invalid privilege on a procedure, function or package.
//Action: Do not attempt to grant or revoke any privilege besides EXECUTE or DEBUG on procedures, functions or packages.
#define OER_ONLY_EXECUTE_AND_DEBUG_PRIVILEGES_ARE_VALID_FOR_PROCEDURES			2225

//ORA-02251: subquery not allowed here
//Cause: Subquery is not allowed here in the statement.
//Action: Remove the subquery from the statement.
#define ORE_INVALID_SUBQUERY_USE 2251

//ORA-02261: such unique or primary key already exists in the table
//Cause: Self-evident.
//Action: Remove the extra key.
#define OER_UK_PK_DUPLICATE  2261

//ORA-02264: name already used by an existing constraint
//Cause: The specified constraint name has to be unique.
//Action: Specify a unique constraint name for the constraint.
#define OER_CONSTRAINT_NAME_DUPLICATE  2264

//ORA-02290: check constraint (string.string) violated
//Cause: The values being inserted do not satisfy the named check constraint.
//Action: do not insert values that violate the constraint.
#define OER_CHECK_CONSTRAINT_VIOLATED  2290

//ORA-02291: integrity constraint (string.string) violated - parent key not found
//Cause: A foreign key value has no matching primary key value.
//Action: Delete the foreign key or add a matching primary key.
#define OER_PARENT_KEY_NOT_FOUND   2291

//ORA-02292: integrity constraint (string.string) violated - child record found
//Cause: attempted to delete a parent key value that had a foreign key dependency.
//Action: delete dependencies first then parent or disable constraint.
#define OER_CHILD_RECORD_FOUND   2292

//ORA-02293: cannot validate (string.string) - check constraint violated
//Cause: an alter table operation tried to validate a check constraint to a populated table that had nocomplying values.
//Action: Obvious
#define OER_ADD_CHECK_CONSTRAINT_VIOLATED   2293

//ORA-02296: cannot enable (string.string) - null values found
//Cause: an alter table enable constraint failed because the table contains values that do not satisfy the constraint.
//Action: Obvious
#define OER_ENABLE_NOT_NULL_CONSTRAINT_VIOLATED   2296

//ORA-02298: cannot validate (string.string) - parent keys not found
//Cause: an alter table validating constraint failed because the table has orphaned child records.
//Action: Obvious
#define OER_ORPHANED_CHILD_RECORD_EXISTS 2298

//ORA-02305: only EXECUTE, DEBUG, and UNDER privileges are valid for types
//Cause: An attempt was made to GRANT or REVOKE an invalid privilege (not EXECUTE, DEBUG, or UNDER) on a type.
//Action: GRANT or REVOKE only the EXECUTE, DEBUG, or UNDER privilege on types.
#define OER_ONLY_EXECUTE_DEBUG_AND_UNDER_PRIVILEGES_ARE_VALID_FOR_TYPES			2305

//ORA-02329: column of datatype string cannot be unique or a primary key
//Cause: An attempt was made to place a UNIQUE or a PRIMARY KEY constraint on a column of datatype VARRAY, nested table, object, LOB, FILE or REF.
//Action: Change the column datatype or remove the constraint. Then retry the operation.
#define OER_DATATYPE_CANNOT_BE_UNIQUE_PRIMARY_KEY  2329

//ORA-02377: invalid resource limit
//Cause: specifying limit of 0
//Action: specify limit > 0
#define OER_INVALID_RESOURCE_LIMIT      2377

//ORA-02379: profile string already exists
//Cause: Try to create a profile which already exist
//Action: n/a
#define OER_PROFILE_STRING_ALREADY_EXISTS      2379

//ORA-02380: profile string does not exist
//Cause: Try to assign a user to a non-existant profile
//Action: n/a
#define OER_PROFILE_STRING_DOES_NOT_EXIST      2380

//ORA-02382: profile string has users assigned, cannot drop without CASCADE
#define OER_PROFILE_STRING_HAS_USERS_ASSIGNED      2382

//ORA-02430: cannot enable constraint (string) - no such constraint
//Cause: the named constraint does not exist for this table.
//Action: Obvious
#define OER_ENABLE_NONEXISTENT_CONSTRAINT  2430

//ORA-02431: cannot disable constraint (string) - no such constraint
//Cause: the named constraint does not exist for this table.
//Action: Obvious
#define OER_DISABLE_NONEXISTENT_CONSTRAINT  2431

//ORA-02436: date or system variable wrongly specified in CHECK constraint
//Cause: An attempt was made to use a date constant or system variable, such as USER, in a check constraint that was not completely specified in a CREATE TABLE or ALTER TABLE statement. For example, a date was specified without the century.
//Action: Completely specify the date constant or system variable. Setting the event 10149 allows constraints like "a1 > '10-MAY-96'", which a bug permitted to be created before version 8.
#define OER_DATE_OR_SYS_VAR_CANNOT_IN_CHECK_CST  2436

//ORA-02438: Column check constraint cannot reference other columns
//Cause: attempted to define a column check constraint that references another column.
//Action: define it as a column check constriant.
#define OER_COL_CHECK_CST_REFER_ANOTHER_COL  2438

//ORA-02443: Cannot drop constraint - nonexistent constraint
//Cause: alter table drop constraint <constraint_name>
//Action: make sure you supply correct constraint name.
#define ORE_NONEXISTENT_CONSTRAINT 2443

//ORA-04036: PGA memory used by the instance exceeds PGA_AGGREGATE_LIMIT
//Cause: Private memory across the instance exceeded the limit specified in the PGA_AGGREGATE_LIMIT initialization parameter. The largest sessions using Program Global Area (PGA) memory were interrupted to get under the limit.
//Action: Increase the PGA_AGGREGATE_LIMIT initialization parameter or reduce memory usage.
#define OER_OUT_OF_PGA_AGGREGATE_LIMIT  4036

//ORA-04077: WHEN clause cannot be used with table level triggers
//Cause: The when clause can only be specified for row level triggers.
//Action: Remove the when clause or specify for each row.
#define OER_STMT_TRIGGER_WITH_WHEN_CLAUSE  4077

//ORA-04080: trigger 'string' does not exist
//Cause: The TRIGGER name is invalid.
//Action: Check the trigger name.
#define OER_TRIGGER_NOT_EXIST  4080

//ORA-04081: trigger 'string' already exists
//Cause: The TRIGGER name or type already exists.
//Action: Use a different trigger name or drop the trigger which is of the same name.
#define OER_TRIGGER_ALREADY_EXIST  4081

//ORA-04091 table string.string is mutating, trigger/function may not see it
//Cause: A trigger (or a user defined PL/SQL function that is referenced in this statement) attempted to look at (or modify) a table that was in the middle of being modified by the statement which fired it.
//Action: Rewrite the trigger (or function) so it does not read that table.
#define OER_MUTATING_TABLE_OPERATION 4091

//ORA-04095: trigger 'string' already exists on another table, cannot replace it
//Cause: Cannot replace a trigger which already exists on a different table than the one being replaced.
//Action: Drop the trigger with the same name and re-create it.
#define OER_TRIGGER_EXIST_ON_OTHER_TABLE  4095

//ORA-06502: PL/SQL: numeric or value error
//Cause: An arithmetic, numeric, string, conversion, or constraint error occurred. For example, this error occurs if an attempt is made to assign the value NULL to a variable declared NOT NULL, or if an attempt is made to assign an integer larger than 99 to a variable declared NUMBER(2).
//Action: Change the data, how it is manipulated, or how it is declared so that values do not violate constraints.
#define OER_NUMERIC_OR_VALUE_ERROR  6502

//ORA-06519 active autonomous transaction detected and rolled back
//Cause: Before returning from an autonomous PL/SQL block, all autonomous transactions started within the block must be completed (either committed or rolled back). If not, the active autonomous transaction is implicitly rolled back and this error is raised.
//Action: Ensure that before returning from an autonomous PL/SQL block, any active autonomous transactions are explicitly committed or rolled back.
#define OER_ACTIVE_AUTONOMOUS_TRANSACTION_ROLLBACK  6519

//ORA-06533: Subscript beyond count
//Cause: An in-limit subscript was greater than the count of a varray or too large for a nested table.
//Action: Check the program logic and explicitly extend if necessary.
#define OER_SUBSCRIPT_BEYOND_COUNT  6533

//ORA-06550: line string, column string:\nstring
//Cause: Usually a PL/SQL compilation error.
//Action: n/a
#define OER_COMPILATION_ERROR  6550

//ORA-06553: PLS-string: string
//ORA-06553: PLS-306: wrong number or types of arguments in call to 'CAST_TO_VARCHAR2'
#define OER_WRONG_FUNC_ARGUMENTS_TYPE  6553

//ORA-08102: index key not found, obj# string, file string, block string (string)
//Cause: Internal error: possible inconsistency in index
//Action: Send trace file to your customer support representative, along with information on reproducing the error
#define OER_INDEX_KEY_NOT_FOUND 8102

//ORA-08177: can't serialize access for this transaction
//Cause: Encountered data changed by an operation that occurred after the start of this serializable transaction.
//Action: In read/write transactions, retry the intended operation or transaction.
#define OER_CANNOT_SERIALIZE_TRANSACTION  8177

//ORA-12401 invalid label string: string
//Cause: The policy could not convert the label string to a valid internal label.
//Action: Correct the syntax of the label string.
#define OER_INVALID_LABEL_STRING          12401

//ORA-12416 policy string not found
//Cause: The specified policy does not exist in the database.
//Action: Enter the correct policy name or create the policy.
#define OER_POLICY_STRING_NOT_FOUND       12416

//ORA-12432 LBAC error: string
//Cause: LBAC enforcement resulted in an error.
//Action: Correct the problem identified in the error message.
#define OER_LBAC_ERROR       12432

//ORA-12444: policy already applied to table
//Cause: You tried to apply a policy to a table that was already protected by the policy.
//Action: To change the policy options, predicate, or label function, remove the policy from the table and re-apply it.
#define OER_POLICY_ALREADY_APPLIED_TO_TABLE      12444

//ORA-12447 policy role already exists for policy string
//Cause: The role named policy_DBA already exists.
//Action: Correct the policy name or delete the existing policy.
#define OER_POLICY_ROLE_ALREADY_EXISTS_FOR_POLICY_STRING      12447

//ORA-12461 undefined level string for policy string
//Cause: The specified level is not defined for the policy.
//Action: Correct the level identifier value.
#define OER_UNDEFINED_LEVEL_STRING_FOR_POLICY_STRING      12461

//ORA-12462 undefined compartment string for policy string
//Cause: The specified compartment is not defined for the policy.
//Action: Correct the compartment identifier value.
#define OER_UNDEFINED_COMPARTMENT_STRING_FOR_POLICY_STRING      12462

//ORA-12463 undefined group string for policy string
//Cause: The specified group is not defined for the policy.
//Action: Correct the group identifier value.
#define OER_UNDEFINED_GROUP_STRING_FOR_POLICY_STRING      12463

//ORA-12470 NULL or invalid user label
//Cause: The label entered is NULL or not within the user's authorizations.
//Action: Enter the authorized labels for the user.
#define OER_NULL_OR_INVALID_USER_LABEL      12470

//ORA-12702: invalid NLS parameter string used in SQL function
//Cause: An unknown parameter name or invalid value is specified in a NLS parameter string.
//Action: n/a
#define OER_INVALID_NLS_PARAMETER_STRING  12702

//ORA-12704: character set mismatch
//Cause: One of the following: - The string operands(other than an nlsparams argument) to an operator or built-in function do not have the same character set. - An nlsparams operand is not in the database character set. - String data with character set other than the database character set is passed to a built-in function not expecting it. - The second argument to CHR() or CSCONVERT() is not CHAR_CS or NCHAR_CS. - A string expression in the VALUES clause of an INSERT statement, or the SET clause of an UPDATE statement, does not have the same character set as the column into which the value would be inserted. - A value provided in a DEFAULT clause when creating a table does not have the same character set as declared for the column. - An argument to a PL/SQL function does not conform to the character set requirements of the corresponding parameter.
//Action: n/a
#define OER_CHARACTER_SET_MISMATCH      12704

//ORA-12725: unmatched parentheses in regular expression
//Cause: The regular expression did not have balanced parentheses.
//Action: Ensure the parentheses are correctly balanced.
#define OER_REGEXP_EPAREN      12725

//ORA-12726: unmatched bracket in regular expression
//Cause: The regular expression did not have balanced brackets.
//Action: Ensure the brackets are correctly balanced.
#define OER_REGEXP_EBRACK      12726

//ORA-12727: invalid back reference in regular expression
//Cause: A back references was found before a sub-expression.
//Action: Ensure a valid sub-expression is being referenced.
#define OER_REGEXP_ESUBREG      12727

//ORA-12728: invalid range in regular expression
//Cause: An invalid range was found in the regular expression.
//Action: Ensure a valid range is being used.
#define OER_REGEXP_ERANGE      12728

//ORA-12729: invalid character class in regular expression
//Cause: An unknown character class was found in the regular expression.
//Action: Ensure a valid characters class is being used.
#define OER_REGEXP_ECTYPE      12729

//ORA-12731: invalid collation class in regular expression
//Cause: An unknown collation class was found in the regular expression.
//Action: Ensure a valid collation class is being used.
#define OER_REGEXP_ECOLLATE      12731

// ORA-12827: insufficient parallel query slaves available
// Cause: PARALLEL_MAX_SERVERS parameter was specified and fewer than minimum slaves were acquired.
// Action: Either re-execute query with lower parallel or wait until some running queries are completed, thus freeing up slaves.
#define OER_INSUFFICIENT_PX_WORKER 12827

// ORA-12991: column is referenced in a multi-column constraint
// Cause: An attempt was made to drop a column referenced by some constraints.
// Action: Drop all constraints referencing the dropped column or specify CASCADE CONSTRAINTS in statement.
#define OER_MODIFY_OR_DROP_MULTI_COLUMN_CONSTRAINT 12991

// ORA-12992: cannot drop parent key column
// Cause: An attempt was made to drop a parent key column.
// Action: Drop all constraints referencing the parent key column, or specify CASCADE CONSTRAINTS in statement.
#define OER_DROP_PARENT_KEY_COLUMN 12992

//ORA-14019: partition bound element must be one of: string, datetime or interval literal, number, or MAXVALUE
//Cause: Partition bound list contained an element of invalid type (i.e. not a number, non-empty string, datetime or interval literal, or MAXVALUE)
//Action: Ensure that all elements of partition bound list are of valid type
#define OER_PARTITION_BOUND_ELEMENT_MUST_NOT_BE_NULL			        14019

//ORA-14036: partition bound value too large for column
//Cause: Length of partition bound value is longer than that of the corresponding partitioning column.
//Action: Ensure that lengths of high bound values do not exceed those of corresponding partitioning columns
#define OER_PARTITION_BOUND_VALUE_TOO_LARGE_FOR_COLUMN			      14036

//ORA-14037: partition bound of partition "string" is too high
//Cause: High bound of the partition whose name (explicitly specified by the user) is displayed in this message did not collate lower than that of the following partition, which is illegal.
//Action: Ensure that high bound of every partition (except for the last one) collates lower than that of a following partition.
#define OER_PARTITION_BOUND_OF_PARTITION_STRING_IS_TOO_HIGH			  14037

//ORA-14402 updating partition key column would cause a partition change
//Cause: An UPDATE statement attempted to change the value of a partition key column causing migration of the row to another partition.
//Action: Do not attempt to update a partition key column or make sure that the new partition key is within the range containing the old partition key.
#define OER_ERR_UPD_CAUSE_PART_CHANGE 14402

//ORA-14019: partition bound element must be one of: string, datetime or interval literal, number, or MAXVALUE
//Cause: Partition bound list contained an element of invalid type (i.e. not a number, non-empty string, datetime or interval literal, or MAXVALUE)
//Action: Ensure that all elements of partition bound list are of valid type
#define OER_ERR_PARTITION_BOUND_ELEMENT_MISMATCH_MAXVALUE 14019

// ORA-14251: Specified subpartition does not exist
// Cause: Subpartition not found for the object.
// Action: Retry with correct subpartition name
#define OER_ERR_SUBPARTITION_NOT_EXIST 14251

//ORA-14308: partition bound element must be one of: string, datetime or interval literal, number, or NULL
//Cause: Partition bound list contained an element of invalid type (i.e. not a number, non-empty string, datetime or interval literal, or NULL)
//Action: Ensure that all elements of partition bound list are of valid type
#define OER_ERR_PARTITION_BOUND_ELEMENT_MISMATCH_NULL 14308

//ORA-14074: partition bound must collate higher than that of the last partition
//Cause: Partition bound specified in ALTER TABLE ADD PARTITION statement did not collate higher than that of the table's last partition, which is illegal.
//Action: Ensure that the partition bound of the partition to be added collates higher than that of the table's last partition.
#define OER_PARTITION_BOUND_MUST_COLLATE_HIGHER_THAN_THAT_OF_THE_LAST_PARTITION			14074

//ORA-14400: inserted partition key does not map to any partition
//Cause: An attempt was made to insert a record into, a Range or Composite Range object, with a concatenated partition key that is beyond the concatenated partition bound list of the last partition -OR- An attempt was made to insert a record into a List object with a partition key that did not match the literal values specified for any of the partitions.
//Action: Do not insert the key. Or, add a partition capable of accepting the key, Or add values matching the key to a partition specification
#define OER_ERR_PARTITION_KEY_MISMATCH 14400

// ORA-14501: object is not partitioned
// Cause: Table or index is not partitioned. Invalid syntax.
// Action: Retry the command with correct syntax.
#define OER_ERR_OBJECT_NOT_PARTITIONED 14501

//ORA-16003: standby database is restricted to read-only access
//Cause: To ensure its integrity, a standby database can only be opened for read-only access.
//Action: Re-issue the ALTER DATABASE OPEN specifying READ ONLY.
#define OER_STANDBY_WEAK_READ_ONLY 16003

//ORA-22928: invalid privilege on directories
//Cause: An attempt was made to grant or revoke an invalid privilege on a directory.
//Action: Only CREATE, DELETE, READ, WRITE and EXECUTE privileges can be granted or revoked on directories. Do not grant or revoke other privileges.
#define OB_ERR_INVALID_PRIVILEGE_ON_DIRECTORIES 22928

//ORA-29257: host string unknown
//Cause: The specified host name was unknown.
//Action: Check the spelling of the host name or the IP address. Make sure that the host name or the IP address is valid.
#define OER_HOST_UNKNOWN 29257

//ORA-22998:CLOB or NCLOB in multibyte character set not supported
//cause:  A CLOB or NCLOB in a fixed-width or varying-width multibyte character set was passed
//        to a SQL character function which does not support multibyte LOB data.
//action: Use DBMS_LOB functions such as DBMS_LOB.INSTR() and DBMS_LOB.SUBSTR() or use PLSQL
//        DBMS_LOB.READ/WRITE to access LOB data.
#define OER_CLOB_ONLY_SUPPORT_WITH_MULTIBYTE_FUN 22998

//
// ORA-25156: 旧样式的外部联接 (+) 不能与 ANSI 联接一起使用
// 25156. 00000 -  "old style outer join (+) cannot be used with ANSI joins"
// *Cause:    When a query block uses ANSI style joins, the old notation
//            for specifying outer joins (+) cannot be used.
// *Action:   Use ANSI style for specifying outer joins also.
#define OER_OUTER_JOIN_WITH_ANSI_JOIN 25156


//ORA-24756: transaction does not exist
//Cause: An invalid transaction identifier or context was used or the transaction has completed.
//Action: Supply a valid identifier if the transaction has not completed and retry the call.
#define OER_TRANSACTION_DOES_NOT_EXIST  24756


//ORA-24761: transaction rolled back
#define OER_TRANSACTION_ROLLED_BACK  24761

//ORA-24784: Transaction exists
//Cause: An attempt was made to start a transaction, while attached to a non-migrateable transaction
//Action: n/a
#define OER_TRANSACTION_EXISTS  24784

//ORA-25128: No insert/update/delete on table with constraint (string.string) disabled and validated
//Cause: Try to insert/update/delete on table with DISABLE VALIDATE constraint.
//Action: Change the constraint's states.
#define OER_CONSTRAINT_DISABLE_VALIDATE 25128

//ORA-25129: cannot modify constraint (string) - no such constraint
//Cause: the named constraint does not exist for this table.
//Action: Obvious
#define OER_MODIFY_NONEXISTENT_CONSTRAINT 25129

//ORA-25137 Data value out of range
//Cause: Value from cast operand is larger than cast target size.
//Action: Increase size of cast target.
#define OER_DATA_VALUE_OUT_OF_RANGE 25137

// ORA-25154
// 25154. 00000 -  "column part of USING clause cannot have qualifier"
// *Cause:    Columns that are used for a named-join (either a NATURAL joiN or a join with a USING clause) cannot have an explicit qualifier.
// *Action:   Remove the qualifier.
#define OER_QUALIFIER_EXISTS_FOR_USING_COLUMN 25154


//ORA-25402: transaction must roll back
//Cause: A failure occured while a transaction was active on this connection.
//Action: The client must roll back.
#define OER_TRANSACTION_MUST_ROLL_BACK  25402

//ORA-25405: transaction status unknown
//Cause: A failure occured while a transaction was attempting to commit. Failover could not automatically determine instance status.
//Action: The user must determine the transaction's status manually.
#define OER_TRANSACTION_STATUS_UNKNOWN  25405

//ORA-28115: policy with check option violation
//Cause: Policy predicate was evaluated to FALSE with the updated values.
//Action: n/a
#define OER_POLICY_WITH_CHECK_OPTION_VIOLATION      28115

//ORA-30006: resource busy; acquire with WAIT timeout expired
//Cause: The requested resource is busy.
//Action: Retry the operation later.
#define OER_RESOURCE_BUSY_AND_ACQUIRE_WITH_WAIT  30006

//ORA-30076 invalid extract field for extract source
//Cause: The extract source does not contain the specified extract field.
//Action: Specify a valid extract field for the extract source.
#define OER_EXTRACT_FIELD_INVALID               30076

//ORA-30078: partition bound must be TIME/TIMESTAMP WITH TIME ZONE literals
//Cause: An attempt was made to use a time/timestamp expression whose format does not explicitly have time zone on a TIME/TIMESTAMP or TIME/TIMESTAMP WITH TIME ZONE column.
//Action: Explicitly use TIME/TIMESTAMP WITH TIME ZONE literal
#define OER_EXPLICITLY_USE_TIMESTAMP_TZ_LITERAL  30078

//ORA-30088: datetime/interval precision is out of range
//Cause: The specified datetime/interval precision was not between 0 and 9.
//Action: Use a value between 0 and 9 for datetime/interval precision.
#define OER_DATETIME_INTERVAL_PRECISION_OUT_OF_RANGE  30088

//ORA-30175: invalid type given for an argument
//Cause: There is an argument with an invalid type in the argument list.
//Action: Use the correct type wrapper for the argument.
#define OER_INVALID_TYPE_FOR_ARGUMENT  30175

// ORA-30929: ORDER SIBLINGS BY clause not allowed here
#define OER_INVALID_SIBLINGS 30929

//ORA-30483: window functions are not allowed here
//Cause: Window functions are allowed only in the SELECT list of a query. And, window function cannot be an argument to another window or group function.
//Action: n/a
#define ORE_INVALID_WINDOW_FUNC_USE 30483

//ORA-30488: argument should be a function of expressions in PARTITION BY
//Cause: The argument of the window function should be a constant for a partition.
//Action: None
#define OER_WIN_FUNC_ARG_NOT_IN_PARTITION_BY 30488

//ORA-30562: SAMPLE percentage must be in the range [0.000001,100)
#define OER_INVALID_SAMPLING_RANGE 30562

// ORA-30563: 此处不允许使用外部联接运算符 (+)
// 30563. 00000 -  "outer join operator (+) is not allowed here"
// *Cause:    An attempt was made to reference (+) in either the select-list,
//            CONNECT BY clause, START WITH clause, or ORDER BY clause.
// *Action:   Do not use the operator in the select-list, CONNECT BY clause,
//            START WITH clause, or ORDER BY clause.
#define OER_OUTER_JOIN_NOT_ALLOWED 30563

// ORA-30929: ORDER SIBLINGS BY clause not allowed here
#define OER_INVALID_SIBLINGS 30929

//ORA-32595: DDL statement cannot be audited with BY SESSION specified
//Cause: An attempt was made to audit a DDL statement with BY SESSION clause specified. DDL statements can be audited BY ACCESS only.
//Action: Specify BY ACCESS instead of BY SESSION to audit the DDL statement.
#define OER_DDL_STATEMENT_CANNOT_BE_AUDITED_WITH_BY_SESSION_SPECIFIED     32595

//ORA-38778: Restore point 'string' already exists.
//Cause: The restore point name of the CREATE RESTORE POINT command already exists. A restore point name must be unique.
//Action:: Either use a different name or delete the existing restore point with the same name.
#define OER_RESTORE_POINT_EXIST 38778

//ORA-38779: cannot create restore point - too many restore points.
//Cause: The maximum number of restore points already have been created.
//Action: Delete some existing restore point and retry the operation.
#define OER_RESTORE_POINT_TOO_MANY 38779

//ORA-38780: Restore point 'string' does not exist.
//Cause: The restore point name of the DROP RESTORE POINT command does not exist.
//Action: No action required.
#define OER_RESTORE_POINT_NOT_EXIST 38780

// ORA-40441: JSON syntax error
// Cause: The provided JavaScript Object Notation (JSON) data had invalid syntax and could not be parsed.
// Action: Provide JSON data with the correct syntax.
#define OB_ERR_JSON_SYNTAX_ERROR  40441

// ORA-40442: JSON path expression syntax error ('string')string
// Cause: The provided JavaScript Object Notation (JSON) path expression had an invalid syntax and could not be parsed.
// Action: Provide JSON path expression with the correct syntax.
#define OER_JSON_PATH_SYNTAX_ERROR  40442

//ORA-40449: invalid data type for return value
// Cause: The provided data type for the return value was invalid or unsupported.
// Action: Provide a supported return value.
#define OER_INVALID_DATA_TYPE_RETURNING 40449

// ORA-40451:invalid default value provided
// The provided default value was not allowed here.
// Provide a valid default value.
#define OER_INVALID_DEFAULT_VALUE_PROVIDED 40451

// ORA-40454: path expression not a literal
// Cause: The provided path expression was not a literal (a constant).
// Action: Provide a constant path expression
#define OER_PATH_EXPRESSION_NOT_LITERAL 40454

// ORA-40455: default value not a literal
// Cause: The provided default value was not a literal (a constant).
// Action: Provide a constant default value.
#define OER_DEFAULT_VALUE_NOT_LITERAL  40455

// ORA-40456: JSON_VALUE evaluated to non-scalar value
// Cause: The provided JavaScript Object Notation (JSON) path expression selected a non-scalar value.
// Action: Correct the JSON path expression or use JSON_QUERY.
#define OER_JSON_VALUE_NO_SCALAR  40456

// ORA-40462: JSON_VALUE evaluated to no value
// Cause: The provided JavaScript Object Notation (JSON) path expression did not select a value.
// Action: Correct the JSON path expression.
#define OER_JSON_VALUE_NO_VALUE  40462

// ORA-42399: cannot perform a DML operation on a read-only view
// Cause: An attempt was made to insert, delete, or update row(s) of a view created with read-only option. DML operations on such views are explicitly prohibited.
// Action: Perform the desired DML operation on the view's underlying base table or issue ALTER VIEW ... READ WRITE command to make the view updatable.
#define OER_MODIFY_READ_ONLY_VIEW 42399

// ORA-54039: table must have at least one column that is not invisible
// Cause: An attempt was made to create or alter a table to have only invisible columns.
// Action: Include at least one column that is not invisible in the table being created or altered.
#define OER_ONLY_HAVE_INVISIBLE_COL_IN_TABLE 54039

// ORA-54042: Invisible column is not supported on this type of table.
// Cause: An attempt was made to create or alter the visibility of a column on an unsupported table type. Invisible columns are only supported for heap and index-organized tables (IOT).
// Action: These columns are not supported, change the DDL.
#define OER_INVISIBLE_COL_ON_UNSUPPORTED_TABLE_TYPE 54042

// ORA-54046: Column visibility modifications cannot be combined with any other modified column DDL option.
// Cause: An attempt was made to combine the modification of the visibility of a column with other modified column property changes.
// Action: Separate column visibility modifications into a standalone DDL.
#define OER_MODIFY_COL_VISIBILITY_COMBINED_WITH_OTHER_OPTION 54046

// ORA-54053: The visibility of a column from a table owned by a SYS user cannot be changed.
// Cause: An attempt was made to alter the visibility of a column from a table owned by SYS user.
// Action: Execute the DDL on a table not owned by a SYS user.
#define OER_MODIFY_COL_VISIBILITY_BY_SYS_USER 54053

//ORA-62550: Invalid SQL ROW LIMITING expression was specified.
//Cause: The SQL ROW LIMITING expression contained components other than literals, binds, correlation variables, or subqueries.
//Action: Modify the SQL ROW LIMITING expression and retry the operation.
#define OER_INVALID_SQL_ROW_LIMITING 62550

//// sequence begin
#define OER_INVALID_SEQUENCE_NAME               2277
#define OER_DUP_CONFL_MAXVALUE_SPEC             2278
#define OER_DUP_CONFL_MINVALUE_SPEC             2279
#define OER_DUP_CONFL_CYCLE_SPEC                2280
#define OER_DUP_CONFL_CACHE_SPEC                2281
#define OER_DUP_CONFL_ORDER_SPEC                2282
#define OER_ALTER_START_SEQ_NUMBER_NOT_ALLOWED  2283
#define OER_DUP_INCREMENT_BY_SPEC               2284
#define OER_DUP_START_WITH_SPEC                 2285
#define OER_REQUIRE_ALTER_SEQ_OPTION            2286
#define OER_SEQ_NOT_ALLOWED_HERE                2287
#define OER_SEQ_NOT_EXIST                       2289
#define OER_SEQ_OPTION_MUST_BE_INTEGER          4001
#define OER_SEQ_INCREMENT_CAN_NOT_BE_ZERO       4002
#define OER_SEQ_OPTION_EXCEED_RANGE             4003
#define OER_MINVALUE_LARGER_THAN_MAXVALUE       4004
#define OER_SEQ_INCREMENT_TOO_LARGE             4005
#define OER_START_WITH_LESS_THAN_MINVALUE       4006
#define OER_MINVALUE_EXCEED_CURRVAL             4007
#define OER_START_WITH_EXCEED_MAXVALUE          4008
#define OER_MAXVALUE_EXCEED_CURRVAL             4009
#define OER_SEQ_CACHE_TOO_SMALL                 4010
#define OER_SEQ_OPTION_OUT_OF_RANGE             4011
#define OER_OBJECT_IS_NOT_SEQ                   4012
#define OER_SEQ_CACHE_TOO_LARGE                 4013
#define OER_SEQ_REQUIRE_MINVALUE                4014
#define OER_SEQ_REQUIRE_MAXVALUE                4015
#define OER_SEQ_NO_LONGER_EXIST                 4016
#define OER_SEQ_VALUE_EXCEED_LIMIT              8004
#define OER_INVALID_TIMESTAMP                   8186

//// sequence end

#define OER_SNAPSHOT_EXPRESSION_NOT_ALLOWED 8187

//px start
#define OER_SIGNALED_IN_PARALLEL_QUERY_SERVER         12801
//px end



//// subquery factoring (CTE, with clause) begin
#define OER_CTE_ILLEGAL_QUERY_NAME                                32031
#define OER_CTE_UNSUPPORTED_COLUMN_ALIASING                       32033
#define OER_UNSUPPORTED_USE_OF_CTE                                32034
#define OER_CTE_COLUMN_NUMBER_NOT_MATCH                           32038
#define OER_NEED_COLUMN_ALIAS_LIST_IN_RECURSIVE_CTE               32039
#define OER_NEED_UNION_ALL_IN_RECURSIVE_CTE                       32040
#define OER_NEED_ONLY_TWO_BRANCH_RECURSIVE_CTE                    32041
#define OER_REFERENCE_ITSELF_DIRECTLY_IN_RECURSIVE_CTE            32042
#define OER_NEED_INIT_BRANCH_IN_RECURSIVE_CTE                     32043
#define OER_CYCLE_FOUND_IN_RECURSIVE_CTE                          32044
#define OER_CTE_REACH_MAX_LEVEL_RECURSION                         32045
#define OER_CTE_ILLEGAL_SEARCH_PSEUDO_NAME                        32046
#define OER_CTE_ILLEGAL_CYCLE_NON_CYCLE_VALUE                     32047
#define OER_CTE_ILLEGAL_CYCLE_PSEUDO_NAME                         32048
#define OER_CTE_COLUMN_ALIAS_DUPLICATE                            32049
#define OER_CTE_ILLEGAL_SEARCH_CYCLE_CLAUSE                       32480
#define OER_CTE_DUPLICATE_CYCLE_NON_CYCLE_VALUE                   32481
#define OER_CTE_DUPLOCATE_SEQ_NAME_CYCLE_COLUMN                   32482
#define OER_CTE_DUPLICATE_NAME_IN_SEARCH_CLAUSE                   32483
#define OER_CTE_DUPLICATE_NAME_IN_CYCLE_CLAUSE                    32484
#define OER_CTE_ILLEGAL_COLUMN_IN_CYCLE_CLAUSE                    32485
#define OER_CTE_ILLEGAL_RECURSIVE_BRANCH                          32486
#define OER_ILLEGAL_JOIN_IN_RECURSIVE_CTE                         32487
#define OER_CTE_NEED_COLUMN_ALIAS_LIST                            32488
#define OER_CTE_ILLEGAL_COLUMN_IN_SERACH_CALUSE                   32489
#define OER_CTE_RECURSIVE_QUERY_NAME_REFERENCED_MORE_THAN_ONCE    32490
//// subquery factoring (CTE, with clause) end

//// connect by begin
#define OER_CBY_PSEUDO_COLUMN_NOT_ALLOWED                           976
#define OER_CBY_LOOP                                               1436
#define OER_CBY_JOIN_NOT_ALLOWED                                   1437
#define OER_CBY_CONNECT_BY_REQUIRED                                1788
#define OER_CBY_CONNECT_BY_PATH_NOT_ALLOWED                       30002
#define OER_CBY_CONNECT_BY_PATH_ILLEGAL_PARAM                     30003
#define OER_CBY_CONNECT_BY_PATH_INVALID_SEPARATOR                 30004
#define OER_CBY_CONNECT_BY_ROOT_ILLEGAL_USED                      30007
#define OER_CBY_NO_MEMORY                                         30009
#define OER_CBY_OREDER_SIBLINGS_BY_NOT_ALLOWED                    30929
#define OER_CBY_NOCYCLE_REQUIRED                                  30930
//// connect by end

// ORA-13207: incorrect use of the [string] operator
// Cause: An error was encountered while evaluating the specified operator.
// Action: Check the parameters and the return type of the specified operator.
#define OER_INCORRECT_USE_OF_OPERATOR 13207

/* ORA-30926: unable to get a stable set of rows in the source tables
 *  * Cause: A stable set of rows could not be got because of large dml activity or a non-deterministic where clause.
 *   * Action: Remove any non-deterministic where clauses and reissue the dml.
 *    */
#define OER_UPDATE_TWICE 30926

/* ORA-38101: Invalid column in the INSERT VALUES Clause: string
 * Cause: INSERT VALUES clause refers to the destination table columns
 * Action: none
 */
#define OER_INVALID_INSERT_COLUMN 38101

/* ORA-38104: Columns referenced in the ON Clause cannot be updated: string
 *  * Cause: LHS of UPDATE SET contains the columns referenced in the ON Clause
 *   * Action: n/a
 *    */
#define OER_UPDATE_ON_EXPR 38104

/* ORA-20000: string
 * Cause: The stored procedure 'raise_application_error' was called which causes this error to be generated.
 * Action: Correct the problem as described in the error message or contact the application administrator or DBA for more information.
 */
#define OER_RAISE_APPLICATION_ERROR                             20000

// ORA-21000: error number argument to raise_application_error of stringstring is out of range
#define OER_RAISE_APPLICATION_ERROR_NUM                         21000

// ORA-22288: file or LOB operation FILEOPEN failed due to soft link or relative path
#define OER_RAISE_PATH_ERROR

//ORA-08006: specified row no longer exists
//Cause: the row has been deleted by another user since the operation began
//Action: re-try the operation
#define OER_SPECIFIED_ROW_NO_LONGER_EXISTS                      8006

//ORA-30487: order by clause not allowed in some window function
//Cause: in some window function, distinct can not be followed by orderby_clause.
#define OER_ORDERBY_NOT_ALLOWED                                 30487
//ORA-30482: distinct not allowed
#define OER_DISTINCT_NOT_ALLOWED                                30482

//ORA-56901: non-constant expression is not allowed for pivot|unpivot values
//Cause: Attempted to use non-constant expression for pivot|unpivot values.
//Action: Use constants for pivot|unpivot values.
#define OER_NON_CONST_EXPR_IS_NOT_ALLOWED_FOR_PIVOT_UNPIVOT_VALUES     56901

//ORA-56902: expect aggregate function inside pivot operation
//Cause: Attempted to use non-aggregate expression inside pivot operation.
//Action: Use aggregate function.
#define OER_EXPECT_AGGREGATE_FUNCTION_INSIDE_PIVOT_OPERATION      56902

//ORA-24381: error(s) in array DML
//Cause: One or more rows failed in the DML.
//Action: Refer to the error stack in the error handle.
#define OER_ERROR_IN_ARRAY_DML  24381

// PLS-00157: Only schema-level programs allow string
// Cause: An AUTHID or DEFAULT COLLATION clause was specified for a subprogram inside a package or type. These clauses are only supported for top-level stored procedures, packages, and types.
// Action: Remove the clause.
#define OER_ONLY_SCHEMA_LEVEL_ALLOW 00157

// PLS-00371: at most one declaration for 'string' is permitted
// Cause: A reference to an identifier was ambiguous because there were multiple declarations for the identifier. At most one local variable with a given identifier is permitted in the declarative part of a block, procedure, or function. At most one label with a given identifier may appear in a block.
// Action: Check the spelling of the identifier. If necessary, remove all but one declaration of the identifier.
#define OER_DECL_MORE_THAN_ONCE 00371

// PLS-00410: duplicate fields in RECORD,TABLE or argument list are not permitted
// Cause: When a user-defined record was declared, the same name was given to two fields. Like column names in a database table, field names in a user-defined record must be unique.
// Action: Check the spelling of the field names, then remove the duplicate.
#define OER_DUPLICATE_FILED 00410



#endif /* _ORACLE_ERRNO_H */
