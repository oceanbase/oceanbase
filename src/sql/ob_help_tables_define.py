#!/usr/bin/env python
# encoding: utf-8

URL = "https://open.oceanbase.com/"

sys_variable_category_id = 1000
sys_config_category_id = 1001

category_list = [
	{
		"help_category_id": "1",
		"name": "Contents",
		"parent_category_id": "0",
		"url": URL
	},
	{
		"help_category_id": "2",
		"name": "Data Types",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "3",
		"name": "Functions",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "4",
		"name": "Operator",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "5",
		"name": "Escape character",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "6",
		"name": "Data Definition",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "7",
		"name": "Data Manipulation",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "8",
		"name": "Transaction Statements",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "9",
		"name": "Prepared Statements",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "10",
		"name": "Compound Statements",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "11",
		"name": "Administration",
		"parent_category_id": "1",
		"url": URL
	},
	{
		"help_category_id": "21",
		"name": "Numeric Types",
		"parent_category_id": "2",
		"url": URL
	},
	{
		"help_category_id": "22",
		"name": "Date and Time Types",
		"parent_category_id": "2",
		"url": URL
	},
	{
		"help_category_id": "23",
		"name": "String Types",
		"parent_category_id": "2",
		"url": URL
	},
	{
		"help_category_id": "24",
		"name": "Bool Types",
		"parent_category_id": "2",
		"url": URL
	},
	{
		"help_category_id": "31",
		"name": "Date and Time Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "32",
		"name": "String Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "33",
		"name": "GROUP BY (Aggregate) Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "34",
		"name": "Cast Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "35",
		"name": "Mathematical Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "36",
		"name": "Other Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "38",
		"name": "Control Flow Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "37",
		"name": "Comparison Functions",
		"parent_category_id": "3",
		"url": URL
	},
	{
		"help_category_id": "41",
		"name": "Logical Operators",
		"parent_category_id": "4",
		"url": URL
	},
	{
		"help_category_id": "42",
		"name": "Comparison Operators",
		"parent_category_id": "4",
		"url": URL
	},
	{
		"help_category_id": "43",
		"name": "Arithmetic Operators",
		"parent_category_id": "4",
		"url": URL
	},
	{
		"help_category_id": "44",
		"name": "Vector Comparison Operators",
		"parent_category_id": "4",
		"url": URL
	},
	{
		"help_category_id": "45",
		"name": "Splicing Operator",
		"parent_category_id": "4",
		"url": URL
	},
	{
		"help_category_id": "46",
		"name": "Operator Precedence",
		"parent_category_id": "4",
		"url": URL
	},
	{
		"help_category_id": "47",
		"name": "Bit Operators",
		"parent_category_id": "4",
		"url": URL
	},
	{
		"help_category_id": "101",
		"name": "Flow Control Statements",
		"parent_category_id": "10",
		"url": URL
	},
	{
		"help_category_id": "111",
		"name": "Account Management",
		"parent_category_id": "11",
		"url": URL
	},
	{
		"help_category_id": "112",
		"name": "Other Management",
		"parent_category_id": "11",
		"url": URL
	},
	# Don‘t modify this, plz.
	{
		"help_category_id": str(sys_variable_category_id),
		"name": "System Variable",
		"parent_category_id": "1",
		"url": URL 
	},
	# Don‘t modify this too, plz.
	{
		"help_category_id": str(sys_config_category_id),
		"name": "System Config",
		"parent_category_id": "1",
		"url": URL 
	}
]

topic_list = [
	{
		"help_topic_id": "1",
		"help_category_id": "21",
		"name": "TINYINT",
		"description": "\\n  TINYINT[(M)] [UNSIGNED] [ZEROFILL]\\n\\n  很小的整数。带符号的范围是-128到127。无符号的范围是0到255。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "2",
		"help_category_id": "21",
		"name": "BOOL,BOOLEAN",
		"description": "\\n  是TINYINT(1)的同义词。zero值被视为假。非zero值视为真。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "3",
		"help_category_id": "21",
		"name": "SMALLINT",
		"description": "\\n  SMALLINT[(M)] [UNSIGNED] [ZEROFILL]\\n\\n  小的整数。带符号的范围是-32768到32767。无符号的范围是0到65535。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "4",
		"help_category_id": "21",
		"name": "MEDIUMINT",
		"description": "\\n  MEDIUMINT[(M)] [UNSIGNED] [ZEROFILL]\\n\\n  中等大小的整数。带符号的范围是-8388608到8388607。无符号的范围是0到16777215。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "5",
		"help_category_id": "21",
		"name": "INT",
		"description": "\\n  INT[(M)] [UNSIGNED] [ZEROFILL]\\n\\n  普通大小的整数。带符号的范围是-2147483648到2147483647。无符号的范围是0到4294967295。\\n\\n  将非法的int值插入表之前自动改为0。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6",
		"help_category_id": "21",
		"name": "INTEGER",
		"description": "\\n  INTEGER[(M)] [UNSIGNED] [ZEROFILL]\\n\\n  这是INT的同义词。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "7",
		"help_category_id": "21",
		"name": "BIGINT",
		"description": "\\n  BIGINT[(M)] [UNSIGNED] [ZEROFILL]\\n\\n  大整数。带符号的范围是-9223372036854775808到9223372036854775807。无符号的范围是0到18446744073709551615。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "8",
		"help_category_id": "21",
		"name": "FLOAT",
		"description": "\\n  FLOAT[(M,D)] [UNSIGNED] [ZEROFILL]\\n\\n  小(单精度)浮点数。允许的值范围为-2^128 ~ +2^128，也即-3.402823466E+38到-1.175494351E-38、0和1.175494351E-38到3.402823466E+38。这些是理论限制，基于IEEE标准。实际的范围根据硬件或操作系统的不同可能稍微小些。\\n\\n  M是小数总位数，D是小数点后面的位数。如果M和D被省略，根据硬件允许的限制来保存值。单精度浮点数精确到大约7位小数位。\\n\\n  如果指定UNSIGNED，不允许负值。\\n\\n  使用浮点数可能会遇到意想不到的问题，因为在所有计算用双精度完成。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "9",
		"help_category_id": "21",
		"name": "DOUBLE",
		"description": "\\n  DOUBLE[(M,D)] [UNSIGNED] [ZEROFILL]\\n\\n  普通大小(双精度)浮点数。允许的值范围为：-2^1024 ~ +2^1024，也即是-1.7976931348623157E+308到-2.2250738585072014E-308、0和2.2250738585072014E-308到 1.7976931348623157E+308。这些是理论限制，基于IEEE标准。实际的范围根据硬件或操作系统的不同可能稍微小些。\\nM是小数总位数，D是小数点后面的位数。如果M和D被省略，根据硬件允许的限制来保存值。双精度浮点数精确到大约15位小数位。\\n\\n  如果指定UNSIGNED，不允许负值。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "10",
		"help_category_id": "21",
		"name": "DOUBLE PRECISION",
		"description": "\\n  DOUBLE PRECISION [(M,D)] [UNSIGNED] [ZEROFILL], REAL[(M,D)] [UNSIGNED] [ZEROFILL]\\n  为DOUBLE的同义词。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "11",
		"help_category_id": "21",
		"name": "FLOAT(p)",
		"description": "\\n  FLOAT(p) [UNSIGNED] [ZEROFILL]\\n\\n  浮点数。p表示精度（以位数表示），只使用该值来确定是否结果列的数据类型为FLOAT或DOUBLE。如果p为从0到24，数据类型变为没有M或D值的FLOAT。如果p为从25到53，数据类型变为没有M或D值的DOUBLE。结果列范围与单精度FLOAT或双精度DOUBLE数据类型相同。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "12",
		"help_category_id": "21",
		"name": "DECIMAL",
		"description": "\\n  DECIMAL[(M[,D])] [UNSIGNED] [ZEROFILL]\\n\\n  压缩的“严格”定点数。M是小数位数(精度)的总数，D是小数点(标度)后面的位数。小数点和(负数)的\\'-\\'符号不包括在M中。如果D是0，则值没有小数点或分数部分。DECIMAL整数最大位数(M)为90。支持的十进制数的最大位数(D)是45。如果D被省略， 默认是0。如果M被省略， 默认是10。\\n\\n  如果指定UNSIGNED，不允许负值。\\n\\n  所有DECIMAL列的基本计算(+，-，*，/)用65位精度完成。\\n\\n  DEC[(M[,D])] [UNSIGNED] [ZEROFILL], NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL]是DECIMAL的同义词。FIXED同义词适用于与其它服务器的兼容性。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "13",
		"help_category_id": "21",
		"name": "NUMERIC",
		"description": "\\n  NUMERIC[(M[,D])] [UNSIGNED] [ZEROFILL]是DECIMAL的同义词。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "14",
		"help_category_id": "23",
		"name": "CHAR",
		"description": "\\n  CHAR是CHARACTER的简写。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "15",
		"help_category_id": "23",
		"name": "VARCHAR",
		"description": "\\n  [NATIONAL] VARCHAR(M) [BINARY]\\n\\n  变长字符串。M 表示最大列长度。 VARCHAR的最大实际长度由最长的行的大小和使用的字符集确定。最大有效长度是256K（262,144字节）。假设系统使用UTF8MB4字符集，每个字符占用4字节，M最大值为：（1024*256）/4 = 65536 (字符)。则M的范围是0到65536。\\n\\n  VARCHAR是字符CHAR VARYING的简写。\\n\\n  varchar类型最大长度限制：\\n\\n  a) 建表的时候，检查varchar型主键长度之和小于等于16K，且总长度小于1.5M，否则报错\\n\\n  b) 建立索引的时候，索引表中varchar类型主键之和小于等于16K，且总长度小于1.5M，否则不允许建立索引\\n\\n  c) 单个varchar列长度小于256K\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "16",
		"help_category_id": "23",
		"name": "BINARY/VARBINARY",
		"description": "\\n  BINARY和VARBINARY类类似于CHAR和VARCHAR，不同的是它们包含二进制字符串而不要非二进制字符串。也就是说，它们包含字节字符串而不是字符字符串。这说明它们没有字符集，并且排序和比较基于列值字节的数值值。\\n\\n  BINARY和VARBINARY允许的最大长度一样，如同CHAR和VARCHAR，不同的是BINARY和VARBINARY的长度是字节长度而不是字符长度。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "17",
		"help_category_id": "22",
		"name": "DATE",
		"description": "\\n  日期。\\n\\n  支持的范围为\\'1000-01-01\\'到\\'9999-12-31\\'。以\\'YYYY-MM-DD\\'格式显示DATE值，但允许使用字符串或数字为DATE列分配值。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "18",
		"help_category_id": "22",
		"name": "DATETIME",
		"description": "\\n  DATETIME[(fsp)]\\n\\n  日期和时间的组合。支持的范围是\\'1000-01-01 00:00:00.000000\\'到\\'9999-12-31 23:59:59.000000\\'。以\\'YYYY-MM-DD HH:MM:SS[.fraction] \\'格式显示DATETIME值，但允许使用字符串或数字为DATETIME列分配值。fsp参数是表示秒精度，取值范围为：0-6。默认值取0；最大值为6，表示精确到微妙。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "19",
		"help_category_id": "22",
		"name": "TIMESTAMP",
		"description": "\\n  TIMESTAMP[(fsp)]\\n\\n  TIMESTAMP类型的格式是按严格模式来判断，不允许“0000-00-00 00:00:00”等非法值\\n\\n  下列字符串类型是不合法的：\\n  \\'2012^12^32\\'\\n  \\'20070523\\'\\n  \\'070523\\'\\n  \\'071332\\' \\n\\n  下列整数类型同样是不合法的：\\n  19830905\\n  830905\\n  071332\\n\\n  用DEFAULT CURRENT_TIMESTAMP和ON UPDATE CURRENT_TIMESTAMP子句，列为默认值使用当前的时间戳，并且自动更新。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "20",
		"help_category_id": "22",
		"name": "TIME",
		"description": "\\n  TIME[(fsp)]\\n\\n  时间。范围是 \\'-838:59:59.000000\\'到\\'838:59:59.000000\\'。以\\'HH:MM:SS[.fraction]\\'格式显示TIME值，但允许使用字符串或数字为TIME列分配值。fsp参数是表示秒精度，取值范围为：0-6。默认值取0；最大值为6，表示精确到微妙。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "21",
		"help_category_id": "22",
		"name": "YEAR",
		"description": "\\n  两位或四位格式的年。\\n\\n  默认是四位格式。在四位格式中，允许的值是1901到2155和0000。在两位格式中，允许的值是70到69，表示从1970年到2069年。以YYYY 格式显示YEAR值，但允许使用字符串或数字为YEAR列分配值。\\n\\n  可以指定各种格式的YEAR值：\\n\\n  四位字符串，范围为\\'1901\\'到\\'2155\\'。\\n\\n  四位数字，范围为1901到2155。\\n\\n  两位字符串，范围为\\'00\\'到\\'99\\'。\\'00\\'到\\'69\\'和\\'70\\'到\\'99\\'范围的值被转换为2000到2069和1970到1999范围的YEAR值。\\n\\n  两位整数，范围为1到99。1到69和70到99范围的值被转换为2001到2069和1970到1999范围的YEAR值。请注意两位整数范围与两位字符串范围稍有不同，因为你不能直接将零指定为数字并将它解释为2000。你必须将它指定为一个字符串\\'0\\'或\\'00\\'或它被解释为0000。\\n\\n  函数返回的结果，其值适合YEAR上下文，例如NOW()。\\n\\n  非法YEAR值被转换为0000。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "22",
		"help_category_id": "31",
		"name": "CURRENT_TIME",
		"description": "\\n  CURRENT_TIME()和CURRENT_TIMESTAMP()这两个函数用于获取系统当前时间，但返回格式有区别。\\n\\n  CURRENT_TIME()是取当前时间，但不包括日期，返回格式为“HH:MI:SS”。\\n\\n  CURRENT_TIMESTAMP()则取当前日期+时间，返回格式为“YYYY-MM-DD HH:MI:SS”。\\n\\n  current_time()和current_timestamp()可以传入0-6的数字参数，表示秒后的小数点精度。\\n\\n",
		"example": "\\nroot@test 04:09:10>SELECT CURRENT_TIME(), CURRENT_TIMESTAMP();\\n+----------------+---------------------+\\n| CURRENT_TIME() | CURRENT_TIMESTAMP() |\\n+----------------+---------------------+\\n| 16:24:09       | 2014-10-31 16:24:09 |\\n+----------------+---------------------+\\n1 row in set (0.00 sec)\\nroot@(none) 09:59:30>select CURRENT_TIME(1);\\n#可以支持传入秒后的小数位精度参数\\n+-----------------+\\n| CURRENT_TIME(1) |\\n+-----------------+\\n| 09:59:32.4      |\\n+-----------------+\\n1 row in set (0.00 sec)\\nroot@(none) 09:59:32>select CURRENT_TIME(7);\\nERROR 1426 (42000): Too big precision 7 specified for column \\'curtime\\'. Maximum is 6.\\n#传入的精度范围是0-6，否则报错，错误码1426 (42000)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "23",
		"help_category_id": "31",
		"name": "CURTIME",
		"description": "\\n  CURTIME()是CURRENT_TIME(),CURRENT_TIME的同义词。\\n\\n",
		"example": "\\nmysql> select curtime(), current_time(), current_time;\\n+-----------+----------------+--------------+\\n| curtime() | current_time() | current_time |\\n+-----------+----------------+--------------+\\n| 14:45:37  | 14:45:37       | 14:45:37     |\\n+-----------+----------------+--------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "24",
		"help_category_id": "31",
		"name": "CURRENT_TIMESTAMP",
		"description": "\\n  CURRENT_TIME()和CURRENT_TIMESTAMP()这两个函数用于获取系统当前时间，但返回格式有区别。\\n\\n  CURRENT_TIME()是取当前时间，但不包括日期，返回格式为“HH:MI:SS”。\\n\\n  CURRENT_TIMESTAMP()则取当前日期+时间，返回格式为“YYYY-MM-DD HH:MI:SS”。 \\n\\n  current_time()和current_timestamp()可以传入0-7的数字参数，表示秒后的小数点精度。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "25",
		"help_category_id": "31",
		"name": "CURRENT_DATE",
		"description": "\\n  CURRENT_DATE()返回当前日期，以\\'YYYY-MM-DD\\'或者 YYYYMMDD的形式显示。以何种形式显示取决于函数中的内容是字符串还是数值。",
		"example": "\\nmysql> select current_date, current_date+5;\\n+--------------+----------------+\\n| current_date | current_date+5 |\\n+--------------+----------------+\\n| 2015-08-27   |       20150832 |\\n+--------------+----------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "26",
		"help_category_id": "31",
		"name": "CURDATE",
		"description": "\\n  CURDATE()是CURRENT_DATE(), CURRENT_DATE的同义词。\\n\\n",
		"example": "\\nmysql> select curdate(), current_date(), current_date;\\n+------------+----------------+--------------+\\n| curdate()  | current_date() | current_date |\\n+------------+----------------+--------------+\\n| 2015-08-27 | 2015-08-27     | 2015-08-27   |\\n+------------+----------------+--------------+\\n1 row in set (0.00 sec)\\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "27",
		"help_category_id": "31",
		"name": "DATE_ADD",
		"description": "\\n  DATE_ADD(date, INTERVAL expr unit\\n\\n  这个函数用来执行时间的算术计算。将date值作为基数，对expr进行相加计算，expr的值允许为负数。DATE_ADD()计算时间值是否使用夏令时，由操作系统系统根据其内部配置和相应时区设定来决定。\\n\\n• date参数类型只能为Time类型(DATETIME, TIMESTAMP等)或者代表时间的一个字符串，不接受其它类型。\\n\\n• expr的值允许为负，对一个负值相加功能等同于对一个正值相减。允许系统函数的调用结果作为该参数，但是所有结果都将作为字符串结果。\\n\\n• unit为单位，支持MICROSECOND、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、QUARTER、YEAR、SECOND_MICROSECOND、MINUTE_MICROSECOND、MINUTE_SECOND、HOUR_MICROSECOND、HOUR_SECOND、HOUR_MINUTE、DAY_MICROSECOND、DAY_SECOND、DAY_MINUTE、DAY_HOUR和YEAR_MONTH。其中QUARTER代表季度。\\n\\n• unit为复合单位时，expr必须加单引号。\\n\\n",
		"example": "\\nmysql> SELECT DATE_ADD(now(), INTERVAL 5 DAY),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 MICROSECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 SECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 MINUTE),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 HOUR),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 DAY),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 WEEK),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 MONTH),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 QUARTER),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 YEAR),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'5.000005\\' SECOND_MICROSECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05.000005\\' MINUTE_MICROSECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05\\' MINUTE_SECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05:05.000005\\' HOUR_MICROSECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05:05\\' HOUR_SECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05\\' HOUR_MINUTE),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05:05:05.000005\\' DAY_MICROSECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05:05:05\\' DAY_SECOND),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05:05\\' DAY_MINUTE),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05\\' DAY_HOUR),\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'1-01\\' YEAR_MONTH) \\\\G\\n*************************** 1. row ***************************\\nDATE_ADD(now(), INTERVAL 5 DAY): 2014-02-22 10:58:26.056992\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 MICROSECOND): 2014-01-10 00:00:00.000005\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 SECOND): 2014-01-10 00:00:05\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 MINUTE): 2014-01-10 00:05:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 HOUR): 2014-01-10 05:00:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 DAY): 2014-01-15 00:00:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 WEEK): 2014-02-14 00:00:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 MONTH): 2014-06-10 00:00:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 QUARTER): 2015-04-10 00:00:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL 5 YEAR): 2019-01-10 00:00:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'5.000005\\' SECOND_MICROSECOND): 2014-01-10 00:00:05.000005\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05.000005\\' MINUTE_MICROSECOND): 2014-01-10 00:05:05.000005\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05\\' MINUTE_SECOND): 2014-01-10 00:05:05\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05:05.000005\\' HOUR_MICROSECOND): 2014-01-10 05:05:05.000005\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05:05\\' HOUR_SECOND): 2014-01-10 05:05:05\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'05:05\\' HOUR_MINUTE): 2014-01-10 05:05:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05:05:05.000005\\' DAY_MICROSECOND): 2014-01-11 05:05:05.000005\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05:05:05\\' DAY_SECOND): 2014-01-11 05:05:05\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05:05\\' DAY_MINUTE): 2014-01-11 05:05:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'01 05\\' DAY_HOUR): 2014-01-11 05:00:00\\nDATE_ADD(\\'2014-01-10\\', INTERVAL \\'1-01\\' YEAR_MONTH): 2015-02-10 00:00:00\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "28",
		"help_category_id": "31",
		"name": "DATE_FORMAT",
		"description": "\\n  DATE_FORMAT(date, format)\\n\\n  DATE_FORMAT()是STR_TO_DATE()的逆函数，DATE_FORMAT()接受一个时间值date，将其按format的格式格式化成一个时间字符串。\\n\\n• date参数给出了被格式化的时间值，date只接受time类型和时间字符串作为参数，其具体描述参考DATE_ADD()的date参数描述。\\n\\n",
		"example": "\\nmysql> SELECT DATE_FORMAT(\\'2014-01-01\\', \\'%Y-%M-%d\\'),\\nDATE_FORMAT(\\'2014-01-01\\', \\'%X-%V\\'),DATE_FORMAT(\\'2014-01-01\\', \\'%U\\') \\\\G\\n*************************** 1. row ***************************\\nDATE_FORMAT(\\'2014-01-01\\', \\'%Y-%M-%d\\'): 2014-January-01\\nDATE_FORMAT(\\'2014-01-01\\', \\'%X-%V\\'): 2013-52\\nDATE_FORMAT(\\'2014-01-01\\', \\'%U\\'): 00\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "29",
		"help_category_id": "31",
		"name": "DATE_SUB",
		"description": "\\n  DATE_SUB(date, INTERVAL expr unit)\\n\\n  对时间进行算数计算。将date作为基数，对expr进行相减计算，expr允许为负，结果相当于做取反做加法。\\n\\n",
		"example": "\\nmysql> SELECT DATE_SUB(\\'2014-01-10\\', INTERVAL 5 HOUR),\\nDATE_SUB(\\'2014-01-10\\', INTERVAL \\'05:05:05.000005\\' HOUR_MICROSECOND)\\\\G\\n*************************** 1. row ***************************\\nDATE_SUB(\\'2014-01-10\\', INTERVAL 5 HOUR): 2014-01-09 19:00:00\\nDATE_SUB(\\'2014-01-10\\', INTERVAL \\'05:05:05.000005\\' HOUR_MICROSECOND): 2014-01-09 18:54:54.999995\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "30",
		"help_category_id": "31",
		"name": "EXTRACT",
		"description": "\\n  EXTRACT(unit FROM date)\\n\\n  提取date表达式中被unit指定的时间组成单元的值。\\n\\n• EXTRACT函数返回的结果为BIGINT类型。\\n\\n• 对于MICROSECOND~YEAR这种Single unit，将直接返回对应的int值。\\n\\n• unit为WEEK返回的是date表达式中指定的日期在该年所对应的周数， 而OceanBase将一年的第一个星期日作为该年第一周的开始，如果某年的第一个星期日不是1月1日，那么该星期日之前的日期处于第0周。例如，2013年第一个星期日是1月6日，所以SELECT EXTRACT(WEEK FROM \\'2013-01-01\\')返回的结果为0，而SELECT EXTRACT(WEEK FROM \\'2013-01-06\\')返回的结果是1。\\n\\n• 对于SECOND_MICROSECOND这种combinative unit，OceanBase将各个值拼接在一起作为返回值。例如，SELECT EXTRACT(YEAR_MONTH FROM \\'2012-03-09\\')返回的结果将是“201203”。\\n\\n",
		"example": "\\nmysql> SELECT EXTRACT(WEEK FROM \\'2013-01-01\\'), \\nEXTRACT(WEEK FROM \\'2013-01-06\\'),  \\nEXTRACT(YEAR_MONTH FROM \\'2012-03-09\\'), \\nEXTRACT(DAY FROM NOW())\\\\G\\n*************************** 1. row ***************************\\nEXTRACT(WEEK FROM \\'2013-01-01\\'): 0\\nEXTRACT(WEEK FROM \\'2013-01-06\\'): 1\\nEXTRACT(YEAR_MONTH FROM \\'2012-03-09\\'): 201203\\nEXTRACT(DAY FROM NOW()): 18\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "31",
		"help_category_id": "31",
		"name": "NOW",
		"description": "\\n  NOW([fsp])\\n\\n  和CURRENT_TIMESTAMP()函数同义。用于获取系统当前时间，精确到秒，格式为“YYYY-MM-DD HH:MI:SS”。\\n\\n  now()函数的括号里可以传入0-6数字参数，表示秒后面的小数点精度，默认now()相当于now(0) 。\\n\\n",
		"example": "\\nmysql> SELECT NOW();\\n+----------------------------+\\n| NOW()                      |\\n+----------------------------+\\n| 2014-02-17 11:46:15|\\n+----------------------------+\\n1 row in set (0.00 sec)\\nroot@(none) 09:52:46>select now(0);\\n+---------------------+\\n| now(0)              |\\n+---------------------+\\n| 2014-11-03 09:55:01 |\\n+---------------------+\\n1 row in set (0.00 sec)\\nroot@(none) 09:55:01>select now(1);\\n+-----------------------+\\n| now(1)                |\\n+-----------------------+\\n| 2014-11-03 09:55:04.2 |\\n+-----------------------+\\n1 row in set (0.00 sec)\\nroot@(none) 09:55:04>select now(2);\\n+------------------------+\\n| now(2)                 |\\n+------------------------+\\n| 2014-11-03 09:55:06.57 |\\n+------------------------+\\n1 row in set (0.00 sec)\\nroot@(none) 09:55:06>select now(3);\\n+-------------------------+\\n| now(3)                  |\\n+-------------------------+\\n| 2014-11-03 09:55:09.576 |\\n+-------------------------+\\n1 row in set (0.00 sec)\\nroot@(none) 09:55:09>select now(7);\\nERROR 1426 (42000): Too big precision 7 specified for column \\'now\\'. Maximum is 6.\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "32",
		"help_category_id": "31",
		"name": "STR_TO_DATE",
		"description": "\\n  STR_TO_DATE(str,format)\\n\\n  STR_TO_DATE函数获取一个字符串 str 和一个格式字符串format。若格式字符串包含日期和时间部分，则 STR_TO_DATE()返回一个DATETIME值，若该字符串只包含日期部分或时间部分，则返回一个DATE或TIME值。\\n\\n• str所包含的日期、时间或日期时间值应该在format指示的格式中被给定。若 str 包含一个非法日期、时间或日期时间值，则 STR_TO_DATE()返回NULL。同时，一个非法值会引起警告。\\n\\n",
		"example": "\\nmysql> SELECT STR_TO_DATE(\\'2014-Jan-1st 5:5:5 pm\\', \\'%Y-%b-%D %r\\');\\n+-----------------------------------------------------+\\n| STR_TO_DATE(\\'2014-Jan-1st 5:5:5 pm\\', \\'%Y-%b-%D %r\\') |\\n+-----------------------------------------------------+\\n| 2014-01-01 17:05:05                                 |\\n+-----------------------------------------------------+\\n1 row in set (0.00 sec)\\n",
		"url": URL
	},
	{
		"help_topic_id": "33",
		"help_category_id": "31",
		"name": "TIME_TO_USEC",
		"description": "\\n  TIME_TO_USEC(date) \\n\\n  将OceanBase的内部时间类型转换成一个微秒数计数。表示date所指的时刻距离“1970-01-01 00:00:00”的微秒数，这是一个UTC时间，不带时区信息。\\n\\n• date为被计算的时刻，且这个时刻附带时区信息，而时区信息是用户当前系统设置的时区信息。该参数为TIMESTAMP类型或者时间格式的字符串。\\n\\n• TIME_TO_USEC能够接受其它函数的调用结果作为参数，但是其的结果类型必须为TIMESTAMP或者时间格式的字符串。\\n\\n• 该函数返回值为微秒计数，返回类型为INT。\\n\\n\\n",
		"example": "\\nmysql> SELECT TIME_TO_USEC(\\'2014-03-25\\'), TIME_TO_USEC(now());\\n+----------------------------+---------------------+\\n| TIME_TO_USEC(\\'2014-03-25\\') | TIME_TO_USEC(now()) |\\n+----------------------------+---------------------+\\n|           1395676800000000 |    1395735415207794 |\\n+----------------------------+---------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "34",
		"help_category_id": "31",
		"name": "USEC_TO_TIME",
		"description": "\\n  USEC_TO_TIME(usec)\\n\\n  该函数为TIME_TO_USEC(date)的逆函数，表示“1970-01-01 00:00:00”增加usec后的时间，且附带了时区信息。例如在东八区调用该函数“USEC_TO_TIME(1)”，返回值为“1970-01-01 08:00:01”。\\n\\n• usec为一个微秒计数值。\\n\\n• 返回值为TIMESTAMP类型。\\n\\n",
		"example": "\\nmysql> SELECT USEC_TO_TIME(1);\\n+----------------------------+\\n| USEC_TO_TIME(1)            |\\n+----------------------------+\\n| 1970-01-01 08:00:00.000001 |\\n+----------------------------+\\n1 row in set (0.00 sec)\\n",
		"url": URL
	},
	{
		"help_topic_id": "35",
		"help_category_id": "31",
		"name": "UNIX_TIMESTAMP ",
		"description": "\\n\\n  若无参数调用，则返回一个Unix timestamp (\\'1970-01-01 00:00:00\\' GMT 之后的秒数) 作为无符号整数。若用date 来调用UNIX_TIMESTAMP()，它会将参数值以\\'1970-01-01 00:00:00\\' GMT后的秒数的形式返回。date 可以是一个DATE 字符串、一个 DATETIME字符串、一个 TIMESTAMP或一个当地时间的YYMMDD 或YYYMMDD格式的数字。\\n\\n  当 UNIX_TIMESTAMP被用在 TIMESTAMP列时, 函数直接返回内部时戳值，而不进行任何隐含的 “string-to-Unix-timestamp”转化。假如你向UNIX_TIMESTAMP()传递一个溢出日期，它会返回0,但请注意只有基本范围检查会被履行(年份从1970 到 2037，月份从01到12,日期从01 到31)。\\n",
		"example": "\\nmysql> SELECT UNIX_TIMESTAMP();\\n+------------------+\\n| UNIX_TIMESTAMP() |\\n+------------------+\\n|       1427176668 |\\n+------------------+\\n1 row in set (0.00 sec)\\nmysql> SELECT UNIX_TIMESTAMP(\\'1997-10-04 22:23:00\\')\\n+---------------------------------------+\\n| UNIX_TIMESTAMP(\\'1997-10-04 22:23:00\\') |\\n+---------------------------------------+\\n|                             875974980 |\\n+---------------------------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "36",
		"help_category_id": "31",
		"name": "DATEDIFF",
		"description": "\\n  DATEDIFF(expr1,expr2)\\n\\n  DATEDIFF()返回起始时间expr1和结束时间expr2之间的天数。expr1和expr2 为日期或日期时间表达式。计算中只用到这些值的日期部分。\\n\\n  此函数必须跟两个参数，多于或少于两个参数，系统执行都将参数个数不正确错误。\\n\\n",
		"example": "\\nmysql> select datediff(\\'2015-06-19\\',\\'1994-12-17\\'), datediff(\\'2015-06-19\\',\\'1998-06-27 10:10:10\\'), datediff(now(), \\'2014-01-02\\')\\\\G\\n*************************** 1. row ***************************\\n         datediff(\\'2015-06-19\\',\\'1994-12-17\\'): 7489\\ndatediff(\\'2015-06-19\\',\\'1998-06-27 10:10:10\\'): 6201\\n               datediff(now(), \\'2014-01-02\\'): 533\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "37",
		"help_category_id": "31",
		"name": "TIMEDIFF",
		"description": "\\n  TIMEDIFF(expr1,expr2)\\n\\n  TIMEDIFF()返回起始时间expr1和结束时间expr2之间的时间。expr1和expr2为时间或日期时间表达式，两个的类型必须一样。\\n\\n  TIMEDIFF()返回结果限于时间值允许的范围。另外，你也可以使用TIMESTAMPDIFF()和UNIX_TIMESTAMP()函数，这两个函数的范围值为整数类型。\\n\\n",
		"example": "\\nmysql>  select timediff(now(), \\'2015-06-06 11:11:22\\'), timediff(\\'2015-06-06 12:12:12\\', \\'2014-06-05 11:11:11\\')\\\\G\\n*************************** 1. row ***************************\\n                timediff(now(), \\'2015-06-06 11:11:22\\'): 315:00:15\\ntimediff(\\'2015-06-06 12:12:12\\', \\'2014-06-05 11:11:11\\'): 838:59:59\\n1 row in set, 1 warning (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "38",
		"help_category_id": "31",
		"name": "TIMESTAMPDIFF",
		"description": "\\n\\n  返回日期或日期时间表达式datetime_expr1和datetime_expr2之间的整数差。其结果的单位由unit参数给出。unit的值为：MICROSECOND（mircoseconds）、SECOND、MINUTE、HOUR、DAY、WEEK、MONTH、QUARTER，以及YEAR。\\n\\n",
		"example": "\\nmysql> select timestampdiff(second,now(), \\'2011-01-01 11:11:11\\'),  timestampdiff(second, \\'2011-01-01 11:11:11\\', now())\\\\G\\n*************************** 1. row ***************************\\n timestampdiff(second,now(), \\'2011-01-01 11:11:11\\'): -140843995\\ntimestampdiff(second, \\'2011-01-01 11:11:11\\', now()): 140843995\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "39",
		"help_category_id": "31",
		"name": "PERIOD_DIFF",
		"description": "\\n  PERIOD_DIFF(p1,p2)\\n\\n  返回周期p1和p2之间的月数。p1和p2的格式应该为 YYMM或YYYYMM。注意周期参数p1和p2不是日期值。\\n\\n\\n",
		"example": "\\nmysql> select period_diff(20150702, 20790503), period_diff(150702, 790503);\\n+---------------------------------+-----------------------------+\\n| period_diff(20150702, 20790503) | period_diff(150702, 790503) |\\n+---------------------------------+-----------------------------+\\n|                          -76777 |                      -76777 |\\n+---------------------------------+-----------------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "40",
		"help_category_id": "32",
		"name": "CONCAT",
		"description": "\\n  CONCAT(str1,…,strN)\\n\\n  把一个或多个字符串连接成一个字符串。左右参数都必须是字符串类型或NULL，否则报错。如果执行成功，则返回连接后的字符串；参数中有一个值是NULL结果就是NULL。\\n\\n  说明：str参数可以是数值类型，隐式转换为字符串处理。\\n\\n",
		"example": "\\nmysql> select concat(\\'test\\'), concat(\\'test\\',\\'OceanBase\\'), concat(\\'test\\', \\'OceanBase\\', \\'1.0\\'), concat(\\'test\\',\\'OceanBase\\',\\'1.0\\', NULL)\\\\G\\n*************************** 1. row ***************************\\n                        concat(\\'test\\'): test\\n            concat(\\'test\\',\\'OceanBase\\'): testOceanBase\\n    concat(\\'test\\', \\'OceanBase\\', \\'1.0\\'): testOceanBase1.0\\nconcat(\\'test\\',\\'OceanBase\\',\\'1.0\\', NULL): NULL\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "41",
		"help_category_id": "32",
		"name": "SUBSTRING",
		"description": "\\n  SUBSTRING(str,pos),SUBSTRING(str FROM pos), SUBSTRING(str,pos,len), SUBSTRING(str FROM pos FOR len)\\n和SUBSTR同语义。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "42",
		"help_category_id": "32",
		"name": "SUBSTR",
		"description": "\\n  SUBSTR(str,pos,len)、SUBSTR(str,pos)、 SUBSTR(str FROM pos)和SUBSTR (str FROM pos FOR len) \\n\\n  返回一个子字符串，起始于位置pos，长度为len。使用FROM的格式为标准SQL语法。\\n\\n• str必须是字符串，pos和len必须是整数。任意参数为NULL，结果总为NULL。\\n\\n• str中的中文字符被当做字节流看待。\\n\\n• 不带有len参数的时，则返回的子字符串从pos位置开始到原字符串结尾。\\n\\n• pos值为负数时，pos的位置从字符串的结尾的字符数起；为零0时，可被看做1。\\n\\n• 当len小于等于0，或者pos指示的字符串位置不存在字符时，返回结果为空字符串。\\n\\n",
		"example": "\\nmysql> SELECT SUBSTR(\\'abcdefg\\',3), SUBSTR(\\'abcdefg\\',3,2), SUBSTR(\\'abcdefg\\',-3), SUBSTR(\\'abcdefg\\',3,-2), SUBSTR(\\'abcdefg\\' from -4 for 2)\\\\G\\n*************************** 1. row ***************************\\n            SUBSTR(\\'abcdefg\\',3): cdefg\\n          SUBSTR(\\'abcdefg\\',3,2): cd\\n           SUBSTR(\\'abcdefg\\',-3): efg\\n         SUBSTR(\\'abcdefg\\',3,-2): \\nSUBSTR(\\'abcdefg\\' from -4 for 2): de\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "43",
		"help_category_id": "32",
		"name": "TRIM",
		"description": "\\n  TRIM([[{BOTH | LEADING | TRAILING}] [remstr] FROM] str)\\n\\n  删除字符串所有前缀和（或）后缀。\\n\\n• remstr和str必须为字符串或NULL类型。当参数中有NULL时结果总为NULL。\\n\\n• 若未指定BOTH、LEADIN或TRAILING,则默认为BOTH。\\n\\n• remstr为可选项，在未指定情况下，删除空格。\\n\\n\\n\\n",
		"example": "\\nmysql> SELECT TRIM(\\' bar \\'),\\nTRIM(LEADING \\'x\\' FROM \\'xxxbarxxx\\'),\\nTRIM(BOTH \\'x\\' FROM \\'xxxbarxxx\\'),\\nTRIM(TRAILING \\'x\\' FROM \\'xxxbarxxx\\')\\\\G\\n*************************** 1. row ***************************\\nTRIM(\\' bar \\'): bar\\nTRIM(LEADING \\'x\\' FROM \\'xxxbarxxx\\'): barxxx\\nTRIM(BOTH \\'x\\' FROM \\'xxxbarxxx\\'): bar\\nTRIM(TRAILING \\'x\\' FROM \\'xxxbarxxx\\'): xxxbar\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "44",
		"help_category_id": "32",
		"name": "LENGTH",
		"description": "\\n\\n  返回字符串的长度，单位为字节。参数必须是字符串类型或NULL，否则报错。 如果执行成功，结果是INT型整数，表示字符串长度；当参数是NULL结果为NULL。\\n\\n  str参数为数值类型时，系统能隐式转换为字符串类型。\\n\\n",
		"example": "\\nmysql> SELECT LENGTH(\\'text\\');\\n+----------------+\\n| LENGTH(\\'text\\') |\\n+----------------+\\n|              4 |\\n+----------------+\\n1 row in set (0.00 sec)\\nmysql> select length(-1.23);\\n+---------------+\\n| length(-1.23) |\\n+---------------+\\n|             5 |\\n+---------------+\\n1 row in set (0.00 sec)\\nmysql> select length(1233e);\\nmysql> select length(1233e);\\nERROR 1054 (42S22): Unknown column \\'1233e\\' in \\'field list\\'\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "45",
		"help_category_id": "32",
		"name": "UPPER",
		"description": "\\n  UPPER(str)\\n\\n  将字符串转化为大写字母的字符。参数必须是字符串类型。若为NULL，结果总为NULL。\\n\\n  str参数为数值类型时，能隐式转换为字符串类型。\\n\\n  由于中文编码的字节区间与ASCII大小写字符不重合，对于中文，UPPER可以很好的兼容。\\n\\n",
		"example": "\\nmysql> SELECT UPPER(\\'OceanBase您好！\\');\\n+-----------------------------+\\n| UPPER(\\'OceanBase您好！\\')    |\\n+-----------------------------+\\n| OCEANBASE您好！             |\\n+-----------------------------+\\n1 row in set (0.00 sec)\\nmysql> select upper(e);\\nERROR 1054 (42S22): Unknown column \\'e\\' in \\'field list\\'\\nmysql> select upper(1.235.);\\nERROR 1064 (42000): You have an error in your SQL syntax; \\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "46",
		"help_category_id": "32",
		"name": "LOWER",
		"description": "\\n  LOWER(str)\\n\\n  将字符串转化为小写字母的字符。参数必须是字符串类型。若为NULL，结果总为NULL。\\n\\n  str参数为数值类型时，能隐式转换为字符串类型。\\n\\n  由于中文编码的字节区间与ASCII大小写字符不重合，对于中文，LOWER可以很好的兼容。\\n\\n",
		"example": "\\nmysql> SELECT LOWER(\\'OceanBase您好！\\');\\n+-----------------------------+\\n| LOWER(\\'OceanBase您好！\\')    |\\n+-----------------------------+\\n| oceanbase您好！             |\\n+-----------------------------+\\n1 row in set (0.00 sec)\\nmysql> select lower(1.23) ;\\n+-------------+\\n| lower(1.23) |\\n+-------------+\\n| 1.23        |\\n+-------------+\\n1 row in set (0.00 sec)\\nmysql> select lower(1.23h);\\nERROR 1583 (42000): Incorrect parameters in the call to native function \\'lower\\'\\nmysql> select lower(1.23e);\\nERROR 1582 (42000): Incorrect parameter count in the call to native function \\'lower\\'\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "47",
		"help_category_id": "32",
		"name": "HEX",
		"description": "\\n  HEX(str)\\n\\n  将字符串转化为十六进制数显示。输入为NULL时，输出也为NULL。\\n\\n  当str是数值时，输出整数的十六进制表示；\\n\\n  当str输入是字符串的时候，返回值为str的十六进制字符串表示， 其中每个str 里的每个字符被转化为两个十六进制数字。\\n\\n\\n",
		"example": "\\nmysql> SELECT HEX(255);\\n        -> \\'FF\\'\\nmysql> SELECT HEX(\\'abc\\');\\n        -> 616263\\nmysql> SELECT HEX(\\'OceanBase\\'),\\nHEX(123),\\nHEX(0x0123);\\n+--------------------+----------+-------------+\\n| HEX(\\'OceanBase\\')   | HEX(123) | HEX(0x0123) |\\n+--------------------+----------+-------------+\\n| 4F6365616E42617365 | 7B       | 0123        |\\n+--------------------+----------+-------------+\\n1 row in set (0.00 sec)\\nmysql> select hex(0x012);\\n+------------+\\n| hex(0x012) |\\n+------------+\\n| 0012       |\\n+------------+\\n1 row in set (0.00 sec)\\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "48",
		"help_category_id": "32",
		"name": "UNHEX",
		"description": "\\n  UNHEX(str)\\n\\n  HEX(str)的反向操作，即将参数中的每一对十六进制数字理解为一个数字，并将其转化为该数字代表的字符。结果字符以二进制字符串的形式返回。\\n\\n  str必须是字符串类型或NULL。当str是合法的十六进制值时将按照十六进制到字节流的转换算法进行，当str不是十六进制字符串的时候返回NULL。当str为NULL的时候输出是NULL。\\n\\n",
		"example": "\\nmysql> SELECT HEX(\\'OceanBase\\'),\\nUNHEX(\\'4f6365616e42617365\\'),\\nUNHEX(HEX(\\'OceanBase\\')),\\nUNHEX(NULL)\\\\G\\n*************************** 1. row ***************************\\nHEX(\\'OceanBase\\'): 4F6365616E42617365\\nUNHEX(\\'4f6365616e42617365\\'): OceanBase\\nUNHEX(HEX(\\'OceanBase\\')): OceanBase\\nUNHEX(NULL): NULL\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "49",
		"help_category_id": "32",
		"name": "INT2IP",
		"description": "\\n  INT2IP(int_value)\\n\\n  注：INT2IP为OceanBase特有函数。\\n\\n  将一个整数转换成IP地址。\\n\\n• 输入数据类型必须为INT。若输入为NULL，则输出为NULL。若输入的数字大于MAX_INT32或小于0则输出为NULL。\\n\\n\\n",
		"example": "\\nmysql> SELECT INT2IP(16777216),\\n    -> HEX(16777216),\\n    -> INT2IP(1);\\n+------------------+---------------+-----------+\\n| INT2IP(16777216) | HEX(16777216) | INT2IP(1) |\\n+------------------+---------------+-----------+\\n| 1.0.0.0          | 1000000       | 0.0.0.1   |\\n+------------------+---------------+-----------+\\n1 row in set (0.00 sec)",
		"url": URL
	},
	{
		"help_topic_id": "50",
		"help_category_id": "32",
		"name": "IP2INT",
		"description": "\\n  IP2INT(\\'ip_addr\\')\\n\\n  注：IP2INT为OceanBase特有函数。\\n\\n  将字符串表示的IP地址转换成整数内码表示。\\n\\n• 输入数据类型必须为字符串类型。若输入为NULL，则输出为NULL。若输入的IP地址不是一个正确的IP地址(包含非数字字符，每一个ip segment的数值大小超过256等），则输出为NULL。\\n\\n• 仅支持ipv4地址，暂不支持ipv6地址。\\n\\n",
		"example": "\\nmysql> SELECT IP2INT(\\'0.0.0.1\\'), \\nHEX(IP2INT(\\'0.0.0.1\\')),\\nHEX(IP2INT(\\'1.0.0.0\\')),\\nIP2INT(\\'1.0.0.257\\')\\\\G\\n*************************** 1. row ***************************\\nIP2INT(\\'0.0.0.1\\'): 1\\nHEX(IP2INT(\\'0.0.0.1\\')): 1\\nHEX(IP2INT(\\'1.0.0.0\\')): 1000000\\nIP2INT(\\'1.0.0.257\\'): NULL\\n1 row in set (0.01 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "51",
		"help_category_id": "32",
		"name": "LIKE",
		"description": "\\n  [NOT] LIKE str2 [ESCAPE str3]\\n\\n  字符串通配符匹配。左右参数都必须是字符串类型或NULL，否则报错。如果执行成功，结果是TRUE或者FALSE，或某一个参数是NULL结果就是NULL。\\n\\n  通配符包括“%”和“_”：\\n\\n• “%”表示匹配任何长度的任何字符，且匹配的字符可以不存在。\\n\\n• “_”表示只匹配单个字符，且匹配的字符必须存在。\\n\\n  如果你需要查找“a_c”，而不是“abc”时，可以使用OceanBase的转义字符“\\\\”，即可以表示为“a\\\\_c”。\\n\\n  ESCAPE用于定义转义符，即表示如果str2中包含str3，那么在匹配时，str3后的字符为普通字符处理，例如：LIKE \\'abc%\\' ESCAPE \\'c\\'，此时“c”为转义符，而“%”为普通字符，不再作为转义字符，本语句匹配的字符串为“ab%”。\\n\\n",
		"example": "\\nmysql> SELECT \\'ab%\\' LIKE \\'abc%\\' ESCAPE \\'c\\';\\n+------------------------------+\\n| \\'ab%\\' LIKE \\'abc%\\' ESCAPE \\'c\\' |\\n+------------------------------+\\n|                            1 |\\n+------------------------------+\\n1 row in set (0.00 sec)\\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "52",
		"help_category_id": "32",
		"name": "REGEXP",
		"description": "\\n  expr [NOT] REGEXP|RLIKE pat\\n\\n  执行字符串表达式 expr 和模式pat 的模式匹配。若expr 匹配 pat，则返回 1; 否则返回0。若 expr 或 pat 任意一个为 NULL, 则结果为 NULL。 RLIKE 是REGEXP的同义词。\\n\\n  expr和pat参数都必须为字符串或NULL，支持隐式转换成字符串类型。数据类型不匹配的，报错。PATTERN必须为合法的正则表达式，否则报错。\\n\\n\\n",
		"example": "\\nmysql> select 1234 regexp 1;\\n+---------------+\\n| 1234 regexp 1 |\\n+---------------+\\n|             1 |\\n+---------------+\\n1 row in set (0.00 sec)\\nmysql>  select \\'hello\\'  rlike \\'h%\\';\\n+---------------------+\\n| \\'hello\\'  rlike \\'h%\\' |\\n+---------------------+\\n|                   0 |\\n+---------------------+\\n1 row in set (0.00 sec) \\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "53",
		"help_category_id": "32",
		"name": "REPEAT",
		"description": "\\n  REPEAT(str, count)\\n\\n  返回一个由重复count次数的字符串str 组成的字符串。若 count <= 0,则返回一个空字符串。\\n\\n  若str 或 count 为 NULL，则返回 NULL\\n\\n  str为数值类型时，系统隐式转换为字符串类型。\\n\\n  count支持隐式转换成数值类型，如果转换失败，则相当于count为0。\\n\\n",
		"example": "\\nmysql> select repeat(\\'1\\',-1),  repeat(null,null),repeat(\\'test\\',4);\\n+----------------+-------------------+------------------+\\n| repeat(\\'1\\',-1) | repeat(null,null) | repeat(\\'test\\',4) |\\n+----------------+-------------------+------------------+\\n|                  | NULL               | testtesttesttest |\\n+----------------+-------------------+------------------+\\n1 row in set (0.00 sec)\\nmysql> select repeat(11111,\\'2\\');\\n+-------------------+\\n| repeat(11111,\\'2\\') |\\n+-------------------+\\n| 1111111111        |\\n+-------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "54",
		"help_category_id": "32",
		"name": "SUBSTRING_INDEX",
		"description": "\\n  SUBSTRING_INDEX(str, delim, count)\\n\\n  在定界符 delim 以及count 出现前，从字符串str返回字符串。若count为正值,则返回最终定界符(从左边开始)左边的一切内容。若count为负值，则返回定界符（从右边开始）右边的一切内容。任意一个参数为NULL，返回NULL；若str或delim为空字符串，则返回空串；若count=0,返回空串。\\n\\n  str, delim, count参数支持数值与字符串的隐式转换。\\n\\n\\n",
		"example": "\\nmysql> select substring_index(\\'abcdabc\\', \\'abc\\', 0), substring_index(\\'abcdabc\\', \\'abc\\', 1), substring_index(\\'abcdabc\\', \\'abc\\', 2), substring_index(\\'abcdabc\\', \\'abc\\', 3), substring_index(\\'abcdabc\\', \\'abc\\', -1), substring_index(\\'abcdabc\\', \\'abc\\', -2), substring_index(\\'abcdabc\\', \\'abc\\', -3)\\\\G\\n*************************** 1. row ***************************\\n substring_index(\\'abcdabc\\', \\'abc\\', 0):\\n substring_index(\\'abcdabc\\', \\'abc\\', 1):\\n substring_index(\\'abcdabc\\', \\'abc\\', 2): abcd\\n substring_index(\\'abcdabc\\', \\'abc\\', 3): abcdabc\\nsubstring_index(\\'abcdabc\\', \\'abc\\', -1):\\nsubstring_index(\\'abcdabc\\', \\'abc\\', -2): dabc\\nsubstring_index(\\'abcdabc\\', \\'abc\\', -3): abcdabc\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "55",
		"help_category_id": "32",
		"name": "LOCATE",
		"description": "\\n  LOCATE(substr,str) , LOCATE(substr,str,pos)\\n\\n  第一个语法返回字符串 str中子字符串substr的第一个出现位置。第二个语法返回字符串 str中子字符串substr的第一个出现位置, 起始位置在pos。如若substr 不在str中，则返回值为0。\\n\\n",
		"example": "\\nmysql> SELECT LOCATE(\\'bar\\', \\'foobarbar\\');\\n        -> 4\\nmysql> SELECT LOCATE(\\'xbar\\', \\'foobar\\');\\n        -> 0\\nmysql> SELECT LOCATE(\\'bar\\', \\'foobarbar\\',5);\\n        -> 7\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "56",
		"help_category_id": "32",
		"name": "INSTR",
		"description": "\\n  INSTR(str,substr)\\n\\n  返回字符串str中子字符串的第一个出现位置。这和LOCATE()的双参数形式相同，除非参数的顺序被颠倒。  \\n\\n",
		"example": "\\nmysql> SELECT INSTR(\\'foobarbar\\', \\'bar\\');\\n        -> 4\\nmysql> SELECT INSTR(\\'xbar\\', \\'foobar\\');\\n        -> 0\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "57",
		"help_category_id": "32",
		"name": "REPLACE()",
		"description": "\\n  REPLACE(str, from_str, to_str)\\n\\n  返回字符串str以及所有被字符to_str替代的字符串from_str。\\n\\n",
		"example": "\\nmysql> SELECT REPLACE(\\'abc.efg.gpg.nowdew.abc.dabc.e\\', \\'abc.\\', \\'www\\');\\n+---------------------------------------------------------+\\n| REPLACE(\\'abc.efg.gpg.nowdew.abc.dabc.e\\', \\'abc.\\', \\'www\\') |\\n+---------------------------------------------------------+\\n| wwwefg.gpg.nowdew.wwwdwwwe                              |\\n+---------------------------------------------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "58",
		"help_category_id": "32",
		"name": "FIELD",
		"description": "\\n  FIELD(str,str1,str2,str3,…)\\n\\n  返回参数str在str1,str2,str3,…列表中的索引位置（从1开始的位置）。在找不到str的情况下，返回值为0。\\n\\n  如果所有的对于FIELD()的参数均为字符串，则所有参数均按照字符串进行比较。如果所有的参数均为数字，则按照数字进行比较。否则，参数均按照double类型进行比较。\\n\\n  如果str为NULL，则返回值为0，原因是NULL不能同任何值进行同等比较。FILED()是ELT()的补数。\\n\\n",
		"example": "\\nmysql> select field(\\'abc\\',\\'abc1\\',\\'abc2\\',\\'abc\\',\\'abc4\\',\\'abc\\'), field(NULL, \\'null1\\', NULL);\\n+-----------------------------------------------+----------------------------+\\n| field(\\'abc\\',\\'abc1\\',\\'abc2\\',\\'abc\\',\\'abc4\\',\\'abc\\') | field(NULL, \\'null1\\', NULL) |\\n+-----------------------------------------------+----------------------------+\\n|                   3 |                          0 |\\n+-----------------------------------------------+----------------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "59",
		"help_category_id": "32",
		"name": "ELT",
		"description": "\\n  ELT(N, str1, str2, str3,…)\\n\\n  若N=1，则返回值为str1, 若N=2，则返回值为str2，以此类推。若N小于1或大于参数的数目，则返回值为NULL。ELT()是FIELD()的补数。\\n\\n",
		"example": "\\nmysql> select elt(3, \\'abc1\\', \\'abc2\\', \\'abc\\', \\'abc4\\', \\'abc\\'), elt(0, \\'null1\\', NULL);\\n+----------------------------------------------+-----------------------+\\n| elt(3, \\'abc1\\', \\'abc2\\', \\'abc\\', \\'abc4\\', \\'abc\\') | elt(0, \\'null1\\', NULL) |\\n+----------------------------------------------+-----------------------+\\n| abc                                          | NULL                  |\\n+----------------------------------------------+-----------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "60",
		"help_category_id": "34",
		"name": "CAST",
		"description": "\\n  CAST(expr AS type)\\n\\n  将某种数据类型的表达式显式转换为另一种数据类型。\\n\\n  将expr字段值转换为type数据类型。\\n\\n",
		"example": "\\nmysql> SELECT CAST(123 AS BOOL);\\n+-------------------+\\n| CAST(123 AS bool) |\\n+-------------------+\\n|                 1 |\\n+-------------------+\\n1 row in set (0.00 sec)\\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "61",
		"help_category_id": "33",
		"name": "AVG",
		"description": "\\n  AVG(([DISTINCT] expr)\\n\\n  返回指定组中的平均值，空值被忽略。DISTINCT选项可用于返回expr的不同值的平均值。若找不到匹配的行，则AVG()返回NULL。\\n\\n",
		"example": "\\nmysql> select * from oceanbasetest;\\n+----+------+------+\\n| id | ip   | ip2  |\\n+----+------+------+\\n|  1 |    4 | NULL |\\n|  3 |    3 | NULL |\\n|  4 |    3 | NULL |\\n+----+------+------+\\n3 rows in set (0.01 sec)\\nmysql> select avg(ip2), avg(ip), avg(distinct(ip)) from oceanbasetest;\\n+----------+---------+-------------------+\\n| avg(ip2) | avg(ip) | avg(distinct(ip)) |\\n+----------+---------+-------------------+\\n|     NULL |  3.3333 |            3.5000 |\\n+----------+---------+-------------------+\\n1 row in set (0.00 sec)\\nmysql> select avg(distinct(ip)),avg(ip),avg(ip2) from oceanbasetest;\\n+-------------------+---------+----------+\\n| avg(distinct(ip)) | avg(ip) | avg(ip2) |\\n+-------------------+---------+----------+\\n|            3.5000 |  3.3333 |     NULL |\\n+-------------------+---------+----------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "62",
		"help_category_id": "33",
		"name": "COUNT",
		"description": "\\n  COUNT([DISTINCT] expr)\\n\\n  COUNT([DISTINCT] expr)返回SELECT语句检索到的行中非NULL值的数目。若找不到匹配的行，则COUNT()返回0。DISTINCT选项可用于返回expr的不同值的数目。\\n\\n  COUNT(*)的稍微不同之处在于，它返回检索行的数目，不论其是否包含NULL值。\\n\\n\\n",
		"example": "\\nmysql> select * from oceanbasetest;\\n+----+------+------+\\n| id | ip   | ip2  |\\n+----+------+------+\\n|  1 |    4 | NULL |\\n|  3 |    3 | NULL |\\n|  4 |    3 | NULL |\\n+----+------+------+\\n3 rows in set (0.00 sec)\\nmysql> select count(ip2), count(ip), count(distinct(ip)), count(*) from oceanbasetest;\\n+------------+-----------+---------------------+----------+\\n| count(ip2) | count(ip) | count(distinct(ip)) | count(*) |\\n+------------+-----------+---------------------+----------+\\n|          0 |         3 |                   2 |        3 |\\n+------------+-----------+---------------------+----------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "63",
		"help_category_id": "33",
		"name": "MAX",
		"description": "\\n  MAX([DISTINCT] expr)\\n\\n  返回指定数据中的最大值。\\n\\n  MAX()的取值可以是一个字符串参数；在这些情况下，它们返回最大字符串值。DISTINCT关键字可以被用来查找expr 的不同值的最大值，这产生的结果与省略DISTINCT 的结果相同。\\n\\n",
		"example": "\\n假设表a有三行数据：id=1，num=10；id=2，num=20；id=3，num=30。\\nmysql> SELECT MAX(num) FROM a;\\n+-----------------+\\n| MAX(num)        |\\n+-----------------+\\n|              30 |\\n+-----------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "64",
		"help_category_id": "33",
		"name": "MIN",
		"description": "\\n  MIN([DISTINCT] expr)\\n\\n  返回指定数据中的最小值。\\n\\n  MIN()的取值可以是一个字符串参数；在这些情况下，它们返回最小字符串值。DISTINCT关键字可以被用来查找expr 的不同值的最小值，然而，这产生的结果与省略DISTINCT 的结果相同。\\n\\n",
		"example": "\\n假设表a有三行数据：id=1，num=10；id=2，num=20；id=3，num=30。\\nmysql> SELECT MIN(num) FROM a;\\n+----------------+\\n| MIN(num)       |\\n+----------------+\\n|             10 |\\n+----------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "65",
		"help_category_id": "33",
		"name": "SUM",
		"description": "\\n  SUM([DISTINCT] expr)\\n\\n  返回expr 的总数。若返回集合中无任何行，则SUM()返回NULL。DISTINCT关键字可用于求得expr不同值的总和。\\n\\n  若找不到匹配的行，则SUM()返回NULL。\\n\\n\\n",
		"example": "\\nmysql> select * from oceanbasetest;\\n+------+------+------+\\n| id   | ip   | ip2  |\\n+------+------+------+\\n|    1 |    4 | NULL |\\n|    3 |    3 | NULL |\\n|    4 |    3 | NULL |\\n+------+------+------+\\n3 rows in set (0.00 sec)\\nmysql> select sum(ip2),sum(ip),sum(distinct(ip)) from oceanbasetest;\\n+----------+---------+-------------------+\\n| sum(ip2) | sum(ip) | sum(distinct(ip)) |\\n+----------+---------+-------------------+\\n|     NULL |      10 |                 7 |\\n+----------+---------+-------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "66",
		"help_category_id": "33",
		"name": "GROUP_CONCAT",
		"description": "\\n  GROUP_CONCAT([DISTINCT] expr)\\n\\n  该函数返回带有来自一个组的连接的非NULL值的字符串结果。\\n\\n  语法: \\n\\n  GROUP_CONCAT([DISTINCT] expr [,expr ...]\\n  [ORDER BY {unsigned_integer | col_name | expr}\\n  [ASC | DESC] [,col_name ...]]\\n  [SEPARATOR str_val])\\n\\n\\n",
		"example": "\\nmysql> select * from book;     //表book（书编号，书名，出版社）+--------+--------------------------------+-----------------------------+\\n| bookid | bookname                       | publishname                 |\\n+--------+--------------------------------+-----------------------------+\\n|      1 | git help                       | alibaba group publisher     |\\n|      2 | MySQL性能优化                  | 浙江大学图文出版社          |\\n|      3 | JAVA编程指南                   | 机械工业出版社              |\\n|      3 | JAVA编程指南                   | 机械工业出版社              |\\n|      4 | 大规模分布式存储系统           | 机械工业出版社              |\\n+--------+--------------------------------+-----------------------------+\\n5 rows in set (0.00 sec)\\n//查找书名信息\\nmysql> select group_concat(bookname) from book group by bookname;\\n+-----------------------------------+\\n| group_concat(bookname)            |\\n+-----------------------------------+\\n| git help                          |\\n| JAVA编程指南,JAVA编程指南         |\\n| MySQL性能优化                     |\\n| 大规模分布式存储系统              |\\n+-----------------------------------+\\n4 rows in set (0.00 sec)\\n//查找书名信息，书名唯一\\nmysql> select group_concat(distinct(bookname)) from book group by bookname;\\n+----------------------------------+\\n| group_concat(distinct(bookname)) |\\n+----------------------------------+\\n| git help                         |\\n| JAVA编程指南                     |\\n| MySQL性能优化                    |\\n| 大规模分布式存储系统             |\\n+----------------------------------+\\n4 rows in set (0.01 sec)\\n//查找书名和出版社信息，以书名分组，出版社信息降序排序显示\\nmysql> select bookname, group_concat(publishname order by publishname desc separator  \\';\\' ) from book group by bookname;\\n+--------------------------------+---------------------------------------------------------------------+\\n| bookname                       | group_concat(publishname order by publishname desc separator  \\';\\' ) |\\n+--------------------------------+---------------------------------------------------------------------+\\n| git help                       | alibaba group publisher                                             |\\n| JAVA编程指南                   | 机械工业出版社;机械工业出版社                                       |\\n| MySQL性能优化                  | 浙江大学图文出版社                                                  |\\n| 大规模分布式存储系统           | 机械工业出版社                                                      |\\n+--------------------------------+---------------------------------------------------------------------+\\n4 rows in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "67",
		"help_category_id": "35",
		"name": "ROUND",
		"description": "\\n  ROUND(X), ROUND(X,D)\\n\\n  返回一个数值，四舍五入到指定的长度或精度。\\n\\n  返回参数X, 其值接近于最近似的整数。在有两个参数的情况下，返回X，其值保留到小数点后D位，而第D位的保留方式为四舍五入。若要接保留X值小数点左边的D位，可将D设为负值。 \\n\\n  返回值的类型同 第一个自变量相同(假设它是一个整数、双精度数或小数)。这意味着对于一个整数参数,结果也是一个整数(无小数部分)。\\n当第一个参数是十进制常数时，对于准确值参数，ROUND() 使用精密数学题库：\\n\\n  对于准确值数字,ROUND() 使用“四舍五入” 或“舍入成最接近的数” 的规则:对于一个分数部分为 .5或大于 .5的值，正数则上舍入到邻近的整数值，负数则下舍入临近的整数值。(换言之, 其舍入的方向是数轴上远离零的方向）。对于一个分数部分小于.5 的值，正数则下舍入下一个整数值，负数则下舍入邻近的整数值，而正数则上舍入邻近的整数值。\\n\\n  对于近似值数字， ROUND()遵循银行家规则“四舍--大于五入--五取最接近的偶数”的规则： 一个带有任何小数部分的值会被舍入成最接近的偶数整数。\\n\\n",
		"example": "\\nmysql> select round(2.15,2);\\n+---------------+\\n| round(2.15,2) |\\n+---------------+\\n|          2.15 |\\n+---------------+\\n1 row in set (0.00 sec) \\nmysql> select round(2555e-2,1);\\n+------------------+\\n| round(2555e-2,1) |\\n+------------------+\\n|             25.6 |\\nmysql> select round(25e-1), round(25.3e-1),round(35e-1);\\n+--------------+----------------+--------------+\\n| round(25e-1) | round(25.3e-1) | round(35e-1) |\\n+--------------+----------------+--------------+\\n|            2 |              3 |            4 |\\n+--------------+----------------+--------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "68",
		"help_category_id": "35",
		"name": "CEIL",
		"description": "\\n  CEIL(expr)\\n\\n  返回大于或者等于指定表达式的最小整数。\\n\\n  还支持比较运算，结果为BOOL值，被转化为数字类型处理，产生的结果为1(TRUE)、0 (FALSE)；\\n\\n  如果输入NULL，返回值为NULL。\\n\\n  如果输入纯数字的字符串，支持自动转换成数字类型。\\n\\n  返回值会被转化为一个BIGINT。\\n\\n\\n",
		"example": "\\nmysql> select ceil(1.2), ceil(-1.2), ceil(1+1.5), ceil(1=1),ceil(1<1),ceil(null);\\n+-----------+------------+-------------+-----------+-----------+------------+\\n| ceil(1.2) | ceil(-1.2) | ceil(1+1.5) | ceil(1=1) | ceil(1<1) | ceil(null) |\\n+-----------+------------+-------------+-----------+-----------+------------+\\n|         2 |         -1  |           3  |         1 |         0 |       NULL |\\n+-----------+------------+-------------+-----------+-----------+------------+\\n1 row in set (0.00 sec)\\nmysql> select ceil(name);\\nERROR 1166 (42703): Unkown column name \\'name\\'\\nmysql> select ceil(\\'2\\');\\n+-----------+\\n| ceil(\\'2\\') |\\n+-----------+\\n|         2 |\\n+-----------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "69",
		"help_category_id": "35",
		"name": "FLOOR",
		"description": "\\n  FLOOR(expr)\\n\\n  和CEIL(expr)函数功能类似，返回小于或者等于指定表达式的最大整数。\\n还支持比较运算，结果为BOOL值，被转化为数字类型处理，产生的结果为1(TRUE)、0 (FALSE)；\\n\\n  如果输入NULL，返回值为NULL。\\n\\n  如果输入纯数字的字符串，支持自动转换成数字类型。\\n\\n  返回值会被转化为一个BIGINT。\\n\\n",
		"example": "\\nmysql> select floor(1.2), floor(-1.2), floor(1+1.5), floor(1=1),floor(1<1),floor(null);\\n+------------+-------------+--------------+------------+------------+-------------+\\n| floor(1.2) | floor(-1.2) | floor(1+1.5) | floor(1=1) | floor(1<1) | floor(null) |\\n+------------+-------------+--------------+------------+------------+-------------+\\n|          1 |          -2 |            2 |          1 |          0 |        NULL |\\n+------------+-------------+--------------+------------+------------+-------------+\\n1 row in set (0.00 sec)\\nmysql> select floor(name);\\nERROR 1166 (42703): Unkown column name \\'name\\'\\nmysql> select floor(\\'2\\');\\n+------------+\\n| floor(\\'2\\') |\\n+------------+\\n|          2 |\\n+------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "70",
		"help_category_id": "35",
		"name": "ABS",
		"description": "\\n  ABS(expr)\\n\\n  绝对值函数，求表达式绝对值，函数返回值类型与数值表达式的数据类型相同。\\n\\n  还支持比较运算，结果为BOOL值，被转化为数字类型处理，产生的结果为1(TRUE)、0 (FALSE)；\\n\\n  如果输入NULL，返回值为NULL。\\n\\n  如果输入纯数字的字符串，支持自动转换成数字类型。\\n\\n  返回值会被转化为一个BIGINT。\\n\\n",
		"example": "\\nmysql> select abs(5),abs(-5.777),abs(0),abs(1/2),abs(1-5);\\n+--------+-------------+--------+----------+----------+\\n| abs(5) | abs(-5.777) | abs(0) | abs(1/2) | abs(1-5) |\\n+--------+-------------+--------+----------+----------+\\n|      5 |       5.777 |      0 |      0.5 |        4 |\\n+--------+-------------+--------+----------+----------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "71",
		"help_category_id": "35",
		"name": "NEG",
		"description": "\\n  NEG(expr)\\n\\n  求补函数，对操作数执行求补运算：用零减去操作数，然后结果返回操作数。\\n\\n  支持比较运算，结果为BOOL值，被转化为数字类型处理，产生的结果为1(TRUE)、0 (FALSE)，再对结果求补。\\n\\n",
		"example": "\\nmysql> select neg(1),neg(1+1),neg(2*3),neg(1=1),neg(5<1);\\n+--------+----------+----------+----------+----------+\\n| neg(1) | neg(1+1) | neg(2*3) | neg(1=1) | neg(5<1) |\\n+--------+----------+----------+----------+----------+\\n|     -1 |       -2 |       -6 |        0 |        1 |\\n+--------+----------+----------+----------+----------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "72",
		"help_category_id": "35",
		"name": "SIGN",
		"description": "\\n  SIGN(X)\\n\\n  SIGN(X)返回参数作为-1、 0或1的符号，该符号取决于X的值为负、零或正。\\n\\n  支持比较运算，结果为BOOL值，被转化为数字类型处理，产生的结果为1(TRUE)、0 (FALSE)；\\n\\n  如果输入NULL，返回值为NULL。\\n\\n  支持浮点数、十六进制数。\\n\\n",
		"example": "\\nmysql> SELECT SIGN(-32);\\n        -> -1\\nmysql> SELECT SIGN(0);\\n        -> 0\\nmysql> SELECT SIGN(234);\\n        -> 1\\nmysql> select sign(null),sign(false),sign(0x01);\\n+------------+-------------+------------+\\n| sign(null) | sign(false) | sign(0x01) |\\n+------------+-------------+------------+\\n|       NULL |           0 |          1 |\\n+------------+-------------+------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "73",
		"help_category_id": "35",
		"name": "CONV",
		"description": "\\n  CONV(N, from_base, to_base)\\n\\n  不同数基间转换数字。返回值为一个字符串，由from_base基转化为to_base基。输入参数N可以是一个整数或字符串。最小基数为2，而最大基数则为36。如果to_base是一个负数，则N被看作一个带符号数。否则，N被看作无符号数。from_base如果是负数，则被当作整数处理，符号被忽略。N参数仅支持int类型和字符串类型输入；from_base和to_base参数仅支持十进制int类型输入，且取值范围为[-36,-2]U[2,36]。\\n\\n  非法输入将导致报错，其中非法输入包括以下情况：\\n\\n• from_base或者to_base不是一个合法的十进制int类型输入；\\n\\n• from_base或者to_base超出[-36,-2]U[2,36]的取值范围；\\n\\n• N不是一个合法的数字表示，例如取值超出0～9，a～z，A～Z的字符范围；\\n\\n• N超出了from_base基的取值范围，例如from_base为2，而N取值为3；\\n\\n• N的取值超出了int64的最大表示范围，即[-9223372036854775807, 9223372036854775807]。\\n\\n\\n",
		"example": "\\nmysql> select conv(9223372036854775807,10,2);\\n+-----------------------------------------------------------------+\\n| conv(9223372036854775807,10,2)                                  |\\n+-----------------------------------------------------------------+\\n| 111111111111111111111111111111111111111111111111111111111111111 |\\n+-----------------------------------------------------------------+\\n1 row in set (0.00 sec)\\nmysql> select conv(\\'-acc\\',21,-7);\\n+--------------------+\\n| conv(\\'-acc\\',21,-7) |\\n+--------------------+\\n| -16425             |\\n+--------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "74",
		"help_category_id": "35",
		"name": "MOD",
		"description": "\\n  MOD(N,M)\\n\\n  取余函数。MOD（N，M）， N % M， N MOD M 三种形式是等效的。\\n\\n  MOD（）对于带有小数部分的数值也起作用，它返回除法运算后的精确余数。\\n\\n  N,M中任何一个参数为NULL，返回值都为NULL。M为0是，也返回NULL。\\n\\n",
		"example": "\\nmysql> select mod(29,19), 29 mod 19, 29 % 19;\\n+------------+-----------+---------+\\n| mod(29,19) | 29 mod 19 | 29 % 19 |\\n+------------+-----------+---------+\\n|         10 |        10 |      10 |\\n+------------+-----------+---------+\\n1 row in set (0.00 sec)\\n\\nmysql> select mod(19.5, 29);\\n+---------------+\\n| mod(19.5, 29) |\\n+---------------+\\n|          19.5 |\\n+---------------+\\n1 row in set (0.00 sec)\\n\\nmysql> select mod(29, null);\\n+---------------+\\n| mod(29, null) |\\n+---------------+\\n|          NULL |\\n+---------------+\\n1 row in set (0.00 sec)\\n\\nmysql> select mod(100,0);\\n+------------+\\n| mod(100,0) |\\n+------------+\\n|       NULL |\\n+------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "75",
		"help_category_id": "37",
		"name": "GREATEST",
		"description": "\\n  GREATEST(value1, ...)\\n\\n  返回参数的最大值，和函数LEAST()相对。\\n\\n  参数至少为一个；如果参数中有NULL, 返回值为NULL。\\n\\n  当参数中同时存在数值和字符时，把字符隐式转换为数值类型处理，不能转换的报错。\\n\\n",
		"example": "\\nmysql> select greatest(2), greatest(\\'2\\',1,0), greatest(\\'a\\',\\'b\\',\\'c\\'), greatest(\\'a\\', NULL, \\'c\\'), greatest(\\'2014-05-15\\',\\'2014-06-01\\')\\\\G\\n*************************** 1. row ***************************\\n                        greatest(2): 2\\n                  greatest(\\'2\\',1,0): 2\\n              greatest(\\'a\\',\\'b\\',\\'c\\'): c\\n           greatest(\\'a\\', NULL, \\'c\\'): NULL\\ngreatest(\\'2014-05-15\\',\\'2014-06-01\\'): 2014-06-01\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "76",
		"help_category_id": "37",
		"name": "LEAST",
		"description": "\\n  LEAST(value1, ...)\\n\\n  返回参数的最小值，和函数GREATEST()相对。\\n\\n  参数至少为一个；如果参数中有NULL, 返回值为NULL。\\n\\n  当参数中同时存在数值和字符时，把字符隐式转换为数值类型处理，不能转换的报错。\\n\\n",
		"example": "\\nmysql> select least(2), least(\\'2\\',4,9), least(\\'a\\',\\'b\\',\\'c\\'), least(\\'a\\',NULL,\\'c\\'), least(\\'2014-05-15\\',\\'2014-06-01\\')\\\\G\\n*************************** 1. row ***************************\\n                        least(2): 2\\n                  least(\\'2\\',4,9): 2\\n              least(\\'a\\',\\'b\\',\\'c\\'): a\\n             least(\\'a\\',NULL,\\'c\\'): NULL\\nleast(\\'2014-05-15\\',\\'2014-06-01\\'): 2014-05-15\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "77",
		"help_category_id": "37",
		"name": "ISNULL",
		"description": "\\n  ISNULL(expr)\\n\\n  如果参数expr 为NULL，那么ISNULL()的返回值为1，否则范围值为0。\\n\\n  ISNULL()函数可以用来替代针对NULL的等值（=）比较。（使用=的NULL值比较通常都是错误的。）\\n\\n  ISNULL()函数同IS NULL比较操作符具有一些相同的特性。\\n\\n",
		"example": "\\nmysql> SELECT ISNULL(null), ISNULL(\\'test\\'), ISNULL(123.456), ISNULL(\\'10:00\\');\\n+--------------+----------------+-----------------+-----------------+\\n| ISNULL(null) | ISNULL(\\'test\\') | ISNULL(123.456) | ISNULL(\\'10:00\\') |\\n+--------------+----------------+-----------------+-----------------+\\n|            1 |              0 |               0 |               0 |\\n+--------------+----------------+-----------------+-----------------+\\n1 row in set (0.01 sec)\\nmysql> SELECT ISNULL(null+1);\\n+----------------+\\n| ISNULL(null+1) |\\n+----------------+\\n|              1 |\\n+----------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "78",
		"help_category_id": "38",
		"name": "CASE",
		"description": "\\n  格式\\n\\n CASE value WHEN [compare-value] THEN result [WHEN [compare-value] THEN result ...] [ELSE result] END \\n\\nOR:\\n\\n CASE WHEN [condition] THEN result [WHEN [condition] THEN result ...] [ELSE result] END \\n\\n  在第一个方案的返回结果中，value=compare-value。而第二个方案的返回结果是第一种条件为真。如果没有匹配的结果值，则返回结果为ELSE后的结果，如果没有ELSE部分，则返回值为NULL。\\n\\n",
		"example": "\\nmysql> select CASE \\'b\\' when \\'a\\' then 1 when \\'b\\' then 2 END;\\n +----------------------------------------------+\\n | CASE \\'b\\' when \\'a\\' then 1 when \\'b\\' then 2 END |\\n +----------------------------------------------+\\n |                                            2 |\\n +----------------------------------------------+\\n 1 row in set (0.00 sec)\\nmysql> select CASE concat(\\'a\\',\\'b\\') when concat(\\'ab\\',) then \\'a\\' when \\'b\\' then \\'b\\' end; +--------------------------------------------------------------------\\n| CASE concat(\\'a\\',\\'b\\') when concat(\\'ab\\',) then \\'a\\' when \\'b\\' then \\'b\\' end| +--------------------------------------------------------------------\\n| a                                                                   +--------------------------------------------------------------------\\n 1 row in set (0.00 sec)\\nmysql> select case when 1>0 then \\'true\\' else \\'false\\' end;\\n+--------------------------------------------+\\n| case when 1>0 then \\'true\\' else \\'false\\' end |\\n+--------------------------------------------+\\n| true                                       |\\n+--------------------------------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "79",
		"help_category_id": "38",
		"name": "IF",
		"description": "\\n  IF(expr1,expr2,expr3)\\n\\n  如果expr1的值为TRUE（即：expr1<>0 且 expr1<>NULL），返回结果为expr2;否则返回结果为expr3。\\n\\n  IF()返回结果可以是数值或字符串类型，它取决于使用的内容。\\n\\n",
		"example": "\\nmysql> select if(5>6, \\'T\\',\\'F\\'), if (5>6, 1, 0), if(null, \\'True\\', \\'False\\'), if(0, \\'True\\', \\'False\\')\\\\G\\n*************************** 1. row ***************************\\n         if(5>6, \\'T\\',\\'F\\'): F\\n           if (5>6, 1, 0): 0\\nif(null, \\'True\\', \\'False\\'): False\\n   if(0, \\'True\\', \\'False\\'): False\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "80",
		"help_category_id": "38",
		"name": "IFNULL",
		"description": "\\n  IFNULL(expr1, expr2)\\n\\n  假设expr1不为 NULL，则IFNULL()的返回值为expr1;否则其返回值为expr2.IFNULL()的返回值是数值或字符串，具体情况取决于其所使用的语境。\\n\\n",
		"example": "\\nmysql> SELECT IFNULL(\\'abc\\', null), IFNULL(NULL+1, NULL+2), IFNULL(1/0, 0/1);\\n+---------------------+------------------------+------------------+\\n| IFNULL(\\'abc\\', null) | IFNULL(NULL+1, NULL+2) | IFNULL(1/0, 0/1) |\\n+---------------------+------------------------+------------------+\\n| abc                 |                   NULL |           0.0000 |\\n+---------------------+------------------------+------------------+\\n1 row in set (0.01 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "81",
		"help_category_id": "38",
		"name": "NULLIF",
		"description": "\\n  NULLIF(expr1, expr2)\\n\\n  如果expr1=expr2成立，那么返回值为NULL，否则返回值为expr1。这和CASE WHEN expr1=expr2 THEN NULL ELSE expr1 END相同。\\n\\n  注意，如果参数不相等，则两次求得的值为expr1。\\n\\n",
		"example": "\\nmysql> SELECT NULLIF(\\'ABC\\', 123), NULLIF(\\'123\\',123), NULLIF(NULL, \\'abc\\');\\n+--------------------+-------------------+---------------------+\\n| NULLIF(\\'ABC\\', 123) | NULLIF(\\'123\\',123) | NULLIF(NULL, \\'abc\\') |\\n+--------------------+-------------------+---------------------+\\n| ABC                | NULL              | NULL                |\\n+--------------------+-------------------+---------------------+\\n1 row in set, 1 warning (0.01 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "82",
		"help_category_id": "39",
		"name": "FOUND_ROWS",
		"description": "\\n  FOUND_ROWS()\\n\\n  一个SELECT语句可能包含一个LIMIT子句，用来限制数据库服务器端返回客户端的行数。在某些情况下，我们需要不再次运行该语句而得知在没有LIMIT时到底该语句返回了多少行。我们可以在SELECT语句中选择使用SQL_CALC_FOUND_ROWS, 然后调用FOUND_ROW()函数，获取该语句在没有LIMIT时返回的行数。\\n\\nmysql> SELECT SQL_CALC_FOUND_ROWS * FROM tbl_name\\n    -> WHERE id > 100 LIMIT 10;\\nmysql> SELECT FOUND_ROWS();\\n\\n  第二个SELECT语句返回一个数字，表示在没有LIMIT子句的情况下，第一个SELECT语句返回了多少行。（若上述的SELECT语句在不使用SQL_CALC_FOUND_ROWS选项时，使用LIMIT和不使用LIMIT时候， FOUND_ROWS()可能会返回不同的结果）。\\n\\n  通过FOUND_ROWS()函数返回的有效行数是瞬时的，并且不能越过SELECT SQL_CALC_FOUND_ROWS语句后面的语句。如果你后续还需要用到这个值，就需要将其保存。如：\\n\\nmysql> SELECT SQL_CALC_FOUND_ROWS * FROM ... ;\\nmysql> SET @rows = FOUND_ROWS();\\n\\n  假如你正在使用SQL_CALC_FOUND_ROWS，系统必须计算出在全部结果集合中有多少行。尽管如此，这也还是比不用LIMIT而再次运行查询要快，原因是结果集合不需要被发送到客户端。\\n\\n  SQL_CALC_FOUND_ROWS和FOUND_ROWS()在当你希望限制一个查询返回的行数时是很有用的，同时还能不需要再次运行查询就可以确定全部结果集合中的行数。一个例子就是提供页式显示的Web脚本，该显示包含显示搜索结果其他部分的页的链接。使用FOUND_ROWS()使你确定剩下的结果需要多少其他的页。\\n\\n  SQL_CALC_FOUND_ROWS 和 FOUND_ROWS() 的应用对于UNION 查询比对于简单SELECT 语句更为复杂，原因是在UNION 中，LIMIT 可能会出现在多个位置。它可能适用于UNION中的独立的SELECT语句，或是整个的UNION 结果。 \\nSQL_CALC_FOUND_ROWS对于 UNION的期望结果是它返回在没有全局的LIMIT的条件下而应返回的行数。SQL_CALC_FOUND_ROWS 和UNION 一同使用的条件是：\\n\\n  SQL_CALC_FOUND_ROWS 关键词必须出现在UNION的第一个 SELECT中。 \\n\\n  FOUND_ROWS()的值只有在使用 UNION ALL时才是精确的。若使用不带ALL的UNION，则会发生两次删除，而FOUND_ROWS() 的指只需近似的。\\n\\n  假若UNION 中没有出现LIMIT ，则SQL_CALC_FOUND_ROWS 被忽略，返回临时表中的创建的用来处理UNION的行数。 \\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "83",
		"help_category_id": "39",
		"name": "LAST_INSERT_ID",
		"description": "\\n  LAST_INSERT_ID()\\n\\n  返回本session最后一次插入的自增字段值，如最近一条insert插入多条记录，LAST_INSERT_ID()返回第一条记录的自增字段值。\\n\\n",
		"example": "\\nmysql>select LAST_INSERT_ID();\\n+------------------+\\n| LAST_INSERT_ID() |\\n+------------------+\\n|                5 |\\n+------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "84",
		"help_category_id": "36",
		"name": "COALESCE",
		"description": "\\n  COALESCE(expr, expr, expr,...)\\n\\n  依次参考各参数表达式，遇到非NULL值即停止并返回该值。如果所有的表达式都是空值，最终将返回一个空值。\\n\\n  所有表达式必须是相同类型，或者可以隐性转换为相同的类型。 \\n\\n  如果expr2,expr3中只有一个明确是NULL，则IF()函数的结果类型为非NULL表达式的结果类型。\\n\\n  假如expr2 和expr3 都是字符串，且其中任何一个字符串区分大小写，则返回结果是区分大小写。\\n\\n",
		"example": "\\nmysql> SELECT COALESCE(NULL,NULL,3,4,5), COALESCE(NULL,NULL,NULL);\\n  +---------------------------+--------------------------+\\n  | COALESCE(NULL,NULL,3,4,5) | COALESCE(NULL,NULL,NULL) |\\n  +---------------------------+--------------------------+\\n  |                         3 |                     NULL |\\n  +---------------------------+--------------------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "85",
		"help_category_id": "36",
		"name": "NVL",
		"description": "\\n  NVL(str1,replace_with)\\n\\n  如果str1为NULL，则替换成replace_with。\\n\\n  任何时候给它一个空值，它都返回一个你所选择的值。这种能够自动替换空值的能力有助于提供看上去更为完善的输出。其中str1一般是一个列名。replace_with可以是任何值：直接值（即硬编码）、对其他列的引用或者表达式。\\n\\n",
		"example": "\\nmysql> SELECT NVL(NULL, 0), NVL(NULL, \\'a\\');\\n+--------------+----------------+\\n| NVL(NULL, 0) | NVL(NULL, \\'a\\') |\\n+--------------+----------------+\\n|            0 | a              |\\n+--------------+----------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "86",
		"help_category_id": "41",
		"name": "Logical Operators",
		"description": "\\n  在OceanBase中，逻辑操作符会把左右操作数都转成BOOL类型进行运算。逻辑运算时返回“Error”表示计算错误。\\n\\n  OceanBase各数据类型转换BOOL类型的规则如下：\\n\\n• 字符串只有是“True”、“False”、“1”和“0”才能够转换到BOOL类型，其中字符串“True”和“1”为“True”，字符串“False”和“0”为“False”。\\n\\n• “INT”、“FLOAT”、“DOUBLE”和“DECIMAL”转换BOOL类型时，数值不为零时为“True”，数值为零时为“False”。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "87",
		"help_category_id": "41",
		"name": "NOT !",
		"description": "\\n  逻辑非\\n\\n",
		"example": "\\nmysql> SELECT NOT 0, NOT 1, NOT NULL;\\n+-------+-------+----------+\\n| NOT 0 | NOT 1 | NOT NULL |\\n+-------+-------+----------+\\n|     1 |     0 |     NULL |\\n+-------+-------+----------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "88",
		"help_category_id": "41",
		"name": "AND &&",
		"description": "\\n  逻辑与\\n\\n",
		"example": "\\nmysql> SELECT (0 AND 0), (0 AND 1), (1 AND 1), (1 AND NULL);\\n+-----------+-----------+-----------+--------------+\\n| (0 AND 0) | (0 AND 1) | (1 AND 1) | (1 AND NULL) |\\n+-----------+-----------+-----------+--------------+\\n|         0 |         0 |         1 |         NULL |\\n+-----------+-----------+-----------+--------------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "89",
		"help_category_id": "41",
		"name": "OR ||",
		"description": "\\n  逻辑或\\n\\n",
		"example": "\\nmysql> SELECT (0 OR 0), (0 OR 1), (1 OR 1), (1 AND NULL);\\n+----------+----------+----------+--------------+\\n| (0 OR 0) | (0 OR 1) | (1 OR 1) | (1 AND NULL) |\\n+----------+----------+----------+--------------+\\n|        0 |        1 |        1 |         NULL |\\n+----------+----------+----------+--------------+\\n1 row in set (0.01 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "90",
		"help_category_id": "41",
		"name": "XOR",
		"description": "\\n  逻辑异或。\\n\\n  当任意一个操作数为NULL时，返回值为NULL。对于 非NULL的操作数，假如一个奇数操作数为非零值，则计算所得结果为1，否则为0。\\n\\n",
		"example": "\\nmysql> SELECT 1 XOR TRUE, 1 XOR 1, 1 XOR 2, 1 XOR NULL, 1 XOR 1 XOR 1;\\n+------------+---------+---------+------------+---------------+\\n| 1 XOR TRUE | 1 XOR 1 | 1 XOR 2 | 1 XOR NULL | 1 XOR 1 XOR 1 |\\n+------------+---------+---------+------------+---------------+\\n|          0 |       0 |       0 |       NULL |             1 |\\n+------------+---------+---------+------------+---------------+\\n1 row in set (0.01 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "91",
		"help_category_id": "43",
		"name": "Arithmetic Operators",
		"description": "\\n  OceanBase中，数值计算只允许在数值类型和VARCHAR直接进行，其它类型直接报错。字符串在做算术运算时，如果无法转成DOUBLE类型则报错，比如“\\'3.4he\\' + 3”。字符串只有在内容全为数字或者开头是“+”或者“-”，且后面跟数字的形式才能转成DOUBLE型\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "92",
		"help_category_id": "43",
		"name": "+",
		"description": "\\n  加法\\n\\n",
		"example": "\\nmysql> SELECT 2+3;\\n+-----+\\n| 2+3 |\\n+-----+\\n|   5 |\\n+-----+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "93",
		"help_category_id": "43",
		"name": "-",
		"description": "\\n  减法\\n\\n",
		"example": "\\nmysql> SELECT 2-3;\\n+-----+\\n| 2-3 |\\n+-----+\\n|  -1 |\\n+-----+\\n1 row in set (0.01 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "94",
		"help_category_id": "43",
		"name": "*",
		"description": "\\n  乘法\\n\\n",
		"example": "\\nmysql> SELECT 2*3;\\n+-----+\\n| 2*3 |\\n+-----+\\n|   6 |\\n+-----+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "95",
		"help_category_id": "43",
		"name": "/",
		"description": "\\n  除法，返回商。如果除数为“0”，则返回结果为“NULL”。\\n\\n",
		"example": "\\nmysql> SELECT 2/3;\\n+--------+\\n| 2/3    |\\n+--------+\\n| 0.6667 |\\n+--------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "96",
		"help_category_id": "43",
		"name": "%",
		"description": "\\n  除法，返回余数。如果除数为“0”，则返回结果为“NULL”\\n\\n",
		"example": "\\nmysql> SELECT 2%3, 2 MOD 3;\\n+------+---------+\\n| 2%3  | 2 MOD 3 |\\n+------+---------+\\n|    2 |       2 |\\n+------+---------+\\n1 row in set (0.01 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "97",
		"help_category_id": "43",
		"name": "^",
		"description": "\\n  返回指定数值的指定幂运算结果。\\n\\n",
		"example": "\\nmysql> SELECT 2^2;\\n+-----+\\n| 2^2 |\\n+-----+\\n|   0 |\\n+-----+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "98",
		"help_category_id": "42",
		"name": "Comparison Operators",
		"description": "\\n  比较运算符用于比较两个数值的大小。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "99",
		"help_category_id": "42",
		"name": "=",
		"description": "\\n  等于\\n\\n",
		"example": "\\nmysql> SELECT 1=0, 1=1, 1=NULL;\\n+-----+-----+--------+\\n| 1=0 | 1=1 | 1=NULL |\\n+-----+-----+--------+\\n|   0 |   1 |   NULL |\\n+-----+-----+--------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "100",
		"help_category_id": "42",
		"name": ">=",
		"description": "\\n  大于等于\\n\\n",
		"example": "\\nmysql> SELECT 1>=0, 1>=1, 1>=2, 1>=NULL;\\n+------+------+------+---------+\\n| 1>=0 | 1>=1 | 1>=2 | 1>=NULL |\\n+------+------+------+---------+\\n|    1 |    1 |    0 |    NULL |\\n+------+------+------+---------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "101",
		"help_category_id": "42",
		"name": ">",
		"description": "\\n  大于\\n\\n",
		"example": "\\nmysql> SELECT 1>0, 1>1, 1>2, 1>NULL;\\n+-----+-----+-----+--------+\\n| 1>0 | 1>1 | 1>2 | 1>NULL |\\n+-----+-----+-----+--------+\\n|   1 |   0 |   0 |   NULL |\\n+-----+-----+-----+--------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "102",
		"help_category_id": "42",
		"name": "<=",
		"description": "\\n  小于等于\\n\\n",
		"example": "\\nmysql> SELECT 1<=0, 1<=1, 1<=2, 1<=NULL;\\n+------+------+------+---------+\\n| 1<=0 | 1<=1 | 1<=2 | 1<=NULL |\\n+------+------+------+---------+\\n|    0 |    1 |    1 |    NULL |\\n+------+------+------+---------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "103",
		"help_category_id": "42",
		"name": "<",
		"description": "\\n  小于\\n\\n",
		"example": "\\nmysql> SELECT 1<0, 1<1, 1<2, 1<NULL;\\n+-----+-----+-----+--------+\\n| 1<0 | 1<1 | 1<2 | 1<NULL |\\n+-----+-----+-----+--------+\\n|   0 |   0 |   1 |   NULL |\\n+-----+-----+-----+--------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "104",
		"help_category_id": "42",
		"name": "!=或<>",
		"description": "\\n  不等于\\n\\n",
		"example": "\\nmysql> select 1!=0, 1!=1, 1<>0, 1<>1,1!=NULL, 1<>NULL;\\n+------+------+------+------+---------+---------+\\n| 1!=0 | 1!=1 | 1<>0 | 1<>1 | 1!=NULL | 1<>NULL |\\n+------+------+------+------+---------+---------+\\n|    1 |    0 |    1 |    0 |    NULL |    NULL |\\n+------+------+------+------+---------+---------+\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "105",
		"help_category_id": "42",
		"name": "BETWEEN … AND …",
		"description": "\\n  [NOT] BETWEEN … AND …\\n\\n  判断是否存在或者不存在于指定范围。\\n\\n",
		"example": "\\nmysql> SELECT 2 BETWEEN 1 AND 2,\\n3 NOT BETWEEN 1 AND 2,\\n1 BETWEEN null AND 0,\\n1 NOT BETWEEN null AND 0\\\\G\\n*************************** 1. row ***************************\\n2 BETWEEN 1 AND 2: 1\\n3 NOT BETWEEN 1 AND 2: 1\\n1 BETWEEN null AND 0: 0\\n1 NOT BETWEEN null AND 0: 1\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "106",
		"help_category_id": "42",
		"name": "IN",
		"description": "\\n  [NOT] IN\\n\\n  判断是否存在于指定集合。\\n\\n",
		"example": "\\nmysql> SELECT 2 IN (1, 2), 3 IN (1, 2)\\\\G\\n*************************** 1. row ***************************\\n2 IN (1, 2): 1\\n3 IN (1, 2): 0\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "107",
		"help_category_id": "42",
		"name": "IS [NOT] NULL | TRUE | FALSE | UNKNOWN",
		"description": "\\n  IS [NOT] NULL | TRUE | FALSE | UNKNOWN\\n\\n  判断是否为NULL、真、假或未知。如果执行成功，结果是TRUE或者FALSE，不会出现NULL。\\n\\n  注意：左参数必须是BOOL类型，或者是NULL，否则报错\\n\\n",
		"example": "\\nNULL IS NULL, NULL IS TRUE,\\n(0>1) IS FALSE, \\nNULL IS UNKNOWN,\\n0 IS NOT NULL, \\nNULL IS NOT NULL,\\nNULL IS NOT TRUE,\\n(0>1) IS NOT FALSE,\\nNULL IS NOT UNKNOWN\\\\G\\n*************************** 1. row ***************************\\n          0 IS NULL: 0\\n       NULL IS NULL: 1\\n       NULL IS TRUE: 0\\n     (0>1) IS FALSE: 1\\n    NULL IS UNKNOWN: 1\\n      0 IS NOT NULL: 1\\n   NULL IS NOT NULL: 0\\n   NULL IS NOT TRUE: 1\\n (0>1) IS NOT FALSE: 0\\nNULL IS NOT UNKNOWN: 0\\n1 row in set (0.00 sec)\\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "108",
		"help_category_id": "44",
		"name": "Vector Comparison Operators",
		"description": "\\n  向量比较运算符对两个向量（ROW）进行比较，支持“<”、“ >”、“=”、“<=”、“>=”、“!=”、 “<=>”、“in”和“not in”等8个操作符。这几个操作符都是二元操作符，被比较的两个向量的维度要求相同。\\n“<=>”表示NULL-safe equal.这个操作符和=操作符执行相同的比较操作，不过在两个操作码均为NULL时，其所得值为1而不为NULL，而当一个操作码为NULL时，其所得值为0而不为NULL。\\n\\n  表达式（1，2）和ROW（1，2）有时被称为行构造符。两者是等同的，在其它的语境中，也是合法的。例如，以下两个语句在语义上是等同的（但是目前只有第二个语句可以被优化）：\\n\\n  SELECT * FROM t1 WHERE (column1, column2) = (1, 1);\\n  SELECT * FROM t1 WHERE column1 = 1 AND column2 = 1;\\n  向量比较操作符和普通操作符相比主要有以下不同：\\n\\n• 如果两个操作数的第i个标量的比较就决定了比较结果，则不再继续比较后续的数值。\\n\\n  向量比较操作符需要注意以下几点：\\n\\n• 多元向量比较，关键字ROW可以省略。例如，ROW(1,2,3) < ROW(1,3,5)等价于(1,2,3) < (1,3,4)。\\n\\n• in/not in操作一定是向量操作，表示左参数（不）在右集合内，集合用“（）”括起。例如，Row(1) in (Row(2), Row(3), Row(1))，等价与1 in (2, 3, 1)。\\n\\n• in/not in操作需要左右操作数对应位置的标量都是可以比较的，否则返回错误。例如，ROW(1,2) in (ROW(1,2), ROW(2,3), ROW(3,4))成功，ROW(1,2) in (ROW(2,1), ROW(2,3), ROW(1,3,4))失败。\\n\\n\\n",
		"example": "\\nmysql> SELECT ROW(1,2) < ROW(1, 3),\\n    -> ROW(1,2,10) < ROW(1, 3, 0),\\n    -> ROW(1,null) < ROW(1,0),\\n    -> ROW(null, 1) < ROW(null, 2),\\n    -> ROW(1,2) in (ROW(1,2), ROW(2,3), ROW(3,4), ROW(4,5)),\\n    -> 1 in (1,2,3),\\n    -> 1 not in (2,3,4),\\n    -> ROW(1,2) not in (ROW(2,1),ROW(2,3), ROW(3,4)),\\n    -> NULL = NULL,\\n    -> NULL <=> NULL,\\n    -> NULL <=> 1,\\n    -> 1 <=> 0 \\\\G\\n*************************** 1. row ***************************\\n                                ROW(1,2) < ROW(1, 3): 1\\n                          ROW(1,2,10) < ROW(1, 3, 0): 1\\n                              ROW(1,null) < ROW(1,0): NULL\\n                         ROW(null, 1) < ROW(null, 2): NULL\\nROW(1,2) in (ROW(1,2), ROW(2,3), ROW(3,4), ROW(4,5)): 1\\n                                        1 in (1,2,3): 1\\n                                    1 not in (2,3,4): 1\\n       ROW(1,2) not in (ROW(2,1),ROW(2,3), ROW(3,4)): 1\\n                                         NULL = NULL: NULL\\n                                       NULL <=> NULL: 1\\n                                          NULL <=> 1: 0\\n                                             1 <=> 0: 0\\n1 row in set (0.00 sec)\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "109",
		"help_category_id": "47",
		"name": "Bit Operators",
		"description": "\\n  对于比特运算，OceanBase使用BITINT（64比特）算法，这些操作符的最大范围是64比特。\\n\\n表 位运算符及函数\\n\\n表达式 含义 举例\\n\\nBIT_COUNT(N) 返回参数N中设置的比特数。 SELECT BIT_COUNT(29);\\n  -> 4\\n\\n& 位运算符与。 SELECT 29 & 15;\\n  -> 13\\n结果为一个64比特无符号整数。\\n\\n~ 反转所有比特。 SELECT 29 & ~15;\\n  -> 16\\n  结果为一个64比特无符号整数。\\n\\n| 位运算或。 SELECT 29 | ~15;\\n  -> 31\\n  结果为一个64比特无符号整数。\\n\\n^ 位运算异或。 SELECT 1 ^ 1;\\n  -> 0\\n  结果为一个64比特无符号整数。\\n\\n<<  把一个BIGINT数左移两位。 SELECT 1 << 2;\\n  -> 4\\n  其结果为一个64比特无符号整数。\\n\\n>>  把一个BIGINT数右移两位。 SELECT 4 << 2;\\n  -> 1\\n  其结果为一个64比特无符号整数。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "110",
		"help_category_id": "46",
		"name": "Operator Precedence",
		"description": "\\n表 优先级\\n优先级 运算符\\n15 !\\n14 -(负号), ~\\n13 ^\\n12 *，/，%，MOD\\n11 +，-\\n10 <<, >>\\n9 &\\n8 |\\n7 =(比较运算符等于)，<=>,>，>=，<，<=，<>，!=，IS，LIKE，REGEXP，IN\\n6 BETWEEN\\n5 NOT\\n4 AND, &&\\n3 XOR\\n2 OR, ||\\n1 =（赋值运算符）\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "111",
		"help_category_id": "6",
		"name": "CREATE DATABASE",
		"description": "\\n\\n  CREATE DATABASE [IF NOT EXISTS] dbname \\n      [create_specification_list];\\n\\n  create_specification_list: \\n      create_specification [, create_specification…]\\n\\n  create_specification: \\n      [DEFAULT] {CHARACTER SET | CHARSET} [=] charsetname \\n      | [DEFAULT] COLLATE [=] collationname\\n      | REPLICA_NUM [=] num \\n      | PRIMARY_ZONE [=] zone\\n\\n  CREATE DATABASE用于创建数据库，并可以指定数据库的默认属性（如数据库默认字符集等）；REPLICA_NUM指定副本数；PRIMARY_ZONE指定主集群。\\n",
		"example": "\\ncreate database test2 default CHARACTER SET UTF8;\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "112",
		"help_category_id": "6",
		"name": "ALTER DATABASE",
		"description": "\\n  语法：\\n\\n  ALTER DATABASE [dbname] \\n     alter_specification_list;\\n\\n  alter_specification_list: \\n      alter_specification [, alter_specification…]\\n\\n  alter_specification: \\n      [DEFAULT] CHARACTER SET charsetname\\n      | [DEFAULT] COLLATE [=] collationname \\n      | REPLICA_NUM [=] num, \\n      | PRIMARY_ZONE [=] zone\\n      | {READ ONLY | READ WRITE}\\n\\n  修改database的属性，如字符集、校对规则、REPLICA_NUM指定副本数，PRIMARY_ZONE指定主集群等。\\n\\n  数据库名称可以忽略，此时，语句对应于当前默认数据库。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "113",
		"help_category_id": "6",
		"name": "DROP DATABASE",
		"description": "\\n  语法：\\n\\n  DROP DATABASE [IF EXISTS] dbname\\n\\n  DROP DATABASE用于取消数据库中的所用表格和取消数据库。\\n\\n  IF EXISTS用于防止当数据库不存在时发生错误。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "114",
		"help_category_id": "6",
		"name": "CREATE TABLE",
		"description": "\\n  语法：\\n\\n  CREATE TABLE [IF NOT EXIST] tblname \\n    (create_definition,...)\\n    [table_options] \\n    [partition_options];\\n\\n  CREATE TABLE [IF NOT EXISTS] tblname\\n    LIKE oldtblname;\\n\\n  create_definition: \\n    colname column_definition\\n    | PRIMARY KEY (index_col_name,...) [index_option]... \\n    | {INDEX|KEY} [indexname] (index_col_name,...) [index_option]...\\n    | UNIQUE {INDEX|KEY} [indexname] (index_col_name,...) [index_option]... \\n\\n  column_definition: \\n      data_type [NOT NULL | NULL] [DEFAULT defaultvalue] \\n      [AUTO_INCREMENT] [UNIQUE [KEY]][[PRIMARY] KEY] \\n      [COMMENT \\'string\\'] \\n\\n  data_type: \\n    TINYINT[(length)] [UNSIGNED] [ZEROFILL] \\n    | SMALLINT[(length)] [UNSIGNED] [ZEROFILL] \\n    | MEDIUMINT[(length)] [UNSIGNED] [ZEROFILL] \\n    | INT[(length)] [UNSIGNED] [ZEROFILL] \\n    | INTEGER[(length)] [UNSIGNED] [ZEROFILL] \\n    | BIGINT[(length)] [UNSIGNED] [ZEROFILL] \\n    | REAL[(length,decimals)] [UNSIGNED] [ZEROFILL] \\n    | DOUBLE[(length,decimals)] [UNSIGNED] [ZEROFILL] \\n    | FLOAT[(length,decimals)] [UNSIGNED] [ZEROFILL] \\n    | DECIMAL[(length[,decimals])] [UNSIGNED] [ZEROFILL] \\n    | NUMERIC[(length[,decimals])] [UNSIGNED] [ZEROFILL] \\n    | DATE\\n    | TIME[(fsp)] \\n    | TIMESTAMP[(fsp)] \\n    | DATETIME[(fsp)] \\n    | YEAR\\n    | CHAR[(length)] \\n      [CHARACTER SET charset_name] [COLLATE collation_name] \\n    | VARCHAR(length) \\n      [CHARACTER SET charset_name] [COLLATE collation_name] \\n    | BINARY[(length)] \\n    | VARBINARY(length)\\n\\n  index_col_name: \\n     col_name [(length)] [ASC | DESC]\\n\\n  index_option: \\n    GLOBAL [LOCAL] \\n    | COMMENT \\'string\\'\\n    | COMPRESS_METHOD [=] {none | lz4_1.0 | lzo_1.0 | snappy_1.0 | zlib_1.0}\\n    | BLOCK_SIZE [=] size\\n    | STORING(columname_list)\\n\\n  table_options: \\n     table_option [[,] table_option]...\\n\\n  table_option: \\n    CHARACTER_SET [=] value\\n    | COMMENT [=] \\'string\\' \\n    | COMPRESSION [=] {none | lz4_1.0 | lzo_1.0 | snappy_1.0|zlib_1.0}\\n    | EXPIRE_INFO [=] expr\\n    | REPLICA_NUM [=] num\\n    | TABLE_ID [=] id\\n    | BLOCK_SIZE [=] size\\n    | USE_BLOOM_FILTER [=] {0| 1}\\n    | STEP_MERGE_NUM [=] num\\n    | TABLEGROUP [=] tablegroupname\\n    | ZONE_LIST [=] (zone [, zone…]), \\n    | PRIMARY_ZONE [=] \\'zone\\'; \\n    | AUTO_INCREMENT [=] num\\n\\n  partition_options: \\n     PARTITION BY\\n         HASH(expr) \\n         | KEY(column_list) \\n         [PARTITIONS num] \\n         [partition_definition ...]\\n\\n  partition_definition: \\n     COMMENT [=] \\'string\\'\\n\\n\\n  该语句用于在OceanBase数据库中创建新表。\\n\\n• 使用“IF NOT EXISTS”时，即使创建的表已经存在，也不会报错，如果不指定时，则会报错。\\n\\n• NOT NULL，DEFAULT，AUTO_INCREMENT用于列的完整性约束。\\n\\n• “table_option”内容请参见表 表选项，各子句间用“,”隔开。\\n\\n• “index_option”中，可以指定GLOBAL，LOCAL关键字，表述全局或局部索引。默认是GLOBAL index. 创建带有Parition的表时index一定要加LOCAL关键字。如果没有加，系统将报错。\\n\\n   表 表选项\\n\\n参数 含义 举例\\n\\nCHARACTER_SET 指定该表所有字符串的编码，用于对外提供元数据信息。目前仅支持UTF8MB4。 CHARACTER_SET = \\'utf8mb4\\'\\nCOMMENT 添加注释信息。 COMMENT=\\'create by Bruce\\'\\nCOMPRESS_METHOD 存储数据时使用的压缩方法名，目前提供的方法有以下几种：\\n• none（默认值，表示不作压缩）\\n• lz4_1.0\\n• lzo_1.0\\n• snappy_1.0\\n• zlib_1.0 \\nCOMPRESSION = \\'none\\'\\n\\nEXPIRE_INFO 在MemTable中的动态数据和SSTable中的静态数据合并时，满足表达式的行会自动删除。\\n一般可用于自动删除过期数据。 expire_info c1 < date_add(merging_frozen_time(),interval -1 HOUR);\\nmerging_frozen_time()表示当前最新的合并时间，此函数只试用于过期条件。\\n\\nTABLE_ID 指定表的ID。如果指定的table_id小于1000，需要打开RootServer的配置项开关“ddl_system_table_switch”。 TABLE_ID =4000\\n\\nBLOCK_SIZET 设置Partition的微块大小。 默认为16K。\\n\\nUSE_BLOOM_FILTER 对本表读取数据时，是否使用Bloom Filter。\\n• 0：默认值，不使用。\\n• 1：使用。 USE_BLOOM_FILTER = 0\\n\\nSTEP_MERGE_NUM 设置渐近合并步数。\\nSTEP_MERGE_NUM现在在限制是1~64。 默认值为1。\\nSTEP_MERGE_NUM = 5\\n\\nTABLEGROUP 表所属表格组。 \\n\\nREPLICA_NUM 这个表的partition总副本数，默认值为3。 REPLICA_NUM = 3\\n\\nZONE_LIST 集群列表 \\n\\nPRIMARY_ZONE 主集群。 \\n\\nAUTO_INCREMENT  自增字段初始值 AUTO_INCREMENT = 5\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "115",
		"help_category_id": "6",
		"name": "ALTER TABLE",
		"description": "\\nALTER TABLE tblname\\nalter_specification [, alter_specification]... ;\\n\\nalter_specification: \\n   ADD [COLUMN] colname column_definition \\n  | ADD [COLUMN] (colname column_definition,...) \\n  | ADD [UNIQUE]{INDEX|KEY} [indexname] (index_col_name,...) [index_options]\\n  | ADD PRIMARY KEY (index_col_name,...) [index_options]\\n  | ALTER [COLUMN] colname {SET DEFAULT literal | DROP DEFAULT}\\n  | CHANGE [COLUMN] oldcolname newcolname column_definition\\n  | MODIFY [COLUMN] colname column_definition  \\n  | DROP [COLUMN] colname\\n  | DROP PRIMARY KEY\\n  | DROP INDEX indexname\\n  | RENAME [TO] newtblname\\n  | ORDER BY colname\\n  | CONVERT TO CHARACTER SET charsetname [COLLATE collationname] \\n  | [DEFAULT] CHARACTER SET charsetname [COLLATE collationname] \\n  | table_options\\n  | partition_options\\n  | ADD TABLEGROUP tablegroupname\\n  | MODIFY TABLEGROUP tablegroupname\\n  | DROP TABLEGROUP tablegroupname\\n\\ncolumn_definition: \\n     data_type [NOT NULL | NULL] [DEFAULT defaultvalue] \\n      [AUTO_INCREMENT] [UNIQUE [KEY]] [[PRIMARY] KEY] \\n      [COMMENT \\'string\\'] \\n\\ntable_options: \\n     [SET] table_option [[,] table_option]... \\n\\ntable_option: \\nCHARACTER_SET [=]charsetname\\n| COMMENT [=] \\'string\\' \\n| COMPRESS_METHOD [=] {NONE | LZ4_1.0 | LZO_1.0 | SNAPPY_1.0|ZLIB_1.0}\\n| EXPIRE_INFO [=] expr\\n| REPLICA_NUM [=] num\\n| TABLE_ID [=] id\\n| BLOCK_SIZE [=] size\\n| USE_BLOOM_FILTER [=] {0| 1}\\n| STEP_MERGE_NUM [=] num\\n| TABLEGROUP [=] tablegroupname\\n| ZONE_LIST [=] (zone [, zone…]) \\n| PRIMARY_ZONE [=] zone \\n| AUTO_INCREMENT [=] num\\n| {READ ONLY | READ WRITE}\\n\\npartition_options: \\n     PARTITION BY\\n         HASH(expr) \\n         | KEY(column_list) \\n         [PARTITIONS num] \\n        [partition_definition ...]\\n\\npartition_definition: \\n COMMENT [=] \\'commenttext\\'\\n\\n\\n1. 增加列\\nALTER TABLE tblname \\n    ADD [COLUMN] colname data_type \\n    [NOT NULL | NULL] \\n    [DEFAULT defaultvalue] \\n• data_type请参见“数据类型”章节\\n2. 修改列属性\\nALTER TABLE tblname\\n    ALTER [COLUMN] colname \\n    [SET DEFAULT literal| DROP DEFAULT]\\n3. 修改列类型\\nALTER TABLE tblname\\n        MODIFY colname column_definition\\n4. 删除列\\nALTER TABLE tblname\\n    DROP [COLUMN] colname \\n• 不允许删除主键列或者包含索引的列。\\n5. 表重命名\\nALTER TABLE tblname\\n        RENAME TO newtblname\\n6. 列重命名\\nALTER TABLE tblname\\n    CHANGE [COLUMN] oldcolname newcolname column_definition\\n例1：\\nALTER TABLE t2 CHANGE COLUMN d c CHAR(10); \\n#把表t2的字段d改名为c，并同时修改了字段类型\\n7. 设置过期数据删除\\nALTER TABLE tblname\\n    SET EXPIRE_INFO [ = ] expr\\n例1：\\nCREATE TABLE example_1(custid INT            \\n  ",
		"example": "\\n  ",
		"url": URL
	},
	{
		"help_topic_id": "116",
		"help_category_id": "6",
		"name": "DROP TABLE",
		"description": "\\n语法：\\nDROP TABLE [IF EXISTS] tbl_list;\\n\\ntbl_list: \\n   tblname [, tblname …] \\n该语句用于删除OceanBase数据库中的表。\\n使用“IF EXISTS”时，即使要删除的表不存在，也不会报错，如果不指定时，则会报错。\\n• 同时删除多个表时，用“,”隔开。\\n\\n",
		"example": "\\nDROP TABLE IF EXISTS test;\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "117",
		"help_category_id": "6",
		"name": "CREATE INDEX",
		"description": "\\n  索引是创建在表上的，对数据库表中一列或多列的值进行排序的一种结构。其作用主要在于提高查询的速度，降低数据库系统的性能开销。\\n\\n  OceanBase中，新创建的索引需要等待一次每日合并后，才生效。\\n\\n\\n语法：\\n\\n  CREATE [UNIQUE] INDEX indexname \\n     ON tblname (index_col_name,…) \\n         [index_options];\\n      \\n  index_options: \\n        index_option [index_option…]\\n\\n  index_option: \\n       GLOBAL [LOCAL] \\n       | COMMENT \\'string\\'\\n       | COMPRESS_METHOD [=] {NONE | LZ4_1.0 | LZO_1.0 | SNAPPY_1.0 | ZLIB_1.0}\\n       | BLOCK_SIZE [=] size\\n       | STORING(columname_list)\\n\\n  columname_list: \\n      colname [, colname…]\\n\\n  index_col_name: \\n      colname [ASC | DESC] \\n\\n• index_col_name中，每个列名后都支持ASC（升序）和DESC（降序）。默认就为升序。\\n\\n• 本语句建立索引的排列方式为：首先以index_col_name中第一个列的值排序；该列值相同的记录，按下一列名的值排序；以此类推。\\n\\n• 执行“SHOW INDEX FROM tblname”可以查看创建的索引。\\n\\n• “index_option”中，可以指定GLOBAL，LOCAL关键字，表述全局或局部索引。默认是GLOBAL　index. 创建带有Parition的表时index一定要加LOCAL关键字。如果没有加，系统将报错。\\n\\n• 使用可选字段STORING，表示索引表中冗余存储某些列，以提高系统查询系统。\\n\\n",
		"example": "\\n1. 执行以下命令，创建表test。\\n\\n  CREATE TABLE test (c1 int primary key, c2 varchar(10));\\n\\n2. 执行以下命令，创建表test的索引。\\n\\n  CREATE INDEX test_index ON test (c1, c2 DESC);\\n\\n3. 执行以下命令，查看表test的索引。\\n\\n  SHOW INDEX FROM test;\\n\\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "118",
		"help_category_id": "6",
		"name": "DROP INDEX",
		"description": "\\n  当索引过多时，维护开销增大，因此，需要删除不必要的索引。\\n\\n  （删除索引的时候需要等待一段时间才能完全删除。）\\n\\n  语法：\\n\\n  DROP INDEX indexname \\n   ON tblname;\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "119",
		"help_category_id": "6",
		"name": "CREATE VIEW",
		"description": "\\n  语法：\\n\\n  CREATE [OR REPLACE] VIEW viewname\\n      [(column_list)] AS select_stmt\\n\\n  创建试图语句，如果指定了OR REPLACE子句，该语句能够替换已有的视图。\\n\\n  select_stmt是一种SELECT语句。它给出了视图的定义。该语句可以从基表或其他视图进行选择。\\n\\n  视图必须具有唯一的列名，不得有重复，就像基表那样。默认情况下，由SELECT语句检索的列名将用作视图列名。要想为视图列定义明确的名称，可使用可选的column_list子句，列出由逗号隔开的ID。column_list中的名称数目必须等于SELECT语句检索的列数。\\n\\n  SELECT语句检索的列可以是对表列的简单引用。也可以是使用函数、常量值、操作符等的表达式。\\n\\n  视图在数据库中实际上并不是以表的形式存在。每次使用时它们就会派生。视图是作为在 CREATE VIEW 语句中指定的 SELECT 语句的结果而派生出来的。\\n\\n  OceanBase 1.0只支持不可更新视图。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "120",
		"help_category_id": "6",
		"name": "DROP VIEW",
		"description": "\\n  语法:\\n\\n  DROP VIEW [IF EXISTS] \\n    viewname [, viewname…] ;\\n\\n  DROP VIEW能够删除1个或多个视图。必须在每个视图上有DROP权限。\\n\\n  使用IF EXISTS关键字来防止因不存在的视图而出错。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "121",
		"help_category_id": "6",
		"name": "ALTER VIEW",
		"description": "\\n  语法：\\n\\n  ALTER VIEW viewname\\n     [(column_list)] AS select_statement\\n\\n  该语句用于更改已有视图的定义。其语法与CREATE VIEW类似。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "122",
		"help_category_id": "6",
		"name": "TRUNCATE TABLE",
		"description": "\\n  语法：\\n\\n  TRUNCATE [TABLE] tblname\\n\\n  该语句用于完全清空指定表，但是保留表结构，包括表中定义的Partition信息。从逻辑上说，该语句与用于删除所有行的DELETE FROM语句相同。执行TRUNCATE语句，必须具有表的删除和创建权限。它属于DDL语句。\\n\\n  TRUNCATE TABLE语句与DELETE FROM语句有以下不同：\\n\\n  删减操作会取消并重新创建表，这比一行一行的删除行要快很多。\\n\\n  TRUNCATE TABLE语句执行结果显示影响行数始终显示为0行。\\n\\n  使用TRUNCATE TABLE语句，表管理程序不记得最后被使用的AUTO_INCREMENT值，但是会从头开始计数。\\n\\n  TRUNCATE语句不能在进行事务处理和表锁定的过程中进行，如果使用，将会报错。\\n\\n  只要表定义文件是合法的，则可以使用TRUNCATE TABLE把表重新创建为一个空表，即使数据或索引文件已经被破坏。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "123",
		"help_category_id": "7",
		"name": "INSERT",
		"description": "\\n  该语句用于添加一个或多个记录到表。\\n\\n  语法：\\n\\n  INSERT [INTO] tblname \\n       [(colname,...)] \\n       {VALUES|VALUE} ({expr | DEFAULT},...)\\n       [ ON DUPLICATE KEY UPDATE\\n           colname=expr\\n           [, colname=expr] ... ] ;\\n\\n  [(colname,...)]用于指定插入数据的列。\\n\\n• 同时插入多列时，用“,”隔开。\\n\\n• 支持ON DUPLICATE KEY UPDATE\\n\\n• Insert语句后面不支持set操作\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "124",
		"help_category_id": "7",
		"name": "REPLACE",
		"description": "\\n  REPLACE的运行与INSERT很相像。只有一点除外，如果表中的一个旧记录与一个用于PRIMARY KEY或一个UNIQUE索引的新记录具有相同的值，则在新记录被插入之前，旧记录被删除格式。\\n\\n  为了能够使用REPLACE，必须同时拥有表的INSERT和DELETE权限。\\n\\n  语法：\\n\\nREPLACE [INTO] tblname\\n       [(colname,...)] \\n      {VALUES|VALUE} ({expr | DEFAULT},...);\\n\\n• [(colname,...)]用于指定插入数据的列。\\n\\n• 同时替换多列时，用“,”隔开。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "125",
		"help_category_id": "7",
		"name": "UPDATE",
		"description": "\\n  该语句用于修改表中的字段值。\\n\\n  语法：\\n\\n  UPDATE tblname\\n     SET colname1={expr1|DEFAULT} \\n     [, colname2={expr2|DEFAULT}] ... \\n     [WHERE where_condition] \\n     [ORDER BY order_list  [ASC|DESC]] \\n     [LIMIT row_count]; \\n\\n  order_list: \\n     colname [ASC|DESC] [, colname [ASC|DESC]…]\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "126",
		"help_category_id": "7",
		"name": "DELETE",
		"description": "\\n  该语句用于删除表中符合条件的行。\\n\\n  语法：\\n\\n  DELETE FROM tblname  \\n       [WHERE where_condition] \\n       [ORDER BY order_list \\n       [LIMIT row_count];\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "127",
		"help_category_id": "7",
		"name": "SELECT",
		"description": "\\n  该语句用于查询表中的内容。\\n\\n  基本查询\\n\\n  语法：\\n\\n  SELECT \\n    [ALL | DISTINCT] \\n      selectexpr [AS othername] [, selectexpr ...] \\n      [FROM table_references]\\n        [WHERE where_conditions] \\n    [GROUP BY group_by_list] \\n    [HAVING search_confitions] \\n    [ORDER BY order_list] \\n    [LIMIT {[offset,] rowcount | rowcount OFFSET offset}]\\n        [FOR UPDATE];\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "128",
		"help_category_id": "8",
		"name": "TRANSACTION",
		"description": "\\n  START TRANSACTION \\n    [WITH CONSISTENT SNAPSHOT];\\n  BEGIN [WORK] ; \\n  COMMIT [WORK] ;\\n  ROLLBACK [WORK]; \\n\\n• WITH CONSISTENT SNAPSHOT子句用于启动一个一致的读取。该子句的效果与发布一个START TRANSACTION，后面跟一个来自任何OceanBase表的SELECT的效果一样。OceanBase 1.0语法上支持 WITH CONSISTENT SNAPSHOT子句，其WITH CONSISTENT SNAPSHOT功能暂时还未实现。\\n\\n• BEGIN和BEGIN WORK被作为START TRANSACTION的别名受到支持，用于对事务进行初始化。START TRANSACTION是标准的SQL语法，并且是启动一个ad-hoc(点对点)事务的推荐方法。一旦开启事务，则随后的SQL数据操作语句（即INSERT，UPDATE，DELETE，不包括REPLACE）直到显式提交时才会生效。\\n\\n  提交当前事务语句格式如下：\\n\\n  COMMIT [WORK];\\n\\n  回滚当前事务语句格式如下：\\n\\n  ROLLBACK [WORK];\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "129",
		"help_category_id": "11",
		"name": "CREATE RESOURCE UNIT",
		"description": "\\n  语法：\\n\\n  CREATE RESOURCE UNIT unitname \\n     MAX_CPU [=] cpunum, \\n     MAX_MEMORY [=] memsize, \\n     MAX_IOPS [=] iopsnum, \\n     MAX_DISK_SIZE [=] disksize, \\n     MAX_SESSION_NUM [=] sessionnum,\\n     [MIN_CPU [=] cpunum,] \\n     [MIN_MEMORY [=] memsize,] \\n     [MIN_IOPS [=] iopsnum];\\n\\n  创建资源单元时，MAX_CPU、MAX_MEMORY、MAX_IOPS、 MAX_DISK_SIZE、MAX_SESSION_NUM必须指定。MIN_CPU、MIN_MEMORY、MIN_IOPS可选，默认值和MAX_CPU、MAX_MEMORY、MAX_IOPS保持一致。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "130",
		"help_category_id": "11",
		"name": "DROP RESOURCE UNIT",
		"description": "\\n  语法：\\n\\n  DROP RESOURCE UNIT unitname;\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "131",
		"help_category_id": "11",
		"name": "CREATE RESOURCE POOL",
		"description": "\\n  语法：\\n\\n  CREATE RESOURCE POOL poolname \\n   UNIT [=] unitname, \\n   UNIT_NUM [=] unitnum, \\n   ZONE_LIST [=] (\\'zone\\' [, \\'zone\\'...]);\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "132",
		"help_category_id": "11",
		"name": "ALTER RESOURCE POOL",
		"description": "\\n  语法：\\n\\n  ALTER RESOURCE POOL poolname \\n    UNIT [=] unitname, \\n    UNIT_NUM [=] unitnum, \\n    ZONE_LIST [=] (\\'zone\\' [, \\'zone\\'...]);\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "133",
		"help_category_id": "11",
		"name": "DROP RESOURCE POOL",
		"description": "\\n  语法：\\n\\n  DROP RESOURCE POOL poolname;\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "134",
		"help_category_id": "11",
		"name": "CREATE TENANT",
		"description": "\\n  语法：\\n\\n  CREATE TENANT [IF NOT EXISTS] tenantname \\n     [tenant_characteristic_list]\\n     [tenant_variables_list]\\n\\n  tenant_characteristic_list: \\n    tenant_characteristic [, tenant_characteristic...]\\n\\n  tenant_characteristic: \\n    COMMENT \\'string\\' \\n    | {CHARACTER SET | CHARSET} [=] value \\n    | REPLICA_NUM [=] num \\n    | ZONE_LIST [=] (zone [, zone\\u2026]) \\n    | PRIMARY_ZONE [=] zone\\n    | RESOURCE_POOL_LIST [=] (poolname) \\n    | {READ ONLY | READ WRITE} \\n\\n  tenant_variables_list: \\n    SET sys_variables_list \\n    | SET VARIABLES sys_variables_list \\n    | VARIABLES sys_variables_list \\n\\n  sys_variables_list: \\n    sys_variables [, sys_variables...] \\n\\n  sys_variables: \\n    sys_variable_name = expr \\n\\n  如果要创建的租户名已存在，并且没有指定IF NOT EXISTS，则会出现错误。\\n\\n  租户名的合法性和变量名一致，最长64个字节，字符只能有大小写英文字母，数字和下划线，而且必须以字母或下划线开头，并且不能OceanBase的关键字。\\n\\n  只有用root用户连接根到租户（root@ROOT）才能执行CREATE TENANT去创建租户。\\n\\n  说明：RESOURCE_POOL_LIST为创建租户时的必填项。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "135",
		"help_category_id": "11",
		"name": "ALTER TENANT",
		"description": "\\n  语法：\\n\\n  ALTER TENANT tenantname [SET] \\n      [tenant_characteristic_list]\\n      [tenant_variables_list]\\n\\n  tenant_characteristic_list: \\n    tenant_characteristic [, tenant_characteristic...]\\n\\n  tenant_characteristic: \\n    COMMENT \\'string\\' \\n    | {CHARACTER SET | CHARSET} [=] value \\n    | REPLICA_NUM [=] num \\n    | ZONE_LIST [=] (zone [, zone\\u2026]) \\n    | PRIMARY_ZONE [=] zone \\n    | RESOURCE_POOL_LIST [=] (poolname)\\n    | {READ ONLY | READ WRITE} \\n  tenant_variables_list: \\n    VARIABLES sys_variables_list \\n\\n  sys_variables_list: \\n    sys_variables [, sys_variables...] \\n\\n  sys_variables: \\n    sys_variable_name = expr \\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "136",
		"help_category_id": "11",
		"name": "LOCK/UNLOCK",
		"description": "\\n\\n  语法：\\n\\n  ALTER TENANT tenantname LOCK|UNLOCK；\\n\\n  对租户进行锁定，锁定后，不能在该租户上创建新的连接，已有连接保持不变。通常用在客户费用到期未续费的场景下使用，客户费用到期后对租户进行锁定，客户续费后再进行解锁。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "137",
		"help_category_id": "11",
		"name": "DROP TENANT",
		"description": "\\n  语法：\\n\\n  DROP TENANT tenantname;\\n\\n  删除租户\\n\\n  只有用root用户连接到根租户（root@ROOT）才能执行CREATE TENANT去创建租户。\\n\\n  只能删除掉处理“锁定”状态下的租户，对非锁定状态下的租户执行DROP是则报错。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "138",
		"help_category_id": "11",
		"name": "CREATE TABLEGROUP",
		"description": "\\n  语法：\\n\\n  CREATE TABLEGROUP [IF NOT EXISTS] tablegroupname\\n\\n  如果要创建的TableGroup名称已存在，并且没有指定IF NOT EXISTS，则会出现错误。\\n\\n  TableGroup名称最长64个字符，字符只能有大小写英文字母，数字和下划线，而且必须以字母或下划线开头，并且不能OceanBase的关键字。\\n\\n  只有租户下的管理员权限才可以创建TableGroup。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "139",
		"help_category_id": "11",
		"name": "DROP TABLEGROUP",
		"description": "\\n  语法：\\n\\n  DROP TABLEGROUP [IF EXISTS] tablegroupname\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "140",
		"help_category_id": "11",
		"name": "ALTER TABLEGROUP",
		"description": "\\n  语法：\\n\\n  ALTER TABLEGROUP tablegroupname ADD [TABLES] tblname [, tblname…]\\n\\n  该语句用于对一个tablegroup增加多张表。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "141",
		"help_category_id": "11",
		"name": "CREATE USER",
		"description": "\\n  语法：\\n\\n  CREATE USER user_specification_list;\\n\\n  user_specification_list: \\n    user_specification [, user_specification]…;\\n\\n  user_specification: \\n    user IDENTIFIED BY \\'authstring\\'\\n    user IDENTIFIED BY PASSWORD \\'hashstring\\'\\n\\n• 必须拥有全局的CREATE USER权限，才可以使用CREATE USER命令。\\n\\n• 新建用户后，“mysql.user”表会新增一行该用户的表项。如果同名用户已经存在，则报错。\\n\\n• 使用自选的IDENTIFIED BY子句，可以为账户给定一个密码。\\n\\n• user IDENTIFIED BY \\'authstring\\'此处密码为明文，存入“mysql.user”表后，服务器端会变为密文存储。\\n\\n• user IDENTIFIED BY PASSWORD \\'hashstring\\'此处密码为密文。\\n\\n• 同时创建多个用户时，用“,”隔开。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "142",
		"help_category_id": "11",
		"name": "DROP USER",
		"description": "\\n  DROP USER语句用于删除一个或多个OceanBase用户。\\n\\n  语法：\\n\\n  DROP USER username [, username...];\\n\\n• 必须拥有全局的CREATE USER权限，才可以使用DROP USER命令。\\n\\n• 不能对“mysql.user”表进行DELETE方式进行权限管理。\\n\\n• 成功删除用户后，这个用户的所有权限也会被一同删除。\\n\\n• 同时删除多个用户时，用“,”隔开。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "143",
		"help_category_id": "11",
		"name": "SET PASSWORD",
		"description": "\\n  语法：\\n\\n  SET PASSWORD [FOR user] = password_option;\\n\\n  password_option: {\\n  PASSWORD(\\'authstring\\')\\n  |\\'hashstring\\'}\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "144",
		"help_category_id": "11",
		"name": "RENAME USER",
		"description": "\\n  语法：\\n\\n  RENAME USER \\n      \\'oldusername\\' TO \\'newusername\\'\\n      [,\\'oldusername\\' TO \\'newusername\\'...];\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "145",
		"help_category_id": "11",
		"name": "ALTER USER",
		"description": "\\n  锁定或者解锁用户。\\n\\n  语法：\\n\\n  锁定用户\\n\\n  ALTER USER \\'user\\' LOCKED;\\n\\n  解锁用户\\n\\n  ALTER USER \\'user\\' UNLOCKED;\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "146",
		"help_category_id": "11",
		"name": "GRANT",
		"description": "\\n  GRANT语句用于系统管理员授予User某些权限。\\n\\n  语法：\\n\\n  GRANT priv_type \\n    ON priv_level \\n    TO user_specification [, user_specification]... \\n        [WITH with_option ...]\\n\\n\\n  priv_level: \\n       *\\n      | *.*\\n      | db_name.* \\n      | db_name.tbl_name\\n      | tbl_name\\n\\n  user_specification: \\n    user [IDENTIFIED BY [PASSWORD] \\'password\\'] \\n\\n  with_option:\\n    GRANT OPTION\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "147",
		"help_category_id": "11",
		"name": "REVOKE",
		"description": "\\n  REVOKE语句用于系统管理员撤销User某些权限。\\n\\n  语法：\\n\\n  REVOKE priv_type \\n     ON database.tblname \\n     FROM \\'user\\';\\n\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "148",
		"help_category_id": "11",
		"name": "SET",
		"description": "\\n  修改用户变量：\\n\\n  用户变量用于保存一个用户自定义的值，以便于在以后引用它，这样可以将该值从一个语句传递到另一个语句。用户变量与连接有关，即一个客户端定义的用户变量不能被其它客户端看到或使用，当客户端退出时，该客户端连接的所有变量将自动释放。\\n\\n  语法：\\n\\n  SET @var_name = expr;\\n\\n• 用户变量的形式为@var_name，其中变量名var_name可以由当前字符集的文字数字字符、“.”、“_”和“$”组成。\\n\\n• 每个变量的expr可以为整数、实数、字符串或者NULL值。\\n\\n• 同时定义多个用户变量时，用“,”隔开。\\n\\n  修改系统变量：\\n\\n  OceanBase维护两种变量：\\n\\n  • 全局变量\\n\\n  影响OceanBase整体操作。当OceanBase启动时，它将所有全局变量初始化为默认值。修改全局变量，必须具有SUPER权限。\\n\\n  • 会话变量\\n\\n  影响当前连接到OceanBase的客户端。在客户端连接OceanBase时，使用相应全局变量的当前值对该客户端的会话变量进行初始化。设置会话变量不需要特殊权限，但客户端只能更改自己的会话变量，而不能更改其它客户端的会话变量。\\n\\n  语法：\\n\\n  设置全局变量的格式：\\n\\n  SET GLOBAL system_var_name = expr;\\n\\n  或者\\n\\n  SET @@GLOBAL.system_var_name = expr;\\n\\n  设置会话变量的格式：\\n\\n  SET [SESSION | @@SESSION. | LOCAL | LOCAL. | @@]system_var_name =expr;\\n\\n  查看系统变量值的格式，如果指定GLOBAL或SESSION修饰符，则分别打印全局或当前会话的系统变量值；如果没有修饰符，则显示当前会话值：\\n\\n  SHOW [GLOBAL | SESSION] \\n    VARIABLES \\n    [LIKE \\'system_var_name\\' | WHERE expr];\\n\\n",
		"example": "\\n  1. 执行以下命令，修改回话变量中的SQL超时时间。\\n\\n  SET @@SESSION.ob_tx_timeout = 900000;\\n\\n  2. 执行以下命令，查看“ob_tx_timeout”的值。\\n\\n  SHOW VARIABLES LIKE \\'ob_tx_timeout\\';\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "149",
		"help_category_id": "11",
		"name": "ALTER SYSTEM",
		"description": "\\n  ALTER SYSTEM语句主要对OceanBase发送命令，执行某项指定操作。\\n\\n系统级管理命令\\n\\n  ALTER SYSTEM BOOTSTRAP\\n    {REGION [=] region ZONE [=] zone SERVER [=] \\'ip:port\\'\\n      | ZONE [=] zone SERVER [=] \\'ip:port\\'}\\n    [,{REGION [=] region ZONE [=] zone SERVER [=] \\'ip:port\\'\\n      | ZONE [=] zone SERVER [=] \\'ip:port\\'}…]\\n\\n\\nSchema管理\\n\\n  刷新Schema\\n\\n    ALTER SYSTEM REFRESH SCHEMA {SERVER [=] \\'ip:port\\' | ZONE [=] \\'zone\\'}\\n\\n  新增服务器\\n\\n    ALTER SYSTEM ADD SERVER \\'ip:port\\' [,\\'ip:port\\'…] [ZONE [=] \\'zone\\']\\n\\n    如果指定Zone，将会对待加入Server执行Zone校验。\\n\\n  删除&取消删除服务器\\n\\n    ALTER SYSTEM DELETE SERVER \\'ip:port\\' [,\\'ip:port\\'…] [ZONE [=] \\'zone\\']\\n\\n    ALTER SYSTEM CANCEL DELETE SERVER \\'ip:port\\' [,\\'ip:port\\'…] [ZONE [=] \\'zone\\']\\n\\n  开始/暂停服务\\n\\n    ALTER SYSTEM {START | STOP} SERVER \\'ip:port\\' [,\\'ip:port\\'…] [ZONE=\\'zone\\']\\n\\n    开始/暂停对外的服务.\\n\\n  升极虚拟表schema\\n\\n    ALTER SYSTEM UPGRADE VIRTUAL SCHEMA\\n\\nZone管理\\n\\n  新增Zone\\n\\n    ALTER SYSTEM ADD ZONE \\'zone\\' [REGION \\'region\\']\\n\\n  删除Zone\\n\\n    ALTER SYSTEM DELETE ZONE \\'zone\\'\\n\\n  主动上下线Zone\\n\\n    ALTER SYSTEM {START|STOP} ZONE \\'zone\\'\\n\\n  变更Zone属性\\n\\n    ALTER SYSTEM {ALTER|CHANGE|MODIFY} ZONE \\'zone\\' [SET] REGION \\'new_region\\'\\n\\nSession管理\\n\\n  结束Session\\n\\n  普通Tenant kill自己的Session\\n\\n    KILL [CONNECTION] \\'sessionid\\'\\n\\nPartition管理\\n\\n  Leader改选\\n\\n    ALTER SYSTEM SWITCH REPLICA {LEADER | FOLLOWER}\\n      {PARTITION_ID [=] \\'partidx%partcount@tableid\\' SERVER [=] \\'ip:port\\'\\n        | SERVER [=] \\'ip:port\\' [TENANT [=] \\'tenant\\']\\n        | ZONE [=] \\'zone\\' [TENANT [=] \\'tenant\\']}\\n\\n  副本删除\\n\\n    ALTER SYSTEM DROP REPLICA\\n      PARTITION_ID [=] \\'partidx%partcount@tableid\\' SERVER [=] \\'ip:port\\'\\n      [CREATE_TIMESTAMP [=] ctimestamp] [ ZONE [=] \\'zone\\']\\n\\n    删除指定ObServer上的Replica，必须指定partition_id，ObServer地址以及create_timestamp。\\n\\n  副本迁移/复制\\n\\n    ALTER SYSTEM {MOVE|COPY} REPLICA\\n    PARTITION_ID [=] \\'partidx%partcount@tableid\\'\\n    SOURCE [=] \\'ip:port\\' DESTINATION [=] \\'ip:port\\'\\n\\n    Replica迁移或者复制，需要指定源ObServer和目的ObServer，以及partition_id。\\n\\n  副本汇报\\n\\n    ALTER SYSTEM REPORT REPLICA [SERVER [=] \\'ip:port\\' | ZONE [=] \\'zone\\']\\n\\n    强制要求某个ObServer或者某个Zone内的所有ObServer进行Replica汇报。\\n\\n  回收无用副本\\n\\n    ALTER SYSTEM RECYCLE REPLICA [SERVER [=] \\'ip:port\\' | ZONE [=] \\'zone\\']\\n\\n\\n每日合并\\n\\n  发起每日合并\\n\\n    ALTER SYSTEM MAJOR FREEZE [IGNORE ip:port\\' [,\\'ip:port\\'…]]\\n\\n  打开手动合并\\n\\n    ALTER SYSTEM SET ENABLE_MANUAL_MEREG=\\'True\\'\\n\\n  关闭手动合并\\n\\n    ALTER SYSTEM SET ENABLE_MANUAL_MEREG=\\'False\\'\\n\\n  开始每日合并\\n\\n    ALTER SYSTEM START MERGE ZONE [=] \\'zone\\'\\n\\n  暂停每日合并\\n\\n    ALTER SYSTEM SUSPEND MERGE [ZONE [=] \\'zone\\']\\n\\n  恢复每日合并\\n\\n    ALTER SYSTEM RESUME MERGE [ZONE [=] \\'zone\\']\\n\\n  清空roottable\\n\\n    ALTER SYSTEM CLEAR ROOTTABLE [TENANT [=] \\'tenantname\\']\\n\\n    清空roottable以后，汇报partition。\\n\\n内存管理\\n\\n  发起Partition转储\\n\\n    ALTER SYSTEM MINOR FREEZE\\n      {PARTITION_ID [=] \\'partidx%partcount@tableid\\' SERVER [=] \\'ip:port\\'| SERVER [=] \\'ip:port\\' | ZONE [=] \\'zone\\'}\\n\\n  强制挤占一部分cache\\n\\n  通过动态修改Cache相关配置项实现\\n\\n  清空location缓存 (not implement)\\n\\n    ALTER SYSTEM CLEAR LOCATION CACHE [SERVER [=] \\'ip:port\\'| ZONE [=] \\'zone\\']\\n\\n配置项\\n\\n  修改系统级配置\\n\\n    ALTER SYSTEM SET param_name = expr [COMMENT \\'text\\']\\n      [SCOPE = conf_scope] {SERVER = \\'ip:port\\' | ZONE = \\'zone\\'}\\n\\n    • 同时修改多个系统配置项时，用“,”隔开。\\n\\n  查看系统配置项的格式：\\n\\n    SHOW PARAMETERS [LIKE \\'pattern\\' | WHERE expr]\\n\\n租户级管理命令\\n\\n  修改租户级配置\\n\\n    ALTER SYSTEM SET param_name = expr\\n      [COMMENT \\'text\\']\\n      [SCOPE = conf_scope]\\n      [TENANT = \\'tenantname\\']\\n\\n  打开/关闭迁移开关\\n\\n    ALTER SYSTEM SET ENABLE_MIGRATION = {\\'true\\'|\\'false\\'}\\n      [TENANT=\\'tenantname\\']\\n\\n  打开/关闭复制开关\\n\\n    ALTER SYSTEM SET ENABLE_REPLICATION= {\\'true\\'|\\'false\\'}\\n      [TENANT=\\'tenantname\\']\\n\\n  发起Tenant级转储\\n\\n    ALTER SYSTEM MINOR FREEZE TENANT= \\'tenantname\\'\\n\\nCACHE管理\\n\\n  清空KVCache.\\n\\n    ALTER SYSTEM FLUSH KVCACHE\\n      [TENANT [=] [\\'tenantname\\'|tenantname]] [CACHE [=] \\'cache\\']\\n\\n    清空KVCache, 如果没有指定 TENANT 和 CACHE 则清楚所有CACHE.\\n\\nSERVER管理\\n\\n\\n  杀死某个Query线程.\\n\\n    KILL QUERY expr\\n\\n重载信息\\n\\n  重新加载 unit 信息\\n\\n    ALTER SYSTEM RELOAD UNIT\\n\\n  重新加载 server 信息\\n\\n    ALTER SYSTEM RELOAD SERVER:\\n\\n  重新加载 zone 信息\\n\\n    ALTER SYSTEM RELOAD ZONE\\n\\n  刷新当前server的时区信息\\n\\n    ALTER SYSTEM REFRESH TIME_ZONE_INFO\\n\\nRS\\n\\n  Leader改选\\n\\n    ALTER SYSTEM SWITCH ROOTSERVICE {LEADER | FOLLOWER} {SERVER [=] \\'ip:port\\' | ZONE [=]\\'zone\\'}\\n\\n任务管理\\n\\n  跑后台任务\\n\\n    ALTER SYSTEM RUN JOB STRING_VAL {SERVER [=] \\'ip:port\\'| ZONE [=] \\'zone\\'}\\n\\n  开始/结束升级\\n\\n    ALTER SYSTEM {BEGING|END} UPGRADE\\n\\n  解除合并错误，继续合并\\n\\n    ALTER CLEAR MERGE ERROR_P\\n\\n  取消任务\\n\\n    ALTER SYSTEM CANCEL [PARTION MIGRATION] TASK \\'string_value\\'\\n\\n其他\\n\\n  发起unit迁移\\n\\n    ALTER SYSTEM MIGRATE UNIT [=] int DESTINATION [=] STRING_VALUE\\n\\n\\n  设置内部trace point及触发错误码\\n\\n    ALTER SYSTEM SET_TP {{TP_NO|OCCUR|FREQUENCY|ERROR_CODE} [=] int}...\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "150",
		"help_category_id": "9",
		"name": "PREPARE",
		"description": "\\n  语法：\\n\\nPREPARE stmt_name \\n      FROM preparable_stmt;\\n\\n• preparable_stmt为SQL数据操作语句，需要用单引号（\\'\\'）引起。\\n\\npreparable_stmt只能预备一条sql语句，不支持多条语句。预备好的语句在整个SQL会话期间可以使用stmt_name这个名字来执行。\\n\\n• 预处理语句的范围是客户端会话。在此会话内，语句被创建。其它客户端看不到它。关闭会话，系统自动将预处理语句执行释放操作；在同一个会话中，另设置一个同名的stmt_name，即使不执行DEALLOCATE/DORP命令，它会被隐含地解除分配。如果新语句包含一个错误并且不能被预备，则会返回一个错误，并且不再存在带有给定名称的预备语句。\\n\\n• 数据操作语句（即SELECT, REPLACE, INSERT, UPDATE, DELETE）都可以被预备执行。\\n\\n• 在被预备的SQL语句中，可以使用问号（?）表明一个之后执行时才绑定的参数。问号只能出现在SQL语句的常量中。一个被预备的语句也可以不包含问号。\\n\\n",
		"example": "\\n  依次使用PREPARE查询表a中id=2的行。\\n\\nPREPARE stmt1 FROM SELECT name FROM a WHERE id=?;\\nSET @id = 2;\\nEXECUTE stmt1 USING @id;\\nDEALLOCATE PREPARE stmt1;\\n\\n\\n",
		"url": URL
	},
	{
		"help_topic_id": "151",
		"help_category_id": "9",
		"name": "EXECUTE",
		"description": "\\n  语法：\\n\\n  EXECUTE stmtname \\n      [USING @varname [, @varname] ...];\\n\\n• 一个使用PREPARE语句预备好的SQL语句，可以使用EXECUTE语句执行。\\n\\n• 如果预备语句中有问号指明的绑定变量，需要使用USING子句指明相同个数的执行时绑定的值。USING子句后只能使用SET语句定义的用户变量。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "152",
		"help_category_id": "9",
		"name": "DEALLOCATE",
		"description": "\\n  语法：\\n\\n  {DEALLOCATE | DROP} PREPARE stmt_name;\\n\\n  删除一个指定的预备语句。一旦删除，以后就不能再执行。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "153",
		"help_category_id": "12",
		"name": "SHOW",
		"description": "\\n  主要用于查看OceanBase有关数据库、表、列或服务器状态的等信息。\\n\\nSHOW CHARACTER SET [like_or_where];\\nSHOW COLLATION [like_or_where];\\nSHOW [FULL] COLUMNS FROM tblname [FROM dbname] [like_or_where];\\nSHOW CREATE DATABASE dbname;\\nSHOW CREATE TABLE tblname;\\nSHOW CREATE VIEW viewname;\\nSHOW DATABASES [like_or_where];\\nSHOW DATABASES STATUS [like_or_where];\\nSHOW ERRORS [LIMIT [offset,] rowcount];\\nSHOW GRANTS FOR user;\\nSHOW INDEX FROM tblname [FROM dbname];\\nSHOW PRIVILEGES;\\nSHOW [FULL] PROCESSLIST;\\nSHOW [GLOBAL | SESSION] STATUS [like_or_where];\\nSHOW TABLE STATUS [FROM dbname] [like_or_where];\\nSHOW [FULL] TABLES [FROM dbname] [like_or_where];\\nSHOW [GLOBAL | SESSION] VARIABLES [like_or_where];\\nSHOW WARNINGS [LIMIT [offset,] rowcount];\\nSHOW CREATE TENANT tenantname;\\nSHOW TENANT;\\nSHOW TENANT STATUS;\\n\\nlike_or_where:\\n    LIKE \\'pattern\\'\\n   | WHERE expr\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "154",
		"help_category_id": "12",
		"name": "KILL",
		"description": "\\n  语法：\\n\\n  KILL [GLOBAL | LOCAL] [CONNECTION | QUERY] \\'sessionid\\' \\n\\n  每个与OceanBase的连接都在一个独立的线程里运行，您可以使用SHOW PROCESSLIST;语句查看哪些线程正在运行，并使用KILL thread_id语句终止一个线程。\\n\\n• KILL CONNECTION与不含修改符的KILL一样：它会终止与给定的thread_id。\\n\\n• KILL QUERY会终止连接当前正在执行的语句，但是会保持连接的原状。\\n如果您拥有PROCESS权限，则您可以查看所有线程。如果您拥有SUPER权限，您可以终止所有线程和语句。否则，您只能查看和终止您自己的线程和语句。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "155",
		"help_category_id": "12",
		"name": "DESCRIBE",
		"description": "\\n  语法：\\n\\n  {DESCRIBE | DESC | EXPLAIN} tblname [colname | wild];\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "156",
		"help_category_id": "12",
		"name": "USE",
		"description": "\\n  语法：\\n\\n  USE dbname\\n\\n  USE dbname语句可以通告客户端把dbname数据库作为默认（当前）数据库使用，用于后续语句。该数据库保持为默认数据库，直到语段的结尾，或者直到发布一个不同的USE语句。\\n\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "157",
		"help_category_id": "12",
		"name": "SET TENANT",
		"description": "\\n  语法：\\n\\n  SET [SESSION | GLOBAL] TENANT tenantname\\n\\n  切换租户\\n\\n",
		"example": "",
		"url": URL
	},
	# 6000 - 7000 sys config
	{
		"help_topic_id": "6001",
		"help_category_id": str(sys_config_category_id),
		"name": "data_dir",
		"description": "\\n  type: cluster parameter\\n  info the directory for the data file\\n  default: store\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6002",
		"help_category_id": str(sys_config_category_id),
		"name": "redundancy_level",
		"description": "\\n  type: cluster parameter\\n  info EXTERNAL: use extrernal redundancyNORMAL: tolerate one disk failureHIGH: tolerate two disk failure if disk count is enough\\n  default: NORMAL\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6003",
		"help_category_id": str(sys_config_category_id),
		"name": "datafile_size",
		"description": "\\n  type: cluster parameter\\n  info size of the data file. Range: [0, +INF)\\n  range: [0M,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6004",
		"help_category_id": str(sys_config_category_id),
		"name": "datafile_next",
		"description": "\\n  type: cluster parameter\\n  info the auto extend step. Range: [0, +INF)\\n  range: [0M,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6005",
		"help_category_id": str(sys_config_category_id),
		"name": "datafile_maxsize",
		"description": "\\n  type: cluster parameter\\n  info the auto extend max size. Range: [0, +INF)\\n  range: [0M,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6006",
		"help_category_id": str(sys_config_category_id),
		"name": "_datafile_usage_upper_bound_percentage",
		"description": "\\n  type: cluster parameter\\n  info the percentage of disk space usage upper bound to trigger datafile extend. Range: [5,99] in integer\\n  range: [5,99]\\n  default: 90\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6007",
		"help_category_id": str(sys_config_category_id),
		"name": "_datafile_usage_lower_bound_percentage",
		"description": "\\n  type: cluster parameter\\n  info the percentage of disk space usage lower bound to trigger datafile shrink. Range: [5,99] in integer\\n  range: [5,99]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6008",
		"help_category_id": str(sys_config_category_id),
		"name": "datafile_disk_percentage",
		"description": "\\n  type: cluster parameter\\n  info the percentage of disk space used by the data files. Range: [5,99] in integer\\n  range: [5,99]\\n  default: 90\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6009",
		"help_category_id": str(sys_config_category_id),
		"name": "memory_reserved",
		"description": "\\n  type: cluster parameter\\n  info the size of the system memory reserved for emergency internal use. Range: [10M, total size of memory]\\n  range: [10M,)\\n  default: 500M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6010",
		"help_category_id": str(sys_config_category_id),
		"name": "config_additional_dir",
		"description": "\\n  type: cluster parameter\\n  info additional directories of configure file\\n  default: etc2etc3\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6011",
		"help_category_id": str(sys_config_category_id),
		"name": "leak_mod_to_check",
		"description": "\\n  type: cluster parameter\\n  info the name of the module under memory leak checks\\n  default: NONE\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6012",
		"help_category_id": str(sys_config_category_id),
		"name": "rpc_port",
		"description": "\\n  type: cluster parameter\\n  info the port number for RPC protocol. Range: (1024. 65536) in integer\\n  range: (1024,65536)\\n  default: 2500\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6013",
		"help_category_id": str(sys_config_category_id),
		"name": "mysql_port",
		"description": "\\n  type: cluster parameter\\n  info port number for mysql connection. Range: (1024, 65536) in integer\\n  range: (1024,65536)\\n  default: 2880\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6014",
		"help_category_id": str(sys_config_category_id),
		"name": "devname",
		"description": "\\n  type: cluster parameter\\n  info name of network adapter\\n  default: bond0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6015",
		"help_category_id": str(sys_config_category_id),
		"name": "zone",
		"description": "\\n  type: cluster parameter\\n  info specifies the zone name\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6016",
		"help_category_id": str(sys_config_category_id),
		"name": "internal_sql_execute_timeout",
		"description": "\\n  type: cluster parameter\\n  info the number of microseconds an internal DML request is permitted to execute before it is terminated. Range: [1000us, 10m]\\n  range: [1000us, 10m]\\n  default: 30s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6017",
		"help_category_id": str(sys_config_category_id),
		"name": "net_thread_count",
		"description": "\\n  type: cluster parameter\\n  info the number of rpc/mysql I/O threads for Libeasy. Range: [0, 64] in integer, 0 stands for max(6, CPU_NUM/8)\\n  range: [0,64]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6018",
		"help_category_id": str(sys_config_category_id),
		"name": "high_priority_net_thread_count",
		"description": "\\n  type: cluster parameter\\n  info the number of rpc I/O threads for high priority messages, 0 means set off. Range: [0, 64] in integer\\n  range: [0,64]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6019",
		"help_category_id": str(sys_config_category_id),
		"name": "tenant_task_queue_size",
		"description": "\\n  type: cluster parameter\\n  info the size of the task queue for each tenant. Range: [1024,+INF)\\n  range: [1024,]\\n  default: 65536\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6020",
		"help_category_id": str(sys_config_category_id),
		"name": "memory_limit",
		"description": "\\n  type: cluster parameter\\n  info the size of the memory reserved for internal use(for testing purpose), 0 means follow memory_limit_percentage. Range: 0, [8G,)\\n  range: [0M,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6021",
		"help_category_id": str(sys_config_category_id),
		"name": "rootservice_memory_limit",
		"description": "\\n  type: cluster parameter\\n  info max memory size which can be used by rs tenant The default value is 2G. Range: [2G,)\\n  range: [2G,)\\n  default: 2G\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6022",
		"help_category_id": str(sys_config_category_id),
		"name": "system_memory",
		"description": "\\n  type: cluster parameter\\n  info the memory reserved for internal use which cannot be allocated to any outer-tenant, and should be determined to guarantee every server functions normally. Range: [0M,)\\n  range: [0M,)\\n  default: 30G\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6023",
		"help_category_id": str(sys_config_category_id),
		"name": "minor_warm_up_duration_time",
		"description": "\\n  type: cluster parameter\\n  info warm up duration time for minor freeze. Range: [0s,60m]\\n  range: [0s, 60m]\\n  default: 30s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6024",
		"help_category_id": str(sys_config_category_id),
		"name": "minor_deferred_gc_time",
		"description": "\\n  type: cluster parameter\\n  info sstable deferred gc time after merge. Range: [0s,24h]\\n  range: [0s, 24h]\\n  default: 0s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6025",
		"help_category_id": str(sys_config_category_id),
		"name": "_minor_deferred_gc_level",
		"description": "\\n  type: cluster parameter\\n  info minor deferred gc_level, 0 means defer gc L0, 1 means defer gc L0/L1\\n  range: [0, 1]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6026",
		"help_category_id": str(sys_config_category_id),
		"name": "cpu_count",
		"description": "\\n  type: cluster parameter\\n  info the number of CPU\\'s in the system. If this parameter is set to zero, the number will be set according to sysconf otherwise, this parameter is used. Range: [0,+INF) in integer\\n  range: [0,]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6027",
		"help_category_id": str(sys_config_category_id),
		"name": "cpu_reserved",
		"description": "\\n  type: cluster parameter\\n  info the number of CPU\\'s reserved for system usage. Range: [0, 15] in integer\\n  range: [0,15]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6028",
		"help_category_id": str(sys_config_category_id),
		"name": "trace_log_sampling_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval for periodically printing log info in trace log. When force_trace_log is set to FALSE, for each time interval specifies by sampling_trace_log_interval, logging info regarding \\'slow query\\' and \\'white list\\' will be printed out. Range: [0ms,+INF)\\n  range: [0ms,]\\n  default: 10ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6029",
		"help_category_id": str(sys_config_category_id),
		"name": "trace_log_slow_query_watermark",
		"description": "\\n  type: cluster parameter\\n  info the threshold of execution time (in milliseconds) of a query beyond which it is considered to be a \\'slow query\\'. Range: [1ms,+INF)\\n  range: [1ms,]\\n  default: 1s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6030",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_one_phase_commit",
		"description": "\\n  type: cluster parameter\\n  info enable one phase commit optimization\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6031",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_pg",
		"description": "\\n  type: cluster parameter\\n  info open partition group\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6032",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_record_trace_log",
		"description": "\\n  type: cluster parameter\\n  info specifies whether to always record the trace log. The default value is True.\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6033",
		"help_category_id": str(sys_config_category_id),
		"name": "system_trace_level",
		"description": "\\n  type: cluster parameter\\n  info system trace log level, 0:none, 1:standard, 2:debug. The default log level for trace log is 1\\n  range: [0,2]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6034",
		"help_category_id": str(sys_config_category_id),
		"name": "max_string_print_length",
		"description": "\\n  type: cluster parameter\\n  info truncate very long string when printing to log file. Range: [0,]\\n  range: [0,]\\n  default: 500\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6035",
		"help_category_id": str(sys_config_category_id),
		"name": "sql_audit_memory_limit",
		"description": "\\n  type: cluster parameter\\n  info the maximum size of the memory used by SQL audit virtual table when the function is turned on. The upper limit is 3G, with defaut 10% of avaiable memory. Range: [64M, +INF]\\n  range: [64M,]\\n  default: 3G\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6036",
		"help_category_id": str(sys_config_category_id),
		"name": "sql_audit_queue_size",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of the records in SQL audit virtual table. (don\\'t use now)The default value is 10000000. Range: [10000, +INF], integer\\n  range: [10000,]\\n  default: 10000000\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6037",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_sql_audit",
		"description": "\\n  type: cluster parameter\\n  info specifies whether SQL audit is turned on. The default value is TRUE. Value: TRUE: turned on FALSE: turned off\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6038",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_record_trace_id",
		"description": "\\n  type: cluster parameter\\n  info specifies whether record app trace id is turned on.\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6039",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_rich_error_msg",
		"description": "\\n  type: cluster parameter\\n  info specifies whether add ip:port, time and trace id to user error message. The default value is FALSE.\\n  default: false\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6040",
		"help_category_id": str(sys_config_category_id),
		"name": "debug_sync_timeout",
		"description": "\\n  type: cluster parameter\\n  info Enable the debug sync facility and optionally specify a default wait timeout in micro seconds. A zero value keeps the facility disabled, Range: [0, +INF]\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6041",
		"help_category_id": str(sys_config_category_id),
		"name": "dead_socket_detection_timeout",
		"description": "\\n  type: cluster parameter\\n  info specify a tcp_user_timeout for RFC5482. A zero value makes the option disabled, Range: [0, 2h]\\n  range: [0s,2h]\\n  default: 10s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6042",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_perf_event",
		"description": "\\n  type: cluster parameter\\n  info specifies whether to enable perf event feature. The default value is False.\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6043",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_separate_sys_clog",
		"description": "\\n  type: cluster parameter\\n  info separate system and user commit log. The default value is False.\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6044",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_upgrade_mode",
		"description": "\\n  type: cluster parameter\\n  info specifies whether upgrade mode is turned on. If turned on, daily merger and balancer will be disabled. Value: True: turned on False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6045",
		"help_category_id": str(sys_config_category_id),
		"name": "schema_history_expire_time",
		"description": "\\n  type: cluster parameter\\n  info the hour of expire time for schema history, from 1hour to 30days, with default 7days. Range: [1h, 30d]\\n  range: [1m, 30d]\\n  default: 7d\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6046",
		"help_category_id": str(sys_config_category_id),
		"name": "default_compress_func",
		"description": "\\n  type: cluster parameter\\n  info default compress function name for create new table, values: none, lz4_1.0, snappy_1.0, zlib_1.0, zstd_1.0, zstd_1.3.8\\n  default: zstd_1.3.8\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6047",
		"help_category_id": str(sys_config_category_id),
		"name": "default_row_format",
		"description": "\\n  type: cluster parameter\\n  info default row format in mysql mode\\n  default: compact\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6048",
		"help_category_id": str(sys_config_category_id),
		"name": "default_compress",
		"description": "\\n  type: cluster parameter\\n  info default compress strategy for create new table within oracle mode\\n  default: oltp\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6049",
		"help_category_id": str(sys_config_category_id),
		"name": "_partition_balance_strategy",
		"description": "\\n  type: cluster parameter\\n  info specifies the partition balance strategy. Value: [auto]: partition and shard amount with disk utilization strategy is used, Value: [standard]: partition amout with disk utilization stragegy is used, Value: [disk_utilization_only]: disk utilization strategy is used.\\n  default: auto\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6050",
		"help_category_id": str(sys_config_category_id),
		"name": "weak_read_version_refresh_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval to refresh cluster weak read version Range: [0ms, +INF)\\n  range: [0ms,)\\n  default: 50ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6051",
		"help_category_id": str(sys_config_category_id),
		"name": "rootservice_list",
		"description": "\\n  type: cluster parameter\\n  info a list of servers against which election candidate is checked for validation\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6052",
		"help_category_id": str(sys_config_category_id),
		"name": "cluster",
		"description": "\\n  type: cluster parameter\\n  info Name of the cluster\\n  default: obcluster\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6053",
		"help_category_id": str(sys_config_category_id),
		"name": "cluster_id",
		"description": "\\n  type: cluster parameter\\n  info ID of the cluster\\n  range: [1,4294901759]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6054",
		"help_category_id": str(sys_config_category_id),
		"name": "obconfig_url",
		"description": "\\n  type: cluster parameter\\n  info URL for OB_Config service\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6055",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_syslog_file_compress",
		"description": "\\n  type: cluster parameter\\n  info specifies whether to compress archive log filesValue: True:turned on False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6056",
		"help_category_id": str(sys_config_category_id),
		"name": "max_syslog_file_time",
		"description": "\\n  type: cluster parameter\\n  info specifies the maximum retention time of the log files. When this value is set to 0s, no log file will be removed due to time. with default 0s. Range: [0s, 3650d]\\n  range: [0s, 3650d]\\n  default: 0s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6057",
		"help_category_id": str(sys_config_category_id),
		"name": "syslog_level",
		"description": "\\n  type: cluster parameter\\n  info specifies the current level of logging. There are DEBUG, TRACE, INFO, WARN, USER_ERR, ERROR, six different log levels.\\n  default: INFO\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6058",
		"help_category_id": str(sys_config_category_id),
		"name": "syslog_io_bandwidth_limit",
		"description": "\\n  type: cluster parameter\\n  info Syslog IO bandwidth limitation, exceeding syslog would be truncated. Use 0 to disable ERROR log.\\n  range: \\n  default: 30MB\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6059",
		"help_category_id": str(sys_config_category_id),
		"name": "max_syslog_file_count",
		"description": "\\n  type: cluster parameter\\n  info specifies the maximum number of the log files that can co-exist before the log file recycling kicks in. Each log file can occupy at most 256MB disk space. When this value is set to 0, no log file will be removed due to the file count. Range: [0, +INF) in integer\\n  range: [0,]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6060",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_async_syslog",
		"description": "\\n  type: cluster parameter\\n  info specifies whether use async log for observer.log, elec.log and rs.log\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6061",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_syslog_wf",
		"description": "\\n  type: cluster parameter\\n  info specifies whether any log message with a log level higher than \\'WARN\\' would be printed into a separate file with a suffix of \\'wf\\'\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6062",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_syslog_recycle",
		"description": "\\n  type: cluster parameter\\n  info specifies whether log file recycling is turned on. Value: True:turned on False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6063",
		"help_category_id": str(sys_config_category_id),
		"name": "memory_limit_percentage",
		"description": "\\n  type: cluster parameter\\n  info the size of the memory reserved for internal use(for testing purpose). Range: [10, 90]\\n  range: [10, 90]\\n  default: 80\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6064",
		"help_category_id": str(sys_config_category_id),
		"name": "cache_wash_threshold",
		"description": "\\n  type: cluster parameter\\n  info size of remaining memory at which cache eviction will be triggered. Range: [0,+INF)\\n  range: [0B,]\\n  default: 4GB\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6065",
		"help_category_id": str(sys_config_category_id),
		"name": "memory_chunk_cache_size",
		"description": "\\n  type: cluster parameter\\n  info the maximum size of memory cached by memory chunk cache. Range: [0M,], 0 stands for adaptive\\n  range: [0M,]\\n  default: 0M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6066",
		"help_category_id": str(sys_config_category_id),
		"name": "autoinc_cache_refresh_interval",
		"description": "\\n  type: cluster parameter\\n  info auto-increment service cache refresh sync_value in this interval, with default 3600s. Range: [100ms, +INF)\\n  range: [100ms,]\\n  default: 3600s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6067",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_sql_operator_dump",
		"description": "\\n  type: cluster parameter\\n  info specifies whether sql operators (sort/hash join/material/window function/interm result/...) allowed to write to disk\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6068",
		"help_category_id": str(sys_config_category_id),
		"name": "_chunk_row_store_mem_limit",
		"description": "\\n  type: cluster parameter\\n  info the maximum size of memory used by ChunkRowStore, 0 means follow operator\\'s setting. Range: [0, +INF)\\n  range: [0,+INF]\\n  default: 0B\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6069",
		"help_category_id": str(sys_config_category_id),
		"name": "tableapi_transport_compress_func",
		"description": "\\n  type: cluster parameter\\n  info compressor used for tableAPI query result. Values: none, lz4_1.0, snappy_1.0, zlib_1.0, zstd_1.0 zstd 1.3.8\\n  default: none\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6070",
		"help_category_id": str(sys_config_category_id),
		"name": "_sort_area_size",
		"description": "\\n  type: tenant parameter\\n  info size of maximum memory that could be used by SORT. Range: [2M,+INF)\\n  range: [2M,+INF]\\n  default: 128M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6071",
		"help_category_id": str(sys_config_category_id),
		"name": "_hash_area_size",
		"description": "\\n  type: tenant parameter\\n  info size of maximum memory that could be used by HASH JOIN. Range: [4M,+INF)\\n  range: [4M, +INF]\\n  default: 100M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6072",
		"help_category_id": str(sys_config_category_id),
		"name": "rebuild_replica_data_lag_threshold",
		"description": "\\n  type: cluster parameter\\n  info size of clog files that a replica lag behind leader to trigger rebuild, 0 means never trigger rebuild on purpose. Range: [0, +INF)\\n  range: [0, +INF\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6073",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_static_typing_engine",
		"description": "\\n  type: cluster parameter\\n  info specifies whether static typing sql execution engine is activated\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6074",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_defensive_check",
		"description": "\\n  type: cluster parameter\\n  info specifies whether allow to do some defensive checks when the query is executed\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6075",
		"help_category_id": str(sys_config_category_id),
		"name": "max_stale_time_for_weak_consistency",
		"description": "\\n  type: tenant parameter\\n  info the max data stale time that cluster weak read version behind current timestamp,no smaller than weak_read_version_refresh_interval, range: [5s, +INF)\\n  range: [5s,)\\n  default: 5s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6076",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_monotonic_weak_read",
		"description": "\\n  type: tenant parameter\\n  info specifies observer supportting atomicity and monotonic order read\\n  default: false\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6077",
		"help_category_id": str(sys_config_category_id),
		"name": "tenant_cpu_variation_per_server",
		"description": "\\n  type: cluster parameter\\n  info the percentage variation for any tenant\\'s CPU quota allocation on each observer. The default value is 50(%). Range: [0, 100] in percentage\\n  range: [0,100]\\n  default: 50\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6078",
		"help_category_id": str(sys_config_category_id),
		"name": "system_cpu_quota",
		"description": "\\n  type: cluster parameter\\n  info the number of vCPUs allocated to the clog tenant\\n  range: [0,16]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6079",
		"help_category_id": str(sys_config_category_id),
		"name": "server_cpu_quota_min",
		"description": "\\n  type: cluster parameter\\n  info the number of minimal vCPUs allocated to the server tenant(a special internal tenant that exists on every observer). Range: [0, 16]\\n  range: [0,16]\\n  default: 2.5\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6080",
		"help_category_id": str(sys_config_category_id),
		"name": "server_cpu_quota_max",
		"description": "\\n  type: cluster parameter\\n  info the number of maximal vCPUs allocated to the server tenant(a special internal tenant that exists on every observer). Range: [0, 16]\\n  range: [0,16]\\n  default: 5\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6081",
		"help_category_id": str(sys_config_category_id),
		"name": "election_cpu_quota",
		"description": "\\n  type: cluster parameter\\n  info the number of vCPUs allocated to the \\'election\\' tenant. Range: [0,10] in integer\\n  range: [0,10]\\n  default: 3\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6082",
		"help_category_id": str(sys_config_category_id),
		"name": "location_cache_cpu_quota",
		"description": "\\n  type: cluster parameter\\n  info the number of vCPUs allocated for the requests regarding location info of the core tables. Range: [0,10] in integer\\n  range: [0,10]\\n  default: 5\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6083",
		"help_category_id": str(sys_config_category_id),
		"name": "workers_per_cpu_quota",
		"description": "\\n  type: cluster parameter\\n  info the ratio(integer) between the number of system allocated workers vs the maximum number of threads that can be scheduled concurrently. Range: [2, 20]\\n  range: [2,20]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6084",
		"help_category_id": str(sys_config_category_id),
		"name": "large_query_worker_percentage",
		"description": "\\n  type: cluster parameter\\n  info the percentage of the workers reserved to serve large query requests. Range: [0, 100] in percentage\\n  range: [0,100]\\n  default: 30\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6085",
		"help_category_id": str(sys_config_category_id),
		"name": "large_query_threshold",
		"description": "\\n  type: cluster parameter\\n  info threshold for execution time beyond which a request may be paused and rescheduled as a \\'large request\\'. Range: [1ms, +INF)\\n  range: [1ms,)\\n  default: 5s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6086",
		"help_category_id": str(sys_config_category_id),
		"name": "cpu_quota_concurrency",
		"description": "\\n  type: cluster parameter\\n  info max allowed concurrency for 1 CPU quota. Range: [1,10]\\n  range: [1,10]\\n  default: 4\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6087",
		"help_category_id": str(sys_config_category_id),
		"name": "token_reserved_percentage",
		"description": "\\n  type: cluster parameter\\n  info specifies the amount of token increase allocated to a tenant based on his consumption from the last round (without exceeding his upper limit). Range: [0, 100] in percentage\\n  range: [0,100]\\n  default: 30\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6088",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_global_freeze_trigger",
		"description": "\\n  type: cluster parameter\\n  info specifies whether to trigger major freeze when global active memstore used reach global_freeze_trigger_percentage, Value:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6089",
		"help_category_id": str(sys_config_category_id),
		"name": "global_major_freeze_residual_memory",
		"description": "\\n  type: cluster parameter\\n  info post global major freeze when observer memstore free memory(plus memory held by frozen memstore and blockcache) reach this limit. Rang:(0, 100)limit calc by (memory_limit - system_memory) * global_major_freeze_residual_memory/100\\n  range: (0, 100)\\n  default: 40\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6090",
		"help_category_id": str(sys_config_category_id),
		"name": "global_write_halt_residual_memory",
		"description": "\\n  type: cluster parameter\\n  info disable write to memstore when observer memstore free memory(plus memory held by blockcache) lower than this limit, Range: (0, 100)limit calc by (memory_limit - system_memory) * global_write_halt_residual_memory/100\\n  range: (0, 100)\\n  default: 30\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6091",
		"help_category_id": str(sys_config_category_id),
		"name": "px_workers_per_cpu_quota",
		"description": "\\n  type: cluster parameter\\n  info the ratio(integer) between the number of system allocated px workers vs the maximum number of threads that can be scheduled concurrently. Range: [0, 20]\\n  range: [0,20]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6092",
		"help_category_id": str(sys_config_category_id),
		"name": "memstore_limit_percentage",
		"description": "\\n  type: cluster parameter\\n  info used in calculating the value of MEMSTORE_LIMIT parameter: memstore_limit_percentage = memstore_limit / min_memory, min_memory, where MIN_MEMORY is determined when the tenant is created. Range: (0, 100)\\n  range: (0, 100)\\n  default: 50\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6093",
		"help_category_id": str(sys_config_category_id),
		"name": "freeze_trigger_percentage",
		"description": "\\n  type: cluster parameter\\n  info the threshold of the size of the mem store when freeze will be triggered. Range: (0, 100)\\n  range: (0, 100)\\n  default: 50\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6094",
		"help_category_id": str(sys_config_category_id),
		"name": "writing_throttling_trigger_percentage",
		"description": "\\n  type: tenant parameter\\n  info the threshold of the size of the mem store when writing_limit will be triggered. Rang:(0, 100]. setting 100 means turn off writing limit\\n  range: (0, 100]\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6095",
		"help_category_id": str(sys_config_category_id),
		"name": "writing_throttling_maximum_duration",
		"description": "\\n  type: tenant parameter\\n  info maximum duration of writing throttling(in minutes), max value is 3 days\\n  range: [1s, 3d]\\n  default: 1h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6096",
		"help_category_id": str(sys_config_category_id),
		"name": "plan_cache_high_watermark",
		"description": "\\n  type: cluster parameter\\n  info (don\\'t use now) memory usage at which plan cache eviction will be trigger immediately. Range: [0, +INF)\\n  range: [0, +INF)\\n  default: 2000M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6097",
		"help_category_id": str(sys_config_category_id),
		"name": "plan_cache_low_watermark",
		"description": "\\n  type: cluster parameter\\n  info (don\\'t use now) memory usage at which plan cache eviction will be stopped. Range: [0, plan_cache_high_watermark)\\n  range: [0, plan_cache_high_watermark)\\n  default: 1500M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6098",
		"help_category_id": str(sys_config_category_id),
		"name": "plan_cache_evict_interval",
		"description": "\\n  type: cluster parameter\\n  info time interval for periodic plan cache eviction. Range: [0s, +INF)\\n  range: [0s,)\\n  default: 1s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6099",
		"help_category_id": str(sys_config_category_id),
		"name": "max_px_worker_count",
		"description": "\\n  type: cluster parameter\\n  info maximum parallel execution worker count can be used for all parallel requests. Range: [0, 65535]\\n  range: [0,65535]\\n  default: 64\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6100",
		"help_category_id": str(sys_config_category_id),
		"name": "default_progressive_merge_num",
		"description": "\\n  type: tenant parameter\\n  info default progressive_merge_num when tenant create tableRange: [0,)\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6101",
		"help_category_id": str(sys_config_category_id),
		"name": "_parallel_min_message_pool",
		"description": "\\n  type: tenant parameter\\n  info DTL message buffer pool reserve the mininum size after extend the size. Range: [16M,8G]\\n  range: [16M, 8G]\\n  default: 400M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6102",
		"help_category_id": str(sys_config_category_id),
		"name": "_parallel_server_sleep_time",
		"description": "\\n  type: tenant parameter\\n  info sleep time between get channel data in millisecond. Range: [0, 2000]\\n  range: [0, 2000]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6103",
		"help_category_id": str(sys_config_category_id),
		"name": "_px_max_message_pool_pct",
		"description": "\\n  type: tenant parameter\\n  info The maxinum percent of tenant memory that DTL message buffer pool can alloc memory. Range: [0,90]\\n  range: [0,90]\\n  default: 40\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6104",
		"help_category_id": str(sys_config_category_id),
		"name": "_px_message_compression",
		"description": "\\n  type: tenant parameter\\n  info Enable DTL send message with compressionValue: True: enable compression False: disable compression\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6105",
		"help_category_id": str(sys_config_category_id),
		"name": "_px_chunklist_count_ratio",
		"description": "\\n  type: cluster parameter\\n  info the ratio of the dtl buffer manager list. Range: [1, 128]\\n  range: [1, 128]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6106",
		"help_category_id": str(sys_config_category_id),
		"name": "_force_hash_join_spill",
		"description": "\\n  type: tenant parameter\\n  info force hash join to dump after get all build hash table Value:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6107",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_hash_join_hasher",
		"description": "\\n  type: tenant parameter\\n  info which hash function to choose for hash join 1: murmurhash, 2: crc, 4: xxhash\\n  range: [1, 7]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6108",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_hash_join_processor",
		"description": "\\n  type: tenant parameter\\n  info which path to process for hash join, default 7 to auto choose 1: nest loop, 2: recursive, 4: in-memory\\n  range: [1, 7]\\n  default: 7\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6109",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_filter_push_down_storage",
		"description": "\\n  type: tenant parameter\\n  info Enable filter push down to storageValue:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6110",
		"help_category_id": str(sys_config_category_id),
		"name": "workarea_size_policy",
		"description": "\\n  type: tenant parameter\\n  info policy used to size SQL working areas (MANUAL/AUTO)\\n  default: AUTO\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6111",
		"help_category_id": str(sys_config_category_id),
		"name": "_gts_core_num",
		"description": "\\n  type: tenant parameter\\n  info core num allocate for gts service. Range: [0, 256]\\n  range: [0, 256]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6112",
		"help_category_id": str(sys_config_category_id),
		"name": "_create_table_partition_distribution_strategy",
		"description": "\\n  type: cluster parameter\\n  info which distribution strategy for create table partition 1: distribute partition by remainder, 2: distribute partition by division\\n  range: [1, 2]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6113",
		"help_category_id": str(sys_config_category_id),
		"name": "__enable_identical_partition_distribution",
		"description": "\\n  type: cluster parameter\\n  info Identify partition distribution methods true: partition leader/follow isomorphism, false: partition leader/follow isomerism\\n  default: false\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6114",
		"help_category_id": str(sys_config_category_id),
		"name": "_temporary_file_io_area_size",
		"description": "\\n  type: tenant parameter\\n  info memory buffer size of temporary file, as a percentage of total tenant memory. Range: [0, 50), percentage\\n  range: [0, 50)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6115",
		"help_category_id": str(sys_config_category_id),
		"name": "__enable_small_tenant",
		"description": "\\n  type: cluster parameter\\n  info specify when small tenant configuration is on true: turn small tenant configuration on, false: turn small tenant configuration off\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6116",
		"help_category_id": str(sys_config_category_id),
		"name": "all_cluster_list",
		"description": "\\n  type: cluster parameter\\n  info a list of servers which access the same config_url\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6117",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_rootservice_standalone",
		"description": "\\n  type: cluster parameter\\n  info specifies whether the \\'SYS\\' tenant is allowed to occupy an observer exclusively, thus running in the \\'standalone\\' mode. Value:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6118",
		"help_category_id": str(sys_config_category_id),
		"name": "lease_time",
		"description": "\\n  type: cluster parameter\\n  info Lease for current heartbeat. If the root server does not received any heartbeat from an observer in lease_time seconds, that observer is considered to be offline. Not recommended for modification. Range: [1s, 5m]\\n  range: [1s, 5m]\\n  default: 10s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6119",
		"help_category_id": str(sys_config_category_id),
		"name": "switchover_process_thread_count",
		"description": "\\n  type: cluster parameter\\n  info maximum of threads allowed for executing switchover task at rootserver. Range: [1, 1000]\\n  range: [1,1000]\\n  default: 6\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6120",
		"help_category_id": str(sys_config_category_id),
		"name": "rootservice_async_task_thread_count",
		"description": "\\n  type: cluster parameter\\n  info maximum of threads allowed for executing asynchronous task at rootserver. Range: [1, 10]\\n  range: [1,10]\\n  default: 4\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6121",
		"help_category_id": str(sys_config_category_id),
		"name": "rootservice_async_task_queue_size",
		"description": "\\n  type: cluster parameter\\n  info the size of the queue for all asynchronous tasks at rootserver. Range: [8, 131072] in integer\\n  range: [8,131072]\\n  default: 16384\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6122",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_sys_table_ddl",
		"description": "\\n  type: cluster parameter\\n  info specifies whether a \\'system\\' table is allowed be to created manually. Value: True: allowed False: not allowed\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6123",
		"help_category_id": str(sys_config_category_id),
		"name": "auto_leader_switch_interval",
		"description": "\\n  type: cluster parameter\\n  info time interval for periodic leadership reorganization taking place. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 30s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6124",
		"help_category_id": str(sys_config_category_id),
		"name": "wait_leader_batch_count",
		"description": "\\n  type: cluster parameter\\n  info leader batch count everytime leader coordinator wait. Range: [128, 5000]\\n  range: [128, 5000]\\n  default: 1024\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6125",
		"help_category_id": str(sys_config_category_id),
		"name": "merger_switch_leader_duration_time",
		"description": "\\n  type: cluster parameter\\n  info switch leader duration time for daily merge. Range: [0s,30m]\\n  range: [0s,30m]\\n  default: 3m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6126",
		"help_category_id": str(sys_config_category_id),
		"name": "merger_warm_up_duration_time",
		"description": "\\n  type: cluster parameter\\n  info warm up duration time for daily merge. Range: [0s,60m]\\n  range: [0s,60m]\\n  default: 0s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6127",
		"help_category_id": str(sys_config_category_id),
		"name": "server_temporary_offline_time",
		"description": "\\n  type: cluster parameter\\n  info the time interval between two heartbeats beyond which a server is considered to be \\'temporarily\\' offline. Range: [15s, +INF)\\n  range: [15s,)\\n  default: 60s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6128",
		"help_category_id": str(sys_config_category_id),
		"name": "server_permanent_offline_time",
		"description": "\\n  type: cluster parameter\\n  info the time interval between any two heartbeats beyond which a server is considered to be \\'permanently\\' offline. Range: [20s,+INF)\\n  range: [20s,)\\n  default: 3600s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6129",
		"help_category_id": str(sys_config_category_id),
		"name": "migration_disable_time",
		"description": "\\n  type: cluster parameter\\n  info the duration in which the observer stays in the \\'block_migrate_in\\' status, which means no partition is allowed to migrate into the server. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 3600s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6130",
		"help_category_id": str(sys_config_category_id),
		"name": "server_check_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval between schedules of a task that examines the __all_server table. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 30s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6131",
		"help_category_id": str(sys_config_category_id),
		"name": "rootservice_ready_check_interval",
		"description": "\\n  type: cluster parameter\\n  info the interval between the schedule of the task that checks on the status of the ZONE during restarting. Range: [100000us, 1m]\\n  range: [100000us, 1m]\\n  default: 3s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6132",
		"help_category_id": str(sys_config_category_id),
		"name": "partition_table_scan_batch_count",
		"description": "\\n  type: cluster parameter\\n  info the number of partition replication info that will be read by each request on the partition-related system tables during procedures such as load-balancing, daily merge, election and etc. Range: (0,65536]\\n  range: (0, 65536]\\n  default: 999\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6133",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_auto_leader_switch",
		"description": "\\n  type: cluster parameter\\n  info specifies whether partition leadership auto-switch is turned on. Value:  True:turned on  False: turned off\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6134",
		"help_category_id": str(sys_config_category_id),
		"name": "replica_safe_remove_time",
		"description": "\\n  type: cluster parameter\\n  info the time interval that replica not existed has not been modified beyond which a replica is considered can be safely removed. Range: [1m,+INF)\\n  range: [1m,)\\n  default: 2h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6135",
		"help_category_id": str(sys_config_category_id),
		"name": "partition_table_check_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval that observer remove replica which not exist in observer from partition table. Range: [1m,+INF)\\n  range: [1m,)\\n  default: 30m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6136",
		"help_category_id": str(sys_config_category_id),
		"name": "get_leader_candidate_rpc_timeout",
		"description": "\\n  type: cluster parameter\\n  info the time during a get leader candidate rpc request is permitted to execute before it is terminated. Range: [2s, 180s]\\n  range: [2s, 180s]\\n  default: 9s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6137",
		"help_category_id": str(sys_config_category_id),
		"name": "min_observer_version",
		"description": "\\n  type: cluster parameter\\n  info the min observer version\\n  default: 3.1.4\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6138",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_ddl",
		"description": "\\n  type: cluster parameter\\n  info specifies whether DDL operation is turned on. Value:  True:turned on  False: turned off\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6139",
		"help_category_id": str(sys_config_category_id),
		"name": "global_index_build_single_replica_timeout",
		"description": "\\n  type: cluster parameter\\n  info build single replica task timeout when rootservice schedule to build global index. Range: [1h,+INF)\\n  range: [1h,)\\n  default: 48h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6140",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_major_freeze",
		"description": "\\n  type: cluster parameter\\n  info specifies whether major_freeze function is turned on. Value:  True:turned on  False: turned off\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6141",
		"help_category_id": str(sys_config_category_id),
		"name": "ob_event_history_recycle_interval",
		"description": "\\n  type: cluster parameter\\n  info the time to recycle event history. Range: [1d, 180d]\\n  range: [1d, 180d]\\n  default: 7d\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6142",
		"help_category_id": str(sys_config_category_id),
		"name": "_recyclebin_object_purge_frequency",
		"description": "\\n  type: cluster parameter\\n  info the time to purge recyclebin. Range: [0m, +INF)\\n  range: [0m,)\\n  default: 10m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6143",
		"help_category_id": str(sys_config_category_id),
		"name": "recyclebin_object_expire_time",
		"description": "\\n  type: cluster parameter\\n  info recyclebin object expire time, default 0 that means auto purge recyclebin off. Range: [0s, +INF)\\n  range: [0s,)\\n  default: 0s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6144",
		"help_category_id": str(sys_config_category_id),
		"name": "resource_soft_limit",
		"description": "\\n  type: cluster parameter\\n  info Used along with resource_hard_limit in unit allocation. If server utilization is less than resource_soft_limit, a policy of \\'best fit\\' will be used for unit allocation otherwise, a \\'least load\\' policy will be employed. Ultimately,system utilization should not be larger than resource_hard_limit. Range: (0,10000] in in\\n  range: (0, 10000]\\n  default: 50\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6145",
		"help_category_id": str(sys_config_category_id),
		"name": "resource_hard_limit",
		"description": "\\n  type: cluster parameter\\n  info Used along with resource_soft_limit in unit allocation. If server utilization is less than resource_soft_limit, a policy of \\'best fit\\' will be used for unit allocation otherwise, a \\'least load\\' policy will be employed. Ultimately,system utilization should not be larger than resource_hard_limit. Range: (0,10000] in integer\\n  range: (0, 10000]\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6146",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_rereplication",
		"description": "\\n  type: cluster parameter\\n  info specifies whether the partition auto-replication is turned on. Value:  True:turned on  False: turned off\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6147",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_rebalance",
		"description": "\\n  type: cluster parameter\\n  info specifies whether the partition load-balancing is turned on. Value:  True:turned on  False: turned off\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6148",
		"help_category_id": str(sys_config_category_id),
		"name": "balancer_idle_time",
		"description": "\\n  type: cluster parameter\\n  info the time interval between the schedules of the partition load-balancing task. Range: [10s, +INF)\\n  range: [10s,]\\n  default: 5m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6149",
		"help_category_id": str(sys_config_category_id),
		"name": "balancer_tolerance_percentage",
		"description": "\\n  type: cluster parameter\\n  info specifies the tolerance (in percentage) of the unbalance of the disk space utilization among all units. The average disk space utilization is calculated by dividing the total space by the number of units. For example, say balancer_tolerance_percentage is set to 10 and a tenant has two units in the system, the average disk use for each unit should be about the same, thus 50% of the total value. Therefore, the system will start a rebalancing task when any unit\\'s disk space goes beyond +-10% of the average usage. Range: [1, 100) in percentage\\n  range: [1, 100)\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6150",
		"help_category_id": str(sys_config_category_id),
		"name": "balancer_emergency_percentage",
		"description": "\\n  type: cluster parameter\\n  info Unit load balance is disabled while zone is merging.But for unit load above emergency percentage suituation, will always try migrate out partitions.Range: [1, 100] in percentage\\n  range: [1, 100]\\n  default: 80\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6151",
		"help_category_id": str(sys_config_category_id),
		"name": "server_data_copy_in_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of partitions allowed to migrate to the server. Range: [1, 1000], integer\\n  range: [1,1000]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6152",
		"help_category_id": str(sys_config_category_id),
		"name": "server_data_copy_out_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of partitions allowed to migrate from the server. Range: [1, 1000], integer\\n  range: [1,1000]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6153",
		"help_category_id": str(sys_config_category_id),
		"name": "data_copy_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of the data replication tasks. Range: [1,+INF) in integer\\n  range: [1,)\\n  default: 20\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6154",
		"help_category_id": str(sys_config_category_id),
		"name": "fast_recovery_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of the datafile fast recovery tasks. Range: [1,1000] in integer\\n  range: [1,1000]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6155",
		"help_category_id": str(sys_config_category_id),
		"name": "balancer_task_timeout",
		"description": "\\n  type: cluster parameter\\n  info the time to execute the load-balancing task before it is terminated. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 20m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6156",
		"help_category_id": str(sys_config_category_id),
		"name": "balancer_timeout_check_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval between the schedules of the task that checks whether the partition load balancing task has timed-out. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 1m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6157",
		"help_category_id": str(sys_config_category_id),
		"name": "balancer_log_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval between logging the load-balancing task\\'s statistics. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 1m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6158",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_unit_balance_resource_weight",
		"description": "\\n  type: cluster parameter\\n  info specifies whether maual configed resource weight is turned on. Value:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6159",
		"help_category_id": str(sys_config_category_id),
		"name": "unit_balance_resource_weight",
		"description": "\\n  type: cluster parameter\\n  info the percentage variation for any tenant\\'s resource weight. The default value is empty. All weight must adds up to 100 if set\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6160",
		"help_category_id": str(sys_config_category_id),
		"name": "__balance_controller",
		"description": "\\n  type: cluster parameter\\n  info specifies whether the balance events are turned on or turned off.\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6161",
		"help_category_id": str(sys_config_category_id),
		"name": "__min_full_resource_pool_memory",
		"description": "\\n  type: cluster parameter\\n  info the min memory value which is specified for a full resource pool.\\n  range: [268435456,)\\n  default: 5368709120\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6162",
		"help_category_id": str(sys_config_category_id),
		"name": "tenant_groups",
		"description": "\\n  type: cluster parameter\\n  info specifies tenant groups for server balancer.\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6163",
		"help_category_id": str(sys_config_category_id),
		"name": "server_balance_critical_disk_waterlevel",
		"description": "\\n  type: cluster parameter\\n  info disk water level to determine server balance strategy\\n  range: [0, 100]\\n  default: 80\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6164",
		"help_category_id": str(sys_config_category_id),
		"name": "server_balance_disk_tolerance_percent",
		"description": "\\n  type: cluster parameter\\n  info specifies the tolerance (in percentage) of the unbalance of the disk space utilization among all servers. The average disk space utilization is calculated by dividing the total space by the number of servers. server balancer will start a rebalancing task when the deviation between the average usage and some server load is greater than this tolerance Range: [1, 100] in percentage\\n  range: [1, 100]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6165",
		"help_category_id": str(sys_config_category_id),
		"name": "server_balance_cpu_mem_tolerance_percent",
		"description": "\\n  type: cluster parameter\\n  info specifies the tolerance (in percentage) of the unbalance of the cpu/memory utilization among all servers. The average cpu/memory utilization is calculated by dividing the total cpu/memory by the number of servers. server balancer will start a rebalancing task when the deviation between the average usage and some server load is greater than this tolerance Range: [1, 100] in percentage\\n  range: [1, 100]\\n  default: 5\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6166",
		"help_category_id": str(sys_config_category_id),
		"name": "balance_blacklist_retry_interval",
		"description": "\\n  type: cluster parameter\\n  info balance task in black list, wait a little time to add\\n  range: [0s, 180m]\\n  default: 30m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6167",
		"help_category_id": str(sys_config_category_id),
		"name": "balance_blacklist_failure_threshold",
		"description": "\\n  type: cluster parameter\\n  info a balance task failed count to be putted into blacklist\\n  range: [0, 1000]\\n  default: 5\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6168",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_sys_unit_standalone",
		"description": "\\n  type: cluster parameter\\n  info specifies whether sys unit standalone deployment is turned on. Value:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6169",
		"help_category_id": str(sys_config_category_id),
		"name": "major_freeze_duty_time",
		"description": "\\n  type: cluster parameter\\n  info the start time of system daily merge procedure. Range: [00:00, 24:00)\\n  range: [00:00, 24:00)\\n  default: 02:00\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6170",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_manual_merge",
		"description": "\\n  type: cluster parameter\\n  info specifies whether manual MERGE is turned on. Value: True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6171",
		"help_category_id": str(sys_config_category_id),
		"name": "zone_merge_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of zones which are allowed to be in the \\'MERGE\\' status concurrently. Range: [0,) in integer\\n  range: [0,]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6172",
		"help_category_id": str(sys_config_category_id),
		"name": "zone_merge_order",
		"description": "\\n  type: cluster parameter\\n  info the order of zone start merge in daily merge\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6173",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_merge_by_turn",
		"description": "\\n  type: cluster parameter\\n  info specifies whether merge tasks can be performed on different zones in an alternating fashion. Value: True:turned on False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6174",
		"help_category_id": str(sys_config_category_id),
		"name": "zone_merge_timeout",
		"description": "\\n  type: cluster parameter\\n  info the time for each zone to finish its merge process before the root service no longer consider it as in \\'MERGE\\' state. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 3h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6175",
		"help_category_id": str(sys_config_category_id),
		"name": "merger_check_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval between the schedules of the task that checks on the progress of MERGE for each zone. Range: [10s, 60m]\\n  range: [10s, 60m]\\n  default: 10m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6176",
		"help_category_id": str(sys_config_category_id),
		"name": "ignore_replica_checksum_error",
		"description": "\\n  type: cluster parameter\\n  info specifies whether error raised from the partition checksum validation can be ignored. Value: True:ignored False: not ignored\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6177",
		"help_category_id": str(sys_config_category_id),
		"name": "merger_completion_percentage",
		"description": "\\n  type: cluster parameter\\n  info the merged partition count percentage and merged data size percentage when MERGE is completed. Range: [5,100]\\n  range: [5, 100]\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6178",
		"help_category_id": str(sys_config_category_id),
		"name": "max_kept_major_version_number",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of kept major versions. Range: [1, 16] in integer\\n  range: [1, 16]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6179",
		"help_category_id": str(sys_config_category_id),
		"name": "trx_2pc_retry_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval between the retries in case of failure during a transaction\\'s two-phase commit phase. Range: [1ms,5000ms]\\n  range: [1ms, 5000ms]\\n  default: 100ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6180",
		"help_category_id": str(sys_config_category_id),
		"name": "clog_sync_time_warn_threshold",
		"description": "\\n  type: cluster parameter\\n  info the time given to the commit log synchronization between a leader and its followers before a \\'warning\\' message is printed in the log file.  Range: [1ms,1000ms]\\n  range: [1ms, 10000ms]\\n  default: 1s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6181",
		"help_category_id": str(sys_config_category_id),
		"name": "row_purge_thread_count",
		"description": "\\n  type: cluster parameter\\n  info maximum of threads allowed for executing row purge task. Range: [1, 64]\\n  range: [1,64]\\n  default: 4\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6182",
		"help_category_id": str(sys_config_category_id),
		"name": "row_compaction_update_limit",
		"description": "\\n  type: cluster parameter\\n  info maximum update count before trigger row compaction. Range: [1, 64]\\n  range: [1, 6400]\\n  default: 6\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6183",
		"help_category_id": str(sys_config_category_id),
		"name": "ignore_replay_checksum_error",
		"description": "\\n  type: cluster parameter\\n  info specifies whether error raised from the memtable replay checksum validation can be ignored. Value: True:ignored False: not ignored\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6184",
		"help_category_id": str(sys_config_category_id),
		"name": "_rpc_checksum",
		"description": "\\n  type: cluster parameter\\n  info Force: always verify Optional: verify when rpc_checksum non-zero Disable: ignore verify\\n  default: Force\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6185",
		"help_category_id": str(sys_config_category_id),
		"name": "trx_try_wait_lock_timeout",
		"description": "\\n  type: cluster parameter\\n  info the time to wait on row lock acquiring before retry. Range: [0ms, 100ms]\\n  range: [0ms, 100ms]\\n  default: 0ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6186",
		"help_category_id": str(sys_config_category_id),
		"name": "trx_force_kill_threshold",
		"description": "\\n  type: cluster parameter\\n  info the time given to the transaction to execute when major freeze or switch leader before it will be killed. Range: [1ms, 10s]\\n  range: [1ms, 10s]\\n  default: 100ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6187",
		"help_category_id": str(sys_config_category_id),
		"name": "flush_log_at_trx_commit",
		"description": "\\n  type: cluster parameter\\n  info 0: commit transactions without waiting clog write to buffer cache,1: commit transactions after clog flush to disk,2: commit transactions after clog write to buffer cache,Range: [0, 2]\\n  range: [0, 2]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6188",
		"help_category_id": str(sys_config_category_id),
		"name": "clog_disk_usage_limit_percentage",
		"description": "\\n  type: cluster parameter\\n  info maximum of clog disk usage percentage before stop submitting or receiving logs, should be greater than clog_disk_utilization_threshold. Range: [80, 100]\\n  range: [80, 100]\\n  default: 95\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6189",
		"help_category_id": str(sys_config_category_id),
		"name": "clog_disk_utilization_threshold",
		"description": "\\n  type: cluster parameter\\n  info clog disk utilization threshold before reuse clog files, should be less than clog_disk_usage_limit_percentage. Range: [10, 100)\\n  range: [10, 100)\\n  default: 80\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6190",
		"help_category_id": str(sys_config_category_id),
		"name": "election_blacklist_interval",
		"description": "\\n  type: cluster parameter\\n  info If leader_revoke, this replica cannot be elected to leader in election_blacklist_intervalThe default value is 1800s. Range: [0s, 24h]\\n  range: [0s, 24h]\\n  default: 1800s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6191",
		"help_category_id": str(sys_config_category_id),
		"name": "gts_refresh_interval",
		"description": "\\n  type: cluster parameter\\n  info The time interval to request gts for high availability gts source (abbr: ha_gts_source)with default 100us. Range: [10us, 1s]\\n  range: [10us, 1s]\\n  default: 100us\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6192",
		"help_category_id": str(sys_config_category_id),
		"name": "clog_max_unconfirmed_log_count",
		"description": "\\n  type: tenant parameter\\n  info maximum of unconfirmed logs in clog module. Range: [100, 50000]\\n  range: [100, 50000]\\n  default: 1500\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6193",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_clog_timeout_to_force_switch_leader",
		"description": "\\n  type: cluster parameter\\n  info When log sync is blocking, leader need wait this interval before revoke.The default value is 0s, use 0s to close this function. Range: [0s, 60m]\\n  range: [0s, 60m]\\n  default: 10s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6194",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_clog_disk_buffer_cnt",
		"description": "\\n  type: cluster parameter\\n  info clog disk buffer cnt. Range: [1, 2000]\\n  range: [1, 2000]\\n  default: 64\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6195",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_trans_rpc_timeout",
		"description": "\\n  type: cluster parameter\\n  info transaction rpc timeout(s). Range: [0s, 3600s]\\n  range: [0s, 3600s]\\n  default: 3s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6196",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_early_lock_release",
		"description": "\\n  type: tenant parameter\\n  info enable early lock release\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6197",
		"help_category_id": str(sys_config_category_id),
		"name": "_trx_commit_retry_interval",
		"description": "\\n  type: cluster parameter\\n  info transaction commit retry interval. Range: [100ms,)\\n  range: [100ms,)\\n  default: 200ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6198",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_get_gts_ahead_interval",
		"description": "\\n  type: cluster parameter\\n  info get gts ahead interval. Range: [0s, 1s]\\n  range: [0s, 1s]\\n  default: 0s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6199",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_enable_log_replica_strict_recycle_mode",
		"description": "\\n  type: cluster parameter\\n  info enable log replica strict recycle mode\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6200",
		"help_category_id": str(sys_config_category_id),
		"name": "rpc_timeout",
		"description": "\\n  type: cluster parameter\\n  info the time during which a RPC request is permitted to execute before it is terminated\\n  range: \\n  default: 2s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6201",
		"help_category_id": str(sys_config_category_id),
		"name": "location_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info priority of location cache among all system caching service. Range: [1, +INF) in integer\\n  range: [1,)\\n  default: 1000\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6202",
		"help_category_id": str(sys_config_category_id),
		"name": "location_cache_expire_time",
		"description": "\\n  type: cluster parameter\\n  info the expiration time for a partition location info in partition location cache. Not recommended for modification. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 600s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6203",
		"help_category_id": str(sys_config_category_id),
		"name": "virtual_table_location_cache_expire_time",
		"description": "\\n  type: cluster parameter\\n  info expiration time for virtual table location info in partiton location cache. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 8s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6204",
		"help_category_id": str(sys_config_category_id),
		"name": "location_refresh_thread_count",
		"description": "\\n  type: cluster parameter\\n  info the number of threads that fetch the partition location information from the root service. Range: (1, 64]\\n  range: (1,64]\\n  default: 4\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6205",
		"help_category_id": str(sys_config_category_id),
		"name": "force_refresh_location_cache_threshold",
		"description": "\\n  type: cluster parameter\\n  info location cache refresh threshold which use sql method in one second. Range: [1, +INF) in integer\\n  range: [1,)\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6206",
		"help_category_id": str(sys_config_category_id),
		"name": "force_refresh_location_cache_interval",
		"description": "\\n  type: cluster parameter\\n  info the max interval for refresh location cache by sql. Range: [1s, +INF)\\n  range: [1s,)\\n  default: 2h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6207",
		"help_category_id": str(sys_config_category_id),
		"name": "location_fetch_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the maximum number of the tasks which fetch the partition location information concurrently. Range: [1, 1000]\\n  range: [1, 1000]\\n  default: 20\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6208",
		"help_category_id": str(sys_config_category_id),
		"name": "location_cache_refresh_min_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval in which no request for location cache renewal will be executed. The default value is 100 milliseconds. [0s, +INF)\\n  range: [0s,)\\n  default: 100ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6209",
		"help_category_id": str(sys_config_category_id),
		"name": "location_cache_refresh_rpc_timeout",
		"description": "\\n  type: cluster parameter\\n  info The timeout used for refreshing location cache by RPC. Range: [1ms, +INF)\\n  range: [1ms,)\\n  default: 500ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6210",
		"help_category_id": str(sys_config_category_id),
		"name": "location_cache_refresh_sql_timeout",
		"description": "\\n  type: cluster parameter\\n  info The timeout used for refreshing location cache by SQL. Range: [1ms, +INF)\\n  range: [1ms,)\\n  default: 1s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6211",
		"help_category_id": str(sys_config_category_id),
		"name": "all_server_list",
		"description": "\\n  type: cluster parameter\\n  info all server addr in cluster\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6212",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_auto_refresh_location_cache",
		"description": "\\n  type: cluster parameter\\n  info enable auto refresh location\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6213",
		"help_category_id": str(sys_config_category_id),
		"name": "auto_refresh_location_cache_rate_limit",
		"description": "\\n  type: cluster parameter\\n  info Maximum number of partitions to refresh location automatically per second\\n  range: [1, 100000]\\n  default: 1000\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6214",
		"help_category_id": str(sys_config_category_id),
		"name": "auto_broadcast_location_cache_rate_limit",
		"description": "\\n  type: cluster parameter\\n  info Maximum number of partitions to broadcast location per second\\n  range: [1, 100000]\\n  default: 1000\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6215",
		"help_category_id": str(sys_config_category_id),
		"name": "clog_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info clog cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6216",
		"help_category_id": str(sys_config_category_id),
		"name": "index_clog_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info index clog cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6217",
		"help_category_id": str(sys_config_category_id),
		"name": "user_tab_col_stat_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info user tab col stat cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6218",
		"help_category_id": str(sys_config_category_id),
		"name": "index_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info index cache priority. Range: [1, )\\n  range: [1,)\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6219",
		"help_category_id": str(sys_config_category_id),
		"name": "index_info_block_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info index info block cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6220",
		"help_category_id": str(sys_config_category_id),
		"name": "user_block_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info user block cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6221",
		"help_category_id": str(sys_config_category_id),
		"name": "user_row_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info user row cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6222",
		"help_category_id": str(sys_config_category_id),
		"name": "bf_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info bf cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6223",
		"help_category_id": str(sys_config_category_id),
		"name": "bf_cache_miss_count_threshold",
		"description": "\\n  type: cluster parameter\\n  info bf cache miss count threshold, 0 means disable bf cache. Range: [0, )\\n  range: [0,)\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6224",
		"help_category_id": str(sys_config_category_id),
		"name": "fuse_row_cache_priority",
		"description": "\\n  type: cluster parameter\\n  info fuse row cache priority. Range: [1, )\\n  range: [1,)\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6225",
		"help_category_id": str(sys_config_category_id),
		"name": "sys_bkgd_io_low_percentage",
		"description": "\\n  type: cluster parameter\\n  info the low disk io percentage of sys io, sys io can use at least low percentage,when the value is 0, it will automatically set low limit for SATA and SSD disk to guarantee at least 128MB disk bandwidth. Range: [0,100]\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6226",
		"help_category_id": str(sys_config_category_id),
		"name": "sys_bkgd_io_high_percentage",
		"description": "\\n  type: cluster parameter\\n  info the high disk io percentage of sys io, sys io can use at most high percentage. Range: [1,100]\\n  range: [1,100]\\n  default: 90\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6227",
		"help_category_id": str(sys_config_category_id),
		"name": "sys_cpu_limit_trigger",
		"description": "\\n  type: cluster parameter\\n  info when the cpu usage percentage exceed the trigger, will limit the sys cpu usage Range: [50,)\\n  range: [50,)\\n  default: 80\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6228",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_sys_high_load_per_cpu_threshold",
		"description": "\\n  type: cluster parameter\\n  info when the load perf cpu exceed the trigger, will limit the background dag taskRange: [0,1000]\\n  range: [0,1000]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6229",
		"help_category_id": str(sys_config_category_id),
		"name": "user_iort_up_percentage",
		"description": "\\n  type: cluster parameter\\n  info variable to control sys io, the percentage of use io rt can raise Range: [0,)\\n  range: [0,)\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6230",
		"help_category_id": str(sys_config_category_id),
		"name": "_data_storage_io_timeout",
		"description": "\\n  type: cluster parameter\\n  info io timeout for data storage, Range [5s,600s]. The default value is 120s\\n  range: [5s,600s]\\n  default: 120s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6231",
		"help_category_id": str(sys_config_category_id),
		"name": "data_storage_warning_tolerance_time",
		"description": "\\n  type: cluster parameter\\n  info time to tolerate disk read failure, after that, the disk status will be set warning. Range [10s,300s]. The default value is 30s\\n  range: [10s,300s]\\n  default: 30s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6232",
		"help_category_id": str(sys_config_category_id),
		"name": "data_storage_error_tolerance_time",
		"description": "\\n  type: cluster parameter\\n  info time to tolerate disk read failure, after that, the disk status will be set error. Range [10s,7200s]. The default value is 300s\\n  range: [10s,7200s]\\n  default: 300s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6233",
		"help_category_id": str(sys_config_category_id),
		"name": "data_disk_usage_limit_percentage",
		"description": "\\n  type: cluster parameter\\n  info the safe use percentage of data diskRange: [50,100] in integer\\n  range: [50,100]\\n  default: 90\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6234",
		"help_category_id": str(sys_config_category_id),
		"name": "sys_bkgd_net_percentage",
		"description": "\\n  type: cluster parameter\\n  info the net percentage of sys background net. Range: [0, 100] in integer\\n  range: [0,100]\\n  default: 60\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6235",
		"help_category_id": str(sys_config_category_id),
		"name": "disk_io_thread_count",
		"description": "\\n  type: cluster parameter\\n  info The number of io threads on each disk. The default value is 8. Range: [2,32] in even integer\\n  range: [2,32]\\n  default: 8\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6236",
		"help_category_id": str(sys_config_category_id),
		"name": "_io_callback_thread_count",
		"description": "\\n  type: cluster parameter\\n  info The number of io callback threads. The default value is 8. Range: [1,64] in integer\\n  range: [1,64]\\n  default: 8\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6237",
		"help_category_id": str(sys_config_category_id),
		"name": "_large_query_io_percentage",
		"description": "\\n  type: cluster parameter\\n  info the max percentage of io resource for big queries. Range: [0,100] in integer. Especially, 0 means unlimited. The default value is 0.\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6238",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_parallel_minor_merge",
		"description": "\\n  type: tenant parameter\\n  info specifies whether to enable parallel minor merge. Value: True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6239",
		"help_category_id": str(sys_config_category_id),
		"name": "merge_thread_count",
		"description": "\\n  type: cluster parameter\\n  info the current work thread num of daily merge. Range: [0,256] in integer\\n  range: [0,256]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6240",
		"help_category_id": str(sys_config_category_id),
		"name": "minor_merge_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the current work thread num of minor merge. Range: [0,64] in integer\\n  range: [0,64]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6241",
		"help_category_id": str(sys_config_category_id),
		"name": "_mini_merge_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the current work thread num of mini merge. Range: [0,64] in integer\\n  range: [0,64]\\n  default: 5\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6242",
		"help_category_id": str(sys_config_category_id),
		"name": "restore_concurrency",
		"description": "\\n  type: cluster parameter\\n  info the current work thread num of restore macro block. Range: [0,512] in integer\\n  range: [0,512]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6243",
		"help_category_id": str(sys_config_category_id),
		"name": "merge_stat_sampling_ratio",
		"description": "\\n  type: cluster parameter\\n  info column stats sampling ratio daily merge. Range: [0,100] in integer\\n  range: [0,100]\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6244",
		"help_category_id": str(sys_config_category_id),
		"name": "minor_freeze_times",
		"description": "\\n  type: cluster parameter\\n  info specifies how many minor freezes should be triggered between two major freezes. Range: [0, 65535]\\n  range: [0, 65535]\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6245",
		"help_category_id": str(sys_config_category_id),
		"name": "minor_compact_trigger",
		"description": "\\n  type: cluster parameter\\n  info minor_compact_trigger, Range: [0,16] in integer\\n  range: [0,16]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6246",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_compaction_diagnose",
		"description": "\\n  type: cluster parameter\\n  info enable compaction diagnose functionValue:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6247",
		"help_category_id": str(sys_config_category_id),
		"name": "_private_buffer_size",
		"description": "\\n  type: cluster parameter\\n  info the trigger remaining data size within transaction for immediate logging, 0B represents not trigger immediate loggingRange: [0B, total size of memory]\\n  range: [0B,)\\n  default: 2M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6248",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_fast_commit",
		"description": "\\n  type: cluster parameter\\n  info whether to enable fast commit strategyValue:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6249",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_sparse_row",
		"description": "\\n  type: cluster parameter\\n  info whether enable using sparse row in SSTableValue:  True:turned on  False: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6250",
		"help_category_id": str(sys_config_category_id),
		"name": "_minor_compaction_amplification_factor",
		"description": "\\n  type: cluster parameter\\n  info the L1 compaction write amplification factor, 0 means default 25, Range: [0,100] in integer\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6251",
		"help_category_id": str(sys_config_category_id),
		"name": "_minor_compaction_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval to start next minor compaction, Range: [0s,30m]Range: [0s, 30m)\\n  range: [0s,30m]\\n  default: 0s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6252",
		"help_category_id": str(sys_config_category_id),
		"name": "_follower_replica_merge_level",
		"description": "\\n  type: cluster parameter\\n  info the merge level for d replica migrate, 0: nomore any compaction 1: self minor 2: self major 3: major+minorRange: [0,3] in integer\\n  range: [0,3]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6253",
		"help_category_id": str(sys_config_category_id),
		"name": "major_compact_trigger",
		"description": "\\n  type: cluster parameter\\n  info major_compact_trigger alias to minor_freeze_times, Range: [0,65535] in integer\\n  range: [0,65535]\\n  default: 5\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6254",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_enable_fast_freeze",
		"description": "\\n  type: tenant parameter\\n  info specifies whether fast freeze for elr table is enabledValue: True:turned on  False: turned off\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6255",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_elr_fast_freeze_threshold",
		"description": "\\n  type: cluster parameter\\n  info per row update counts threshold to trigger minor freeze for tables with ELR optimization\\n  range: [10000,)\\n  default: 500000\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6256",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_minor_merge_schedule_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval to schedule minor mrerge, Range: [3s,3m]Range: [3s, 3m]\\n  range: [3s,3m]\\n  default: 20s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6257",
		"help_category_id": str(sys_config_category_id),
		"name": "sys_bkgd_migration_retry_num",
		"description": "\\n  type: cluster parameter\\n  info retry num limit during migration. Range: [3, 100] in integer\\n  range: [3,100]\\n  default: 3\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6258",
		"help_category_id": str(sys_config_category_id),
		"name": "sys_bkgd_migration_change_member_list_timeout",
		"description": "\\n  type: cluster parameter\\n  info the timeout for migration change member list retry. The default value is 1h. Range: [0s,24h]\\n  range: [0s,24h]\\n  default: 1h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6259",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_election_group",
		"description": "\\n  type: cluster parameter\\n  info specifies whether election group is turned on. Value:  True:turned on  False: turned off\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6260",
		"help_category_id": str(sys_config_category_id),
		"name": "tablet_size",
		"description": "\\n  type: cluster parameter\\n  info default tablet size, has to be a multiple of 2M\\n  range: \\n  default: 128M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6261",
		"help_category_id": str(sys_config_category_id),
		"name": "meta_table_read_write_mode",
		"description": "\\n  type: cluster parameter\\n  info meta table read write mode. Range: [0,2] in integer. 0 : read write __all_meta_table 1 : read write __all_meta_table while write __all_tenant_meta_table 2 : read write __all_tenant_meta_table The default value is 0\\n  range: [0,2]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6262",
		"help_category_id": str(sys_config_category_id),
		"name": "builtin_db_data_verify_cycle",
		"description": "\\n  type: cluster parameter\\n  info check cycle of db data. Range: [0, 360] in integer. Unit: day. 0: check nothing. 1-360: check all data every specified days. The default value is 20. The real check cycle maybe longer than the specified value for insuring performance.\\n  range: [0, 360]\\n  default: 20\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6263",
		"help_category_id": str(sys_config_category_id),
		"name": "micro_block_merge_verify_level",
		"description": "\\n  type: cluster parameter\\n  info specify what kind of verification should be done when merging micro block. 0 : no verification will be done 1 : verify encoding algorithm, encoded micro block will be read to ensure data is correct 2 : verify encoding and compression algorithm, besides encoding verification, compressed block will be decompressed to ensure data is correct3 : verify encoding, compression algorithm and lost write protect\\n  range: [0,3]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6264",
		"help_category_id": str(sys_config_category_id),
		"name": "_migrate_block_verify_level",
		"description": "\\n  type: cluster parameter\\n  info specify what kind of verification should be done when migrating macro block. 0 : no verification will be done 1 : physical verification2 : logical verification\\n  range: [0,2]\\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6265",
		"help_category_id": str(sys_config_category_id),
		"name": "_cache_wash_interval",
		"description": "\\n  type: cluster parameter\\n  info specify interval of cache background wash\\n  range: [1ms, 1m]\\n  default: 200ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6266",
		"help_category_id": str(sys_config_category_id),
		"name": "_max_partition_cnt_per_server",
		"description": "\\n  type: cluster parameter\\n  info specify max partition count on one observer\\n  range: [10000, 500000]\\n  default: 500000\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6267",
		"help_category_id": str(sys_config_category_id),
		"name": "__schema_split_mode",
		"description": "\\n  type: cluster parameter\\n  info determinate if observer start with schema split mode.\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6268",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_clog_cache_max_hit_rate",
		"description": "\\n  type: cluster parameter\\n  info module test: clog cache max hit rate, 0 means disable clog cache. Range: [0,100]\\n  range: [0,100]\\n  default: 100\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6269",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_clog_packet_loss_rate",
		"description": "\\n  type: cluster parameter\\n  info module test: clog packet loss rate, 0 means no packet loss deliberately. Range: [0,100]\\n  range: [0, 100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6270",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_clog_ignore_log_id_in_election_priority",
		"description": "\\n  type: cluster parameter\\n  info module test: on_get_election_priority whether refer to log id\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6271",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_clog_delay_submit",
		"description": "\\n  type: cluster parameter\\n  info module test: delay submiting operation by 50 ms, one percent. simulate logs out of order\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6272",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_clog_mem_alloc_thrashing",
		"description": "\\n  type: cluster parameter\\n  info module test: clog allocator will fail one per mille deliberately if true\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6273",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_clog_flush_delay",
		"description": "\\n  type: cluster parameter\\n  info module test: flush will delay random ([0~10ms]) if true\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6274",
		"help_category_id": str(sys_config_category_id),
		"name": "multiblock_read_size",
		"description": "\\n  type: cluster parameter\\n  info multiple block batch read size in one read io request. Range: [0K,2M]\\n  range: [0K,2M]\\n  default: 128K\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6275",
		"help_category_id": str(sys_config_category_id),
		"name": "multiblock_read_gap_size",
		"description": "\\n  type: cluster parameter\\n  info max gap size in one read io request, gap means blocks that hit in block cache. Range: [0K,2M]\\n  range: [0K,2M]\\n  default: 0K\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6276",
		"help_category_id": str(sys_config_category_id),
		"name": "dtl_buffer_size",
		"description": "\\n  type: cluster parameter\\n  info to be removed\\n  range: [4K,2M]\\n  default: 64K\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6277",
		"help_category_id": str(sys_config_category_id),
		"name": "px_task_size",
		"description": "\\n  type: cluster parameter\\n  info to be removed\\n  range: [2M,)\\n  default: 2M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6278",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_smooth_leader_switch",
		"description": "\\n  type: cluster parameter\\n  info to be removed\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6279",
		"help_category_id": str(sys_config_category_id),
		"name": "migrate_concurrency",
		"description": "\\n  type: cluster parameter\\n  info set concurrency of migration, set upper limit to migrate_concurrency and set lower limit to migrate_concurrency/2, range: [0,64]\\n  range: [0,64]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6280",
		"help_category_id": str(sys_config_category_id),
		"name": "_max_elr_dependent_trx_count",
		"description": "\\n  type: cluster parameter\\n  info max elr dependent transaction count\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6281",
		"help_category_id": str(sys_config_category_id),
		"name": "skip_report_pg_backup_task_table_id",
		"description": "\\n  type: cluster parameter\\n  info skip_report_pg_backup_task table idRange: [0,) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6282",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_archive_log_checkpoint_ts",
		"description": "\\n  type: cluster parameter\\n  info fake_archive_log_checkpoint_tsRange: [0,) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6283",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_archive_log_status",
		"description": "\\n  type: cluster parameter\\n  info fake_archive_log_statusRange: [0,) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6284",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_once_init_restore_macro_index_store",
		"description": "\\n  type: cluster parameter\\n  info 0 for not fake, 1 for major, 2 for minorRange: [0,) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6285",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_disk_error_test",
		"description": "\\n  type: cluster parameter\\n  info when disk error test mode is enabled, observer send disk error status to rs in lease request\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6286",
		"help_category_id": str(sys_config_category_id),
		"name": "skip_update_estimator",
		"description": "\\n  type: cluster parameter\\n  info skip update_estimator during daily merge. \\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6287",
		"help_category_id": str(sys_config_category_id),
		"name": "with_new_migrated_replica",
		"description": "\\n  type: cluster parameter\\n  info it is new migrated replica, can not be removed by old migration. \\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6288",
		"help_category_id": str(sys_config_category_id),
		"name": "sys_bkgd_migration_large_task_threshold",
		"description": "\\n  type: cluster parameter\\n  info the timeout for migration change member list retry. The default value is 2h. Range: [0s,24h]\\n  range: [0s,24h]\\n  default: 2h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6289",
		"help_category_id": str(sys_config_category_id),
		"name": "fail_write_checkpoint_alert_interval",
		"description": "\\n  type: cluster parameter\\n  info fail_write_checkpoint_alert_interval. The default value is 6h. Range: [0s,24h]\\n  range: [0s,24h]\\n  default: 6h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6290",
		"help_category_id": str(sys_config_category_id),
		"name": "retry_write_checkpoint_min_interval",
		"description": "\\n  type: cluster parameter\\n  info retry_write_checkpoint_min_interval. The default value is 1h. Range: [0s,24h]\\n  range: [0s,24h]\\n  default: 1h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6291",
		"help_category_id": str(sys_config_category_id),
		"name": "macro_block_hold_alert_time",
		"description": "\\n  type: cluster parameter\\n  info macro_block_hold_alert_time. The default value is 12h. Range: [0s,24h]\\n  range: [0s,24h]\\n  default: 12h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6292",
		"help_category_id": str(sys_config_category_id),
		"name": "macro_block_builder_errsim_flag",
		"description": "\\n  type: cluster parameter\\n  info macro_block_builder_errsim_flagRange: [0,100] in integer\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6293",
		"help_category_id": str(sys_config_category_id),
		"name": "mark_and_sweep_interval",
		"description": "\\n  type: cluster parameter\\n  info mark_and_sweep_interval. The default value is 60s. Range: [0s,24h]\\n  range: [0s,24h]\\n  default: 60s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6294",
		"help_category_id": str(sys_config_category_id),
		"name": "skip_balance_disk_limit",
		"description": "\\n  type: cluster parameter\\n  info skip_dbalance_isk_limit. \\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6295",
		"help_category_id": str(sys_config_category_id),
		"name": "pwrite_errsim",
		"description": "\\n  type: cluster parameter\\n  info pwrite_errsim\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6296",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_disk_error",
		"description": "\\n  type: cluster parameter\\n  info fake_disk_error\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6297",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_remove_member_error",
		"description": "\\n  type: cluster parameter\\n  info fake_remove_member_errorRange: [0,100] in integer\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6298",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_add_member_error",
		"description": "\\n  type: cluster parameter\\n  info fake_remove_member_errorRange: [0,100] in integer\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6299",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_wait_batch_member_chagne_done",
		"description": "\\n  type: cluster parameter\\n  info fake_wait_batch_member_chagne_doneRange: [0,100] in integer\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6300",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_backup_archive_log_batch_count",
		"description": "\\n  type: cluster parameter\\n  info backup_backup_archive_log_batch_countRange: [1,1024] in integer\\n  range: [1,1024]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6301",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_backup_archivelog_retry_interval",
		"description": "\\n  type: cluster parameter\\n  info control backup backup archivelog retry interval. Range: [0s, 1h]\\n  range: [0s, 1h]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6302",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_backupset_batch_count",
		"description": "\\n  type: cluster parameter\\n  info backup_backupset_batch_countRange: [1,1024] in integer\\n  range: [1,1024]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6303",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_backupset_retry_interval",
		"description": "\\n  type: cluster parameter\\n  info control backup backupset retry interval. Range: [0s, 1h]\\n  range: [0s, 1h]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6304",
		"help_category_id": str(sys_config_category_id),
		"name": "max_wait_batch_member_change_done_us",
		"description": "\\n  type: cluster parameter\\n  info max_wait_batch_member_change_done_us. The default value is 300s. Range: [0s,24h]\\n  range: [0s,24h]\\n  default: 300s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6305",
		"help_category_id": str(sys_config_category_id),
		"name": "allow_major_sstable_merge",
		"description": "\\n  type: cluster parameter\\n  info allow_major_sstable_merge. \\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6306",
		"help_category_id": str(sys_config_category_id),
		"name": "trigger_reuse_table_in_table_mgr",
		"description": "\\n  type: cluster parameter\\n  info trigger_reuse_table_in_table_mgr\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6307",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_backup_date_for_incremental_backup",
		"description": "\\n  type: cluster parameter\\n  info incremental backup use fake backup date\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6308",
		"help_category_id": str(sys_config_category_id),
		"name": "slog_size",
		"description": "\\n  type: cluster parameter\\n  info size of the slog file. Range: [0, 256M]\\n  range: [0M,256M]\\n  default: 256M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6309",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_replay_start_point",
		"description": "\\n  type: cluster parameter\\n  info fake_replay_start_point\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6310",
		"help_category_id": str(sys_config_category_id),
		"name": "allow_batch_remove_member_during_change_replica",
		"description": "\\n  type: cluster parameter\\n  info allow_batch_remove_member_during_change_replica\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6311",
		"help_category_id": str(sys_config_category_id),
		"name": "allow_batch_change_member",
		"description": "\\n  type: cluster parameter\\n  info allow_batch_change_member\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6312",
		"help_category_id": str(sys_config_category_id),
		"name": "schema_drop_gc_delay_time",
		"description": "\\n  type: cluster parameter\\n  info max delay time for schema_drop gc.The default value is 1800s. Range: [0s, 24h]\\n  range: [0s, 24h]\\n  default: 1800s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6313",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_enable_rebuild_on_purpose",
		"description": "\\n  type: cluster parameter\\n  info whether open rebuild test mode in observer\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6314",
		"help_category_id": str(sys_config_category_id),
		"name": "_max_migration_status_count",
		"description": "\\n  type: cluster parameter\\n  info the max count of __all_virtual_partition_migration_statusRange: [0,max) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6315",
		"help_category_id": str(sys_config_category_id),
		"name": "_max_block_per_backup_task",
		"description": "\\n  type: cluster parameter\\n  info the max macro block count per backup sub taskRange: [0,max) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6316",
		"help_category_id": str(sys_config_category_id),
		"name": "_max_backup_piece_id",
		"description": "\\n  type: cluster parameter\\n  info the max backup log archive piece id that the RS can switch to, Range: [0,max) in integer, 0 means that RS can switch to any piece\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6317",
		"help_category_id": str(sys_config_category_id),
		"name": "ilog_flush_trigger_time",
		"description": "\\n  type: cluster parameter\\n  info max trigger time for ilog flush.The default value is 1800s. Range: [0s, 24h]\\n  range: [0s, 24h]\\n  default: 1800s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6318",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_ilog_recycle",
		"description": "\\n  type: cluster parameter\\n  info switch for ilog recycling\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6319",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_block_log_archive",
		"description": "\\n  type: cluster parameter\\n  info switch for blcoking log_archive\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6320",
		"help_category_id": str(sys_config_category_id),
		"name": "skip_buffer_minor_merge",
		"description": "\\n  type: cluster parameter\\n  info switch for buffer minor merge\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6321",
		"help_category_id": str(sys_config_category_id),
		"name": "skip_update_storage_info",
		"description": "\\n  type: cluster parameter\\n  info switch for update storage info\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6322",
		"help_category_id": str(sys_config_category_id),
		"name": "mock_oss_delete",
		"description": "\\n  type: cluster parameter\\n  info mock device touch and delete\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6323",
		"help_category_id": str(sys_config_category_id),
		"name": "mock_nfs_device_touch",
		"description": "\\n  type: cluster parameter\\n  info mock device touch and delete\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6324",
		"help_category_id": str(sys_config_category_id),
		"name": "incremental_backup_limit",
		"description": "\\n  type: cluster parameter\\n  info the max count of incremental backupRange: [0,max) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6325",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_lease_takeover_time",
		"description": "\\n  type: cluster parameter\\n  info Lease Takeover Time for Rootserver Backup heartbeat. Range: [1s, 5m]\\n  range: [1s, 5m]\\n  default: 10s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6326",
		"help_category_id": str(sys_config_category_id),
		"name": "fake_report_server_tenant_start_ts",
		"description": "\\n  type: cluster parameter\\n  info for test start ts rollbackRange: [0,max) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6327",
		"help_category_id": str(sys_config_category_id),
		"name": "_backup_pg_retry_max_count",
		"description": "\\n  type: cluster parameter\\n  info the max of one pg backup retry count, Range: [0,max) in integer, 0 means that pg backup no need retry\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6328",
		"help_category_id": str(sys_config_category_id),
		"name": "_backup_pg_max_batch_count",
		"description": "\\n  type: cluster parameter\\n  info the max of pg backup batch count, Range: [0,max) in integer\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6329",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_trx_memory_errsim_percentage",
		"description": "\\n  type: cluster parameter\\n  info the percentage of memory errsim. Rang:[0, 100]\\n  range: [0, 100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6330",
		"help_category_id": str(sys_config_category_id),
		"name": "module_test_trx_fake_commit",
		"description": "\\n  type: tenant parameter\\n  info module test: transaction fake commit\\n  default: false\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6331",
		"help_category_id": str(sys_config_category_id),
		"name": "sql_work_area",
		"description": "\\n  type: tenant parameter\\n  info Work area memory limitation for tenant\\n  range: [10M,)\\n  default: 1G\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6332",
		"help_category_id": str(sys_config_category_id),
		"name": "_max_trx_size",
		"description": "\\n  type: cluster parameter\\n  info the limit for memstore memory used per partition involved in each database transaction\\n  range: [0B,)\\n  default: 100G\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6333",
		"help_category_id": str(sys_config_category_id),
		"name": "__easy_memory_limit",
		"description": "\\n  type: cluster parameter\\n  info max memory size which can be used by libeasy. The default value is 4G. Range: [1G,)\\n  range: [1G,)\\n  default: 4G\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6334",
		"help_category_id": str(sys_config_category_id),
		"name": "stack_size",
		"description": "\\n  type: cluster parameter\\n  info the size of routine execution stackRange: [512K, 20M]\\n  range: [512K, 20M]\\n  default: 1M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6335",
		"help_category_id": str(sys_config_category_id),
		"name": "__easy_memory_reserved_percentage",
		"description": "\\n  type: cluster parameter\\n  info the percentage of easy memory reserved size. The default value is 0. Range: [0,100]\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6336",
		"help_category_id": str(sys_config_category_id),
		"name": "_px_max_pipeline_depth",
		"description": "\\n  type: cluster parameter\\n  info max parallel execution pipeline depth, range: [2,3]\\n  range: [2,3]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6337",
		"help_category_id": str(sys_config_category_id),
		"name": "ssl_client_authentication",
		"description": "\\n  type: cluster parameter\\n  info enable server SSL support. Takes effect after ca/cert/key file is configured correctly. \\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6338",
		"help_category_id": str(sys_config_category_id),
		"name": "_alter_column_mode",
		"description": "\\n  type: cluster parameter\\n  info use to check if alter column is allowed. 0: always forbid 1: always allow 2: forbid when major freeze is not finished Range: [0,2] in integer\\n  range: [0,2]\\n  default: 2\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6339",
		"help_category_id": str(sys_config_category_id),
		"name": "use_ipv6",
		"description": "\\n  type: cluster parameter\\n  info Whether this server uses ipv6 address\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6340",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_ddl_timeout",
		"description": "\\n  type: cluster parameter\\n  info the config parameter of ddl timeoutRange: [1s, +INF)\\n  range: [1s,)\\n  default: 1000s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6341",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_dest",
		"description": "\\n  type: cluster parameter\\n  info backup dest\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6342",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_backup_dest",
		"description": "\\n  type: cluster parameter\\n  info backup backup dest\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6343",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_dest_option",
		"description": "\\n  type: cluster parameter\\n  info backup_dest_option\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6344",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_backup_dest_option",
		"description": "\\n  type: cluster parameter\\n  info backup_backup_dest_option\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6345",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_log_archive_option",
		"description": "\\n  type: cluster parameter\\n  info backup log archive option, support MANDATORY/OPTIONAL, COMPRESSION\\n  default: OPTIONAL\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6346",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_concurrency",
		"description": "\\n  type: cluster parameter\\n  info backup concurrency limit. Range: [0, 100] in integer\\n  range: [0,100]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6347",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_net_limit",
		"description": "\\n  type: cluster parameter\\n  info backup net limit for whole clusterRange: [0M, max)\\n  range: [0M,)\\n  default: 0M\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6348",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_log_archive",
		"description": "\\n  type: cluster parameter\\n  info control if enable log archive\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6349",
		"help_category_id": str(sys_config_category_id),
		"name": "log_restore_concurrency",
		"description": "\\n  type: cluster parameter\\n  info concurrency for log restoringRange: [1, ] in integer\\n  range: [1,]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6350",
		"help_category_id": str(sys_config_category_id),
		"name": "log_archive_concurrency",
		"description": "\\n  type: cluster parameter\\n  info concurrency  for log_archive_sender and log_archive_spiterRange: [0, ] in integer\\n  range: [0,]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6351",
		"help_category_id": str(sys_config_category_id),
		"name": "log_archive_checkpoint_interval",
		"description": "\\n  type: cluster parameter\\n  info control interval of generating log archive checkpoint for cold partitionRange: [5s, 1h]\\n  range: [5s, 1h]\\n  default: 120s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6352",
		"help_category_id": str(sys_config_category_id),
		"name": "log_archive_batch_buffer_limit",
		"description": "\\n  type: cluster parameter\\n  info batch buffer limit for log archive, capacity smaller than 1G only for mini mode, Range: [4M, max)\\n  range: [4M,)\\n  default: 1G\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6353",
		"help_category_id": str(sys_config_category_id),
		"name": "gc_wait_archive",
		"description": "\\n  type: cluster parameter\\n  info control whether partition GC need to wait for all partition log be archived\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6354",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_recovery_window",
		"description": "\\n  type: cluster parameter\\n  info backup expired day limit, 0 means not expiredRange: [0, max) in time interval, for example 7d\\n  range: [0,)\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6355",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_region",
		"description": "\\n  type: cluster parameter\\n  info user suggest backup region\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6356",
		"help_category_id": str(sys_config_category_id),
		"name": "backup_zone",
		"description": "\\n  type: cluster parameter\\n  info user suggest backup zone, format like z1,z2z3,z4z5\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6357",
		"help_category_id": str(sys_config_category_id),
		"name": "auto_delete_expired_backup",
		"description": "\\n  type: cluster parameter\\n  info control if auto delete expired backup\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6358",
		"help_category_id": str(sys_config_category_id),
		"name": "_auto_update_reserved_backup_timestamp",
		"description": "\\n  type: cluster parameter\\n  info control if auto update reserved backup timestamp.True means only updating needed backup data file timestamp which usually used in OSS without delete permission.\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6359",
		"help_category_id": str(sys_config_category_id),
		"name": "_backup_retry_timeout",
		"description": "\\n  type: cluster parameter\\n  info control backup retry timeout. Range: [10s, 1h]\\n  range: [10s, 1h]\\n  default: 10m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6360",
		"help_category_id": str(sys_config_category_id),
		"name": "ob_enable_batched_multi_statement",
		"description": "\\n  type: tenant parameter\\n  info enable use of batched multi statement\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6361",
		"help_category_id": str(sys_config_category_id),
		"name": "_single_zone_deployment_on",
		"description": "\\n  type: cluster parameter\\n  info specify cluster deployment mode, True: single zone deployment False: multiple zone deploment, Default: False\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6362",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_ha_gts_full_service",
		"description": "\\n  type: cluster parameter\\n  info enable ha gts full service, default false for general oceanbase version, true for tpcc version. \\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6363",
		"help_category_id": str(sys_config_category_id),
		"name": "_bloom_filter_ratio",
		"description": "\\n  type: cluster parameter\\n  info the px bloom filter false-positive rate.the default value is 1, range: [0,100]\\n  range: [0, 100]\\n  default: 35\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6364",
		"help_category_id": str(sys_config_category_id),
		"name": "_restore_idle_time",
		"description": "\\n  type: cluster parameter\\n  info the time interval between the schedules of physical restore task. Range: [10s, +INF)\\n  range: [10s,]\\n  default: 1m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6365",
		"help_category_id": str(sys_config_category_id),
		"name": "_backup_idle_time",
		"description": "\\n  type: cluster parameter\\n  info the time interval between the schedules of physical backup task. Range: [10s, +INF)\\n  range: [10s,]\\n  default: 5m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6366",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_enable_prepared_statement",
		"description": "\\n  type: cluster parameter\\n  info control if enable prepared statement\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6367",
		"help_category_id": str(sys_config_category_id),
		"name": "_upgrade_stage",
		"description": "\\n  type: cluster parameter\\n  info specifies the upgrade stage. NONE: in non upgrade stage, PREUPGRADE: in pre upgrade stage, DBUPGRADE: in db uprade stage, POSTUPGRADE: in post upgrade stage. \\n  default: NONE\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6368",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_plan_cache_gc_strategy",
		"description": "\\n  type: cluster parameter\\n  info OFF: disabled, REPORT: check leaked cache object infos only, AUTO: check and release leaked cache obj.\\n  default: REPORT\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6369",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_plan_cache_mem_diagnosis",
		"description": "\\n  type: cluster parameter\\n  info wether turn plan cache ref count diagnosis on\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6370",
		"help_category_id": str(sys_config_category_id),
		"name": "_clog_aggregation_buffer_amount",
		"description": "\\n  type: tenant parameter\\n  info the amount of clog aggregation buffer\\n  range: [0, 128]\\n  default: 0\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6371",
		"help_category_id": str(sys_config_category_id),
		"name": "schema_history_recycle_interval",
		"description": "\\n  type: cluster parameter\\n  info the time interval between the schedules of schema history recyle task. Range: [0s, +INF)\\n  range: [0s,]\\n  default: 10m\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6372",
		"help_category_id": str(sys_config_category_id),
		"name": "_flush_clog_aggregation_buffer_timeout",
		"description": "\\n  type: tenant parameter\\n  info the timeout for flushing clog aggregation buffer. Range: [0ms, 100ms]\\n  range: [0ms, 100ms]\\n  default: 0ms\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6373",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_oracle_priv_check",
		"description": "\\n  type: cluster parameter\\n  info whether turn on oracle privilege check \\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6374",
		"help_category_id": str(sys_config_category_id),
		"name": "plsql_ccflags",
		"description": "\\n  type: tenant parameter\\n  info provides a mechanism that allows PL/SQL programmers to controlconditional compilation of each PL/SQL library unit independently\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6375",
		"help_category_id": str(sys_config_category_id),
		"name": "plsql_code_type",
		"description": "\\n  type: tenant parameter\\n  info specifies the compilation mode for PL/SQL library units\\n  default: native\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6376",
		"help_category_id": str(sys_config_category_id),
		"name": "plsql_debug",
		"description": "\\n  type: tenant parameter\\n  info specifies whether or not PL/SQL library units will be compiled for debugging\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6377",
		"help_category_id": str(sys_config_category_id),
		"name": "plsql_optimize_level",
		"description": "\\n  type: tenant parameter\\n  info specifies the optimization level that will be used tocompile PL/SQL library units. The higher the setting of this parameter, the more effortthe compiler makes to optimize PL/SQL library units\\n  range: \\n  default: 1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6378",
		"help_category_id": str(sys_config_category_id),
		"name": "plsql_v2_compatibility",
		"description": "\\n  type: tenant parameter\\n  info allows some abnormal behavior that Version 8 disallows, not available\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6379",
		"help_category_id": str(sys_config_category_id),
		"name": "plsql_warnings",
		"description": "\\n  type: tenant parameter\\n  info enables or disables the reporting of warning messages by thePL/SQL compiler, and specifies which warning messages to show as errors\\n  default: DISABLE::ALL\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6380",
		"help_category_id": str(sys_config_category_id),
		"name": "_bloom_filter_enabled",
		"description": "\\n  type: tenant parameter\\n  info enable join bloom filter\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6381",
		"help_category_id": str(sys_config_category_id),
		"name": "use_large_pages",
		"description": "\\n  type: cluster parameter\\n  info used to manage the database\\'s use of large pages, values: false, true, only\\n  default: false\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6382",
		"help_category_id": str(sys_config_category_id),
		"name": "ob_ssl_invited_common_names",
		"description": "\\n  type: tenant parameter\\n  info when server use ssl, use it to control client identity with ssl subject common name. default NONE\\n  default: NONE\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6383",
		"help_category_id": str(sys_config_category_id),
		"name": "ssl_external_kms_info",
		"description": "\\n  type: cluster parameter\\n  info when using the external key management center for ssl, this parameter will store some key management information\\n  default: \\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6384",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_ssl_invited_nodes",
		"description": "\\n  type: cluster parameter\\n  info when rpc need use ssl, we will use it to store invited server ipv4 during grayscale change.when it is finish, it can use ALL instead of all server ipv4\\n  default: NONE\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6385",
		"help_category_id": str(sys_config_category_id),
		"name": "_max_schema_slot_num",
		"description": "\\n  type: cluster parameter\\n  info the max schema slot number for each tenant, Range: [2, 8192] in integer\\n  range: [2,8192]\\n  default: 128\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6386",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_fulltext_index",
		"description": "\\n  type: cluster parameter\\n  info enable full text index\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6387",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_query_rate_limit",
		"description": "\\n  type: tenant parameter\\n  info the maximum throughput allowed for a tenant per observer instance\\n  range: \\n  default: -1\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6388",
		"help_category_id": str(sys_config_category_id),
		"name": "_xa_gc_timeout",
		"description": "\\n  type: cluster parameter\\n  info specifies the threshold value for a xa record to be considered as obsolete\\n  range: [1s,)\\n  default: 24h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6389",
		"help_category_id": str(sys_config_category_id),
		"name": "_xa_gc_interval",
		"description": "\\n  type: cluster parameter\\n  info specifies the scan interval of the gc worker\\n  range: [1s,)\\n  default: 1h\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6390",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_easy_keepalive",
		"description": "\\n  type: cluster parameter\\n  info enable keepalive for each TCP connection.\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6391",
		"help_category_id": str(sys_config_category_id),
		"name": "open_cursors",
		"description": "\\n  type: tenant parameter\\n  info specifies the maximum number of open cursors a session can have at once.can use this parameter to prevent a session from opening an excessive number of cursors.Range: [0, 65535] in integer\\n  range: [0,65535]\\n  default: 50\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6392",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_tcp_keepalive",
		"description": "\\n  type: cluster parameter\\n  info enable TCP keepalive for the TCP connection of sql protocol. Take effect for new established connections.\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6393",
		"help_category_id": str(sys_config_category_id),
		"name": "tcp_keepidle",
		"description": "\\n  type: cluster parameter\\n  info The time (in seconds) the connection needs to remain idle before TCP starts sending keepalive probe. Take effect for new established connections. Range: [1s, +INF]\\n  range: [1s,]\\n  default: 7200s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6394",
		"help_category_id": str(sys_config_category_id),
		"name": "tcp_keepintvl",
		"description": "\\n  type: cluster parameter\\n  info The time (in seconds) between individual keepalive probes. Take effect for new established connections. Range: [1s, +INF]\\n  range: [1s,]\\n  default: 6s\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6395",
		"help_category_id": str(sys_config_category_id),
		"name": "tcp_keepcnt",
		"description": "\\n  type: cluster parameter\\n  info The maximum number of keepalive probes TCP should send before dropping the connection. Take effect for new established connections. Range: [1,+INF)\\n  range: [1,]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6396",
		"help_category_id": str(sys_config_category_id),
		"name": "ilog_index_expire_time",
		"description": "\\n  type: cluster parameter\\n  info specifies the expire time of ilog_index, can use this parameter to limit thememory usage of file_id_cache\\n  range: [0s, 60d]\\n  default: 7d\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6397",
		"help_category_id": str(sys_config_category_id),
		"name": "_auto_drop_tenant_if_restore_failed",
		"description": "\\n  type: cluster parameter\\n  info auto drop restoring tenant if physical restore fails\\n  default: True\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6398",
		"help_category_id": str(sys_config_category_id),
		"name": "ob_proxy_readonly_transaction_routing_policy",
		"description": "\\n  type: tenant parameter\\n  info Proxy route policy for readonly sql\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6399",
		"help_category_id": str(sys_config_category_id),
		"name": "_enable_block_file_punch_hole",
		"description": "\\n  type: cluster parameter\\n  info specifies whether to punch whole when free blocks in block_file\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6400",
		"help_category_id": str(sys_config_category_id),
		"name": "_ob_enable_px_for_inner_sql",
		"description": "\\n  type: cluster parameter\\n  info specifies whether inner sql uses px. The default value is TRUE. Value: TRUE: turned on FALSE: turned off\\n  default: true\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6401",
		"help_category_id": str(sys_config_category_id),
		"name": "kv_ttl_duty_duration",
		"description": "\\n  type: tenant parameter\\n  info ttl background task working time durationbegin_time or end_time in Range [0:00:00, 24:00:00]\\n  default: [0:00:00, 24:00:00]\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6402",
		"help_category_id": str(sys_config_category_id),
		"name": "kv_ttl_history_recycle_interval",
		"description": "\\n  type: tenant parameter\\n  info the time to recycle ttl history. Range: [1d, 180d]\\n  range: [1d, 180d]\\n  default: 7d\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6403",
		"help_category_id": str(sys_config_category_id),
		"name": "enable_kv_ttl",
		"description": "\\n  type: tenant parameter\\n  info specifies whether ttl task is enbled\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6404",
		"help_category_id": str(sys_config_category_id),
		"name": "query_response_time_stats",
		"description": "\\n  type: tenant parameter\\n  info Enable or disable QUERY_RESPONSE_TIME statistics collectingThe default value is False. Value: TRUE: turned on FALSE: turned off\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6405",
		"help_category_id": str(sys_config_category_id),
		"name": "query_response_time_flush",
		"description": "\\n  type: tenant parameter\\n  info Flush QUERY_RESPONSE_TIME table and re-read query_response_time_range_baseThe default value is False. Value: TRUE: trigger flush FALSE: do not trigger\\n  default: False\\n",
		"example": "",
		"url": URL
	},
	{
		"help_topic_id": "6406",
		"help_category_id": str(sys_config_category_id),
		"name": "query_response_time_range_base",
		"description": "\\n  type: tenant parameter\\n  info Select base of log for QUERY_RESPONSE_TIME ranges. WARNING: variable change takes affect only after flush.The default value is False. Value: TRUE: trigger flush FALSE: do not trigger\\n  range: [2,10000]\\n  default: 10\\n",
		"example": "",
		"url": URL
	},
]
	

keyword_list = [
	{
		"help_keyword_id": "1",
		"name": "TINYINT",
		"relation_topic": [1]
	},
	{
		"help_keyword_id": "2",
		"name": "BOOL",
		"relation_topic": [2]
	},
	{
		"help_keyword_id": "3",
		"name": "SMALLINT",
		"relation_topic": [3]
	},
	{
		"help_keyword_id": "4",
		"name": "MEDIUMINT",
		"relation_topic": [4]
	},
	{
		"help_keyword_id": "5",
		"name": "INT",
		"relation_topic": [5]
	},
	{
		"help_keyword_id": "6",
		"name": "INTEGER",
		"relation_topic": [6]
	},
	{
		"help_keyword_id": "7",
		"name": "BIGINT",
		"relation_topic": [7]
	},
	{
		"help_keyword_id": "8",
		"name": "FLOAT",
		"relation_topic": [8]
	},
	{
		"help_keyword_id": "9",
		"name": "DOUBLE",
		"relation_topic": [9]
	},
	{
		"help_keyword_id": "10",
		"name": "DOUBLE PRECISION",
		"relation_topic": [10]
	},
	{
		"help_keyword_id": "11",
		"name": "FLOAT(p)",
		"relation_topic": [11]
	},
	{
		"help_keyword_id": "12",
		"name": "DECIMAL",
		"relation_topic": [12]
	},
	{
		"help_keyword_id": "13",
		"name": "NUMERIC",
		"relation_topic": [13]
	},
	{
		"help_keyword_id": "14",
		"name": "CHAR",
		"relation_topic": [14]
	},
	{
		"help_keyword_id": "15",
		"name": "VARCHAR",
		"relation_topic": [15]
	},
	{
		"help_keyword_id": "16",
		"name": "BINARY",
		"relation_topic": [16]
	},
	{
		"help_keyword_id": "17",
		"name": "DATE",
		"relation_topic": [17]
	},
	{
		"help_keyword_id": "18",
		"name": "DATETIME",
		"relation_topic": [18]
	},
	{
		"help_keyword_id": "19",
		"name": "TIMESTAMP",
		"relation_topic": [19]
	},
	{
		"help_keyword_id": "20",
		"name": "TIME",
		"relation_topic": [20]
	},
	{
		"help_keyword_id": "21",
		"name": "YEAR",
		"relation_topic": [21]
	},
	{
		"help_keyword_id": "22",
		"name": "CURRENT_TIME",
		"relation_topic": [22]
	},
	{
		"help_keyword_id": "23",
		"name": "CURTIME",
		"relation_topic": [23]
	},
	{
		"help_keyword_id": "24",
		"name": "CURRENT_TIMESTAMP",
		"relation_topic": [24]
	},
	{
		"help_keyword_id": "25",
		"name": "CURRENT_DATE",
		"relation_topic": [25]
	},
	{
		"help_keyword_id": "26",
		"name": "CURDATE",
		"relation_topic": [26]
	},
	{
		"help_keyword_id": "27",
		"name": "DATE_ADD",
		"relation_topic": [27]
	},
	{
		"help_keyword_id": "28",
		"name": "DATE_FORMAT",
		"relation_topic": [28]
	},
	{
		"help_keyword_id": "29",
		"name": "DATE_SUB",
		"relation_topic": [29]
	},
	{
		"help_keyword_id": "30",
		"name": "EXTRACT",
		"relation_topic": [30]
	},
	{
		"help_keyword_id": "31",
		"name": "NOW",
		"relation_topic": [31]
	},
	{
		"help_keyword_id": "32",
		"name": "STR_TO_DATE",
		"relation_topic": [32]
	},
	{
		"help_keyword_id": "33",
		"name": "TIME_TO_USEC",
		"relation_topic": [33]
	},
	{
		"help_keyword_id": "34",
		"name": "USEC_TO_TIME",
		"relation_topic": [34]
	},
	{
		"help_keyword_id": "35",
		"name": "UNIX_TIMESTAMP ",
		"relation_topic": [35]
	},
	{
		"help_keyword_id": "36",
		"name": "DATEDIFF",
		"relation_topic": [36]
	},
	{
		"help_keyword_id": "37",
		"name": "TIMEDIFF",
		"relation_topic": [37]
	},
	{
		"help_keyword_id": "38",
		"name": "TIMESTAMPDIFF",
		"relation_topic": [38]
	},
	{
		"help_keyword_id": "39",
		"name": "PERIOD_DIFF",
		"relation_topic": [39]
	},
	{
		"help_keyword_id": "40",
		"name": "CONCAT",
		"relation_topic": [40]
	},
	{
		"help_keyword_id": "41",
		"name": "SUBSTRING",
		"relation_topic": [41]
	},
	{
		"help_keyword_id": "42",
		"name": "SUBSTR",
		"relation_topic": [42]
	},
	{
		"help_keyword_id": "43",
		"name": "TRIM",
		"relation_topic": [43]
	},
	{
		"help_keyword_id": "44",
		"name": "LENGTH",
		"relation_topic": [44]
	},
	{
		"help_keyword_id": "45",
		"name": "UPPER",
		"relation_topic": [45]
	},
	{
		"help_keyword_id": "46",
		"name": "LOWER",
		"relation_topic": [46]
	},
	{
		"help_keyword_id": "47",
		"name": "HEX",
		"relation_topic": [47]
	},
	{
		"help_keyword_id": "48",
		"name": "UNHEX",
		"relation_topic": [48]
	},
	{
		"help_keyword_id": "49",
		"name": "INT2IP",
		"relation_topic": [49]
	},
	{
		"help_keyword_id": "50",
		"name": "IP2INT",
		"relation_topic": [50]
	},
	{
		"help_keyword_id": "51",
		"name": "LIKE",
		"relation_topic": [51]
	},
	{
		"help_keyword_id": "52",
		"name": "REGEXP",
		"relation_topic": [52]
	},
	{
		"help_keyword_id": "53",
		"name": "REPEAT",
		"relation_topic": [53]
	},
	{
		"help_keyword_id": "54",
		"name": "SUBSTRING_INDEX",
		"relation_topic": [54]
	},
	{
		"help_keyword_id": "55",
		"name": "LOCATE",
		"relation_topic": [55]
	},
	{
		"help_keyword_id": "56",
		"name": "INSTR",
		"relation_topic": [56]
	},
	{
		"help_keyword_id": "57",
		"name": "REPLACE()",
		"relation_topic": [57]
	},
	{
		"help_keyword_id": "58",
		"name": "FIELD",
		"relation_topic": [58]
	},
	{
		"help_keyword_id": "59",
		"name": "ELT",
		"relation_topic": [59]
	},
	{
		"help_keyword_id": "60",
		"name": "CAST",
		"relation_topic": [60]
	},
	{
		"help_keyword_id": "61",
		"name": "AVG",
		"relation_topic": [61]
	},
	{
		"help_keyword_id": "62",
		"name": "COUNT",
		"relation_topic": [62]
	},
	{
		"help_keyword_id": "63",
		"name": "MAX",
		"relation_topic": [63]
	},
	{
		"help_keyword_id": "64",
		"name": "MIN",
		"relation_topic": [64]
	},
	{
		"help_keyword_id": "65",
		"name": "SUM",
		"relation_topic": [65]
	},
	{
		"help_keyword_id": "66",
		"name": "GROUP_CONCAT",
		"relation_topic": [66]
	},
	{
		"help_keyword_id": "67",
		"name": "ROUND",
		"relation_topic": [67]
	},
	{
		"help_keyword_id": "68",
		"name": "CEIL",
		"relation_topic": [68]
	},
	{
		"help_keyword_id": "69",
		"name": "FLOOR",
		"relation_topic": [69]
	},
	{
		"help_keyword_id": "70",
		"name": "ABS",
		"relation_topic": [70]
	},
	{
		"help_keyword_id": "71",
		"name": "NEG",
		"relation_topic": [71]
	},
	{
		"help_keyword_id": "72",
		"name": "SIGN",
		"relation_topic": [72]
	},
	{
		"help_keyword_id": "73",
		"name": "CONV",
		"relation_topic": [73]
	},
	{
		"help_keyword_id": "74",
		"name": "MOD",
		"relation_topic": [74]
	},
	{
		"help_keyword_id": "75",
		"name": "GREATEST",
		"relation_topic": [75]
	},
	{
		"help_keyword_id": "76",
		"name": "LEAST",
		"relation_topic": [76]
	},
	{
		"help_keyword_id": "77",
		"name": "ISNULL",
		"relation_topic": [77]
	},
	{
		"help_keyword_id": "78",
		"name": "CASE",
		"relation_topic": [78]
	},
	{
		"help_keyword_id": "79",
		"name": "IF",
		"relation_topic": [79]
	},
	{
		"help_keyword_id": "80",
		"name": "IFNULL",
		"relation_topic": [80]
	},
	{
		"help_keyword_id": "81",
		"name": "NULLIF",
		"relation_topic": [81]
	},
	{
		"help_keyword_id": "82",
		"name": "FOUND_ROWS",
		"relation_topic": [82]
	},
	{
		"help_keyword_id": "83",
		"name": "LAST_INSERT_ID",
		"relation_topic": [83]
	},
	{
		"help_keyword_id": "84",
		"name": "COALESCE",
		"relation_topic": [84]
	},
	{
		"help_keyword_id": "85",
		"name": "NVL",
		"relation_topic": [85]
	},
	{
		"help_keyword_id": "86",
		"name": "Logical Operators",
		"relation_topic": [86]
	},
	{
		"help_keyword_id": "87",
		"name": "NOT",
		"relation_topic": [87]
	},
	{
		"help_keyword_id": "88",
		"name": "AND",
		"relation_topic": [88]
	},
	{
		"help_keyword_id": "89",
		"name": "OR ",
		"relation_topic": [89]
	},
	{
		"help_keyword_id": "90",
		"name": "XOR",
		"relation_topic": [90]
	},
	{
		"help_keyword_id": "91",
		"name": "Arithmetic Operators",
		"relation_topic": [91]
	},
	{
		"help_keyword_id": "92",
		"name": "+",
		"relation_topic": [92]
	},
	{
		"help_keyword_id": "93",
		"name": "-",
		"relation_topic": [93]
	},
	{
		"help_keyword_id": "94",
		"name": "*",
		"relation_topic": [94]
	},
	{
		"help_keyword_id": "95",
		"name": "/",
		"relation_topic": [95]
	},
	{
		"help_keyword_id": "96",
		"name": "%",
		"relation_topic": [96]
	},
	{
		"help_keyword_id": "97",
		"name": "^",
		"relation_topic": [97]
	},
	{
		"help_keyword_id": "98",
		"name": "Comparison Operators",
		"relation_topic": [98]
	},
	{
		"help_keyword_id": "99",
		"name": "=",
		"relation_topic": [99]
	},
	{
		"help_keyword_id": "100",
		"name": ">=",
		"relation_topic": [100]
	},
	{
		"help_keyword_id": "101",
		"name": ">",
		"relation_topic": [101]
	},
	{
		"help_keyword_id": "102",
		"name": "<=",
		"relation_topic": [102]
	},
	{
		"help_keyword_id": "103",
		"name": "<",
		"relation_topic": [103]
	},
	{
		"help_keyword_id": "104",
		"name": "!=",
		"relation_topic": [104]
	},
	{
		"help_keyword_id": "105",
		"name": "BETWEEN … AND …",
		"relation_topic": [105]
	},
	{
		"help_keyword_id": "106",
		"name": "IN",
		"relation_topic": [106]
	},
	{
		"help_keyword_id": "107",
		"name": "IS [NOT] NULL | TRUE | FALSE | UNKNOWN",
		"relation_topic": [107]
	},
	{
		"help_keyword_id": "108",
		"name": "Vector Comparison Operators",
		"relation_topic": [108]
	},
	{
		"help_keyword_id": "109",
		"name": "Bit Operators",
		"relation_topic": [109]
	},
	{
		"help_keyword_id": "110",
		"name": "Operator Precedence",
		"relation_topic": [110]
	},
	{
		"help_keyword_id": "111",
		"name": "CREATE DATABASE",
		"relation_topic": [111]
	},
	{
		"help_keyword_id": "112",
		"name": "ALTER DATABASE",
		"relation_topic": [112]
	},
	{
		"help_keyword_id": "113",
		"name": "DROP DATABASE",
		"relation_topic": [113]
	},
	{
		"help_keyword_id": "114",
		"name": "CREATE TABLE",
		"relation_topic": [114]
	},
	{
		"help_keyword_id": "115",
		"name": "ALTER TABLE",
		"relation_topic": [115]
	},
	{
		"help_keyword_id": "116",
		"name": "DROP TABLE",
		"relation_topic": [116]
	},
	{
		"help_keyword_id": "117",
		"name": "CREATE INDEX",
		"relation_topic": [117]
	},
	{
		"help_keyword_id": "118",
		"name": "DROP INDEX",
		"relation_topic": [118]
	},
	{
		"help_keyword_id": "119",
		"name": "CREATE VIEW",
		"relation_topic": [119]
	},
	{
		"help_keyword_id": "120",
		"name": "DROP VIEW",
		"relation_topic": [120]
	},
	{
		"help_keyword_id": "121",
		"name": "ALTER VIEW",
		"relation_topic": [121]
	},
	{
		"help_keyword_id": "122",
		"name": "TRUNCATE TABLE",
		"relation_topic": [122]
	},
	{
		"help_keyword_id": "123",
		"name": "INSERT",
		"relation_topic": [123]
	},
	{
		"help_keyword_id": "124",
		"name": "REPLACE",
		"relation_topic": [124]
	},
	{
		"help_keyword_id": "125",
		"name": "UPDATE",
		"relation_topic": [125]
	},
	{
		"help_keyword_id": "126",
		"name": "DELETE",
		"relation_topic": [126]
	},
	{
		"help_keyword_id": "127",
		"name": "SELECT",
		"relation_topic": [127]
	},
	{
		"help_keyword_id": "128",
		"name": "TRANSACTION",
		"relation_topic": [128]
	},
	{
		"help_keyword_id": "129",
		"name": "CREATE RESOURCE UNIT",
		"relation_topic": [129]
	},
	{
		"help_keyword_id": "130",
		"name": "DROP RESOURCE UNIT",
		"relation_topic": [130]
	},
	{
		"help_keyword_id": "131",
		"name": "CREATE RESOURCE POOL",
		"relation_topic": [131]
	},
	{
		"help_keyword_id": "132",
		"name": "ALTER RESOURCE POOL",
		"relation_topic": [132]
	},
	{
		"help_keyword_id": "133",
		"name": "DROP RESOURCE POOL",
		"relation_topic": [133]
	},
	{
		"help_keyword_id": "134",
		"name": "CREATE TENANT",
		"relation_topic": [134]
	},
	{
		"help_keyword_id": "135",
		"name": "ALTER TENANT",
		"relation_topic": [135]
	},
	{
		"help_keyword_id": "136",
		"name": "LOCK",
		"relation_topic": [136]
	},
	{
		"help_keyword_id": "137",
		"name": "DROP TENANT",
		"relation_topic": [137]
	},
	{
		"help_keyword_id": "138",
		"name": "CREATE TABLEGROUP",
		"relation_topic": [138]
	},
	{
		"help_keyword_id": "139",
		"name": "DROP TABLEGROUP",
		"relation_topic": [139]
	},
	{
		"help_keyword_id": "140",
		"name": "ALTER TABLEGROUP",
		"relation_topic": [140]
	},
	{
		"help_keyword_id": "141",
		"name": "CREATE USER",
		"relation_topic": [141]
	},
	{
		"help_keyword_id": "142",
		"name": "DROP USER",
		"relation_topic": [142]
	},
	{
		"help_keyword_id": "143",
		"name": "SET PASSWORD",
		"relation_topic": [143]
	},
	{
		"help_keyword_id": "144",
		"name": "RENAME USER",
		"relation_topic": [144]
	},
	{
		"help_keyword_id": "145",
		"name": "ALTER USER",
		"relation_topic": [145]
	},
	{
		"help_keyword_id": "146",
		"name": "GRANT",
		"relation_topic": [146]
	},
	{
		"help_keyword_id": "147",
		"name": "REVOKE",
		"relation_topic": [147]
	},
	{
		"help_keyword_id": "148",
		"name": "SET",
		"relation_topic": [148]
	},
	{
		"help_keyword_id": "149",
		"name": "ALTER SYSTEM",
		"relation_topic": [149]
	},
	{
		"help_keyword_id": "150",
		"name": "PREPARE",
		"relation_topic": [150]
	},
	{
		"help_keyword_id": "151",
		"name": "EXECUTE",
		"relation_topic": [151]
	},
	{
		"help_keyword_id": "152",
		"name": "DEALLOCATE",
		"relation_topic": [152]
	},
	{
		"help_keyword_id": "153",
		"name": "SHOW",
		"relation_topic": [153]
	},
	{
		"help_keyword_id": "154",
		"name": "KILL",
		"relation_topic": [154]
	},
	{
		"help_keyword_id": "155",
		"name": "DESCRIBE",
		"relation_topic": [155]
	},
	{
		"help_keyword_id": "156",
		"name": "USE",
		"relation_topic": [156]
	},
	{
		"help_keyword_id": "157",
		"name": "SET TENANT",
		"relation_topic": [156]
	},
	{
		"help_keyword_id": "158",
		"name": "BOOLEAN",
		"relation_topic": [2]
	},
	{
		"help_keyword_id": "159",
		"name": "VARBINARY",
		"relation_topic": [16]
	},
	{
		"help_keyword_id": "161",
		"name": "<>",
		"relation_topic": [104]
	},
	{
		"help_keyword_id": "162",
		"name": "!",
		"relation_topic": [87]
	},
	{
		"help_keyword_id": "163",
		"name": "&&",
		"relation_topic": [88]
	},
	{
		"help_keyword_id": "164",
		"name": "||",
		"relation_topic": [89]
	},
	{
		"help_keyword_id": "165",
		"name": "UNLOCK",
		"relation_topic": [136]
	},
]