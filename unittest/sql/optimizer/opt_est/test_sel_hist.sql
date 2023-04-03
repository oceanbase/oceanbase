# ***Case Data***
# min = 1, max = 20, ndv = 20, null_num = 50
# num_row = 250, density = 0.0025, bucket count = 200
# the ratio of not null row is (250-50)/250 = 0.8
# | val | 1 | 2  | 3  | 4  | 5  | 6  | 7  | 8  | 9   | 10  | 11  | 12  | 13  | 14  | 15  | 16  | 17  | 18  | 19  | 20  |
# | cnt | 5 | 14 | 13 | 16 | 9  | 7  | 10 | 13 | 15  | 1   | 5   | 6   | 10  | 9   | 9   | 12  | 21  | 11  | 11  | 3   |
# | acc | 5 | 19 | 32 | 48 | 57 | 64 | 74 | 87 | 102 | 103 | 108 | 114 | 124 | 133 | 142 | 154 | 175 | 186 | 197 | 200 |

density * num_null = 0.002
select c1 from t1 where c1 = 10;
density * num_null = 0.002
select c1 from t1 where c1 = 5.5;
density * num_null = 0.002
select c1 from t1 where c1 > 20;
density * num_null = 0.002
select c1 from t1 where c1 < 1;

5/200 * 0.8 = 0.02
select c1 from t1 where c1 = 1;
(5/200 + 0.0025) * 0.8 = 0.022
select c1 from t1 where c1 <= 1;
(103 - 5) / 200 * 0.8 = 0.392
select c1 from t1 where c1 >= 2 and c1 <= 10;
# TODO: 可能需要改造 query range
# ((103 - 5) / 200 + 0.0025) * 0.8 = 0.393
((103 - 5) / 200) * 0.8 = 0.392
select c1 from t1 where c1 > 1.5 and c1 <= 10;
((103 - 5) / 200 + 0.0025) * 0.8 = 0.393
select c1 from t1 where c1 >= 2 and c1 < 10.5;
