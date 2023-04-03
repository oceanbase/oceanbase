create procedure p(x bigint)
begin
declare i bigint default 1;
if(i=1)
then
select 42 from dual;
end if;
end

