--CREATE EXTENSION IF NOT EXISTS pg_cron;

CREATE TABLE public.param (
    id integer NOT NULL GENERATED ALWAYS AS IDENTITY,
    "name" varchar NOT NULL
);

CREATE TABLE public.arhive (
    id integer NOT NULL,
    "time" timestamp PRIMARY KEY NOT NULL,
    value integer NULL
);

CREATE TABLE public."event" (
    id integer NOT NULL,
    "time" timestamp NOT NULL,
    value integer NULL,
    state integer NULL
);

INSERT INTO public.param  ("name") VALUES
     ('Параметр 01'), ('Параметр 02'), ('Параметр 03'), ('Параметр 04'),
     ('Параметр 05'), ('Параметр 06'), ('Параметр 07'), ('Параметр 08'),
     ('Параметр 09'), ('Параметр 10'), ('Параметр 11'), ('Параметр 12'),
     ('Параметр 13'), ('Параметр 14'), ('Параметр 15'), ('Параметр 16');

CREATE OR REPLACE PROCEDURE public.feel_param() LANGUAGE plpgsql AS $procedure$
declare r record;
    begin
	for i in 1..5 loop
	    for r in SELECT id FROM param WHERE id = (SELECT floor((SELECT max(id) FROM param) * random()))
	    loop
		insert into arhive ("id","time","value")
		values (r.id, clock_timestamp(), round(10*random()));
		perform pg_sleep(.001);
	    end loop;
	end loop;
    end;
$procedure$;

CREATE OR REPLACE FUNCTION public.check_state()
 RETURNS trigger
 LANGUAGE plpgsql
AS $function$
 declare r record; cur integer;
begin
    select id, "time", value  from arhive where id = new.id order by "time" desc limit 1 into r ; 
    cur = coalesce(r.value,0)::integer;
    if cur < 8 and new.value >= 8 then
	insert into "event" ("id","time","value","state") values (new.id,new."time",new.value,1);
    elseif cur >= 8 and new.value < 8 then
	insert into "event" ("id","time","value","state") values (new.id,new."time",new.value,0);
    end if;
    return new;
end;$function$
;


CREATE TRIGGER arhive_tr_ins
BEFORE INSERT ON arhive
FOR EACH ROW EXECUTE PROCEDURE check_state();