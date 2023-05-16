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


CREATE OR REPLACE PROCEDURE public."feel_on_5_sec"()
    LANGUAGE plpgsql
AS $procedure$
    BEGIN
    while true loop
	CALL "feel_param"();
	commit;
	perform pg_sleep(5);
    end loop;
    END;
$procedure$
;

CREATE TABLE public.temp_table (
    "minute" integer NULL
);

INSERT INTO public.temp_table  ("minute") VALUES (5);

CREATE OR REPLACE VIEW public.n_state
AS WITH arh AS (
         SELECT q.rnk,            q.id,
            q."time",
            q.value
           FROM ( SELECT rank() OVER (PARTITION BY a_1.id ORDER BY a_1."time" DESC) AS rnk,
                    a_1.id,
                    a_1."time",
                    a_1.value
                   FROM arhive a_1
                  WHERE EXTRACT(epoch FROM now() - a_1."time"::timestamp with time zone) < ((( SELECT minute FROM temp_table limit 1)) * 60)::numeric) q
          WHERE q.rnk = 1
        ), evt AS (
         SELECT q.rnk,
            q.id,
            q."time",
            q.value,
            q.state
           FROM ( SELECT rank() OVER (PARTITION BY e_1.id ORDER BY e_1."time" DESC) AS rnk,
                    e_1.id,
                    e_1."time",
                    e_1.value,
                    e_1.state
                   FROM event e_1) q
          WHERE q.rnk = 1
        )
 SELECT p.name,
    a."time",
    a.value,
    e.state
   FROM arh a
     JOIN param p ON p.id = a.id
     LEFT JOIN evt e ON e.id = a.id;


CREATE OR REPLACE VIEW public.one_min
AS SELECT arhive.id,
    avg(arhive.value) AS avg,
    date_trunc('minute'::text, arhive."time") AS time_m
   FROM arhive
  GROUP BY arhive.id, (date_trunc('minute'::text, arhive."time"))
  ORDER BY (date_trunc('minute'::text, arhive."time")) DESC;


CREATE OR REPLACE FUNCTION public.show_in_period(period_in_min integer)
 RETURNS TABLE(name character varying, last_time timestamp without time zone, last_value integer, last_state integer)
 LANGUAGE plpgsql
AS $function$
begin
  return query
    with arh as(
    select q.id, q."time", q.value from  
      (select rank() over (partition by id order by time desc) as rnk, a.id, a."time", a.value
        from arhive a where extract (epoch from now()-"time") < period_in_min * 60
      )q where q.rnk = 1
    )
  select param.name, arh."time", arh.value, 
    (select state from event where event.id = arh.id order by time desc limit 1) as state
  from arh
  join param on param.id=arh.id;
end
$function$
;
