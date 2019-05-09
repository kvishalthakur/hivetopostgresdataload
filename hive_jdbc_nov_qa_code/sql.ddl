DROP TABLE app.ami_alerts_audit;

CREATE TABLE app.ami_alerts_audit
(
    source_hostname character varying COLLATE pg_catalog."default",
    event_type character varying COLLATE pg_catalog."default",
    event_result character varying COLLATE pg_catalog."default",
    event_result_info1 character varying COLLATE pg_catalog."default",
    event_extra_info1 character varying COLLATE pg_catalog."default",
    event_extra_info2 character varying COLLATE pg_catalog."default",
	event_extra_info3 character varying COLLATE pg_catalog."default",
	businesspartnernumber character varying COLLATE pg_catalog."default",
	contractaccount character varying COLLATE pg_catalog."default",
	equipmentnumber character varying COLLATE pg_catalog."default",
	last_read_time timestamp without time zone,
	sum_consumption numeric,
	current_usage numeric,
	high_usage_limit numeric 
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE app.ami_alerts_audit ADD COLUMN created_at TIMESTAMP DEFAULT NOW()
ALTER TABLE app.ami_alerts_audit OWNER to postgres;



-- PROD 1 =>
-- PGPASSWORD="@mw@t3er@cld" psql -U postgres -d postgresql://hsyplclds001.amwater.net:5432/ao-aw-cloudseer3 -c "DROP TABLE app.ami_alerts_audit";
-- PGPASSWORD="@mw@t3er@cld" psql -U postgres -d postgresql://hsyplclds001.amwater.net:5432/ao-aw-cloudseer3 -c "CREATE TABLE app.ami_alerts_audit ( source_hostname character varying COLLATE pg_catalog."default", event_type character varying COLLATE pg_catalog."default", event_result character varying COLLATE pg_catalog."default", event_result_info1 character varying COLLATE pg_catalog."default", event_extra_info1 character varying COLLATE pg_catalog."default", event_extra_info2 character varying COLLATE pg_catalog."default", 	event_extra_info3 character varying COLLATE pg_catalog."default", 	businesspartnernumber character varying COLLATE pg_catalog."default", 	contractaccount character varying COLLATE pg_catalog."default", 	equipmentnumber character varying COLLATE pg_catalog."default", 	last_read_time timestamp without time zone, 	sum_consumption numeric, 	current_usage numeric, 	high_usage_limit numeric ) WITH ( OIDS = FALSE ) TABLESPACE pg_default";
-- PGPASSWORD="@mw@t3er@cld" psql -U postgres -d postgresql://hsyplclds001.amwater.net:5432/ao-aw-cloudseer3 -c "ALTER TABLE app.ami_alerts_audit ADD COLUMN created_at TIMESTAMP DEFAULT NOW()"
-- PGPASSWORD="@mw@t3er@cld" psql -U postgres -d postgresql://hsyplclds001.amwater.net:5432/ao-aw-cloudseer3 -c "ALTER TABLE app.ami_alerts_audit OWNER to postgres";
-- PGPASSWORD="@mw@t3er@cld" psql -U postgres -d postgresql://hsyplclds001.amwater.net:5432/ao-aw-cloudseer3 -c "\d app.ami_alerts_audit";
-- PGPASSWORD="@mw@t3er@cld" psql -U postgres -d postgresql://hsyplclds001.amwater.net:5432/ao-aw-cloudseer3 -c "SELECT * FROM app.ami_alerts_audit" | tail -3
-- PGPASSWORD="@mw@t3er@cld" psql -U postgres -d postgresql://hsyplclds001.amwater.net:5432/ao-aw-cloudseer3 -c "TRUNCATE app.ami_alerts_audit";


-- QA 1 =>
-- PGPASSWORD="System" psql -U postgres -d postgresql://hsynlapps004.amwaternp.net:5432/ao-aw-cloudseer3 -c "DROP TABLE app.ami_alerts_audit";
-- PGPASSWORD="System" psql -U postgres -d postgresql://hsynlapps004.amwaternp.net:5432/ao-aw-cloudseer3 -c "CREATE TABLE app.ami_alerts_audit ( source_hostname character varying COLLATE pg_catalog."default", event_type character varying COLLATE pg_catalog."default", event_result character varying COLLATE pg_catalog."default", event_result_info1 character varying COLLATE pg_catalog."default", event_extra_info1 character varying COLLATE pg_catalog."default", event_extra_info2 character varying COLLATE pg_catalog."default", 	event_extra_info3 character varying COLLATE pg_catalog."default", 	businesspartnernumber character varying COLLATE pg_catalog."default", 	contractaccount character varying COLLATE pg_catalog."default", 	equipmentnumber character varying COLLATE pg_catalog."default", 	last_read_time timestamp without time zone, 	sum_consumption numeric, 	current_usage numeric, 	high_usage_limit numeric ) WITH ( OIDS = FALSE ) TABLESPACE pg_default";
-- PGPASSWORD="System" psql -U postgres -d postgresql://hsynlapps004.amwaternp.net:5432/ao-aw-cloudseer3 -c "ALTER TABLE app.ami_alerts_audit ADD COLUMN created_at TIMESTAMP DEFAULT NOW()"
-- PGPASSWORD="System" psql -U postgres -d postgresql://hsynlapps004.amwaternp.net:5432/ao-aw-cloudseer3 -c "ALTER TABLE app.ami_alerts_audit OWNER to postgres";
-- PGPASSWORD="System" psql -U postgres -d postgresql://hsynlapps004.amwaternp.net:5432/ao-aw-cloudseer3 -c "\d app.ami_alerts_audit";
-- PGPASSWORD="System" psql -U postgres -d postgresql://hsynlapps004.amwaternp.net:5432/ao-aw-cloudseer3 -c "SELECT * FROM app.ami_alerts_audit" | tail -3
-- PGPASSWORD="System" psql -U postgres -d postgresql://hsynlapps004.amwaternp.net:5432/ao-aw-cloudseer3 -c "TRUNCATE app.ami_alerts_audit";



-- Table: app.meter_ami_reads

DROP TABLE app.meter_ami_reads;

CREATE TABLE app.meter_ami_reads
(
    read_interval_type character varying COLLATE pg_catalog."default",
    headend_meter_id character varying COLLATE pg_catalog."default",
    functionallocation character varying COLLATE pg_catalog."default",
    reading_datetime timestamp without time zone,
    timezone character varying COLLATE pg_catalog."default",
    reading_value numeric,
    unit_of_measure character varying COLLATE pg_catalog."default",
    consumption numeric,
    read_interval integer,
    equipmentnumber character varying COLLATE pg_catalog."default",
    installation character varying COLLATE pg_catalog."default",
    register character varying COLLATE pg_catalog."default",
    logicalregisternumber character varying COLLATE pg_catalog."default",
    businesspartnernumber character varying COLLATE pg_catalog."default",
    contractaccount character varying COLLATE pg_catalog."default",
    contract character varying COLLATE pg_catalog."default",
    district character varying COLLATE pg_catalog."default"
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE app.meter_ami_reads
    OWNER to postgres;


-- Table: app.meter_next_read

DROP TABLE app.meter_next_read;

CREATE TABLE app.meter_next_read
(
    meter_reading_unit character varying(20) COLLATE pg_catalog."default",
    next_read_date date
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE app.meter_next_read
    OWNER to postgres;

-- Table: app.meter_ami_projected_consumption

DROP TABLE app.meter_ami_projected_consumption;

CREATE TABLE app.meter_ami_projected_consumption
(
    business_partner_number character varying COLLATE pg_catalog."default",
    premise_id character varying COLLATE pg_catalog."default",
    connection_contract_number character varying COLLATE pg_catalog."default",
    meter_id character varying COLLATE pg_catalog."default",
    service_period_start date,
    service_period_end date,
    curr_read_consumption numeric,
    curr_read_date date,
    per_day_consumption numeric,
    days_to_next_read integer,
    projected_consumption numeric
)
WITH (
    OIDS = FALSE
)
TABLESPACE pg_default;

ALTER TABLE app.meter_ami_projected_consumption
    OWNER to postgres;
