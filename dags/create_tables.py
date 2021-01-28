immigration_table_create = ("""
CREATE TABLE IF NOT EXISTS public.immigration (
	migration_id bigint NOT NULL,
	persons_id bigint NOT NULL,
	city_id varchar NOT NULL,
	date_id date NOT NULL,
	visa_type varchar NULL,
    mode_of_entry bigint NULL,
    visa_post varchar NULL,
    arrival_flag varchar NULL,
    departure_flag varchar NULL,
    update_flag varchar NULL,
	CONSTRAINT immigration_pkey PRIMARY KEY (migration_id)
);
""")

persons_table_create = ("""
CREATE TABLE IF NOT EXISTS public.persons (
	persons_id bigint NOT NULL,
    birthyear int NULL,
    age int NULL,
    gender varchar NULL,
    occupation varchar NULL,
    country_of_origin int NULL,
    country_of_residence int NULL,
	CONSTRAINT persons_pkey PRIMARY KEY (persons_id)
);
""")

dates_table_create = ("""
CREATE TABLE IF NOT EXISTS public.dates (
	date_id date NOT NULL,
	day int NOT NULL,
	week int NOT NULL,
	month varchar NOT NULL,
	year int NOT NULL,
	weekday varchar NOT NULL,
	CONSTRAINT dates_pkey PRIMARY KEY (date_id)
);
""")

city_table_create = ("""
CREATE TABLE IF NOT EXISTS public.city (
    city_id varchar NOT NULL,
	name varchar(256) NULL,
	state varchar(256) NULL,
	latitude varchar(256) NULL,
	longitude varchar(256) NULL,
    average_temperature numeric NULL,
    total_population int NULL,
    race varchar NULL,
    count int NULL,
    foreign_born numeric NULL,
	CONSTRAINT city_pkey PRIMARY KEY (city_id)
);
""")

staging_immigration_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_immigration (
    id INTEGER NULL,
    cicid NUMERIC NOT NULL,
    i94yr NUMERIC NULL,
    i94mon NUMERIC NULL,
    i94cit NUMERIC NULL,
    i94res NUMERIC NULL,
    i94port VARCHAR NULL,
    arrdate VARCHAR NULL,
    i94mode NUMERIC NULL,
    i94addr VARCHAR NULL,
    depdate VARCHAR NULL,
    i94bir NUMERIC NULL,
    i94visa NUMERIC NULL,
    count NUMERIC NULL,
    dtadfile DATE NULL,
    visapost VARCHAR NULL,
    occup VARCHAR NULL,
    entdepa VARCHAR NULL,
    entdepd VARCHAR NULL,
    entdepu VARCHAR NULL,
    matflag VARCHAR NULL,
    biryear NUMERIC NULL,
    dtaddto DATE NULL,
    gender VARCHAR NULL,
    insnum VARCHAR NULL,
    airline VARCHAR NULL,
    admnum NUMERIC NOT NULL,
    fltno VARCHAR NULL,
    visatype VARCHAR NULL,
    PRIMARY KEY(cicid)
    );
"""
                                   )
                              

staging_demographics_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_usdemographics (
    id INTEGER NULL,
    city VARCHAR NOT NULL,
    state VARCHAR NOT NULL,
    median_age NUMERIC NULL,
    male_population NUMERIC NULL,
    female_population NUMERIC NULL,
    total_population INTEGER NULL,
    number_of_veterans NUMERIC NULL,
    foreign_born NUMERIC NULL,
    average_household_size NUMERIC NULL,
    state_code VARCHAR NULL,
    race VARCHAR NULL,
    count INTEGER NULL,
    PRIMARY KEY(city, state)
);
"""
                                    )

staging_climate_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_climate (
    id INTEGER NULL,
    date VARCHAR NULL,
    average_temperature NUMERIC NULL,
    average_temp_uncertainty NUMERIC NULL,
    city VARCHAR NOT NULL,
    country VARCHAR NULL,
    latitude VARCHAR NOT NULL,
    longitude VARCHAR NOT NULL,
    PRIMARY KEY(city)
);
"""
                               )

staging_airportcodes_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_airportcodes (
    id int NULL,
    ident varchar NULL,
    type varchar NULL,
    elevation numeric NULL,
    continent VARCHAR NULL,
    iso_country VARCHAR NULL,
    iso_region VARCHAR NULL,
    municipality VARCHAR NOT NULL,
    gps_code varchar NULL,
    iata_code varchar NULL,
    local_code varchar NULL,
    PRIMARY KEY(municipality)
);
"""
                               )
