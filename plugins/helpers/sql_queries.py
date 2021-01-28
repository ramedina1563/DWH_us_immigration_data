class SqlQueries:
    
    immigration_table_insert = ("""
        SELECT 
            cast(immi.admnum AS BIGINT),
            cast(immi.cicid AS BIGINT),
            immi.i94port,
            immi.dtadfile,
            immi.visatype,            
            cast(immi.i94mode AS BIGINT),
            immi.visapost,
            immi.entdepa,
            immi.entdepd,
            immi.entdepu     
        FROM staging_immigration immi
    """)
    
    persons_table_insert = ("""
        SELECT 
            cast(cicid AS INTEGER),
            cast(i94yr AS INTEGER),
            cast(i94bir AS INTEGER),
            gender,
            occup,
            cast(i94cit AS INTEGER),
            cast(i94res AS INTEGER)
        FROM staging_immigration
    """)
    
    dates_table_insert = ("""
        SELECT DISTINCT
            immi.dtadfile,
            EXTRACT(d FROM immi.dtadfile),
            EXTRACT(w FROM immi.dtadfile),
            EXTRACT(mon FROM immi.dtadfile),
            EXTRACT(y FROM immi.dtadfile),
            EXTRACT(weekday FROM immi.dtadfile)
        FROM staging_immigration immi
    """)
    
    city_table_insert = ("""
    SELECT DISTINCT
            immi.i94port,
            air.municipality,
            demo.state,
            clim.latitude,
            clim.longitude,
            clim.average_temperature,
            demo.total_population,
            demo.foreign_born
        FROM staging_immigration immi
            LEFT JOIN staging_airportcodes air
                ON air.iata_code = immi.i94port
                INNER JOIN staging_usdemographics demo
                    ON air.municipality = demo.city
                    LEFT JOIN staging_climate clim
                        ON clim.city = demo.city
    
    """)