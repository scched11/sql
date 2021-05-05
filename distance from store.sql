SELECT DISTINCT MEMBER_ID,
CAST(geo.LAT as float) as member_lat,
CAST(geo.LNG as float) as member_long

FROM member.member_xref xref
INNER JOIN sedwa8.zip_lat_long geo --created in a separate notebook
on geo.Convert_zip = xref.MEMLOC_V2_POSTAL_CODE

WHERE MEMLOC_V2_FLAG_HAS_POSTAL_CODE = 1

;


%sql  
drop table if exists sedwa8.member_distance_master;
create table sedwa8.member_distance_master
location 's3://ngap2-user-data/integratedknowledge/sedwa8/member_distance_master'

        SELECT 
        Member_ID,
        max(case when distance_Rank = 1 and store_type = 'NFS' then nearest_store_distance else null end) as nearest_nfs_distance, 
        max(case when distance_Rank = 1 and store_type = 'NFS' then store_nbr_name else null end) as nearest_nfs_store,
        max(case when distance_Rank = 1 and store_type = 'NSO' then nearest_store_distance else null end) as nearest_nso_distance,
        max(case when distance_Rank = 1 and store_type = 'NSO' then store_nbr_name else null end) as nearest_nso_store,
        min(nearest_store_distance) as nearest_store_distance,
        ntile(10) over (order by min(nearest_store_distance) asc) as decile

        from ( select distinct a.member_id,
                store_type,
                store_nbr_name,
                nearest_store_distance,
                rank() over (partition by store_type, a.member_id order by nearest_store_distance asc) as distance_rank
                
                from ( /*FOR EACH MEMBER THAT HAS A ZIP CODE LAT & LONG*/
                            select distinct member_id,
                            cast(geo.lat as float) as member_lat,
                            cast(geo.lng as float) as member_long,
                            s.store_nbr_name,
                            s.store_type,
                            s.store_lat,
                            s.store_long,
                            2 * 3961 * asin(sqrt( power((sin(radians((s.store_lat - cast(geo.lat as float)) / 2))) , 2) + cos(radians(cast(geo.lat as float))) * cos(radians(s.store_lat)) * power((sin(radians((s.store_long - cast(geo.lng as float)) / 2))) , 2) )) as nearest_store_distance

                            from member.member_xref xref
                            inner join sedwa8.zip_lat_long geo --joining in the table we created of the center of every zip code. We then join it to the member's zip to get the approx. member lat/long
                            on geo.convert_zip = xref.memloc_v2_postal_code

                            cross join   --gives us every combination of table A and table B
                                   ( select distinct concat(str_id_pad,concat(' - ',str_nm)) as store_nbr_name,
                                      str_channel_rollup as store_type,
                                      cast(str_lat as float) as store_lat,
                                      cast(str_long as float) as store_long

                                      from dtc_integrated.dtc_bm_order_line  bmol
                                      where bmol.str_lat is not null
                                      and bmol.str_long is not null
                                      and to_date(bmol.trans_end_dttm) between '2020-10-01' and '2020-11-30' --lets get only stores open at the end of jan 2020 2019
                                      and str_id_pad not in ('0109','0224','0324') --what is this? Employee? 0324 is SNKRS ATL
                                      and bmol.str_region_cd = 'NA'
                                      and str_channel_rollup in ('NSO','NFS')

                                      group by 1,2,3,4
                                      having sum(gross_amt_usd) > 1000 --only give us stores with sales to further prevent noise from entering the data
                                   )  s   
                            where memloc_v2_flag_has_postal_code = 1
                            and lower(preferred_geography) in ('north america')
                            --and member_id in ('674044b3310fb70a5f060be597e6fec6','0000312cd7f75e0d9e1b59017e688ac8','0000dce7e54f81234717b1ce8f6e4ad4') --For QA against previous cell
                      ) a

               group by 1,2,3,4
              )

        where distance_rank = 1
        group by 1


        ;





        %sql
SELECT 
COUNT(DISTINCT MEMBER_ID) as Total_Members_NA_WithZip,

COUNT(CASE WHEN Nearest_store_distance < 1 then member_id ELSE NULL end) as Mems_within_1_cnt,
ROUND(COUNT(CASE WHEN Nearest_store_distance < 1 then member_id ELSE NULL end) / COUNT(DISTINCT MEMBER_ID),3) as Mems_within_1_pct,

COUNT(CASE WHEN Nearest_store_distance < 5 then member_id ELSE NULL end) as Mems_within_5_cnt,
ROUND(COUNT(CASE WHEN Nearest_store_distance < 5 then member_id ELSE NULL end) / COUNT(DISTINCT MEMBER_ID),3) as Mems_within_5_pct,

COUNT(CASE WHEN Nearest_store_distance < 10 then member_id ELSE NULL end) as Mems_within_10_cnt,
ROUND(COUNT(CASE WHEN Nearest_store_distance < 10 then member_id ELSE NULL end) / COUNT(DISTINCT MEMBER_ID),3) as Mems_within_10_pct,

COUNT(CASE WHEN Nearest_store_distance < 20 then member_id ELSE NULL end) as Mems_within_20_cnt,
ROUND(COUNT(CASE WHEN Nearest_store_distance < 20 then member_id ELSE NULL end) / COUNT(DISTINCT MEMBER_ID),3) as Mems_within_20_pct,

COUNT(CASE WHEN Nearest_store_distance < 50 then member_id ELSE NULL end) as Mems_within_50_cnt,
ROUND(COUNT(CASE WHEN Nearest_store_distance < 50 then member_id ELSE NULL end) / COUNT(DISTINCT MEMBER_ID),3) as Mems_within_50_pct,

COUNT(CASE WHEN Nearest_store_distance < 100 then member_id ELSE NULL end) as Mems_within_100_cnt,
ROUND(COUNT(CASE WHEN Nearest_store_distance < 100 then member_id ELSE NULL end) / COUNT(DISTINCT MEMBER_ID),3) as Mems_within_100_pct

FROM sedwa8.member_distance_master

