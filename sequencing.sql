 --TY
 refresh table dtc_integrated.dtc_bm_order_line
 ;
 drop table if exists sedwa8.o2o_channel_order_ty
 ; 
 create table sedwa8.o2o_channel_order_ty 
 location 's3://ngap2-user-data/integratedknowledge/sedwa8/o2o_channel_order_ty'
stored as parquet
 as
 
 select a.member_id,
 min(first_web) as first_web,
 min(first_sneakers) as first_snkrs,
 min(first_nrc) as first_nrc,
 min(first_nike_app) as first_nike_app,
 min(first_ntc) as first_ntc,
 min(first_NFS) as first_NFS,
 min(first_NSO) as first_NSO,
 min(first_assist) as first_assist
 from
 (
 select distinct
    member_id
    ,browse_firstactive as first_web
    ,app_nsw_firstactive as first_sneakers
    ,app_running_firstactive as first_nrc
    ,app_omega_firstactive as first_nike_app
    ,case when app_womens_training_firstactive < app_men_training_firstactive then app_womens_training_firstactive else app_men_training_firstactive end as first_ntc
    ,NULL as first_NFS
    ,NULL as first_NSO
    ,NULL as first_assist
  from member.membership_results
  where time_slice between '201809' and '202008'
    and date_category = 'month_over_month'
    and all_activity_isactive = 1
    and lower(memloc_v2_geography) = 'north america'
    
    union
    
     select
      member_id
    ,NULL as first_web
    ,NULL as first_sneakers
    ,NULL as first_nrc
    ,NULL as first_nike_app
    ,NULL as first_ntc
      ,min(case when str_channel_rollup = 'NFS' then trans_dt else NULL end) as first_NFS
      ,min(case when str_channel_rollup = 'NSO' then trans_dt else NULL end) as first_NSO
      ,min(case when assist_dgtl_fulfilled_ind = 1 then trans_dt else NULL end) as first_assist
      from dtc_integrated.dtc_bm_order_line bm
      inner join dtc_reference.commerce_mapping m on bm.commerce_id = m.commerce_id
      inner join (select distinct member_id
                                ,source_id
                                ,preferred_geography
                           from member.member_xref mx 
                          where member_indicator = 'member'
                          and is_user_email_only = 'false'
                          and source_cd in ('profile','commerce')
                          and lower(is_user_converse) != 'true'
                          and email_addr is not null
                          and source_id is not null
                  ) mx on lower(m.buyer_user_id) = lower(mx.source_id)
        where bm.commerce_id is not null 
        and bm.commerce_id != ''
        and trans_month between '201809' and '202008'
        and lower(mx.preferred_geography) = 'north america'
        and str_region_cd = 'NA'
        group by member_id,first_web,first_sneakers,first_nrc,first_nike_app,first_ntc
    ) a
    join (select distinct member_id
    from member.member_xref
    where member_indicator = 'member'
                          and is_user_email_only = 'false'
                          and source_cd in ('profile','commerce')
                          and lower(is_user_converse) != 'true'
                          and email_addr is not null
                          and source_id is not null
                          and lower(preferred_geography) = 'north america'
                          and to_date(registration_date) between '2018-09-01' and '2020-08-31' --onboarded within last 2 years
                          --and to_date(registration_date) between '2018-09-01' and '2019-08-31' --onboarded 12 - 24 months ago
    
    )b
    on a.member_id = b.member_id
    group by a.member_id

    ;

    ###########################
    #SEQUENCING ON TOP QUERY 2:
    ###########################

    
    select exp1 || ' ' || exp2 || ' ' || exp3 || ' ' || exp4 || ' ' || exp5 || ' ' || exp6 || ' ' || exp7 || ' ' || exp8 as seq_order, count(distinct member_id) as members
from
(
select 
member_id,
max(case when seq = 1 then exp else 0 end) as exp1,
max(case when seq = 2 then exp else 0 end) as exp2,
max(case when seq = 3 then exp else 0 end) as exp3,
max(case when seq = 4 then exp else 0 end) as exp4,
max(case when seq = 5 then exp else 0 end) as exp5,
max(case when seq = 6 then exp else 0 end) as exp6,
max(case when seq = 7 then exp else 0 end) as exp7,
max(case when seq = 8 then exp else 0 end) as exp8
from
(
    select member_id, exp, first_dt, row_number() over (partition by member_id order by first_dt) as seq
    from
    (
      select distinct member_id,'web' as exp, substr(first_web,1,4) || '-' || substr(first_web,5,2) || '-' || substr(first_web,7,2)  as first_dt from sedwa8.o2o_channel_order_ty
          UNION 
         select distinct member_id,'SNKRS' as exp, substr(first_snkrs,1,4) || '-' || substr(first_snkrs,5,2) || '-' || substr(first_snkrs,7,2) as first_dt from sedwa8.o2o_channel_order_ty
          UNION 
         select distinct member_id,'NRC' as exp, substr(first_nrc,1,4) || '-' || substr(first_nrc,5,2) || '-' || substr(first_nrc,7,2) as first_dt from sedwa8.o2o_channel_order_ty
          UNION 
         select distinct member_id,'App' as exp, substr(first_nike_app,1,4) || '-' || substr(first_nike_app,5,2) || '-' || substr(first_nike_app,7,2) as first_dt from sedwa8.o2o_channel_order_ty
          UNION 
         select distinct member_id,'NTC' as exp, substr(first_ntc,1,4) || '-' || substr(first_ntc,5,2) || '-' || substr(first_ntc,7,2) as first_dt from sedwa8.o2o_channel_order_ty
          UNION 
         select distinct member_id,'NFS' as exp, first_NFS as first_dt from sedwa8.o2o_channel_order_ty
          UNION 
         select distinct member_id,'NSO' as exp, first_NSO as first_dt from sedwa8.o2o_channel_order_ty
         )
         where first_dt is not null
 ) group by member_id
) 
group by seq_order
order by members desc