#Step 1) Build a dataset with mixed control / treatment members. 
#Step 2) Build 2 mutually exclusive datasets from Step 1), 1 for control and 1 for treatment.  Most often the control group will be much larger than the treatment group
#Step 3) Build an understanding of which dimensions are of particular interest across the difference between control and treatment, these will be used to match pairs across. i.e. spend, age, geography, etc.
#Step 4) The first part (subtitled 'c') randomly ranks control members by the dimensions in the partition, for each combination of dimensions you'll get a random ranking of members.
		#The second part ('t') counts the size of each combination of dimensions in the treatment (again, almost always much smaller).  
		#We then join 'c' and 't' on each dimension and apply a where clause limiting the number of control members to the count of members in each combination of treatment dimensions.  This forces the counts across the dimensions to be much close than they normally would be, depending on the size of the strata it usually will match perfectly (one exception is matching on MSA 20 or zip codes...).  


drop table if exists x.omnichannel_matched_pairs_control_matched
;
create table x.omnichannel_matched_pairs_control_matched
location 's3://ngap2-user-data/gck/x.db/omnichannel_matched_pairs_control_matched'
stored as parquet
as
select c.*
from (
  select *
    , rank() over(partition by preferred_retail_geo, preferred_gender, age_group, demand_last2years order by rand()) as rnk
  from jponzi.omnichannel_matched_pairs_control #dataset of control members
) c
join (
  -- counts the number of members in each strata
  select preferred_retail_geo, preferred_gender, age_group, demand_last2years
    , count(distinct member_id) as members
  from jponzi.omnichannel_matched_pairs_treatment #dataset of "treated" members
  group by 1, 2, 3, 4
) t on c.preferred_retail_geo = t.preferred_retail_geo #where the magic happens -- the features we're matching on
  and c.preferred_gender = t.preferred_gender
  and c.age_group = t.age_group
  and c.demand_last2years = t.demand_last2years
where rnk <= members

#union the below to verify cohort counts line up. 

select 'control' as cohort, preferred_retail_geo, preferred_gender, age_group, demand_last2years, count(distinct member_id) as members
from jponzi.omnichannel_matched_pairs_control_matched
group by 1, 2, 3, 4, 5
 
union all
 
select 'treatment' as cohort, preferred_retail_geo, preferred_gender, age_group, demand_last2years, count(distinct member_id) as members
from jponzi.omnichannel_matched_pairs_treatment
group by 1, 2, 3, 4, 5