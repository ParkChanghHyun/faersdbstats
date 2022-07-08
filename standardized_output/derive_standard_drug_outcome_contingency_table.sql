------------------------------
--
-- This SQL script computes the 2x2 contingency table for all unique legacy and current case drug/outcome pairs in a table called standard_drug_current_outcome_contingency_table
--
--
-- LTS COMPUTING LLC
------------------------------

set search_path = faers;

drop index if exists standard_drug_outcome_count_ix;
create index standard_drug_outcome_count_ix on standard_drug_outcome_count(drug_concept_id, outcome_concept_id);
drop index if exists standard_drug_outcome_count_2_ix;
create index standard_drug_outcome_count_2_ix on standard_drug_outcome_count(drug_concept_id);
drop index if exists standard_drug_outcome_count_3_ix;
create index standard_drug_outcome_count_3_ix on standard_drug_outcome_count(outcome_concept_id);
drop index if exists standard_drug_outcome_count_4_ix;
create index standard_drug_outcome_count_4_ix on standard_drug_outcome_count(drug_outcome_pair_count);
analyze verbose standard_drug_outcome_count;

-- get count_d1 
drop table if exists standard_drug_outcome_count_d1;
create table standard_drug_outcome_count_d1 as
with cte as (
select sum(drug_outcome_pair_count) as count_d1 from standard_drug_outcome_count 
)  
select drug_concept_id, outcome_concept_id, count_d1
from standard_drug_outcome_count a,  cte -- we need the same total for all rows so do cross join!

--============= On a 4+ CPU postgresql server, run the following 3 queries in 3 different postgresql sessions so they run concurrently!

-- get count_a and count_b 
set search_path = faers;
drop table if exists standard_drug_outcome_count_a_count_b;

--##2. b create
create table standard_drug_outcome_count_a_count_b 
as with aa  as  
(select drug_concept_id,sum(drug_outcome_pair_count) as sum_cnt
	from standard_drug_outcome_count
	group by drug_concept_id
)
select t1.drug_concept_id
	, outcome_concept_id
	,drug_outcome_pair_count as count_a
	,sum_cnt-drug_outcome_pair_count as count_b
from standard_drug_outcome_count t1
	left outer join aa  t2
	on t1.drug_concept_id=t2.drug_concept_id
;


-- get count_c 
set search_path = faers;
drop table if exists standard_drug_outcome_count_c;
--##3. C Create
create table standard_drug_outcome_count_c 
as with aa  as  
(select outcome_concept_id,sum(drug_outcome_pair_count) as sum_cnt
	from standard_drug_outcome_count
	group by outcome_concept_id
)
select t1.drug_concept_id
	, t1.outcome_concept_id
	,sum_cnt-drug_outcome_pair_count as count_c
from standard_drug_outcome_count t1
	left outer join aa  t2
	on t1.outcome_concept_id=t2.outcome_concept_id
;

-- get count d2 
set search_path = faers;
drop table if exists standard_drug_outcome_count_d2;
--##4. D 생성
create table standard_drug_outcome_count_d2 
as with aa  as  
(select drug_concept_id,sum(drug_outcome_pair_count) as sum_cnt_aa
	from faers.standard_drug_outcome_count
	group by drug_concept_id
)
,bb  as  
(select outcome_concept_id,sum(drug_outcome_pair_count) as sum_cnt_bb
	from standard_drug_outcome_count
	group by outcome_concept_id
)

,uni_t_a as
(
	select t1.drug_concept_id
		, t1.outcome_concept_id
		,t1.drug_outcome_pair_count 
		,t2.sum_cnt_aa
	from standard_drug_outcome_count t1
		left outer join aa  t2
		on t1.drug_concept_id=t2.drug_concept_id
)
,uni_t_b as
(
	select t1.drug_concept_id
		, t1.outcome_concept_id
		,t1.drug_outcome_pair_count
		,t1.sum_cnt_aa
		,t2.sum_cnt_bb
	from uni_t_a t1
		left outer join bb  t2
		on t1.outcome_concept_id=t2.outcome_concept_id
)
select drug_concept_id
	,outcome_concept_id
	,sum_cnt_aa+sum_cnt_bb-drug_outcome_pair_count as count_d2
from uni_t_b;


--=============

-- Only run the below query when ALL OF THE ABOVE 3 QUERIES HAVE COMPLETED!
-- combine all the counts into a single contingency table
drop table if exists standard_drug_outcome_contingency_table;
create table standard_drug_outcome_contingency_table as		-- 1 second
select ab.drug_concept_id, ab.outcome_concept_id, count_a, count_b, count_c, (count_d1 - count_d2) as count_d
from standard_drug_outcome_count_a_count_b ab
inner join standard_drug_outcome_count_c c
on ab.drug_concept_id = c.drug_concept_id and ab.outcome_concept_id = c.outcome_concept_id
inner join standard_drug_outcome_count_d1 d1
on ab.drug_concept_id = d1.drug_concept_id and ab.outcome_concept_id = d1.outcome_concept_id
inner join standard_drug_outcome_count_d2 d2
on ab.drug_concept_id = d2.drug_concept_id and ab.outcome_concept_id = d2.outcome_concept_id;

