------------------------------
-- map all unique case drug drugnames to rxnorm Vocabulary concept_ids
--
-- we will include non-standard and standard codes so we pick up brand names as well as ingredients etc
-- and roll-up to standard codes when we produce the statistics in a later process.
--
-- we map using the following precedence order.
--
-- regex drug name mapping
-- active ingredient drug name mapping (only current FAERS data has active ingredient)
-- nda drug_name mapping
-- manual usagi drug name mapping
--
-- Note. We map all drug roles including concomitant drugs
--
-- LTS COMPUTING LLC
------------------------------

-- temporarily create an index on the cdmv5 schema concept table to improve performance of all the mapping lookups
-- we will then drop it at the end of this script
set search_path = cdmv5;
drop index if exists vocab_concept_name_ix;
create index vocab_concept_name_ix on cdmv5.concept(vocabulary_id, standard_concept, upper(concept_name), concept_id);
analyze verbose cdmv5.concept;

set search_path = faers;

-- build a mapping table to generate a cleaned up version of the drugname for exact match joins to the concept table concept_name column 
-- for RxNorm concepts only 
-- NOTE we join to unique_all_case because we only need to map drugs for unique cases 
-- ie. where there are multiple versions of cases we only process the case with the latest (max) caseversion)

drop table if exists drug_regex_mapping;
create table drug_regex_mapping as
select distinct drug_name_original, drug_name_clean, concept_id, update_method
from (
	select distinct drugname as drug_name_original, upper(drugname) as drug_name_clean, cast(null as integer) as concept_id, null as update_method
	from drug a
	inner join unique_all_case b on a.primaryid = b.primaryid
	where b.isr is null
	union all
	select distinct drugname as drug_name_original, upper(drugname) as drug_name_clean, cast(null as integer) as concept_id, null as update_method
	from drug_legacy a
	inner join unique_all_case b on a.isr = b.isr
	where b.isr is not null
) aa

-- create an index on the mapping table to improve performance
drop index if exists drug_name_clean_ix;
create index drug_name_clean_ix on drug_regex_mapping(drug_name_clean);

-- remove the word tablet or "(tablet)" or the plural forms from drug name
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(.*)(\W|^)\(TABLETS?\)|TABLETS?(\W|$)', '\1\2', 'gi') 
where concept_id is null
and drug_name_clean ~*  '.*TABLET.*'

-- remove the word capsule or (capsule) or the plural forms from drug name
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(.*)(\W|^)\(CAPSULES?\)|CAPSULES?(\W|$)', '\1\2', 'gi')
where concept_id is null
and drug_name_clean ~*  '.*CAPSULE.*'

-- remove the drug strength in MG or MG/MG or MG\MG or MG / MG and their plural forms
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\(*(\y\d*\.*\d*\ *MG\,*\ *\/*\\*\ *\d*\.*\d*\ *(M2|ML)*\ *\,*\+*\ *\y)\)*', '\3', 'gi')
where concept_id is null
and drug_name_clean ~*  '\(*(\y\d*\.*\d*\ *MG\,*\ *\/*\\*\ *\d*\.*\d*\ *(M2|ML)*\ *\,*\+*\ *\y)\)*'

-- remove the drug strength in MILLIGRAMS or MILLIGRAMS/MILLILITERS or MILLIGRAMS\MILLIGRAM and their plural forms
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\(*(\y\d*\.*\d*\ *MILLIGRAMS?\,*\ *\/*\\*\ *\d*\.*\d*\ *(M2|MILLILITERS?)*\ *\,*\+*\ *\y)\)*', '\3', 'gi')
where concept_id is null
and drug_name_clean ~*  '\(*(\y\d*\.*\d*\ *MILLIGRAMS?\,*\ *\/*\\*\ *\d*\.*\d*\ *(M2|MILLILITERS?)*\ *\,*\+*\ *\y)\)*'

-- remove HYDROCHLORIDE and HCL
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(\y\ *(HCL|HYDROCHLORIDE)\y)', '\3', 'gi') 
where concept_id is null
and drug_name_clean ~*  '\(*(\y\ *(HCL|HYDROCHLORIDE)\ *\y)\)*'

-- remove FORMULATION, GENERIC, NOS
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\(\y(FORMULATION|GENERIC|NOS)\y\)|\y(FORMULATION|GENERIC|NOS)\y', '\3', 'gi')  
where concept_id is null
and drug_name_clean ~*  '\y(FORMULATION|GENERIC|NOS)\y'

-- lookup active ingredient from EU drug name
UPDATE drug_regex_mapping a
SET update_method = 'regex EU drug name to active ingredient', drug_name_clean = upper(b.active_substance)
FROM eu_drug_name_active_ingredient_mapping b
WHERE upper(a.drug_name_clean) = upper(b.brand_name)
AND a.concept_id is null

-- find exact mapping for active ingredient
UPDATE drug_regex_mapping a
SET update_method = 'regex EU drug name to active ingredient' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- lookup active ingredient from EU drug name in parentheses
update drug_regex_mapping a
set update_method = 'regex EU drug name in parentheses to active ingredient', drug_name_clean = upper(b.active_substance) 
from eu_drug_name_active_ingredient_mapping b
where upper(regexp_replace(a.drug_name_clean, '.* \((.*)\)', '\1', 'gi')) = upper(b.brand_name)
and a.concept_id is null
and a.drug_name_clean ~*  '.* \((.*)\)'

-- find exact mapping for active ingredient
UPDATE drug_regex_mapping a
SET update_method = 'regex EU drug name in parentheses to active ingredient' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- lookup RxNorm concept name using words from last set of parentheses in the drug name (typically this is the ingredient name(s) for a branded drug
UPDATE drug_regex_mapping a
SET update_method = 'regex ingredient name in parentheses' , concept_id = b.concept_id, drug_name_clean = upper(b.concept_name)
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = regexp_replace(a.drug_name_clean, '.* \((.*)\)', '\1', 'gi')
AND a.concept_id is null
and drug_name_clean ~*  '.* \((.*)\)';

-- lookup RxNorm concept name using words from last set of parentheses in the drug name for EU drug names (typically this is the ingredient name(s) for a branded drug
UPDATE drug_regex_mapping a
SET update_method = 'regex EU drug name ingredient name in parentheses' , concept_id = b.concept_id, drug_name_clean = upper(b.concept_name)
FROM cdmv5.concept b
inner join eu_drug_name_active_ingredient c
on upper(regexp_replace(a.drug_name_clean, '.* \((.*)\)', '\1', 'gi')) = upper(c.brand_name)
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = c.active_substance
AND a.concept_id is null
and drug_name_clean ~*  '.* \((.*)\)';

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex upper' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean;

-- remove trailing spaces or period or , characters
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '[ \.\,]$', '', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex trailing space or period chars' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove multiple occurrences of white space '
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '(\S) +', '\1 ', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove multiple white space' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove trailing spaces
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, ' +$', '', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove trailing spaces' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove leading spaces
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '^ +', '', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove leading spaces' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove single quotes and double quotes'
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '[''""]', '', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove single quotes' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove '^*$?' chars
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '[\*\^\$\?]', '', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove ^*$? punctuation chars' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- change \ to / char
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\\', '/', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex change forward slash to back slash' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove spaces before closing parenthesis char
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, ' +\)', ')', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove spaces before closing parenthesis' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove UNKNOWN or UNK 
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, '\(\y(UNKNOWN|UNK)\y\)|\y(UNKNOWN|UNK)\y', '', 'gi')  
where concept_id is null
and drug_name_clean ~*  '\y(UNKNOWN|UNK)\y'

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove (unknown)' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove BLINDED
update drug_regex_mapping
set drug_name_clean = regexp_replace(drug_name_clean, ' *blinded *', '', 'gi')
where concept_id is null;

-- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove blinded' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- remove \nnnnn\
update drug_regex_mapping a
SET drug_name_clean = regexp_replace(drug_name_clean, '\/\d+\/\ *', '', 'gi') 
where concept_id is null and 
drug_name_original ~* '.*\/\d+\/.*';

- find exact mapping
UPDATE drug_regex_mapping a
SET update_method = 'regex remove /nnnnn/' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.drug_name_clean
and a.concept_id is null;

-- derive RxNorm concepts for multi ingredient drugs (in any order of occurrence in the drug name) and for single ingredient clinical names and brand name drugs from within complex drug name strings

set search_path = faers;

drop table if exists drug_regex_mapping_words;
create table drug_regex_mapping_words as
select distinct *
from (
select drug_name_original, concept_name, concept_id, update_method, unnest(word_list::text[]) as word
from (
select drug_name_original, concept_name, concept_id, update_method, regexp_split_to_array(upper(drug_name_original), E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
from (
	select distinct drugname as drug_name_original, cast(null as varchar) as concept_name, cast(null as integer) as concept_id, null as update_method
	from drug a
	inner join unique_all_case b on a.primaryid = b.primaryid
	where b.isr is null
	union all
	select distinct drugname as drug_name_original, cast(null as varchar) as concept_name, cast(null as integer) as concept_id, null as update_method
	from drug_legacy a
	inner join unique_all_case b on a.isr = b.isr
	where b.isr is not null
) aa
order by drug_name_original desc
) bb
) cc 
where word NOT IN ('','CREAM','PATCH','GEL','POWDER','SOLUTION','SUSPENSION','OIL','LOTION','CAPSULE','CAPLET','TABLET','SPRAY','LIQUID','OINTMENT','MOUTHWASH','SYRUP','HCL','HYDROCHLORIDE','SUPPOSITORY','ACETIC','SODIUM','CALCIUM','SULPHATE','MONOHYDRATE')
order by 1

-- map drug names containing a brand name
update drug_regex_mapping_words c
SET update_method = 'brand name match' , concept_name = b.concept_name, concept_id = b.concept_id 
from (
select distinct a.drug_name_original, max(upper(b.concept_name)) as concept_name, max(b.concept_id) as concept_id 
from drug_regex_mapping_words a
inner join cdmv5.concept b
on a.word = upper(b.concept_name)
and b.vocabulary_id = 'RxNorm'
and b.concept_class_id = 'Brand Name' 
group by a.drug_name_original
) b
where c.drug_name_original = b.drug_name_original
and c.concept_id is null;


-- create a target mapping table of multi ingredient drug names with each ingredient word concatenated alphabetically into a space separated string 
drop table if exists rxnorm_mapping_multi_ingredient_list;
create table rxnorm_mapping_multi_ingredient_list as
select ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
	select concept_id, concept_name, string_agg(word, ' ' order by word) as ingredient_list  from (
		select concept_id,  concept_name, unnest(word_list::text[]) as word
		from (
			select concept_id, concept_name, regexp_split_to_array(upper(concept_name), E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
			from (
				select upper(concept_name) as concept_name, concept_id
				from cdmv5.concept b
				where b.vocabulary_id = 'RxNorm'
				and b.concept_class_id = 'Clinical Drug Form' 
				and concept_name like '%\/%'
			) aa
			order by concept_name desc
		) bb
	) cc
	where word not in ('')
	and word not in (select distinct unnest(regexp_split_to_array(upper(concept_name), E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
			from  cdmv5.concept b
			where b.vocabulary_id = 'RxNorm'
			and b.concept_class_id = 'Dose Form' order by 1)
	and word not in ('','-', ' ', 'A', 'AND', 'EX', '10A','11A','12F','18C','19F','99M','G','G1','G2',
			'G3','G4','H','I','IN','JELLY','LEAF','O','OF','OR','P','S','T','V','WITH','X','Y','Z') 
	and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y' 
	group by concept_id, concept_name
) dd
group by ingredient_list
order by 2,3;


-- create a source multi-ingredient drug mapping table by extracting the multi-ingredient drug names with each ingredient word concatenated alphabetically into a space separated string
drop table if exists drug_mapping_multi_ingredient_list;
create table drug_mapping_multi_ingredient_list as
select drug_name_original, ingredient_list, max(concept_id) as concept_id, max(concept_name) as concept_name
from (
	select concept_id, drug_name_original, concept_name, string_agg(word, ' ' order by word) as ingredient_list  from (
		select distinct concept_id,  drug_name_original, concept_name, unnest(word_list::text[]) as word
		from (
			select concept_id, drug_name_original, concept_name, regexp_split_to_array(upper(drug_name_original), E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+') as word_list
			from (
				select distinct drugname as drug_name_original, cast(null as varchar) as concept_name, cast(null as integer) as concept_id, null as update_method
				from drug a
				inner join unique_all_case b on a.primaryid = b.primaryid
				where b.isr is null
				union all
				select distinct drugname as drug_name_original, cast(null as varchar) as concept_name, cast(null as integer) as concept_id, null as update_method
				from drug_legacy a
				inner join unique_all_case b on a.isr = b.isr
				where b.isr is not null
			) aa
			order by concept_name desc
		) bb
	) cc
	where word not in ('')
	and word not in (select distinct unnest(regexp_split_to_array(upper(concept_name), E'[\ \,\(\)\{\}\\\\/\^\%\.\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+'))
			from  cdmv5.concept b
			where b.vocabulary_id = 'RxNorm'
			and b.concept_class_id = 'Dose Form' order by 1)
	and word  in (	select * 
			from (
				select distinct unnest(regexp_split_to_array(upper(concept_name), E'[\ \,\(\)\{\}\\\\/\^\%\~\`\@\#\$\;\:\"\'\?\<\>\&\^\!\*\_\+\=]+')) as word
				from  cdmv5.concept b
				where b.vocabulary_id = 'RxNorm'
				and b.concept_class_id in ('Clinical Drug Form')
				and b.concept_name like '%\/%' 
			) aa 
			where word not in ('','-', ' ', 'A', 'AND', 'EX', '10A','11A','12F','18C','19F','99M','G','G1','G2',
			'G3','G4','H','I','IN','JELLY','LEAF','O','OF','OR','P','S','T','V','WITH','X','Y','Z') 
			and word !~ '^\d+$|\y\d+\-\d+\y|\y\d+\.\d+\y' 
			order by 1
			)
	group by concept_id, drug_name_original, concept_name
) dd
group by drug_name_original, ingredient_list
order by 2,3


-- map drug names containing multiple ingredient names to clinical drug form
update drug_regex_mapping_words c
SET update_method = 'multiple ingredient match' , concept_name = b.concept_name, concept_id = b.concept_id 
from (
select distinct a.drug_name_original, max(upper(b1.concept_name)) as concept_name, max(b1.concept_id) as concept_id 
from drug_mapping_multi_ingredient_list a
inner join rxnorm_mapping_multi_ingredient_list b1
on a.ingredient_list = b1.ingredient_list
group by a.drug_name_original
) b
where c.drug_name_original = b.drug_name_original
and c.concept_id is null

-- map drug names containing a single ingredient name
update drug_regex_mapping_words c
SET update_method = 'single ingredient match' , concept_name = b.concept_name, concept_id = b.concept_id 
from (
select distinct a.drug_name_original, max(upper(b.concept_name)) as concept_name, max(b.concept_id) as concept_id 
from drug_regex_mapping_words a
inner join cdmv5.concept b
on a.word = upper(b.concept_name)
and b.vocabulary_id = 'RxNorm'
and b.concept_class_id = 'Ingredient' 
where a.concept_id is null
group by a.drug_name_original
having count(*) = 1
) b
where c.drug_name_original = b.drug_name_original
and c.concept_id is null;

-- update the drug regex mapping table with the brand names, multiple and single ingredient drug names 
update drug_regex_mapping c
SET update_method = b.update_method , drug_name_clean = b.concept_name, concept_id = b.concept_id 
from (
select distinct drug_name_original, concept_name, concept_id, update_method from drug_regex_mapping_words where concept_id is not null
) b
where c.drug_name_original = b.drug_name_original
and c.concept_id is null;


--------------------------------------------------

-- create active ingredient mapping table -- note only FAERS current data has active ingredient 

drop table if exists drug_ai_mapping;
create table drug_ai_mapping as
select distinct drugname as drug_name_original, prod_ai, cast(null as integer) as concept_id, null as update_method
from drug a
inner join unique_case b on a.primaryid = b.primaryid;

drop index if exists prod_ai_ix;
create index prod_ai_ix on drug_ai_mapping(prod_ai);

-- find exact mapping using the active ingredient provided in the drug table
UPDATE drug_ai_mapping a
SET update_method = 'drug active ingredients' , concept_id = b.concept_id
FROM cdmv5.concept b
WHERE b.vocabulary_id = 'RxNorm'
AND upper(b.concept_name) = a.prod_ai;

-----------------------------------------------

-- create NDA (new drug application) number mapping table
-- (NDA num maps to ingredient(s) in the FDA orange book reference dataset)

-- note the following table should be created one time when the FDA orange book (NDA ingredient lookup) table is loaded
drop table if exists nda_ingredient;
create table nda_ingredient as
select distinct appl_no, ingredient
from nda; 

drop table if exists drug_nda_mapping;
create table drug_nda_mapping as
select distinct drug_name_original, nda_num, nda_ingredient, concept_id, update_method
from (
	select distinct drugname as drug_name_original, nda_num, null as nda_ingredient, cast(null as integer) as concept_id, null as update_method
	from drug a
	inner join unique_all_case b on a.primaryid = b.primaryid
	where b.isr is null and nda_num is not null
	union all
	select distinct drugname as drug_name_original, nda_num, null as nda_ingredient, cast(null as integer) as concept_id, null as update_method
	from drug_legacy a
	inner join unique_all_case b on a.isr = b.isr
	where b.isr is not null and nda_num is not null
) aa;

drop index if exists nda_num_ix;
create index nda_num_ix on drug_nda_mapping(nda_num);

-- find exact mapping using the drug table nda_num, NDA to ingredient lookup
UPDATE drug_nda_mapping a
SET update_method = 'drug nda_num ingredients' , nda_ingredient = nda_ingredient.ingredient, concept_id = b.concept_id
FROM cdmv5.concept b
inner join nda_ingredient
on upper(b.concept_name) = nda_ingredient.ingredient
WHERE b.vocabulary_id = 'RxNorm'
AND nda_ingredient.appl_no = a.nda_num;

-----------------------------------------------

-- combine all the different types of mapping into a single combined drug mapping table across legacy LAERS data and current FAERS data

drop table if exists combined_drug_mapping;
create table combined_drug_mapping as
select distinct primaryid, isr, drug_name_original, lookup_value, concept_id, update_method
from (
	select distinct b.primaryid, b.isr, drugname as drug_name_original, cast(null as varchar) as lookup_value, cast(null as integer) as concept_id, cast(null as varchar) as update_method
	from drug a
	inner join unique_all_case b on a.primaryid = b.primaryid
	where b.isr is null
	union all
	select distinct b.primaryid, b.isr, drugname as drug_name_original, cast(null as varchar) as lookup_value, cast(null as integer) as concept_id, cast(null as varchar) as update_method
	from drug_legacy a
	inner join unique_all_case b on a.isr = b.isr
	where b.isr is not null
) aa ;

drop index if exists combined_drug_mapping_ix;
create index combined_drug_mapping_ix on combined_drug_mapping(upper(drug_name_original));

-- update using drug_regex_mapping 
UPDATE combined_drug_mapping a
SET  update_method = b.update_method , lookup_value = drug_name_clean, concept_id = b.concept_id
FROM drug_regex_mapping b
WHERE upper(a.drug_name_original) = upper(b.drug_name_original);

-- update using drug_ai_mapping
UPDATE combined_drug_mapping a
SET  update_method = b.update_method , lookup_value = prod_ai, concept_id = b.concept_id
FROM drug_ai_mapping b
WHERE upper(a.drug_name_original) = upper(b.drug_name_original)
and a.concept_id is null;

-- update using drug_nda_mapping
UPDATE combined_drug_mapping a
SET  update_method = b.update_method , lookup_value = nda_ingredient, concept_id = b.concept_id
FROM drug_nda_mapping b
WHERE upper(a.drug_name_original) = upper(b.drug_name_original)
and a.concept_id is null;

-- update using drug_usagi_mapping
-- manually curated drug mappings
UPDATE combined_drug_mapping a
SET  update_method = b.update_method , lookup_value = b.concept_name, concept_id = b.concept_id
FROM drug_usagi_mapping b
WHERE upper(a.drug_name_original) = upper(b.drug_name_original)
and a.concept_id is null;

-- update unknown drugs where drug name starts with UNKNOWN
update combined_drug_mapping 
set update_method = 'unknown drug'
where upper(drug_name_original) ~* '^UNKNOWN.*' 
and update_method is null;

-- update unknown drugs where drug name starts with OTHER
update combined_drug_mapping 
set update_method = 'unknown drug'
where upper(drug_name_original) ~* '^OTHER.*' 
and update_method is null;

-- update unknown drugs where drug name starts with UNSPECIFIED
update combined_drug_mapping 
set update_method = 'unknown drug'
where upper(drug_name_original) ~* '^UNSPECIFIED.*' 
and update_method is null;

