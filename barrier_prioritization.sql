--create prioritized barriers layer
drop table if exists automated.barriers_prioritized_multis;
create table automated.barriers_prioritized_multis (
  id SERIAL PRIMARY KEY,
  geom geometry (multipoint,26915),
--safety/existing conditions
  bkpd_crash_500ft int,
  bw_modeshr float,
  rail_volume int,
  avg_slope float,
  wiki_probarea_qtr int,
--demand
  popden_2014 float,
  popden_2040 float,
  empden_2014 float,
  empden_2040 float,
  trans_riders_qtr float,
  schools_half int,
  colleges_half int,
  reg_parks_half int,
  wiki_new_xing_qtr int,
--connectivity
  ex_local_miles_qtr float,
  pl_local_miles_qtr float,
  ex_reg_fac_half int,
  pl_reg_fac_half int,
  expl_reg_miles_half float,
  rbtn_corridor int,
  rbtn_alignment int,
  dist2ex_crossing_m float,
--equtiy
  acp int,
  acp50 int,
  age_under_15_pct float,
  age_65_plus_pct float,
  zero_car_pct float,
  ppl_of_color_pct float,
  wiki_women_qtr int,
  wiki_pplofcolor_qtr int,

  nearest_crossing_id int,
--scld_ safety/existing conditions
  scld__bkpd_crash_500ft float,
  scld__bw_modeshr float,
  scld__rail_volume float,
  scld__avg_slope float,
  scld__wiki_probarea_qtr float,
--scld_ safety/existing conditions
  scld__popden_2014 float,
  scld__popden_2040 float,
  scld__empden_2014 float,
  scld__empden_2040 float,
  scld_trans_riders_qtr float,
  scld__schools_half float,
  scld__colleges_half float,
  scld__regional_parks_half_mile float,
  scld__wiki_new_xing_qtr float,
--scld_ connectivity
  scld__ex_local_miles_qtr float,
  scld__pl_local_miles_qtr float,
  scld__ex_reg_fac_half float,
  scld__pl_reg_fac_half float,
  scld_expl_reg_miles_half float,
  scld_rbtn_corridor float,
  scld_rbtn_alignment float,
  scld__dist2ex_crossing_m float,
--scld_ equity
  scld__age_under_15_pct float,
  scld__age_65_plus_pct float,
  scld__zero_car_pct float,
  scld__ppl_of_color_pct float,
  scld__wiki_women_qtr float,
  scld__wiki_pplofcolor_qtr float,
--raw factor scores
  connectivity_raw float,
  demand_raw float,
  safety_raw float,
  equity_raw float,
--scld_ factor scores
  connectivity_score float,
  demand_score float,
  safety_score float,
  equity_score float,
--priority scores & ranks
  priority_raw float,
  priority_score float,
  priority_rank int,
  county_priority_rank int,
  county varchar(30),
  city varchar(100),
  community_designation varchar (30),
  major_river int,
  barrier_type varchar(50),
  barrier_desc varchar(200),
  location_desc varchar(500),
  point_type text
);

alter table barrier_points drop column if exists buff_2;
alter table barrier_points add column buff_2 geometry(polygon,26915);
update barrier_points
set buff_2 = st_buffer(geom, 2);

create index sidx_buff_2 on barrier_points using GIST (buff_2);
analyze barrier_points (buff_2);

--create master client removals layer
drop table if exists scratch.client_removals;
create table scratch.client_removals (
  id serial primary key,
  geom geometry(multipoint,26915),
  ptstat int
);

Insert into scratch.client_removals (geom, ptstat)

select st_multi(geom), ptstat
from tooledatacodeddeletes_20170630
where tooledatacodeddeletes_20170630.ptstat <>0;

create index sidx_client_removals on scratch.client_removals using GIST (geom);
  analyze client_removals;


-- delete duplicate geoms from client_removals
alter table client_removals add column dupe int;
update client_removals
set dupe = 1
from client_removals p2
where client_removals.id <> p2.id
and client_removals.geom = p2.geom
and client_removals.ptstat <> p2.ptstat
;
delete from client_removals
where dupe = 1
and ptstat = 0;

--insert more removals from more recent client existing_crossings_client_edits
insert into client_removals (geom)
  select st_multi(geom)
  from client_point_edits_20170709
  where newpointty = 'Remove';
analyze client_removals(geom);



--create master additions layer
drop table if exists scratch.all_additions;
create table scratch.all_additions (
  id serial primary key,
  geom geometry(multipoint,26915),
  point_type varchar(50)
);


insert into all_additions (geom, point_type)
select st_multi(geom), 'metcouncil addition'
from client_study_point_additions_20170630;

insert into all_additions (geom, point_type)
select st_multi(geom), newpointty
from client_point_edits_20170709
where tdg_county is null
and newpointty != 'Remove';

create index sidx_all_additions on scratch.all_additions using GIST(geom);
  analyze scratch.all_additions(geom);

--make sure no additions were also identified as removals
alter table all_additions drop column if exists remove;
alter table all_additions add column remove int;

update all_additions
  set remove = 1
  from client_removals
  where st_dwithin(all_additions.geom,client_removals.geom,2);
delete from all_additions
  where remove = 1;
analyze all_additions(geom);



--delete new and unchanged points from client removals




-- -- remove from tooledatacodeddeletes_20170630 points marked as remove in client_point_edits_20170709
-- alter table tooledatacodeddeletes_20170630 add column remove int;
-- update tooledatacodeddeletes_20170630
-- set remove = 1
-- from client_point_edits_20170709
-- where tooledatacodeddeletes_20170630.geom = client_point_edits_20170709.geom
-- and client_point_edits_20170709.newpointty = 'Remove';
-- delete from tooledatacodeddeletes_20170630
-- where remove = 1;
--

-- -- categorize and buffer points to be removed from analysis based on MetCouncil feedback
-- with u as (select st_union(buff_2) as geom
-- from barrier_points)
-- update tooledatacodeddeletes_20170630
-- set ptstat = 2
-- from u
-- where not st_intersects(tooledatacodeddeletes_20170630.geom, u.geom)
-- and ptstat = 0
-- ;
--
-- alter table tooledatacodeddeletes_20170630 drop column if exists buff_2;
-- alter table tooledatacodeddeletes_20170630 add column buff_2 geometry (polygon, 26915);
-- update tooledatacodeddeletes_20170630
--   set buff_2 = st_buffer(geom,2)
--   where ptstat = 1;
-- create index sidx_coded_deletes on tooledatacodeddeletes_20170630 using GIST (buff_2);
-- analyze tooledatacodeddeletes_20170630 (buff_2);


--buffer/union existing crossings provided by client
drop table if exists scratch.client_existing_union;
create table scratch.client_existing_union as
(select st_union(st_buffer(geom, 152.4)) as geom
from existing_crossings_client_edits);
alter table scratch.client_existing_union add column id serial primary key;

create index sidx_client_ex_union on scratch.client_existing_union using GIST (geom);
  analyze scratch.client_existing_union(geom);



--insert geometry and attribute data from spencer's barrier points layer
with u as
(select st_union(geom) as geom
from client_removals)

INSERT into automated.barriers_prioritized_multis
(geom,point_type)

select st_multi(barrier_points.geom), barrier_points.point_type
from automated.barrier_points, u, client_existing_union
where not st_dwithin(barrier_points.geom, u.geom,2)
and not st_intersects(barrier_points.geom, client_existing_union.geom)
;

Insert into automated.barriers_prioritized_multis (geom, point_type)
select st_multi(geom),point_type
from all_additions
;



--convert from multis back to singles

--create prioritized barriers layer
drop table if exists automated.barriers_prioritized;
create table automated.barriers_prioritized (
  id SERIAL PRIMARY KEY,
  geom geometry (point,26915),
--safety/existing conditions
  bkpd_crash_500ft int,
  bw_modeshr float,
  rail_volume int,
  avg_slope float,
  wiki_probarea_qtr int,
--demand
  popden_2014 float,
  popden_2040 float,
  empden_2014 float,
  empden_2040 float,
  trans_riders_qtr float,
  schools_half int,
  colleges_half int,
  reg_parks_half int,
  wiki_new_xing_qtr int,
--connectivity
  ex_local_miles_qtr float,
  pl_local_miles_qtr float,
  ex_reg_fac_half int,
  pl_reg_fac_half int,
  expl_reg_miles_half float,
  rbtn_corridor int,
  rbtn_alignment int,
  dist2ex_crossing_m float,
--equtiy
  acp int,
  acp50 int,
  age_under_15_pct float,
  age_65_plus_pct float,
  zero_car_pct float,
  ppl_of_color_pct float,
  wiki_women_qtr int,
  wiki_pplofcolor_qtr int,

  nearest_crossing_id int,
--scld_ safety/existing conditions
  scld__bkpd_crash_500ft float,
  scld__bw_modeshr float,
  scld__rail_volume float,
  scld__avg_slope float,
  scld__wiki_probarea_qtr float,
--scld_ safety/existing conditions
  scld__popden_2014 float,
  scld__popden_2040 float,
  scld__empden_2014 float,
  scld__empden_2040 float,
  scld_trans_riders_qtr float,
  scld__schools_half float,
  scld__colleges_half float,
  scld__regional_parks_half_mile float,
  scld__wiki_new_xing_qtr float,
--scld_ connectivity
  scld__ex_local_miles_qtr float,
  scld__pl_local_miles_qtr float,
  scld__ex_reg_fac_half float,
  scld__pl_reg_fac_half float,
  scld_expl_reg_miles_half float,
  scld_rbtn_corridor float,
  scld_rbtn_alignment float,
  scld__dist2ex_crossing_m float,
--scld_ equity
  scld__age_under_15_pct float,
  scld__age_65_plus_pct float,
  scld__zero_car_pct float,
  scld__ppl_of_color_pct float,
  scld__wiki_women_qtr float,
  scld__wiki_pplofcolor_qtr float,
--raw factor scores
  connectivity_raw float,
  demand_raw float,
  safety_raw float,
  equity_raw float,
--scld_ factor scores
  connectivity_score float,
  demand_score float,
  safety_score float,
  equity_score float,
--priority scores & ranks
  priority_raw float,
  priority_score float,
  priority_rank int,
  county_priority_rank int,
  county varchar(30),
  city varchar(100),
  community_designation varchar (30),
  major_river int,
  barrier_type varchar(50),
  barrier_desc varchar(200),
  location_desc varchar(500),
  point_type text
);

Insert into barriers_prioritized(geom,point_type)
select (st_dump(geom)).geom as geom, point_type
from barriers_prioritized_multis;


create index sidx_barprigeom on automated.barriers_prioritized using GIST (geom);
analyze automated.barriers_prioritized;

drop table if exists barriers_prioritized_multis;

--insert additional points from client_point_edits_20170709
insert into barriers_prioritized (geom, point_type)
  select geom, newpointty
  from client_point_edits_20170709
  where tdg_county is null
  and newpointty != 'Remove';

analyze barriers_prioritized(geom);





--remove duplicate geometries
drop table if exists point_unique;
create table point_unique as
with unique_geoms (id,geom, point_type) as
(select row_number() over (partition by geom) as id, geom, point_type
from barriers_prioritized)
select geom, point_type
from unique_geoms
where id=1
;

delete from barriers_prioritized;

insert into barriers_prioritized(geom, point_type)
  select geom, point_type from point_unique;

drop index if exists sidx_barprigeom;
create index sidx_barprigeom on automated.barriers_prioritized using GIST (geom);
analyze automated.barriers_prioritized(geom);

drop table if exists point_unique;

--identify metcouncil added points
update barriers_prioritized
  set point_type = 'metcouncil addition'
  where point_type is null;


  alter table barriers_prioritized drop column if exists remove;
  alter table barriers_prioritized add column remove int;


-- --delete any slated for removal that were previously missed
-- with u as
-- (select st_union(geom) as geom
-- from client_point_edits_20170709
-- where newpointty = 'Remove' )
-- update barriers_prioritized
-- set remove = 1
-- from u
-- where st_dwithin(u.geom, barriers_prioritized.geom, 2);
--
-- delete from barriers_prioritized
-- where remove = 1;
--pull new exising crossings from client delivered layer
--buffer/union spencer's existing crossing pts
drop table if exists scratch.barrier_crossings_union;
create table scratch.barrier_crossings_union as
  (select st_union(st_buffer(geom,152.4)) as geom
  from barrier_crossings);
create index sidx_barrier_crossings_union on scratch.barrier_crossings_union using GIST(geom);
  analyze scratch.barrier_crossings_union(geom);


drop table if exists scratch.new_existing_crossings;
create table scratch.new_existing_crossings as (
  select existing_crossings_client_edits.geom
  from barrier_crossings_union, existing_crossings_client_edits
  where not st_intersects(barrier_crossings_union.geom, existing_crossings_client_edits.geom));
alter table new_existing_crossings add column id serial primary key;

create index sidx_new_excross on scratch.new_existing_crossings using GIST (geom);
  analyze scratch.new_existing_crossings (geom);

alter table barrier_crossings drop column if exists source;
alter table barrier_crossings add column source varchar(25);
Insert into barrier_crossings(geom,source)
select (st_dump(geom)).geom as geom, 'metcouncil addition'
from scratch.new_existing_crossings;
analyze automated.barrier_crossings;

--last check to remove any poitns identified by client
update barriers_prioritized
set remove = 1
from client_removals
where st_dwithin(barriers_prioritized.geom, client_removals.geom,2);

delete from barriers_prioritized
where remove = 1;



-- --identify points steve/dave deleted in their confirmed points layer
-- --create mc_removals_20170714 table
-- drop table if exists received.mc_removals_20170714;
-- create table received.mc_removals_20170714 (
-- 	id serial primary key,
-- 	geom geometry(point,26915));
--
-- with n as (select st_union(geom) as geom from confirmed_points_20170714)
-- insert into mc_removals_20170714 (geom)
-- select o.geom
-- from barriers_prioritized_no_maj_river o, n
-- where not st_dwithin(o.geom,n.geom,2);

--create additions table
drop table if exists received.mc_adds_20170714;
create table received.mc_adds_20170714 (
  id serial primary key,
  geom geometry(point,26915),
  point_type varchar(25)
);
 -- with old as (select st_union(geom) as geom from barriers_prioritized_no_maj_river)
 -- insert into received.mc_adds_20170714 (geom, point_type)
 --  select old.geom, 'metcouncil add 7/14'
 --  from confirmed_points_20170714 new, old
 --  where not st_dwithin(old.geom,new.geom,2);
insert into received.mc_adds_20170714 (geom, point_type)
select geom, 'metcouncil add 7/14'
from missed_points_20170714;
--
create index sidx_mc_adds_20170714 on received.mc_adds_20170714 using GIST (geom);
  analyze received.mc_adds_20170714(geom);

--delete metcouncil flagged removals from 7/14 edits
with u as (select st_union(geom) as geom from mc_removals_20170714)
update barriers_prioritized
  set remove = 1
  from u
  where st_dwithin(barriers_prioritized.geom,u.geom,2);
delete from barriers_prioritized
  where remove = 1;

--create table identifying removals from emailed TAWG feedback
create table scratch.tawg_missed_removals (
id serial primary key,
geom geometry(point,26915));

with u as (select st_union(geom) as geom from remove_crossings)

insert into scratch.tawg_missed_removals(geom)
select barrier_points.geom
from barrier_points, u
where st_intersects(u.geom, barrier_points.geom);

create index sidx_tawg_missed_removals on scratch.tawg_missed_removals using GIST (geom);
analyze tawg_missed_removals(geom);

--remove all points from TAWG emailed feedback
with u as (select st_union(geom) as geom from generated.remove_crossings)
update barriers_prioritized
  set remove = 1
  from u
  where st_intersects(u.geom, barriers_prioritized.geom);
delete from barriers_prioritized
  where remove = 1;


--add metcouncil flagged additions from 7/14 edits
insert into barriers_prioritized (geom, point_type)
  select geom, point_type
  from mc_adds_20170714;
analyze barriers_prioritized (geom);









--create buffers
alter table automated.barriers_prioritized drop column if exists buff_qtr;
alter table automated.barriers_prioritized add column buff_qtr geometry(polygon,26915);

update automated.barriers_prioritized
  set buff_qtr = st_buffer(geom, 402.336);

create index sidx_barriers_prioritized_buffqtr on automated.barriers_prioritized using GIST (buff_qtr);
analyze barriers_prioritized (buff_qtr);

alter table automated.barriers_prioritized drop column if exists buff_half;
alter table automated.barriers_prioritized add column buff_half geometry(polygon,26915);

update automated.barriers_prioritized
  set buff_half = st_buffer(geom, 804.672);

create index sidx_barriers_prioritized_buffhalf on automated.barriers_prioritized using GIST (buff_half);
analyze barriers_prioritized (buff_half);


--join in point_type field
alter table barriers_prioritized drop column if exists point_type;
alter table barriers_prioritized add column point_type text;

update barriers_prioritized
  set point_type = barrier_points.point_type
  from barrier_points
  where st_dwithin(barrier_points.geom,barriers_prioritized.geom, 0.5);


--join in county name
update automated.barriers_prioritized
  set county = counties.cty_name
  from received.counties
  where st_intersects(barriers_prioritized.geom, counties.geom);
update automated.barriers_prioritized
  set county = counties.cty_name
  from received.counties
  where st_dwithin(barriers_prioritized.geom, counties.geom, 1609.34)
  and county is null;

--join in city name
update automated.barriers_prioritized
  set city = city_township.name
  from received.city_township
  where st_intersects(barriers_prioritized.geom, city_township.geom);
update automated.barriers_prioritized
  set city = city_township.name
  from received.city_township
  where st_dwithin(barriers_prioritized.geom, city_township.geom, 1609.34)
  and city is null;


delete from barriers_prioritized
  where county in ('Chisago','Wright');



--join in community designation attribute
update automated.barriers_prioritized
  set community_designation = community_designations.tdg_community_designation
  from received.community_designations
  where st_intersects(barriers_prioritized.geom, community_designations.geom);

--flag crossings over the Mississippi and Minnesota
alter table received.streams drop column if exists buff100m;
alter table received.streams add column buff100m geometry (multipolygon,26915);
update received.streams set buff100m = st_multi(st_buffer(geom,100,'endcap=round'))
  where kittle_nam in ('Mississippi River','Minnesota River', 'St. Croix River');
create index sidx_strmbuff100m on received.streams using GIST (buff100m);
analyze received.streams (buff100m);
update automated.barriers_prioritized
  set major_river = 1
  from received.streams
  where st_intersects(barriers_prioritized.geom, streams.buff100m)
  ;

--populate barrier type
update automated.barriers_prioritized
  set barrier_type = (
    select source
    from barriers
    order by ST_Distance(barriers.geom,barriers_prioritized.geom) ASC
    limit 1
  );

--stream description
update automated.barriers_prioritized
  set barrier_desc = concat
    ((select kittle_nam
    from received.streams
    where barriers_prioritized.barrier_type = 'Streams'
    and streams.kittle_nam is not null
    and st_dwithin(barriers_prioritized.geom, streams.geom, 1609.34)
    order by st_distance(barriers_prioritized.geom, streams.geom) ASC
    limit 1),
    ' near ',
    (select streetall
    from received.roads_metro
    where barriers_prioritized.barrier_type = 'Streams'
    and st_dwithin(barriers_prioritized.geom, roads_metro.geom, 1609.34)
    order by st_distance(barriers_prioritized.geom, roads_metro.geom) ASC
    limit 1))
    from received.streams
    where barriers_prioritized.barrier_type = 'Streams'
    and streams.kittle_nam is not null
  ;
  update automated.barriers_prioritized
    set barrier_desc = concat
    ('Unnamed Stream near ',
    (select streetall
    from received.roads_metro
    where barriers_prioritized.barrier_type = 'Streams'
    and st_dwithin(barriers_prioritized.geom, roads_metro.geom, 1609.34)
    order by st_distance(barriers_prioritized.geom, roads_metro.geom) ASC
    limit 1))
    from received.streams
    where barriers_prioritized.barrier_type = 'Streams'
    and streams.kittle_nam is null
    ;


--RR crossing description
update automated.barriers_prioritized
  set barrier_desc = concat
    ((select name
    from received.osm_railroads
    where barriers_prioritized.barrier_type = 'Railroads'
    and osm_railroads.name is not null
    order by st_distance(barriers_prioritized.geom, osm_railroads.geom) ASC
    limit 1), ' near ',
    (select streetall
    from received.roads_metro
    where barriers_prioritized.barrier_type = 'Railroads'
    and st_dwithin(barriers_prioritized.geom, roads_metro.geom, 800)
    order by st_distance(barriers_prioritized.geom, roads_metro.geom) ASC
    limit 1))
    from received.osm_railroads
    where barriers_prioritized.barrier_type = 'Railroads'
    and osm_railroads.name is not null;
update automated.barriers_prioritized
  set barrier_desc = concat
    ('RR near ',
    (select streetall
    from received.roads_metro
    where barriers_prioritized.barrier_type = 'Railroads'
    and st_dwithin(barriers_prioritized.geom, roads_metro.geom, 800)
    order by st_distance(barriers_prioritized.geom, roads_metro.geom) ASC
    limit 1))
    from received.osm_railroads
    where barriers_prioritized.barrier_type = 'Railroads'
    and osm_railroads.name is null;

 --expressway crossing description
update automated.barriers_prioritized
  set barrier_desc = concat
    ((select streetall
    from received.expy
    where barriers_prioritized.barrier_type = 'Expressways'
    and st_dwithin(barriers_prioritized.geom, expy.geom, 1609.34)
    order by st_distance(barriers_prioritized.geom, expy.geom) ASC
    limit 1),
     ' near ',
    (select CASE
      when f_xstreet is null and t_xstreet is not null then t_xstreet
      when t_xstreet is null and f_xstreet is not null then f_xstreet
      when f_xstreet is not null and t_xstreet is not null then concat(f_xstreet, '/',t_xstreet)
      end
    from received.expy
    where barriers_prioritized.barrier_type = 'Expressways'
    and st_dwithin(barriers_prioritized.geom, expy.geom, 1609.34)
    order by st_distance(barriers_prioritized.geom, expy.geom) ASC
    limit 1))
    from received.expy
    where barriers_prioritized.barrier_type = 'Expressways'
    and f_xstreet is not null or t_xstreet is not null;
update automated.barriers_prioritized
  set barrier_desc = concat
    ((select streetall
    from received.expy
    where barriers_prioritized.barrier_type = 'Expressways'
    and st_dwithin(barriers_prioritized.geom, expy.geom, 1609.34)
    order by st_distance(barriers_prioritized.geom, expy.geom) ASC
    limit 1),
     ' near ',
     (select streetall
     from received.roads_metro
     where barriers_prioritized.barrier_type = 'Expressways'
     and st_dwithin(barriers_prioritized.geom, roads_metro.geom, 800)
     order by st_distance(barriers_prioritized.geom, roads_metro.geom) ASC
     limit 1))
     where barriers_prioritized.barrier_type = 'Expressways'
     and barrier_desc is null;




update automated.barriers_prioritized
  set location_desc = concat(barrier_type, '   ',
    barrier_desc, '  -  ', city, ' , ', county, ' County'
  );



-- --calculate average slope within 1/4 mile
-- WITH    clip AS (
--             SELECT  bp.id,
--                     ST_SummaryStats(ST_Clip(slope.rast,ST_Buffer(bp.geom,402.336))) AS stats
--             FROM    automated.barriers_prioritized bp,
--                     generated.slope
--             WHERE   ST_Intersects(slope.rast,ST_Buffer(bp.geom,402.336))
--         ),
--         barrier_stats AS (
--             SELECT  id,
--                     (stats).*           -- expands all the stats out into their own columns
--             FROM    clip
--         ),
--         stats_agg AS (
--             SELECT  id,
--                     SUM(mean*count)/SUM(count) AS average
--             FROM    barrier_stats
--             GROUP BY id
--         )
-- UPDATE  automated.barriers_prioritized
-- SET     avg_slope = average
-- FROM    stats_agg
-- WHERE   stats_agg.id = barriers_prioritized.id;
-- --scale avg slope to 1
-- update automated.barriers_prioritized
--   set scld__avg_slope =
--   avg_slope::float *
--   (10::float/maxscore_table.maxscore)
--   from (select max(avg_slope) as maxscore from
--   barriers_prioritized) as maxscore_table
--   where avg_slope <>0
--   and maxscore <>0;
-- update automated.barriers_prioritized
--   set scld__avg_slope = 0
--   where avg_slope = 0;
--
-- --create dissolved existing regional trails layer
-- --(dissolve by fac type only where touching)
--
--
--
--
-- -----------------------------------------------------------------------------------
--
--
-- DROP TABLE IF EXISTS scratch.existing_regional_dissolve;
-- CREATE TABLE scratch.existing_regional_dissolve (
--     id SERIAL PRIMARY KEY,
--     status varchar(50),
--     geom geometry(multilinestring,26915)
-- );
--
-- INSERT INTO scratch.existing_regional_dissolve (status, geom)
-- SELECT      status,
--             ST_CollectionExtract(
--                 ST_SetSRID(
--                     unnest(ST_ClusterIntersecting(geom)),
--                     26915
--                 ),
--                 2   --linestrings
--             )
-- FROM        trails_regional_mngov
-- WHERE       status in ('Existing')
-- GROUP BY    status;
--
-- create index sidx_ex_reg_dissolve on scratch.existing_regional_dissolve
--   using GIST (geom);
-- analyze scratch.existing_regional_dissolve (geom);
--
-- --create dissolved planned regional trails layer (dissolve by fac type only where touching)
-- DROP TABLE IF EXISTS scratch.planned_regional_dissolve;
-- CREATE TABLE scratch.planned_regional_dissolve (
--     id SERIAL PRIMARY KEY,
--     status varchar(50),
--     geom geometry(multilinestring,26915)
-- );
--
-- INSERT INTO scratch.planned_regional_dissolve (status, geom)
-- SELECT      status,
--             ST_CollectionExtract(
--                 ST_SetSRID(
--                     unnest(ST_ClusterIntersecting(geom)),
--                     26915
--                 ),
--                 2   --linestrings
--             )
-- FROM        trails_regional_mngov
-- WHERE       status in ('Alternate','Planned')
-- GROUP BY    status;
--
-- create index sidx_pl_reg_dissolve on scratch.planned_regional_dissolve using GIST (geom);
-- analyze scratch.planned_regional_dissolve (geom);
--
--
--
-- --create local trails layer by buffering regional trails and erasing them from bikeways_RBTN
-- alter table received.trails_regional_mngov drop column if exists buff200;
-- alter table received.trails_regional_mngov add column buff200 geometry(multipolygon, 26915);
-- update received.trails_regional_mngov set buff200 = st_multi(st_buffer(geom,60.96,'endcap=round'));
-- create index sidx_trlsreg_buff200 on received.trails_regional_mngov using GIST (buff200);
-- analyze received.trails_regional_mngov (buff200);
-- alter table received.rbtn_alignments drop column if exists buff200;
-- alter table received.rbtn_alignments add column buff200 geometry(multipolygon, 26915);
-- update received.rbtn_alignments set buff200 = st_multi(st_buffer(geom,60.96,'endcap=round'));
-- create index sidx_rbtn_align_buff on received.rbtn_alignments using GIST (buff200);
-- analyze received.rbtn_alignments (buff200);
--
--
-- DROP TABLE IF EXISTS scratch.rbtn_local;
-- CREATE TABLE scratch.rbtn_local (
--   id SERIAL PRIMARY KEY,
--   regstat int,
--   geom geometry (multilinestring, 26915))
--   ;
--
-- DROP table if exists scratch.all_regional;
-- CREATE table scratch.all_regional(
--   id serial primary key,
--   buff200 geometry (multipolygon,26915)
-- );
--
-- Insert into all_regional (buff200)
-- select buff200
-- from trails_regional_mngov;
--
-- Insert into all_regional (buff200)
-- select buff200
-- from rbtn_alignments;
--
-- update scratch.all_regional
--   set buff200 = st_makevaild(buff200);
--
-- With trails_union as (select st_union(buff200) as geom from all_regional)
-- INSERT INTO scratch.rbtn_local (regstat, geom)
-- select regstat::int,
-- 	st_collectionextract(st_force2d(st_multi(st_difference(bikeways_rbtn.geom, trails_union.geom))),2)
-- from received.bikeways_rbtn, trails_union
-- where exists (
--     select 1 from all_regional where st_intersects(bikeways_rbtn.geom,
--       all_regional.buff200)
-- )
-- ;
-- insert into scratch.rbtn_local (regstat,geom)
--   select regstat::int, bikeways_rbtn.geom
--   from received.bikeways_rbtn
--   where not exists (
--     select 1 from all_regional where st_intersects(bikeways_rbtn.geom,
--        all_regional.buff200));
--
-- create index sidx_rbtnlocal_idx on scratch.rbtn_local using GIST (geom);
-- analyze scratch.rbtn_local (geom);
--
--
--
-- --dissovle existing locals only where touuching
-- DROP TABLE IF EXISTS scratch.existing_local_dissolve;
-- CREATE TABLE scratch.existing_local_dissolve (
--     id SERIAL PRIMARY KEY,
--     regstat varchar(50),
--     geom geometry(multilinestring,26915)
-- );
--
--
--
-- INSERT INTO scratch.existing_local_dissolve (regstat, geom)
-- SELECT      regstat,
--             ST_CollectionExtract(
--                 ST_SetSRID(
--                     unnest(ST_ClusterIntersecting(geom)),
--                     26915
--                 ),
--                 2   --linestrings
--             )
-- FROM       scratch.rbtn_local
-- WHERE       regstat = 1
-- GROUP BY    regstat;
--
-- create index ex_loc_dissolve_idx on scratch.existing_local_dissolve using GIST (geom);
-- analyze scratch.existing_local_dissolve (geom);
--
--
-- --dissovle planned locals only where touuching
-- DROP TABLE IF EXISTS scratch.planned_local_dissolve;
-- CREATE TABLE scratch.planned_local_dissolve (
--     id SERIAL PRIMARY KEY,
--     regstat varchar(50),
--     geom geometry(multilinestring,26915)
-- );
--
-- INSERT INTO scratch.planned_local_dissolve (regstat, geom)
-- SELECT      regstat,
--             ST_CollectionExtract(
--                 ST_SetSRID(
--                     unnest(ST_ClusterIntersecting(geom)),
--                     26915
--                 ),
--                 2   --linestrings
--             )
-- FROM       rbtn_local
-- WHERE       regstat = 2
-- GROUP BY    regstat;
--
-- create index pl_loc_dissolve_idx on scratch.planned_local_dissolve using GIST (geom);
-- analyze scratch.planned_local_dissolve (geom);
--
alter table barrier_points drop column if exists buff_qtr;
alter table barrier_points add column buff_qtr geometry(multipolygon, 26915);

update barrier_points
  set buff_qtr = st_multi(st_buffer(geom, 402.336));

create index sidx_barrier_pt_buff_qtr on automated.barrier_points using GIST (buff_qtr);
analyze barrier_points (buff_qtr);



--intersect rbtn with 1/4 buff around points
DROP table if exists scratch.rbtn_qtr_mile_intersect;
Create table scratch.rbtn_qtr_mile_intersect (
  id serial primary key,
  geom geometry(multilinestring, 26915),
  regstat int,
  barrier_id int);

Insert into scratch.rbtn_qtr_mile_intersect
(geom,
regstat,
barrier_id)

select st_multi(st_collectionextract(
	st_setsrid(
	st_intersection(
		bikeways_rbtn.geom, barriers_prioritized.buff_qtr
		),
		26915),
		2 )),
regstat,
barriers_prioritized.id
from received.bikeways_rbtn, automated.barriers_prioritized
where st_intersects(bikeways_rbtn.geom, barriers_prioritized.buff_qtr)
or st_within(bikeways_rbtn.geom, barriers_prioritized.buff_qtr);

create index sidx_rbtn_qtr_intersect on scratch.rbtn_qtr_mile_intersect using GIST (geom);
  analyze scratch.rbtn_qtr_mile_intersect (geom);


  --intersect regional trails with 1/2 mile buffer
DROP table if exists scratch.trails_reg_half_int;
create table scratch.trails_reg_half_int(
  id serial primary key,
  geom geometry(multilinestring, 26915),
  barrier_id int,
  lgth_miles float);
Insert into scratch.trails_reg_half_int (geom,
  barrier_id)

select st_multi(st_collectionextract(
        st_setsrid(
            st_intersection(
              trails_regional_mngov.geom, barriers_prioritized.buff_half
            ),
        26915),
        2)),
        barriers_prioritized.id
from received.trails_regional_mngov, automated.barriers_prioritized
;

create index sidx_trails_reg_half_int on scratch.trails_reg_half_int using GIST (geom);
  analyze scratch.trails_reg_half_int (geom);

update scratch.trails_reg_half_int
  set lgth_miles = (st_length(geom)/1609.34);
delete from scratch.trails_reg_half_int
  where lgth_miles is null or lgth_miles = 0;








-- -- spencer's stuff
-- UPDATE  barriers_prioritized
-- SET     name_of_column = (
--           SELECT  SUM(ST_length(ST_Intersection(trails.geom,barriers_prioritized.buff_qtr)))
--           FROM    trails
--           WHERE   ST_Intersects(trails.geom,barriers_prioritized.buff_qtr)
--         )
--         +
--         (
--           SELECT  SUM(ST_length(ST_Intersection(trails.geom,barriers_prioritized.buff_qtr)))
--           FROM    trails
--           WHERE   ST_Intersects(trails.geom,barriers_prioritized.buff_qtr)
--         )
--         ;









--create local trails layer by buffering regional trails and erasing them from bikeways_RBTN
alter table received.trails_regional_mngov drop column if exists buff200;
alter table received.trails_regional_mngov add column buff200 geometry(multipolygon, 26915);
update received.trails_regional_mngov set buff200 = st_multi(st_buffer(geom,60.96,'endcap=round'));
create index sidx_trlsreg_buff200 on received.trails_regional_mngov using GIST (buff200);
analyze received.trails_regional_mngov (buff200);
alter table received.rbtn_alignments drop column if exists buff200;
alter table received.rbtn_alignments add column buff200 geometry(multipolygon, 26915);
update received.rbtn_alignments set buff200 = st_multi(st_buffer(geom,60.96,'endcap=round'));
create index sidx_rbtn_align_buff on received.rbtn_alignments using GIST (buff200);
analyze received.rbtn_alignments (buff200);


DROP TABLE IF EXISTS scratch.rbtn_local;
CREATE TABLE scratch.rbtn_local (
  id SERIAL PRIMARY KEY,
  regstat int,
  geom geometry (multilinestring, 26915),
  lgth_miles float,
  barrier_id int)
  ;

--
With trails_union as (select st_union(buff200) as geom from all_regional)
 INSERT INTO scratch.rbtn_local (regstat, geom, barrier_id)
 select regstat::int,
 	CASE
    WHEN exists (
        select 1 from trails_union where st_intersects(rbtn_qtr_mile_intersect.geom,
          trails_union.geom)) then
    st_collectionextract(st_force2d(st_multi(st_difference(rbtn_qtr_mile_intersect.geom, trails_union.geom))),2)
    ELSE rbtn_qtr_mile_intersect.geom
    END,
  rbtn_qtr_mile_intersect.barrier_id
 from scratch.rbtn_qtr_mile_intersect, trails_union
 ;
 -- With trails_union as (select st_union(buff200) as geom from all_regional)
 --  INSERT INTO scratch.rbtn_local (regstat, geom, barrier_id)
 --  select regstat::int,
 --  	st_multi(rbtn_qtr_mile_intersect.geom),
 --   rbtn_qtr_mile_intersect.barrier_id
 --  from scratch.rbtn_qtr_mile_intersect, trails_union
 --  where not st_intersects(rbtn_qtr_mile_intersect.geom, trails_union.geom);

update scratch.rbtn_local
 set lgth_miles = (st_length(geom)/1609.34);
delete from scratch.rbtn_local
 where lgth_miles is null or lgth_miles = 0;

 create index sidx_rbtnlocal_idx on scratch.rbtn_local using GIST (geom);
 analyze scratch.rbtn_local (geom);

-- drop table if exists scratch.rbtn_local_no_dupes;
-- create table scratch.rbtn_local_no_dupes (
-- id serial primary key,
-- geom geometry(multilinestring, 26915),
-- regstat int,
-- barrier_id int,
-- lgth_miles float);
--
-- insert into rbtn_local_no_dupes (geom,
-- regstat, barrier_id)
--
-- select st_multi(st_union(rbtn_local.geom)),
-- regstat,
-- barrier_id
-- from scratch.rbtn_local
-- group by regstat, barrier_id;
--
-- create index sidx_rbtn_local_no_dupes on scratch.rbtn_local_no_dupes using GIST (geom);
-- analyze rbtn_local_no_dupes (geom);
--
-- update scratch.rbtn_local_no_dupes
-- set lgth_miles = (st_length(geom)/1609.344);

-- --create local trails layer by buffering regional trails and erasing them from bikeways_RBTN
-- alter table received.trails_regional_mngov drop column if exists buff200;
-- alter table received.trails_regional_mngov add column buff200 geometry(multipolygon, 26915);
-- update received.trails_regional_mngov set buff200 = st_multi(st_buffer(geom,60.96,'endcap=round'));
-- create index sidx_trlsreg_buff200 on received.trails_regional_mngov using GIST (buff200);
-- analyze received.trails_regional_mngov (buff200);
-- alter table received.rbtn_alignments drop column if exists buff200;
-- alter table received.rbtn_alignments add column buff200 geometry(multipolygon, 26915);
-- update received.rbtn_alignments set buff200 = st_multi(st_buffer(geom,60.96,'endcap=round'));
-- create index sidx_rbtn_align_buff on received.rbtn_alignments using GIST (buff200);
-- analyze received.rbtn_alignments (buff200);


-- DROP TABLE IF EXISTS scratch.rbtn_local;
-- CREATE TABLE scratch.rbtn_local (
--   id SERIAL PRIMARY KEY,
--   regstat int,
--   geom geometry (multilinestring, 26915),
--   lgth_miles float)
--   ;
--
--
-- With trails_union as (select st_union(buff200) as geom from all_regional)
--  INSERT INTO scratch.rbtn_local (regstat, geom, lgth_miles)
--  select regstat::int,
--  	st_collectionextract(st_force2d(st_multi(st_difference(rbtn_qtr_mile_intersect.geom, trails_union.geom))),2),
--   (st_length(rbtn_local.geom)/5280)
--  from scratch.rbtn_qtr_mile_intersect, trails_union
--  where exists (
--      select 1 from all_regional where st_intersects(rbtn_qtr_mile_intersect.geom,
--        all_regional.buff200)
--  )
--  ;
--  insert into scratch.rbtn_local (regstat,geom)
--    select regstat::int, rbtn_qtr_mile_intersect.geom
--    from scratch.rbtn_qtr_mile_intersect
--    where not exists (
--      select 1 from all_regional where st_intersects(bikeways_rbtn.geom,
--         all_regional.buff200));
--
--  create index sidx_rbtnlocal_idx on scratch.rbtn_local using GIST (geom);
--  analyze scratch.rbtn_local (geom);




--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--RUN FROM HERE FOR QUICKER CALCULATING
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------


--calculate number of bike/ped crashes within 1/4 mile
update automated.barriers_prioritized
  set bkpd_crash_500ft = (select count(crash_non_motor_2010_2015.id)
  from received.crash_non_motor_2010_2015
  where st_dwithin(barriers_prioritized.geom, crash_non_motor_2010_2015.geom, 152.4)
);
update automated.barriers_prioritized
  set bkpd_crash_500ft = 0
  where bkpd_crash_500ft is null
  ;
--scale crashes proportionall to 10
update automated.barriers_prioritized
  set scld__bkpd_crash_500ft =
  bkpd_crash_500ft *
  (10::float/maxscore_table.maxscore)
  from (select max(bkpd_crash_500ft) as maxscore
  from barriers_prioritized) as maxscore_table
  where maxscore <>0;


-- update automated.barriers_prioritized
--   set scld__bkpd_crash_500ft = CASE
--   when bkpd_crash_500ft < quarts.quarts[1] then 2.5
--   when bkpd_crash_500ft <quarts.quarts[2] then 5
--   when bkpd_crash_500ft < quarts.quarts[3] then 7.5
--   else 10
--   end
-- from (select quantile(bkpd_crash_500ft, array[0.25,0.5,0.75,1]) as
--   quarts from barriers_prioritized
--   where bkpd_crash_500ft >0) as quarts
--   where bkpd_crash_500ft >0;
update automated.barriers_prioritized
  set scld__bkpd_crash_500ft = 0
  where bkpd_crash_500ft = 0;





--bike/walk modeshare factor calc from demographics layer
update automated.barriers_prioritized
  set bw_modeshr =
  (select avg(demographics.bike_walk_modeshare)
  from generated.demographics
  where ST_DWithin(barriers_prioritized.geom, demographics.geom,804.672));
--scale bike/walk modeshare proportionally to 1
update automated.barriers_prioritized
  set scld__bw_modeshr =
  bw_modeshr *
  (10::float/maxscore_table.maxscore)
  from (select max(bw_modeshr) as maxscore
  from barriers_prioritized) as maxscore_table
  where maxscore <>0;


--calculate rail volume within 400ft (should only join on rail crossing points)
update automated.barriers_prioritized
  set rail_volume = rail_volumes.volume
  from received.rail_volumes
  where st_Dwithin(barriers_prioritized.geom, rail_volumes.geom, 121.92 )
  and rail_volumes.volume is not null
  and rail_volumes.volume <>0
  and rail_volumes.lvolu = 'D';
update automated.barriers_prioritized
  set rail_volume = 0
  where rail_volume is null;
--scale rail volume proportionately to 10
update automated.barriers_prioritized
  set scld__rail_volume =
  rail_volume *
  (10::float/maxscore_table.maxscore)
  from (select max(rail_volume) as maxscore
  from barriers_prioritized) as maxscore_table
  where maxscore <>0;
--scale rail volumes by quartile
-- update automated.barriers_prioritized
--   set scld__rail_volume = CASE
--   when rail_volume < quarts.quarts[1] then 2.5
--   when rail_volume <quarts.quarts[2] then 5
--   when rail_volume < quarts.quarts[3] then 7.5
--   else 10
--   end
-- from (select quantile(rail_volume, array[0.25,0.5,0.75,1]) as
--   quarts from barriers_prioritized
--   where rail_volume > 0) as quarts
--   where rail_volume > 0;
update automated.barriers_prioritized
  set scld__rail_volume = 0
  where rail_volume is null or
  rail_volume = 0;

--calculate wiki problem areas within 1/4 mile
update automated.barriers_prioritized
  set wiki_probarea_qtr = (select count(wiki_comments.id)
  from received.wiki_comments
  where kml_name = 'improvement needed'
    and st_dwithin(barriers_prioritized.geom, wiki_comments.geom, 402.336)
);
update automated.barriers_prioritized
  set wiki_probarea_qtr = 0
  where wiki_probarea_qtr is null
  ;
--scale wiki problem areas to 1
update automated.barriers_prioritized
  set scld__wiki_probarea_qtr =
  wiki_probarea_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_probarea_qtr) as maxscore from
  barriers_prioritized) as maxscore_table
  where wiki_probarea_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__wiki_probarea_qtr = 0
  Where wiki_probarea_qtr =0;



--2014 pop density calc
update automated.barriers_prioritized
  set popden_2014 =
  (select avg(taz.popden2014)
  from received.taz
  where ST_Dwithin(barriers_prioritized.geom,taz.geom,804.672));
--scale current pop density proportionally to 1
update automated.barriers_prioritized
  set scld__popden_2014 =
  popden_2014 *
  (10::float/maxscore_table.maxscore)
  from (select max(popden_2014) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;

--2040 pop density calc
update automated.barriers_prioritized
  set popden_2040 =
  (Select avg(taz.popden2040)
  from received.taz
  where st_dwithin(barriers_prioritized.geom,taz.geom,804.672));

--scale 2040 pop density proportionally to 1
update automated.barriers_prioritized
  set scld__popden_2040 =
  popden_2040 *
  (10::float/maxscore_table.maxscore)
  from (select max(popden_2040) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;


--2014 employment density calc
update automated.barriers_prioritized
  set empden_2014 = (
  select avg(  taz.empden2014)
  from received.taz
  where st_dwithin(barriers_prioritized.geom,taz.geom,804.672));

--scale current employment density proportionally to 1
update automated.barriers_prioritized
  set scld__empden_2014 =
  empden_2014 *
  (10::float/maxscore_table.maxscore)
  from (select max(empden_2014) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;


--2040 employment density calc
update automated.barriers_prioritized
  set empden_2040 = (
    select avg(taz.empden2040)
  from received.taz
  where st_dwithin(barriers_prioritized.geom,taz.geom,804.672));
--scale 2040 employment density proportionally to 1
update automated.barriers_prioritized
  set scld__empden_2040 =
  empden_2040 *
  (10::float/maxscore_table.maxscore)
  from (select max(empden_2040) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;



--assess whether barrier is in an area of concentrated poverty
update automated.barriers_prioritized
  set acp = CASE when
    st_intersects(barriers_prioritized.geom, acp.geom) then 10
    Else 0
    end
  from received.acp
  ;

--assess whether barrier is in an ACP 50 area
--(concentrated poverty with 50%+ ppl of color)
update automated.barriers_prioritized
  set acp50 = CASE
  WHEN st_intersects(barriers_prioritized.geom, acp50.geom) then 10
  ELSE 0
  END
  from received.acp50
;


--calculate age 65+ pct
update automated.barriers_prioritized
  set age_65_plus_pct = (
    select avg(demographics.seniors_per)
  from generated.demographics
  where st_dwithin(barriers_prioritized.geom, demographics.geom,804.672));
--scale age 65+ to 1
update automated.barriers_prioritized
  set scld__age_65_plus_pct =
  age_65_plus_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(age_65_plus_pct) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;


--age under 15 %
update automated.barriers_prioritized
  set age_under_15_pct = (
    select avg(demographics.pop_under_15_per)
  from generated.demographics
  where st_dwithin(barriers_prioritized.geom, demographics.geom,804.672));
--scale age under 15 % to 1
update automated.barriers_prioritized
  set scld__age_under_15_pct =
  age_under_15_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(age_under_15_pct) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;



--zero car household percent
update automated.barriers_prioritized
  set zero_car_pct = (
    select avg(demographics.zero_car_housing_units_per)
  from generated.demographics
  where st_dwithin(barriers_prioritized.geom, demographics.geom,804.672));
--scale zero car household % to 1
update automated.barriers_prioritized
  set scld__zero_car_pct =
  zero_car_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(zero_car_pct) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;


--calculate percent people of color
update automated.barriers_prioritized
  set ppl_of_color_pct = (
    select avg(demographics.ppl_of_color_percent)
  from generated.demographics
  where st_dwithin(barriers_prioritized.geom, demographics.geom,804.672));
--scale percent peaople of color to 1
update automated.barriers_prioritized
  set scld__ppl_of_color_pct =
  ppl_of_color_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(ppl_of_color_pct) as maxscore
  from barriers_prioritized) as maxscore_table
  where maxscore <>0;

--calculate wiki comments from women within 1/4 mile
update automated.barriers_prioritized
  set wiki_women_qtr =
  (select count (wiki_comments.id)
  from received.wiki_comments
  where st_dwithin(barriers_prioritized.geom, wiki_comments.geom, 402.336)
  and gender = 'Female'
  );
update automated.barriers_prioritized
  set wiki_women_qtr = 0
  where wiki_women_qtr is null
  ;
--scale wiki comments from women to 0-1
update automated.barriers_prioritized
  set scld__wiki_women_qtr =
  wiki_women_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_women_qtr) as maxscore from
  barriers_prioritized) as maxscore_table
  WHERE wiki_women_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__wiki_women_qtr = 0
  WHERE wiki_women_qtr = 0;

--calculate wiki comments from people of color within 1/4 mile
update automated.barriers_prioritized
  set wiki_pplofcolor_qtr =
  (select count (wiki_comments.id)
  from received.wiki_comments
  where st_dwithin(barriers_prioritized.geom, wiki_comments.geom, 402.336)
  and (race_black is not null or race_native_american is not null or
  race_asia is not null or race_hawaiian is not null or
  race_hispanic is not null)
  );
update automated.barriers_prioritized
  set wiki_pplofcolor_qtr = 0
  where wiki_pplofcolor_qtr is null
  ;
--scale wiki comments from people of color within 1/4 mile to 0-1
update automated.barriers_prioritized
  set scld__wiki_pplofcolor_qtr =
  wiki_pplofcolor_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_pplofcolor_qtr) as maxscore from
  barriers_prioritized) as maxscore_table
  WHERE wiki_pplofcolor_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__wiki_pplofcolor_qtr = 0
  WHERE wiki_pplofcolor_qtr = 0;





--calculate schools within half mile of the barrier
update automated.barriers_prioritized
  set schools_half = (select count(public_school.id)
  from received.public_school
  where sch_type1 not like 'COLLEGE%'
    and st_dwithin(barriers_prioritized.geom, public_school.geom, 804.672)
);
update automated.barriers_prioritized
  set schools_half = 0
  where schools_half is null
  ;
--scale schools to 1
update automated.barriers_prioritized
  set scld__schools_half =
  schools_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(schools_half) as maxscore from
  barriers_prioritized) as maxscore_table
  where schools_half <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__schools_half = 0
  Where schools_half =0;



--calculate college/universities within half mile of the barrier
update automated.barriers_prioritized
  set colleges_half =
  (select count(public_school.id)
  from received.public_school
  where sch_type1 like 'COLLEGE%'
  and st_dwithin(barriers_prioritized.geom, public_school.geom, 804.672)
  );
update automated.barriers_prioritized
  set colleges_half = 0
  where colleges_half is null
  ;
--scale colleges to 0-1
update automated.barriers_prioritized
  set scld__colleges_half =
  colleges_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(colleges_half) as maxscore from
  barriers_prioritized) as maxscore_table
  WHERE colleges_half <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__colleges_half = 0
  WHERE colleges_half = 0;



--calcalte number of regional parks within a half mile
update automated.barriers_prioritized
  set reg_parks_half =
  (select count (regional_park.id)
  from received.regional_park
  where st_dwithin(barriers_prioritized.geom, regional_park.geom, 804.672)
  );
update automated.barriers_prioritized
  set reg_parks_half = 0
  where reg_parks_half is null
  ;
--scale regional parks to 0-1
update automated.barriers_prioritized
  set scld__regional_parks_half_mile =
  reg_parks_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(reg_parks_half) as maxscore from
  barriers_prioritized) as maxscore_table
  WHERE reg_parks_half <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__regional_parks_half_mile = 0
  WHERE reg_parks_half = 0;


--calcalte number of wiki suggested crossings within 1/4 mile
update automated.barriers_prioritized
  set wiki_new_xing_qtr =
  (select count (wiki_comments.id)
  from received.wiki_comments
  where st_dwithin(barriers_prioritized.geom, wiki_comments.geom, 402.336)
  and kml_name = 'suggested new crossing'
  );
update automated.barriers_prioritized
  set wiki_new_xing_qtr = 0
  where wiki_new_xing_qtr is null
  ;
--scale wiki suggest crossing qtr mile to 0-1
update automated.barriers_prioritized
  set scld__wiki_new_xing_qtr =
  wiki_new_xing_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_new_xing_qtr) as maxscore from
  barriers_prioritized) as maxscore_table
  WHERE wiki_new_xing_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__wiki_new_xing_qtr = 0
  WHERE wiki_new_xing_qtr = 0;

--avg trasit ridership within a quartermile
update automated.barriers_prioritized
  set trans_riders_qtr =
  (select sum(transit_ridership.on_off)
  from received.transit_ridership
  where st_dwithin(barriers_prioritized.geom, transit_ridership.geom,804.672));
update automated.barriers_prioritized
  set trans_riders_qtr = 0
  where trans_riders_qtr is null;
--scale avg transit ridership proportionately to 10
update automated.barriers_prioritized
  set scld_trans_riders_qtr =
  trans_riders_qtr *
  (10::float/maxscore_table.maxscore)
  from (select max(trans_riders_qtr) as maxscore
  from barriers_prioritized) as maxscore_table
  where maxscore <>0;
--scale transit ridership into quartiles
-- update automated.barriers_prioritized
--   set scld_trans_riders_qtr = CASE
--   when trans_riders_qtr < quarts.quarts[1] then 2.5
--   when trans_riders_qtr <quarts.quarts[2] then 5
--   when trans_riders_qtr < quarts.quarts[3] then 7.5
--   else 10
--   end
-- from (select quantile(trans_riders_qtr, array[0.25,0.5,0.75,1]) as
--   quarts from barriers_prioritized
--   where trans_riders_qtr > 0) as quarts
--   where trans_riders_qtr > 0 ;
update automated.barriers_prioritized
  set scld_trans_riders_qtr = 0
  where trans_riders_qtr = 0;




--calculate number of exsiting regional trails within a half mile of the barrier
update automated.barriers_prioritized
  set ex_reg_fac_half =
  (select count(existing_regional_dissolve.id) as ex_reg
  from scratch.existing_regional_dissolve
  where st_dwithin(barriers_prioritized.geom, existing_regional_dissolve.geom, 804.672)
  group by barriers_prioritized.id);
update automated.barriers_prioritized
  set ex_reg_fac_half = 0
  where ex_reg_fac_half is null
  ;
--scale existing regional trails to 1
update automated.barriers_prioritized
  set scld__ex_reg_fac_half =
  ex_reg_fac_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(ex_reg_fac_half) as maxscore from
  barriers_prioritized) as maxscore_table
  where ex_reg_fac_half <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__ex_reg_fac_half = 0
  where ex_reg_fac_half = 0;



--calculate number of planned regional trails within a half mile of the barrier
update automated.barriers_prioritized
  set pl_reg_fac_half =
  (select count(planned_regional_dissolve.id) as pl_reg
  from scratch.planned_regional_dissolve
  where st_dwithin(barriers_prioritized.geom, planned_regional_dissolve.geom, 804.672)
  group by barriers_prioritized.id);
update automated.barriers_prioritized
  set pl_reg_fac_half = 0
  where pl_reg_fac_half is null;
--scale planned regional trails to 1
update automated.barriers_prioritized
  set scld__pl_reg_fac_half =
  pl_reg_fac_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(pl_reg_fac_half) as maxscore from
  barriers_prioritized) as maxscore_table
  where pl_reg_fac_half <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__pl_reg_fac_half = 0
  where pl_reg_fac_half = 0;

--calculate number of regional trails (existing or planned) within a half mile of the barriers_prioritized
update automated.barriers_prioritized
  set expl_reg_miles_half =
  (select sum(trails_reg_half_int.lgth_miles) as expl_reg_miles_half
  from trails_reg_half_int
  where trails_reg_half_int.barrier_id = barriers_prioritized.id
  group by barriers_prioritized.id);
update automated.barriers_prioritized
  set expl_reg_miles_half = 0
  where expl_reg_miles_half is null;
--scale expl_reg_miles_half to 10
update automated.barriers_prioritized
  set scld_expl_reg_miles_half =
  expl_reg_miles_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(expl_reg_miles_half) as maxscore from
  barriers_prioritized) as maxscore_table
  where expl_reg_miles_half <>0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld_expl_reg_miles_half = 0
  where expl_reg_miles_half = 0;



-- --populate rbtn_cor_score
-- update automated.barriers_prioritized
--   set rbtn_corridor = 1
--   from received.rbtn_corridor_centerlines
--   where st_dwithin(barriers_prioritized.geom, rbtn_corridor_centerlines.geom, 804.672)
--   and rbtn_corridor_centerlines.tier = '1';
-- update automated.barriers_prioritized
--   set rbtn_corridor = 2
--   from received.rbtn_corridor_centerlines
--   where st_dwithin(barriers_prioritized.geom, rbtn_corridor_centerlines.geom, 804.672)
--   and rbtn_corridor_centerlines.tier = '2'
--   and barriers_prioritized.rbtn_corridor is null;
-- update automated.barriers_prioritized
--   set rbtn_corridor = '0'
--   where rbtn_corridor is null;
-- --scale rbtn tier
-- update automated.barriers_prioritized
--   set scld_rbtn_corridor = 10
--   where rbtn_corridor = 1;
-- update automated.barriers_prioritized
--   set scld_rbtn_corridor = 5
--   where rbtn_corridor = 2;
--   update automated.barriers_prioritized
--     set scld_rbtn_corridor = 0
--     where rbtn_corridor = 0;

--populate rbtn_corridor
update automated.barriers_prioritized
  set rbtn_corridor = 10
  from received.rbtn_corridor_buff
  where st_intersects(barriers_prioritized.geom, rbtn_corridor_buff.geom);
update automated.barriers_prioritized
  set rbtn_corridor = 0
  where rbtn_corridor is null;
--scale rbtn_corridor to 10
update automated.barriers_prioritized
  set scld_rbtn_corridor = rbtn_corridor;


--calc whether crossing is within 1/4 mile of rbtn_alignment
update automated.barriers_prioritized
  set rbtn_alignment = 10
  from rbtn_alignments
  where st_dwithin(barriers_prioritized.geom, rbtn_alignments.geom, 402.336);
update automated.barriers_prioritized
  set rbtn_alignment = 0
  where rbtn_alignment is null;
--scale rbtn_alignment to 10
update automated.barriers_prioritized
  set scld_rbtn_alignment = rbtn_alignment;


--calculate number of exsiting local trails within a half mile of the barrier
update automated.barriers_prioritized
  set ex_local_miles_qtr=
  (select count(existing_local_dissolve.id) as ex_loc
  from scratch.existing_local_dissolve
  where st_dwithin(barriers_prioritized.geom, existing_local_dissolve.geom, 402.336)
  group by barriers_prioritized.id);
update automated.barriers_prioritized
  set ex_local_miles_qtr= 0
  where ex_local_miles_qtr is null;

--calculate mileaeg of existing local bikewyas within 1/4 mile
update automated.barriers_prioritized
  set ex_local_miles_qtr = (
    select rbtn_local_no_dupes.lgth_miles
    from scratch.rbtn_local_no_dupes
    where barriers_prioritized.id = rbtn_local_no_dupes.barrier_id
    and rbtn_local_no_dupes.regstat in (1)
  );
update automated.barriers_prioritized
  set ex_local_miles_qtr = 0
  where ex_local_miles_qtr is null;

-- --calculate mileaeg of existing local bikewyas within 1/4 mile
-- update automated.barriers_prioritized
--   set ex_local_miles_qtr = (
--     select (sum(st_length(st_intersection(rbtn_local.geom, barriers_prioritized.buff_qtr)))/1609.34)
--     from rbtn_local
--     where st_intersects(rbtn_local.buff_qtr, barriers_prioritized.geom)
--     and rbtn_local.regstat in (1)
--   );


--scale existing local trails calc to 0-1
update automated.barriers_prioritized
  set scld__ex_local_miles_qtr =
  ex_local_miles_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(ex_local_miles_qtr) as maxscore from
  barriers_prioritized) as maxscore_table
  where ex_local_miles_qtr <> 0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__ex_local_miles_qtr = 0
  where ex_local_miles_qtr = 0;





--create nearest crossing TABLE
drop table if exists scratch.nearest_crossing;
create table scratch.nearest_crossing (
  tdgid serial primary key,
  barrier_id int,
  crossing_id int
);

Insert into scratch.nearest_crossing (barrier_id, crossing_id)
select barrier.id,
(select crossing.id
from barrier_crossings as crossing
order by barrier.geom <#> crossing.geom limit 1)
from barriers_prioritized as barrier;

--populate distance to nearest crossing
update automated.barriers_prioritized
  set nearest_crossing_id = nearest_crossing.crossing_id
  from scratch.nearest_crossing
  where barriers_prioritized.id = nearest_crossing.barrier_id;
--calculate distance to nearest existing crossing
update automated.barriers_prioritized
  set dist2ex_crossing_m =
  (select st_distance(barriers_prioritized.geom, barrier_crossings.geom))
  from automated.barrier_crossings
  where barriers_prioritized.nearest_crossing_id = barrier_crossings.id;
--scale dist to existing crossing proportionally to 1
update automated.barriers_prioritized
  set scld__dist2ex_crossing_m =
  dist2ex_crossing_m::float *
  (10::float/maxscore_table.maxscore)
  from (select max(dist2ex_crossing_m) as maxscore from
  barriers_prioritized) as maxscore_table;


--calculate number of planned local trails within a half mile of the barrier
update automated.barriers_prioritized
  set pl_local_miles_qtr= rbtn_local_no_dupes.lgth_miles
  from scratch.rbtn_local_no_dupes
  where rbtn_local_no_dupes.barrier_id = barriers_prioritized.id
  and rbtn_local_no_dupes.regstat in (2,3)
  ;
update automated.barriers_prioritized
  set pl_local_miles_qtr= 0
  where pl_local_miles_qtr is null
  ;
--scale planned local trails calc to 0-1
update automated.barriers_prioritized
  set scld__pl_local_miles_qtr =
  pl_local_miles_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(pl_local_miles_qtr) as maxscore from
  barriers_prioritized) as maxscore_table
  where pl_local_miles_qtr <> 0
  and maxscore <>0;
update automated.barriers_prioritized
  set scld__pl_local_miles_qtr = 0
  where pl_local_miles_qtr = 0;



--calculate raw connectivity score
update automated.barriers_prioritized
  set connectivity_raw =(
    scld__pl_local_miles_qtr+scld__ex_local_miles_qtr+
    --scld__pl_reg_fac_half + scld__ex_reg_fac_half+
    scld_expl_reg_miles_half +
    scld_rbtn_corridor+ scld_rbtn_alignment +
    coalesce(scld__dist2ex_crossing_m,0))/6;
--scale raw connectivity
update automated.barriers_prioritized
  set connectivity_score =
  connectivity_raw::float * 4.825;


--calculate raw demand
update automated.barriers_prioritized
  set demand_raw =
  (scld__popden_2040 +
  scld__empden_2040 +
  scld_trans_riders_qtr + scld__schools_half +
  scld__colleges_half + scld__regional_parks_half_mile +
  scld__wiki_new_xing_qtr)/7;
--scale demand score
update automated.barriers_prioritized
  set demand_score =
  demand_raw::float *2.425;




--calculate raw safety/existing conditions
update automated.barriers_prioritized
  set safety_raw = (scld__bkpd_crash_500ft +
  scld__bw_modeshr +
  scld__popden_2014 +
  scld__empden_2014 +
  --+ scld__avg_slope
  --+scld__rail_volume
  + scld__wiki_probarea_qtr::float)/5;
--scale safety/exsiting conditions score
update automated.barriers_prioritized
  set safety_score =
  safety_raw::float *1.525;



--calculate raw equity
update automated.barriers_prioritized
  set equity_raw = (acp + acp50 +
  scld__age_under_15_pct + scld__age_65_plus_pct +
  scld__zero_car_pct + scld__ppl_of_color_pct +
  scld__wiki_women_qtr +
  scld__wiki_pplofcolor_qtr)/8;
--scale equity to 100
update automated.barriers_prioritized
  set equity_score =
  equity_raw::float *1.225;


--calculate raw priority score
update automated.barriers_prioritized
  set priority_raw =
  connectivity_score + demand_score +
  safety_score + equity_score;
--scalculate scld_ priority score
update automated.barriers_prioritized
  set priority_score =
  priority_raw::float *
  (100/maxscore_table.maxscore)
  from (select max(priority_raw) as maxscore from
  barriers_prioritized) as maxscore_table
  where maxscore <>0;

--rank projects regionally
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
)
UPDATE automated.barriers_prioritized
set priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
;

--rank projects in individual counties
--Anoka
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Anoka'
)
UPDATE automated.barriers_prioritized
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
and county = 'Anoka'
;
--Carver
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Carver'
)
UPDATE automated.barriers_prioritized
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
and county = 'Carver'
;
--Dakota
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Dakota'
)
UPDATE automated.barriers_prioritized
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
and county = 'Dakota'
;
--Hennepin
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Hennepin'
)
UPDATE automated.barriers_prioritized
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
and county = 'Hennepin'
;
--Ramsey
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Ramsey'
)
UPDATE automated.barriers_prioritized
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
and county = 'Ramsey'
;
--Scott
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Scott'
)
UPDATE automated.barriers_prioritized
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
and county = 'Scott'
;
--Washington
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Washington'
)
UPDATE automated.barriers_prioritized
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized.id = ranks.id
and county = 'Washington'
;






--create separate barrier table without major river crossings
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
--------------------------------------------------------------------------------
------------------no major rivers-----------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------
-------------------------------------------------------------------------------


drop table if exists automated.barriers_prioritized_no_maj_river;
create table automated.barriers_prioritized_no_maj_river (
  id SERIAL PRIMARY KEY,
  geom geometry (point,26915),
--safety/existing conditions
  bkpd_crash_500ft int,
  bw_modeshr float,
  rail_volume int,
  avg_slope float,
  wiki_probarea_qtr int,
--demand
  popden_2014 float,
  popden_2040 float,
  empden_2014 float,
  empden_2040 float,
  trans_riders_qtr float,
  schools_half int,
  colleges_half int,
  reg_parks_half int,
  wiki_new_xing_qtr int,
--connectivity
  ex_local_miles_qtr float,
  pl_local_miles_qtr float,
  ex_reg_fac_half int,
  pl_reg_fac_half int,
  expl_reg_miles_half float,
  rbtn_corridor int,
  rbtn_alignment int,
  dist2ex_crossing_m float,
--equtiy
  acp int,
  acp50 int,
  age_under_15_pct float,
  age_65_plus_pct float,
  zero_car_pct float,
  ppl_of_color_pct float,
  wiki_women_qtr int,
  wiki_pplofcolor_qtr int,

  nearest_crossing_id int,
--scld_ safety/existing conditions
  scld__bkpd_crash_500ft float,
  scld__bw_modeshr float,
  scld__rail_volume float,
  scld__avg_slope float,
  scld__wiki_probarea_qtr float,
--scld_ safety/existing conditions
  scld__popden_2014 float,
  scld__popden_2040 float,
  scld__empden_2014 float,
  scld__empden_2040 float,
  scld_trans_riders_qtr float,
  scld__schools_half float,
  scld__colleges_half float,
  scld__regional_parks_half_mile float,
  scld__wiki_new_xing_qtr float,
--scld_ connectivity
  scld__ex_local_miles_qtr float,
  scld__pl_local_miles_qtr float,
  scld__ex_reg_fac_half float,
  scld__pl_reg_fac_half float,
  scld_expl_reg_miles_half float,
  scld_rbtn_corridor float,
  scld_rbtn_alignment float,
  scld__dist2ex_crossing_m float,
--scld_ equity
  scld__age_under_15_pct float,
  scld__age_65_plus_pct float,
  scld__zero_car_pct float,
  scld__ppl_of_color_pct float,
  scld__wiki_women_qtr float,
  scld__wiki_pplofcolor_qtr float,
--raw factor scores
  connectivity_raw float,
  demand_raw float,
  safety_raw float,
  equity_raw float,
--scld_ factor scores
  connectivity_score float,
  demand_score float,
  safety_score float,
  equity_score float,
--priority scores & ranks
  priority_raw float,
  priority_score float,
  priority_rank int,
  county_priority_rank int,
  county varchar(30),
  city varchar(100),
  community_designation varchar (30),
  major_river int,
  barrier_type varchar(50),
  barrier_desc varchar(200),
  location_desc varchar(500),
  point_type text
);

INSERT into barriers_prioritized_no_maj_river
(
    id ,
    geom ,
  --safety/existing conditions
    bkpd_crash_500ft,
    bw_modeshr,
    rail_volume,
    avg_slope,
    wiki_probarea_qtr,
  --demand
    popden_2014,
    popden_2040,
    empden_2014,
    empden_2040,
    trans_riders_qtr,
    schools_half,
    colleges_half,
    reg_parks_half,
    wiki_new_xing_qtr,
  --connectivity
    ex_local_miles_qtr,
    pl_local_miles_qtr,
    ex_reg_fac_half,
    pl_reg_fac_half,
    expl_reg_miles_half,
    rbtn_corridor,
    rbtn_alignment,
    dist2ex_crossing_m,
  --equtiy
    acp,
    acp50,
    age_under_15_pct,
    age_65_plus_pct,
    zero_car_pct,
    ppl_of_color_pct,
    wiki_women_qtr,
    wiki_pplofcolor_qtr,

    nearest_crossing_id,
    county,
    city,
    community_designation,
    major_river,
    barrier_type,
    barrier_desc,
    location_desc,
    point_type
)
select id ,
geom ,
--safety/existing conditions
bkpd_crash_500ft,
bw_modeshr,
rail_volume,
avg_slope,
wiki_probarea_qtr,
--demand
popden_2014,
popden_2040,
empden_2014,
empden_2040,
trans_riders_qtr,
schools_half,
colleges_half,
reg_parks_half,
wiki_new_xing_qtr,
--connectivity
ex_local_miles_qtr,
pl_local_miles_qtr,
ex_reg_fac_half,
pl_reg_fac_half,
expl_reg_miles_half,
rbtn_corridor,
rbtn_alignment,
dist2ex_crossing_m,
--equtiy
acp,
acp50,
age_under_15_pct,
age_65_plus_pct,
zero_car_pct,
ppl_of_color_pct,
wiki_women_qtr,
wiki_pplofcolor_qtr,
nearest_crossing_id,
county,
city,
community_designation,
major_river,
barrier_type,
barrier_desc,
location_desc,
point_type
from automated.barriers_prioritized
where major_river is null
-- and id not in ('315')
;

create index sidx_barpri_no_maj_geom on barriers_prioritized_no_maj_river using GIST (geom);
analyze barriers_prioritized_no_maj_river (geom);


--
-- --last check to remove locations indicated by client
-- alter table barriers_prioritized_no_maj_river drop column if exists remove;
-- alter table barriers_prioritized_no_maj_river add column remove int;
--
-- --last check to remove any poitns identified by client
-- update barriers_prioritized_no_maj_river
-- set remove = 1
-- from client_removals
-- where st_dwithin(barriers_prioritized_no_maj_river.geom, client_removals.geom,2);
--
-- delete from barriers_prioritized_no_maj_river
-- where remove = 1;


--scale crashes proportionately to 10
update automated.barriers_prioritized_no_maj_river
  set scld__bkpd_crash_500ft =
  bkpd_crash_500ft *
  (10::float/maxscore_table.maxscore)
  from (select max(bkpd_crash_500ft) as maxscore
  from barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;

--scale crashes by quartile
-- update automated.barriers_prioritized_no_maj_river
--   set scld__bkpd_crash_500ft = CASE
--   when bkpd_crash_500ft < quarts.quarts[1] then 2.5
--   when bkpd_crash_500ft <quarts.quarts[2] then 5
--   when bkpd_crash_500ft < quarts.quarts[3] then 7.5
--   else 10
--   end
-- from (select quantile(bkpd_crash_500ft, array[0.25,0.5,0.75,1]) as
--   quarts from barriers_prioritized_no_maj_river
--   where bkpd_crash_500ft >0) as quarts
--   where bkpd_crash_500ft >0;
update automated.barriers_prioritized_no_maj_river
  set scld__bkpd_crash_500ft = 0
  where bkpd_crash_500ft = 0;
--scale avg slope to 1
update automated.barriers_prioritized_no_maj_river
  set scld__avg_slope =
  avg_slope::float *
  (10::float/maxscore_table.maxscore)
  from (select max(avg_slope) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where avg_slope <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__avg_slope = 0
  where avg_slope = 0;
--scale bike/walk modeshare proportionally to 1
update automated.barriers_prioritized_no_maj_river
  set scld__bw_modeshr =
  bw_modeshr *
  (10::float/maxscore_table.maxscore)
  from (select max(bw_modeshr) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;

  --scale rail_volume proportionately to 10
  update automated.barriers_prioritized_no_maj_river
    set scld__rail_volume =
    rail_volume *
    (10::float/maxscore_table.maxscore)
    from (select max(rail_volume) as maxscore
    from barriers_prioritized_no_maj_river) as maxscore_table
    where maxscore <>0;
--scale rail volumes by quartile
-- update automated.barriers_prioritized_no_maj_river
--   set scld__rail_volume = CASE
--   when rail_volume < quarts.quarts[1] then 2.5
--   when rail_volume <quarts.quarts[2] then 5
--   when rail_volume < quarts.quarts[3] then 7.5
--   else 10
--   end
-- from (select quantile(rail_volume, array[0.25,0.5,0.75,1]) as
--   quarts from barriers_prioritized_no_maj_river
--   where rail_volume > 0) as quarts
--   where rail_volume > 0;
update automated.barriers_prioritized_no_maj_river
  set scld__rail_volume = 0
  where rail_volume = 0;
--scale wiki problem areas to 1
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_probarea_qtr =
  wiki_probarea_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_probarea_qtr) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where wiki_probarea_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_probarea_qtr = 0
  Where wiki_probarea_qtr =0 or
  wiki_probarea_qtr is null;
--scale current pop density proportionally to 1
update automated.barriers_prioritized_no_maj_river
  set scld__popden_2014 =
  popden_2014 *
  (10::float/maxscore_table.maxscore)
  from (select max(popden_2014) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale 2040 pop density proportionally to 1
update automated.barriers_prioritized_no_maj_river
  set scld__popden_2040 =
  popden_2040 *
  (10::float/maxscore_table.maxscore)
  from (select max(popden_2040) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale current employment density proportionally to 1
update automated.barriers_prioritized_no_maj_river
  set scld__empden_2014 =
  empden_2014 *
  (10::float/maxscore_table.maxscore)
  from (select max(empden_2014) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale 2040 employment density proportionally to 1
update automated.barriers_prioritized_no_maj_river
  set scld__empden_2040 =
  empden_2040 *
  (10::float/maxscore_table.maxscore)
  from (select max(empden_2040) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale age 65+ to 1
update automated.barriers_prioritized_no_maj_river
  set scld__age_65_plus_pct =
  age_65_plus_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(age_65_plus_pct) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale age under 15 % to 1
update automated.barriers_prioritized_no_maj_river
  set scld__age_under_15_pct =
  age_under_15_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(age_under_15_pct) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale zero car household % to 1
update automated.barriers_prioritized_no_maj_river
  set scld__zero_car_pct =
  zero_car_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(zero_car_pct) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale percent peaople of color to 1
update automated.barriers_prioritized_no_maj_river
  set scld__ppl_of_color_pct =
  ppl_of_color_pct::float *
  (10::float/maxscore_table.maxscore)
  from (select max(ppl_of_color_pct) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;
--scale wiki comments from women to 0-1
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_women_qtr =
  wiki_women_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_women_qtr) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  WHERE wiki_women_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_women_qtr = 0
  WHERE wiki_women_qtr = 0;
--scale wiki comments from people of color within 1/4 mile to 0-1
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_pplofcolor_qtr =
  wiki_pplofcolor_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_pplofcolor_qtr) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  WHERE wiki_pplofcolor_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_pplofcolor_qtr = 0
  WHERE wiki_pplofcolor_qtr = 0;
--scale schools to 1
update automated.barriers_prioritized_no_maj_river
  set scld__schools_half =
  schools_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(schools_half) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where schools_half <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__schools_half = 0
  Where schools_half =0;
--scale colleges to 0-1
update automated.barriers_prioritized_no_maj_river
  set scld__colleges_half =
  colleges_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(colleges_half) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  WHERE colleges_half <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__colleges_half = 0
  WHERE colleges_half = 0;
--scale regional parks to 0-1
update automated.barriers_prioritized_no_maj_river
  set scld__regional_parks_half_mile =
  reg_parks_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(reg_parks_half) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  WHERE reg_parks_half <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__regional_parks_half_mile = 0
  WHERE reg_parks_half = 0;
--scale wiki suggest crossing qtr mile to 0-1
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_new_xing_qtr =
  wiki_new_xing_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(wiki_new_xing_qtr) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  WHERE wiki_new_xing_qtr <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__wiki_new_xing_qtr = 0
  WHERE wiki_new_xing_qtr = 0;

  --scale transit ridership to 10
  update automated.barriers_prioritized_no_maj_river
    set scld_trans_riders_qtr =
    trans_riders_qtr *
    (10::float/maxscore_table.maxscore)
    from (select max(trans_riders_qtr) as maxscore
    from barriers_prioritized_no_maj_river) as maxscore_table
    where maxscore <>0;
--scale transit ridership into quartiles
-- update automated.barriers_prioritized_no_maj_river
--   set scld_trans_riders_qtr = CASE
--   when trans_riders_qtr < quarts.quarts[1] then 2.5
--   when trans_riders_qtr <quarts.quarts[2] then 5
--   when trans_riders_qtr < quarts.quarts[3] then 7.5
--   else 10
--   end
-- from (select quantile(trans_riders_qtr, array[0.25,0.5,0.75,1]) as
--   quarts from barriers_prioritized_no_maj_river
--   where trans_riders_qtr > 0) as quarts
--   where trans_riders_qtr > 0;
update automated.barriers_prioritized_no_maj_river
  set scld_trans_riders_qtr = 0
  where trans_riders_qtr = 0;
--scale existing regional trails to 1
update automated.barriers_prioritized_no_maj_river
  set scld__ex_reg_fac_half =
  ex_reg_fac_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(ex_reg_fac_half) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where ex_reg_fac_half <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__ex_reg_fac_half = 0
  where ex_reg_fac_half = 0;
--scale planned regional trails to 1
update automated.barriers_prioritized_no_maj_river
  set scld__pl_reg_fac_half =
  pl_reg_fac_half::float *
  (10::float/maxscore_table.maxscore)
  from (select max(pl_reg_fac_half) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where pl_reg_fac_half <>0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__pl_reg_fac_half = 0
  where pl_reg_fac_half = 0;
--scale expl_reg_miles_half to 10
update automated.barriers_prioritized_no_maj_river
  set scld_expl_reg_miles_half =
  expl_reg_miles_half::float*
  (10::float/maxscore_table.maxscore)
  from (select max(expl_reg_miles_half) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where expl_reg_miles_half <> 0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld_expl_reg_miles_half = 0
  where expl_reg_miles_half = 0;
--scale existing local trails calc to 0-1
update automated.barriers_prioritized_no_maj_river
  set scld__ex_local_miles_qtr =
  ex_local_miles_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(ex_local_miles_qtr) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where ex_local_miles_qtr <> 0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__ex_local_miles_qtr = 0
  where ex_local_miles_qtr = 0;

  --scale rbtn corridor
  update automated.barriers_prioritized_no_maj_river
    set scld_rbtn_corridor = rbtn_corridor;
  --scale rbtn_alignment
  update automated.barriers_prioritized_no_maj_river
    set scld_rbtn_alignment = rbtn_alignment;
--scale dist to existing crossing proportionally to 1
update automated.barriers_prioritized_no_maj_river
  set scld__dist2ex_crossing_m =
  dist2ex_crossing_m::float *
  (10::float/maxscore_table.maxscore)
  from (select max(dist2ex_crossing_m) as maxscore
  from barriers_prioritized_no_maj_river) as maxscore_table;
--scale planned local trails calc to 0-1
update automated.barriers_prioritized_no_maj_river
  set scld__pl_local_miles_qtr =
  pl_local_miles_qtr::float *
  (10::float/maxscore_table.maxscore)
  from (select max(pl_local_miles_qtr) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where pl_local_miles_qtr <> 0
  and maxscore <>0;
update automated.barriers_prioritized_no_maj_river
  set scld__pl_local_miles_qtr = 0
  where pl_local_miles_qtr = 0;





--calculate raw connectivity score
update automated.barriers_prioritized_no_maj_river
  set connectivity_raw =(
    scld__pl_local_miles_qtr+scld__ex_local_miles_qtr+
    --scld__pl_reg_fac_half + scld__ex_reg_fac_half+
    scld_expl_reg_miles_half +
    scld_rbtn_corridor+scld_rbtn_alignment +
    coalesce(scld__dist2ex_crossing_m,0))/6;
--scale raw connectivity
update automated.barriers_prioritized_no_maj_river
  set connectivity_score =
  connectivity_raw::float * 4.825
;


--calculate raw demand
update automated.barriers_prioritized_no_maj_river
  set demand_raw =
  (scld__popden_2040 +
  scld__empden_2040 +
  scld_trans_riders_qtr + scld__schools_half +
  scld__colleges_half + scld__regional_parks_half_mile +
  scld__wiki_new_xing_qtr)/7;
--scale demand score
update automated.barriers_prioritized_no_maj_river
  set demand_score =
  demand_raw::float *2.425;




--calculate raw safety/existing conditions
update automated.barriers_prioritized_no_maj_river
  set safety_raw = (scld__bkpd_crash_500ft +
  scld__bw_modeshr
  + scld__popden_2014
  + scld__empden_2014
  --+ scld__avg_slope
  --+scld__rail_volume
  + scld__wiki_probarea_qtr)/5;
--scale safety/exsiting conditions score
update automated.barriers_prioritized_no_maj_river
  set safety_score =
  safety_raw::float *1.525;



--calculate raw equity
update automated.barriers_prioritized_no_maj_river
  set equity_raw = (acp + acp50 +
  scld__age_under_15_pct + scld__age_65_plus_pct +
  scld__zero_car_pct + scld__ppl_of_color_pct +
  scld__wiki_women_qtr +
  scld__wiki_pplofcolor_qtr)/8;
--scale equity to 100
update automated.barriers_prioritized_no_maj_river
  set equity_score =
  equity_raw::float *1.225;


--calculate raw priority score
update automated.barriers_prioritized_no_maj_river
  set priority_raw =
  connectivity_score + demand_score +
  safety_score + equity_score;
--scalculate scld_ priority score
update automated.barriers_prioritized_no_maj_river
  set priority_score =
  priority_raw::float *
  (100/maxscore_table.maxscore)
  from (select max(priority_raw) as maxscore from
  barriers_prioritized_no_maj_river) as maxscore_table
  where maxscore <>0;

--rank projects regionally
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized_no_maj_river
)
UPDATE automated.barriers_prioritized_no_maj_river
set priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
;

--rank projects in individual counties
--Anoka
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized_no_maj_river
  where county = 'Anoka'
)
UPDATE automated.barriers_prioritized_no_maj_river
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
and county = 'Anoka'
;
--Carver
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized_no_maj_river
  where county = 'Carver'
)
UPDATE automated.barriers_prioritized_no_maj_river
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
and county = 'Carver'
;
--Dakota
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized_no_maj_river
  where county = 'Dakota'
)
UPDATE automated.barriers_prioritized_no_maj_river
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
and county = 'Dakota'
;
--Hennepin
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized_no_maj_river
  where county = 'Hennepin'
)
UPDATE automated.barriers_prioritized_no_maj_river
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
and county = 'Hennepin'
;
--Ramsey
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized_no_maj_river
  where county = 'Ramsey'
)
UPDATE automated.barriers_prioritized_no_maj_river
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
and county = 'Ramsey'
;
--Scott
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized_no_maj_river
  where county = 'Scott'
)
UPDATE automated.barriers_prioritized_no_maj_river
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
and county = 'Scott'
;
--Washington
WITH ranks AS (
  Select id, rank() over (order by priority_score desc) as rank
  from automated.barriers_prioritized
  where county = 'Washington'
)
UPDATE automated.barriers_prioritized_no_maj_river
set county_priority_rank = ranks.rank
from ranks
where barriers_prioritized_no_maj_river.id = ranks.id
and county = 'Washington'
;


--create county crossing ID
alter table barriers_prioritized_no_maj_river drop column if exists tdg_county_id;
alter table barriers_prioritized_no_maj_river add column tdg_county_id varchar(10);

with ranks as (
  select id, rank() over
  (order by
    st_y(geom) DESC,
    st_x(geom) ASC) as rank
from barriers_prioritized_no_maj_river
where county = 'Anoka')
update barriers_prioritized_no_maj_river
  set tdg_county_id = concat('A', lpad(ranks.rank::text,3,'0'))
  from ranks
  where barriers_prioritized_no_maj_river.id = ranks.id
  and county = 'Anoka'
  ;

with ranks as (
  select id, rank() over
  (order by
    st_y(geom) DESC,
    st_x(geom) ASC) as rank
from barriers_prioritized_no_maj_river
where county = 'Carver')
update barriers_prioritized_no_maj_river
  set tdg_county_id = concat('C', lpad(ranks.rank::text,3,'0'))
  from ranks
  where barriers_prioritized_no_maj_river.id = ranks.id
  and county = 'Carver'
  ;

with ranks as (
  select id, rank() over
  (order by
    st_y(geom) DESC,
    st_x(geom) ASC) as rank
from barriers_prioritized_no_maj_river
where county = 'Dakota')
update barriers_prioritized_no_maj_river
  set tdg_county_id = concat('D', lpad(ranks.rank::text,3,'0'))
  from ranks
  where barriers_prioritized_no_maj_river.id = ranks.id
  and county = 'Dakota'
  ;


with ranks as (
  select id, rank() over
  (order by
    st_y(geom) DESC,
    st_x(geom) ASC) as rank
from barriers_prioritized_no_maj_river
where county = 'Hennepin')
update barriers_prioritized_no_maj_river
  set tdg_county_id = concat('H',lpad(ranks.rank::text,3,'0'))
  from ranks
  where barriers_prioritized_no_maj_river.id = ranks.id
  and county = 'Hennepin'
  ;


with ranks as (
  select id, rank() over
  (order by
    st_y(geom) DESC,
    st_x(geom) ASC) as rank
from barriers_prioritized_no_maj_river
where county = 'Ramsey')
update barriers_prioritized_no_maj_river
  set tdg_county_id = concat('R', lpad(ranks.rank::text,3,'0'))
  from ranks
  where barriers_prioritized_no_maj_river.id = ranks.id
  and county = 'Ramsey'
  ;


with ranks as (
  select id, rank() over
  (order by
    st_y(geom) DESC,
    st_x(geom) ASC) as rank
from barriers_prioritized_no_maj_river
where county = 'Scott')
update barriers_prioritized_no_maj_river
  set tdg_county_id = concat('S',lpad(ranks.rank::text,3,'0'))
  from ranks
  where barriers_prioritized_no_maj_river.id = ranks.id
  and county = 'Scott'
  ;


with ranks as (
  select id, rank() over
  (order by
    st_y(geom) DESC,
    st_x(geom) ASC) as rank
from barriers_prioritized_no_maj_river
where county = 'Washington')
update barriers_prioritized_no_maj_river
  set tdg_county_id = concat('W', lpad(ranks.rank::text,3,'0'))
  from ranks
  where barriers_prioritized_no_maj_river.id = ranks.id
  and county = 'Washington'
  ;

-- make sure point_type is filled out owhere null
  update barriers_prioritized_no_maj_river
  set point_type = client_point_edits_20170709.newpointty
  from client_point_edits_20170709
  where barriers_prioritized_no_maj_river.point_type is null
  and st_dwithin(barriers_prioritized_no_maj_river.geom, client_point_edits_20170709.geom,2);

  update barriers_prioritized_no_maj_river
    set point_type = 'metcouncil addition'
    where point_type is null;

--flag source relating to metcouncil comments 7/14
  alter table barriers_prioritized_no_maj_river drop column if exists source_check;
  alter table barriers_prioritized_no_maj_river add column source_check varchar(50);
  with u as (select st_union(geom) as geom from confirmed_points_20170714)
  update barriers_prioritized_no_maj_river
  	set source_check = 'confirmed v2'
  	from u
  	where st_dwithin(u.geom, barriers_prioritized_no_maj_river.geom,2);

  with u as (select st_union(geom) as geom from missed_points_20170714)
  update barriers_prioritized_no_maj_river b
  	set source_check = 'new adds'
  	from u
  	where st_dwithin(u.geom,b.geom,0.01)
  	;
