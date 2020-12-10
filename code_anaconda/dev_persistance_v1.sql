-- CREATE SCHEMA scratch;
-- CREATE TABLE  scratch.work_div AS SELECT * from af_modvrs_na_2012.work_div;
-- CREATE TABLE  scratch.work_lrg AS SELECT * FROM af_modvrs_na_2012.work_lrg;

SET search_path TO scratch, public;
-- 
-- -----------------------------------------
-- -- Part 2.1: pnt2grp (points to group) --
-- -----------------------------------------
-- 
drop type if exists p2grp cascade;
create type p2grp as (
	fireid integer,
	lhs integer,
	rhs integer,
	ndetect integer
);


-- given edges, return id of connected components to which it belongs to
-- edges are defined as two integer vectors lhs (verctor of id of start points) and rhs (end points)
-- return value is setof p2grp, which has 
--   fireid (lowest id within the component, which can be think of as id of connected component)
--   lhs (input)
--   rhs (input)
--   ndetect (count of nodes within components
create or replace function pnt2grp(lhs integer[], rhs integer[])
returns setof p2grp as
$$
    """given edges, return connected components"""
    plpy.notice("pnt2pgp")
    import time, datetime
    plpy.notice("pnt2pgp called, %s" % ( datetime.datetime.now()))
    t0 = time.time()
    import networkx as nx
    g = nx.Graph()
    g.add_edges_from((l,r) for (l,r) in zip(lhs,rhs))
    plpy.notice("g.size(): %d, %s" % (g.size(), datetime.datetime.now()))
    #plpy.notice("g.order(): %d" % g.order())
    
    results = []
    ccs = nx.connected_component_subgraphs(g)

    for sg in ccs:
        clean0 = min(sg.nodes())
        n = sg.order()
        for e in sg.edges():
            e = e if e[0] < e[1] else (e[1],e[0])
            results.append([clean0, e[0], e[1], n])
    #plpy.notice("elapsed: %d", (time.time() - t0)) 
    return results
    
$$ 
language plpython3u volatile;
-- language plpythonu volatile;
-- 
-- 
-- do language plpgsql $$ begin
-- raise notice 'tool: create index, %', clock_timestamp();
-- end $$;
-- 
-- CREATE INDEX work_div_idx
-- ON work_div
-- USING GIST(geom);
-- 
-- do language plpgsql $$ begin
-- raise notice 'tool: tbl_near , %', clock_timestamp();
-- end $$;
-- -- near table
-- DROP TABLE IF EXISTS tbl_near0;
-- 
-- CREATE TABLE tbl_near0 AS 
-- WITH foo AS ( 
--   SELECT
--   a.polyid AS aid,
--   a.geom AS ageom,
--   b.polyid AS bid,
--   b.geom AS bgeom
--   FROM work_div_newbraunfels AS a
--   INNER JOIN work_div_newbraunfels AS b
-- --  FROM work_div AS a
-- --  INNER JOIN work_div AS b
--   ON a.geom && b.geom
--   AND ST_Overlaps(a.geom, b.geom)
--   and a.polyid < b.polyid
-- ) 
-- SELECT aid AS lhs, bid AS rhs 
-- FROM foo
-- ;
-- 
-- CREATE UNIQUE INDEX idx_near_pair ON tbl_near(lhs, rhs);

do language plpgsql $$ begin
raise notice 'tool: tbl_togrp , %', clock_timestamp();
end $$;

-- DROP TABLE IF EXISTS tbl_togrp;
-- 
-- -- This chokes memory on large problem
-- CREATE TABLE tbl_togrp AS
-- WITH foo AS
-- (
--   SELECT array_agg(lhs) lhs, array_agg(rhs) rhs
--   FROM tbl_near
-- ),
-- bar AS
-- (
--   SELECT pnt2grp(lhs, rhs) pnt2grp
--   FROM foo
-- )
-- SELECT (pnt2grp).fireid polyid,  (pnt2grp).lhs, (pnt2grp).rhs, (pnt2grp).ndetect
-- FROM bar;

drop table if exists tbl_grpcnt0;
create table tbl_grpcnt0 as
with foo as
(select polyid,lhs,ndetect from tbl_togrp0
  union all
select polyid,rhs,ndetect from tbl_togrp0
)
select distinct polyid,lhs,ndetect from foo;

-- -- This is very slow?
-- https://stackoverflow.com/questions/45212799/how-to-identify-groups-clusters-in-set-of-arcs-edges-in-sql?noredirect=1&lq=1
-- found here, looked good but
-- https://www.fusionbox.com/blog/detail/graph-algorithms-in-a-database-recursive-ctes-and-topological-sort-with-postgres/620/
-- seems to be a bad idea to do this with database.  better stick with networkx
-- CREATE TABLE tbl_togrp0 AS 
-- WITH RECURSIVE nodecluster (
--   lhs, rhs, cluster1) AS ( 
--   SELECT lhs, rhs, Rank() Over( ORDER BY lhs )
--   FROM tbl_near0 AS n1
--   WHERE NOT EXISTS (
--     SELECT lhs,rhs from tbl_near0 AS n2 
--     WHERE n1.lhs = n2.rhs )
--   UNION ALL
--   SELECT n1.lhs, n1.rhs, nodecluster.cluster1
--   FROM tbl_near0 n1, nodecluster
--   WHERE nodecluster.rhs = n1.lhs )
-- SELECT * FROM nodecluster
-- ORDER BY cluster1, lhs, rhs;
       




do language plpgsql $$ begin
raise notice 'tool: done , %', clock_timestamp();
end $$;

-- vim: et sw=2
