-- CREATE SCHEMA scratch;
-- CREATE TABLE  scratch.work_div AS SELECT * from af_modvrs_na_2012.work_div;
-- CREATE TABLE  scratch.work_lrg AS SELECT * FROM af_modvrs_na_2012.work_lrg;

-- pure sql based answer is probably inefficient, better use external network library
-- https://www.fusionbox.com/blog/detail/graph-algorithms-in-a-database-recursive-ctes-and-topological-sort-with-postgres/620/
-- igraph is probably more efficient than networkx as it is implemented in C (networkx is pure python)
-- https://graph-tool.skewed.de/performance
-- But still, the approach I took was to load all the edges as array_agg (python list), which needs to be fixed (memory short)
-- maybe pass table to python function, then inside python, load the edges to graph and return connected components?
-- try divide and concur
-- 


SET search_path TO scratch, public;



-- assume that there is work_lrg
-- grab 10 deg by 10 deg subregion at a time
--   make near table
--   find overlaps (connected components)
--   pick the fire of overlapping detections
-- go through one more time grabbing everything?
-- you got list of fireid that overlaps
-- 
-- count # of days for each overlaps
-- if more than 10 day, or something like that, drop them

-- better make above to some kind of pgsql function or procedure, as i need to 
-- do this across different part of domains


-- determine extenty of domain, come up with list of tiles

DROP TABLE IF EXISTS tbl_ext0;
CREATE TABLE tbl_ext0 AS
WITH foo AS ( 
  SELECT min(floor(ST_Xmin(geom_lrg))) xmn, min(ceil(ST_Ymin(geom_lrg))) ymn, max(floor(ST_XMax(geom_lrg))) xmx, max(ceil(ST_YMax(geom_lrg))) ymx, 10 dx, 10 dy
  FROM work_lrg
)
SELECT xmn, xmx, ymn, ymx, dx, dy, ceil((xmx-xmn)/dx) nx, ceil((ymx-ymn)/dy) ny
FROM foo;


DROP TABLE IF EXISTS tbl_ext;
CREATE TABLE tbl_ext AS
WITH baz AS
(
  WITH foo AS 
  (
    SELECT row_number()  over ( ) - 1 idx
    FROM (
      SELECT unnest(array_fill(1, array[nx::integer]))
      FROM tbl_ext0
    ) x
  ), 
  bar AS 
  (
    SELECT row_number() over () - 1 jdx
    FROM (
      SELECT unnest(array_fill(1, array[ny::integer]))
      FROM tbl_ext0
    ) x
  )
  SELECT idx, jdx, 
  xmn + idx * dx x0 , 
  xmn + (idx+1) * dx x1 ,
  ymn + jdx * dy y0 , 
  ymn + (jdx+1) * dy y1 
  FROM foo CROSS JOIN bar, tbl_ext0
)
SELECT *,  
('SRID=4326; POLYGON((' || 
    x0::text || ' '  ||  y0::text || ',' ||
    x0::text || ' '  ||  y1::text || ',' ||
    x1::text || ' '  ||  y1::text || ',' ||
    x1::text || ' '  ||  y0::text || ',' ||
    x0::text || ' '  ||  y0::text || '))')::geometry geom
FROM baz;



DO language plpgsql $$
  DECLARE
    r RECORD;
--    idx INTEGER := 0;
--    jdx INTEGER := 0;
--    nx INTEGER;
--    ny INTEGER;
        n INTEGER;
  BEGIN

    FOR r IN SELECT * FROM tbl_ext LOOP
      --raise notice '% % %', r.idx, r.jdx,  ST_AsText(r.geom);

      drop table if exists tbl_dupdet;
      create table tbl_dupdet as
      (



      SELECT * from work_lrg 
      where 
      work_lrg.area_sqkm < 3.0 
      and work_lrg.geom_lrg && r.geom
      and st_intersects(work_lrg.geom_lrg, r.geom)
    );
    n:= (select count(*) from tbl_dupdet);
      raise notice '% % % %', r.idx, r.jdx, n, ST_AsText(r.geom);

--
    END LOOP;
END
$$ ;










-- go over each tile, come up with overlapping detections

-- screen to find fireid's to drop




-- -- I am going to reuse this function defined in step1_prep
-- -- 
-- -- -----------------------------------------
-- -- -- Part 2.1: pnt2grp (points to group) --
-- -- -----------------------------------------
-- -- 
-- drop type if exists p2grp cascade;
-- create type p2grp as (
-- 	fireid integer,
-- 	lhs integer,
-- 	rhs integer,
-- 	ndetect integer
-- );
-- 
-- 
-- -- given edges, return id of connected components to which it belongs to
-- -- edges are defined as two integer vectors lhs (verctor of id of start points) and rhs (end points)
-- -- return value is setof p2grp, which has 
-- --   fireid (lowest id within the component, which can be think of as id of connected component)
-- --   lhs (input)
-- --   rhs (input)
-- --   ndetect (count of nodes within components
-- create or replace function pnt2grp(lhs integer[], rhs integer[])
-- returns setof p2grp as
-- $$
--     """given edges, return connected components"""
--     import time, datetime
--     t0 = time.time()
--     import networkx as nx
--     g = nx.Graph()
--     g.add_edges_from((l,r) for (l,r) in zip(lhs,rhs))
--     plpy.notice("g.size(): %d, %s" % (g.size(), datetime.datetime.now()))
--     #plpy.notice("g.order(): %d" % g.order())
--     
--     results = []
--     ccs = nx.connected_component_subgraphs(g)
-- 
--     for sg in ccs:
--         clean0 = min(sg.nodes())
--         n = sg.order()
--         for e in sg.edges():
--             e = e if e[0] < e[1] else (e[1],e[0])
--             results.append([clean0, e[0], e[1], n])
--     #plpy.notice("elapsed: %d", (time.time() - t0)) 
--     return results
--     
-- $$ 
-- language plpython3u volatile;
-- -- language plpythonu volatile;
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
-- -- 
-- -- CREATE UNIQUE INDEX idx_near_pair ON tbl_near(lhs, rhs);
-- 
-- do language plpgsql $$ begin
-- raise notice 'tool: tbl_togrp , %', clock_timestamp();
-- end $$;
-- 
-- DROP TABLE IF EXISTS tbl_togrp0;
-- 
-- -- This chokes memory on large problem
-- CREATE TABLE tbl_togrp0 AS
-- WITH foo AS
-- (
--   SELECT array_agg(lhs) lhs, array_agg(rhs) rhs
--   FROM tbl_near0
-- ),
-- bar AS
-- (
--   SELECT pnt2grp(lhs, rhs) pnt2grp
--   FROM foo
-- )
-- SELECT (pnt2grp).fireid polyid,  (pnt2grp).lhs, (pnt2grp).rhs, (pnt2grp).ndetect
-- FROM bar;
-- 
-- -- -- This is very slow?
-- -- https://stackoverflow.com/questions/45212799/how-to-identify-groups-clusters-in-set-of-arcs-edges-in-sql?noredirect=1&lq=1
-- -- found here, looked good but
-- -- https://www.fusionbox.com/blog/detail/graph-algorithms-in-a-database-recursive-ctes-and-topological-sort-with-postgres/620/
-- -- seems to be a bad idea to do this with database.  better stick with networkx
-- -- CREATE TABLE tbl_togrp0 AS 
-- -- WITH RECURSIVE nodecluster (
-- --   lhs, rhs, cluster1) AS ( 
-- --   SELECT lhs, rhs, Rank() Over( ORDER BY lhs )
-- --   FROM tbl_near0 AS n1
-- --   WHERE NOT EXISTS (
-- --     SELECT lhs,rhs from tbl_near0 AS n2 
-- --     WHERE n1.lhs = n2.rhs )
-- --   UNION ALL
-- --   SELECT n1.lhs, n1.rhs, nodecluster.cluster1
-- --   FROM tbl_near0 n1, nodecluster
-- --   WHERE nodecluster.rhs = n1.lhs )
-- -- SELECT * FROM nodecluster
-- -- ORDER BY cluster1, lhs, rhs;
--        
-- 
-- 
-- 
-- 
-- do language plpgsql $$ begin
-- raise notice 'tool: done , %', clock_timestamp();
-- end $$;

-- vim: et sw=2
