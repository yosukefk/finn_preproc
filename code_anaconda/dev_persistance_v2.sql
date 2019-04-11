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
  --SELECT min(floor(ST_Xmin(geom_lrg))) xmn, min(ceil(ST_Ymin(geom_lrg))) ymn, max(floor(ST_XMax(geom_lrg))) xmx, max(ceil(ST_YMax(geom_lrg))) ymx, 10 dx, 10 dy
  SELECT min(floor(ST_Xmin(geom_lrg))) xmn, min(ceil(ST_Ymin(geom_lrg))) ymn, max(floor(ST_XMax(geom_lrg))) xmx, max(ceil(ST_YMax(geom_lrg))) ymx, 2 dx, 2 dy
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

DROP TYPE IF EXISTS persistence CASCADE;
CREATE TYPE persistence AS ( 
  grpid integer,
  fireid integer,
  ndetect integer
);

-- i read that i shouldnt create/drop tables inside function, makes many locks
-- better make one outside
-- https://stackoverflow.com/questions/16490664/error-out-of-shared-memory
CREATE OR REPLACE FUNCTION prep_find_persistence(tbl regclass)
RETURNS void
AS $$

BEGIN

  DROP TABLE IF EXISTS tbl_pers_in;
  EXECUTE 'CREATE TEMPORARY TABLE tbl_pers_in (
    LIKE ' || tbl || ');';

  DROP TABLE IF EXISTS tbl_pers_near;
  CREATE TEMPORARY TABLE tbl_pers_near (
    lhs integer,
    rhs integer
  );

  DROP TABLE IF EXISTS tbl_pers_togrp;
  CREATE TEMPORARY TABLE tbl_pers_togrp (
    fireid integer,
    lhs integer,
    rhs integer,
    ndetect integer
  );

  DROP TABLE IF EXISTS tbl_pers_grpcnt;
  CREATE TEMPORARY TABLE tbl_pers_grpcnt (
    grpid integer,
    fireid integer,
    ndetect integer
  );

END
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION find_persistence(tbl regclass)
RETURNS setof persistence 
AS $$ 

-- get table of work_lrg, find list of persistent detection (i.e. collocated detections across days)
DECLARE
  n integer;

BEGIN

  -- subset smaller fires
  EXECUTE 'TRUNCATE TABLE tbl_pers_in;';
  EXECUTE 'INSERT INTO tbl_pers_in (
    SELECT * from ' || tbl || 
    ' WHERE area_sqkm < 2 ' || 
    ');';
  n := (SELECT count(*) FROM tbl_pers_in);
  raise notice 'pers: in %', n;

  -- create near table
  TRUNCATE TABLE tbl_pers_near;
  INSERT INTO tbl_pers_near ( 
    WITH foo AS ( 
      SELECT 
      a.fireid AS aid, 
      a.geom_lrg AS ageom, 
      b.fireid AS bid, 
      b.geom_lrg AS bgeom 
      FROM tbl_pers_in AS a 
      INNER JOIN tbl_pers_in AS b 
      ON a.geom_lrg && b.geom_lrg
      AND ST_Overlaps(a.geom_lrg, b.geom_lrg) 
      and a.fireid < b.fireid
    ) 
    SELECT aid AS lhs, bid AS rhs 
    FROM foo) 
  ;
--  CREATE UNIQUE INDEX idx_pers_near ON tbl_pers_near(lhs, rhs);
  n := (SELECT count(*) FROM tbl_pers_near);
  raise notice 'pers: near %', n;

  IF n = 0 THEN
    RETURN;
  END IF;

  -- identify connected components
  TRUNCATE TABLE tbl_pers_togrp;
  INSERT INTO tbl_pers_togrp ( 
    WITH foo AS
    (
      SELECT array_agg(lhs) lhs, array_agg(rhs) rhs
      FROM tbl_pers_near
    ),
    bar AS
    (
      SELECT pnt2grp(lhs, rhs) pnt2grp
      FROM foo
    )
    SELECT (pnt2grp).fireid,  (pnt2grp).lhs, (pnt2grp).rhs, (pnt2grp).ndetect
    FROM bar
  );

  n := (SELECT count(*) FROM tbl_pers_togrp);
  raise notice 'pers: togrp %', n;

  -- make list of nearby fires with count of repeated obs
  TRUNCATE tbl_pers_grpcnt;
  INSERT INTO tbl_pers_grpcnt ( 
    with foo AS ( 
      SELECT fireid,lhs,ndetect FROM tbl_pers_togrp 
      UNION ALL 
      SELECT fireid,rhs,ndetect FROM tbl_pers_togrp
    ) 
    SELECT DISTINCT fireid grpid, lhs fireid, ndetect FROM foo
  );

  n := (SELECT count(*) FROM tbl_pers_grpcnt);
  raise notice 'pers: grpcnt %', n;

  RETURN QUERY SELECT grpid, fireid, ndetect FROM tbl_pers_grpcnt;


  RETURN;

END
$$ LANGUAGE plpgsql;



DO language plpgsql $$
  DECLARE
    r RECORD;
        n INTEGER;
        p persistence[];

	run boolean := FALSE;
  BEGIN

    -- by-pass generation of tbl_persistent
    IF NOT run THEN
      RETURN;
    END IF;
      

    DROP TABLE IF EXISTS tbl_persistent;
    CREATE TABLE tbl_persistent (
      grpid  integer,
      fireid integer,
      ndetect integer
    );

    DROP TABLE IF EXISTS tbl_dupdet;
    CREATE TEMPORARY TABLE tbl_dupdet (
      LIKE work_lrg
    );
    PERFORM prep_find_persistence('work_lrg');



    FOR r IN SELECT * FROM tbl_ext LOOP 
      --raise notice '% % %', r.idx, r.jdx,  ST_AsText(r.geom); 

      --IF r.idx = 4 AND r.jdx = 2 THEN -- Texas?
      --IF r.idx = 4 AND r.jdx = 0 THEN -- only 1000 points
--      IF r.x0 < -98.1 AND r.x1 > -98.1 AND r.y0 < 29.7 AND r.y1 > 29.7 OR -- New Braunfels
--        r.x0 < -99.3 AND r.x1 > -99.3 AND r.y0 < 28.3 AND r.y1 > 28.3 -- Artesia Wells
      IF TRUE
      THEN 

      -- create scratch table
      TRUNCATE TABLE tbl_dupdet;
      INSERT INTO tbl_dupdet
      ( 
        SELECT * FROM work_lrg 
        WHERE 
        work_lrg.geom_lrg && r.geom 
        AND st_intersects(work_lrg.geom_lrg, r.geom)
      ); 
      
      n:= (select count(*) from tbl_dupdet);
      raise notice '% % % %', r.idx, r.jdx, n, ST_AsText(r.geom);

      CONTINUE WHEN n < 2;


      WITH foo AS (
        SELECT find_persistence('tbl_dupdet') x
      )
      INSERT INTO tbl_persistent
      SELECT (x).grpid, (x).fireid, (x).ndetect 
      FROM foo;



      END IF;


--
    END LOOP;
END
$$ ;

DROP TABLE IF EXISTS tbl_pers_fire_across_grps ;
CREATE TEMPORARY TABLE tbl_pers_fire_across_grps AS
WITH foo AS (
  SELECT DISTINCT grpid, fireid
  FROM tbl_persistent
), bar AS ( 
  SELECT fireid, count(*)
  FROM foo
  GROUP BY fireid
), baz AS (
  SELECT * FROM bar 
  WHERE count > 1
), qux AS (
  SELECT p.* 
  FROM tbl_persistent p
  INNER JOIN baz b
  ON p.fireid = b.fireid
  ORDER BY p.fireid, p.grpid
)
SELECT array_agg(grpid) arr_grpid, fireid 
FROM qux 
GROUP BY fireid
;

DROP TABLE IF EXISTS tbl_pers_map_grpid;
CREATE TABLE tbl_pers_map_grpid AS
WITH foo AS (
  SELECT DISTINCT arr_grpid
  FROM tbl_pers_fire_across_grps
)
SELECT unnest(arr_grpid) grpid_from, 
arr_grpid[1] grpid_to
FROM foo
;


DROP TABLE IF EXISTS tbl_persistent2;
CREATE TEMPORARY TABLE tbl_persistent2 AS
TABLE tbl_persistent;

UPDATE tbl_persistent2 p
SET grpid = m.grpid_to
FROM tbl_pers_map_grpid m
WHERE p.grpid = m.grpid_from;

DROP TABLE IF EXISTS tbl_persistent3;
CREATE TEMPORARY TABLE tbl_persistent3 AS 
WITH foo AS (
  SELECT DISTINCT grpid, fireid
  FROM tbl_persistent2
), bar AS (
  SELECT grpid, count(*) ndetect
  FROM foo
  GROUP BY grpid
)
SELECT f.grpid, f.fireid, b.ndetect, 1.0 fac_rept, ST_Area(w.geom_lrg, TRUE)/1000./1000. a_fire, 0.0 a_fire_sum, 0.0 a_grp, w.geom_lrg geom
FROM foo f
INNER JOIN bar b
ON f.grpid = b.grpid
INNER JOIN work_lrg w
ON f.fireid = w.fireid
;


DROP TABLE IF EXISTS tbl_pers_grparea;
CREATE TABLE tbl_pers_grparea AS 
WITH foo AS ( 
  SELECT grpid, count(*) as ndetect, sum(a_fire) a_fire_sum, ST_Union(geom) geom
  FROM tbl_persistent3
  GROUP BY grpid
), bar AS (
  SELECT grpid, ndetect, ST_Area(geom, TRUE)/1000./1000. a_grp, a_fire_sum, geom geom_a, ST_Centroid(geom) geom_p
  FROM foo
)
SELECT  grpid, ndetect, a_grp, a_fire_sum, (a_fire_sum / a_grp) fac_rept, geom_a, geom_p
from bar
;

UPDATE tbl_persistent3 p
SET a_grp = g.a_grp, 
fac_rept = g.fac_rept,
a_fire_sum = g.a_fire_sum
FROM tbl_pers_grparea g
WHERE p.grpid = g.grpid;


DROP TABLE IF EXISTS tbl_persistent4;
CREATE TABLE tbl_persistent4 AS 
SELECT grpid, fireid, ndetect, fac_rept
FROM tbl_persistent3;






-- SELECT p.grpid, p.fireid
-- FROM tbl_persistent p
-- INNER JOIN fire_across_grps f
-- ON p.fireid = f.fireid
-- ;
-- SELECT a.* 
-- FROM tbl_persistent a
-- INNER JOIN bar b
-- ON a.fireid = b.fireid
-- ORDER BY fireid;












-- vim: et sw=2
