SET search_path TO scratch, public;

-- CREATE INDEX work_lrg_gix
-- ON work_lrg
-- using gist(geom_lrg);

DROP TABLE IF EXISTS tbl_oilgas;
CREATE TABLE tbl_oilgas AS
SELECT l.fireid , v."Type"
FROM work_lrg l 
INNER JOIN 
"VIIRS_Global_flaring_2012_2016" v 
ON l.geom_lrg && v.geom 
AND ST_Intersects(l.geom_lrg, v.geom);

-- vim: et sw=2
