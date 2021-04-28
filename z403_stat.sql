/* Z403-Auswertung Kataloganreicherung */

SET PAUSE OFF;
SET ECHO OFF;
-- SET HEADING OFF;
SET FEEDBACK OFF;
SET PAGESIZE 60;
SET LINESIZE 48;
SET TERMOUT OFF;

SPOOL $dsv01_dev/dsv01/scripts/mybib_tox/log/z403_statistik_aktuell.txt

select count(*) "IHV-Gesamt" from z403 where (Z403_U_PATH like '%tox%' or Z403_U_PATH like '%d-nb%') and (Z403_U_PATH not like '%OCR');

select count(*) "IHV-IDSBB" from z403 where Z403_U_PATH like '%IDSBB%';

select count(*) "IHV-DNB" from z403 where Z403_U_PATH like '%d-nb%';

select count(*) "IHV-HBZ" from z403 where Z403_U_PATH like '%HBZ%' and Z403_U_PATH not like '%OCR';

select count(*) "IHV-ILU" from z403 where Z403_U_PATH like '%IDSLU%';

select count(*) "IHV-GBV" from z403 where Z403_U_PATH like '%GBV%' and Z403_U_PATH not like '%OCR';

select count(*) "IHV-SNB" from z403 where Z403_U_PATH like '%SNB%';

SPOOL off;

exit;
