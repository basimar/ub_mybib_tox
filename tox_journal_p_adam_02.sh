#!/bin/csh -f

# tox_journal_p_adam_02.sh 
# Shellscript fuer Steuerung des Abrufs der MyBIB-TOX-Journale und Verknuepfung mit p_adam_02
# HINWEISE: Achtung, Verzeichnispfade sind ggf. verbundspezifisch
# 1. Environment der library setzen
# 2. Folgende Verzeichnisse muessen vorhanden sein, oder umdefiniert werden:
#          $data_root/scripts/mybib_tox
#          $data_root/scripts/mybib_tox/done/
#          $data_root/scripts/mybib_tox/log
# 3. Verbund muss definiert werden ($verbund)
# 4. Verarbeitete XML-Dateien werden nach done/ verschoben. 
# 5. Das Script soll und kann nicht versehentlich zweimal hintereinander am selben Tag aufgerufen werden. 
# 6. Das Script bereinigt dublette Z403-Saetze und redundante 500, 505 u. 856 Kategorien nach dem Import
# 7. Das Script spielt zusatzliche Journale ein, falls vorhanden (nur an Tagen mit E-Doc-Journalen).
# 8. Eine Statistik aufgrund der z403-Tabelle wird erstellt.
# 9. Das Script prueft, ob Oracle laeuft und bricht sonst ab.
#
# History:
# 28.10.2014    Erstellt / blu
# 29.06.2018    Angepasst fÃr Virtualisierung / bmt

# --- Start Definitionen --------------------------------------------

# Datum
if ($#argv == 0) then
   set date = `date +'%Y%m%d'`
else
   if ($1 > 20090301) then
     set date = $1
     echo "KatAnreicherungslauf Datum: $date"
   else
     echo "usage: tox_journal_p_adam_02.sh"
     echo "With date param: csh -f tox_journal_p_adam_02.sh JJJJMMDD"
     echo "Date must be greater than 20090301. Can be a range JJJJMMDD-JJJJMMDD"
     exit
   endif
endif

# Environment der library setzen, mit der die ADAM-Objekt verknuepft sind
set source_par = "dsv01"; source $alephm_proc/set_lib_env; unset source_par;

# Datenverzeichis 
set work_dir = "$data_root/scripts/mybib_tox"

# Upload-Verzeichnis 
set upload_dir = "$data_root/import_files/mybib_tox"

# LOGDir
set LOG = "$work_dir/log/tox_journal_p_adam_02.log.$date"

# Verbund
set verbund = "IDSBB"
# set verbund = "IDSLUZ"

# EMail-Adresse(n) - mehrere Adressen Komma-Delimited
set email = "@unibas.ch"

# --- Ende Definitionen ---------------------------------------------

# Check if Oracle running
pgrep -fl ora_db.._aleph > /dev/null
if ( $status ) then
     echo 'Oracle not running, exiting...' >> $LOG
     mailx -s "KatAnreicherungslauf vom $date abgebrochen, Oracle not running" $email 
     exit
endif

# Zusaetzliches Journal aufbereiten an Tagen mit E-Doc-Journalen
# if ( `date +%a` != 'Sun' && `date +%a` != 'Mon' ) then
#   cd $add_jr_dir
#   if (-e `ls -1 tox_isbn_id.* | head -1`) then
#      set add_jr = `ls -1 tox_isbn_id.* | head -1`
#      cp $add_jr done/$add_jr.$date
#      mv $add_jr tox_isbn_id.dat
#      sqlldr dsv01/dsv01 control=load_z11_isbn
#      sqlplus dsv01/dsv01 @tox_isbn_abgleich_listen.sql
#      cp output/tox_isbn_liste.csv done/tox_isbn_liste.csv.$date
#      mv output/tox_isbn_liste.csv ../import_files/mybib_tox/tox_isbn_liste.csv
#      rm tox_isbn_id.dat
#   else
#      echo "Keine tox_isbn_id-Datei mehr vorhanden." >> $LOG
#   endif
# endif

echo "--------- tox_journal_p_adam_02-Lauf vom $date -----------------" >> $LOG
echo '' >> $LOG

# Pruefen, ob Lauf an diesem Tag schon erfolgt ist. 
# p_adam_02 legt andernfalls dieselben Verknuepfungen nochmals an.

if ( `cat $work_dir/log/date_last_run` == $date ) then
   echo "Achtung: tox_journal_p_adam_02.sh ist heute bereits gelaufen"
#   echo 'ACHTUNG: Datum-Check-Abbruch auskommentiert' >> $LOG
   echo "Wiederholung ist nicht sinnvoll. Bitte ggf. manuell weiter verfahren"
   echo "Attempt to run a second time the same date $date. Stopped." >> $LOG
   exit
endif

# Datum des Laufs ablegen
echo $date > $work_dir/log/date_last_run

# Ins Arbeitsverzeichnis wechseln
cd $work_dir

# Script zur Abholung und Konversion der Journale
perl tox_journal_2_z403.pl
# Zum Abholen eines best. Datums
# perl tox_journal_2_z403.pl $date
echo 'Fetch und XML-Konversion mit tox_journal_2_z403.pl gelaufen' >> $LOG
echo '' >> $LOG
# echo 'ACHTUNG: Abholung Duplicates auskommentiert' >> $LOG
perl tox_duplicates_2_z403.pl
# Zum Abholen eines best. Datums
#perl tox_duplicates_2_z403.pl $date
echo 'Fetch und XML-Konversion mit tox_duplicates_2_z403.pl gelaufen' >> $LOG
echo '' >> $LOG

# TOX-Journal
# Sicherstellen, dass es keine leere Datei ist
grep 'KEINE DATEN GELIEFERT' tox-journal-$verbund-PDF* > /dev/null
if ( $status ) then

   # Aktuelle XML-Datei kopieren f. ADAM-Job
   cp `ls -1rt tox-journal-$verbund-PDF* | tail -1` $upload_dir/mybib_tox.xml

   # Aktuelle tox.csv auswerten f. Bereinigungen
   awk -F';' '{print $9"DSV01"}' tox-journal-$verbund-*.csv | sed -e 's/\"//g' -e '/SYS/d' > $alephe_scratch/mybib_tox_$date.bib.log

   # ADAM-Lauf
   echo '-------------------------------------' >> $LOG
   echo 'ADAM-Lauf TOX-Journal ohne Indexierung' >> $LOG
   echo '' >> $LOG
   csh -f $aleph_proc/p_adam_02 DSV01,mybib_tox,mybib_tox_rejected,mybib_tox_$date.log,N,N,N,M,,,BATCH, > $alephe_scratch/dsv01_p_adam_02.tox.$date.log
   rm $upload_dir/mybib_tox.xml

else
   echo "TOX: KEINE DATEN GELIEFERT, Abbruch" >> $LOG
endif 


# EDoc-Journal
# Sicherstellen, dass Log-Datei vorhanden, denn edoc-duplicate-log wird nicht jeden Tag geschrieben
set mybib_dir = `ls -1rt`

foreach f ( echo $mybib_dir )
  if ($f =~ edoc*) then
     set ok = 1
  else set ok = 0
  endif
end


if ( $ok ) then 
   # Sicherstellen, dass es keine leere Datei ist
   grep 'KEINE DATEN GELIEFERT' edoc-journal-$verbund-PDF* > /dev/null
   if ( $status ) then

      # Aktuelle XML-Datei kopieren f. ADAM-Job
      cp `ls -1rt edoc-journal-$verbund-PDF* | tail -1` $upload_dir/mybib_tox.xml

      # Aktuelle edoc.csv auswerten f. Bereinigungen und anfuegen
      awk -F';' '{print $4"DSV01"}' edoc-journal-$verbund-*.csv | sed -e 's/\"//g' -e '/sys/d' >> $alephe_scratch/mybib_tox_$date.bib.log

      # ADAM-Lauf
      echo '---------------------------------------' >> $LOG
      echo 'ADAM-Lauf Dubletten PDF ohne Indexierung' >> $LOG
      echo '' >> $LOG
      csh -f $aleph_proc/p_adam_02 DSV01,mybib_tox,mybib_tox_rejected,mybib_edoc_pdf_$date.log,N,N,N,M,,,BATCH, > $alephe_scratch/dsv01_p_adam_02.edoc.pdf.$date.log
      rm $upload_dir/mybib_tox.xml

      # Aktuelle XML-Datei kopieren f. ADAM-Job
      if ( -e edoc-journal-$verbund-IND* ) then
         cp `ls -1rt edoc-journal-$verbund-IND* | tail -1` $upload_dir/mybib_tox.xml

         # ADAM-Lauf
         echo '---------------------------------------------' >> $LOG
         echo 'ADAM-Lauf Dubletten OCR (HBZ) ohne Indexierung' >> $LOG
         echo '' >> $LOG
         csh -f $aleph_proc/p_adam_02 DSV01,mybib_tox,mybib_tox_rejected,mybib_edoc_ocr_$date.log,N,N,N,M,,,BATCH, > $alephe_scratch/dsv01_p_adam_02.edoc.ocr.$date.log
         rm $upload_dir/mybib_tox.xml
      endif
   else
      echo "EDoc: KEINE DATEN GELIEFERT, Abbruch" >> $LOG
   endif

else
   echo "EDoc: Kein Log vom Vortag des $date vorhanden, Abbruch" >> $LOG
endif 

# Auswertung, Mail
# Ab SP 2153 werden keine Dokumentnummern in das Dokumentnummern-Log mehr geschrieben, 
# wenn keine neuen BIB-Datensaetze von p_adam_02 erzeugt werden. 
# Daher muss auf die Auszaehlung dieser Dokumentnummern-Logs verzichtet werden.
# Stattdessen werden die Zaehlungen der p_adam_02-Logs im Mail ausgegeben.
# Gesamtsummen muessen via z403 ermittelt werden.

echo '---------------------------------------------' >> $LOG
echo " " >> mail.txt
if ( -e $alephe_scratch/dsv01_p_adam_02.tox.$date.log ) then
   echo "Eigenscans:" >> mail.txt
   tail -2 ./log/tox-journal-$verbund.log >> mail.txt
   echo " " >> mail.txt
   echo "Job:" >> mail.txt
   grep 'Anzahl der' $alephe_scratch/dsv01_p_adam_02.tox.$date.log >> mail.txt
else
   echo "Keine TOX-Journaldaten vom Vortag des $date" >> mail.txt
endif
echo " " >> mail.txt
if ( -e $alephe_scratch/dsv01_p_adam_02.edoc.pdf.$date.log ) then
   echo "Dubletten:" >> mail.txt
   tail -2 ./log/edoc-journal-$verbund.log >> mail.txt
   echo " " >> mail.txt
   echo "Job:" >> mail.txt
   grep 'Anzahl der' $alephe_scratch/dsv01_p_adam_02.edoc.pdf.$date.log >> mail.txt
else
   echo "Kein Dubletten-Log vom Vortag des $date" >> mail.txt
endif
echo " " >> mail.txt

# Redundante Felder ermitteln und loeschen (global change)
if ( -e $alephe_scratch/mybib_tox_$date.bib.log ) then
   echo "500, 505, 856, 970 der Eigenscans bereinigen:" >> mail.txt
   csh -f $aleph_proc/p_manage_21 DSV01,mybib_tox_$date.bib.log,mybib_tox_500_$date.rep,Y,,500,,,\$\$aContains,Y,,,,,,,,,,,,,,,,N,,BLU, > $alephe_scratch/dsv01_p_manage_21_mybib_$date.log
   csh -f $aleph_proc/p_manage_21 DSV01,mybib_tox_$date.bib.log,mybib_tox_505_$date.rep,Y,,505,,,\$\$a,Y,,,,,,,,,,,,,,,,N,,BLU, >> $alephe_scratch/dsv01_p_manage_21_mybib_$date.log
   csh -f $aleph_proc/p_manage_21 DSV01,mybib_tox_$date.bib.log,mybib_tox_856t_$date.rep,Y,,856,,,\$\$zTable^of^contents,Y,,,,,,,,,,,,,,,,N,,BLU, >> $alephe_scratch/dsv01_p_manage_21_mybib_$date.log
   csh -f $aleph_proc/p_manage_21 DSV01,mybib_tox_$date.bib.log,mybib_tox_856i_$date.rep,Y,,856,,,\$\$zInhaltsverzeichnis,Y,,,,,,,,,,,,,,,,N,,BLU, >> $alephe_scratch/dsv01_p_manage_21_mybib_$date.log
   csh -f $aleph_proc/p_manage_21 DSV01,mybib_tox_$date.bib.log,mybib_tox_856d_$date.rep,Y,,856,,,\$\$uhttp://d-nb.info/\*/04\$\$zInhaltsverzeichnis,Y,,,,,,,,,,,,,,,,N,,BLU, >> $alephe_scratch/dsv01_p_manage_21_mybib_$date.log
   csh -f $aleph_proc/p_manage_21 DSV01,mybib_tox_$date.bib.log,mybib_tox_970_$date.rep,Y,,970,,,,Y,,,,,,,,,,,,,,,,N,,BLU, >> $alephe_scratch/dsv01_p_manage_21_mybib_$date.log
   set docs_berein = `grep doc-number $data_print/mybib_tox_*_$date.rep | wc -l`
   echo "Bereinigung 500, 505, 856, 970 der Eigenscans bei $docs_berein Dokumenten gelaufen" >> mail.txt
   echo " " >> mail.txt
endif

# Statistik
sqlplus dsv01/dsv01 @$dsv01_dev/dsv01/scripts/mybib_tox/z403_stat.sql

echo "Statistik:" >> mail.txt
cat $dsv01_dev/dsv01/scripts/mybib_tox/log/z403_statistik_aktuell.txt >> mail.txt
echo " " >> mail.txt

cat $LOG mail.txt | mailx -S sendcharsets=utf-8 -S ttycharset=utf-8 -s "KatAnreicherungslauf vom $date - done" $email 
echo 'Mail generiert' >> $LOG

# Aufraeumen
mv ./tox-journal-$verbund-* ./done/
mv ./edoc-journal-$verbund-* ./done/
rm ./mail.txt
# rm ./tox_isbn_liste.csv
rm ./edoc-journal.*

echo "--------- tox_journal_p_adam_02-Lauf vom $date Ende ---------------------" >> $LOG

exit 

