#!/usr/bin/perl

# Abholen und Konversion von MyBIB-EDoc-Logdaten zu z403-xml fuer ADAM-Import
# Input: .csv-Datei (UTF-8, Semikolon-delimited, "" als Feldtrenner)
# Output: 2 xml-files im Ladeformat fuer p_adam_02-Job

use strict;
use DBI;
use Data::Dumper;
use POSIX qw(strftime);
use LWP::UserAgent;

require 'parseCSV.pl';

# Aufruf mit Datum oder help
my $date;
if ( @ARGV ) {
   $date = substr($ARGV[0],0,4) . "-" . substr($ARGV[0],4,2) . "-" . substr($ARGV[0],6,4);
   unless ( $date =~ '(\d{4}-\d{2}-\d{2})') {
      print<<EOD;
      tox_duplicates_2_z403.pl:
      Abholen und Konversion von MyBIB-EDoc-Logdaten der auf MyBIB-TOX gefundenen Dubletten 
      zu z403-xml fuer ADAM-Import.
      - Ohne Parameter: Logdaten vom Vortag werden geholt.
      - Mit Datumsparameter im Format JJJJ-MM-TT wird Log vom JJJJ-MM-TT geholt.
      - mit allen anderen Parametern: Dieser Text.
      (c) UB Basel, Bernd Luchner
EOD
      exit;
   }
}

# ---------------------------------------------
# -- Start Konfiguration
# ---------------------------------------------

# -- Verbundeinstellungen
my $verbund = 'IDSBB';
# my $verbund = 'IDSLUZ';
my $u_path = $ENV{dsv01_dev};
my $wrk_dir = "$u_path/dsv01/scripts/mybib_tox";
my $log_dir = "$wrk_dir/log"; 
# -- Ende Verbundeinstellungen

# Datum im JJJJ-MM-TT-Format uebergeben oder vom Vortag;
( $date ) || ( $date = strftime("%Y-%m-%d", localtime(time - 86400)));

my $edoc_csv = "$wrk_dir/edoc-journal-$verbund-$date.csv";
my $add_jr = "$wrk_dir/tox_isbn_liste.csv";
my $xml_pdf = "$wrk_dir/edoc-journal-$verbund-PDF-$date.xml";
my $xml_ind = "$wrk_dir/edoc-journal-$verbund-IND-$date.xml";
my $edoc_log = "$log_dir/edoc-journal-$verbund.log";

my $DEBUG = 0;  # falls 1: Ausgabe im Debug-Format
                # falls 0: Ausgabe im XML Format

# Z403-Zugriffseinstellungen IDSBB, da im Dublettenlog ca. 15% der Eintraege zu bereits vorhandenen IHV kommen.
# Das erklaert sich aus den Parallelanschaffungen Basel Bern.

$ENV{ORACLE_SID} or die 'ORACLE_SID ???';
$ENV{ORACLE_HOME} or die 'ORACLE_HOME ???';
my $dbh = DBI->connect('dbi:Oracle:', 'DSV01', 'DSV01')
    or die "$DBI::errstr\n";

my $sql_find_toc = "
    SELECT Z403_U_PATH
    FROM DSV01.Z403
    WHERE Z403_U_PATH = ?
    ";

my $sth_find_toc = $dbh->prepare($sql_find_toc)
    or die $dbh->errstr;

END {
    ( $sth_find_toc ) and $sth_find_toc->finish;
    ( $dbh ) and $dbh->disconnect;
}


# ---------------------------------------------
# -- Ende Konfiguration
# ---------------------------------------------

open(LOG,">>$edoc_log") or die("cannot write $edoc_log: $!");

# -----------------------------------------------
# -- Journaldatei abholen
# -----------------------------------------------

get_journal(qq|http://ubmybibcat01.unibe.ch/tox-duplicates/$verbund/ISBN_duplicates_$date.csv|);

# -----------------------------------------------
# -- Spaltendefinition Dublettendatei
# -----------------------------------------------

my $def = <<'EOD';
A Date
B Institution
C Barcode
D SYS-ID
E ISBN
F TOX_URL_COUNT
G TOX_URL
EOD

my @spalten;                          # Liste der Spaltencodes A ..Q 
my %spalten_kommentar;                # Kommentar zu den Spalten A ..Q 
my $in;                               # Spaltenhandle
my ($title, $ext, $url_ext, $u_path, $type, $display);  # Platzhalter fuer XML-Struktur

my @tmp = split(/\n/,$def);
while ( @tmp ) {
    $_ = shift @tmp;
    s/^(\w+)\s+//;
    my $spalten_code = $1;
    push(@spalten,$spalten_code);
    $spalten_kommentar{$spalten_code}=$_;
}

# Additional Journal appenden:
open(ADD,"<$add_jr") or print "No additional journal today\n";
open(IN,">>$edoc_csv") or die("cannot open $edoc_csv for append: $!"); 

my $add_recno = 0;
while ( <ADD> ) {
    print IN $_;
    $add_recno++;
}
close ADD;
close IN;

# EDoc-Journal sortieren ohne Dubletten
my $status_sed = `sed /datetime/d $edoc_csv > $wrk_dir/edoc-journal.tmp`;
my $status_sort = `sort -t";" -k4 -u $wrk_dir/edoc-journal.tmp > $wrk_dir/edoc-journal.srt`;
my $status_cat = `cat $wrk_dir/header_edoc_journal $wrk_dir/edoc-journal.srt > $edoc_csv`;

open(IN,"<$edoc_csv") or die("cannot read $edoc_csv: $!");
open(OUTPDF,">$xml_pdf") or die("cannot write $xml_pdf: $!");
open(OUTIND,">$xml_ind") or die("cannot write $xml_ind: $!");

my $recno = 0;                        # Satznummer verarbeitet
my @fields = parseCSV(*IN);           # Feldbeschreibung

unless ( $DEBUG ) {
    print OUTPDF sub_header();
    print OUTIND sub_header();
}

while ( @fields = parseCSV(*IN) ) {
    foreach my $spalte ( @spalten ) {
        $in->{$spalte} = shift @fields;
    }
    $recno++;

# ------------------------------------------
# -- Konsistenzpruefung
# ------------------------------------------
    if ( ! $in->{D} ) {
        die "Sys-Nummer fehlt in Satz $recno";
    }

    # Debug
    # print "Satz Nr. $recno: $in->{D}, $in->{F}, $in->{G}\n";

# -----------------------------------------
# -- Ausgabe
# ------------------------------------------

    if ( $DEBUG ) {
        print OUTPDF qq|*$recno*\n|;
        print OUTIND qq|*$recno*\n|;
    }
    else {
        next if ( $in->{G} =~ /$verbund/ );                         # Verbunddubletten ausschliessen

        my @urls = split(/\|/,$in->{G});                            # Pruefung auf vorhandenes IHV
        $_ = shift @urls;
        my $toc_path;
        if ( $in->{F} > 1 && $_ =~ /HBZ/ ) {           
           $_ = shift @urls;
        }
        if ( $_ =~ /DNB/ ) {
           s/tox.imageware.de\/tox\/service\/DNB\/(.*)/d-nb.info\/$1\/04/;
           $toc_path= $_;
        }
        else { 
           s/tox.imageware.de\/tox\/service/www.ub.unibas.ch\/tox/;
           $toc_path = $_ . '/PDF';
        }

# Pruefung auf schon vorhandenes IHV
        $sth_find_toc->bind_param(1, $toc_path)
             or die $dbh->errstr;
        $sth_find_toc->execute
             or die $dbh->errstr;
        my $z403_u_path;
        my $found = 0;
        while ( my $arec = $sth_find_toc->fetchrow_arrayref ) {
           $z403_u_path = trim($arec->[0]);
           # print "Z403_U_PATH: $z403_u_path\n";                    # Debug
           if ( $z403_u_path eq $toc_path ) {
              $found = 1;
              # print "TOC vorhanden: $toc_path\n";
           };
        };
        next if ( $found );

        print OUTPDF sub_record('VIEW');
#           print "PDF-Kandidat: $in->{D}, $in->{F}, $in->{G}\n";   # Debug
        if ( $in->{F} == 1 && $in->{G} =~ /HBZ/ ) {
#           print "IND-Kandidat: $in->{D}, $in->{F}, $in->{G}\n";   # Debug
           print OUTIND sub_record('INDEX');                # hier noetig, da PDF von HBZ z.T. nicht durchsuchbar
        }
    }
}                                                           # Ende der Satzverarbeitung

unless ( $recno > 0 ) {
    print OUTPDF "KEINE DATEN GELIEFERT\n";
    print OUTIND "KEINE DATEN GELIEFERT\n";
}

unless ( $DEBUG ) {
    print OUTPDF sub_tail();
    print OUTIND sub_tail();
}

# -----------------------------------------
# -- Statistik, Ende
# -----------------------------------------

$recno = $recno - $add_recno;

print LOG "--------------------------------------------------------\n";
print LOG "MyBIB-EDoc-Journal vom $date: $recno Saetze und $add_recno Saetze add. Journal verarbeitet\n";

close IN;
close LOG;
close OUTPDF;
close OUTIND;

sub get_journal {
my $ua = LWP::UserAgent->new;
my $req = new HTTP::Request GET => @_;
my $res = $ua->request($req);

my $datestamp = `date '+%Y-%m-%d %H:%M:%S'`;
chomp($datestamp);

  if ($res->is_success) {
        open (JOURNAL,">$edoc_csv") or die ("cannot write journal file $edoc_csv: $!\n");
        print JOURNAL $res->content;
        close JOURNAL;
  }
  else {
        print LOG $datestamp." [err] get_journal: Can't fetch file $edoc_csv, stopped\n";
        die ("Stopped because no edoc-duplicate-log download available for $date, see $edoc_log\n");
  }
}

sub sub_header {
  my $return=<<END_HEADER;
<?xml version="1.0" encoding="UTF-8"?>
<file>

END_HEADER
}

sub sub_record {
     $type = shift;
  if ( $type eq 'VIEW' ) {
     $title = 'Inhaltsverzeichnis';
     $ext = 'pdf';
     $url_ext = '/PDF';
     $display = 'Y';
  }
  elsif ( $type eq 'INDEX' ) {
     $title = 'Indexdatei'; 
     $ext = 'text';
     $url_ext = '/OCR';
     $display = 'N';
  }
  else {
     die ("Internal error: $!\n");
  }

my @urls = split(/\|/,$in->{G});
$_ = shift @urls;
# Wenn mehr als eine Dublette, dann lieber ein durchsuchbares PDF
if ( $in->{F} > 1 && $_ =~ /HBZ/ ) {           
   $_ = shift @urls;
}
# DNB-Digitalisate werden direkt mit d-nb.info verknuepft, TOX-Zugriff nicht erlaubt
# print "Alter Pfad: $_\n";                    # Debug
if ( $_ =~ /DNB/ ) {
   s/tox.imageware.de\/tox\/service\/DNB\/(.*)/d-nb.info\/$1\/04/;
   $url_ext = '';
}
else { 
   s/tox.imageware.de\/tox\/service/www.ub.unibas.ch\/tox/;
}

$u_path = $_;
# print "Neuer PDF Pfad: $u_path$url_ext\n";   # Debug

my $return=<<END_RECORD;
<record xmlns="http://www.loc.gov/MARC21/slim/" 
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xsi:schemaLocation="http://www.loc.gov/MARC21/slim 
http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
  <controlfield tag="SYS">$in->{D}</controlfield>
</record>

<z403>
  <z403-doc-number>000000000</z403-doc-number>
  <z403-sequence>000000</z403-sequence>
  <z403-derived-from-sequence>000000</z403-derived-from-sequence>
  <z403-title>$title</z403-title>
  <z403-f-directory></z403-f-directory>
  <z403-f-filename></z403-f-filename>
  <z403-original-file-name></z403-original-file-name>
  <z403-object-extension>$ext</z403-object-extension>
  <z403-object-size></z403-object-size>
  <z403-u-path>$u_path$url_ext</z403-u-path>
  <z403-usage-type>$type</z403-usage-type>
  <z403-sub-library>$in->{B}</z403-sub-library>
  <z403-note-1></z403-note-1>
  <z403-note-2></z403-note-2>
  <z403-note-3></z403-note-3>
  <z403-note-4></z403-note-4>
  <z403-note-5></z403-note-5>
  <z403-open-date></z403-open-date>
  <z403-update-date></z403-update-date>
  <z403-cataloger>BATCH</z403-cataloger>
  <z403-character-set></z403-character-set>
  <z403-color-setting></z403-color-setting>
  <z403-resolution></z403-resolution>
  <z403-dimensions></z403-dimensions>
  <z403-compression-ratio></z403-compression-ratio>
  <z403-creation-date></z403-creation-date>
  <z403-creation-hardware></z403-creation-hardware>
  <z403-creation-software></z403-creation-software>
  <z403-copyright-contact></z403-copyright-contact>
  <z403-copyright-owner></z403-copyright-owner>
  <z403-copyright-type></z403-copyright-type>
  <z403-copyright-note></z403-copyright-note>
  <z403-copyright-notice></z403-copyright-notice>
  <z403-copyright-notice-type></z403-copyright-notice-type>
  <z403-display-link>$display</z403-display-link>
  <z403-display-code></z403-display-code>
  <z403-expiry-date></z403-expiry-date>
  <z403-guest>Y</z403-guest>
  <z403-ip-address></z403-ip-address>
  <z403-course></z403-course>
  <z403-user-status></z403-user-status>
  <z403-restrict-sub-library></z403-restrict-sub-library>
  <z403-no-of-copies></z403-no-of-copies>
  <z403-view-time></z403-view-time>
  <z403-item-library></z403-item-library>
  <z403-item-doc-number></z403-item-doc-number>
  <z403-item-sequence></z403-item-sequence>
  <z403-enumeration-a></z403-enumeration-a>
  <z403-enumeration-b></z403-enumeration-b>
  <z403-enumeration-c></z403-enumeration-c>
  <z403-chronological-i></z403-chronological-i>
  <z403-chronological-j></z403-chronological-j>
  <z403-pid></z403-pid>
</z403>

END_RECORD
}

sub sub_tail {
  my $return=<<END_TAIL;

</file>

END_TAIL
}

sub trim {
    local $_ = shift or return undef;
    s/^\s+//;
    s/\s+$//;
    $_;
}

__END__

=head1 NAME

tox_duplicates_2_z403.pl - Perl Script to fetch and convert ImageWare MyBIB-EDoc log files into z403-xml structure for load with ADAM-Job p_adam_02.

=head1 SYNOPSIS

=over

=item

  perl tox_duplicates_2_z403.pl            # With no parameters the log file from yesterday will be fetched.

=item

  perl tox_duplicates_2_z403.pl JJJJ-MM-TT # The log file from JJJJ-MM-TT will be fetched

=back

=head1 INSTALLATION

The Perl-Script parseCSV.pl is required with configuration of semicolon as field separator and must be installed in the same directory (or be available through $path).

=head1 DESCRIPTION

The log files about found duplicates on MyBIB-TOX are written each day on MyBIB-EDoc. These logs contain the URLs of the duplicates on MyBIB-TOX. 

The script computes yesterday's date and looks for a corresponding log from the MyBIB EDoc-server ubmybib01.unibe.ch/tox-duplicates/<Verbund>/.

It fetches the log if there. If not, an error message and a logfile entry are written. No further files are created.

If the log file is there but without data, the output files contain the string 'KEINE DATEN GELIEFERT'.

A normal log file with data is converted to the z403-xml structure required by the ADAM load job p_adam_02.
Because the pdf files of the duplicates might not be searchable two xml files are created: one for the pdf link and one for the ocr data for indexing.

Therefore p_adam_02 must be run twice: once with option 'Index: no' and the second with option 'Index: yes'.

You might integrate this script into a shell script which is invoked by the ALEPH job list and which starts the ADAM jobs if necessary.

=head1 INSTALLATION

The Perl-Script parseCSV.pl is required and must be installed in the same directory (or be available through $path)

=head2 KONFIGURATION

Two configurations must be done in the script:

=over

=item 1.

The directory of the library on the MyBIB EDoc-Server must be configured ($verbund)

=item 2.

The working and logging directory on the ALEPH server must be specified ($wrk_dir, $log_dir)

=back

=head1 STRUCTURE OF THE DUPLICATES LOG FILE

The log file is a semicolon delimited .csv file with the following structure:

  A Date
  B Institution
  C Barcode
  D SYS-ID
  E ISBN
  F TOX_URL_COUNT
  G TOX_URL


=head1 AUTHOR

Bernd Luchner

=head1 COPYRIGHT AND LICENCE

Copyright (C) 2009 by Basel University Library

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself, either Perl version 5.8.8 or, at your option, any later version of Perl 5 you may have available.

=head1 VERSION

Version 1.0,  13.03.2009

Version 1.2,  23.03.2009: Verbunddubletten ausschliessen

Version 1.3,  26.03.2009: DNB-Links werden direkt auf d-nb.info gesetzt


=cut

