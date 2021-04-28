#!/usr/bin/perl

# Abholen und Konversion von MyBIB-TOX-Jornaldaten zu z403-xml fuer ADAM-Import
# Input: .csv-Datei (UTF-8, Semikolon-delimited, "" als Feldtrenner)
# Output: tox_journal.xml. Ladeformat fuer p_adam_02-Job

use strict;
use Data::Dumper;
use POSIX qw(strftime);
use LWP::UserAgent;

require 'parseCSV.pl';

# Aufruf mit Datum oder help
my $date;
if ( @ARGV ) {
   if ( $ARGV[0] =~ '(\d{8})' && ( $1 > 20090301 )) {
      $date = $ARGV[0];
   }
   else {
      print<<EOD;
      tox_journal_2_z403.pl:
      Abholen und Konversion von MyBIB-TOX-Journaldaten zu z403-xml fuer ADAM-Import.
      - Ohne Parameter: Journal vom Vortag wird geholt.
      - Mit Datumsparameter im Format JJJJMMTT (ab 20090301) wird Journal vom JJJJMMTT geholt.
      - Mit Datumsparameter im Format JJJJMMTT-JJJJMMTT (bei Auslieferungsproblemen).
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
# my $verbund = 'IDSLU';
my $u_path = $ENV{dsv01_dev};
my $wrk_dir = "$u_path/dsv01/scripts/mybib_tox";
my $log_dir = "$wrk_dir/log"; 
# -- Ende Verbundeinstellungen

# Datum im JJJJMMTT-Format uebergeben oder vom Vortag;
( $date ) || ( $date = strftime("%Y%m%d", localtime(time - 86400)));

my $tox_csv = "$wrk_dir/tox-journal-$verbund-$date.csv";
my $xml_pdf = "$wrk_dir/tox-journal-$verbund-PDF-$date.xml";
# my $xml_ind = "$wrk_dir/tox-journal-$verbund-IND-$date.xml";
my $tox_log = "$log_dir/tox-journal-$verbund.log";

my $DEBUG = 0;  # falls 1: Ausgabe im Debug-Format
                # falls 0: Ausgabe im XML Format

# ---------------------------------------------
# -- Ende Konfiguration
# ---------------------------------------------

open(LOG,">>$tox_log") or die("cannot write $tox_log: $!");

# -----------------------------------------------
# -- Journaldatei abholen
# -----------------------------------------------

get_journal(qq|http://tox.imageware.de/tox-journal/$verbund/$verbund/tox-journal-mono-ihv-$verbund-$date.csv|);

# -----------------------------------------------
# -- Spaltendefinition Journaldatei
# -----------------------------------------------

my $def = <<'EOD';
A Auftrag
B NBK
C Complaint
D Redelivery
E Anz.Seiten
F BIB
G Barcode
H Signatur
I SYS-ID
J Titel
K Sprache
L ISBN
M ISSN
N SERVICES
O MTIFF
P PDF
Q OCR
EOD

my @spalten;                          # Liste der Spaltencodes A ..Q 
my %spalten_kommentar;                # Kommentar zu den Spalten A ..Q 
my $in;                               # Spaltenhandle
my ($title, $ext, $u_path, $type, $display);  # Platzhalter fuer XML-Struktur

my @tmp = split(/\n/,$def);
while ( @tmp ) {
    $_ = shift @tmp;
    s/^(\w+)\s+//;
    my $spalten_code = $1;
    push(@spalten,$spalten_code);
    $spalten_kommentar{$spalten_code}=$_;
}

open(IN,"<$tox_csv") or die("cannot read $tox_csv: $!");
open(OUTPDF,">$xml_pdf") or die("cannot write $xml_pdf: $!");
# open(OUTIND,">$xml_ind") or die("cannot write $xml_ind: $!");

my $recno = 0;                        # Satznummer verarbeitet
my @fields = parseCSV(*IN);           # Feldbeschreibung

unless ( $DEBUG ) {
    print OUTPDF sub_header();
#    print OUTIND sub_header();
}

while ( @fields = parseCSV(*IN) ) {
    $recno++;
    foreach my $spalte ( @spalten ) {
        $in->{$spalte} = shift @fields;
    }

# ------------------------------------------
# -- Konsistenzpruefung
# ------------------------------------------
    if ( ! $in->{I} ) {
        die "Sys-Nummer fehlt in Satz $recno";
    }

    # Debug
    #   print LOG "Satz Nr. $recno: $in->{I}, $in->{F}, $in->{P}\n";

# -----------------------------------------
# -- Ausgabe
# ------------------------------------------

    if ( $DEBUG ) {
        print OUTPDF qq|*$recno*\n|;
#        print OUTIND qq|*$recno*\n|;
    }
    else {
        print OUTPDF sub_record('VIEW');
#        print OUTIND sub_record('INDEX');     # 2. XML-Satz, momentan nicht noetig 
    }
}                                              # Ende der Satzverarbeitung

unless ( $recno > 0 ) {
    print OUTPDF "KEINE DATEN GELIEFERT\n";
#    print OUTIND "KEINE DATEN GELIEFERT\n";
}

unless ( $DEBUG ) {
    print OUTPDF sub_tail();
#    print OUTIND sub_tail();
}

# -----------------------------------------
# -- Statistik, Ende
# -----------------------------------------

print LOG "--------------------------------------------------------\n";
print LOG "MyBIB-TOX-Journal vom $date: $recno Saetze verarbeitet\n";

close IN;
close LOG;
close OUTPDF;
# close OUTIND;

sub get_journal {
my $ua = LWP::UserAgent->new;
my $req = new HTTP::Request GET => @_;
my $res = $ua->request($req);

my $datestamp = `date '+%Y-%m-%d %H:%M:%S'`;
chomp($datestamp);

  if ($res->is_success) {
        open (JOURNAL,">$tox_csv") or die ("cannot write journal file $tox_csv: $!\n");
        print JOURNAL $res->content;
        close JOURNAL;
  }
  else {
        print LOG $datestamp." [err] get_journal: Can't fetch file $tox_csv, stopped\n";
        die ("Stopped because no tox-journal download available for $date, see $tox_log\n");
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
     $_ = $in->{P};
     s/tox.imageware.de\/tox\/service\/$verbund/www.ub.unibas.ch\/tox\/$verbund/;
     $u_path = $_;
#     print "Neuer Pfad: $u_path\n";   # Debug
     $display = 'Y';
  }
  elsif ( $type eq 'INDEX' ) {
     $title = 'Indexdatei'; 
     $ext = 'txt';
     $u_path = $in->{Q};
     $display = '';
  }
  else {
     die ("Internal error: $!\n");
  }

  my $return=<<END_RECORD;
<record xmlns="http://www.loc.gov/MARC21/slim/" 
xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
xsi:schemaLocation="http://www.loc.gov/MARC21/slim 
http://www.loc.gov/standards/marcxml/schema/MARC21slim.xsd">
  <controlfield tag="SYS">$in->{I}</controlfield>
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
  <z403-u-path>$u_path</z403-u-path>
  <z403-usage-type>$type</z403-usage-type>
  <z403-sub-library>$in->{F}</z403-sub-library>
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

__END__

=head1 NAME

tox_journal_2_z403.pl - Perl Script to fetch and convert ImageWare MyBIB-TOX journal files into z403-xml structure for load with ADAM-Job p_adam_02.

=head1 SYNOPSIS

=over

=item

  perl tox_journal_2_z403.pl          # With no parameters the journal file from yesterday will be fetched.

=item

  perl tox_journal_2_z403.pl JJJJMMTT # The journal file from JJJJMMTT will be fetched

=back

=head1 INSTALLATION

The Perl-Script parseCSV.pl is required with configuration of semicolon as field separator and must be installed in the same directory (or be available through $path).

=head1 DESCRIPTION

The journals of a day's scan work that has been transferred to MyBIB-TOX are generated by ImageWare short after midnight the next day.

The script computes yesterday's date and looks for a corresponding journal from the TOX server tox.imageware.de/tox-journal/<Verbund>/<Verbund>/.

It fetches the journal if there. If not, an error message and a logfile entry are written. No further files are created.

If the journal file is there but without data, the output files contain the string 'KEINE DATEN GELIEFERT'.

A normal journal file with data is converted to the z403-xml structure required by the ADAM load job p_adam_02.

The URL of the digital objects ($u_path) has to be converted to a reverse proxy URL running at UB Basel, because the ImageWare repository is not open to the world.

Currently one xml file is created as input for p_adam_02. p_adam_02 must be run with option "Index: yes".

You might integrate this script into a shell script which is invoked by the ALEPH job list and which starts the ADAM job if necessary.

=head1 INSTALLATION

The Perl-Script parseCSV.pl is required and must be installed in the same directory (or be available through $path)

=head2 KONFIGURATION

Two configurations must be done in the script:

=over

=item 1.

The directory of the library on the TOX-Server must be configured ($verbund)

=item 2.

The working and logging directory on the ALEPH server must be specified ($wrk_dir, $log_dir)

=back

=head1 STRUCTURE OF THE JOURNAL FILE

The TOX jornal file is a semicolon delimited .csv file with the following structure:

  A Auftrag
  B NBK
  C Complaint
  D Redelivery
  E Anz.Seiten
  F BIB
  G Barcode
  H Signatur
  I SYS-ID
  J Titel
  K Sprache
  L ISBN
  M ISSN
  N SERVICES
  O MTIFF
  P PDF
  Q OCR


=head1 AUTHOR

Bernd Luchner

=head1 COPYRIGHT AND LICENCE

Copyright (C) 2009 by Basel University Library

This library is free software; you can redistribute it and/or modify it under the same terms as Perl itself, either Perl version 5.8.8 or, at your option, any later version of Perl 5 you may have available.

=head1 VERSION

Version 1.0, 03.03.2009

Version 1.1, 04.03.2009

Version 1.2, 06.03.2009, date as parameter and minor changes

Version 1.3, 11.03.2009, change of original URL to reverse proxy URL


=cut

