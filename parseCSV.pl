use strict;
use ava::utf::ansi2utf;
use FileHandle;
no warnings "uninitialized";

# our $FS=',';    # CSV field separator
our $FS=';';    # CSV field separator
our $NL="\n";
our $CONVERT_TO_UTF8="0";   # if true convert from Windows-Latin1 to UTF8
our $utf;

=for debug
open(IN,"test.csv") or die $!;
while ( my @fields = parseCSV(*IN) ) {
    print "- number of field: ", $#fields + 1, "\n";
    foreach my $field ( @fields ) {
        print "- field: >>$field<<\n";
    }
    print "\n",'-' x 50, "\n";
}
=cut

if ( $CONVERT_TO_UTF8 ) {
    $utf = ava::utf::ansi2utf->new({charset => 'windows'});
}

sub parseCSV {
    # returns an array of CSV entries for one CSV record
    # based on code by Jeffrey E.F. Friedl
    # source: http://www.oreilly.com/catalog/regex/errata/regex.unconfirmed
    #
    # modified by Andres von Arx 26.04.2006:
    # - takes a file handle as argument
    # - handles newlines within fields
    # - configurable $FS, $NL
    # - does optional character set conversion

    my $FH = $_[0];
    my @fields = (); # initialize to null

    my $str = <$FH>;
    $str =~ s/\r//g;
    chomp $str;
    return @fields unless ( $str );
    $str=$utf->convert($str) if ( $CONVERT_TO_UTF8 );
    my $rest='';
    my $orig_str=$str;
    until ( $str eq '' ) {
        my $thisField='';
        my $nextline='';
        if ( $str =~ m{^"([^"$FS]*)"(|$FS(.*))$}so ) {
            # -- text field
            $thisField = $1;
            $str=$3;
            $rest=$2;
        }
        elsif ( $str =~ m{^([^"$FS]*)(|$FS(.*))$}so ) {
            # numeric field
            $thisField = $1;
            $str=$3;
            $rest=$2;
        }
        elsif ( $str =~ m{^"(.*)$}s ) {
            # there is a leading "
            $str=$1;
            # get all "" in remainder of this field
            while( $str =~ m{^([^"]*")"(.*)$}s ) {
                $thisField .= $1;
                $str=$2;
            };
            # hopefully we are at a [^,"]*", or " at end of line
            if( $str =~ m{([^"]*)"(|$FS(.*))$}so ) {
                $thisField .= $1;
                $str=$3;
                $rest=$2;
            }
            else {
                # join next line to input
                $nextline = <$FH>;
                $nextline = $utf->convert($nextline) if ( $CONVERT_TO_UTF8 );
                if ( $nextline ) {
                    $nextline =~ s/\r//;
                    chomp $nextline;
                    $str = $orig_str .$NL .$nextline;
                }
                else {
                    warn "Could not find a \" following >$thisField< in\n>$orig_str<";
                    $thisField .= $str;
                    $str='';
                }
            };
        }
        else {
            warn "Could not match >$str< in\n>$orig_str<";
            $str = '';
        };
        if ( $nextline ) {
            # restart parsing
            @fields=();
        }
        else {
            push( @fields, $thisField );
        }
    }
    push( @fields, undef) if $rest =~ m/$FS$/o; #account for an empty last field
    return @fields;
} # end parseCSV

1;
