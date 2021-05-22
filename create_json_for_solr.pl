#!/usr/bin/env perl
#                 -*- mode: cperl -*-
#
# usage: bin/create_json_for_solr.pl globaldothealth_2021-03-22.csv output_prefix
#
# will write files to output-prefix-01.json.gz output-prefix-02.json.gz AND output-prefix-ac-01.json.gz etc
#
# it will do both the main index and autocomplete
#
#
# writes an error log to output-prefix.log
#
# option:
#   --chunk-size          # how many docs per main output file chunk (autocomplete will have 5x this)
#   --nounique            # will switch off the unique-ification of the autocomplete Solr dump
#

use strict;
use warnings;
use feature 'switch';
use lib 'lib';
use Getopt::Long;
use JSON;
use DateTime::Format::ISO8601;
use DateTime;
use DateTime::EpiWeek;
use Geohash;
use Tie::IxHash;
use Scalar::Util qw(looks_like_number);
use List::MoreUtils;
use utf8::all;
use IO::Compress::Gzip;
use Text::CSV_XS;
use Memoize;

my $dry_run;
my $chunk_size = 2000000;

GetOptions("dry-run|dryrun"=>\$dry_run,
	   "chunk_size|chunksize=i"=>\$chunk_size, # number of docs in each output chunk
	  );


my ($input_file, $output_prefix) = @ARGV;

die "must provide output prefix commandline arg\n" unless ($output_prefix);

# for speed
memoize('date_fields');
memoize('geo_coords_fields');

# configuration for autocomplxete
my $ac_config =
  {
   covid_case =>
   {
    sex_s =>                        { type => "Sex" },
    geo_resolution_s =>             { type => "Geographic resolution" },
    # new to add to UI
    case_confirmation_s =>          { type => "Case confirmation" },
    occupation_s =>                 { type => "Occupation" },
    outcome_s =>                    { type => "Outcome" },
   }
  };


my $ac_chunk_size = $chunk_size * scalar keys %{$ac_config->{covid_case}};

my $log_filename = "$output_prefix.log";
my $log_size = 0;

my ($document_counter, $ac_document_counter, $chunk_counter, $ac_chunk_counter) = (0, 0, 0, 0);
my ($chunk_fh, $ac_chunk_fh);

my $json = JSON->new->pretty; # useful for debugging
my $gh = Geohash->new();
my $done;
my ($needcomma, $ac_needcomma) = (0, 0);

my $iso8601 = DateTime::Format::ISO8601->new;

$done = 0;

### FIRST PASS to do some standardisation, automated value mapping ###

my $csv = Text::CSV_XS->new ({ binary => 1, auto_diag => 1 });
open my $fh, "<:encoding(utf8)", $input_file or die "$input_file: $!";
my $headers = $csv->getline($fh);

my $h2i = {}; # header to column index
for (my $i=0; $i<@$headers; $i++) {
  $h2i->{$headers->[$i]} = $i;
}
map { print "$_\n" } @$headers;


my %values;
my %orig_age_counts;

while (my $row = $csv->getline ($fh)) {
  my $age = get_age($row, $h2i);
  $orig_age_counts{$age}++;
}
close $fh;


my $n = scalar keys %values;
print "$n unique values\n";
my @sorted = sort { $values{$b} <=> $values{$a} } keys %values;
splice @sorted, 1000 if (@sorted > 1000);
map { printf "%-7d %s\n", $values{$_}, $_ } @sorted;


my @sorted_ages = sort { $orig_age_counts{$b} <=> $orig_age_counts{$a} } keys %orig_age_counts;
my %range_min_max; # 1-4 => [ 1, 4 ]
my %range_counts;  # 1-4 => count
my $age2range = ohr(); # age string => range string

foreach my $age (@sorted_ages) {
  next unless ($age =~ /\d/); # skip empty value

  my ($min, $max) = ($age, $age);
  if ($age =~ /^(\d+)\s*-\s*(\d+)$/) {
    ($min, $max) = ($1, $2);
  }
  # convert 85+ to 85-120
  if ($age =~ /^(\d+)[+-]$/) {
    ($min, $max) = ($1, 120);
  }
  # convert any 'months' age to 0-2
  if ($age =~ /month|week/) {
    ($min, $max) = (0, 2);
  }

  foreach my $rangekey (sort { $range_counts{$b} <=> $range_counts{$a} } keys %range_counts) {
    my $range = $range_min_max{$rangekey};
    if ($min >= $range->[0] &&
        $min <= $range->[1] &&
        $max >= $range->[0] &&
        $max <= $range->[1]) {
      $age2range->{$age} = $rangekey;
      $range_counts{$rangekey} += $orig_age_counts{$age};
      last;
    }
  }
  # if we couldn't fit the current age into an existing range
  # make a new range
  unless ($age2range->{$age}) {
    my $rangekey = $age2range->{$age} = "$min-$max";
    $range_min_max{$rangekey} = [$min, $max];
    $range_counts{$rangekey} += $orig_age_counts{$age};
  }
}

#print "AGE MAPPING:\n";
#map { printf "%10s -> %s\n", $_, $age2range->{$_} } keys %{$age2range};

print "FINAL AGE BINS:\n";
map { printf "%-20s %d\n", $_, $range_counts{$_} } sort { $range_counts{$b} <=> $range_counts{$a} } keys %range_counts;


### SECOND PASS FOR REAL ###


open $fh, "<:encoding(utf8)", $input_file or die "$input_file: $!";
my $dont_need = $csv->getline($fh);


while (my $row = $csv->getline ($fh)) {

 #  my $val = $row->[$h2i->{age}]++;
  my $id = $row->[$h2i->{_id}];
  my $age = get_age($row, $h2i);
  my $sex = $row->[$h2i->{'demographics.gender'}];
  my $geo_resolution = $row->[$h2i->{'location.geoResolution'}];
  my $latitude = $row->[$h2i->{'location.geometry.latitude'}];
  my $longitude = $row->[$h2i->{'location.geometry.longitude'}];
  my $latlong = defined $latitude && defined $longitude && length($latitude) && length($longitude) ? "$latitude,$longitude" : undef;

  my $outcome = $row->[$h2i->{'events.outcome.value'}];
  my $case_confirmation_type = $row->[$h2i->{'events.confirmed.value'}];
  my $date = $row->[$h2i->{'events.confirmed.date'}] || undef;

  my $occupation = $row->[$h2i->{'demographics.occupation'}];

  my $document = ohr(
                     label => $id,
                     id => $id,
                     accession => $id,
                     bundle => 'covid_case',
                     bundle_name => 'Covid-19 case',
                     #	  	    site => 'Population Biology',
                     #		    url => '/popbio/sample/?id='.$stable_id,

                     age_orig_s => $age || 'no data',
                     age_ranges_s => $age2range->{$age} // 'no data',
                     sex_s => $sex || 'no data',
                     geo_resolution_s => $geo_resolution || 'no data',
                     case_confirmation_s => $case_confirmation_type || 'no data',
		     occupation_s => $occupation || 'no data',
		     outcome_s => $outcome || 'no data',

                     has_geodata => (defined $latlong ? 'true' : 'false'),
                     (defined $latlong ? geo_coords_fields($latlong) : ()),

                     has_date_b => (defined $date ? 'true' : 'false'),
                     (defined $date ? date_fields($date) : ()),

                    );

  print_document($output_prefix, $document, $ac_config);


}
close($fh);


#
# close the final chunks of output if necessary
#
if (defined $chunk_fh) {
  print $chunk_fh "]\n";
  close($chunk_fh);
}

if (defined $ac_chunk_fh) {
  print $ac_chunk_fh "]\n";
  close($ac_chunk_fh);
}

if ($log_size) {
  warn "$log_size errors or warnings reported in $log_filename\n";
}


#
# input DD.MM.YYYY
# output YYYY-MM-DD
# doesn't do any error checking
sub make_iso_date {
  my ($input) = @_;

  my ($day, $month, $year) = split /\./, $input;
  return "$year-$month-$day";
}

# returns
# 1. collection_date => always an iso8601 date for the date or start_date
# 2. collection_date_range => a multi-valued DateRangeField with the Chado-resolution dates, or start-end date ranges
# 3. collection_season => One or more DateRangeField values in the year 1600 (an arbitrary leap year) used for seasonal search
# 4. collection_duration_days_i => number of days of collection effort
#
# by Chado-resolution we mean "2010-10" will refer automatically to a range including the entire month of October 2010
#
sub date_fields {
  my $start_date = shift;
  my $end_date = undef;

  return (
          collection_date_range => [ $end_date ? "[$start_date TO $end_date]" : $start_date ],
          collection_season => [ season($start_date, $end_date) ],

          collection_date => iso8601_date($start_date),
          collection_year_s => substr($start_date, 0, 4),
          collection_month_s => substr($start_date, 0, 7),
          collection_epiweek_s => epiweek($start_date),
          collection_day_s => $start_date,
          collection_date_resolution_s => 'day',
         );
}

#
# inputs are strings YYYY-MM-DD
#
sub season {
  my ($start_date, $end_date) = @_;
  if (!defined $end_date) {
    # a single date or low-resolution date (e.g. 2014) will be returned as-is
    # and converted by Solr into a date range as appropriate
    $start_date =~ s/^\d{4}/1600/;
    return $start_date;
  } else {
    # we already parsed them in the calling function, but never mind...
    my ($start_dt, $end_dt) = ($iso8601->parse_datetime($start_date), $iso8601->parse_datetime($end_date));

    # is start to end range >= 1 year?
    if ($start_dt->add( years => 1 )->compare($end_dt) <= 0) {
      return "1600";
    }

    my ($start_month, $end_month) = ($start_dt->month, $end_dt->month);

    # change the Chado-sourced date strings to year 1600
    $start_date =~ s/^\d{4}/1600/;
    $end_date =~ s/^\d{4}/1600/;

    if ($start_month <= $end_month) {
      return ( "[$start_date TO $end_date]" );
    } else {
      # range spans new year, so return two ranges
      return ( "[$start_date TO 1600-12-31]",
	       "[1600-01-01 TO $end_date]" );
    }
  }
}

# converts poss truncated string date into ISO8601 Zulu time (hacked with an extra Z for now)
sub iso8601_date {
  my $string = shift;
  my $datetime = $iso8601->parse_datetime($string);
  if (defined $datetime) {
    return $datetime->datetime."Z";
  }
}

sub epiweek {
  my $string = shift;
  my $datetime = $iso8601->parse_datetime($string);
  return sprintf "%d-W%02d", $datetime->epiweek;
}



#
# returns list of all key-value pairs for geo-coordinates
#
# arg 1 = latlong comma separated string
#
# uses global $gh object
#
sub geo_coords_fields {
  my $latlong = shift;
  my ($lat, $long) = split /,/, $latlong;
  unless (defined $lat && defined $long) {
    log_message("!! some unexpected problem with latlog arg '$latlong' to geo_coords_fields - look for latlong_error_s field in Solr docs");
    return (latlong_error_s => $latlong);
  }

  my $geohash = $gh->encode($lat, $long, 7);

  return (geo_coords => $latlong,
	  geohash_7 => $geohash,
	  geohash_6 => substr($geohash, 0, 6),
	  geohash_5 => substr($geohash, 0, 5),
	  geohash_4 => substr($geohash, 0, 4),
	  geohash_3 => substr($geohash, 0, 3),
	  geohash_2 => substr($geohash, 0, 2),
	  geohash_1 => substr($geohash, 0, 1));
}


#
# geolocation_extra_fields
#
# output country_s, adm1_s, adm2_s from geolocation props
#

sub geolocation_extra_fields {
  my ($fc) = @_;
  return () unless ($fc->geolocation);
  my @result;
  my @props = $fc->geolocation->multiprops;
#  foreach my $prop (@props) {
#    my ($header_term, @value_terms) = $prop->cvterms;
#    if ($header_term->id == $country_term->id) {
#      push @result, ( 'country_s' => $prop->value );
#    } elsif ($header_term->id == $adm1_term->id) {
#      push @result, ( 'adm1_s' => $prop->value );
#    } elsif ($header_term->id == $adm2_term->id) {
#      push @result, ( 'adm2_s' => $prop->value );
#    }
#  }
#
#  my @aprops = $fc->multiprops;
#  foreach my $prop (@aprops) {
#    my ($header_term, @value_terms) = $prop->cvterms;
#    if ($header_term->id == $geoloc_provenance_term->id) {
#      push @result, ( 'geolocation_provenance_s' => $value_terms[0]->name,
#                      'geolocation_provenance_cvterms' => [ flattened_parents($value_terms[0]) ] );
#    } elsif ($header_term->id == $geoloc_precision_term->id) {
#      push @result, ( 'geolocation_precision_s' => $value_terms[0]->name,
#                      'geolocation_precision_cvterms' => [ flattened_parents($value_terms[0]) ] );
#    }
#  }
#
  return @result;
}



#
# ohr = ordered hash reference
#
# return order-maintaining hash reference
# with optional arguments as key-value pairs
#
sub ohr {
  my $ref = { };
  tie %$ref, 'Tie::IxHash', @_;
  return $ref;
}


#
# if an empty $arrayref is passed, $value (e.g. 'no data') is pushed onto the array that is referenced
#
sub fallback_value {
  my ($arrayref, $value) = @_;
  unless (@$arrayref) {
    push @$arrayref, $value;
  }
}

#
# print JSON documents for main and autocomplete to chunked output files
#

#
# uses global variables $document_counter, $chunk_counter, $chunk_size, $chunk_fh, $needcomma
# (and the ac_* equivalents)
#

sub print_document {
  my ($prefix, $document, $ac_config) = @_;

  #
  # main document first
  #

  if (!defined $chunk_fh) { # start a new chunk
    $chunk_counter++;
    $chunk_fh = new IO::Compress::Gzip sprintf("$prefix-main-%02d.json.gz", $chunk_counter);
    die unless (defined $chunk_fh);
    print $chunk_fh "[\n";
    $needcomma = 0;
  }

  my $json_text = $json->encode($document);
  chomp($json_text);
  print $chunk_fh ",\n" if ($needcomma++);
  print $chunk_fh qq!$json_text\n!;

  $document_counter++;

  if ($document_counter % $chunk_size == 0) { # close the current chunk
    print $chunk_fh "]\n";
    close($chunk_fh);
    undef $chunk_fh;
  }

  #
  # autocomplete next
  #
  my $bundle = $document->{bundle};
  my $has_abundance_data = $document->{has_abundance_data_b};
  my $phenotype_type = $document->{phenotype_type_s};
  my $genotype_type = $document->{genotype_type_s};

  if ($ac_config && $bundle && $ac_config->{$bundle}) {
    my $config = $ac_config->{$bundle};
    # process $document to find fields to add for a/c
    foreach my $field (keys %{$document}) {
      if (exists $config->{$field}) {
	my $type = $config->{$field}{type};
	my $typedot = $type; $typedot =~ s/\s/./g;

	my @common_fields =
	  (
	   type => $type,
	   bundle => $bundle,
	   field => $field,
	   geo_coords => $document->{geo_coords}, # used for "local suggestions"
           stable_id => $document->{accession}
	  );

	if ($config->{$field}{multi} || $config->{$field}{cvterms}) {
	  my $last_was_accession;
	  for (my $i=0; $i<@{$document->{$field}}; $i++) {
	    my $text = $document->{$field}[$i];
	    my $is_accession = $text =~ /^\w+:\d+$/; # is this an ontology term accession? e.g. VBsp:0012345
	    my $ac_document =
	      ohr(
		  id => "$document->{id}.$typedot.$i",
		  textsuggest => $text,
		  is_synonym => ($config->{$field}{cvterms} && $i>0 &&
                                 !$is_accession && !$last_was_accession ? 'true' : 'false'),
		  @common_fields
		 );
	    $last_was_accession = $is_accession;
	    print_ac_document($prefix, $ac_document);
	  }
	} else {
	  my $ac_document =
	    ohr(
		id => "$document->{id}.$typedot",
		textsuggest => $document->{$field},
		@common_fields
	       );
	  print_ac_document($prefix, $ac_document);
	}
      }
    }
  }
}


#
# the following is a bit cut and paste-y
# could do all the document and chunk counts hashed on $prefix
# and then just use print_document for both?
#
sub print_ac_document {
  my ($prefix, $ac_document) = @_;

  if (!defined $ac_chunk_fh) {	# start a new chunk
    $ac_chunk_counter++;
    $ac_chunk_fh = new IO::Compress::Gzip sprintf("$prefix-ac-%02d.json.gz", $ac_chunk_counter);
    die unless (defined $ac_chunk_fh);
    print $ac_chunk_fh "[\n";
    $ac_needcomma = 0;
  }

  my $ac_json_text = $json->encode($ac_document);
  chomp($ac_json_text);
  print $ac_chunk_fh ",\n" if ($ac_needcomma++);
  print $ac_chunk_fh qq!$ac_json_text\n!;
  $ac_document_counter++;

  if ($ac_document_counter % $ac_chunk_size == 0) { # close the current chunk
    print $ac_chunk_fh "]\n";
    close($ac_chunk_fh);
    undef $ac_chunk_fh;
  }
}

#
# log_message
#
# write $message to the global logfile and increment a counter
#


sub log_message {
  my ($message) = @_;
  open LOG, ">>$log_filename" || die "can't write to $log_filename\n";
  print LOG "$message\n";
  close(LOG);
  $log_size++;
}

#
# convert the two age columns into one
#

sub get_age {
  my ($row, $h2i) = @_;

  my $age_start = $row->[$h2i->{'demographics.ageRange.start'}];
  my $age_end = $row->[$h2i->{'demographics.ageRange.end'}];
  return $age_start if ($age_start eq $age_end);
  return "$age_start-$age_end";
}
