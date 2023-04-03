#!/usr/bin/perl -w

use vars qw(%free_ptrs_map);
use 5.010;

open my $fh, "<", "observer.log"
    or die "Can't read trace log";

my @free_ptrs;
while (<$fh>) {
  chomp;
  if (/free.*ptr=(0x[0-9a-f]+).*/i) {
    my $ptr = $1;
    push @free_ptrs, $ptr;
  }
}

for (my $idx = 0; $idx < @free_ptrs; $idx++) {
  $free_ptrs_map{$free_ptrs[$idx]} = $idx;
}

# rewind to begin
seek $fh, 0, 0;

while (<$fh>) {
  chomp;
  if (/alloc.*size=([0-9]+).*ptr=(0x[0-9a-f]+).*/i) {
    my $size = $1;
    my $ptr = $2;
    my $idx = $free_ptrs_map{$ptr};
    if (defined $idx) {
      say "1 $idx $size";
    } else {
      say "1 -1 $size";
    }
  } elsif (/free.*ptr=(0x[0-9a-f]+).*/i) {
    my $ptr = $1;
    my $idx = $free_ptrs_map{$ptr};
    say "2 $idx";
  }
}

close $fh;
