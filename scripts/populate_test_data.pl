#!/usr/bin/env perl
use strict;
use warnings;
use JSON::PP qw(encode_json);
use HTTP::Tiny;
use Data::Dumper qw(Dumper);

my $port = $ARGV[0] // 8100;

my %tag_soup = (
    'servers-status' => [qw(live maint deprovision)],
    'servers-dc' => [qw(us-east us-west eur-east asia-west)],
    'servers-hw' => [qw(dell hp vm container)],
    'servers-network' => [qw(1g 10g 25g 40g)],
    'loadbal-enabled' => [qw(true false)],
    'loadbal-pool' => [qw(www internal api)],
    'database-chain' => [qw(users shop partners internal)],
    'database-type' => [qw(master intermediate replica)],
);

my @metric_prefixes = qw(
    sys
    computer
    host
    box
);

my @hosts = qw(
    bar
    qux
    db
    lb
    proxy
);

my @suffixes = qw(
    cpu.loadavg
    mem.totalfree
    net.tcp.rx_byte
    net.tcp.tx_byte
    disk.df
    net.tcp.total_open
);

sub metrics {
    my $count = shift;

    my @metrics;
    for (1 .. $count) {
        my $prefix = $metric_prefixes[int(rand(@metric_prefixes))];
        my $suffix = $suffixes[int(rand(@suffixes))];
        my $host = $hosts[int(rand(@hosts))];
        my $num = sprintf("%0.3d", int(rand(400)));
        push @metrics, "$prefix.$host\_$num.$suffix";
    }
    return \@metrics;
}

sub tags {
    my $count = shift;

    my @tags;
    for (1 .. $count) {
        my @keys = keys %tag_soup;
        my $key = $keys[int(rand(@keys))];
        my @vals = @{$tag_soup{$key}};
        my $val = $vals[int(rand(@vals))];
        push @tags, "$key:$val";
    }
    return \@tags;
}

my $http = HTTP::Tiny->new;
for my $num (1..10) {
    my $content = encode_json({
        Tags => tags(10),
        Metrics => metrics(10),
    });

    my $res = $http->post("http://localhost:$port/consumer/custom", {
        content => $content
    });
    if ($res->{status} ne 200) {
        my $err = $res->{content};
        chomp($err);
        print STDERR <<USAGE
error while trying to load carbonsearch: $err.

usage: perl scripts/populate_test_data <port_for_http_consumer>
port defaults to 8100.

USAGE
;
    exit(1);
    }
}
