#!/usr/bin/env perl
use strict;
use warnings;
use feature qw(say);

use JSON::PP qw(encode_json);
use HTTP::Tiny;
use Data::Dumper qw(Dumper);
# bit of a hack, but included in core perl
use CPAN::Meta::YAML;

my $main_config = read_config("carbonsearch.yaml");
if (!exists $main_config->{consumers}->{httpapi}) {
    die "carbonsearch HTTP consumer not enabled. populate_test_data.pl uses the HTTP consumer, so carbonsearch needs to enable it in carbonsearch.yaml\n"
};

my $http_config = read_config("httpapi.yaml");

my $port = $http_config->{port};
(my $endpoint = $http_config->{endpoint}) =~ s/^\///;
my $splits = $main_config->{split_indexes};
my %valid_services = map { $_ => 1 } sort values %$splits;

my %tag_soup = (
    servers => {
        'status' => [qw(live maint deprovision)],
        'dc' => [qw(us-east us-west eur-east asia-west)],
        'hw' => [qw(dell hp vm container)],
        'network' => [qw(1g 10g 25g 40g)],
    },
    loadbal => {
        'enabled' => [qw(true false)],
        'pool' => [qw(www internal api)],
    },
    database => {
        'chain' => [qw(users shop partners internal)],
        'type' => [qw(master intermediate replica)],
    }
);

my @metric_prefixes = qw(
    sys
    computer
    host
    box
);

my @suffixes = qw(
    cpu.loadavg
    mem.totalfree
    net.tcp.rx_byte
    net.tcp.tx_byte
    disk.df
    net.tcp.total_open
);

sub host_metrics {
    my ($host, $count) = @_;
    my %metrics;
    for (1 .. $count) {
        my $prefix = $metric_prefixes[int(rand(@metric_prefixes))];
        my $suffix = $suffixes[int(rand(@suffixes))];
        my $metric = "$prefix.$host.$suffix";
        $metrics{$metric} = 1;
    }
    return [ sort keys %metrics ];
}

sub tags {
    my $index_name= shift;
    my $count = shift;

    my %tags;
    for (1 .. $count) {
        my @services = keys %tag_soup;
        for my $service (@services) {
            next if !$valid_services{$service};
            my @service_keys = keys %{$tag_soup{$service}};

            my $key = $service_keys[int(rand(@service_keys))];
            my @vals = @{$tag_soup{$service}->{$key}};
            my $val = $vals[int(rand(@vals))];
            my $tag = "$service-$key:$val";
            # avoid adding multiple tags with the same key in one batch
            $tags{"$service-$key"} = $tag;
        }
    }
    return [ sort values %tags ];
}

my $http = HTTP::Tiny->new;
for my $num (1..10) {
    my $hosts = generate_hosts(10);
    for my $host (@$hosts) {
        my @indexes = keys %$splits;
        my $index = $indexes[int(rand(@indexes))];
        my $tags = encode_json({
            Key => $index,
            Value => $host,
            Tags => tags($splits->{$index}, 10),
        });

        my $metrics = encode_json({
            Key => $index,
            Value => $host,
            Metrics => host_metrics($host, 10),
        });

        my $res = $http->post("http://localhost:$port/$endpoint/tag", {
            content => $tags
        });
        $res = $http->post("http://localhost:$port/$endpoint/metric", {
            content => $metrics
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

        say STDERR "Here's the response from carbonsearch: " . Dumper($res);
        exit(1);
        }
    }
}

sub read_config {
    my $filename = shift;

    open my $fh, "<", $filename
        or die "could not open $filename: $!. Perhaps the script isn't being run from the main carbonsearch directory?";

    my $text = do { local $/; <$fh> };
    my $parsed;

    eval {
        $parsed = CPAN::Meta::YAML->read_string($text);
        1;
    } or do {
        my $err = $@;
        die "Failure to parse $filename: $err";
    };

    # YAML, eh.
    return $parsed->[0];
}

sub generate_hosts {
    my $count = shift;
    my %hosts;
    my @host_types = qw(
        frontend
        worker
        bar
        qux
        db
        lb
        proxy
    );
    for (1 .. $count) {
        my $host =  sprintf(
            "%s-%0.3d",
            $host_types[int(rand(@host_types))],
            int(rand(400)),
        );
        $hosts{$host} = 1;
    }
    return [ sort keys %hosts ];
}
