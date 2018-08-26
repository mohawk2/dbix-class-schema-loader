package # hide from PAUSE
    SQL::Translator::Parser::DCSL::Utils;

use strict;
use warnings;
use Exporter 'import';
use Carp::Clan qw/^(DBIx::Class|SQL::Translator::Parser::DCSL)/;
use Try::Tiny;

our @EXPORT_OK = qw/
    columns_info_for_quoted maybe_lc
    filter_tables_sql filter_tables_constraints
    table_uniq_info
/;

=head1 NAME

SQL::Translator::Parser::DCSL::Utils - utility functions for parsing schemas out of DBI handles

=head1 DESCRIPTION

None yet.

=head1 FUNCTIONS

=head2 columns_info_for_quoted

=head3 Arguments

C<$dbh>, C<$table>

=cut

sub columns_info_for_quoted {
    my ($dbh, $schema, $table, $preserve_case) = @_;

    my ($result,$raw) = columns_info_for(@_);

    while (my ($col, $info) = each %$result) {
        if (my $def = $info->{default_value}) {
            $def =~ s/^\s+//;
            $def =~ s/\s+\z//;

# remove Pg typecasts (e.g. 'foo'::character varying) too
            if ($def =~ /^["'](.*?)['"](?:::[\w\s]+)?\z/) {
                $info->{default_value} = $1;
            }
# Some DBs (eg. Pg) put parenthesis around negative number defaults
            elsif ($def =~ /^\((-?\d.*?)\)(?:::[\w\s]+)?\z/) {
                $info->{default_value} = $1;
            }
            elsif ($def =~ /^(-?\d.*?)(?:::[\w\s]+)?\z/) {
                $info->{default_value} = $1;
            }
            elsif ($def =~ /^NULL:?/i) {
                my $null = 'null';
                $info->{default_value} = \$null;
            }
            else {
                $info->{default_value} = \$def;
            }
        }
    }

    return wantarray ? ($result, $raw) : $result;
}

sub columns_info_for {
    my ($dbh, $schema, $table, $preserve_case) = @_;

    my %result;
    my %raw_result;

    if (my $sth = try { $dbh->column_info(undef, $schema, $table, '%' ) }) {
        COL_INFO: while (my $info = try { $sth->fetchrow_hashref } catch { +{} }) {
            next COL_INFO unless %$info;

            my $column_info = {};
            $column_info->{data_type}     = lc $info->{TYPE_NAME};

            my $size = $info->{COLUMN_SIZE};

            if (defined $size && defined $info->{DECIMAL_DIGITS}) {
                $column_info->{size} = [$size, $info->{DECIMAL_DIGITS}];
            }
            elsif (defined $size) {
                $column_info->{size} = $size;
            }

            $column_info->{is_nullable}   = $info->{NULLABLE} ? 1 : 0;
            $column_info->{default_value} = $info->{COLUMN_DEF} if defined $info->{COLUMN_DEF};
            my $col_name = $info->{COLUMN_NAME};
            $col_name =~ s/^\"(.*)\"$/$1/;

            # _extra_column_info happens here

            $raw_result{$col_name} = $info;
            $result{$col_name} = $column_info;
        }
        $sth->finish;
    }

    my $sth = sth_for($dbh, $table);
    $sth->execute;

    my @columns = @{ $sth->{NAME} };

    COL: for my $i (0 .. $#columns) {
        next COL if %{ $result{ $columns[$i] }||{} };

        my $column_info = {};
        $column_info->{data_type} = lc $sth->{TYPE}[$i];

        my $size = $sth->{PRECISION}[$i];

        if (defined $size && defined $sth->{SCALE}[$i]) {
            $column_info->{size} = [$size, $sth->{SCALE}[$i]];
        }
        elsif (defined $size) {
            $column_info->{size} = $size;
        }

        $column_info->{is_nullable} = $sth->{NULLABLE}[$i] ? 1 : 0;

        if ($column_info->{data_type} =~ m/^(.*?)\((.*?)\)$/) {
            $column_info->{data_type} = $1;
            $column_info->{size}    = $2;
        }

        # _extra_column_info happens here

        $result{ $columns[$i] } = $column_info;
    }
    $sth->finish;

    foreach my $col (keys %result) {
        my $colinfo = $result{$col};
        my $type_num = $colinfo->{data_type};
        my $type_name;
        if (defined $type_num && $type_num =~ /^-?\d+\z/ && $dbh->can('type_info')) {
            my $type_name = dbh_type_info_type_name($dbh, $type_num);
            $colinfo->{data_type} = lc $type_name if $type_name;
        }
    }

    # check for instances of the same column name with different case in preserve_case=0 mode
    if (not $preserve_case) {
        my %lc_colnames;

        foreach my $col (keys %result) {
            push @{ $lc_colnames{lc $col} }, $col;
        }

        if (keys %lc_colnames != keys %result) {
            my @offending_colnames = map @$_, grep @$_ > 1, values %lc_colnames;

            my $offending_colnames = join ", ", map "'$_'", @offending_colnames;

            croak "columns $offending_colnames in table @{[ $table->sql_name ]} collide in preserve_case=0 mode. preserve_case=1 mode required";
        }

        # apply lowercasing
        my %lc_result;

        while (my ($col, $info) = each %result) {
            $lc_result{ maybe_lc($col, $preserve_case) } = $info;
        }

        %result = %lc_result;
    }

    return wantarray ? (\%result, \%raw_result) : \%result;
}

sub maybe_lc {
    my ($name, $preserve_case) = @_;
    return $preserve_case ? $name : lc($name)
}

sub maybe_uc {
    my ($name, $preserve_case) = @_;
    return $preserve_case ? $name : uc($name)
}

sub sth_for {
    my ($dbh, $table) = @_;
    $table = $table->sql_name if ref $table;
    my $sth = $dbh->prepare(qq{select * from $table where 1 = 0});
    return $sth;
}

# ignore bad tables and views
sub filter_tables_sql {
    my ($dbh, $tables) = @_;
    my @tables = @$tables;
    my @filtered_tables;
    TABLE: for my $table (@tables) {
        try {
            local $^W = 0; # for ADO
            my $sth = sth_for($dbh, $table);
            $sth->execute;
            1;
        }
        catch {
            warn "Bad table or view '$table', ignoring: $_\n";
            0;
        } or next TABLE;
        push @filtered_tables, $table;
    }
    return @filtered_tables;
}

# apply constraint/exclude
sub filter_tables_constraints {
    my ($tables, $constraint, $exclude) = @_;
    my @tables = @$tables;
    @tables = check_constraint(1, $constraint, @tables);
    @tables = check_constraint(0, $exclude, @tables);
    return @tables;
}

sub check_constraint {
    my ($include, $constraint, @tables) = @_;
    return @tables unless defined $constraint;
    return grep { !$include xor recurse_constraint($constraint, @{$_}) } @tables
        if ref $constraint eq 'ARRAY';
    return grep { !$include xor /$constraint/ } @tables;
}

sub recurse_constraint {
    my ($constraint, @parts) = @_;
    my $name = shift @parts;
    # If there are any parts left, the constraint must be an arrayref
    croak "depth of constraint/exclude array does not match length of moniker_parts"
        unless !!@parts == !!(ref $constraint eq 'ARRAY');
    # if ths is the last part, use the constraint directly
    return $name =~ $constraint unless @parts;
    # recurse into the first matching subconstraint
    foreach (@{$constraint}) {
        my ($re, $sub) = @{$_};
        return recurse_constraint($sub, @parts)
            if $name =~ $re;
    }
    return 0;
}

sub table_uniq_info {
    my ($dbh, $schema, $table, $preserve_case) = @_;
    if (not $dbh->can('statistics_info')) {
        warn "No UNIQUE constraint information can be gathered for this driver";
        return [];
    }
    my %indices;
    my $sth = $dbh->statistics_info(undef, $schema, $table, 1, 1);
    while(my $row = $sth->fetchrow_hashref) {
        # skip table-level stats, conditional indexes, and any index missing
        #  critical fields
        next if $row->{TYPE} eq 'table'
            || defined $row->{FILTER_CONDITION}
            || !$row->{INDEX_NAME}
            || !defined $row->{ORDINAL_POSITION};
        # starts from 1
        $indices{$row->{INDEX_NAME}}[$row->{ORDINAL_POSITION} - 1] = maybe_lc($row->{COLUMN_NAME} || '', $preserve_case);
    }
    $sth->finish;
    my @retval;
    foreach my $index_name (sort keys %indices) {
        my @cols = @{$indices{$index_name}};
        # skip indexes with missing column names (e.g. expression indexes)
        next unless @cols == grep $_, @cols;
        push(@retval, [ $index_name => \@cols ]);
    }
    return \@retval;
}

1;

=head1 SEE ALSO

None yet.

=head1 AUTHORS

See L<DBIx::Class::Schema::Loader/AUTHORS>.

=head1 LICENSE

This library is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut

1;
# vim:et sts=4 sw=4 tw=0:
