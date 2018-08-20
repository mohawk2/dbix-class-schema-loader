package # hide from PAUSE
    SQL::Translator::Parser::DCSL::Utils;

use strict;
use warnings;
use Exporter 'import';
use Carp::Clan qw/^(DBIx::Class|SQL::Translator::Parser::DCSL)/;
use Try::Tiny;

our @EXPORT_OK = qw/columns_info_for_quoted maybe_lc/;

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
    my ($dbh, $table, $preserve_case, $sql_maker) = @_;

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

# $self->schema->storage->sql_maker
sub columns_info_for {
    my ($dbh, $table, $preserve_case, $sql_maker) = @_;

    my %result;
    my %raw_result;

    if (my $sth = try { $dbh->column_info(undef, $table->schema, $table->name, '%' ) }) {
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

    my $sth = sth_for($dbh, $sql_maker, $table, undef, \'1 = 0');
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
    my ($dbh, $sql_maker, $table, $fields, $where) = @_;

    my $sth = $dbh->prepare($sql_maker
        ->select(\$table->sql_name, $fields || \'*', $where));

    return $sth;
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
