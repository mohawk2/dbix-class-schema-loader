package SQL::Translator::Parser::DCSL::SQLite;

use strict;
use warnings;
use SQL::Translator::Parser::DCSL::Utils qw/columns_info_for_quoted maybe_lc filter_tables_sql/;

=head1 NAME

SQL::Translator::Parser::DCSL::SQLite - SQL::Translator parser for SQLite instance over DBI

=head1 DESCRIPTION

None yet.

=cut

sub columns_info_for {
    my ($dbh, $schema, $table, $preserve_case) = @_;

    my $result = columns_info_for_quoted(@_);

    local $dbh->{FetchHashKeyName} = 'NAME_lc';

    my $sth = $dbh->prepare(
        "pragma table_info(" . $dbh->quote_identifier($table) . ")"
    );
    $sth->execute;
    my $cols = $sth->fetchall_hashref('name');

    # copy and case according to preserve_case mode
    # no need to check for collisions, SQLite does not allow them
    my %cols;
    while (my ($col, $info) = each %$cols) {
        $cols{ maybe_lc($col, $preserve_case) } = $info;
    }

    my ($num_pk, $pk_col) = (0);
    # SQLite doesn't give us the info we need to do this nicely :(
    # If there is exactly one column marked PK, and its type is integer,
    # set it is_auto_increment. This isn't 100%, but it's better than the
    # alternatives.
    while (my ($col_name, $info) = each %$result) {
        if ($cols{$col_name}{pk}) {
            $num_pk++;
            if (lc($cols{$col_name}{type}) eq 'integer') {
                $pk_col = $col_name;
            }
        }
    }

    while (my ($col, $info) = each %$result) {
        if ((eval { ${ $info->{default_value} } }||'') eq 'CURRENT_TIMESTAMP') {
            ${ $info->{default_value} } = 'current_timestamp';
        }
        if ($num_pk == 1 and defined $pk_col and $pk_col eq $col) {
            $info->{is_auto_increment} = 1;
        }
    }

    return $result;
}

sub table_fk_info {
    my ($dbh, $table, $preserve_case) = @_;

    my $sth = $dbh->prepare(
        "pragma foreign_key_list(" . $dbh->quote_identifier($table) . ")"
    );
    $sth->execute;

    my @rels;
    while (my $fk = $sth->fetchrow_hashref) {
        my $rel = $rels[ $fk->{id} ] ||= {
            local_columns => [],
            remote_columns => undef,
            remote_table => $fk->{table}, # patch up in DCSL-land
        };

        push @{ $rel->{local_columns} }, maybe_lc($fk->{from}, $preserve_case);
        push @{ $rel->{remote_columns} }, maybe_lc($fk->{to}, $preserve_case) if defined $fk->{to};

        $rel->{attrs} ||= {
            on_delete => uc $fk->{on_delete},
            on_update => uc $fk->{on_update},
        };

        warn "This is supposed to be the same rel but remote_table changed from ",
            $rel->{remote_table}, " to ", $fk->{table}
            if $rel->{remote_table} ne $fk->{table};
    }
    $sth->finish;

    # now we need to determine whether each FK is DEFERRABLE, this can only be
    # done by parsing the DDL from sqlite_master

    my $ddl = $dbh->selectcol_arrayref(<<"EOF", undef, $table->name, $table->name)->[0];
select sql from sqlite_master
where name = ? and tbl_name = ?
EOF

    foreach my $fk (@rels) {
        my $local_cols  = '"?' . (join '"? \s* , \s* "?', map quotemeta, @{ $fk->{local_columns} })        . '"?';
        my $remote_cols = '"?' . (join '"? \s* , \s* "?', map quotemeta, @{ $fk->{remote_columns} || [] }) . '"?';
        my ($deferrable_clause) = $ddl =~ /
                foreign \s+ key \s* \( \s* $local_cols \s* \) \s* references \s* (?:\S+|".+?(?<!")") \s*
                (?:\( \s* $remote_cols \s* \) \s*)?
                (?:(?:
                    on \s+ (?:delete|update) \s+ (?:set \s+ null|set \s+ default|cascade|restrict|no \s+ action)
                |
                    match \s* (?:\S+|".+?(?<!")")
                ) \s*)*
                ((?:not)? \s* deferrable)?
        /sxi;

        if ($deferrable_clause) {
            $fk->{attrs}{is_deferrable} = $deferrable_clause =~ /not/i ? 0 : 1;
        }
        else {
            # check for inline constraint if 1 local column
            if (@{ $fk->{local_columns} } == 1) {
                my ($local_col)  = @{ $fk->{local_columns} };
                my ($remote_col) = @{ $fk->{remote_columns} || [] };
                $remote_col ||= '';

                my ($deferrable_clause) = $ddl =~ /
                    "?\Q$local_col\E"? \s* (?:\w+\s*)* (?: \( \s* \d\+ (?:\s*,\s*\d+)* \s* \) )? \s*
                    references \s+ (?:\S+|".+?(?<!")") (?:\s* \( \s* "?\Q$remote_col\E"? \s* \))? \s*
                    (?:(?:
                      on \s+ (?:delete|update) \s+ (?:set \s+ null|set \s+ default|cascade|restrict|no \s+ action)
                    |
                      match \s* (?:\S+|".+?(?<!")")
                    ) \s*)*
                    ((?:not)? \s* deferrable)?
                /sxi;

                if ($deferrable_clause) {
                    $fk->{attrs}{is_deferrable} = $deferrable_clause =~ /not/i ? 0 : 1;
                }
                else {
                    $fk->{attrs}{is_deferrable} = 0;
                }
            }
            else {
                $fk->{attrs}{is_deferrable} = 0;
            }
        }
    }

    return \@rels;
}

sub table_uniq_info {
    my ($dbh, $table, $preserve_case) = @_;

    my $sth = $dbh->prepare(
        "pragma index_list(" . $dbh->quote($table) . ")"
    );
    $sth->execute;

    my @uniqs;
    while (my $idx = $sth->fetchrow_hashref) {
        next unless $idx->{unique};

        my $name = $idx->{name};

        my $get_idx_sth = $dbh->prepare("pragma index_info(" . $dbh->quote($name) . ")");
        $get_idx_sth->execute;
        my @cols;
        while (my $idx_row = $get_idx_sth->fetchrow_hashref) {
            push @cols, maybe_lc($idx_row->{name}, $preserve_case);
        }
        $get_idx_sth->finish;

        # Rename because SQLite complains about sqlite_ prefixes on identifiers
        # and ignores constraint names in DDL.
        $name = (join '_', @cols) . '_unique';

        push @uniqs, [ $name => \@cols ];
    }
    $sth->finish;
    return [ sort { $a->[0] cmp $b->[0] } @uniqs ];
}

sub tables_list {
    my ($dbh) = @_;
    my $sth = $dbh->prepare(
        "SELECT * FROM sqlite_master where type in ('table', 'view')"
            . " and tbl_name not like 'sqlite_%'"
    );
    $sth->execute;
    my @tables;
    while ( my $row = $sth->fetchrow_hashref ) {
        push @tables, $row->{tbl_name};
    }
    $sth->finish;
    return filter_tables_sql($dbh, \@tables);
}

=head1 SEE ALSO

L<DBIx::Class::Schema::Loader>, L<DBIx::Class::Schema::Loader::Base>,
L<DBIx::Class::Schema::Loader::DBI>

=head1 AUTHORS

See L<DBIx::Class::Schema::Loader/AUTHORS>.

=head1 LICENSE

This library is free software; you can redistribute it and/or modify it under
the same terms as Perl itself.

=cut

1;
# vim:et sts=4 sw=4 tw=0:
