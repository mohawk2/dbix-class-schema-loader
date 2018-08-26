package SQL::Translator::Parser::DCSL::SQLite;

use strict;
use warnings;
use SQL::Translator::Parser::DCSL::Utils qw/columns_info_for_quoted maybe_lc filter_tables_sql/;
require SQL::Translator::Schema::Constraint;

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
    my ($dbh, $table) = @_;

    my $sth = $dbh->prepare(
        "pragma foreign_key_list(" . $dbh->quote_identifier($table) . ")"
    );
    $sth->execute;

    my @rels;
    while (my $fk = $sth->fetchrow_hashref) {
        my $rel = $rels[ $fk->{id} ] ||= {
            fields => [],
            reference_fields => undef,
            reference_table => $fk->{table},
        };

        push @{ $rel->{fields} }, $fk->{from};
        push @{ $rel->{reference_fields} }, $fk->{to} if defined $fk->{to};

        $rel->{attrs} ||= {
            on_delete => uc $fk->{on_delete},
            on_update => uc $fk->{on_update},
        };

        warn "This is supposed to be the same rel but remote_table changed from ",
            $rel->{reference_table}, " to ", $fk->{table}
            if $rel->{reference_table} ne $fk->{table};
    }
    $sth->finish;

    # now we need to determine whether each FK is DEFERRABLE, this can only be
    # done by parsing the DDL from sqlite_master

    my $ddl = $dbh->selectcol_arrayref(<<"EOF", undef, $table, $table)->[0];
select sql from sqlite_master
where name = ? and tbl_name = ?
EOF

    my @constraints;
    foreach my $fk (@rels) {
        my $local_cols  = '"?' . (join '"? \s* , \s* "?', map quotemeta, @{ $fk->{fields} })        . '"?';
        my $remote_cols = '"?' . (join '"? \s* , \s* "?', map quotemeta, @{ $fk->{reference_fields} || [] }) . '"?';
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
            $fk->{attrs}{deferrable} = $deferrable_clause =~ /not/i ? 0 : 1;
        }
        else {
            # check for inline constraint if 1 local column
            if (@{ $fk->{fields} } == 1) {
                my ($local_col)  = @{ $fk->{fields} };
                my ($remote_col) = @{ $fk->{reference_fields} || [] };
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
                    $fk->{attrs}{deferrable} = $deferrable_clause =~ /not/i ? 0 : 1;
                }
                else {
                    $fk->{attrs}{deferrable} = 0;
                }
            }
            else {
                $fk->{attrs}{deferrable} = 0;
            }
        }
        push @constraints, SQL::Translator::Schema::Constraint->new(
            type => 'foreign_key',
            fields => $fk->{fields},
            reference_fields => $fk->{reference_fields},
            reference_table => $fk->{reference_table},
            %{ $fk->{attrs} },
        );
    }

    return \@constraints;
}

sub table_uniq_info {
    my ($dbh, $schema, $table, $preserve_case) = @_;
    my $result = SQL::Translator::Parser::DCSL::Utils::table_uniq_info(@_);
    for my $r (@$result) {
        $r->[0] = (join '_', @{$r->[1]}) . '_unique';
    }
    return [ sort { $a->[0] cmp $b->[0] } @$result ];
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

# second arg will be a $dbh - temporary code to make here cribbed from ::Parser::DBI until this gets put as ::Parser::DBI::SQLite
sub parse {
    my ($tr, $dbh) = @_;
    my $args = $tr->parser_args;
    # XXX temp code start
    $dbh = $args->{'dbh'};
    my $dsn = $args->{'dsn'};
    my $db_user = $args->{'db_user'};
    my $db_password = $args->{'db_password'};
    my $dbh_is_local;
    unless ( $dbh ) {
        die 'No DSN' unless $dsn;
        $dbh = DBI->connect( $dsn, $db_user, $db_password,
            {
                FetchHashKeyName => 'NAME_lc',
                LongReadLen      => 3000,
                LongTruncOk      => 1,
                RaiseError       => 1,
            }
        );
        $dbh_is_local = 1;
    }
    die 'No database handle' unless defined $dbh;
    # XXX temp code end
    my $preserve_case = $args->{preserve_case};
    my $schema_name = $args->{schema};
    my $schema = $tr->schema;
    my @tables = tables_list($dbh);
    for my $table_name ( @tables ) {
        my $table = $schema->add_table( name => $table_name )
            or die $schema->error;
        my $cols_info = columns_info_for($dbh, $schema_name, $table_name, $preserve_case);
        for my $colname ( keys %$cols_info ) {
            my $info = $cols_info->{$colname};
            my $field = $table->add_field(
                name => $colname,
                %$info,
            ) or die $table->error;
            $table->primary_key( $field->name ) if $info->{is_primary_key};
        }
        my $fk_info = table_fk_info($dbh, $table, $preserve_case);
        for my $rel ( @$fk_info ) {
            $table->add_constraint($rel);
        }
    }
    # XXX temp code start
    eval { $dbh->disconnect } if (defined $dbh and $dbh_is_local);
    # XXX temp code end
    return 1;
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
