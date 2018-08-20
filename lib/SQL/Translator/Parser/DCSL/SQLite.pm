package SQL::Translator::Parser::DCSL::SQLite;

use strict;
use warnings;
use SQL::Translator::Parser::DCSL::Utils qw/columns_info_for_quoted maybe_lc/;

=head1 NAME

SQL::Translator::Parser::DCSL::SQLite - SQL::Translator parser for SQLite instance over DBI

=head1 DESCRIPTION

None yet.

=cut

sub columns_info_for {
    my ($dbh, $table, $preserve_case, $sql_maker) = @_;

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
