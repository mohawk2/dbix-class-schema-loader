package DBIx::Class::Schema::Loader::DBI::SQLite;

use strict;
use warnings;
use base 'DBIx::Class::Schema::Loader::DBI';
use mro 'c3';
use DBIx::Class::Schema::Loader::Table ();

require SQL::Translator::Parser::DCSL::SQLite;

our $VERSION = '0.07049';

=head1 NAME

DBIx::Class::Schema::Loader::DBI::SQLite - DBIx::Class::Schema::Loader::DBI SQLite Implementation.

=head1 DESCRIPTION

See L<DBIx::Class::Schema::Loader> and L<DBIx::Class::Schema::Loader::Base>.

=head1 METHODS

=head2 rescan

SQLite will fail all further commands on a connection if the underlying schema
has been modified.  Therefore, any runtime changes requiring C<rescan> also
require us to re-connect to the database.  The C<rescan> method here handles
that reconnection for you, but beware that this must occur for any other open
sqlite connections as well.

=cut

sub _setup {
    my $self = shift;

    $self->next::method(@_);

    if (not defined $self->preserve_case) {
        $self->preserve_case(0);
    }

    if ($self->db_schema) {
        warn <<'EOF';
db_schema is not supported on SQLite, the option is implemented only for qualify_objects testing.
EOF
        if ($self->db_schema->[0] eq '%') {
            $self->db_schema(undef);
        }
    }
}

sub rescan {
    my ($self, $schema) = @_;

    $schema->storage->disconnect if $schema->storage;
    $self->next::method($schema);
}

sub _columns_info_for {
    my $self = shift;
    my ($table) = @_;

    return SQL::Translator::Parser::DCSL::SQLite::columns_info_for(
        $self->dbh, $table, $self->preserve_case,
        $self->schema->storage->sql_maker
    );
}

sub _table_fk_info {
    my ($self, $table) = @_;
    my $rels = SQL::Translator::Parser::DCSL::SQLite::table_fk_info(
        $self->dbh, $table, $self->preserve_case,
    );
    for my $rel (@$rels) {
        $rel->{remote_table} = DBIx::Class::Schema::Loader::Table->new(
            loader => $self,
            name   => $rel->{remote_table},
            ($self->db_schema ? (
                schema        => $self->db_schema->[0],
                ignore_schema => 1,
            ) : ()),
        );
    }
    return $rels;
}

sub _table_uniq_info {
    my ($self, $table) = @_;
    return SQL::Translator::Parser::DCSL::SQLite::table_uniq_info(
        $self->dbh, $table, $self->preserve_case,
    );
}

sub _tables_list {
    my ($self) = @_;

    my $sth = $self->dbh->prepare(
        "SELECT * FROM sqlite_master where type in ('table', 'view')"
            . " and tbl_name not like 'sqlite_%'"
    );
    $sth->execute;
    my @tables;
    while ( my $row = $sth->fetchrow_hashref ) {
        push @tables, DBIx::Class::Schema::Loader::Table->new(
            loader => $self,
            name   => $row->{tbl_name},
            ($self->db_schema ? (
                schema        => $self->db_schema->[0],
                ignore_schema => 1, # for qualify_objects tests
            ) : ()),
        );
    }
    $sth->finish;
    return $self->_filter_tables(\@tables);
}

sub _table_info_matches {
    my ($self, $table, $info) = @_;

    my $table_schema = $table->schema;
    $table_schema = 'main' if !defined $table_schema;
    return $info->{TABLE_SCHEM} eq $table_schema
        && $info->{TABLE_NAME}  eq $table->name;
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
