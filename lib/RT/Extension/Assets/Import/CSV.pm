use strict;
use warnings;

package RT::Extension::Assets::Import::CSV;
use Text::CSV_XS;

our $VERSION = '0.01';

sub run {
    my $class = shift;
    my %args  = (
        CurrentUser => undef,
        File        => undef,
        Update      => undef,
        @_,
    );

    my $identified_field = RT->Config->Get('AssetsImportIdentifiedField');
    unless ($identified_field) {
        RT->Logger->error(
'Missing identified field, please set config AssetsImportIdentifiedField'
        );
        return (0,0,0);
    }

    my $identified_cf = RT::CustomField->new( $args{CurrentUser} );
    $identified_cf->LoadByCols(
        Name       => $identified_field,
        LookupType => 'RT::Asset',
    );
    unless ($identified_cf->id) {
        RT->Logger->error( "Can't find custom field $identified_field for RT::Assets" );
        return (0, 0, 0);
    }

    my $cf2csv = RT->Config->Get('AssetsImportFieldMapping');
    my $csv2cf = { reverse %$cf2csv };
    my %cfmap;
    for my $cfname (keys %{ $cf2csv }) {
        my $cf = RT::CustomField->new( $args{CurrentUser} );
        $cf->LoadByCols(
            Name       => $cfname,
            LookupType => 'RT::Asset',
        );
        if ( $cf->id ) {
            $cfmap{$cfname} = $cf->id;
        } else {
            RT->Logger->warning(
                "Missing custom field $cfname for column $cf2csv->{$cfname}, skipping");
            delete $cf2csv->{$cfname};
        }
    }

    my @required_columns = map { $cf2csv->{$_} } $identified_field;

    my @items = $class->parse_csv( $args{File} );
    unless (@items) {
        RT->Logger->warning( "No items found in file $args{File}" );
        return (0, 0, 0);
    }

    RT->Logger->debug( "Found unused column '$_'" )
        for grep {not $csv2cf->{$_}} keys %{ $items[0] };
    RT->Logger->warning( "No column $_ found for CF ".$csv2cf->{$_} )
        for grep {not exists $items[0]->{$_} } keys %{ $csv2cf };

    RT->Logger->debug( 'Found ' . scalar(@items) . ' record(s)' );
    my ( $created, $updated, $skipped ) = (0) x 3;
    my $i = 1; # Because of header row
    for my $item (@items) {
        $i++;
        next unless grep {/\S/} values %{$item};

        my @missing = grep {not $item->{$_}} @required_columns;
        if (@missing) {
            RT->Logger->warning(
                "Missing value for required column@{[@missing > 1 ? 's':'']} @missing at row $i, skipping");
            $skipped++;
            next;
        }

        my $asset;
        my $assets = RT::Assets->new( $args{CurrentUser} );
        my $id_value = $item->{$cf2csv->{$identified_field}};
        $assets->LimitCustomField(
            CUSTOMFIELD => $identified_cf->id,
            VALUE       => $id_value,
        );

        if ( $assets->Count ) {
            if ( $assets->Count > 1 ) {
                RT->Logger->warning(
                    "Found multiple assets for identifying CF $identified_field = $id_value"
                );
                $skipped++;
                next;
            }
            unless ( $args{Update} ) {
                RT->Logger->debug(
                    "Found existing asset at row $i but without 'Update' option, skipping."
                );
                $skipped++;
                next;
            }

            $asset = $assets->First;
            $updated++;
        } else {
            $asset = RT::Asset->new( $args{CurrentUser} );
            my ($ok, $msg) = $asset->Create();
            if ($ok) {
                $created++;
            } else {
                RT->Logger->error("Failed to create asset for row $i: $msg");
            }
        }

        for my $field ( keys %$item ) {
            if ( defined $item->{$field} and length $item->{$field} and $cfmap{$field} ) {
                my ($ok, $msg) = $asset->AddCustomFieldValue(
                    Field => $cfmap{$field},
                    Value => $item->{$field},
                );
                unless ($ok) {
                    RT->Logger->error("Failed to set CF ".$csv2cf->{$field}." to ".$item->{$field}." for row $i: $msg");
                }
            }
        }
    }
    return ( $created, $updated, $skipped );
}

sub parse_csv {
    my $class = shift;
    my $file  = shift;

    my @rows;
    my $csv = Text::CSV_XS->new( { binary => 1 } );

    open my $fh, '<', $file or die "failed to read $file: $!";
    my $header = $csv->getline($fh);

    my @items;
    while ( my $row = $csv->getline($fh) ) {
        my $item;
        for ( my $i = 0 ; $i < @$header ; $i++ ) {
            if ( $header->[$i] ) {
                $item->{ $header->[$i] } = $row->[$i];
            }
        }

        push @items, $item;
    }

    $csv->eof or $csv->error_diag();
    close $fh;
    return @items;
}

=head1 NAME

RT-Extension-Assets-Import-CSV - RT Assets Import from CSV

=head1 INSTALLATION

=over

=item perl Makefile.PL

=item make

=item make install

May need root permissions

=item make initdb

Only run this the first time you install this module.

If you run this twice, you may end up with duplicate data
in your database.

If you are upgrading this module, check for upgrading instructions
in case changes need to be made to your database.

=item Edit your /opt/rt4/etc/RT_SiteConfig.pm

Add this line:

    Set(@Plugins, qw(RT::Extension::Assets::Import::CSV));

or add C<RT::Extension::Assets::Import::CSV> to your existing C<@Plugins> line.

Configure imported fields:

    Set( $AssetsImportIdentifiedField, 'Service Tag' );
    Set( %AssetsImportFieldMapping,
        # 'RT custom field name' => 'CSV field name'
        'Service Tag'            => 'serviceTag',
        'Location'               => 'building',
        'Serial #'               => 'serialNo',
    );

=back

=head1 AUTHOR

sunnavy <sunnavy@bestpractical.com>

=head1 BUGS

All bugs should be reported via
L<http://rt.cpan.org/Public/Dist/Display.html?Name=RT-Extension-Assets-Import-CSV>
or L<bug-RT-Extension-Assets-Import-CSV@rt.cpan.org>.


=head1 LICENSE AND COPYRIGHT

This software is Copyright (c) 2012 by Best Practical Solutions

This is free software, licensed under:

  The GNU General Public License, Version 2, June 1991

=cut

1;
