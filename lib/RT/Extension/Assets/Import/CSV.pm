use strict;
use warnings;

package RT::Extension::Assets::Import::CSV;

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
        RT->Logger->error(
            "Unable to load identified field, please check that it exists and the exact name");
        return (0,0,0);
    }

    my @items = $class->parse_csv( $args{File} );

    my $map   = RT->Config->Get('AssetsImportFieldMapping');
    my $r_map = { reverse %$map };

    my $required_fields = RT->Config->Get('AssetsImportRequiredFields') || [];

    my ( $created, $updated, $skipped ) = (0) x 3;

    my $first = 1;

    RT->Logger->debug( 'Found ' . scalar(@items) . ' record(s)' );

    my $i = 0;
  OUTER:
    for my $item (@items) {
        $i++;
        my @fields;

        if ($first) {
            for my $field (keys %$item) {
                unless ($map->{$field}) {
                    RT->Logger->debug("No mapping for import field '$field', skipping");
                    next;
                }
                push @fields, $field;
            }

            for my $field (@fields) {
                my $cf = RT::CustomField->new( $args{CurrentUser} );
                $cf->LoadByCols(
                    Name       => $map->{$field},
                    LookupType => 'RT::Asset',
                );
                unless ( $cf->id ) {
                    RT->Logger->warning(
                        "Missing custom field $map->{$field}, skipping");
                }
            }
            $first = 0;
        }

        for my $field (@$required_fields) {
            unless ( $item->{ $r_map->{$field} } ) {
                RT->Logger->warning(
                    "Missing $r_map->{$field} at row $i, skipping");
                $skipped++;
                next OUTER;
            }
        }

        my $asset;
        my $assets = RT::Assets->new( $args{CurrentUser} );
        $assets->LimitCustomField(
            CUSTOMFIELD => $identified_cf->id,
            VALUE       => $item->{$r_map->{$identified_field}},
        );

        if ( $assets->Count ) {
            if ( $assets->Count > 1 ) {
                RT->Logger->warning(
                    'Found multiple assets with the condition');
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
        }
        else {
            $asset = RT::Asset->new( $args{CurrentUser} );
            $asset->Create();
            $created++;
        }

        for my $field (@fields) {
            if ( defined $item->{$field} and length $item->{$field} ) {
                $asset->AddCustomFieldValue(
                    Field => $map->{$field},
                    Value => $item->{$field},
                );
            }
        }
    }
    return ( $created, $updated, $skipped );
}

sub parse_csv {
    my $class = shift;
    my $file  = shift;
    require Text::CSV;

    my @rows;
    my $csv = Text::CSV->new( { binary => 1 } );

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

    Set( $AssetsImportIdentifiedField, 'Service Tag', );
    Set( @AssetsImportRequiredFields, 'Service Tag', );
    Set( %AssetsImportFieldMapping,
        # 'CSV field name'  => 'RT custom field name'
        'serviceTag'        => 'Service Tag',
        'building'          => 'Location',
        'serialNo'          => 'Serial #',
    );

=item Clear your mason cache

    rm -rf /opt/rt4/var/mason_data/obj

=item Restart your webserver

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
