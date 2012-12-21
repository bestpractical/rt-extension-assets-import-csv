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

    my $unique = RT->Config->Get('AssetsImportUniqueCF');
    unless ($unique) {
        RT->Logger->error(
            'Missing identified field, please set config AssetsImportUniqueCF'
        );
        return (0,0,0);
    }

    my $unique_cf = RT::CustomField->new( $args{CurrentUser} );
    $unique_cf->LoadByCols(
        Name       => $unique,
        LookupType => 'RT::Asset',
    );
    unless ($unique_cf->id) {
        RT->Logger->error( "Can't find custom field $unique for RT::Assets" );
        return (0, 0, 0);
    }

    my $field2csv = RT->Config->Get('AssetsImportFieldMapping');
    my $csv2fields = {};
    push @{$csv2fields->{ $field2csv->{$_} }}, $_ for keys %{$field2csv};

    my %cfmap;
    for my $fieldname (keys %{ $field2csv }) {
        if ($fieldname =~ /^CF\.(.*)/) {
            my $cfname = $1;
            my $cf = RT::CustomField->new( $args{CurrentUser} );
            $cf->LoadByCols(
                Name       => $cfname,
                LookupType => 'RT::Asset',
            );
            if ( $cf->id ) {
                $cfmap{$cfname} = $cf->id;
            } else {
                RT->Logger->warning(
                    "Missing custom field $cfname for column $field2csv->{$fieldname}, skipping");
                delete $field2csv->{$fieldname};
            }
        } elsif ($fieldname !~ /^(Name|Status|Description|Created|LastUpdated|Owner)$/) {
            RT->Logger->warning(
                "Unknown asset field $fieldname for column $field2csv->{$fieldname}, skipping");
            delete $field2csv->{$fieldname};
        }
    }

    my @required_columns = ( $field2csv->{"CF.$unique"} );

    my @items = $class->parse_csv( $args{File} );
    unless (@items) {
        RT->Logger->warning( "No items found in file $args{File}" );
        return (0, 0, 0);
    }

    RT->Logger->debug( "Found unused column '$_'" )
        for grep {not $csv2fields->{$_}} keys %{ $items[0] };
    RT->Logger->warning( "No column $_ found for @{$csv2fields->{$_}}" )
        for grep {not exists $items[0]->{$_} } keys %{ $csv2fields };

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

        my $assets = RT::Assets->new( $args{CurrentUser} );
        my $id_value = $item->{$field2csv->{"CF.$unique"}};
        $assets->LimitCustomField(
            CUSTOMFIELD => $unique_cf->id,
            VALUE       => $id_value,
        );

        if ( $assets->Count ) {
            if ( $assets->Count > 1 ) {
                RT->Logger->warning(
                    "Found multiple assets for $unique = $id_value"
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

            my $asset = $assets->First;
            for my $field ( keys %$field2csv ) {
                my $value = $item->{$field2csv->{$field}};
                next unless defined $value and length $value;
                if ($field =~ /^CF\.(.*)/) {
                    my $cfname = $1;
                    my ($ok, $msg) = $asset->AddCustomFieldValue(
                        Field => $cfmap{$cfname},
                        Value => $value,
                    );
                    unless ($ok) {
                        RT->Logger->error("Failed to set CF $cfname to $value for row $i: $msg");
                    }
                } elsif ($asset->$field ne $value) {
                    my $method = "Set" . $field;
                    my ($ok, $msg) = $asset->$method( $value );
                    unless ($ok) {
                        RT->Logger->error("Failed to set $field to $value for row $1: $msg");
                    }
                }
            }
            $updated++;
        } else {
            my $asset = RT::Asset->new( $args{CurrentUser} );
            my %args;

            for my $field (keys %$field2csv ) {
                my $value = $item->{$field2csv->{$field}};
                next unless defined $value and length $value;
                if ($field =~ /^CF\.(.*)/) {
                    my $cfname = $1;
                    $args{"CustomField-".$cfmap{$cfname}} = $value;
                } else {
                    $args{$field} = $value;
                }
            }

            my ($ok, $msg) = $asset->Create( %args );
            if ($ok) {
                $created++;
            } else {
                RT->Logger->error("Failed to create asset for row $i: $msg");
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

    Set( $AssetsImportUniqueCF, 'Service Tag' );
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
