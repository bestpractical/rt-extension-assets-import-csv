use inc::Module::Install;


RTx 'RT-Extension-Assets-Import-CSV';
all_from 'lib/RT/Extension/Assets/Import/CSV.pm';
readme_from 'lib/RT/Extension/Assets/Import/CSV.pm';
license  'gplv2';

# XXX: This should be reported by M::I::RTx
my ($lib_path) = $INC{'RT.pm'} =~ /^(.*)[\\\/]/;
my $local_lib_path = "$RT::LocalPath/lib";
unshift @INC, $local_lib_path, $lib_path;
substitute( {
        RT_LIB_PATH => join( ' ', $local_lib_path, $lib_path ),
    },
    'bin/rt-assets-import-csv',
);

requires 'RT::Extension::Assets';
requires 'Text::CSV_XS';

sign;
WriteAll;
