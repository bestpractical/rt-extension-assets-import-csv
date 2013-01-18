use inc::Module::Install;


RTx 'RT-Extension-Assets-Import-CSV';
all_from 'lib/RT/Extension/Assets/Import/CSV.pm';
readme_from 'lib/RT/Extension/Assets/Import/CSV.pm';
license  'gplv2';

use Config;
my $perl_path = $Config{perlpath};
$perl_path .= $Config{_exe}
    if $^O ne 'VMS' and $perl_path !~ m/$Config{_exe}$/i;

substitute( {
        RT_LIB_PATH => "$RT::LocalPath/lib " . File::Basename::dirname( $INC{'RT.pm'} ),
        PERL        => $perl_path,
    },
    { sufix => ".in" },
    'bin/rt-assets-import-csv',
);

requires 'RT::Extension::Assets';
requires 'Text::CSV_XS';

sign;
WriteAll;
