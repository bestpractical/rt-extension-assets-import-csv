use inc::Module::Install;


RTx 'RT-Extension-Assets-Import-CSV';
all_from 'lib/RT/Extension/Assets/Import/CSV.pm';
readme_from 'lib/RT/Extension/Assets/Import/CSV.pm';
license  'gplv2';

requires 'RT::Extension::Assets';
requires 'Text::CSV_XS';

sign;
WriteAll;
