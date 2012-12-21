use inc::Module::Install;


RTx 'RT-Extension-Assets-Import';
all_from 'lib/RT/Extension/Assets/Import.pm';
readme_from 'lib/RT/Extension/Assets/Import.pm';
license  'gplv2';

requires 'RT::Extension::Assets';
requires 'Text::CSV';
requires 'Text::CSV_XS';

sign;
WriteAll;
