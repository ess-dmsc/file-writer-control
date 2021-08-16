# Version changes

## Version 1.1.7

* Fixed command timeout configuration.
* Fixed serious datetime bug.

## Version 1.1.6

* Added the ability to configure the command-timeout.
* New commands now get the initial state: "waiting for response" when they are sent.

## Version 1.1.4

* Simplify importing of classes.
* Fixed issue with re-used job identifiers.

## Version 1.1.3

* Added the version to the installed package.
* Minor exception message improvements.
* Minor documentation updates.

## Version 1.1.2

* Slightly better exceptions when unable to connect to a broker.

## Version 1.0.2

* Some code re-factoring and minor improvements.
* Fix of leaking memory: jobs, commands and workers will now be cleaned up if not heard from for an hour.
* Added continuous integration.
* `is_done()` calls may now throw an exception if an error has been encountered or a command has timed out. In the case
    of job handlers, only a failing/timed out start command will result in an exception being thrown.
* It is now possible to access the command response codes as `response_code` from the command handler.

## Version 1.0.1

Minor deployment bug fix.

## Version 1.0

Initial release.