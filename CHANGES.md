# Version changes

## Version 1.2.1
* It is now possible to set the maximum message size for the WorkerJobPool. An exception will be thrown if this size is
    exceeded when starting a new job.

## Version 1.2.0
* Added code for making mata data from status messages and "wrdn" messages available.

## Version 1.1.7

* Fixed command timeout configuration.
* Fixed serious datetime bug.
* Reverted removal of `stop_now()`, it now directly calls the `abort_write_job()` function.
* Reverted removal of `try_send_stop_now()`, it now directly calls the `try_send_abort()` function.

## Version 1.1.6

* Added the ability to configure the command-timeout.
* New commands now get the initial state: "waiting for response" when they are sent.

## Version 1.1.5

* Implemented command line interface script to library.

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