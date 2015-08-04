#NDbUnit is a .NET library for managing database state during unit testing.#

[![Build-Status](https://img.shields.io/teamcity/https/build.fubar-dev.com/s/NDbUnit_ReleaseBuild.svg?label=TeamCity)](https://build.fubar-dev.com/viewType.html?buildTypeId=NDbUnit_ReleaseBuild&guest=1)

NDbUnit may be used to increase repeat-ability in unit tests that
interact with a database by ensuring that the database's state is
consistent across execution of tests. NDbUnit does this by allowing
the unit test to perform an operation on a dataset before or after
the execution of a test, thus ensuring a consistent state.

To better understand how NDbUnit works, check out the
[Quick Start Guide](https://github.com/NDbUnit/NDbUnit/wiki/Quick-Start-Guide)

The easiest way to get NDbUnit is via [the NuGet Package Manager](http://nuget.org) from right inside Visual Studio!

**NDbUnit:**
    
* is an open-source Apache 2.0-licensed project
* is written in C#
* compiled against the .NET 4.0 CLR and runs with all full .NET Frameworks of version 4.0 or later
* borrows many ideas from DbUnit, and makes them available for the .NET platform

**NDbUnit supports the following Database Server Targets:**

* Microsoft SQL Server 2005 2008, 2012, 2014 (Express thru Enterprise)
* Microsoft SQL Server CE 2005 and 2008
* Microsoft OleDB-supported databases
* SQLLite
* MySQL
* PostgreSQL through 9.4
* Oracle (XE thru Enterprise, tested up to 12c)

For additional planned features and enhancements, see the
[Project Road-Map](https://github.com/NDbUnit/NDbUnit/wiki/Project-Road-Map).
