# CARibdis

<p align="center"><img src="logo.png" alt="logo" width="250" height="250" /></p>

CARibdis swallows IPLD CAR files and does things with them.

This project is named after https://en.wikipedia.org/wiki/Charybdis ("Caribdis" in Spanish).

## Install

```
go install github.com/hsanjuan/caribdis@latest
```

## Usage

```
$ caribdis --help
NAME:
   caribdis - CAR files passing by will be swallowed

USAGE:
   caribdis [global options] [subcommand]...

DESCRIPTION:

   caribdis is a command-line tool to work with CAR files.


COMMANDS:
   cat      Concatenate CAR files
   ls       List blocks or roots in CAR files
   roots    List roots in CAR files
   ...
   ...
   help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --help, -h  show help
```
