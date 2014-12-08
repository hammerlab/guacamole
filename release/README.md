

## Releasing through Maven

```sh
mvn release:prepare release:perform
```

If you are simply interested in the testing the commands above without actually performing the updates add the `-DdryRun=true` flag.

## Rolling back a release

If there are any issues with the release it can be rolled-back with the following command:

```sh
mvn release:rollback
```

## Cleaning up a release

To remove any temporary files created during hte release process run the following command:

```sh
mvn release:clean
```