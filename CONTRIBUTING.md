# How to Contribute

## Filing issues

If you have found any bugs or issues with Guacamole or have any feature requests, please feel free to file an issue against the project.  When doing so, please follow the guidelines below:

- To report any bugs, issues, or feature requests, [please open an issue](https://github.com/hammerlab/guacamole/issues)
- Please check the [current open issues](https://github.com/hammerlab/guacamole/issues) to see if the request already exists
- If you are filing a bug report, please describe the version of Guacamole, Hadoop and Spark that is being used.

## Submitting Code

We are happy to receive contributions. Please follow the guidelines below.

- All work should be done in a branch in a forked repository of Guacamole.
- Contributions should come in the form of GitHub pull requests.
- If the work is based on an existing [issue](https://github.com/hammerlab/guacamole/issues), please reference the issue in the PR.
- If the PR completes the issue, please state "Closes #XYZ" or "Fixes #XYZ", where XYZ is the issue number.
- For any large changes, please file an issue and allow for discussion before submitting the PR.
- All pull requests should describe the change in detail along with clear and concise commit messages.
- All pull requests should come with unit tests attached and pass all existing tests.
- All PRs will be reviewed after the the tests pass - please run the the unit tests locally using `mvn test`.  Travis CI will run the tests as well when the PR is opened.
- For any code in performance critical areas, please provide benchmarking information.

## License

All contributions must be original work and agree to the terms of [APL v2.0](https://github.com/hammerlab/guacamole/blob/master/LICENSE)