# Release numbering strategies

OceanBase Database numbers its releases following these strategies:

- The version number is named in this way:

    ```bash
    MAJOR.MINOR.PATCH
    ```

    Code Status | Stage | Rule | Example
    --- | --- | --- | ---
    Changes that break backward compatibility. | `MAJOR` release | Increase the third digit. | 2.0.0
    Backward compatible new features. | `MINOR` release | Increase the middle digit and reset last digit to zero. | 1.1.0
    Backward compatible bug fixes. | `PATCH` release | Increase the first digit and reset the middle and last digits to zero. | 1.0.1

    > **NOTE**: A `MAJOR.MINOR.PATCH` version is a GA or stable version.
- Descriptions for other semantics:

    Release name | Descriptions
    --- | ---
    Pre-Alpha | A pre-release version before Alpha. A pre-Alpha phase is a less complete version.
    Alpha | A pre-release version.
    Beta | A pre-release version after Alpha. A beta phase is feature complete but likely to contain a number of known or unknown bugs.
    RC | A release candidate (RC) version is a beta version likely to be a stable product.
    GA | A general available version is available for purchase.
    BP | A bundle patch version of a GA version.
    Stable/Release | A stable or release version passed all the test is for production. The remaining bugs are acceptable.

    <!-- Nightly | A nightly version is built automatically. The building takes place at night. -->

## References

- [Semantic Versioning 2.0.0](https://semver.org/)
