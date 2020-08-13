# curbside-clojure-beam

A Clojure library for Apache Beam.

## Tests

```
docker-compose up
lein test
```

## Code formatting

Run `lein fix` to format Clojure code. 

## Releasing to Artifactory

Releases are performed by CI/Jenkins (not from local developer machines):

https://jk.curbside.com/job/Jobs/job/Flow/job/Release%20curbside-clojure-beam%20patch/

Replace `release-patch` with `release-minor` or `release-major` as needed. 
This determines which of the version numbers will be changed in `project.clj` (the version number format is MAJOR.MINOR.PATCH).

This release job will:
    1. Update the project version, 
    2. Create a git release tag
    3. Deploy a _released_ jar (i.e., with a _non-SNAPSHOT_ version) to `Github packages`.

