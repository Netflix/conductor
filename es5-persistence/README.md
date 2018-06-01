## Usage

1. In `server/build.gradle` file,  change to `compile project(':conductor-es5-persistence')` in dependencies, if not present already.
2. In `settings.gradle` file, change to `es5-persistence`, if not present already.
Make sure only one implementation of Elasticsearch (i.e es{v}-persistence) exists in gradle dependency tree.
3. Generate new locks with `gradlew generateLocks saveLocks`.
