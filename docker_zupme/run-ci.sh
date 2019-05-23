rm `find . -name *.lock`

./gradlew generateLock updateLock saveLock

./gradlew clean build