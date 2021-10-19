# GCS External Storage Module 

This module uses google cloud storage sdk to store and retrieve workflows/tasks input/output payload that
went over the thresholds defined in properties named `conductor.[workflow|task].[input|output].payload.threshold.kb`.

## Configuration

### Usage

Cf. Documentation [External Payload Storage](https://netflix.github.io/conductor/externalpayloadstorage/#azure-blob-storage)

### Example
```properties
conductor.external-payload-storage.type=gcs
conductor.external-payload-storage.gcs.googleApplicationCredentialsPath=path/to/service-account.json
conductor.external-payload-storage.gcs.bucketname=my-bucket
conductor.external-payload-storage.gcs.projectid=some-gcp=project-id
conductor.external-payload-storage.gcs.signedurlexpirationseconds=360
```

## Testing

You can use [LocalStorageHelper](https://github.com/googleapis/google-cloud-java/blob/main/TESTING.md#testing-code-that-uses-storage) to simulate an Google Storage.
