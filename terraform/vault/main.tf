resource "vault_aws_secret_backend_role" "conductor" {
  backend         = "aws_${var.enclave}"
  name            = "conductor"
  credential_type = "federation_token"

  # 12h
  default_sts_ttl = 43200

  policy_document = "${data.aws_iam_policy_document.conductor.json}"
}

data "aws_iam_policy_document" "conductor" {
  statement {
    sid = "ProcessingBucketAccess"
    actions = [
      "s3:*",
    ]
    resources =  [
     "arn:aws:s3:::dlx-one-processing-us-west-2-${var.env}/*",
     "arn:aws:s3:::dlx-one-processing-us-west-2-${var.env}"
    ]
  }
}
