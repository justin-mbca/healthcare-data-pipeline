# Example: Terraform for AWS Glue
resource "aws_glue_job" "ehr_etl" {
  name     = "ehr-etl-job"
  role_arn = var.glue_role_arn
  command {
    name            = "glueetl"
    script_location = "s3://my-bucket/scripts/ehr_etl.py"
    python_version  = "3"
  }
}
