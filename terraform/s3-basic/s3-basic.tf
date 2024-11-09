
# Create an S3 bucket
resource "aws_s3_bucket" "upload_bucket" {
  bucket = "johndauphine-delete-me" # Replace with a unique bucket name
  acl    = "private"

  # Enable versioning
  versioning {
    enabled = true
  }
  tags = {
    created-by ="johnd"
  }
}
