
# Create an S3 bucket
resource "aws_s3_bucket" "basic_bucket" {
  bucket = "johndauphine-delete-me"  # Replace with a unique bucket name
  acl    = "private"
}
