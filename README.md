# s3fs
S3 fuse mount to Local FS

# Setup for AWS-IAM

- Create User for IAM
- Generate Access Key Credentials

# Install AWS CLI

- sudo apt install -y curl unzip python-is-python3
- curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
- unzip awscli-bundle.zip
- sudo ./awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
- sudo aws configure
- (input credentials)

# Build & Install

- task build install

# Direct Install

- go install github.com/nobonobo/s3fs/amd/s3mount@latest

# Create Bucket and Root folder

S3-Console:
- Create Bucket: "global-unique-bucket"
- Create Folder: "root/"

or

CLI:
- aws s3 mb s3://global-unique-bucket --region region-id
- aws s3api put-object --bucket "global-unique-bucket" --key "root/"

# Run

- mkdir dest
- sudo s3mount -region "region-id" -bucket "global-unique-bucket" -root "root" dest
