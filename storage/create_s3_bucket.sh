#!/bin/bash
# Script to create S3 bucket for streaming pipeline output

set -e

# Configuration
BUCKET_NAME="${S3_BUCKET_NAME:-streaming-pipeline-output}"
REGION="${AWS_REGION:-us-east-1}"
PREFIX="${S3_PREFIX:-streaming-output}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}Creating S3 bucket for streaming pipeline...${NC}"
echo "Bucket name: $BUCKET_NAME"
echo "Region: $REGION"
echo "Prefix: $PREFIX"
echo ""

# Check if AWS CLI is installed
if ! command -v aws &> /dev/null; then
    echo -e "${RED}Error: AWS CLI is not installed${NC}"
    echo "Please install AWS CLI: https://aws.amazon.com/cli/"
    exit 1
fi

# Check AWS credentials
if ! aws sts get-caller-identity &> /dev/null; then
    echo -e "${RED}Error: AWS credentials not configured${NC}"
    echo "Please configure AWS credentials using:"
    echo "  aws configure"
    echo "  or"
    echo "  export AWS_ACCESS_KEY_ID=your_key"
    echo "  export AWS_SECRET_ACCESS_KEY=your_secret"
    exit 1
fi

# Create bucket
echo -e "${YELLOW}Creating S3 bucket: $BUCKET_NAME...${NC}"
if aws s3api head-bucket --bucket "$BUCKET_NAME" 2>/dev/null; then
    echo -e "${YELLOW}Bucket $BUCKET_NAME already exists${NC}"
else
    if [ "$REGION" == "us-east-1" ]; then
        # us-east-1 doesn't require LocationConstraint
        aws s3api create-bucket --bucket "$BUCKET_NAME" --region "$REGION"
    else
        aws s3api create-bucket \
            --bucket "$BUCKET_NAME" \
            --region "$REGION" \
            --create-bucket-configuration LocationConstraint="$REGION"
    fi
    echo -e "${GREEN}Bucket $BUCKET_NAME created successfully${NC}"
fi

# Enable versioning (optional but recommended)
echo -e "${YELLOW}Enabling versioning...${NC}"
aws s3api put-bucket-versioning \
    --bucket "$BUCKET_NAME" \
    --versioning-configuration Status=Enabled

# Set lifecycle policy (optional - delete old versions after 90 days)
echo -e "${YELLOW}Setting lifecycle policy...${NC}"
cat > /tmp/lifecycle.json <<EOF
{
    "Rules": [
        {
            "Id": "DeleteOldVersions",
            "Status": "Enabled",
            "Prefix": "$PREFIX/",
            "NoncurrentVersionExpiration": {
                "Days": 90
            }
        },
        {
            "Id": "DeleteIncompleteMultipartUploads",
            "Status": "Enabled",
            "Prefix": "$PREFIX/",
            "AbortIncompleteMultipartUpload": {
                "DaysAfterInitiation": 7
            }
        }
    ]
}
EOF

aws s3api put-bucket-lifecycle-configuration \
    --bucket "$BUCKET_NAME" \
    --lifecycle-configuration file:///tmp/lifecycle.json

rm /tmp/lifecycle.json

# Create prefix directory structure
echo -e "${YELLOW}Creating prefix structure: $PREFIX/...${NC}"
aws s3api put-object \
    --bucket "$BUCKET_NAME" \
    --key "$PREFIX/.keep" \
    --body /dev/null || true

# Set bucket policy for Spark access (if needed)
echo -e "${YELLOW}Configuring bucket permissions...${NC}"
cat > /tmp/bucket-policy.json <<EOF
{
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "AllowSparkAccess",
            "Effect": "Allow",
            "Principal": {
                "AWS": "*"
            },
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                "arn:aws:s3:::$BUCKET_NAME/*",
                "arn:aws:s3:::$BUCKET_NAME"
            ],
            "Condition": {
                "StringEquals": {
                    "aws:SourceAccount": "$(aws sts get-caller-identity --query Account --output text)"
                }
            }
        }
    ]
}
EOF

# Note: Uncomment if you want to apply a bucket policy
# aws s3api put-bucket-policy --bucket "$BUCKET_NAME" --policy file:///tmp/bucket-policy.json
# rm /tmp/bucket-policy.json

echo ""
echo -e "${GREEN}✓ S3 bucket setup completed!${NC}"
echo ""
echo "Bucket details:"
echo "  Name: $BUCKET_NAME"
echo "  Region: $REGION"
echo "  Prefix: $PREFIX"
echo "  S3 Path: s3://$BUCKET_NAME/$PREFIX/"
echo "  S3A Path: s3a://$BUCKET_NAME/$PREFIX/"
echo ""
echo "To use this bucket in Spark streaming job:"
echo "  export S3_BUCKET=$BUCKET_NAME"
echo "  export S3_PREFIX=$PREFIX"
echo "  export AWS_ACCESS_KEY_ID=your_key"
echo "  export AWS_SECRET_ACCESS_KEY=your_secret"
echo ""
