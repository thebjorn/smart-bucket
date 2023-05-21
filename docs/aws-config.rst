AWS configuration
------------------
You should probably create a separate user for this, and only give it
access to the bucket you want to use.

You can use the following policy as a starting point:

.. code-block:: json

    {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Action": [
                    "s3:ListBucket"
                ],
                "Effect": "Allow",
                "Resource": "arn:aws:s3:::<bucket-name>"
            },
            {
                "Action": [
                    "s3:DeleteObject",
                    "s3:GetObject",
                    "s3:PutObject",
                    "s3:PutObjectAcl"
                ],
                "Effect": "Allow",
                "Resource": "arn:aws:s3:::<bucket-name>/*"
            }
        ]
    }
