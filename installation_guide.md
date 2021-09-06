# Installation Guide for MacOS #

1. Ensure you have Python installed and validly added to your path
  * If you do not already have it installed, the recommended way is to use
  the installer located [here.](https://www.python.org/ftp/python/3.7.9/python-3.7.9-macosx10.9.pkg)
2. Install other required software
  * XCode: `xcode-select install`
  * Homebrew: `/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"`
  * SASL libraries: `brew install cyrus-sasl`
3. Setup environment variables
  * Lake zone variables:
  ```
  export HC_LANDING_ZONE_BUCKET="nhds-data-lake-landing-zone"
  export HC_STAGING_ZONE_BUCKET="nhds-data-lake-staging-zone"
  export HC_EXPERIMENTAL_ZONE_BUCKET="nhds-data-lake-experimental-zone"
  export HC_CURATED_ZONE_BUCKET="nhds-data-lake-curated-zone"
  ```
  * SSM path for pulling credentials - primarily for querying Salesforce directly
  ```
  export HC_SF_SSM_PATH="/prod/lead-routing-serverless-functions/salesforce-data-science/"
  ```
  * AWS default region - this is used by a variety of things, so it may already be set
  ```
  export AWS_DEFAULT_REGION="us-east-1"
  ```
  * If you have not already set up your AWS access credentials, you can do
  so via environment variables if you wish. Setting them up this specific way
  (as opposed to, for example, using the CLI config tool) is not required,
  but setting them up in SOME way is required for writing data to tables.
  ```
  export AWS_ACCESS_KEY_ID="<your_access_key_id>"
  export AWS_SECRET_ACCESS_KEY="<your_secret_access_key>"
  ```
