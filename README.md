# SafetyIncidentPipleine

The purpose of this pipeline is to capture messages from the safety incident sqs queue and publish them in a digestible format to Redshift to give stakeholders visibility into tracking safety incident rates and the effectiveness of safety incident reduction programs. This pipeline is used for director level reporting to drive down safety incidents within Fulfillment Centers


# Workflow

    - Continuous polling of an SQS queue for new safety incident messages
    - Transformation of data into CSV format
    - Storage of transformed data in Amazon S3
    - Upload of CSV data into Amazon Redshift for further analysis
    - Enables efficient and automated processing of safety incident data
    - Facilitates timely insights and effective management of safety incidents

