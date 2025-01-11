python sfmc_reconciliation.py \
  --landing-s3-uri s3://471112572806-athenixlake-qa-bronze/sfmc_data/SFMC_Recon/SFMC_Reconciliation_Data20250108.csv \
  --athena-output-bucket 471112572806-athenixlake-qa-athena/athena-query-results/ \
  --sns-topic-arn arn:aws:sns:us-east-1:471112572806:sfmc-topic-qa \
  --athena-date-column modifieddate \
  --report-s3-uri s3://471112572806-athenixlake-qa-bronze/sfmc_data/SFMC_Recon/reports/reconciliation_2025-01-08.csv \
  --athena-database prestage \
  --athena-table-pattern "sfmc_{business_unit}_{object}" \
  --region us-east-1 \
  --threshold 10.0