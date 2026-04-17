This Project helps us to change the tier of your SQL Server Elastic Pool, in case you want to automate it via Python to save cost.

Use the following command to execute the change.

python change_sql_elastic_pool_tier.py \
  --subscription-id <Your_Subscription_ID> \
  --resource-group <Your_RG> \
  --server-name <Your_SQL_Server_name> \
  --pool-name <Your_SQL_Server_Elastic-Pool_name> \
  --target-tier Basic \
  --pool-dtu 100 \
  --db-min-dtu 0 \
  --db-max-dtu 5 \
  --pool-max-size 10485760000 \
  --dry-run \
  --prefer-az-cli \
  --auto-adjust

Remove --dry-run for actual implemenation.

