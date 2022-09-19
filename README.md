# Looker to BigQuery

The code in this repository allows to export data from Looker to BigQuery.
This task might sound redundant because data presented in Looker are supposed to be in a database like BigQuery in the first place. However, there is one key exception: Lookers `system_activity` explores.

There are multiple reasons why it is worth exporting at least parts of this data:

1. history: the data in `system_activity` is live, or may be stored between 30 and 90 days.
1. performance: the Looker database is a transactional system not optimized for analytics - on a busy instance running usage reports is not the most efficient
1. security: storing the `system_activity` data for longer than 90 days might be required for security reasons
1. flexibility: loading data like `users` and `user_groups` periodically allows time series analysis on entities that are otherwise static.

This package provides all code required to load generic data from Looker to BigQuery.
It also entails the definitions of some example queries.

