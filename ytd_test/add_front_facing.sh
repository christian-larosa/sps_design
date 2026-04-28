#!/bin/bash

# ytd_sps_price_index_month.sql
sed -i '' "s/sku_supplier_mapping_for_price_index AS (/sku_supplier_mapping_for_price_index AS (\n    SELECT global_entity_id, sku_id, COALESCE(front_facing_level_one, '_unknown_') AS front_facing_level_one, COALESCE(front_facing_level_two, '_unknown_') AS front_facing_level_two FROM \`dh-darkstores-live.csm_automated_tables.ytd_sps_product\` GROUP BY ALL\n  ),\n  front_facing_price_index AS (/" ytd_sps_price_index_month.sql

# ytd_sps_line_rebate_metrics_month.sql
sed -i '' "s/sp.level_three, '_unknown_') AS l3_master_category,/sp.level_three, '_unknown_') AS l3_master_category,\n    COALESCE(sp.front_facing_level_one, '_unknown_') AS front_facing_level_one,\n    COALESCE(sp.front_facing_level_two, '_unknown_') AS front_facing_level_two,/" ytd_sps_line_rebate_metrics_month.sql

# ytd_sps_listed_sku_month.sql
sed -i '' "s/sp.level_three, '_unknown_') AS l3_master_category,/sp.level_three, '_unknown_') AS l3_master_category,\n    COALESCE(sp.front_facing_level_one, '_unknown_') AS front_facing_level_one,\n    COALESCE(sp.front_facing_level_two, '_unknown_') AS front_facing_level_two,/" ytd_sps_listed_sku_month.sql

# ytd_sps_shrinkage_month.sql
sed -i '' "s/sp.level_three, '_unknown_') AS l3_master_category,/sp.level_three, '_unknown_') AS l3_master_category,\n    COALESCE(sp.front_facing_level_one, '_unknown_') AS front_facing_level_one,\n    COALESCE(sp.front_facing_level_two, '_unknown_') AS front_facing_level_two,/" ytd_sps_shrinkage_month.sql

