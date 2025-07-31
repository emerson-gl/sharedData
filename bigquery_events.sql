SELECT
  -- =====================
  -- 1) Top-level columns
  -- =====================
  t.event_date,
  t.event_timestamp,
  t.event_name,
  t.event_value_in_usd,
  t.event_bundle_sequence_id,
  t.user_pseudo_id,
  t.user_first_touch_timestamp,

  -- ==========================
  -- 2) user_ltv (non-repeated)
  -- ==========================
  t.user_ltv.revenue  AS user_ltv_revenue,
  t.user_ltv.currency AS user_ltv_currency,

  -- =========================================
  -- 3) event_params (subselect for each key)
  --    Using LIMIT 1 because each key typically
  --    appears once per event, but if it appears
  --    multiple times, we pick the first.
  -- =========================================

  -- Example: "action" => string_value only
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'action'
    LIMIT 1
  ) AS param_action,

  -- "batch_ordering_id" => int_value only
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'batch_ordering_id'
    LIMIT 1
  ) AS param_batch_ordering_id,

  -- "batch_page_id" => int_value only
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'batch_page_id'
    LIMIT 1
  ) AS param_batch_page_id,

  -- "campaign" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'campaign'
    LIMIT 1
  ) AS param_campaign,

  -- "category" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'category'
    LIMIT 1
  ) AS param_category,

  -- "channel" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'channel'
    LIMIT 1
  ) AS param_channel,

  -- "content" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'content'
    LIMIT 1
  ) AS param_content,

  -- "coupon" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'coupon'
    LIMIT 1
  ) AS param_coupon,

  -- "currency" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'currency'
    LIMIT 1
  ) AS param_currency,

  -- "engaged_session_event" => int_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'engaged_session_event'
    LIMIT 1
  ) AS param_engaged_session_event,

  -- "engagement_time_msec" => int_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'engagement_time_msec'
    LIMIT 1
  ) AS param_engagement_time_msec,

  -- "entrances" => int_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'entrances'
    LIMIT 1
  ) AS param_entrances,

  -- "ga_session_id" => int_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'ga_session_id'
    LIMIT 1
  ) AS param_ga_session_id,

  -- "ga_session_number" => int_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'ga_session_number'
    LIMIT 1
  ) AS param_ga_session_number,

  -- "gclid" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'gclid'
    LIMIT 1
  ) AS param_gclid,

  -- "ignore_referrer" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'ignore_referrer'
    LIMIT 1
  ) AS param_ignore_referrer,

  -- "label" => string_value, int_value, double_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'label'
    LIMIT 1
  ) AS param_label_string_value,
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'label'
    LIMIT 1
  ) AS param_label_int_value,
  (
    SELECT ep.value.double_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'label'
    LIMIT 1
  ) AS param_label_double_value,

  -- "link_classes" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'link_classes'
    LIMIT 1
  ) AS param_link_classes,

  -- "link_domain" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'link_domain'
    LIMIT 1
  ) AS param_link_domain,

  -- "link_text" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'link_text'
    LIMIT 1
  ) AS param_link_text,

  -- "link_url" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'link_url'
    LIMIT 1
  ) AS param_link_url,

  -- "medium" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'medium'
    LIMIT 1
  ) AS param_medium,

  -- "network" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'network'
    LIMIT 1
  ) AS param_network,

  -- "outbound" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'outbound'
    LIMIT 1
  ) AS param_outbound,

  -- "page_location" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'page_location'
    LIMIT 1
  ) AS param_page_location,

  -- "page_referrer" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'page_referrer'
    LIMIT 1
  ) AS param_page_referrer,

  -- "page_title" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'page_title'
    LIMIT 1
  ) AS param_page_title,

  -- "promo_code" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'promo_code'
    LIMIT 1
  ) AS param_promo_code,

  -- "session_engaged" => string_value, int_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'session_engaged'
    LIMIT 1
  ) AS param_session_engaged_string_value,
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'session_engaged'
    LIMIT 1
  ) AS param_session_engaged_int_value,

  -- "shipping" => int_value, double_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'shipping'
    LIMIT 1
  ) AS param_shipping_int_value,
  (
    SELECT ep.value.double_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'shipping'
    LIMIT 1
  ) AS param_shipping_double_value,

  -- "source" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'source'
    LIMIT 1
  ) AS param_source,

  -- "srsltid" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'srsltid'
    LIMIT 1
  ) AS param_srsltid,

  -- "stateData" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'stateData'
    LIMIT 1
  ) AS param_stateData,

  -- "tax" => int_value, double_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'tax'
    LIMIT 1
  ) AS param_tax_int_value,
  (
    SELECT ep.value.double_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'tax'
    LIMIT 1
  ) AS param_tax_double_value,

  -- "term" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'term'
    LIMIT 1
  ) AS param_term,

  -- "transaction_id" => string_value
  (
    SELECT ep.value.string_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'transaction_id'
    LIMIT 1
  ) AS param_transaction_id,

  -- "value" => int_value, double_value
  (
    SELECT ep.value.int_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'value'
    LIMIT 1
  ) AS param_value_int_value,
  (
    SELECT ep.value.double_value
    FROM UNNEST(t.event_params) ep
    WHERE ep.key = 'value'
    LIMIT 1
  ) AS param_value_double_value,

  -- ==================================
  -- 4) device (flatten nested web_info)
  -- ==================================
  t.device.category                  AS device_category,
  t.device.mobile_brand_name         AS device_mobile_brand_name,
  t.device.mobile_model_name         AS device_mobile_model_name,
  t.device.mobile_marketing_name     AS device_mobile_marketing_name,
  t.device.mobile_os_hardware_model  AS device_mobile_os_hardware_model,
  t.device.operating_system          AS device_operating_system,
  t.device.operating_system_version  AS device_operating_system_version,
  t.device.language                  AS device_language,
  t.device.is_limited_ad_tracking    AS device_is_limited_ad_tracking,
  t.device.web_info.browser          AS device_web_info_browser,
  t.device.web_info.browser_version  AS device_web_info_browser_version,
  t.device.web_info.hostname         AS device_web_info_hostname,

  -- ===================
  -- 5) geo
  -- ===================
  t.geo.city       AS geo_city,
  t.geo.country    AS geo_country,
  t.geo.continent  AS geo_continent,
  t.geo.region     AS geo_region,
  t.geo.sub_continent AS geo_sub_continent,
  t.geo.metro      AS geo_metro,

  -- ==========================
  -- 6) traffic_source
  -- ==========================
  t.traffic_source.name   AS traffic_source_name,
  t.traffic_source.medium AS traffic_source_medium,
  t.traffic_source.source AS traffic_source_source,

  t.stream_id,
  t.platform,

  -- ======================
  -- 7) ecommerce
  -- ======================
  t.ecommerce.total_item_quantity    AS ecommerce_total_item_quantity,
  t.ecommerce.purchase_revenue_in_usd AS ecommerce_purchase_revenue_in_usd,
  t.ecommerce.purchase_revenue       AS ecommerce_purchase_revenue,
  t.ecommerce.refund_value_in_usd    AS ecommerce_refund_value_in_usd,
  t.ecommerce.refund_value           AS ecommerce_refund_value,
  t.ecommerce.shipping_value_in_usd  AS ecommerce_shipping_value_in_usd,
  t.ecommerce.shipping_value         AS ecommerce_shipping_value,
  t.ecommerce.tax_value_in_usd       AS ecommerce_tax_value_in_usd,
  t.ecommerce.tax_value              AS ecommerce_tax_value,
  t.ecommerce.unique_items           AS ecommerce_unique_items,
  t.ecommerce.transaction_id         AS ecommerce_transaction_id,

  -- ===================================================
  -- 8) items (CROSS JOIN for each item in the array)
  --    One row per item
  -- ===================================================
  i.item_id,
  i.item_name,
  i.item_brand,
  i.item_variant,
  i.item_category,
  i.item_category2,
  i.item_category3,
  i.item_category4,
  i.item_category5,
  i.price_in_usd,
  i.quantity,
  i.item_revenue_in_usd,
  i.item_revenue,
  i.item_refund_in_usd,
  i.item_refund,
  i.coupon,
  i.affiliation,
  i.location_id,
  i.item_list_id,
  i.item_list_name,
  i.item_list_index,
  i.promotion_id,
  i.promotion_name,
  i.creative_name,
  i.creative_slot,

  -- ======================================
  -- 9) collected_traffic_source
  -- ======================================
  t.collected_traffic_source.gclid                 AS cts_gclid,
  t.collected_traffic_source.srsltid               AS cts_srsltid,
  t.collected_traffic_source.manual_source         AS cts_manual_source,
  t.collected_traffic_source.manual_term           AS cts_manual_term,
  t.collected_traffic_source.manual_campaign_name  AS cts_manual_campaign_name,
  t.collected_traffic_source.manual_content        AS cts_manual_content,
  t.collected_traffic_source.manual_medium         AS cts_manual_medium,

  -- =========================================
  -- 10) session_traffic_source_last_click
  --     -> manual_campaign
  -- =========================================
  t.session_traffic_source_last_click.manual_campaign.campaign_id AS stslc_manual_campaign_campaign_id,
  t.session_traffic_source_last_click.manual_campaign.campaign_name AS stslc_manual_campaign_campaign_name,
  t.session_traffic_source_last_click.manual_campaign.source        AS stslc_manual_campaign_source,
  t.session_traffic_source_last_click.manual_campaign.medium        AS stslc_manual_campaign_medium,
  t.session_traffic_source_last_click.manual_campaign.term          AS stslc_manual_campaign_term,
  t.session_traffic_source_last_click.manual_campaign.content       AS stslc_manual_campaign_content,
  t.session_traffic_source_last_click.manual_campaign.source_platform AS stslc_manual_campaign_source_platform,
  t.session_traffic_source_last_click.manual_campaign.creative_format AS stslc_manual_campaign_creative_format,
  t.session_traffic_source_last_click.manual_campaign.marketing_tactic AS stslc_manual_campaign_marketing_tactic,

  -- -> google_ads_campaign
  t.session_traffic_source_last_click.google_ads_campaign.customer_id   AS stslc_google_ads_campaign_customer_id,
  t.session_traffic_source_last_click.google_ads_campaign.account_name   AS stslc_google_ads_campaign_account_name,
  t.session_traffic_source_last_click.google_ads_campaign.campaign_id    AS stslc_google_ads_campaign_campaign_id,
  t.session_traffic_source_last_click.google_ads_campaign.campaign_name  AS stslc_google_ads_campaign_campaign_name,
  t.session_traffic_source_last_click.google_ads_campaign.ad_group_id    AS stslc_google_ads_campaign_ad_group_id,
  t.session_traffic_source_last_click.google_ads_campaign.ad_group_name  AS stslc_google_ads_campaign_ad_group_name,

  -- -> cross_channel_campaign
  t.session_traffic_source_last_click.cross_channel_campaign.campaign_id AS stslc_cross_channel_campaign_campaign_id,
  t.session_traffic_source_last_click.cross_channel_campaign.campaign_name AS stslc_cross_channel_campaign_campaign_name,
  t.session_traffic_source_last_click.cross_channel_campaign.source         AS stslc_cross_channel_campaign_source,
  t.session_traffic_source_last_click.cross_channel_campaign.medium         AS stslc_cross_channel_campaign_medium,
  t.session_traffic_source_last_click.cross_channel_campaign.source_platform AS stslc_cross_channel_campaign_source_platform,
  t.session_traffic_source_last_click.cross_channel_campaign.default_channel_group AS stslc_cross_channel_campaign_default_channel_group,
  t.session_traffic_source_last_click.cross_channel_campaign.primary_channel_group AS stslc_cross_channel_campaign_primary_channel_group,

  -- -> sa360_campaign (not explicitly listed above, but you can add if needed)

  -- -> cm360_campaign
  t.session_traffic_source_last_click.cm360_campaign.campaign_id   AS stslc_cm360_campaign_campaign_id,
  t.session_traffic_source_last_click.cm360_campaign.campaign_name AS stslc_cm360_campaign_campaign_name,
  t.session_traffic_source_last_click.cm360_campaign.source        AS stslc_cm360_campaign_source,
  t.session_traffic_source_last_click.cm360_campaign.medium        AS stslc_cm360_campaign_medium,
  t.session_traffic_source_last_click.cm360_campaign.account_id    AS stslc_cm360_campaign_account_id,
  t.session_traffic_source_last_click.cm360_campaign.account_name  AS stslc_cm360_campaign_account_name,
  t.session_traffic_source_last_click.cm360_campaign.advertiser_id AS stslc_cm360_campaign_advertiser_id,
  t.session_traffic_source_last_click.cm360_campaign.advertiser_name AS stslc_cm360_campaign_advertiser_name,
  t.session_traffic_source_last_click.cm360_campaign.creative_id   AS stslc_cm360_campaign_creative_id,
  t.session_traffic_source_last_click.cm360_campaign.creative_format AS stslc_cm360_campaign_creative_format,
  t.session_traffic_source_last_click.cm360_campaign.creative_name AS stslc_cm360_campaign_creative_name,
  t.session_traffic_source_last_click.cm360_campaign.creative_type AS stslc_cm360_campaign_creative_type,
  t.session_traffic_source_last_click.cm360_campaign.creative_type_id AS stslc_cm360_campaign_creative_type_id,
  t.session_traffic_source_last_click.cm360_campaign.creative_version AS stslc_cm360_campaign_creative_version,
  t.session_traffic_source_last_click.cm360_campaign.placement_id  AS stslc_cm360_campaign_placement_id,
  t.session_traffic_source_last_click.cm360_campaign.placement_cost_structure AS stslc_cm360_campaign_placement_cost_structure,
  t.session_traffic_source_last_click.cm360_campaign.placement_name AS stslc_cm360_campaign_placement_name,
  t.session_traffic_source_last_click.cm360_campaign.rendering_id  AS stslc_cm360_campaign_rendering_id,
  t.session_traffic_source_last_click.cm360_campaign.site_id       AS stslc_cm360_campaign_site_id,
  t.session_traffic_source_last_click.cm360_campaign.site_name     AS stslc_cm360_campaign_site_name,

  -- -> dv360_campaign (add if needed)

  -- ===========================
  -- 11) Other top-level fields
  -- ===========================
  t.is_active_user,
  t.batch_event_index,
  t.batch_page_id,
  t.batch_ordering_id

FROM `sessioninfo.analytics_312502432.events_*` AS t
LEFT JOIN UNNEST(t.items) AS i
WHERE _TABLE_SUFFIX BETWEEN '{{ start_date }}' AND '{{ today }};'
