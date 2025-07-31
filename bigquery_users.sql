SELECT
  -- Top-level
  pseudo_user_id,
  stream_id,

  -- user_info (flattened)
  user_info.last_active_timestamp_micros,
  user_info.user_first_touch_timestamp_micros,
  user_info.first_purchase_date,

  -- device (flattened)
  device.operating_system,
  device.category,
  device.mobile_brand_name,
  device.mobile_model_name,
  device.unified_screen_name,

  -- geo (flattened)
  geo.city,
  geo.country,
  geo.continent,
  geo.region,

  -- audience info (flattened with subselect for repeated)
  (
    SELECT ARRAY_AGG(STRUCT(
      a.id,
      a.name,
      a.membership_start_timestamp_micros,
      a.membership_expiry_timestamp_micros,
      a.npa
    ))
    FROM UNNEST(audiences) a
  ) AS audience_details,

  -- user_properties (key-value pairs as array of structs)
  (
    SELECT ARRAY_AGG(STRUCT(
      up.key,
      up.value.string_value AS string_value,
      up.value.set_timestamp_micros AS set_timestamp_micros,
      up.value.user_property_name AS user_property_name
    ))
    FROM UNNEST(user_properties) up
  ) AS user_properties,


  -- user_ltv
  user_ltv.revenue_in_usd,
  user_ltv.sessions,
  user_ltv.engagement_time_millis,
  user_ltv.purchases,
  user_ltv.engaged_sessions,
  user_ltv.session_duration_micros,

  -- predictions
  predictions.in_app_purchase_score_7d,
  predictions.purchase_score_7d,
  predictions.churn_score_7d,
  predictions.revenue_28d_in_usd,

  -- privacy_info
  privacy_info.is_limited_ad_tracking AS privacy_is_limited_ad_tracking,
  privacy_info.is_ads_personalization_allowed AS privacy_is_ads_personalization_allowed,

  -- timestamps
  occurrence_date,
  last_updated_date

FROM `sessioninfo.analytics_312502432.pseudonymous_users_*`;