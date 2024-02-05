WITH hw6 AS
(
SELECT 
      fabd.ad_date,
      fabd.url_parameters,
      COALESCE(fabd.spend, 0) AS spend,
      COALESCE(fabd.impressions, 0) AS impressions,
      COALESCE(fabd.reach, 0) AS readch,
      COALESCE(fabd.clicks, 0) AS clicks,
      COALESCE(fabd.leads, 0) AS leads,  
      COALESCE(fabd.value, 0) AS value
FROM facebook_ads_basic_daily fabd
FULL JOIN facebook_adset fa ON fa.adset_id = fabd.adset_id
FULL JOIN facebook_campaign fc ON fc.campaign_id = fabd.campaign_id   
UNION ALL 
SELECT 
      gabd.ad_date,
      gabd.url_parameters,
      COALESCE(gabd.spend, 0) AS spend,
      COALESCE(gabd.impressions, 0) AS impressions,
      COALESCE(gabd.reach, 0) AS readch,
      COALESCE(gabd.clicks, 0) AS clicks,  
      COALESCE(gabd.leads, 0) AS leads,
      COALESCE(gabd.value, 0) AS value
FROM google_ads_basic_daily gabd 
),
hw7 AS
(
SELECT
      CASE
      	 WHEN DATE_PART('month', ad_date)<=9 THEN CAST(CONCAT(DATE_PART('year', ad_date), '-0', DATE_PART('month', ad_date), '-01') AS date)
      	 ELSE CAST(CONCAT(DATE_PART('year', ad_date), '-', DATE_PART('month', ad_date), '-01') AS date)
      END ad_month,
      CASE
         WHEN LOWER(SUBSTRING(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) = 'nan' THEN NULL 
         ELSE LOWER(SUBSTRING(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)'))
      END utm_campaign,
      SUM(spend) AS total_spend,
      SUM(impressions) AS total_impressions,
      SUM(clicks) AS total_clicks,
      SUM(value) AS total_conversion_value,
      CASE
         WHEN (SUM(impressions)<>0) THEN ROUND((CAST(SUM(spend) AS DECIMAL)/CAST(SUM(impressions) AS decimal)*1000),2) 
         ELSE 0
      END CPM,
      CASE 
      	  WHEN (SUM(spend) <> 0) THEN ROUND(((CAST(SUM(value) AS decimal)-(CAST(SUM(spend) AS decimal)))/CAST(SUM(spend) AS decimal)*100),2)
      	  ELSE 0
      END ROMI,
      CASE 
      	  WHEN (SUM(impressions) <> 0) THEN ROUND(CAST(SUM(clicks) AS decimal)/CAST(SUM(impressions) AS decimal),2)
      	  ELSE 0
      END CTR,
      CASE  
      	  WHEN (SUM(clicks) <> 0) THEN ROUND(CAST(SUM(spend) AS decimal)/(CAST(SUM(clicks) AS decimal)),2)
      	  ELSE 0
      END CPC
FROM hw6
WHERE CASE
         WHEN LOWER(SUBSTRING(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) = 'nan' THEN NULL 
         ELSE LOWER(SUBSTRING(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) 
         END IS NOT NULL
GROUP BY ad_month,
         utm_campaign
),
hw7_lag AS
(
SELECT 
      ad_month,
      utm_campaign,
      total_spend,
      total_impressions,
      total_clicks,
      total_conversion_value,
      CPM,
      CTR,
      ROMI,
      CPC,
      LAG(CPM) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS CPM_PRIV_MONTH,
      LAG(CTR) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS CTR_PRIV_MONTH,
      LAG(ROMI) OVER (PARTITION BY utm_campaign ORDER BY ad_month) AS ROMI_PRIV_MONTH
FROM hw7
)
SELECT 
     ad_month,
     utm_campaign,
     total_spend,
     total_impressions,
     total_clicks,
     total_conversion_value,
     CPM,
     ROMI,
     CTR,
     CPC,
     CASE
     	WHEN CPM_PRIV_MONTH<>0 THEN ROUND((CPM-CPM_PRIV_MONTH)/CPM_PRIV_MONTH*100,1)
     	ELSE 0
     END CPM_DIFF,
     CASE
     	WHEN CTR_PRIV_MONTH<>0 THEN ROUND((CTR-CTR_PRIV_MONTH)/CTR_PRIV_MONTH*100,1)
     	ELSE 0
     END CTR_DIFF,
     CASE
     	WHEN ROMI_PRIV_MONTH<>0 THEN ROUND((ROMI-ROMI_PRIV_MONTH)/ROMI_PRIV_MONTH*100,1)
     	ELSE 0
     END ROMI_DIFF
FROM hw7_lag;
