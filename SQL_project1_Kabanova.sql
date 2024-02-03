CREATE OR REPLACE FUNCTION pg_temp.decode_url_part(p varchar) RETURNS varchar AS $$
SELECT convert_from(CAST(E'\\x' || string_agg(CASE WHEN length(r.m[1]) = 1 THEN encode(convert_to(r.m[1], 'SQL_ASCII'), 'hex') ELSE substring(r.m[1] from 2 for 2) END, '') AS bytea), 'UTF8')
FROM regexp_matches($1, '%[0-9a-f][0-9a-f]|.', 'gi') AS r(m);
$$ LANGUAGE SQL IMMUTABLE STRICT;

with hw6 as
(
select 
      fabd.ad_date,
      fabd.url_parameters,
      coalesce(fabd.spend, 0) as spend,
      coalesce(fabd.impressions, 0) as impressions,
      coalesce(fabd.reach, 0) as readch,
      coalesce(fabd.clicks, 0) as clicks,
      coalesce(fabd.leads, 0) as leads,  
      coalesce(fabd.value, 0) as value
from facebook_ads_basic_daily fabd
full join facebook_adset fa on fa.adset_id = fabd.adset_id
full join facebook_campaign fc on fc.campaign_id = fabd.campaign_id   
union all 
select 
      gabd.ad_date,
      gabd.url_parameters,
      coalesce(gabd.spend, 0) as spend,
      coalesce(gabd.impressions, 0) as impressions,
      coalesce(gabd.reach, 0) as readch,
      coalesce(gabd.clicks, 0) as clicks,  
      coalesce(gabd.leads, 0) as leads,
      coalesce(gabd.value, 0) as value
from google_ads_basic_daily gabd 
)
select
      ad_date, 
      case
         when lower(substring(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) = 'nan' then null 
         else lower(substring(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)'))
      end utm_campaign,
      sum(spend) as total_spend,
      sum(impressions) as total_impressions,
      sum(clicks) as total_clicks,
      sum(value) as total_conversion_value,
      case
         when (sum(impressions)<>0) then round((cast(sum(spend) as decimal)/cast(sum(impressions) as decimal)*1000),2) 
         else 0
      end CPM,
      case 
      	  when (sum(spend) <> 0) then round(((cast(sum(value)as decimal)-(cast(sum(spend)as decimal)))/cast(sum(spend)as decimal)*100),2)
      	  else 0
      end ROMI,
      case 
      	  when (sum(impressions) <> 0) then round(cast(sum(clicks)as decimal)/cast(sum(impressions)as decimal),2)
      	  else 0
      end CTR,
      case  
      	  when (sum(clicks) <> 0) then round(cast(sum(spend) as decimal)/(cast(sum(clicks) as decimal)),2)
      	  else 0
      end CPC
from hw6
where case
         when lower(substring(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) = 'nan' then null 
         else lower(substring(decode_url_part(url_parameters),'utm_campaign=([^&#$]+)')) 
         end is not null
group by ad_date,
         utm_campaign;
