# Snowplow BigQuery Loader

[![Build Status][travis-image]][travis]
[![Release][release-image]][releases]
[![License][license-image]][license]

This project contains applications used to load [Snowplow][snowplow] enriched data into [Google BigQuery][bigquery].

## Quickstart

Assuming git and [SBT][sbt] installed:

```bash
$ git clone https://github.com/snowplow-incubator/snowplow-bigquery-loader
$ cd snowplow-bigquery-loader
$ sbt "project loader" test
$ sbt "project mutator" test
```

## Deduplication steps on BigQuery

1. Before running these queires, `duplicates` and `duplicate_structs` datesets should be deleted if exists and created again after that. After configure datasets we need to run below sql script to save duplicated event ids in another table.

```sql
CREATE TABLE duplicates.tmp_events_id
AS (

  SELECT event_id
  FROM (

    SELECT event_id, COUNT(*) AS count
    FROM prisma_dataset.events
      --AND collector_tstamp > DATEADD(week, -4, CURRENT_DATE) -- restricts table scan for the previous 4 weeks to make queries more efficient; uncomment after running the first time
    GROUP BY 1

  )

  WHERE count > 1

);
```

2. We have duplicated event ids. We also need to save duplicated event rows with row numbers in another table. Because in the next step we'll delete all these events from events table and add just first row of each duplicated event.

```sql
CREATE TABLE duplicates.tmp_events
AS (

  SELECT *, ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY dvce_created_tstamp) as event_number
  FROM prisma_dataset.events
  WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id)

);
```

3. Now we can delete these events

```sql
DELETE FROM prisma_dataset.events
  WHERE event_id IN (SELECT event_id FROM duplicates.tmp_events_id) AND collector_tstamp < TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 40 MINUTE);
```

We need to use this where condition because BQ doesn't allow deleting of just streamed rows. Or we need to shut down collector and after wait a while we can run the query without this time condition in where state.

4. We have clean table now but we lost events which was duplicated. We can add just first line of them with the query below.

```sql
INSERT INTO prisma_dataset.events (

    SELECT

      app_id, platform, etl_tstamp, collector_tstamp, dvce_created_tstamp, event, event_id, txn_id,
      name_tracker, v_tracker, v_collector, v_etl,
      user_id, user_ipaddress, user_fingerprint, domain_userid, domain_sessionidx, network_userid,
      geo_country, geo_region, geo_city, geo_zipcode, geo_latitude, geo_longitude, geo_region_name,
      ip_isp, ip_organization, ip_domain, ip_netspeed, page_url, page_title, page_referrer,
      page_urlscheme, page_urlhost, page_urlport, page_urlpath, page_urlquery, page_urlfragment,
      refr_urlscheme, refr_urlhost, refr_urlport, refr_urlpath, refr_urlquery, refr_urlfragment,
      refr_medium, refr_source, refr_term, mkt_medium, mkt_source, mkt_term, mkt_content, mkt_campaign,
      se_category, se_action, se_label, se_property, se_value,
      tr_orderid, tr_affiliation, tr_total, tr_tax, tr_shipping, tr_city, tr_state, tr_country,
      ti_orderid, ti_sku, ti_name, ti_category, ti_price, ti_quantity,
      pp_xoffset_min, pp_xoffset_max, pp_yoffset_min, pp_yoffset_max,
      useragent, br_name, br_family, br_version, br_type, br_renderengine, br_lang, br_features_pdf, br_features_flash,
      br_features_java, br_features_director, br_features_quicktime, br_features_realplayer, br_features_windowsmedia,
      br_features_gears, br_features_silverlight, br_cookies, br_colordepth, br_viewwidth, br_viewheight,
      os_name, os_family, os_manufacturer, os_timezone, dvce_type, dvce_ismobile, dvce_screenwidth, dvce_screenheight,
      doc_charset, doc_width, doc_height, tr_currency, tr_total_base, tr_tax_base, tr_shipping_base,
      ti_currency, ti_price_base, base_currency, geo_timezone, mkt_clickid, mkt_network, etl_tags,
      dvce_sent_tstamp, refr_domain_userid, refr_dvce_tstamp, domain_sessionid,
      derived_tstamp, event_vendor, event_name, event_format, event_version, event_fingerprint, true_tstamp,
      contexts_com_snowplowanalytics_snowplow_mobile_context_1_0_1, contexts_com_snowplowanalytics_snowplow_client_session_1_0_1, unstruct_event_com_snowplowanalytics_snowplow_screen_view_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_application_error_1_0_0, contexts_com_snowplowanalytics_snowplow_geolocation_context_1_1_0, unstruct_event_com_snowplowanalytics_snowplow_timing_1_0_0, contexts_com_snowplowanalytics_mobile_application_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_application_background_1_0_0, contexts_com_snowplowanalytics_mobile_screen_1_0_0, unstruct_event_com_snowplowanalytics_mobile_screen_view_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_link_click_1_0_1, unstruct_event_com_snowplowanalytics_mobile_application_install_1_0_0, unstruct_event_com_snowplowanalytics_snowplow_application_foreground_1_0_0

    FROM duplicates.tmp_events WHERE event_number = 1

  );
```


## Copyright and License

Snowplow BigQuery Loader is copyright 2018 Snowplow Analytics Ltd.

Licensed under the **[Apache License, Version 2.0][license]** (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[snowplow]: https://github.com/snowplow/snowplow/
[bigquery]: https://cloud.google.com/bigquery/
[sbt]: https://www.scala-sbt.org/

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[travis]: https://travis-ci.org/snowplow-incubator/snowplow-bigquery-loader
[travis-image]: https://travis-ci.org/snowplow-incubator/snowplow-bigquery-loader.png?branch=master

[release-image]: http://img.shields.io/badge/release-0.1.0-blue.svg?style=flat
[releases]: https://github.com/snowplow-incubator/snowplow-bigquery-loader

[techdocs-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/techdocs.png
[setup-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/setup.png
[contributing-image]: https://d3i6fms1cm1j0i.cloudfront.net/github/images/contributing.png

[techdocs]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/wiki
[setup]: https://github.com/snowplow-incubator/snowplow-bigquery-loader/wiki/Setup-guide
[contributing]: https://github.com/snowplow/snowplow/wiki/Contributing
