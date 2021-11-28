package com.janio.jobs

import org.apache.spark.sql.types.{StructField, _}

object JanioBacknedSchema {

  /**
    * @return Janio_order_order table schema
    */
  def janioOrderOrder() = StructType(Array(
    StructField("order_id", IntegerType),
    StructField("tracking_no", StringType),
    StructField("hawb_no", StringType),
    StructField("consignee_name", StringType),
    StructField("consignee_address", StringType),
    StructField("consignee_postal", StringType),
    StructField("consignee_country", StringType),
    StructField("consignee_city", StringType),
    StructField("consignee_state", StringType),
    StructField("order_length",DoubleType),
    StructField("order_width",DoubleType),
    StructField("order_height",DoubleType),
    StructField("order_weight",DoubleType),
    StructField("created_on",StringType),
    StructField("updated_on",StringType),
    StructField("pickup_address",StringType),
    StructField("agent_id_id",IntegerType),
    StructField("agent_application_id_id",IntegerType),
    StructField("service_id_id",IntegerType),
    StructField("warehouse_address",StringType),
    StructField("status_code",StringType),
    StructField("tracker_status_code",StringType),
    StructField("pickup_country",StringType),
    StructField("payment_type",StringType),
    StructField("shipper_order_id",StringType),
    StructField("submit_external_service_datetime",StringType),
    StructField("submit_firstmile_datetime",StringType),
    StructField("submit_warehouse_datetime",StringType),
    StructField("print_url",StringType),
    StructField("invoice_created",BooleanType),
    StructField("upload_batch_no",StringType),
    StructField("consignee_province",StringType),
    StructField("cod_amt_to_collect",DoubleType),
    StructField("tracker_main_text",StringType),
    StructField("store_id",StringType),
    StructField("delivery_note",StringType),
    StructField("pickup_city",StringType),
    StructField("pickup_contact_name",StringType),
    StructField("pickup_contact_number",StringType),
    StructField("pickup_postal",StringType),
    StructField("pickup_province",StringType),
    StructField("pickup_state",StringType),
    StructField("incoterm",StringType),
    StructField("print_default_label",BooleanType),
    StructField("is_processing",BooleanType),
    StructField("shipper_sub_account_id",StringType),
    StructField("shipper_sub_order_id",StringType),
    StructField("model_log_link_id",IntegerType),
    StructField("order_label_url",StringType),
    StructField("additional_data",StringType),
    StructField("private_tracker_status_code",StringType),
    StructField("private_tracker_updated_on",StringType),
    StructField("tracker_updated_on",StringType),
    StructField("est_completion_date",StringType),
    StructField("forward_order_id",IntegerType),
    StructField("service_level",StringType),
    StructField("is_return_order",BooleanType),
    StructField("returns_consolidation_period",StringType),
    StructField("latest_private_update_id",IntegerType),
    StructField("latest_update_id",IntegerType),
    StructField("latest_private_update_v2_id",IntegerType),
    StructField("latest_public_update_v2_id",IntegerType),
    StructField("latest_public_update_v2_shipper_id",IntegerType),
    StructField("redelivery_forward_order_id",IntegerType),
    StructField("failed_delivery_count",IntegerType),
    StructField("est_early_completion_date",StringType),
    StructField("order_type",StringType)

  ))

  /**
    * @return Janio_tracker_update table schema
    */
  def janioTrackerUpdate() :StructType = StructType(Array(
    StructField("update_id", IntegerType),
    StructField("tracking_no", StringType),
    StructField("main_text", StringType),
    StructField("detail_text", StringType),
    StructField("status", StringType),
    StructField("original_country", StringType),
    StructField("destination_country", StringType),
    StructField("updated_on", StringType),
    StructField("created_on", StringType),
    StructField("tracker_application_id_id",IntegerType),
    StructField("address",StringType),
    StructField("is_manual",BooleanType),
    StructField("model_log_link_id",IntegerType),
    StructField("partner_old_id",IntegerType),
    StructField("order_id",IntegerType),
    StructField("partner_data",StringType),
    StructField("partner_id_id",IntegerType),
    StructField("private_update_id",IntegerType)
  ))


  /**
    * @return janio_order_agentapplication table schema
    */
  def janioOrderAgentapplication() :StructType = StructType(Array(
    StructField("agent_application_id", IntegerType),
    StructField("agent_application_name", StringType),
    StructField("agent_application_contact_person", StringType),
    StructField("agent_application_secret_key", StringType),
    StructField("is_active", BooleanType),
    StructField("created_on", StringType),
    StructField("agent_id_id", IntegerType),
    StructField("agent_application_xero_contact_id",StringType),
    StructField("agent_application_credit_term",IntegerType),
    StructField("auto_invoice",BooleanType),
    StructField("require_pickup",BooleanType),
    StructField("send_consignee_pickup_notification",BooleanType),
    StructField("master_agent_application_id_id",IntegerType),
    StructField("privilege",StringType),
    StructField("agent_application_group",StringType),
    StructField("allow_order_submit",BooleanType),
    StructField("currencyinvoice_created",StringType),
    StructField("is_prepaid",BooleanType),
    StructField("webhook_url",StringType),
    StructField("billing_address",StringType),
    StructField("billing_country",StringType),
    StructField("company_registration_number",StringType),
    StructField("storage_url",StringType),
    StructField("billing_company_name",StringType),
    StructField("model_log_link_id",IntegerType),
    StructField("label_logo_url",StringType),
    StructField("order_label_webhook_url",StringType),
    StructField("webhook_basic_auth_password",StringType),
    StructField("webhook_basic_auth_username",StringType),
    StructField("acc_manager_name",StringType),
    StructField("can_view_invoices",BooleanType),
    StructField("label_preference",StringType),
    StructField("default_returns_consolidation_period",StringType)
  ))

  /**
    * @return janio_tracker_privateupdate table schema
    */
  def janioTrackerPrivateUpdate :StructType = StructType(Array(
    StructField("private_update_id", IntegerType),
    StructField("tracking_no", StringType),
    StructField("status", StringType),
    StructField("partner_data", StringType),
    StructField("updated_on", StringType),
    StructField("created_on", StringType),
    StructField("order_id", IntegerType),
    StructField("partner_old_id", IntegerType),
    StructField("partner_id_id", IntegerType),
    StructField("address",StringType),
    StructField("destination_country",StringType),
    StructField("detail_text",StringType),
    StructField("main_text",StringType),
    StructField("original_country",StringType),
    StructField("model_log_link_id",IntegerType)
  ))

  /**
    * @return janio_order_service table schema
    */
  def janioOrderService :StructType = StructType(Array(
    StructField("service_id", IntegerType),
    StructField("service_name", StringType),
    StructField("service_type", IntegerType),
    StructField("agent_id_id", IntegerType),
    StructField("service_destination_country", StringType),
    StructField("service_origin_city", StringType),
    StructField("service_external_service_id", IntegerType),
    StructField("allow_pickup", BooleanType),
    StructField("pickup_address", StringType),
    StructField("pickup_city",StringType),
    StructField("pickup_contact_name",StringType),
    StructField("pickup_country",StringType),
    StructField("pickup_postal",StringType),
    StructField("pickup_province",StringType),
    StructField("pickup_state", StringType),
    StructField("allow_cod", BooleanType),
    StructField("service_category",StringType),
    StructField("model_log_link_id",IntegerType),
    StructField("order_submission_config_id",IntegerType),
    StructField("dropoff_point_id",IntegerType)
  ))

  /**
    * @return janio_payment_paymentsettings table schema
    */
  def janioPaymentPaymentSettings :StructType = StructType(Array(
    StructField("payment_settings_id", IntegerType),
    StructField("agent_application_id_id", IntegerType),
    StructField("billing_currency", StringType),
    StructField("volumetric_weight_factor", IntegerType),
    StructField("xero_invoice_merge_lanes", BooleanType),
    StructField("xero_organisation_name", StringType),
    StructField("xero_contact_id", StringType),
    StructField("weight_policy", StringType)
  ))


  /**
    * @return janio_payment_paymentsettings_pricing_groups table schema
    */
  def janioPaymentSettingsPricingGroups :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("paymentsettings_id", IntegerType),
    StructField("pricinggroup_id", IntegerType)
  ))

  /**
    * @return janio_payment_pricinggroup table schema
    */
  def janioPaymentPricingGroup :StructType = StructType(Array(
    StructField("pricing_group_id", IntegerType),
    StructField("pricing_group_name", StringType),
    StructField("consignee_country", StringType),
    StructField("pickup_country", StringType),
    StructField("destination_zone_id", IntegerType),
    StructField("origin_zone_id", IntegerType),
    StructField("order_direction", StringType)

  ))

  /**
    * @return janio_partner_partner table schema
    */
  def janioPartnerPartner :StructType = StructType(Array(
    StructField("partner_id", IntegerType),
    StructField("partner_name", StringType),
    StructField("is_active", BooleanType),
    StructField("created_on", StringType)

  ))

  /**
    * @return janio_order_item table schema
    */
  def janioOrderItem :StructType = StructType(Array(
    StructField("item_id", IntegerType),
    StructField("item_desc", StringType),
    StructField("item_category", StringType),
    StructField("item_quantity", IntegerType),
    StructField("item_price_value", DoubleType),
    StructField("item_price_currency", StringType),
    StructField("order_id_id", IntegerType),
    StructField("item_product_id", StringType),
    StructField("item_sku", StringType),
    StructField("model_log_link_id", IntegerType)

  ))

  /**
    * @return janio_order_orderpickupdetails table schema
    */
  def janioOrderOrderPickUpDetails :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("pickup_date", StringType),
    StructField("pickup_time_from", StringType),
    StructField("pickup_time_to", StringType),
    StructField("order_id", IntegerType),
    StructField("pickup_notes", StringType)
  ))

  /**
    * @return janio_analytics_ordertrackerinfoanalytics table schema
    */
  def janioAnalyticsOrderTrackerInfoAnalytics :StructType = StructType(Array(
    StructField("analytics_created_on", StringType),
    StructField("analytics_updated_on", StringType),
    StructField("order_tracker_info_analytics_id", IntegerType),
    StructField("order_id", IntegerType),
    StructField("undelivered_reason", StringType),
    StructField("attempt_1_on", StringType),
    StructField("attempt_2_on", StringType),
    StructField("attempt_3_on", StringType),
    StructField("tracking_no", StringType),
    StructField("attempt_count", IntegerType)
  ))

  /**
    * @return janio_sla_orderslazone table schema
    */
  def janioSLAOrdersLAZone :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("order_type", StringType),
    StructField("destination_zone_id", IntegerType),
    StructField("order_id", IntegerType),
    StructField("origin_zone_id", IntegerType)
  ))

  /**
    * @return janio_location_zone table schema
    */
  def janioLocationZone :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("type", StringType),
    StructField("address_type", StringType)
  ))

  /**
    * @return janio_order_orderzone table schema
    */
  def janioOrderOrderZone :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("address_type", StringType),
    StructField("zone_name", StringType),
    StructField("zone_type", StringType),
    StructField("is_active", BooleanType),
    StructField("created_on", StringType),
    StructField("address_translation_id", IntegerType),
    StructField("order_id", IntegerType),
    StructField("zone_id", IntegerType)
  ))

  /**
    * @return janio_cod_orderremittancedata table schema
    */
  def janioCodOrderRemittanceData :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("agent_application_remitted_total_amount", DoubleType),
    StructField("partner_remitted_total_amount", DoubleType),
    StructField("order_id", IntegerType)
  ))

  /**
    * @return janio_order_externalorderinfo table schema
    */
  def janioOrderExternalorderinfo :StructType = StructType(Array(
    StructField("ext_order_info_id", IntegerType),
    StructField("partner_name", StringType),
    StructField("external_tracking_no", StringType),
    StructField("external_print_url", StringType),
    StructField("order_id_id", IntegerType),
    StructField("model_log_link_id", IntegerType),
    StructField("additional_data", StringType),
    StructField("third_party_label_url", StringType)
  ))

  /**
    * @return janio_tracker_publicupdatev2 table schema
    */
  def janioTrackerPublicupdatev2 :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("status", StringType),
    StructField("is_shipper_only", BooleanType),
    StructField("private_update_id", IntegerType)
  ))

  /**
    * @return janio_sla_partnerdelay table schema
    */
  def janioSlaPartnerdelay :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("delay_category", StringType),
    StructField("delay_reason", StringType),
    StructField("other_remarks", StringType),
    StructField("response_date", StringType),
    StructField("revised_estimated_completion_date", StringType),
    StructField("no_of_days_delayed", IntegerType),
    StructField("delay_method", StringType),
    StructField("created_on", StringType),
    StructField("created_by_id", IntegerType),
    StructField("mawb_id", IntegerType),
    StructField("order_id", IntegerType),
    StructField("partner_id", IntegerType)
  ))

  /**
    * @return janio_order_orderpickupdatelog table schema
    */
  def janioOrderOrderpickupdatelog :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("confirmed_on", StringType),
    StructField("pickup_date", StringType),
    StructField("order_id", IntegerType)
  ))

  /**
   * @return janio_order_weightreconrecord table schema
   */
  def janioOrderWeightreconrecord :StructType = StructType(Array(
    StructField("weight_recon_id", IntegerType),
    StructField("reconciled_weight", DoubleType),
    StructField("created_on", StringType),
    StructField("order_id_id", IntegerType),
    StructField("reconciled_height", DoubleType),
    StructField("reconciled_length", DoubleType),
    StructField("reconciled_width", DoubleType),
    StructField("source", StringType)
  ))

  /**
   * @return janio_tracker_partnerupdate table schema
   */
  def janioTrackerPartnerupdate :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("partner_status", StringType),
    StructField("remark", StringType),
    StructField("updated_on", StringType),
    StructField("created_on", StringType),
    StructField("order_id", IntegerType),
    StructField("partner_id", IntegerType),
    StructField("private_update_id", IntegerType),
    StructField("tracking_no", StringType),
    StructField("private_update_v2_id", IntegerType)
  ))

  /**
   * @return janio_activity_orderactivity table schema
   */
  def janioActivityOrderactivity :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("name", StringType),
    StructField("source_type", StringType),
    StructField("source_value", StringType),
    StructField("data", StringType),
    StructField("reason", StringType),
    StructField("activity_on", StringType),
    StructField("created_on", StringType),
    StructField("order_id", IntegerType),
    StructField("source_agent_application_id", IntegerType),
    StructField("source_user_id", IntegerType)
  ))

  /**
   * @return janio_tracker_privateupdatev1v2link table schema
   */
  def janioTrackerPrivateupdatev1v2link :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("private_update_v1_id", IntegerType),
    StructField("private_update_v2_id", IntegerType)
  ))

  /**
   * @return janio_tracker_privateupdatev2 table schema
   */
  def janio_tracker_privateupdatev2 :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("status", StringType),
    StructField("description", StringType),
    StructField("remarks", StringType),
    StructField("country", StringType),
    StructField("address", StringType),
    StructField("partner_data", StringType),
    StructField("additional_data", StringType),
    StructField("updated_on", StringType),
    StructField("created_on", StringType),
    StructField("order_id", IntegerType),
    StructField("partner_id", IntegerType),
    StructField("user_id", IntegerType)
  ))

  /**
   * @return janio_midmile_tracker_masterairwaybill_orders table schema
   */
  def janioMidmileTrackerMasterairwaybillOrders :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("masterairwaybill_id", IntegerType),
    StructField("order_id", IntegerType)
  ))

  /**
   * @return janio_midmile_tracker_houseairwaybill_orders table schema
   */
  def janioMidmileTrackerHouseairwaybillOrders :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("houseairwaybill_id", IntegerType),
    StructField("order_id", IntegerType)
  ))

  /**
   * @return janio_hms_session table schema
   */
  def janioHmsSession :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("session_id", StringType),
    StructField("created_on", StringType),
    StructField("hub_id", IntegerType),
    StructField("hub_scanner_id", IntegerType),
    StructField("scan_type_id", IntegerType)
  ))

  /**
   * @return janio_hms_sessionorder table schema
   */
  def janioHmsSessionorder :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("order_id", IntegerType),
    StructField("session_id", IntegerType),
    StructField("scan_type_id", IntegerType),
    StructField("created_on", StringType),
    StructField("group_id", StringType),
    StructField("updated_on", StringType)
  ))

  /**
   * @return janio_hms_sessionorderlog table schema
   */
  def janioHmsSessionorderlog :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("tracking_no", StringType),
    StructField("status_text", StringType),
    StructField("status_code", StringType),
    StructField("created_on", StringType),
    StructField("hub_scanner_id", IntegerType),
    StructField("order_id", IntegerType),
    StructField("scan_type", StringType),
    StructField("session_id", IntegerType)
  ))

  /**
   * @return janio_order_orderlink table schema
   */
  def janioOrderOrderlink :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("link_type", StringType),
    StructField("linked_order_id", IntegerType),
    StructField("original_order_id", IntegerType)
  ))

  /**
   * @return janio_hms_temporarygroup table schema
   */
  def janioHmsTemporarygroup :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("group_id", StringType),
    StructField("created_on", StringType),
    StructField("original_order_id", IntegerType)
  ))

  /**
   * @return janio_hms_temporarygrouporders table schema
   */
  def janioHmsTemporarygrouporders :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("tracking_no", StringType),
    StructField("created_on", StringType),
    StructField("temporary_group_id", IntegerType)
  ))

  /**
   * @return janio_hms_temporaryorder table schema
   */
  def janioHmsTemporaryorder :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("tracking_no", StringType),
    StructField("created_on", StringType),
    StructField("updated_on", StringType),
    StructField("active", BooleanType)
  ))

  /**
   * @return janio_order_orderconsigneedetails table schema
   */
  def janioOrderOrderconsigneedetails :StructType = StructType(Array(
    StructField("id", IntegerType),
    StructField("identification_type", StringType),
    StructField("identification_number", StringType),
    StructField("order_id_id", IntegerType)
  ))
}

