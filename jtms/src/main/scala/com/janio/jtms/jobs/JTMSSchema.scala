package com.janio.jtms.jobs

import org.apache.spark.sql.types._

object JTMSSchema {

  /**
    * @return shipments table schema
    */
  def shipments = StructType(Array(
    StructField("id", IntegerType),
    StructField("shipment_id", StringType),
    StructField("status", StringType),
    StructField("driver_id", IntegerType),
    StructField("shipment_type", StringType),
    StructField("shipper_reference_number", StringType),
    StructField("master_airwaybill_number", StringType),
    StructField("pickup_instructions", StringType),
    StructField("scheduled_delivery_start_time", StringType),
    StructField("scheduled_delivery_end_time",StringType),
    StructField("delivery_instructions",StringType),
    StructField("package_items_qty",IntegerType),
    StructField("packaging_type",StringType),
    StructField("is_insured",StringType),
    StructField("service_type",StringType),
    StructField("priority",StringType),
    StructField("optional_1",StringType),
    StructField("optional_2",StringType),
    StructField("optional_3",StringType),
    StructField("optional_4",StringType),
    StructField("created_at",StringType),
    StructField("updated_at",StringType),
    StructField("partner_id",IntegerType),
    StructField("scheduled_pickup_start_time",StringType),
    StructField("scheduled_pickup_end_time",StringType),
    StructField("sender_or_receiver_region",StringType),
    StructField("is_completed",IntegerType),
    StructField("assigned_date",StringType),
    StructField("completed_date",StringType),
    StructField("is_pending",IntegerType),
    StructField("customer_signature",StringType),
    StructField("scheduled_pickup_date",StringType),
    StructField("scheduled_delivery_date",StringType),
    StructField("driver_note",StringType),
    StructField("completed_reason",StringType),
    StructField("failed_attempt",IntegerType),
    StructField("reschedule_date",StringType),
    StructField("reschedule_time",StringType),
    StructField("sender_address_details",StringType),
    StructField("receiver_address_details",StringType),
    StructField("package_items",StringType),
    StructField("package",StringType),
    StructField("payment",StringType),
    StructField("dropped_off",IntegerType),
    StructField("collected",IntegerType),
    StructField("pickup_or_delivery_date",StringType),
    StructField("receiver_type",StringType),
    StructField("receiver_name",StringType),
    StructField("sender_or_receiver_address_details",StringType),
    StructField("pop_pod_signature",StringType),
    StructField("rescheduled_attempt",IntegerType),
    StructField("merchant_name",StringType)
  ))


  /**
    * @return shipments table schema
    */
  def shipmenttrackers = StructType(Array(
    StructField("id", IntegerType),
    StructField("shipment_id", IntegerType),
    StructField("shipper_reference_number", StringType),
    StructField("created_at", StringType),
    StructField("action", StringType),
    StructField("extra_data", StringType),
    StructField("created_by", IntegerType)
  ))

  /**
    * @return drivers table schema
    */
  def drivers = StructType(Array(
    StructField("id", IntegerType),
    StructField("photo", StringType),
    StructField("pickup_shift_start", StringType),
    StructField("pickup_shift_end", StringType),
    StructField("delivery_shift_start", StringType),
    StructField("delivery_shift_end", StringType),
    StructField("vehicle_license_plate_number", StringType),
    StructField("vehicle_type", StringType),
    StructField("auth_id", IntegerType),
    StructField("partner_id", IntegerType),
    StructField("created_at", StringType),
    StructField("updated_at", StringType),
    StructField("employment_type", StringType),
    StructField("vehicle_volume", DoubleType),
    StructField("vehicle_weight", DoubleType),
    StructField("type", StringType),
    StructField("licence_front_card", StringType),
    StructField("licence_back_card", StringType),
    StructField("working_days", StringType),
    StructField("license_number", StringType),
    StructField("country", StringType),
    StructField("deactivate_type", StringType),
    StructField("deactivate_from_date", StringType),
    StructField("deactivate_to_date", StringType),
    StructField("region", StringType),
    StructField("password_text", StringType),
    StructField("pickup_region", StringType),
    StructField("delivery_region", StringType),
    StructField("company_name", StringType)
  ))


  /**
    * @return users table schema
    */
  def users = StructType(Array(
    StructField("id", IntegerType),
    StructField("email", StringType),
    StructField("password", StringType),
    StructField("role", StringType),
    StructField("status", IntegerType),
    StructField("name", StringType),
    StructField("contact_number", StringType),
    StructField("remember_token", StringType),
    StructField("created_at", StringType),
    StructField("updated_at", StringType)
  ))
}
