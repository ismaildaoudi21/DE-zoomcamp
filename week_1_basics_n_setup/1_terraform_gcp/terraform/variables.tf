locals {
  data_lake_bucket = "dtc_data_lake"
}

variable "project" {
  description = "Project"
}

variable "region" {
  description = "Region"
  #Update the below to your desired region
  default     = "europe-west9"
}

variable "bucket_name" {
  description = "the name of the google cloud storage bucket"
  default = ""
}

variable "storage_class" {
  default = "STANDARD"
}

variable "BQ_DATASET" {
  type = string
  default = "trips_data_all"
}

variable "TABLE_NAME" {
  description = "BigQuery Table"
  type = string
  default = "ny_trips"
}