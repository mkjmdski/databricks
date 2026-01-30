resource "databricks_job" "parallel_bronze_load" {
  name = "parallel-bronze-load"

  run_as {
    service_principal_name = data.databricks_current_user.this.user_name
  }

  #   schedule {
  #     quartz_cron_expression = "0 5 * * * ?"
  #     timezone_id            = "UTC"
  #     pause_status           = "UNPAUSED"
  #   }

  task {
    task_key = "parallel_bronze_car_inventory"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/car_inventory"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_bronze_geo_staff_store"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/geo_staff_store"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_bronze_payment"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/payment"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_bronze_rental"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/rental"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_bronze_service_customer"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/service_customer"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "trigger_parallel_dim_load"
    depends_on { task_key = "parallel_bronze_car_inventory" }
    depends_on { task_key = "parallel_bronze_geo_staff_store" }
    depends_on { task_key = "parallel_bronze_payment" }
    depends_on { task_key = "parallel_bronze_rental" }
    depends_on { task_key = "parallel_bronze_service_customer" }

    run_job_task {
      job_id = databricks_job.parallel_dim_load.id
    }
  }
}

resource "databricks_job" "parallel_dim_load" {
  name = "parallel-dim-load"

  run_as {
    service_principal_name = data.databricks_current_user.this.user_name
  }

  task {
    task_key = "parallel_dim_car_date"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_car_date"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_customer_equipment"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_customer_equipment"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_manager_staff"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_manager_staff"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_store_fact_service"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/fact/dim_store_fact_service"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_fact_rental"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/fact/fact_rental"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "trigger_parallel_test"
    depends_on { task_key = "parallel_dim_car_date" }
    depends_on { task_key = "parallel_dim_customer_equipment" }
    depends_on { task_key = "parallel_dim_manager_staff" }
    depends_on { task_key = "parallel_dim_store_fact_service" }
    depends_on { task_key = "parallel_fact_rental" }

    run_job_task {
      job_id = databricks_job.parallel_test.id
    }
  }
}

resource "databricks_job" "parallel_test" {
  name = "parallel-test"

  run_as {
    service_principal_name = data.databricks_current_user.this.user_name
  }

  task {
    task_key    = "test_business_logic_parallel"
    max_retries = 0

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test/business_logc"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "test_data_quality_parallel"
    max_retries = 0

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test/data_quality"
      source        = "WORKSPACE"
    }
  }
}
