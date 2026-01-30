resource "databricks_job" "parallel_incremental_pipeline" {
  name = "wheelie-parallel-load-and-validate"

  run_as {
    service_principal_name = data.databricks_current_user.this.user_name
  }

  #   schedule {
  #     quartz_cron_expression = "0 5 * * * ?"
  #     timezone_id            = "UTC"
  #     pause_status           = "UNPAUSED"
  #   }

  task {
    task_key = "parallel_bronze_geo"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/geo"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_bronze_staff_store"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/staff_store"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_bronze_car_inventory"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/car_inventory"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_bronze_customer"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/customer"
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
    task_key = "parallel_bronze_service"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/bronze/service"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_customer"
    depends_on { task_key = "parallel_bronze_geo" }
    depends_on { task_key = "parallel_bronze_customer" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_customer"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_staff"
    depends_on { task_key = "parallel_bronze_geo" }
    depends_on { task_key = "parallel_bronze_staff_store" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_staff"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_manager"
    depends_on { task_key = "parallel_bronze_geo" }
    depends_on { task_key = "parallel_bronze_staff_store" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_manager"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_store"
    depends_on { task_key = "parallel_bronze_geo" }
    depends_on { task_key = "parallel_bronze_staff_store" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_store"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_car"
    depends_on { task_key = "parallel_bronze_car_inventory" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_car"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_dim_equipment"
    depends_on { task_key = "parallel_bronze_car_inventory" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/dim_equipment"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_fact_service"
    depends_on { task_key = "parallel_bronze_service" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/fact/fact_service"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_fact_rental"
    depends_on { task_key = "parallel_bronze_rental" }
    depends_on { task_key = "parallel_bronze_payment" }
    depends_on { task_key = "parallel_bronze_car_inventory" }
    depends_on { task_key = "parallel_bronze_staff_store" }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/fact/fact_rental"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "parallel_role_playing_dates"
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/parallel/dim/role_playing_dates"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "test_business_logic_parallel"
    max_retries = 0
    depends_on { task_key = "parallel_dim_customer" }
    depends_on { task_key = "parallel_dim_staff" }
    depends_on { task_key = "parallel_dim_manager" }
    depends_on { task_key = "parallel_dim_store" }
    depends_on { task_key = "parallel_dim_car" }
    depends_on { task_key = "parallel_dim_equipment" }
    depends_on { task_key = "parallel_fact_service" }
    depends_on { task_key = "parallel_fact_rental" }
    depends_on { task_key = "parallel_role_playing_dates" }

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test/business_logc"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "test_data_quality_parallel"
    max_retries = 0
    depends_on { task_key = "parallel_dim_customer" }
    depends_on { task_key = "parallel_dim_staff" }
    depends_on { task_key = "parallel_dim_manager" }
    depends_on { task_key = "parallel_dim_store" }
    depends_on { task_key = "parallel_dim_car" }
    depends_on { task_key = "parallel_dim_equipment" }
    depends_on { task_key = "parallel_fact_service" }
    depends_on { task_key = "parallel_fact_rental" }
    depends_on { task_key = "parallel_role_playing_dates" }

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test/data_quality"
      source        = "WORKSPACE"
    }
  }
}
