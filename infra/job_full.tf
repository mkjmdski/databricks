resource "databricks_job" "full_load_pipeline" {
  name = "wheelie-full-load-and-validate"

  run_as {
    service_principal_name = data.databricks_current_user.this.user_name
  }

  #   schedule {
  #     quartz_cron_expression = "0 5 * * * ?"
  #     timezone_id            = "UTC"
  #     pause_status           = "UNPAUSED"
  #   }

  task {
    task_key    = "full_load_dim"
    max_retries = 0
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/full_load/dim"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "full_load_facts"
    max_retries = 0
    depends_on {
      task_key = "full_load_dim"
    }
    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/full_load/facts"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "test_business_logic_full"
    max_retries = 0
    depends_on { task_key = "full_load_facts" }

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test/business_logc"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "test_data_quality_full"
    max_retries = 0
    depends_on { task_key = "full_load_facts" }

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test/data_quality"
      source        = "WORKSPACE"
    }
  }
}
