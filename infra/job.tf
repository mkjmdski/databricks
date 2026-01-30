resource "databricks_job" "incremental_pipeline" {
  name = "wheelie-incremental-load-and-validate"

  run_as {
    service_principal_name = data.databricks_current_user.this.user_name
  }

  schedule {
    quartz_cron_expression = "0 5 * * * ?"
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  task {
    task_key = "incremental_load_facts"

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/incremental_load_facts"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "incremental_load_dim"
    depends_on {
      task_key = "incremental_load_facts"
    }

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/incremental_load_dim"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "test_business_logic"
    max_retries = 0
    depends_on {
      task_key = "incremental_load_dim"
    }

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test_business_logic"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key    = "test_data_quality"
    max_retries = 0
    depends_on {
      task_key = "incremental_load_dim"
    }

    notebook_task {
      notebook_path = "${databricks_repo.main.path}/notebooks/test_data_quality"
      source        = "WORKSPACE"
    }
  }
}
