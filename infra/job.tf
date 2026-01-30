resource "databricks_job" "incremental_pipeline" {
  name = "wheelie-incremental-load-and-validate"

  schedule {
    quartz_cron_expression = "0 5 * * * ?"
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  task {
    task_key = "load_incremental_facts"

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/load_incremental_facts.ipynb"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "load_incremental_dim"
    depends_on {
      task_key = "load_incremental_facts"
    }

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/load_incremental_dim.ipynb"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "test_business_logic"
    depends_on {
      task_key = "load_incremental_dim"
    }

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/test_business_logic.ipynb"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key = "test_data_quality"
    depends_on {
      task_key = "test_business_logic"
    }

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/test_data_quality.ipynb"
      source        = "WORKSPACE"
    }
  }
}
