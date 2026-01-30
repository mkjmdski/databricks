data "databricks_spark_version" "latest_lts" {
  long_term_support = true
}

data "databricks_node_type" "smallest" {
  local_disk = true
}

resource "databricks_job" "incremental_pipeline" {
  name = "wheelie-incremental-load-and-validate"

  schedule {
    quartz_cron_expression = "0 5 * * * ?"
    timezone_id            = "UTC"
    pause_status           = "UNPAUSED"
  }

  job_cluster {
    job_cluster_key = "wheelie_job_cluster"
    new_cluster {
      spark_version = data.databricks_spark_version.latest_lts.id
      node_type_id  = data.databricks_node_type.smallest.id
      num_workers   = 1
    }
  }

  task {
    task_key        = "load_incremental_facts"
    job_cluster_key = "wheelie_job_cluster"

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/load_incremental_facts.ipynb"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key        = "load_incremental_dim"
    job_cluster_key = "wheelie_job_cluster"
    depends_on {
      task_key = "load_incremental_facts"
    }

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/load_incremental_dim.ipynb"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key        = "test_business_logic"
    job_cluster_key = "wheelie_job_cluster"
    depends_on {
      task_key = "load_incremental_dim"
    }

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/test_business_logic.ipynb"
      source        = "WORKSPACE"
    }
  }

  task {
    task_key        = "test_data_quality"
    job_cluster_key = "wheelie_job_cluster"
    depends_on {
      task_key = "test_business_logic"
    }

    notebook_task {
      notebook_path = "${databricks_repo.nutter_in_home.path}/notebooks/test_data_quality.ipynb"
      source        = "WORKSPACE"
    }
  }
}
