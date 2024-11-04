@cli.command()
@click.option(
    "--metric",
    required=True,
    help="The name of the metric for which to generate SQL queries",
)
@click.option(
    "--output-dir",
    required=True,
    help="The directory where to save the SQL files",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def dump_queries(cfg: CLIContext, metric: str, output_dir: str) -> None:
    """Generate SQL queries for a given metric for each of its dimensions with accompanying dimensions."""
    import pathlib
    from halo import Halo

    spinner = Halo(text=f"Fetching metric '{metric}'...", spinner="dots")
    spinner.start()

    # Get the list of metrics
    metrics = cfg.mf.list_metrics()
    # Find the metric with the given name
    metric_obj = next((m for m in metrics if m.name == metric), None)

    if not metric_obj:
        spinner.fail(f"Metric '{metric}' not found.")
        exit(1)
    else:
        spinner.succeed(f"Metric '{metric}' found.")

    # Build the semantic manifest
    project_root = pathlib.Path.cwd()
    semantic_manifest = dbtArtifacts.build_semantic_manifest_from_dbt_project_root(project_root=project_root)
    semantic_models = semantic_manifest.semantic_models

    # Get the dimensions of the metric
    dimensions = [dimension.granularity_free_qualified_name for dimension in metric_obj.dimensions]

    # Create the output directory if it doesn't exist
    output_path = pathlib.Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    # For each dimension, generate the query and write to a file
    for dimension in dimensions:
        spinner = Halo(text=f"Generating query for dimension '{dimension}'...", spinner="dots")
        spinner.start()

        # Find accompanying dimensions from the same semantic model
        accompanying_dimensions = set()
        for semantic_model in semantic_models:
            # Check if the dimension is in this semantic model
            if any(dim.name == dimension for dim in semantic_model.dimensions):
                # Add all dimensions from this semantic model
                accompanying_dimensions.update([dim.name for dim in semantic_model.dimensions])
                break  # Assuming each dimension is unique across semantic models

        group_by_dims = list(accompanying_dimensions)

        # Create a MetricFlowQueryRequest with group_by_names including accompanying dimensions
        mf_request = MetricFlowQueryRequest.create_with_random_request_id(
            metric_names=[metric],
            group_by_names=group_by_dims,
            # Include any additional parameters if necessary
        )

        # Get the SQL query
        explain_result = cfg.mf.explain(mf_request=mf_request)
        sql_query = explain_result.rendered_sql.sql_query

        # Sanitize the dimension name for filename
        safe_dimension_name = dimension.replace('.', '_').replace('/', '_')
        file_name = f"{metric}_by_{safe_dimension_name}.sql"
        file_path = output_path / file_name
        with open(file_path, 'w') as f:
            f.write(sql_query)

        spinner.succeed(f"Query for dimension '{dimension}' written to '{file_path}'")

    click.echo(f"All queries have been generated and saved to '{output_dir}'.")

if __name__ == "__main__":
    cli()