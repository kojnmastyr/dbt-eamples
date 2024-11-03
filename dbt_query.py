@cli.command()
@query_options
@click.option(
    "--csv",
    type=click.File("w"),
    required=False,
    help="Provide filepath for data_table output to csv",
)
@click.option(
    "--explain",
    is_flag=True,
    required=False,
    default=False,
    help="In the query output, show the query that was executed against the data warehouse",
)
@click.option(
    "--show-dataflow-plan",
    is_flag=True,
    required=False,
    default=False,
    help="Display dataflow plan in explain output",
)
@click.option(
    "--display-plans",
    is_flag=True,
    required=False,
    help="Display plans (e.g. metric dataflow) in the browser",
)
@click.option(
    "--decimals",
    required=False,
    default=2,
    help="Choose the number of decimal places to round for the numerical values",
)
@click.option(
    "--show-sql-descriptions",
    is_flag=True,
    default=False,
    help="Shows inline descriptions of nodes in displayed SQL",
)
@click.option(
    "--saved-query",
    required=False,
    help="Specify the name of the saved query to use for applicable parameters",
)
@click.option(
    "--output-dir",
    type=click.Path(file_okay=False, dir_okay=True, writable=True),
    required=True,
    help="Directory to save generated SQL files",
)
@pass_config
@exception_handler
@log_call(module_name=__name__, telemetry_reporter=_telemetry_reporter)
def query(
    cfg: CLIContext,
    metrics: Optional[Sequence[str]] = None,
    group_by: Optional[Sequence[str]] = None,
    where: Optional[str] = None,
    start_time: Optional[dt.datetime] = None,
    end_time: Optional[dt.datetime] = None,
    order: Optional[List[str]] = None,
    limit: Optional[int] = None,
    csv: Optional[click.utils.LazyFile] = None,
    explain: bool = False,
    show_dataflow_plan: bool = False,
    display_plans: bool = False,
    decimals: int = DEFAULT_RESULT_DECIMAL_PLACES,
    show_sql_descriptions: bool = False,
    saved_query: Optional[str] = None,
    output_dir: Optional[str] = None,
) -> None:
    """Create a new query with MetricFlow and assembles a MetricFlowQueryResult."""
    start = time.time()
    spinner = Halo(text="Initiating query…", spinner="dots")
    spinner.start()
    mf_request = MetricFlowQueryRequest.create_with_random_request_id(
        saved_query_name=saved_query,
        metric_names=metrics,
        group_by_names=group_by,
        limit=limit,
        time_constraint_start=start_time,
        time_constraint_end=end_time,
        where_constraints=[where] if where else None,
        order_by_names=order,
    )

    explain_result: Optional[MetricFlowExplainResult] = None
    query_result: Optional[MetricFlowQueryResult] = None

    if explain:
        explain_result = cfg.mf.explain(mf_request=mf_request)
    else:
        query_result = cfg.mf.query(mf_request=mf_request)

    spinner.succeed(f"Success 🦄 - query completed after {time.time() - start:.2f} seconds")

    if explain:
        assert explain_result
        sql = (
            explain_result.rendered_sql_without_descriptions.sql_query
            if not show_sql_descriptions
            else explain_result.rendered_sql.sql_query
        )
        if show_dataflow_plan:
            click.echo("🔎 Generated Dataflow Plan + SQL (remove --explain to see data):")
            click.echo(
                textwrap.indent(
                    jinja2.Template(
                        textwrap.dedent(
                            """\
                            Metric Dataflow Plan:
                                {{ plan_text | indent(4) }}
                            """
                        ),
                        undefined=jinja2.StrictUndefined,
                    ).render(plan_text=explain_result.dataflow_plan.structure_text()),
                    prefix="-- ",
                )
            )
            click.echo("")
        else:
            click.echo(
                "🔎 SQL (remove --explain to see data or add --show-dataflow-plan to see the generated dataflow plan):"
            )
        click.echo(sql)
        if display_plans:
            click.echo("Creating temporary directory for storing visualization output.")
            temp_path = tempfile.mkdtemp()
            svg_path = display_dag_as_svg(explain_result.dataflow_plan, temp_path)
            click.echo("")
            click.echo(f"Plan SVG saved to: {svg_path}")
        exit()

    assert query_result
    df = query_result.result_df
    # Show the data if returned successfully
    if df is not None:
        if df.row_count == 0:
            click.echo("🕳 Successful MQL query returned an empty result set.")
        elif csv is not None:
            # csv is a LazyFile that is file-like that works in this case.
            csv_writer = csv_module.writer(csv)
            csv_writer.writerow(df.column_names)
            for row in df.rows:
                csv_writer.writerow(row)
            click.echo(f"🖨 Successfully written query output to {csv.name}")
        else:
            click.echo(df.text_format(decimals))
        if display_plans:
            temp_path = tempfile.mkdtemp()
            svg_path = display_dag_as_svg(query_result.dataflow_plan, temp_path)
            click.echo(f"Plan SVG saved to: {svg_path}")
        
        # New functionality: Generate SQL files for each dimension
        if metrics and not group_by:
            dimensions = cfg.mf.simple_dimensions_for_metrics(metrics)
            if not dimensions:
                click.echo(f"No dimensions found for the provided metric(s): {metrics}")
                exit(1)
            
            output_dir_path = pathlib.Path(output_dir)
            if not output_dir_path.exists():
                output_dir_path.mkdir(parents=True)
            
            for dimension in dimensions:
                dimension_name = dimension.granularity_free_qualified_name
                mf_request = MetricFlowQueryRequest.create_with_random_request_id(
                    saved_query_name=saved_query,
                    metric_names=metrics,
                    group_by_names=[dimension_name],
                    limit=limit,
                    time_constraint_start=start_time,
                    time_constraint_end=end_time,
                    where_constraints=[where] if where else None,
                    order_by_names=order,
                )
                explain_result = cfg.mf.explain(mf_request=mf_request)
                sql = explain_result.rendered_sql.sql_query
                sql_file_path = output_dir_path / f"{dimension_name}.sql"
                with open(sql_file_path, 'w') as sql_file:
                    sql_file.write(sql)
                click.echo(f"SQL for dimension {dimension_name} written to {sql_file_path}")