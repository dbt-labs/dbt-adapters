# dbt-hologres Example Project

This directory contains example dbt models demonstrating the key features of the dbt-hologres adapter.

## Setup

1. Configure your `profiles.yml`:

```yaml
hologres_example:
  target: dev
  outputs:
    dev:
      type: hologres
      host: hgpostcn-cn-wyc4l7i67022-cn-hangzhou.hologres.aliyuncs.com
      port: 80
      user: BASIC$dbt_user
      password: Leeyd#1988
      database: from_dbt
      schema: public
      threads: 4
```

2. Run the example:

```bash
dbt debug  # Verify connection
dbt run    # Run all models
dbt test   # Run tests
```

## Example Models

### 1. Simple Table Model
`models/staging/stg_orders.sql` - Basic table materialization

### 2. View Model
`models/marts/orders_summary.sql` - View materialization

### 3. Incremental Model
`models/marts/orders_incremental.sql` - Incremental updates with merge strategy

### 4. Dynamic Table Model
`models/marts/orders_dynamic_table.sql` - Hologres Dynamic Table with auto-refresh

## Model Configurations

See `dbt_project.yml` for model-specific configurations including:
- Materialization strategies
- Dynamic Table settings
- Incremental strategies
- Schema configurations
