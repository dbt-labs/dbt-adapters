QUOTE_MODEL = """
version: 2
models:
  - name: quote_model
    description: "model to test column quotes and comments"
    columns:
      - name: 2id
        description: "XXX My description"
        quote: true
"""


MISSING_COL = """
version: 2
models:
  - name: missing_column
    columns:
      - name: id
        description: "test id column description"
      - name: column_that_does_not_exist
        description: "comment that cannot be created"
"""


SCHEMA_YML = """
version: 2

models:
  - name: table_model
    description: |
      Table model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
  - name: view_model
    description: |
      View model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting

seeds:
  - name: seed
    description: |
      Seed model description "with double quotes"
      and with 'single  quotes' as welll as other;
      '''abc123'''
      reserved -- characters
      80% of statistics are made up on the spot
      --
      /* comment */
      Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
    columns:
      - name: id
        description: |
          id Column description "with double quotes"
          and with 'single  quotes' as welll as other;
          '''abc123'''
          reserved -- characters
          80% of statistics are made up on the spot
          --
          /* comment */
          Some $lbl$ labeled $lbl$ and $$ unlabeled $$ dollar-quoting
      - name: name
        description: |
          Some stuff here and then a call to
          {{ doc('my_fun_doc')}}
"""
