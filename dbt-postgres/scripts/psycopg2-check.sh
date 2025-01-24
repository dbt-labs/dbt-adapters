python -m venv venv
source venv/bin/activate
python -m pip install .

if [[ "$PSYCOPG2_WORKAROUND" == true ]]; then
    if [[ $(pip show psycopg2-binary) ]]; then
        PSYCOPG2_VERSION=$(pip show psycopg2-binary | grep Version | cut -d " " -f 2)
        pip uninstall -y psycopg2-binary
        pip install psycopg2==$PSYCOPG2_VERSION
    fi
fi

PSYCOPG2_NAME=$((pip show psycopg2 || pip show psycopg2-binary) | grep Name | cut -d " " -f 2)
if [[ "$PSYCOPG2_NAME" != "$PSYCOPG2_EXPECTED_NAME" ]]; then
    echo -e 'Expected: "$PSYCOPG2_EXPECTED_NAME" but found: "$PSYCOPG2_NAME"'
    exit 1
fi

deactivate
rm -r ./venv
